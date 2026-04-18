"""
Eval harness — measure extraction precision/recall against a golden set.

Seed ``eval_districts`` in Supabase with 20-30 rows you already trust, then
call ``run_eval()`` to score the current pipeline. Each run is persisted to
``eval_runs`` so you can compare numbers across phases.

Ground-truth format (per ``expected_*`` column)::

    [{"name": "Jane Doe", "job_title": "Superintendent",
      "email": "jdoe@district.org", "phone": "555-123-4567"}, ...]

Matching is name-based (normalized token overlap) scoped to the same
role cohort, so small title differences don't count as misses.
"""
from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.pipedrive import PipedriveClient
from app.pipeline_research import run_firecrawl_research
from app.role_coverage import _COHORTS as _COHORT_ROLE_IDS  # noqa: F401 — re-exported


_SALUTATION_RE = re.compile(
    r"^(dr\.?|mr\.?|mrs\.?|ms\.?|prof\.?|rev\.?)\s+",
    re.IGNORECASE,
)


def _norm_name(name: str | None) -> str:
    if not name:
        return ""
    s = _SALUTATION_RE.sub("", str(name).strip())
    s = re.sub(r",\s*(Ph\.?D\.?|Ed\.?D\.?|Jr\.?|Sr\.?|III|II|IV|M\.?D\.?|Esq\.?).*$", "", s, flags=re.I)
    return s.lower().strip()


def _name_tokens(name: str | None) -> set[str]:
    return {t for t in _norm_name(name).split() if len(t) > 2}


def _names_match(a: str | None, b: str | None) -> bool:
    a_norm, b_norm = _norm_name(a), _norm_name(b)
    if not a_norm or not b_norm:
        return False
    if a_norm == b_norm:
        return True
    return len(_name_tokens(a_norm) & _name_tokens(b_norm)) >= 2


def _norm_email(email: str | None) -> str:
    return (email or "").strip().lower()


def _norm_phone(phone: str | None) -> str:
    digits = re.sub(r"\D", "", str(phone or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def _cohort_for_expected(cohort_key: str) -> str:
    return cohort_key


_COHORT_TO_COLUMN = {
    "superintendent": "expected_superintendent",
    "curriculum": "expected_curriculum",
    "cte": "expected_cte",
}


def _cohort_for_extracted_contact(contact: dict) -> str | None:
    try:
        rid = int(contact.get("role_category_id"))
    except (TypeError, ValueError):
        return None
    for cohort, ids in _COHORT_ROLE_IDS.items():
        if rid in ids:
            return cohort
    return None


@dataclass
class ContactScore:
    """Per-contact match result against ground truth."""

    expected: dict[str, Any]
    matched: dict[str, Any] | None = None
    name_match: bool = False
    email_match: bool = False
    phone_match: bool = False

    def as_dict(self) -> dict:
        return {
            "expected": self.expected,
            "matched": self.matched,
            "name_match": self.name_match,
            "email_match": self.email_match,
            "phone_match": self.phone_match,
        }


@dataclass
class DistrictEvalResult:
    district_id: str
    district_name: str
    website_url: str | None
    cohort_scores: dict[str, list[ContactScore]] = field(default_factory=dict)
    extra_contacts: list[dict[str, Any]] = field(default_factory=list)
    precision: float = 0.0
    recall: float = 0.0
    name_accuracy: float = 0.0
    email_accuracy: float = 0.0
    phone_accuracy: float = 0.0
    error: str | None = None
    pipeline_usage: dict[str, Any] = field(default_factory=dict)
    firecrawl_usage: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "district_id": self.district_id,
            "district_name": self.district_name,
            "website_url": self.website_url,
            "precision": round(self.precision, 3),
            "recall": round(self.recall, 3),
            "name_accuracy": round(self.name_accuracy, 3),
            "email_accuracy": round(self.email_accuracy, 3),
            "phone_accuracy": round(self.phone_accuracy, 3),
            "cohort_scores": {
                cohort: [s.as_dict() for s in scores]
                for cohort, scores in self.cohort_scores.items()
            },
            "extra_contacts": self.extra_contacts,
            "error": self.error,
            "pipeline_usage": self.pipeline_usage,
            "firecrawl_usage": self.firecrawl_usage,
        }


def _compare_contact(expected: dict, extracted: dict | None) -> ContactScore:
    score = ContactScore(expected=expected, matched=extracted)
    if not extracted:
        return score
    score.name_match = _names_match(expected.get("name"), extracted.get("name"))
    score.email_match = (
        _norm_email(expected.get("email")) != ""
        and _norm_email(expected.get("email")) == _norm_email(extracted.get("email"))
    )
    exp_p = _norm_phone(expected.get("phone"))
    ext_p = _norm_phone(extracted.get("phone"))
    score.phone_match = bool(exp_p) and exp_p == ext_p
    return score


def _best_match_for_expected(
    expected: dict,
    candidates: list[dict],
    used: set[int],
) -> int | None:
    """Pick the first unused candidate whose name matches the expected name."""
    for i, c in enumerate(candidates):
        if i in used:
            continue
        if _names_match(expected.get("name"), c.get("name")):
            return i
    return None


def _score_one_district(
    expected_by_cohort: dict[str, list[dict]],
    extracted: list[dict],
) -> tuple[dict[str, list[ContactScore]], list[dict], dict[str, float]]:
    """Return (cohort_scores, extra_contacts, rollup_metrics)."""
    extracted_by_cohort: dict[str, list[dict]] = {"superintendent": [], "curriculum": [], "cte": [], "_other": []}
    for c in extracted:
        cohort = _cohort_for_extracted_contact(c) or "_other"
        extracted_by_cohort.setdefault(cohort, []).append(c)

    cohort_scores: dict[str, list[ContactScore]] = {}
    total_expected = 0
    total_matched = 0
    name_hits = email_hits = phone_hits = 0
    email_total = phone_total = 0
    matched_indices: dict[str, set[int]] = {k: set() for k in extracted_by_cohort}

    for cohort, expected_list in expected_by_cohort.items():
        scores: list[ContactScore] = []
        candidates = extracted_by_cohort.get(cohort, [])
        for expected in expected_list:
            total_expected += 1
            idx = _best_match_for_expected(expected, candidates, matched_indices[cohort])
            matched_contact = None
            if idx is not None:
                matched_indices[cohort].add(idx)
                matched_contact = candidates[idx]
                total_matched += 1
            s = _compare_contact(expected, matched_contact)
            scores.append(s)
            if s.name_match:
                name_hits += 1
            if expected.get("email"):
                email_total += 1
                if s.email_match:
                    email_hits += 1
            if expected.get("phone"):
                phone_total += 1
                if s.phone_match:
                    phone_hits += 1
        cohort_scores[cohort] = scores

    # "Extras" = extracted in-cohort contacts that didn't match any expected row,
    # plus any role_category_id that doesn't belong to a tracked cohort.
    extras: list[dict] = []
    for cohort, candidates in extracted_by_cohort.items():
        if cohort == "_other":
            extras.extend(candidates)
            continue
        for i, c in enumerate(candidates):
            if i not in matched_indices[cohort]:
                extras.append(c)

    total_extracted_in_cohort = sum(
        len(v) for k, v in extracted_by_cohort.items() if k != "_other"
    )

    precision = (total_matched / total_extracted_in_cohort) if total_extracted_in_cohort else 0.0
    recall = (total_matched / total_expected) if total_expected else 0.0
    name_accuracy = (name_hits / total_expected) if total_expected else 0.0
    email_accuracy = (email_hits / email_total) if email_total else 0.0
    phone_accuracy = (phone_hits / phone_total) if phone_total else 0.0

    return cohort_scores, extras, {
        "precision": precision,
        "recall": recall,
        "name_accuracy": name_accuracy,
        "email_accuracy": email_accuracy,
        "phone_accuracy": phone_accuracy,
    }


def _supabase_client():
    from app.database import _get_client
    return _get_client()


async def _evaluate_one(
    district: dict,
    research_mode: str,
) -> DistrictEvalResult:
    district_id = district["id"]
    district_name = district.get("name") or ""
    website_url = district.get("website_url")
    pipedrive_org_id = district.get("pipedrive_org_id")
    district_state = district.get("state")

    expected_by_cohort: dict[str, list[dict]] = {}
    for cohort, column in _COHORT_TO_COLUMN.items():
        expected_by_cohort[cohort] = [
            x for x in (district.get(column) or []) if isinstance(x, dict) and x.get("name")
        ]

    if not website_url:
        return DistrictEvalResult(
            district_id=district_id,
            district_name=district_name,
            website_url=None,
            error="No website_url on district record",
        )

    existing_contacts: list[dict] = []
    all_person_names: dict[str, int] = {}
    if pipedrive_org_id:
        try:
            pipedrive = PipedriveClient()
            raw = await pipedrive.get_org_persons(int(pipedrive_org_id))
            existing_contacts = pipedrive.format_persons_for_prompt(raw)
            all_person_names = pipedrive.get_all_person_names(raw)
        except Exception as e:
            print(f"[eval] Pipedrive fetch failed for {district_name}: {e}")

    try:
        result = await run_firecrawl_research(
            org_name=district_name,
            website_url=website_url,
            existing_contacts=existing_contacts,
            all_person_names=all_person_names,
            district_state=district_state,
            district_id=str(district_id),
            research_mode=research_mode,
        )
    except TypeError:
        # research_mode kwarg lands in Phase 3; fall back to default signature.
        result = await run_firecrawl_research(
            org_name=district_name,
            website_url=website_url,
            existing_contacts=existing_contacts,
            all_person_names=all_person_names,
            district_state=district_state,
        )

    if isinstance(result, dict) and result.get("error"):
        return DistrictEvalResult(
            district_id=district_id,
            district_name=district_name,
            website_url=website_url,
            error=str(result["error"]),
        )

    buckets = (result or {}).get("contacts") or {}
    extracted: list[dict] = []
    for key in ("confirmed", "updated", "new"):
        for c in buckets.get(key) or []:
            extracted.append({
                "name": c.get("name"),
                "job_title": c.get("job_title"),
                "role_category_id": c.get("role_category_id"),
                "email": c.get("email"),
                "phone": c.get("phone"),
                "source_url": c.get("source_url"),
            })

    cohort_scores, extras, rollup = _score_one_district(expected_by_cohort, extracted)

    return DistrictEvalResult(
        district_id=district_id,
        district_name=district_name,
        website_url=website_url,
        cohort_scores=cohort_scores,
        extra_contacts=extras,
        precision=rollup["precision"],
        recall=rollup["recall"],
        name_accuracy=rollup["name_accuracy"],
        email_accuracy=rollup["email_accuracy"],
        phone_accuracy=rollup["phone_accuracy"],
        pipeline_usage=result.get("usage") or {},
        firecrawl_usage=result.get("firecrawl_usage") or {},
    )


def _load_eval_districts(client, limit: int | None) -> list[dict]:
    cols = (
        "district_id, expected_superintendent, expected_curriculum, expected_cte, "
        "districts!inner(id, name, website_url, pipedrive_org_id, state)"
    )
    q = client.table("eval_districts").select(cols).limit(limit or 500)
    result = q.execute()
    rows: list[dict] = []
    for row in result.data or []:
        district = row.get("districts") or {}
        rows.append({
            "id": district.get("id") or row.get("district_id"),
            "name": district.get("name"),
            "website_url": district.get("website_url"),
            "pipedrive_org_id": district.get("pipedrive_org_id"),
            "state": district.get("state"),
            "expected_superintendent": row.get("expected_superintendent"),
            "expected_curriculum": row.get("expected_curriculum"),
            "expected_cte": row.get("expected_cte"),
        })
    return rows


def _persist_run(
    client,
    run_label: str | None,
    research_mode: str,
    results: list[DistrictEvalResult],
    started_at: datetime,
    completed_at: datetime,
    summary: dict[str, Any],
) -> str:
    row = {
        "id": str(uuid.uuid4()),
        "run_label": run_label,
        "research_mode": research_mode,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "district_count": len(results),
        "overall_precision": summary.get("precision"),
        "overall_recall": summary.get("recall"),
        "per_district": [r.as_dict() for r in results],
        "summary": summary,
    }
    client.table("eval_runs").insert(row).execute()
    return row["id"]


async def run_eval(
    *,
    run_label: str | None = None,
    research_mode: str = "pipeline",
    limit: int | None = None,
    concurrency: int | None = None,
) -> dict[str, Any]:
    """
    Run the eval harness. Pulls ground-truth rows from ``eval_districts``,
    re-runs the research pipeline, and stores a scored summary in
    ``eval_runs``. Returns the in-memory summary as well.
    """
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set to run the eval harness"
        )

    client = _supabase_client()
    rows = _load_eval_districts(client, limit)
    if not rows:
        return {
            "status": "empty",
            "message": "No rows found in eval_districts — seed the table first.",
        }

    conc = max(1, concurrency or settings.batch_concurrency)
    semaphore = asyncio.Semaphore(conc)
    started_at = datetime.now(timezone.utc)

    async def _run(d: dict) -> DistrictEvalResult:
        async with semaphore:
            try:
                return await _evaluate_one(d, research_mode=research_mode)
            except Exception as e:
                return DistrictEvalResult(
                    district_id=d["id"],
                    district_name=d.get("name") or "",
                    website_url=d.get("website_url"),
                    error=f"{type(e).__name__}: {e}",
                )

    results: list[DistrictEvalResult] = await asyncio.gather(*(_run(d) for d in rows))
    completed_at = datetime.now(timezone.utc)

    ok_results = [r for r in results if not r.error]
    if ok_results:
        precision = sum(r.precision for r in ok_results) / len(ok_results)
        recall = sum(r.recall for r in ok_results) / len(ok_results)
        name_accuracy = sum(r.name_accuracy for r in ok_results) / len(ok_results)
        email_accuracy = sum(r.email_accuracy for r in ok_results) / len(ok_results)
        phone_accuracy = sum(r.phone_accuracy for r in ok_results) / len(ok_results)
    else:
        precision = recall = name_accuracy = email_accuracy = phone_accuracy = 0.0

    summary = {
        "districts_scored": len(ok_results),
        "districts_errored": len(results) - len(ok_results),
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "name_accuracy": round(name_accuracy, 3),
        "email_accuracy": round(email_accuracy, 3),
        "phone_accuracy": round(phone_accuracy, 3),
        "research_mode": research_mode,
    }

    run_id = _persist_run(
        client=client,
        run_label=run_label,
        research_mode=research_mode,
        results=results,
        started_at=started_at,
        completed_at=completed_at,
        summary=summary,
    )
    summary["run_id"] = run_id
    summary["duration_seconds"] = (completed_at - started_at).total_seconds()
    return summary


def format_eval_summary_for_slack(summary: dict[str, Any]) -> str:
    if summary.get("status") == "empty":
        return (
            ":warning: *Eval harness:* no rows in `eval_districts`. "
            "Seed the golden set before running again."
        )
    return (
        f":bar_chart: *Eval run complete* ({summary.get('research_mode')})\n"
        f"• Districts scored: {summary.get('districts_scored', 0)}"
        f" (errors: {summary.get('districts_errored', 0)})\n"
        f"• Precision: {summary.get('precision', 0):.2f}\n"
        f"• Recall: {summary.get('recall', 0):.2f}\n"
        f"• Name accuracy: {summary.get('name_accuracy', 0):.2f}\n"
        f"• Email accuracy: {summary.get('email_accuracy', 0):.2f}\n"
        f"• Phone accuracy: {summary.get('phone_accuracy', 0):.2f}\n"
        f"• Duration: {summary.get('duration_seconds', 0):.1f}s"
        + (f"\n• Run ID: `{summary['run_id']}`" if summary.get("run_id") else "")
    )
