from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any

from app.batch_agent import BatchExtractionAgent
from app.config import ROLE_CATEGORY_OPTIONS, get_settings
from app.firecrawl_scraper import scrape_district
from app.role_coverage import cohort_labels, score_role_coverage


def normalize_research_mode(mode: str | None) -> str:
    """
    Map ``Settings.contact_hunter_mode`` and ``districts.research_mode`` onto
    keys used inside :func:`run_firecrawl_research`.

    * ``off`` / ``pipeline`` → ``pipeline`` (fixed Firecrawl + batch LLM only)
    * ``gap_fill`` / ``hybrid`` → ``hybrid`` (pipeline first, then ContactHunter gap-fill)
    * ``full`` / ``full_agent`` → ``full_agent`` (ContactHunter carries the whole task)
    """
    if mode is None:
        return "pipeline"
    raw = str(mode).strip().lower()
    if not raw:
        return "pipeline"
    if raw in ("off", "pipeline"):
        return "pipeline"
    if raw in ("gap_fill", "hybrid"):
        return "hybrid"
    if raw in ("full", "full_agent"):
        return "full_agent"
    return "pipeline"


def _norm_name(name: str | None) -> str:
    return (name or "").strip().lower()


def _token_set(name: str | None) -> set[str]:
    return {t for t in _norm_name(name).replace(",", "").split() if len(t) > 2}


def _names_similar(a: str | None, b: str | None) -> bool:
    a_norm = _norm_name(a)
    b_norm = _norm_name(b)
    if not a_norm or not b_norm:
        return False
    if a_norm == b_norm:
        return True
    return len(_token_set(a_norm) & _token_set(b_norm)) >= 2


def _role_label(role_id: int | None, fallback: str | None = None) -> str:
    if role_id in ROLE_CATEGORY_OPTIONS:
        return ROLE_CATEGORY_OPTIONS[role_id]
    return fallback or "Other"


def reconcile_extracted_contacts(
    *,
    extracted_contacts: list[dict[str, Any]],
    existing_contacts: list[dict[str, Any]],
    all_person_names: dict[str, int] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Reconcile raw extracted contacts against existing Pipedrive contacts.

    Returns the webhook-compatible buckets:
    confirmed, updated, new, missing.
    """
    contacts_out: dict[str, list[dict[str, Any]]] = {
        "confirmed": [],
        "updated": [],
        "new": [],
        "missing": [],
    }
    all_names = all_person_names or {}

    existing_by_id: dict[int, dict[str, Any]] = {
        int(c["person_id"]): c for c in existing_contacts if c.get("person_id")
    }
    existing_name_to_id: dict[str, int] = {}
    for pid, c in existing_by_id.items():
        nm = _norm_name(c.get("name"))
        if nm:
            existing_name_to_id[nm] = pid

    matched_existing_ids: set[int] = set()

    for c in extracted_contacts:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        norm_name = _norm_name(name)

        matched_id = existing_name_to_id.get(norm_name)
        if not matched_id:
            for pid, ex in existing_by_id.items():
                if pid in matched_existing_ids:
                    continue
                if _names_similar(name, ex.get("name")):
                    matched_id = pid
                    break

        role_category_id = c.get("role_category_id")
        if role_category_id is not None:
            try:
                role_category_id = int(role_category_id)
            except (TypeError, ValueError):
                role_category_id = None

        if matched_id and matched_id in existing_by_id:
            existing = existing_by_id[matched_id]
            matched_existing_ids.add(matched_id)

            existing_title = (existing.get("job_title") or "").strip()
            existing_email = (existing.get("email") or "").strip().lower()
            existing_phone = (existing.get("phone") or "").strip()
            existing_role_id = existing.get("role_category_id")
            try:
                existing_role_id = int(existing_role_id) if existing_role_id is not None else None
            except (TypeError, ValueError):
                existing_role_id = None

            incoming_title = (c.get("job_title") or "").strip()
            incoming_email = (c.get("email") or "").strip().lower()
            incoming_phone = (c.get("phone") or "").strip()

            changes: list[str] = []
            if incoming_title and incoming_title != existing_title:
                changes.append("title")
            if incoming_email and incoming_email != existing_email:
                changes.append("email")
            if incoming_phone and incoming_phone != existing_phone:
                changes.append("phone")
            if role_category_id and role_category_id != existing_role_id:
                changes.append("role_category")

            base_payload = {
                "name": name,
                "job_title": incoming_title or existing_title,
                "role_category_id": role_category_id or existing_role_id,
                "role_category_label": _role_label(role_category_id or existing_role_id, c.get("role_category")),
                "email": incoming_email or existing_email or None,
                "email_confidence": c.get("email_confidence") or "low",
                "phone": incoming_phone or existing_phone or None,
                "source_url": c.get("source_url"),
                "pipedrive_person_id": matched_id,
                "notes": c.get("notes") or "",
            }
            if changes:
                contacts_out["updated"].append(
                    {
                        **base_payload,
                        "previous_title": existing_title,
                        "previous_email": existing_email or None,
                        "previous_phone": existing_phone or None,
                        "previous_role_category_id": existing_role_id,
                        "previous_role_category_label": _role_label(
                            existing_role_id,
                            existing.get("role_category_label"),
                        ),
                        "changes": changes,
                    }
                )
            else:
                contacts_out["confirmed"].append(base_payload)
            continue

        # Safety net: do not suggest net-new contacts that already exist in org.
        if norm_name and norm_name in all_names:
            continue

        contacts_out["new"].append(
            {
                "name": name,
                "job_title": c.get("job_title") or "",
                "role_category_id": role_category_id,
                "role_category_label": _role_label(role_category_id, c.get("role_category")),
                "email": c.get("email"),
                "email_confidence": c.get("email_confidence") or "low",
                "phone": c.get("phone"),
                "source_url": c.get("source_url"),
                "notes": c.get("notes") or "",
            }
        )

    for pid, ex in existing_by_id.items():
        if pid in matched_existing_ids:
            continue
        contacts_out["missing"].append(
            {
                "name": ex.get("name") or "",
                "previous_title": ex.get("job_title") or "",
                "pipedrive_person_id": pid,
                "notes": "Not found on current district website pages reviewed in this run.",
            }
        )

    return contacts_out


@contextmanager
def _pipedrive_max_lead_overrides() -> Any:
    """
    Temporarily apply high-yield scrape/extract caps for Pipedrive-triggered runs.
    """
    settings = get_settings()
    original = {
        "batch_map_candidate_limit": settings.batch_map_candidate_limit,
        "batch_triage_max_candidates": settings.batch_triage_max_candidates,
        "batch_max_scrape_urls": settings.batch_max_scrape_urls,
        "batch_enrichment_url_cap": settings.batch_enrichment_url_cap,
        "batch_chars_per_page": settings.batch_chars_per_page,
    }
    settings.batch_map_candidate_limit = settings.pipedrive_map_candidate_limit
    settings.batch_triage_max_candidates = settings.pipedrive_triage_max_candidates
    settings.batch_max_scrape_urls = settings.pipedrive_max_scrape_urls
    settings.batch_enrichment_url_cap = settings.pipedrive_enrichment_url_cap
    settings.batch_chars_per_page = settings.pipedrive_chars_per_page
    try:
        yield
    finally:
        for k, v in original.items():
            setattr(settings, k, v)


async def _run_contact_hunter_gap_fill(
    *,
    org_name: str,
    website_url: str,
    district_state: str | None,
    district_id: str | None,
    extracted: list[dict[str, Any]],
    firecrawl_usage: dict,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Hybrid mode: after the pipeline extraction, check which target role
    cohorts are still missing and let :class:`ContactHunter` take another
    pass at filling them. Returns (hunter_contacts, hunter_meta).
    """
    # Import lazily so the pipeline module doesn't pull anthropic on cold paths.
    from app.contact_hunter import Budget, ContactHunter, HuntGoal
    from app.database import persist_hunter_trace
    from app.hunter_tools import build_default_tool_impls

    coverage = score_role_coverage(extracted)
    if not coverage.has_gaps:
        return [], {"skipped": True, "reason": "no gaps"}

    labels = cohort_labels()
    missing_labels = [labels.get(m, m) for m in sorted(coverage.missing)]
    print(
        f"[pipeline_research] coverage gap for {org_name}: missing {missing_labels}; "
        "running ContactHunter gap-fill"
    )

    settings = get_settings()
    goal = HuntGoal(
        district_name=org_name,
        district_state=district_state,
        base_url=website_url,
        missing_roles=missing_labels,
        known_contacts=extracted,
    )
    budget = Budget(
        max_tool_calls=int(settings.hunter_max_tool_calls_gap_fill),
        max_output_tokens=int(settings.hunter_max_output_tokens),
        max_seconds=float(settings.hunter_max_seconds),
    )
    tools = build_default_tool_impls(website_url, usage=firecrawl_usage)

    hunter = ContactHunter(goal=goal, tool_impls=tools, budget=budget)
    result = await hunter.run()

    hunt_id = str(uuid.uuid4())
    persist_hunter_trace(
        hunt_id=hunt_id,
        trace=result.trace,
        district_id=district_id,
        district_name=org_name,
    )

    print(
        f"[pipeline_research] ContactHunter {hunt_id} finished: "
        f"contacts={len(result.contacts)}, tool_calls={result.tool_calls}, "
        f"stop={result.stop_reason}, elapsed={result.elapsed_seconds:.1f}s"
    )

    meta = {
        "hunt_id": hunt_id,
        "stop_reason": result.stop_reason,
        "tool_calls": result.tool_calls,
        "elapsed_seconds": round(result.elapsed_seconds, 2),
        "token_usage": result.token_usage,
        "missing_before": sorted(coverage.missing),
    }
    return result.contacts, meta


async def run_firecrawl_research(
    *,
    org_name: str,
    website_url: str,
    existing_contacts: list[dict[str, Any]],
    all_person_names: dict[str, int] | None,
    district_state: str | None = None,
    district_id: str | None = None,
    research_mode: str | None = None,
) -> dict[str, Any]:
    """
    Shared Pipedrive research pipeline using Firecrawl + batch extraction.

    ``research_mode`` controls strategy:
      * ``pipeline`` (default) — fixed pipeline only.
      * ``hybrid`` — run pipeline, then ContactHunter to fill coverage gaps.
      * ``full_agent`` — skip the pipeline and hand the task to ContactHunter.
    """
    settings = get_settings()
    if research_mode is not None and str(research_mode).strip():
        mode = normalize_research_mode(str(research_mode))
    else:
        mode = normalize_research_mode(settings.contact_hunter_mode)

    hunter_meta: dict[str, Any] | None = None

    if mode == "full_agent":
        return await _run_full_agent_research(
            org_name=org_name,
            website_url=website_url,
            existing_contacts=existing_contacts,
            all_person_names=all_person_names,
            district_state=district_state,
            district_id=district_id,
        )

    extractor = BatchExtractionAgent()
    with _pipedrive_max_lead_overrides():
        pages, firecrawl_usage, url_triage = await scrape_district(
            website_url,
            district_name=org_name,
            district_state=district_state,
            batch_agent=extractor,
        )
        extracted, email_pattern, usage = extractor.extract_contacts_raw(
            pages=pages,
            org_name=org_name,
            website_url=website_url,
        )
    if not pages:
        return {"error": f"No readable pages found on {website_url}"}

    if mode == "hybrid":
        hunter_contacts, hunter_meta = await _run_contact_hunter_gap_fill(
            org_name=org_name,
            website_url=website_url,
            district_state=district_state,
            district_id=district_id,
            extracted=extracted,
            firecrawl_usage=firecrawl_usage,
        )
        if hunter_contacts:
            extracted = extracted + _normalize_hunter_contacts(hunter_contacts)

    reconciled = reconcile_extracted_contacts(
        extracted_contacts=extracted,
        existing_contacts=existing_contacts,
        all_person_names=all_person_names,
    )

    return {
        "district_name": org_name,
        "website": website_url,
        "research_mode": mode,
        "email_pattern": email_pattern or {},
        "contacts": reconciled,
        "research_notes": (
            f"Firecrawl pages: {len(pages)}. "
            + (
                f"URL triage: {url_triage.get('rationale', '')}"
                if isinstance(url_triage, dict)
                else ""
            )
        ).strip(),
        "usage": usage or {},
        "firecrawl_usage": firecrawl_usage or {},
        "url_triage": url_triage or {},
        "hunter": hunter_meta or {},
    }


def _normalize_hunter_contacts(
    hunter_contacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Coerce ContactHunter output into the dict shape the reconciler expects.

    Hunter produces ``role_category`` string labels; map them back to the
    Pipedrive role id when we can.
    """
    label_to_id = {label.strip().lower(): rid for rid, label in ROLE_CATEGORY_OPTIONS.items()}
    out: list[dict[str, Any]] = []
    for c in hunter_contacts:
        label = (c.get("role_category") or "").strip()
        rid = label_to_id.get(label.lower())
        out.append(
            {
                "name": c.get("name") or "",
                "job_title": c.get("title") or "",
                "email": c.get("email"),
                "email_confidence": "medium" if c.get("email") else "low",
                "phone": c.get("phone"),
                "role_category": label or None,
                "role_category_id": rid,
                "source_url": c.get("source_url"),
                "notes": (c.get("evidence") or "")[:500],
            }
        )
    return out


async def _run_full_agent_research(
    *,
    org_name: str,
    website_url: str,
    existing_contacts: list[dict[str, Any]],
    all_person_names: dict[str, int] | None,
    district_state: str | None,
    district_id: str | None,
) -> dict[str, Any]:
    """
    Replace the pipeline entirely with a ContactHunter run. Intended for
    districts that consistently lose the pipeline's heuristic game.
    """
    from app.contact_hunter import Budget, ContactHunter, HuntGoal
    from app.database import persist_hunter_trace
    from app.firecrawl_scraper import new_firecrawl_usage
    from app.hunter_tools import build_default_tool_impls

    settings = get_settings()
    labels = cohort_labels()
    all_missing = list(labels.values())

    firecrawl_usage = new_firecrawl_usage()
    goal = HuntGoal(
        district_name=org_name,
        district_state=district_state,
        base_url=website_url,
        missing_roles=all_missing,
        known_contacts=[],
    )
    budget = Budget(
        max_tool_calls=int(settings.hunter_max_tool_calls_full),
        max_output_tokens=int(settings.hunter_max_output_tokens),
        max_seconds=float(settings.hunter_max_seconds),
    )
    tools = build_default_tool_impls(website_url, usage=firecrawl_usage)

    hunter = ContactHunter(goal=goal, tool_impls=tools, budget=budget)
    result = await hunter.run()

    hunt_id = str(uuid.uuid4())
    persist_hunter_trace(
        hunt_id=hunt_id,
        trace=result.trace,
        district_id=district_id,
        district_name=org_name,
    )

    extracted = _normalize_hunter_contacts(result.contacts)
    reconciled = reconcile_extracted_contacts(
        extracted_contacts=extracted,
        existing_contacts=existing_contacts,
        all_person_names=all_person_names,
    )

    return {
        "district_name": org_name,
        "website": website_url,
        "research_mode": "full_agent",
        "email_pattern": {},
        "contacts": reconciled,
        "research_notes": (
            f"Full-agent run: {result.tool_calls} tool calls, "
            f"stop={result.stop_reason}, {len(extracted)} contacts found."
        ),
        "usage": {},
        "firecrawl_usage": firecrawl_usage,
        "url_triage": {
            "used_heuristic": False,
            "used_full_agent": True,
            "rationale": "full_agent mode — pipeline bypassed",
        },
        "hunter": {
            "hunt_id": hunt_id,
            "stop_reason": result.stop_reason,
            "tool_calls": result.tool_calls,
            "elapsed_seconds": round(result.elapsed_seconds, 2),
            "token_usage": result.token_usage,
        },
    }
