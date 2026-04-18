from __future__ import annotations
import re
import uuid
from datetime import datetime, timezone
from typing import Any
from supabase import create_client, Client
from app.config import get_settings

# Salutation prefixes to strip when normalising names for fuzzy matching.
_SALUTATION_RE = re.compile(
    r"^(dr\.?|mr\.?|mrs\.?|ms\.?|prof\.?|rev\.?)\s+",
    re.IGNORECASE,
)


def _normalize_name(name: str) -> str:
    """
    Lowercase and strip common salutations so 'Dr. Gregory Nehen' and
    'Gregory Nehen' match during fuzzy comparison.
    """
    name = name.strip()
    name = _SALUTATION_RE.sub("", name)
    return name.lower().strip()


def _get_client() -> Client:
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set to use the batch pipeline"
        )
    return create_client(settings.supabase_url, settings.supabase_service_key)


# ─────────────────────────────────────────────────────────────
# Districts
# ─────────────────────────────────────────────────────────────

def claim_next_district() -> dict | None:
    """
    Atomically claim the next district for processing.

    Prefers status='manual' (most recently updated first), then 'pending'
    (oldest created_at first). Uses FOR UPDATE SKIP LOCKED so parallel
    workers never claim the same row. Returns None if the queue is empty.
    """
    client = _get_client()
    result = client.rpc("claim_next_district").execute()
    if result.data:
        return result.data[0] if isinstance(result.data, list) else result.data
    return None


def get_pending_districts(limit: int = 50) -> list[dict]:
    """
    Return up to `limit` districts with status='pending', ordered by
    created_at (oldest first). Used when single-worker sequential processing
    is preferred over the atomic claim function.
    """
    client = _get_client()
    result = (
        client.table("districts")
        .select("*")
        .eq("status", "pending")
        .order("created_at")
        .limit(limit)
        .execute()
    )
    return result.data or []


def mark_district_processing(district_id: str) -> None:
    client = _get_client()
    client.table("districts").update({
        "status": "processing",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", district_id).execute()


def mark_district_done(district_id: str) -> None:
    client = _get_client()
    client.table("districts").update({
        "status": "done",
        "last_crawled_at": datetime.now(timezone.utc).isoformat(),
        "crawl_error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", district_id).execute()


def get_district_by_pipedrive_org_id(pipedrive_org_id: int) -> dict[str, Any] | None:
    """
    Lookup a districts row by Pipedrive organization id.

    Used by the Pipedrive webhook to pass ``district_id`` / ``research_mode``
    into :func:`run_firecrawl_research` so hybrid ContactHunter traces and
    per-row overrides apply. Returns None if Supabase is not configured or no
    row matches.
    """
    try:
        client = _get_client()
    except RuntimeError:
        return None
    try:
        result = (
            client.table("districts")
            .select("id, research_mode, state")
            .eq("pipedrive_org_id", pipedrive_org_id)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        return rows[0] if rows else None
    except Exception:
        return None


def mark_district_error(district_id: str, error_message: str) -> None:
    client = _get_client()
    client.table("districts").update({
        "status": "error",
        "crawl_error": error_message[:1000],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", district_id).execute()


def reset_district_to_pending(district_id: str) -> None:
    """Re-queue a district that was interrupted mid-processing."""
    client = _get_client()
    client.table("districts").update({
        "status": "pending",
        "crawl_error": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", district_id).execute()


def import_districts(rows: list[dict]) -> int:
    """
    Bulk insert districts. Each row should have at minimum: name.
    Optional: website_url, state, pipedrive_org_id.
    Returns the number of rows inserted.
    """
    client = _get_client()
    records = []
    for row in rows:
        records.append({
            "name": row["name"],
            "website_url": row.get("website_url"),
            "state": row.get("state"),
            "pipedrive_org_id": row.get("pipedrive_org_id"),
            "status": "pending",
        })
    if not records:
        return 0
    result = client.table("districts").insert(records).execute()
    return len(result.data or [])


# ─────────────────────────────────────────────────────────────
# Found contacts
# ─────────────────────────────────────────────────────────────

def save_found_contacts(
    district_id: str,
    pipedrive_org_id: int | None,
    contacts: list[dict],
) -> list[str]:
    """
    Insert extracted contacts into found_contacts.
    Returns a list of the inserted row IDs.
    """
    if not contacts:
        return []
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for c in contacts:
        records.append({
            "id": str(uuid.uuid4()),
            "district_id": district_id,
            "pipedrive_org_id": pipedrive_org_id,
            "name": c.get("name"),
            "job_title": c.get("job_title"),
            "role_category": c.get("role_category"),
            "role_category_id": c.get("role_category_id"),
            "email": c.get("email"),
            "email_confidence": c.get("email_confidence"),
            "phone": c.get("phone"),
            "source_url": c.get("source_url"),
            "crawled_at": now,
        })
    result = client.table("found_contacts").insert(records).execute()
    return [r["id"] for r in (result.data or [])]


# ─────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────

def run_dedup_job(since_hours: int = 25) -> int:
    """
    Compare recently crawled found_contacts against the Pipedrive snapshot
    and insert unmatched contacts into new_contacts for human review.

    Matching logic (scoped to same pipedrive_org_id):
    1. Exact email match → already in Pipedrive, skip.
    2. Fuzzy name match (rapidfuzz token_sort_ratio >= 85) → already in Pipedrive, skip.
    3. No match → insert into new_contacts with review_status='pending'.

    Returns the number of new rows inserted into new_contacts.
    """
    from rapidfuzz import fuzz

    client = _get_client()
    cutoff = datetime.now(timezone.utc)
    from datetime import timedelta
    since = (cutoff - timedelta(hours=since_hours)).isoformat()

    # Load recently crawled found_contacts that haven't been deduped yet
    fc_result = (
        client.table("found_contacts")
        .select("id, district_id, pipedrive_org_id, name, email")
        .gte("crawled_at", since)
        .execute()
    )
    found = fc_result.data or []
    if not found:
        print("[dedup] No recent found_contacts to process")
        return 0

    # Load already-processed found_contact IDs to avoid re-inserting
    existing_nc_result = (
        client.table("new_contacts")
        .select("found_contact_id")
        .execute()
    )
    already_queued = {r["found_contact_id"] for r in (existing_nc_result.data or [])}

    # Group snapshot by org_id for efficient lookup
    unique_org_ids = {c["pipedrive_org_id"] for c in found if c.get("pipedrive_org_id")}
    snapshot: dict[int, list[dict]] = {}
    for org_id in unique_org_ids:
        s_result = (
            client.table("pipedrive_contacts_snapshot")
            .select("pipedrive_person_id, name_normalized, email")
            .eq("pipedrive_org_id", org_id)
            .execute()
        )
        snapshot[org_id] = s_result.data or []

    new_rows = []
    for fc in found:
        fc_id = fc["id"]
        if fc_id in already_queued:
            continue

        org_id = fc.get("pipedrive_org_id")
        if not org_id:
            # No org mapping — can't dedup, queue for review
            new_rows.append({
                "found_contact_id": fc_id,
                "pipedrive_org_id": None,
                "review_status": "pending",
            })
            continue

        existing = snapshot.get(org_id, [])
        fc_email = (fc.get("email") or "").strip().lower()
        fc_name_norm = _normalize_name(fc.get("name") or "")

        matched = False

        # 1. Exact email match (only when email is present)
        if fc_email:
            for ex in existing:
                ex_email = (ex.get("email") or "").strip().lower()
                if ex_email and ex_email == fc_email:
                    matched = True
                    break

        # 2. Fuzzy name match (scoped to same org)
        if not matched and fc_name_norm:
            for ex in existing:
                ex_name = ex.get("name_normalized") or ""
                if not ex_name:
                    continue
                score = fuzz.token_sort_ratio(fc_name_norm, ex_name)
                if score >= 85:
                    matched = True
                    break

        if not matched:
            new_rows.append({
                "found_contact_id": fc_id,
                "pipedrive_org_id": org_id,
                "review_status": "pending",
            })

    if new_rows:
        client.table("new_contacts").insert(new_rows).execute()
        print(f"[dedup] Inserted {len(new_rows)} new contacts for review")
    else:
        print("[dedup] No new unique contacts found")

    return len(new_rows)


# ─────────────────────────────────────────────────────────────
# Google Sheets export
# ─────────────────────────────────────────────────────────────

def export_new_contacts_to_sheet(sheet_id: str | None = None) -> int:
    """
    Export all pending new_contacts (with full contact detail from found_contacts
    and district name) to a Google Sheet tab named with today's date.

    Requires:
    - GOOGLE_SHEET_ID env var (or pass sheet_id directly)
    - Google service account credentials configured for gspread
      (set GOOGLE_APPLICATION_CREDENTIALS env var pointing to the JSON key file,
       or place the key at ~/.config/gcloud/application_default_credentials.json)

    Returns the number of rows exported.
    """
    import gspread
    from google.oauth2.service_account import Credentials
    import os

    settings = get_settings()
    target_sheet_id = sheet_id or settings.google_sheet_id
    if not target_sheet_id:
        raise ValueError("No Google Sheet ID provided. Set GOOGLE_SHEET_ID env var.")

    client = _get_client()

    # Load pending new_contacts with joined found_contact and district data
    nc_result = (
        client.table("new_contacts")
        .select(
            "id, pipedrive_org_id, review_status, created_at, "
            "found_contacts(name, job_title, role_category, email, email_confidence, phone, source_url, "
            "districts(name, website_url, state))"
        )
        .eq("review_status", "pending")
        .order("created_at")
        .execute()
    )
    rows = nc_result.data or []
    if not rows:
        print("[sheets] No pending new_contacts to export")
        return 0

    # Build sheet rows
    headers = [
        "new_contact_id", "district_name", "state", "website_url",
        "pipedrive_org_id", "name", "job_title", "role_category",
        "email", "email_confidence", "phone", "source_url", "queued_at",
    ]
    sheet_rows: list[list[Any]] = [headers]
    for nc in rows:
        fc = nc.get("found_contacts") or {}
        district = fc.get("districts") or {}
        sheet_rows.append([
            nc.get("id", ""),
            district.get("name", ""),
            district.get("state", ""),
            district.get("website_url", ""),
            nc.get("pipedrive_org_id", ""),
            fc.get("name", ""),
            fc.get("job_title", ""),
            fc.get("role_category", ""),
            fc.get("email", ""),
            fc.get("email_confidence", ""),
            fc.get("phone", ""),
            fc.get("source_url", ""),
            nc.get("created_at", ""),
        ])

    # Connect to Google Sheets
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    else:
        # Fall back to gspread's default auth (ADC)
        import google.auth
        creds, _ = google.auth.default(scopes=scopes)

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(target_sheet_id)

    tab_name = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        worksheet = spreadsheet.worksheet(tab_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=len(sheet_rows) + 10, cols=len(headers))

    worksheet.update(sheet_rows, value_input_option="RAW")
    print(f"[sheets] Exported {len(sheet_rows) - 1} rows to sheet '{tab_name}'")
    return len(sheet_rows) - 1


# ─────────────────────────────────────────────────────────────
# Pending actions (Phase 4 Slack Block Kit)
# ─────────────────────────────────────────────────────────────


def create_pending_action(
    *,
    kind: str,
    payload: dict,
    pipedrive_org_id: int | None = None,
    pipedrive_person_id: int | None = None,
    note_payload: dict | None = None,
    slack_channel: str | None = None,
) -> str:
    """
    Insert a ``pending`` row in ``pending_actions`` and return its id. The
    caller later posts a Slack Block Kit message referencing this id; when
    the user taps the button, the ``/slack/interact`` endpoint claims and
    executes the action.
    """
    client = _get_client()
    row = {
        "kind": kind,
        "payload": payload or {},
        "pipedrive_org_id": pipedrive_org_id,
        "pipedrive_person_id": pipedrive_person_id,
        "note_payload": note_payload,
        "slack_channel": slack_channel,
        "status": "pending",
    }
    result = client.table("pending_actions").insert(row).execute()
    data = result.data or []
    if not data:
        raise RuntimeError("pending_actions insert returned no row")
    return str(data[0]["id"])


def attach_slack_message_to_action(
    *,
    action_id: str,
    slack_message_ts: str | None = None,
    slack_response_url: str | None = None,
) -> None:
    """Record the Slack message metadata after the Block Kit message posts."""
    client = _get_client()
    updates: dict[str, Any] = {}
    if slack_message_ts:
        updates["slack_message_ts"] = slack_message_ts
    if slack_response_url:
        updates["slack_response_url"] = slack_response_url
    if not updates:
        return
    client.table("pending_actions").update(updates).eq("id", action_id).execute()


def claim_pending_action(action_id: str, claimed_by: str | None = None) -> dict | None:
    """
    Atomically transition a pending action → ``executing``. Returns the full
    row if we won the race, or ``None`` if it's already been claimed /
    executed / cancelled (prevents duplicate API calls from double-clicks).
    """
    client = _get_client()
    update = {
        "status": "executing",
        "executed_by": claimed_by,
    }
    result = (
        client.table("pending_actions")
        .update(update)
        .eq("id", action_id)
        .eq("status", "pending")
        .execute()
    )
    data = result.data or []
    return data[0] if data else None


def mark_action_executed(action_id: str, result: dict | None = None) -> None:
    client = _get_client()
    client.table("pending_actions").update(
        {
            "status": "executed",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "result": result or {},
        }
    ).eq("id", action_id).execute()


def mark_action_failed(action_id: str, error: str) -> None:
    client = _get_client()
    client.table("pending_actions").update(
        {"status": "failed", "error": error[:2000]}
    ).eq("id", action_id).execute()


def mark_action_cancelled(action_id: str) -> None:
    client = _get_client()
    client.table("pending_actions").update({"status": "cancelled"}).eq(
        "id", action_id
    ).execute()


def get_pending_action(action_id: str) -> dict | None:
    client = _get_client()
    result = (
        client.table("pending_actions").select("*").eq("id", action_id).limit(1).execute()
    )
    data = result.data or []
    return data[0] if data else None


# ─────────────────────────────────────────────────────────────
# Hunter traces (Phase 3 observability)
# ─────────────────────────────────────────────────────────────

def persist_hunter_trace(
    *,
    hunt_id: str,
    trace: list[dict],
    district_id: str | None = None,
    district_name: str | None = None,
) -> int:
    """
    Bulk-insert a ContactHunter trace into ``hunter_traces``. Returns the
    number of rows written. Swallows exceptions (logging only) so trace
    failures never break a hunt.
    """
    if not trace:
        return 0
    client = _get_client()
    rows: list[dict] = []
    for i, entry in enumerate(trace, 1):
        if not isinstance(entry, dict):
            continue
        tool = (entry.get("tool") or entry.get("event") or "unknown")[:120]
        rows.append(
            {
                "hunt_id": hunt_id,
                "district_id": district_id,
                "district_name": district_name,
                "step": i,
                "tool": tool,
                "args": entry.get("input") or {},
                "result_summary": entry.get("result") or {},
                "duration_ms": entry.get("elapsed_ms"),
                "error": entry.get("error"),
            }
        )
    if not rows:
        return 0
    try:
        client.table("hunter_traces").insert(rows).execute()
        return len(rows)
    except Exception as e:
        print(f"[database] persist_hunter_trace failed: {type(e).__name__}: {e}")
        return 0


# ─────────────────────────────────────────────────────────────
# Batch status helpers
# ─────────────────────────────────────────────────────────────

def get_district_counts() -> dict[str, int]:
    """Return counts of districts by status."""
    client = _get_client()
    result = client.rpc("get_district_status_counts").execute()
    if result.data:
        return {r["status"]: r["count"] for r in result.data}
    # Fallback: query each status individually
    counts = {}
    for status in ("manual", "pending", "processing", "done", "error"):
        r = (
            client.table("districts")
            .select("id", count="exact")
            .eq("status", status)
            .execute()
        )
        counts[status] = r.count or 0
    return counts
