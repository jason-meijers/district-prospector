from __future__ import annotations
import re
import httpx
from supabase import create_client
from app.config import get_settings, PIPEDRIVE_WEBSITE_FIELD_KEY, PIPEDRIVE_DISTRICT_NAME_FIELD_KEY
from app.database import _normalize_name

# Only sync persons in roles we care about — keeps the snapshot table
# focused and prevents noise from irrelevant contacts.
_TARGET_ROLE_IDS = {
    482,  # Superintendent
    468,  # Assistant Superintendent
    472,  # Curriculum Director
    623,  # Curriculum Coordinator
    471,  # CTE Director
    467,  # Assistant CTE Director
    470,  # CTE Coordinator
    480,  # Principal
    474,  # Director
    478,  # Other
}

PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY = "7f22b8624616bef2d0adce26b28f8b3055dcbaae"


async def sync_pipedrive_contacts() -> dict:
    """
    Nightly sync: fetch all active Pipedrive persons in target roles and
    upsert them into the pipedrive_contacts_snapshot table.

    Clears the snapshot first and rebuilds from scratch each run —
    this is simpler and safer than tracking deletes/merges incrementally
    for a table that is read-only by the pipeline.

    Returns a summary dict with counts.
    """
    settings = get_settings()

    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError("Supabase credentials not configured")

    db = create_client(settings.supabase_url, settings.supabase_service_key)
    base_url = f"https://{settings.pipedrive_domain}/api/v1"
    token = settings.pipedrive_api_token

    print("[sync] Starting Pipedrive contacts sync...")
    persons = await _fetch_all_persons(base_url, token)
    print(f"[sync] Fetched {len(persons)} total persons from Pipedrive")

    # Filter to target roles only
    target_persons = []
    for p in persons:
        if not p.get("active_flag", True):
            continue
        if p.get("deleted") or p.get("archived") or p.get("merge_into_id"):
            continue

        role_id = _extract_role_id(p)
        if role_id not in _TARGET_ROLE_IDS:
            continue

        target_persons.append(p)

    print(f"[sync] {len(target_persons)} persons match target roles")

    # Clear old snapshot and rebuild
    db.table("pipedrive_contacts_snapshot").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

    if not target_persons:
        return {"total_fetched": len(persons), "synced": 0}

    records = []
    for p in target_persons:
        org = p.get("org_id")
        org_id = org.get("value") if isinstance(org, dict) else org

        email = None
        for e in (p.get("email") or []):
            if e.get("primary") or not email:
                email = e.get("value") or None

        name = (p.get("name") or "").strip()

        records.append({
            "pipedrive_person_id": p["id"],
            "pipedrive_org_id": org_id,
            "name": name,
            "name_normalized": _normalize_name(name),
            "email": email,
            "job_title": p.get("job_title") or None,
            "role_category_id": _extract_role_id(p),
        })

    # Upsert in batches of 500
    batch_size = 500
    inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        db.table("pipedrive_contacts_snapshot").upsert(
            batch,
            on_conflict="pipedrive_person_id",
        ).execute()
        inserted += len(batch)
        print(f"[sync] Upserted {inserted}/{len(records)} records...")

    print(f"[sync] Sync complete — {inserted} records in snapshot")
    return {"total_fetched": len(persons), "synced": inserted}


async def _fetch_all_persons(base_url: str, token: str) -> list[dict]:
    """Paginate through all Pipedrive persons."""
    persons = []
    start = 0
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                f"{base_url}/persons",
                params={"api_token": token, "start": start, "limit": 500},
            )
            resp.raise_for_status()
            body = resp.json()
            items = body.get("data") or []
            persons.extend(items)

            pagination = body.get("additional_data", {}).get("pagination", {})
            if pagination.get("more_items_in_collection"):
                start = pagination.get("next_start", start + 500)
            else:
                break

    return persons


async def _fetch_all_organizations(base_url: str, token: str) -> list[dict]:
    """Paginate through all Pipedrive organizations."""
    orgs = []
    start = 0
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                f"{base_url}/organizations",
                params={"api_token": token, "start": start, "limit": 500},
            )
            resp.raise_for_status()
            body = resp.json()
            items = body.get("data") or []
            if items:
                orgs.extend(items)

            pagination = body.get("additional_data", {}).get("pagination", {})
            if pagination.get("more_items_in_collection"):
                start = pagination.get("next_start", start + 500)
            else:
                break

    return orgs


def _extract_org_website(org: dict) -> str | None:
    """
    Extract website URL from a Pipedrive org record using the same
    field priority as the existing PipedriveClient.get_org_website().
    """
    url = (
        org.get("website")
        or org.get("url")
        or org.get(PIPEDRIVE_WEBSITE_FIELD_KEY)
    )
    if url and not str(url).startswith("http"):
        url = f"https://{url}"
    return url or None


def _extract_org_state(org: dict) -> str | None:
    """
    Try to extract US state from Pipedrive org address fields.
    Pipedrive stores address as a structured object with admin_area_level_1
    containing the state name or abbreviation.
    """
    address = org.get("address") or {}
    if isinstance(address, dict):
        state = (
            address.get("admin_area_level_1")
            or address.get("state")
        )
        if state:
            return str(state).strip() or None
    return None


async def sync_districts_from_pipedrive(skip_existing: bool = True) -> dict:
    """
    Fetch all Pipedrive organizations and upsert them into the districts table.

    Args:
        skip_existing: if True, districts already in the table (matched by
                       pipedrive_org_id) are not overwritten. Set to False
                       to update website_url and name from Pipedrive.

    Returns a summary dict with counts.
    """
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError("Supabase credentials not configured")

    db = create_client(settings.supabase_url, settings.supabase_service_key)
    base_url = f"https://{settings.pipedrive_domain}/api/v1"
    token = settings.pipedrive_api_token

    print("[sync] Fetching all Pipedrive organizations...")
    orgs = await _fetch_all_organizations(base_url, token)
    print(f"[sync] Fetched {len(orgs)} organizations from Pipedrive")

    if not orgs:
        return {"fetched": 0, "inserted": 0, "skipped": 0}

    # Load existing pipedrive_org_ids to handle skip_existing
    existing_result = db.table("districts").select("pipedrive_org_id").execute()
    existing_org_ids = {r["pipedrive_org_id"] for r in (existing_result.data or []) if r.get("pipedrive_org_id")}

    to_insert = []
    to_update = []
    skipped = 0

    for org in orgs:
        if not org.get("id") or not org.get("name"):
            continue

        # Only include orgs where the district name custom field is non-empty.
        # This separates school district orgs from all other org types.
        district_name_field = (
            org.get(PIPEDRIVE_DISTRICT_NAME_FIELD_KEY)
            or (org.get("custom_fields") or {}).get(PIPEDRIVE_DISTRICT_NAME_FIELD_KEY)
        )
        if not district_name_field or not str(district_name_field).strip():
            continue

        org_id = org["id"]
        name = (org.get("name") or "").strip()
        website = _extract_org_website(org)
        state = _extract_org_state(org)

        if org_id in existing_org_ids:
            if skip_existing:
                skipped += 1
                continue
            to_update.append({
                "pipedrive_org_id": org_id,
                "name": name,
                "website_url": website,
                "state": state,
            })
        else:
            to_insert.append({
                "name": name,
                "website_url": website,
                "state": state,
                "pipedrive_org_id": org_id,
                "status": "pending",
            })

    inserted = 0
    if to_insert:
        batch_size = 500
        for i in range(0, len(to_insert), batch_size):
            batch = to_insert[i: i + batch_size]
            db.table("districts").insert(batch).execute()
            inserted += len(batch)
            print(f"[sync] Inserted {min(inserted, len(to_insert))}/{len(to_insert)} districts...")

    updated = 0
    if to_update:
        for row in to_update:
            db.table("districts").update({
                "name": row["name"],
                "website_url": row["website_url"],
                "state": row["state"],
            }).eq("pipedrive_org_id", row["pipedrive_org_id"]).execute()
            updated += 1

    print(f"[sync] Districts sync complete — {inserted} inserted, {updated} updated, {skipped} skipped")
    return {"fetched": len(orgs), "inserted": inserted, "updated": updated, "skipped": skipped}


def _extract_role_id(person: dict) -> int | None:
    """Extract the role_category_id from a Pipedrive person record."""
    custom_fields = person.get("custom_fields") or {}
    role_raw = (
        custom_fields.get(PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY)
        or person.get(PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY)
    )
    if isinstance(role_raw, dict):
        role_raw = role_raw.get("id")
    try:
        return int(role_raw) if role_raw is not None else None
    except (TypeError, ValueError):
        return None
