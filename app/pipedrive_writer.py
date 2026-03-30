from __future__ import annotations
import httpx
from datetime import datetime, timezone
from supabase import create_client
from app.config import get_settings, PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY, PIPEDRIVE_SALUTATION_FIELD_KEY, SALUTATION_OPTIONS
import re


def _strip_salutation(raw_name: str) -> tuple[str, int | None]:
    """
    Strip a recognised salutation prefix from a name and return
    (clean_name, salutation_option_id).
    """
    for prefix, opt_id in SALUTATION_OPTIONS.items():
        prefix_bare = prefix.rstrip(".")
        if (raw_name.lower().startswith(f"{prefix.lower()} ")
                or raw_name.lower().startswith(f"{prefix_bare.lower()} ")):
            clean = raw_name[len(prefix):].lstrip(" .").strip()
            return (clean or raw_name), opt_id
    return raw_name, None


async def write_approved_contacts(new_contact_ids: list[str]) -> dict:
    """
    For each new_contact ID:
    1. Load the full contact detail from found_contacts (via the FK).
    2. POST to Pipedrive v2 /persons to create the person.
    3. Mark the new_contacts row as approved with the Pipedrive person ID and timestamp.

    Returns {"created": int, "errors": int, "details": list[dict]}
    """
    settings = get_settings()
    db = create_client(settings.supabase_url, settings.supabase_service_key)
    pd_base = f"https://{settings.pipedrive_domain}/api/v2"
    pd_token = settings.pipedrive_api_token

    # Load new_contacts + joined found_contacts in one query
    result = (
        db.table("new_contacts")
        .select(
            "id, pipedrive_org_id, "
            "found_contacts(name, job_title, role_category_id, email, email_confidence, phone)"
        )
        .in_("id", new_contact_ids)
        .eq("review_status", "pending")
        .execute()
    )
    rows = result.data or []

    created = 0
    errors = 0
    details = []

    async with httpx.AsyncClient(timeout=15) as client:
        for row in rows:
            nc_id = row["id"]
            org_id = row.get("pipedrive_org_id")
            fc = row.get("found_contacts") or {}

            if not org_id:
                details.append({"id": nc_id, "status": "error", "reason": "No pipedrive_org_id"})
                errors += 1
                continue

            raw_name = (fc.get("name") or "").strip()
            if not raw_name:
                details.append({"id": nc_id, "status": "error", "reason": "Empty name"})
                errors += 1
                continue

            clean_name, salutation_id = _strip_salutation(raw_name)
            role_category_id = fc.get("role_category_id")

            custom_fields: dict = {}
            if role_category_id:
                custom_fields[PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY] = role_category_id
            if salutation_id:
                custom_fields[PIPEDRIVE_SALUTATION_FIELD_KEY] = salutation_id

            body: dict = {
                "name": clean_name,
                "org_id": org_id,
                "job_title": fc.get("job_title") or "",
                "custom_fields": custom_fields,
            }
            if fc.get("email"):
                body["emails"] = [{"value": fc["email"], "primary": True, "label": "work"}]
            if fc.get("phone"):
                body["phones"] = [{"value": fc["phone"], "primary": True, "label": "work"}]

            try:
                resp = await client.post(
                    f"{pd_base}/persons",
                    params={"api_token": pd_token},
                    json=body,
                )
                resp.raise_for_status()
                pd_person_id = resp.json().get("data", {}).get("id")

                # Mark as approved in Supabase
                db.table("new_contacts").update({
                    "review_status": "approved",
                    "reviewed_at": datetime.now(timezone.utc).isoformat(),
                    "pipedrive_created_at": datetime.now(timezone.utc).isoformat(),
                    "reviewer_note": f"Auto-created Pipedrive person ID {pd_person_id}",
                }).eq("id", nc_id).execute()

                created += 1
                details.append({"id": nc_id, "status": "created", "pipedrive_person_id": pd_person_id})
                print(f"[pipedrive_writer] Created person '{clean_name}' (PD ID {pd_person_id})")

            except httpx.HTTPStatusError as e:
                errors += 1
                reason = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                details.append({"id": nc_id, "status": "error", "reason": reason})
                print(f"[pipedrive_writer] Failed to create '{clean_name}': {reason}")

            except Exception as e:
                errors += 1
                reason = f"{type(e).__name__}: {str(e)[:200]}"
                details.append({"id": nc_id, "status": "error", "reason": reason})
                print(f"[pipedrive_writer] Error for '{clean_name}': {reason}")

    return {"created": created, "errors": errors, "details": details}
