"""
Slack interactivity endpoint helpers.

Slack posts block-action callbacks to a single URL (``/slack/interact``) as
``application/x-www-form-urlencoded`` with a ``payload`` field containing
JSON. We verify the signature, claim the matching ``pending_actions`` row,
execute the appropriate Pipedrive call, and update the original message via
``response_url`` so the button disappears and the outcome is visible inline.

Idempotency comes from the atomic ``claim_pending_action`` transition: a
double-click results in the second attempt simply no-ops with a friendly
note.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import httpx

from app.config import (
    PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY,
    PIPEDRIVE_SALUTATION_FIELD_KEY,
    ROLE_CATEGORY_BY_LABEL,
    SALUTATION_OPTIONS,
    get_settings,
)
from app.database import (
    claim_pending_action,
    get_pending_action,
    mark_action_cancelled,
    mark_action_executed,
    mark_action_failed,
)
from app.pipedrive import PipedriveClient

FORMER_ROLE_CATEGORY_ID = int(ROLE_CATEGORY_BY_LABEL["Former"])


def _slack_replace(text: str) -> dict[str, Any]:
    """Interactive response: replace the message and remove Block Kit buttons."""
    return {"replace_original": True, "text": text}


async def _post_v1_note(
    *,
    person_id: int,
    org_id: int | None,
    content: str,
) -> dict:
    settings = get_settings()
    pd_base = f"https://{settings.pipedrive_domain}/api/v1"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{pd_base}/notes",
            params={"api_token": settings.pipedrive_api_token},
            json={
                "content": content,
                "person_id": int(person_id),
                "org_id": int(org_id) if org_id else None,
            },
        )
        resp.raise_for_status()
        return resp.json().get("data") or {}


# ── Signature verification ────────────────────────────────────────


def verify_slack_signature(
    signing_secret: str,
    body: bytes,
    timestamp: str,
    signature: str,
    max_age_seconds: int = 300,
) -> bool:
    """
    Verify a Slack webhook signature per
    https://api.slack.com/authentication/verifying-requests-from-slack.

    Returns True if the signature matches and the timestamp is recent.
    """
    if not signing_secret or not timestamp or not signature:
        return False
    try:
        ts_int = int(timestamp)
    except ValueError:
        return False
    if abs(time.time() - ts_int) > max_age_seconds:
        return False

    basestring = f"v0:{timestamp}:".encode() + body
    digest = hmac.new(
        signing_secret.encode(), basestring, hashlib.sha256
    ).hexdigest()
    expected = f"v0={digest}"
    return hmac.compare_digest(expected, signature)


# ── Action execution ───────────────────────────────────────────────


def _strip_salutation(raw_name: str) -> tuple[str, int | None]:
    for prefix, opt_id in SALUTATION_OPTIONS.items():
        prefix_bare = prefix.rstrip(".")
        if raw_name.lower().startswith(f"{prefix.lower()} ") or raw_name.lower().startswith(
            f"{prefix_bare.lower()} "
        ):
            clean = raw_name[len(prefix):].lstrip(" .").strip()
            return (clean or raw_name), opt_id
    return raw_name, None


async def _execute_create_person(action: dict) -> dict:
    settings = get_settings()
    payload = action.get("payload") or {}
    org_id = action.get("pipedrive_org_id")
    if not org_id:
        raise ValueError("create_person missing pipedrive_org_id")

    raw_name = (payload.get("name") or "").strip()
    if not raw_name:
        raise ValueError("create_person missing name")
    clean_name, sal_id = _strip_salutation(raw_name)

    custom_fields: dict[str, Any] = {}
    if payload.get("role_category_id"):
        custom_fields[PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY] = int(payload["role_category_id"])
    if sal_id or payload.get("salutation"):
        custom_fields[PIPEDRIVE_SALUTATION_FIELD_KEY] = sal_id or payload.get("salutation")

    body: dict[str, Any] = {
        "name": clean_name,
        "org_id": int(org_id),
        "job_title": payload.get("job_title") or "",
        "custom_fields": custom_fields,
    }
    if payload.get("email"):
        body["emails"] = [{"value": payload["email"], "primary": True, "label": "work"}]
    if payload.get("phone"):
        body["phones"] = [{"value": payload["phone"], "primary": True, "label": "work"}]

    pd_base = f"https://{settings.pipedrive_domain}/api/v2"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{pd_base}/persons",
            params={"api_token": settings.pipedrive_api_token},
            json=body,
        )
        resp.raise_for_status()
        person = resp.json().get("data") or {}

    pid = person.get("id")
    note_meta: dict[str, Any] = {}
    note_row = action.get("note_payload") or {}
    note_content = (note_row.get("content") or "").strip()
    if note_content and pid:
        note = await _post_v1_note(
            person_id=int(pid),
            org_id=int(org_id) if org_id else None,
            content=note_content,
        )
        note_meta = {"note_id": note.get("id"), "note_added": True}

    out: dict[str, Any] = {
        "pipedrive_person_id": pid,
        "name": clean_name,
        **note_meta,
    }
    return out


async def _execute_update_person(action: dict) -> dict:
    settings = get_settings()
    payload = action.get("payload") or {}
    person_id = action.get("pipedrive_person_id")
    if not person_id:
        raise ValueError("update_person missing pipedrive_person_id")

    body: dict[str, Any] = {}
    custom_fields: dict[str, Any] = {}
    if payload.get("role_category_id"):
        custom_fields[PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY] = int(payload["role_category_id"])

    raw_name = (payload.get("name") or "").strip()
    if raw_name:
        clean_name, sal_id = _strip_salutation(raw_name)
        body["name"] = clean_name
        if sal_id:
            custom_fields[PIPEDRIVE_SALUTATION_FIELD_KEY] = sal_id

    if payload.get("job_title"):
        body["job_title"] = payload["job_title"]
    if custom_fields:
        body["custom_fields"] = custom_fields
    if payload.get("email"):
        body["emails"] = [{"value": payload["email"], "primary": True, "label": "work"}]
    if payload.get("phone"):
        body["phones"] = [{"value": payload["phone"], "primary": True, "label": "work"}]

    if not body:
        return {"pipedrive_person_id": person_id, "note": "nothing to update"}

    pd_base = f"https://{settings.pipedrive_domain}/api/v2"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.patch(
            f"{pd_base}/persons/{int(person_id)}",
            params={"api_token": settings.pipedrive_api_token},
            json=body,
        )
        resp.raise_for_status()

    return {"pipedrive_person_id": person_id, "fields_updated": sorted(body.keys())}


async def _execute_mark_former(action: dict) -> dict:
    """
    Set Role Category to *Former* in Pipedrive (v2 PATCH), then add an audit note (v1).
    """
    settings = get_settings()
    payload = action.get("payload") or {}
    person_id = action.get("pipedrive_person_id")
    org_id = action.get("pipedrive_org_id")
    if not person_id:
        raise ValueError("mark_former missing pipedrive_person_id")

    pd_v2 = f"https://{settings.pipedrive_domain}/api/v2"
    patch_body = {
        "custom_fields": {
            PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY: FORMER_ROLE_CATEGORY_ID,
        }
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.patch(
            f"{pd_v2}/persons/{int(person_id)}",
            params={"api_token": settings.pipedrive_api_token},
            json=patch_body,
        )
        resp.raise_for_status()

    content = (
        f"District Prospector flagged {payload.get('name') or 'this contact'} as "
        "possibly no longer in this role — not found on the district site "
        "during the most recent research run."
    )
    if payload.get("previous_title"):
        content += f" Previous title: {payload['previous_title']}."
    if payload.get("notes"):
        content += f" {payload['notes']}"

    note = await _post_v1_note(
        person_id=int(person_id),
        org_id=int(org_id) if org_id else None,
        content=content,
    )

    return {
        "pipedrive_person_id": person_id,
        "role_category_id": FORMER_ROLE_CATEGORY_ID,
        "note_id": note.get("id"),
    }


async def _execute_make_poc(action: dict) -> dict:
    """
    Set this person as the main contact on open deals where the current
    contact is tagged Former (deal ids stored in payload at post time).
    """
    payload = action.get("payload") or {}
    deal_ids = payload.get("deal_ids") or []
    org_id = action.get("pipedrive_org_id")
    person_id = action.get("pipedrive_person_id")
    hint_name = (payload.get("contact_name") or "").strip()

    if not org_id:
        raise ValueError("make_poc missing pipedrive_org_id")
    if not deal_ids:
        return {"pipedrive_person_id": person_id, "deals_updated": []}

    pd = PipedriveClient()
    if not person_id:
        if not hint_name:
            raise ValueError(
                "Approve “Create in Pipedrive” first, then click Make PoC — "
                "no person id yet."
            )
        resolved = await pd.find_org_person_id_by_name(int(org_id), hint_name)
        if not resolved:
            raise ValueError(
                f"Could not find a person named “{hint_name}” in this org. "
                "Create the contact or adjust the name in Pipedrive, then try again."
            )
        person_id = resolved

    pid = int(person_id)
    updated: list[int] = []
    for raw_id in deal_ids:
        await pd.update_deal_main_contact(int(raw_id), pid)
        updated.append(int(raw_id))

    return {"pipedrive_person_id": pid, "deals_updated": updated}


_EXECUTORS = {
    "create_person": _execute_create_person,
    "update_person": _execute_update_person,
    "mark_former": _execute_mark_former,
    "make_poc": _execute_make_poc,
}


# ── Main dispatcher ────────────────────────────────────────────────


async def handle_slack_interaction(payload: dict) -> dict:
    """
    Entry point for ``/slack/interact``. Returns a dict for the HTTP response
    and ``response_url`` (always ``replace_original`` so Block Kit buttons
    are removed).
    """
    actions = payload.get("actions") or []
    if not actions:
        return _slack_replace(":warning: Unrecognized interaction (no actions in payload).")

    user = payload.get("user") or {}
    user_label = user.get("username") or user.get("id") or "unknown"
    response_url = payload.get("response_url")

    # One interactive message triggers one action at a time in practice; we
    # handle the first and ignore the rest to keep the response simple.
    action = actions[0]
    action_id = action.get("action_id") or ""
    value = action.get("value") or ""

    if action_id == "pending_action_skip":
        _safely(mark_action_cancelled, value)
        return _slack_replace(f":no_entry: Skipped by {user_label}.")

    if action_id != "pending_action_execute":
        return _slack_replace(f":warning: Unknown action `{action_id}`.")

    pending = get_pending_action(value)
    if not pending:
        return _slack_replace(f":x: Action `{value}` not found.")

    claimed = claim_pending_action(value, claimed_by=user_label)
    if not claimed:
        current = pending.get("status")
        return _slack_replace(
            f":hourglass: Already `{current}` — ignoring duplicate click."
        )

    kind = claimed.get("kind") or ""
    executor = _EXECUTORS.get(kind)
    if not executor:
        mark_action_failed(value, f"no executor for kind {kind!r}")
        return _slack_replace(f":x: No executor registered for `{kind}`.")

    try:
        result = await executor(claimed)
        mark_action_executed(value, result=result)
        summary = _format_success(kind, result, user_label)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:400]}"
        mark_action_failed(value, err)
        summary = f":x: Failed: `{err}`"

    outgoing = _slack_replace(summary)
    if response_url:
        # Fire-and-forget post back to Slack so the message updates inline.
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(response_url, json=outgoing)
        except Exception as e:
            print(f"[slack_interact] response_url POST failed: {type(e).__name__}: {e}")
    return outgoing


def _safely(fn, *args, **kwargs) -> None:
    try:
        fn(*args, **kwargs)
    except Exception as e:
        print(f"[slack_interact] {fn.__name__} failed: {type(e).__name__}: {e}")


def _format_success(kind: str, result: dict, user_label: str) -> str:
    if kind == "create_person":
        pid = result.get("pipedrive_person_id")
        name = result.get("name") or "contact"
        extra = ""
        if result.get("note_added"):
            extra = " · extraction note attached"
        return (
            f":white_check_mark: Created *{name}* in Pipedrive (person `{pid}`)"
            f"{extra} · by {user_label}"
        )
    if kind == "update_person":
        pid = result.get("pipedrive_person_id")
        fields = ", ".join(result.get("fields_updated") or []) or "(none)"
        return f":white_check_mark: Updated person `{pid}` ({fields}) · by {user_label}"
    if kind == "mark_former":
        pid = result.get("pipedrive_person_id")
        return (
            f":white_check_mark: Set Role Category to *Former* for person `{pid}` "
            f"(audit note added) · by {user_label}"
        )
    if kind == "make_poc":
        pid = result.get("pipedrive_person_id")
        deals = result.get("deals_updated") or []
        n = len(deals)
        return (
            f":white_check_mark: Set person `{pid}` as main contact on *{n}* "
            f"deal(s) · by {user_label}"
        )
    return f":white_check_mark: Done · by {user_label}"


def parse_form_payload(body: bytes) -> dict:
    """Parse the ``payload`` field out of Slack's form-encoded body."""
    from urllib.parse import parse_qs

    parsed = parse_qs(body.decode())
    payload_list = parsed.get("payload") or []
    if not payload_list:
        return {}
    try:
        return json.loads(payload_list[0])
    except json.JSONDecodeError:
        return {}


__all__ = [
    "handle_slack_interaction",
    "parse_form_payload",
    "verify_slack_signature",
]
