"""
Slack Block Kit builders for interactive contact actions.

Each builder takes a contact-review decision (create / update / mark former),
persists the full Pipedrive payload in ``pending_actions``, and returns a
Block Kit ``blocks`` array whose buttons reference the action id rather than
the payload itself. The payload never appears in Slack, which keeps the
message readable and prevents users from hand-editing request bodies.
"""

from __future__ import annotations

from typing import Any

from app.database import create_pending_action


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _divider() -> dict:
    return {"type": "divider"}


def _confirm_dialog(title: str, body: str, ok: str, cancel: str = "Nevermind") -> dict:
    return {
        "title": {"type": "plain_text", "text": title[:100]},
        "text": {"type": "mrkdwn", "text": body[:300]},
        "confirm": {"type": "plain_text", "text": ok[:30]},
        "deny": {"type": "plain_text", "text": cancel[:30]},
    }


def _action_buttons(
    primary_action_id: str,
    primary_label: str,
    primary_value: str,
    *,
    primary_style: str = "primary",
    include_cancel_value: str | None = None,
    confirm: dict | None = None,
) -> dict:
    elements: list[dict] = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": primary_label[:75]},
            "style": primary_style,
            "action_id": primary_action_id,
            "value": primary_value,
        }
    ]
    if confirm:
        elements[0]["confirm"] = confirm
    if include_cancel_value:
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Skip"},
                "action_id": "pending_action_skip",
                "value": include_cancel_value,
            }
        )
    return {"type": "actions", "elements": elements}


# ── Builder: create_person ─────────────────────────────────────────


def build_create_person_blocks(
    *,
    district_name: str,
    pipedrive_org_id: int,
    contact: dict[str, Any],
    slack_channel: str | None = None,
) -> tuple[list[dict], str]:
    """
    Returns ``(blocks, action_id)`` for a "create new person" message. The
    caller posts ``blocks`` to Slack and should later record the returned
    ``slack_message_ts`` on the action via
    :func:`database.attach_slack_message_to_action`.
    """
    payload = {
        "name": contact.get("name") or "",
        "job_title": contact.get("job_title") or "",
        "email": contact.get("email"),
        "phone": contact.get("phone"),
        "role_category_id": contact.get("role_category_id"),
        "role_category_label": contact.get("role_category_label"),
        "source_url": contact.get("source_url"),
        "salutation": contact.get("salutation"),
    }
    note_payload = None
    if contact.get("notes"):
        note_payload = {
            "content": f"Extracted during District Prospector run: {contact['notes']}",
        }

    action_id = create_pending_action(
        kind="create_person",
        payload=payload,
        pipedrive_org_id=pipedrive_org_id,
        note_payload=note_payload,
        slack_channel=slack_channel,
    )

    role_label = contact.get("role_category_label") or contact.get("role_category") or "Unknown role"
    header = f"*New contact found — {district_name}*"
    detail_lines = [
        f"*Name:* {payload['name'] or 'Unknown'}",
        f"*Title:* {payload['job_title'] or 'Unknown'}",
        f"*Role category:* {role_label}",
    ]
    if payload.get("email"):
        detail_lines.append(f"*Email:* `{payload['email']}`")
    if payload.get("phone"):
        detail_lines.append(f"*Phone:* `{payload['phone']}`")
    if payload.get("source_url"):
        detail_lines.append(f"*Source:* `{payload['source_url']}`")

    blocks = [
        _section(header),
        _section("\n".join(detail_lines)),
        _action_buttons(
            primary_action_id="pending_action_execute",
            primary_label="Create in Pipedrive",
            primary_value=action_id,
            include_cancel_value=action_id,
        ),
        _context(f":id: `{action_id}`"),
    ]
    return blocks, action_id


# ── Builder: update_person ─────────────────────────────────────────


def build_update_person_blocks(
    *,
    district_name: str,
    pipedrive_org_id: int,
    pipedrive_person_id: int,
    contact: dict[str, Any],
    slack_channel: str | None = None,
) -> tuple[list[dict], str]:
    changes = contact.get("changes") or []
    payload = {
        "name": contact.get("name") or "",
        "job_title": contact.get("job_title"),
        "email": contact.get("email"),
        "phone": contact.get("phone"),
        "role_category_id": contact.get("role_category_id"),
        "role_category_label": contact.get("role_category_label"),
        "source_url": contact.get("source_url"),
        "changes": list(changes),
        "previous_title": contact.get("previous_title"),
    }
    action_id = create_pending_action(
        kind="update_person",
        payload=payload,
        pipedrive_org_id=pipedrive_org_id,
        pipedrive_person_id=pipedrive_person_id,
        slack_channel=slack_channel,
    )

    prev = contact.get("previous_title") or "(unknown)"
    header = f"*Existing contact updated — {district_name}*"
    detail_lines = [
        f"*Name:* {payload['name']}",
        f"*Previous title:* {prev}",
        f"*New title:* {payload.get('job_title') or '(unchanged)'}",
    ]
    if "email" in changes and payload.get("email"):
        detail_lines.append(f"*Email →* `{payload['email']}`")
    if "phone" in changes and payload.get("phone"):
        detail_lines.append(f"*Phone →* `{payload['phone']}`")
    if "role_category" in changes and payload.get("role_category_label"):
        detail_lines.append(f"*Role category →* {payload['role_category_label']}")

    blocks = [
        _section(header),
        _section("\n".join(detail_lines)),
        _action_buttons(
            primary_action_id="pending_action_execute",
            primary_label="Apply update",
            primary_value=action_id,
            include_cancel_value=action_id,
        ),
        _context(f":id: `{action_id}` · person `{pipedrive_person_id}`"),
    ]
    return blocks, action_id


# ── Builder: mark_former ───────────────────────────────────────────


def build_mark_former_blocks(
    *,
    district_name: str,
    pipedrive_org_id: int,
    pipedrive_person_id: int,
    contact: dict[str, Any],
    slack_channel: str | None = None,
) -> tuple[list[dict], str]:
    payload = {
        "name": contact.get("name") or "",
        "previous_title": contact.get("previous_title"),
        "notes": contact.get("notes") or "",
    }
    action_id = create_pending_action(
        kind="mark_former",
        payload=payload,
        pipedrive_org_id=pipedrive_org_id,
        pipedrive_person_id=pipedrive_person_id,
        slack_channel=slack_channel,
    )

    header = f"*Possibly departed — {district_name}*"
    detail_lines = [
        f"*Name:* {payload['name']}",
        f"*Previous title:* {payload.get('previous_title') or '(unknown)'}",
        f"_Not found on district site during this run._",
    ]

    confirm = _confirm_dialog(
        title="Mark as former?",
        body=(
            f"This will tag {payload['name']} as a former employee in "
            "Pipedrive. You can reverse it manually later."
        ),
        ok="Yes, mark former",
    )

    blocks = [
        _section(header),
        _section("\n".join(detail_lines)),
        _action_buttons(
            primary_action_id="pending_action_execute",
            primary_label="Mark as former",
            primary_value=action_id,
            primary_style="danger",
            include_cancel_value=action_id,
            confirm=confirm,
        ),
        _context(f":id: `{action_id}` · person `{pipedrive_person_id}`"),
    ]
    return blocks, action_id


__all__ = [
    "build_create_person_blocks",
    "build_update_person_blocks",
    "build_mark_former_blocks",
]
