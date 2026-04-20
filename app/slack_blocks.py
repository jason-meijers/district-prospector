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


def _fmt_field_val(value: Any, *, code: bool = False) -> str:
    if value is None or value == "":
        return "(none)"
    s = str(value).strip()
    return f"`{s}`" if code else s


def _build_update_detail_lines(contact: dict[str, Any]) -> list[str]:
    """
    Always include job title, then before → after only for fields in ``changes``.
    """
    changes = contact.get("changes") or []
    title = (contact.get("job_title") or "").strip() or "(unknown)"
    lines: list[str] = [
        f"*Job title:* {title}",
        "",
        "*What changed (values in Pipedrive → values from this research run):*",
    ]
    if not changes:
        lines.append("_No diff list on this record — compare job title above to Pipedrive if needed._")
        return lines

    if "title" in changes:
        lines.append(
            f"• *Title:* {_fmt_field_val(contact.get('previous_title'))} → "
            f"{_fmt_field_val(contact.get('job_title'))}"
        )
    if "email" in changes:
        lines.append(
            f"• *Email:* {_fmt_field_val(contact.get('previous_email'), code=True)} → "
            f"{_fmt_field_val(contact.get('email'), code=True)}"
        )
    if "phone" in changes:
        lines.append(
            f"• *Phone:* {_fmt_field_val(contact.get('previous_phone'), code=True)} → "
            f"{_fmt_field_val(contact.get('phone'), code=True)}"
        )
    if "role_category" in changes:
        lines.append(
            f"• *Role category:* {_fmt_field_val(contact.get('previous_role_category_label'))} → "
            f"{_fmt_field_val(contact.get('role_category_label'))}"
        )
    return lines


def _confirm_dialog(title: str, body: str, ok: str, cancel: str = "Nevermind") -> dict:
    return {
        "title": {"type": "plain_text", "text": title[:100]},
        "text": {"type": "mrkdwn", "text": body[:300]},
        "confirm": {"type": "plain_text", "text": ok[:30]},
        "deny": {"type": "plain_text", "text": cancel[:30]},
    }


def _make_poc_confirm(contact_name: str, deal_count: int, sample_titles: list[str]) -> dict:
    titles = ", ".join(sample_titles[:3]) if sample_titles else "(see Pipedrive)"
    if len(sample_titles) > 3:
        titles += " …"
    body = (
        f"This assigns *{contact_name}* as the *main contact* on *{deal_count}* open "
        f"deal(s) where the current contact is tagged *Former*.\n"
        f"Includes: {titles}"
    )
    if len(body) > 280:
        body = body[:277] + "…"
    return _confirm_dialog(
        title="Make point of contact?",
        body=body,
        ok="Yes, assign PoC",
    )


def _make_poc_actions_row(
    *,
    action_uuid: str,
    deal_count: int,
    contact_name: str,
    sample_titles: list[str],
) -> dict:
    confirm = _make_poc_confirm(contact_name, deal_count, sample_titles)
    label = f"Make PoC ({deal_count} deal{'s' if deal_count != 1 else ''})"
    return {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": label[:75]},
                "action_id": "pending_action_execute",
                "value": action_uuid,
                "confirm": confirm,
            }
        ],
    }


def _action_buttons(
    primary_action_id: str,
    primary_label: str,
    primary_value: str,
    *,
    primary_style: str = "primary",
    include_cancel_value: str | None = None,
    include_dismiss_not_target_value: str | None = None,
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
    if include_dismiss_not_target_value:
        dismiss_confirm = _confirm_dialog(
            title="Not a target role?",
            body=(
                "We will stop suggesting this person in Slack for this district "
                "(same name + title). You can still add them in Pipedrive manually."
            ),
            ok="Yes, dismiss",
        )
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Not a target role"},
                "action_id": "pending_action_dismiss_not_target_role",
                "value": include_dismiss_not_target_value,
                "confirm": dismiss_confirm,
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
    former_poc_deals: list[dict] | None = None,
) -> tuple[list[dict], str, str | None]:
    """
    Returns ``(blocks, primary_action_id, make_poc_action_id_or_none)``.
    ``make_poc_action_id`` is set when there are open deals whose main contact
    is tagged Former — see :func:`build_update_person_blocks`.
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

    blocks: list[dict] = [
        _section(header),
        _section("\n".join(detail_lines)),
    ]

    make_poc_id: str | None = None
    deals = [d for d in (former_poc_deals or []) if d.get("deal_id") is not None]
    if deals:
        deal_ids = [int(d["deal_id"]) for d in deals]
        titles = [
            (d.get("title") or "").strip() or f"Deal #{d.get('deal_id')}"
            for d in deals[:8]
        ]
        poc_payload: dict[str, Any] = {
            "deal_ids": deal_ids,
            "contact_name": payload["name"] or "",
            "deal_titles_sample": titles,
            # Lets /slack/interact create the person if they click Make PoC first.
            "create_payload": dict(payload),
        }
        if contact.get("notes"):
            poc_payload["notes_for_create"] = contact["notes"]
        make_poc_id = create_pending_action(
            kind="make_poc",
            payload=poc_payload,
            pipedrive_org_id=pipedrive_org_id,
            pipedrive_person_id=None,
            slack_channel=slack_channel,
        )
        blocks.append(
            _section(
                f"_There {'are' if len(deals) != 1 else 'is'} *{len(deals)}* open "
                f"deal(s) with a *Former* main contact. Use *Create in Pipedrive* or "
                f"*Make PoC* (we create the person if they are not in Pipedrive yet)._"
            )
        )

    blocks.append(
        _action_buttons(
            primary_action_id="pending_action_execute",
            primary_label="Create in Pipedrive",
            primary_value=action_id,
            include_cancel_value=action_id,
            include_dismiss_not_target_value=action_id,
        )
    )
    if make_poc_id:
        blocks.append(
            _make_poc_actions_row(
                action_uuid=make_poc_id,
                deal_count=len(deals),
                contact_name=payload["name"] or "This contact",
                sample_titles=titles,
            )
        )

    blocks.append(_context(f":id: `{action_id}`"))
    return blocks, action_id, make_poc_id


# ── Builder: update_person ─────────────────────────────────────────


def build_update_person_blocks(
    *,
    district_name: str,
    pipedrive_org_id: int,
    pipedrive_person_id: int,
    contact: dict[str, Any],
    slack_channel: str | None = None,
    former_poc_deals: list[dict] | None = None,
) -> tuple[list[dict], str, str | None]:
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
        "previous_email": contact.get("previous_email"),
        "previous_phone": contact.get("previous_phone"),
        "previous_role_category_id": contact.get("previous_role_category_id"),
        "previous_role_category_label": contact.get("previous_role_category_label"),
    }
    action_id = create_pending_action(
        kind="update_person",
        payload=payload,
        pipedrive_org_id=pipedrive_org_id,
        pipedrive_person_id=pipedrive_person_id,
        slack_channel=slack_channel,
    )

    header = f"*Existing contact updated — {district_name}*"
    intro = f"*Name:* {payload['name']}"
    detail_body = "\n".join([intro, ""] + _build_update_detail_lines(contact))

    blocks: list[dict] = [
        _section(header),
        _section(detail_body),
    ]

    make_poc_id: str | None = None
    deals = [d for d in (former_poc_deals or []) if d.get("deal_id") is not None]
    if deals:
        deal_ids = [int(d["deal_id"]) for d in deals]
        titles = [
            (d.get("title") or "").strip() or f"Deal #{d.get('deal_id')}"
            for d in deals[:8]
        ]
        make_poc_id = create_pending_action(
            kind="make_poc",
            payload={
                "deal_ids": deal_ids,
                "contact_name": payload["name"] or "",
                "deal_titles_sample": titles,
            },
            pipedrive_org_id=pipedrive_org_id,
            pipedrive_person_id=int(pipedrive_person_id),
            slack_channel=slack_channel,
        )
        blocks.append(
            _section(
                f"_Open deal(s) above have a *Former* main contact — use *Make PoC* "
                f"to switch them to *{payload['name'] or 'this person'}*._"
            )
        )

    blocks.append(
        _action_buttons(
            primary_action_id="pending_action_execute",
            primary_label="Apply update",
            primary_value=action_id,
            include_cancel_value=action_id,
        )
    )
    if make_poc_id:
        blocks.append(
            _make_poc_actions_row(
                action_uuid=make_poc_id,
                deal_count=len(deals),
                contact_name=payload["name"] or "This contact",
                sample_titles=titles,
            )
        )

    blocks.append(_context(f":id: `{action_id}` · person `{pipedrive_person_id}`"))
    return blocks, action_id, make_poc_id


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
