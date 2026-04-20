"""
Normalize contact text before Slack and Pipedrive: no SHOUTING (all-caps) in
names/titles/notes; emails lowercased.
"""

from __future__ import annotations

from typing import Any


def fix_all_caps_field(value: str | None) -> str | None:
    """
    If the string is entirely uppercase (aside from non-letters), convert to
    :meth:`str.title` so CRM and Slack stay readable.
    """
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if not any(c.isalpha() for c in s):
        return s
    if s == s.upper():
        return s.title()
    return s


def sanitize_email(value: str | None) -> str | None:
    if value is None:
        return None
    t = value.strip().lower()
    return t or None


def sanitize_phone(value: str | None) -> str | None:
    if value is None:
        return None
    t = value.strip()
    return t or None


_CAPS_KEYS = frozenset(
    {
        "name",
        "job_title",
        "title",
        "previous_title",
        "notes",
        "evidence",
        "role_category_label",
        "role_category",
    }
)


def sanitize_contact_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy with string fields normalized for display and CRM."""
    if not data:
        return data
    out = dict(data)
    for k in _CAPS_KEYS:
        if k not in out or not isinstance(out[k], str):
            continue
        raw = out[k].strip()
        if not raw:
            out[k] = ""
            continue
        if not any(c.isalpha() for c in raw):
            out[k] = raw
            continue
        if raw == raw.upper():
            out[k] = raw.title()
        else:
            out[k] = raw
    if "email" in out and out["email"] is not None:
        out["email"] = sanitize_email(str(out["email"]))
    if "phone" in out and out["phone"] is not None:
        out["phone"] = sanitize_phone(str(out["phone"]))
    if "previous_email" in out and isinstance(out.get("previous_email"), str):
        out["previous_email"] = sanitize_email(out["previous_email"])
    if "previous_phone" in out and isinstance(out.get("previous_phone"), str):
        out["previous_phone"] = sanitize_phone(out["previous_phone"])
    return out


__all__ = [
    "fix_all_caps_field",
    "sanitize_contact_dict",
    "sanitize_email",
    "sanitize_phone",
]
