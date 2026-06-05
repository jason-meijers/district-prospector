"""
Deterministic job-title filters for target-role extraction.

The LLM prompts already list exclusions, but large staff rosters (especially
from platform adapters) cause the model to surface irrelevant "Director of X"
titles. These helpers enforce the same rules in code.
"""
from __future__ import annotations

import re

# Pipedrive role IDs we actively prospect.
TARGET_ROLE_CATEGORY_IDS: frozenset[int] = frozenset(
    {482, 468, 472, 623, 471, 467, 470, 480}
)

# Generic buckets that need a strong title match, not just "Director".
_AMBIGUOUS_ROLE_CATEGORY_IDS: frozenset[int] = frozenset({474, 478, 481})

# Phrases that disqualify a title outright (checked on normalized text).
_EXCLUSION_PHRASES: tuple[str, ...] = (
    "human resource",
    "human relation",
    "community relation",
    "public relation",
    "corporate relation",
    "transportation",
    "transport ",
    " director of transport",
    "facilit",
    "maintenance",
    "custodial",
    "grounds",
    "construction",
    "operations",
    "business service",
    "finance",
    "accounting",
    "payroll",
    "purchasing",
    "procurement",
    "accounts payable",
    "food service",
    "nutrition",
    "information technolog",
    " technology",
    " it director",
    "police",
    "security",
    "school resource officer",
    " sro",
    "athletic",
    "sport",
    "fine art",
    "band",
    "choir",
    "theater",
    "theatre",
    "equity",
    "diversity",
    "inclusion",
    "mtss",
    "multi-tiered",
    "gifted",
    " gate",
    "advanced learning",
    "expedition",
    "elementary",
    "primary",
    "middle school",
    " k-5",
    " k-8",
    "grades 1-6",
    "counselor",
    "teacher",
    "nurse",
    "coach",
    "trustee",
    "board member",
    "communications",
    "marketing",
    "foundation",
    "partnership",
    "volunteer",
    "registrar",
    "secretary",
    "administrative assistant",
    "paraprofessional",
    "aide",
    "custodian",
    "librarian",
    "media specialist",
)

# Strong positive signals — title is clearly in our prospecting set.
_STRONG_TARGET_RE = re.compile(
    r"""
    superintendent
    |deputy\s+superintendent
    |assistant\s+superintendent
    |associate\s+superintendent
    |interim\s+superintendent
    |acting\s+superintendent
    |chief\s+academic
    |curriculum
    |instruction
    |teaching\s+(?:&|and)\s+learning
    |instructional\s+service
    |academic\s+program
    |career\s+(?:technical|tech|pathway|&|and)
    |career\s+pathway
    |vocational
    |\bcte\b
    |secondary\s+education
    |high\s+school\s+program
    |director\s+of\s+secondary
    |(?:high|secondary)\s+school\s+principal
    |principal.*(?:high|secondary)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _normalize_title(title: str | None) -> str:
    t = (title or "").strip().lower()
    t = t.replace("&", " and ")
    t = re.sub(r"\s+", " ", t)
    return f" {t} "


def is_excluded_job_title(title: str | None) -> bool:
    """True when the title clearly falls outside our target roles."""
    norm = _normalize_title(title)
    if not norm.strip():
        return True
    return any(phrase in norm for phrase in _EXCLUSION_PHRASES)


def is_strong_target_job_title(title: str | None) -> bool:
    """True when the title unambiguously matches superintendent/curriculum/CTE."""
    norm = _normalize_title(title)
    if not norm.strip() or is_excluded_job_title(title):
        return False
    return bool(_STRONG_TARGET_RE.search(norm))


def _coerce_role_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_target_extracted_contact(contact: dict) -> bool:
    """
    Whether an extracted contact should flow through to Slack / CRM.

    Uses role_category_id when present, but never trusts generic Director/Other
    buckets without a strong title match.
    """
    title = contact.get("job_title") or contact.get("title")
    if is_excluded_job_title(title):
        return False

    role_id = _coerce_role_id(contact.get("role_category_id"))
    if role_id in _AMBIGUOUS_ROLE_CATEGORY_IDS:
        return is_strong_target_job_title(title)
    if role_id is not None and role_id not in TARGET_ROLE_CATEGORY_IDS:
        return is_strong_target_job_title(title)
    if role_id in TARGET_ROLE_CATEGORY_IDS:
        return True
    return is_strong_target_job_title(title)


def is_obviously_non_target_roster_title(title: str | None) -> bool:
    """
    For platform-adapter roster pre-filtering: drop clear non-targets before
    the LLM sees a 500-person directory. Slightly looser than post-filter —
    we only remove obvious junk and leave borderline titles for the model.
    """
    return is_excluded_job_title(title)
