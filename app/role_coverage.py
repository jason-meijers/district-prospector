"""
Role coverage scoring — figure out which target roles are *missing* from
an extracted contact set so the ContactHunter agent can be pointed at the
right gaps.

The three cohorts we always want at least one contact for:

- ``superintendent``  → the top district leader (role id 482 or
  assistant superintendent 468 as a fallback)
- ``curriculum``      → curriculum/instruction leadership (role ids 472/623)
- ``cte``             → career & technical education leadership (role ids 471/467/470)

Usage::

    coverage = score_role_coverage(contacts)
    if coverage.has_gaps:
        hunter.hunt(goal=HuntGoal.fill_gaps(coverage.missing), ...)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


# Role cohort → set of Pipedrive role_category IDs that satisfy the cohort.
_COHORTS: dict[str, frozenset[int]] = {
    "superintendent": frozenset({482, 468}),
    "curriculum": frozenset({472, 623}),
    "cte": frozenset({471, 467, 470}),
}


def _coerce_role_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class CoverageReport:
    """Which target cohorts are already covered and which are still open."""

    covered: frozenset[str] = field(default_factory=frozenset)
    missing: frozenset[str] = field(default_factory=frozenset)
    by_cohort: dict[str, list[str]] = field(default_factory=dict)

    @property
    def has_gaps(self) -> bool:
        return bool(self.missing)

    def as_dict(self) -> dict:
        return {
            "covered": sorted(self.covered),
            "missing": sorted(self.missing),
            "by_cohort": {k: list(v) for k, v in self.by_cohort.items()},
        }


def score_role_coverage(contacts: Iterable[dict]) -> CoverageReport:
    """Return a :class:`CoverageReport` for the given extracted contacts."""
    by_cohort: dict[str, list[str]] = {k: [] for k in _COHORTS}

    for c in contacts or []:
        rid = _coerce_role_id(c.get("role_category_id"))
        name = (c.get("name") or "").strip()
        if rid is None or not name:
            continue
        for cohort, ids in _COHORTS.items():
            if rid in ids:
                by_cohort[cohort].append(name)

    covered = {k for k, v in by_cohort.items() if v}
    missing = set(_COHORTS.keys()) - covered
    return CoverageReport(
        covered=frozenset(covered),
        missing=frozenset(missing),
        by_cohort=by_cohort,
    )


def cohort_labels() -> dict[str, str]:
    """Human labels for cohorts (used in prompts + Slack messages)."""
    return {
        "superintendent": "Superintendent (or Assistant Superintendent)",
        "curriculum": "Curriculum / Instruction leader",
        "cte": "CTE leader",
    }
