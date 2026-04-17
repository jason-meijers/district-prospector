"""
Platform adapter package.

A *platform adapter* knows how to recognize a specific school-website CMS
(SchoolInsites, Finalsite, Apptegy, etc.) and pull its staff-contact data
through a cheaper / more precise path than Firecrawl+LLM. Examples:

* SchoolInsites → hit ``/sys/api/directory`` JSON and format locally.
* Finalsite → request the staff-module JSON endpoint.
* Apptegy/Thrillshare → parse Nuxt SSR blobs that already contain the
  directory.

Adapters are plain Python objects implementing :class:`PlatformAdapter`.
The dispatcher :func:`detect_platform` runs each registered adapter's
``detect`` on a pre-fetched homepage HTML blob and returns the highest-
confidence hit (or ``None`` if nothing matches above the threshold).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from app.config import get_settings


@dataclass
class PlatformPage:
    """A single logical page extracted by an adapter."""

    url: str
    content: str


@dataclass
class PlatformDetection:
    """
    Result of :meth:`PlatformAdapter.detect`. ``confidence`` is a 0..1 score
    - values below ``Settings.platform_adapter_min_confidence`` are ignored by
    the dispatcher. ``context`` is adapter-private data passed back into
    :meth:`PlatformAdapter.fetch` so detection work isn't duplicated.
    """

    name: str
    confidence: float
    context: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class PlatformAdapter(Protocol):
    """Adapter contract — see module docstring."""

    name: str

    async def detect(
        self,
        homepage_html: str,
        base_url: str,
    ) -> PlatformDetection | None:
        """Return a detection hit or None. Must be cheap (no paginated fetches)."""

    async def fetch(
        self,
        base_url: str,
        detection: PlatformDetection,
        usage: dict | None = None,
    ) -> list[PlatformPage]:
        """Do the expensive work: call platform APIs, parse SSR blobs, etc."""


_REGISTRY: list[PlatformAdapter] = []


def register(adapter: PlatformAdapter) -> PlatformAdapter:
    """Register an adapter at import time. Safe to call multiple times."""
    if adapter not in _REGISTRY:
        _REGISTRY.append(adapter)
    return adapter


def registered_adapters() -> list[PlatformAdapter]:
    return list(_REGISTRY)


async def detect_platform(
    homepage_html: str,
    base_url: str,
) -> PlatformDetection | None:
    """
    Ask every registered adapter to self-identify against the provided
    homepage HTML. Returns the highest-confidence hit at or above
    ``Settings.platform_adapter_min_confidence``, else None. When
    ``Settings.platform_adapters_enabled`` is false, always returns None.
    """
    if not homepage_html:
        return None

    settings = get_settings()
    if not bool(settings.platform_adapters_enabled):
        return None
    threshold = float(settings.platform_adapter_min_confidence)

    best: PlatformDetection | None = None
    for adapter in _REGISTRY:
        try:
            hit = await adapter.detect(homepage_html, base_url)
        except Exception as e:
            print(f"[platforms] {adapter.name}.detect raised {type(e).__name__}: {e}")
            continue
        if not hit or hit.confidence < threshold:
            continue
        if best is None or hit.confidence > best.confidence:
            best = hit
    return best


async def fetch_with_adapter(
    base_url: str,
    detection: PlatformDetection,
    usage: dict | None = None,
) -> list[PlatformPage]:
    """Invoke the matching adapter's ``fetch`` and return its pages."""
    for adapter in _REGISTRY:
        if adapter.name.lower() == detection.name.lower():
            try:
                return await adapter.fetch(base_url, detection, usage=usage)
            except Exception as e:
                print(
                    f"[platforms] {adapter.name}.fetch failed "
                    f"({type(e).__name__}: {e})"
                )
                return []
    return []


# Importing the concrete adapters registers them via ``register(...)``.
# Kept at the bottom to avoid circular imports at module load.
from app.platforms import schoolinsites as _schoolinsites  # noqa: E402, F401
from app.platforms import finalsite as _finalsite  # noqa: E402, F401
from app.platforms import apptegy as _apptegy  # noqa: E402, F401


__all__ = [
    "PlatformAdapter",
    "PlatformDetection",
    "PlatformPage",
    "detect_platform",
    "fetch_with_adapter",
    "register",
    "registered_adapters",
]
