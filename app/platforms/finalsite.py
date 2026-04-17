"""
Finalsite adapter.

Finalsite (Composer / Connect) is a very common K-12 CMS. Their pages
typically include a ``<meta name="generator" content="Finalsite Composer">``
tag, reference ``.finalsite.com`` or ``fs-webfiles`` in script srcs, and use
``fsContent`` / ``fsStaffDirectoryListing`` class names.

We currently only *detect* Finalsite so upstream metrics can measure its
prevalence. A real fetch implementation (hitting the ListManager JSON
endpoints) will land once we've profiled a few live instances. Until then
the dispatcher continues into the normal Firecrawl+LLM path.
"""

from __future__ import annotations

import re

from app.platforms import PlatformAdapter, PlatformDetection, PlatformPage, register


_SIGNAL_PATTERNS = [
    re.compile(r"name=\"generator\"\s+content=\"[^\"]*Finalsite", re.IGNORECASE),
    re.compile(r"\.finalsite\.com", re.IGNORECASE),
    re.compile(r"fs-webfiles", re.IGNORECASE),
    re.compile(r"class=\"fsContent", re.IGNORECASE),
    re.compile(r"fsStaffDirectory", re.IGNORECASE),
]


class FinalsiteAdapter:
    name = "finalsite"

    async def detect(
        self,
        homepage_html: str,
        base_url: str,
    ) -> PlatformDetection | None:
        if not homepage_html:
            return None
        hits = sum(1 for p in _SIGNAL_PATTERNS if p.search(homepage_html))
        if hits == 0:
            return None
        confidence = min(0.6 + 0.1 * hits, 0.95)
        return PlatformDetection(
            name=self.name,
            confidence=confidence,
            context={"signal_hits": hits},
        )

    async def fetch(
        self,
        base_url: str,
        detection: PlatformDetection,
        usage: dict | None = None,
    ) -> list[PlatformPage]:
        print(
            f"[platforms.finalsite] detected at {base_url} "
            f"(confidence={detection.confidence:.2f}); deferring to Firecrawl+LLM"
        )
        return []


register(FinalsiteAdapter())
