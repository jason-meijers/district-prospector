"""
Apptegy / Thrillshare adapter.

Apptegy-built district sites ship a Nuxt SSR payload (``__NUXT__``) that
includes the full staff roster — visible in the HTML before any JS runs.
Signatures we look for:

* ``window.__NUXT__`` script blocks with ``Thrillshare`` or ``Apptegy``
  references.
* ``thrillshare-spa``, ``apptegy``, or ``tsassets`` in script srcs.
* ``<meta name="generator" content="Apptegy">``.

Like the Finalsite adapter this currently performs detection only; the SSR
payload is already mined by :mod:`app.scraper.clean_html`, so bypassing
Firecrawl would duplicate logic. The metric is the value today.
"""

from __future__ import annotations

import re

from app.platforms import PlatformAdapter, PlatformDetection, PlatformPage, register


_SIGNAL_PATTERNS = [
    re.compile(r"window\.__NUXT__", re.IGNORECASE),
    re.compile(r"thrillshare-spa|thrillshare\.com", re.IGNORECASE),
    re.compile(r"apptegy(\.|\-)", re.IGNORECASE),
    re.compile(r"tsassets\.", re.IGNORECASE),
    re.compile(r"name=\"generator\"\s+content=\"[^\"]*Apptegy", re.IGNORECASE),
]


class ApptegyAdapter:
    name = "apptegy"

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
            f"[platforms.apptegy] detected at {base_url} "
            f"(confidence={detection.confidence:.2f}); SSR extraction handled "
            f"downstream by clean_html()"
        )
        return []


register(ApptegyAdapter())
