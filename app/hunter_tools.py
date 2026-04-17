"""
Thin wrappers the ContactHunter agent loop calls out to. Each function takes
the tool input dict (already JSON-parsed from Claude's tool_use block) and
returns a JSON-serializable dict.

Grouping these here keeps ``app/contact_hunter.py`` focused on loop mechanics
and makes the tool layer trivially mockable in tests.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from app.config import get_settings
from app.firecrawl_scraper import (
    _fetch_html,
    _get_async_firecrawl,
    _extract_markdown,
    _extract_search_urls,
    discover_map_candidates,
    scrape_page,
)
from app.platforms import detect_platform, fetch_with_adapter


ToolImpl = Callable[[dict], Awaitable[dict]]


def build_default_tool_impls(
    base_url: str,
    usage: dict | None = None,
) -> dict[str, ToolImpl]:
    """
    Return the tool-name → async callable mapping the ContactHunter expects.

    ``usage`` is the Firecrawl usage accumulator. Tools mutate it so callers
    can roll the cost up alongside the pipeline-first pass.
    """
    shared_usage = usage if usage is not None else {}

    async def firecrawl_map(args: dict) -> dict:
        items = await discover_map_candidates(base_url, usage=shared_usage)
        query = (args.get("query") or "").strip().lower()
        if query:
            items = [
                it
                for it in items
                if query in (it.get("url") or "").lower()
                or query in (it.get("title") or "").lower()
                or query in (it.get("description") or "").lower()
            ]
        sample = []
        for it in items[:25]:
            sample.append(
                {
                    "url": it.get("url"),
                    "title": (it.get("title") or "")[:120],
                    "description": (it.get("description") or "")[:200],
                }
            )
        return {"count": len(items), "sample": sample}

    async def firecrawl_search(args: dict) -> dict:
        query = (args.get("query") or "").strip()
        if not query:
            return {"error": "query is required"}
        domain = urlparse(base_url).netloc.lstrip("www.")
        scoped_query = f"site:{domain} {query}" if domain else query
        app = _get_async_firecrawl()
        try:
            result = await app.search(query=scoped_query, limit=8)
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}", "query": scoped_query}
        if shared_usage is not None:
            shared_usage["search_calls"] = int(shared_usage.get("search_calls") or 0) + 1
        urls = _extract_search_urls(result) or []
        return {"query": scoped_query, "urls": urls[:10]}

    async def firecrawl_scrape(args: dict) -> dict:
        url = (args.get("url") or "").strip()
        if not url:
            return {"error": "url is required"}
        interact_priority = bool(args.get("interact_priority"))
        content = await scrape_page(
            url,
            usage=shared_usage,
            interact_priority=interact_priority,
        )
        if not content:
            return {"url": url, "error": "no content returned"}
        return {
            "url": url,
            "chars": len(content),
            "markdown": content[:20_000],
            "truncated": len(content) > 20_000,
        }

    async def fetch_platform_api(args: dict) -> dict:
        html = await _fetch_html(base_url)
        if not html:
            return {"error": "could not fetch homepage html"}
        detection = await detect_platform(html, base_url)
        if detection is None:
            return {"detected": False, "note": "no known platform adapter matched"}
        pages = await fetch_with_adapter(base_url, detection, usage=shared_usage)
        if not pages:
            return {
                "detected": True,
                "platform": detection.name,
                "confidence": round(detection.confidence, 3),
                "note": "adapter detected but returned no pages",
            }
        return {
            "detected": True,
            "platform": detection.name,
            "confidence": round(detection.confidence, 3),
            "pages": [
                {
                    "url": p.url,
                    "chars": len(p.content),
                    "markdown": p.content[:20_000],
                }
                for p in pages[:5]
            ],
        }

    return {
        "firecrawl_map": firecrawl_map,
        "firecrawl_search": firecrawl_search,
        "firecrawl_scrape": firecrawl_scrape,
        "fetch_platform_api": fetch_platform_api,
    }


__all__ = ["ToolImpl", "build_default_tool_impls"]
