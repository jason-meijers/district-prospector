from __future__ import annotations
import asyncio
import json
import time
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from app.config import get_settings
from app.batch_agent import BatchExtractionAgent

# Playwright (node) — paginate staff directories in one interact call; __MAX_PAGES__ substituted at runtime.
_INTERACT_NODE_TEMPLATE = r"""
const maxPages = __MAX_PAGES__;
const chunks = [];
for (let p = 0; p < maxPages; p++) {
  await page.waitForTimeout(300);
  const txt = await page.evaluate(() => (document.body && document.body.innerText) ? document.body.innerText : '');
  chunks.push('--- Page ' + (p + 1) + ' ---\n' + String(txt).slice(0, 60000));
  if (p >= maxPages - 1) break;
  const urlBefore = page.url();
  let clicked = false;
  const selectors = [
    'a[rel="next"]',
    'a[aria-label*="Next" i]',
    'button[aria-label*="Next" i]',
    'a.pagination-next',
    '.pagination a.next',
    'a[class*="next" i]',
  ];
  for (const sel of selectors) {
    try {
      const loc = page.locator(sel).first();
      if (await loc.isVisible({ timeout: 600 })) {
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 20000 }).catch(() => {}),
          loc.click(),
        ]);
        clicked = true;
        break;
      }
    } catch (e) {}
  }
  if (!clicked) break;
  if (page.url() === urlBefore) break;
}
JSON.stringify({ markdown: chunks.join('\n\n') });
"""

_interact_semaphore: asyncio.Semaphore | None = None


def new_firecrawl_usage() -> dict:
    return {
        "scrape_calls": 0,
        "map_calls": 0,
        "search_calls": 0,
        "scrape_credits_sum": 0,
        "interact_calls": 0,
        "interact_seconds": 0.0,
        "interact_mode": None,
        "interact_stop_credits": 0,
    }


def estimate_firecrawl_credits(usage: dict) -> dict:
    """
    Rough credit estimate (Firecrawl bills in credits; interact is time-based for code vs prompt).
    When the API returns ``interact_stop_credits``, we use max(time-based, stop) for interact.
    """
    scrape_est = float(usage.get("scrape_calls") or 0)
    secs = float(usage.get("interact_seconds") or 0)
    mode = usage.get("interact_mode") or "code"
    rate = 7.0 if mode == "prompt" else 2.0
    interact_from_time = (secs / 60.0) * rate
    stop = float(usage.get("interact_stop_credits") or 0)
    interact_est = max(interact_from_time, stop) if stop > 0 else interact_from_time
    map_est = float(usage.get("map_calls") or 0)
    search_est = float(usage.get("search_calls") or 0)
    total = scrape_est + interact_est + map_est + search_est
    return {
        "scrape_estimate": scrape_est,
        "interact_estimate": round(interact_est, 2),
        "stop_credits": stop,
        "map_estimate": map_est,
        "search_estimate": search_est,
        "total_estimate": round(total, 2),
    }


def format_firecrawl_usage_slack_line(usage: dict | None) -> str:
    if not usage:
        return ""
    if not (
        usage.get("scrape_calls")
        or usage.get("interact_calls")
        or usage.get("map_calls")
        or usage.get("search_calls")
    ):
        return ""
    est = estimate_firecrawl_credits(usage)
    parts: list[str] = []
    if usage.get("scrape_calls"):
        parts.append(f"{usage['scrape_calls']} scrape(s)")
    if usage.get("map_calls"):
        parts.append(f"{usage['map_calls']} map(s)")
    if usage.get("search_calls"):
        parts.append(f"{usage['search_calls']} search(es)")
    if usage.get("interact_calls"):
        mode = usage.get("interact_mode") or "code"
        secs = float(usage.get("interact_seconds") or 0)
        parts.append(f"{usage['interact_calls']} interact, {secs:.1f}s ({mode})")
    summary = "; ".join(parts)
    return (
        f"Firecrawl (est.): {summary} — ≈ {est['total_estimate']} credits "
        f"(~1/scrape & map/search; interact ~2/min code, ~7/min prompt)"
    )


def _get_interact_semaphore() -> asyncio.Semaphore:
    global _interact_semaphore
    if _interact_semaphore is None:
        n = max(1, get_settings().firecrawl_interact_concurrency)
        _interact_semaphore = asyncio.Semaphore(n)
    return _interact_semaphore


def _add_scrape_credits_from_document(usage: dict, doc: object) -> None:
    try:
        if hasattr(doc, "metadata_typed"):
            cu = doc.metadata_typed.credits_used
            if cu is not None:
                usage["scrape_credits_sum"] = int(usage.get("scrape_credits_sum") or 0) + int(cu)
    except (TypeError, ValueError):
        pass


def _extract_scrape_id(result: object) -> str | None:
    if result is None:
        return None
    md = getattr(result, "metadata", None)
    if md is None:
        return None
    if hasattr(md, "scrape_id") and md.scrape_id:
        return str(md.scrape_id)
    if isinstance(md, dict):
        sid = md.get("scrape_id") or md.get("scrapeId")
        return str(sid) if sid else None
    return None


def _directory_like_url(url: str) -> bool:
    """True if path suggests staff/leadership directory (same signals as map scoring)."""
    return _score_url(url) >= 1


def _should_run_interact(
    url: str,
    markdown: str | None,
    settings,
    interact_priority: bool = False,
) -> bool:
    if not settings.firecrawl_interact_enabled:
        return False
    n = len((markdown or "").strip())
    thin = n < int(settings.firecrawl_interact_min_markdown_chars)
    if interact_priority:
        return thin
    if not _directory_like_url(url):
        return False
    return thin


def _text_from_interact_response(resp: object) -> str | None:
    if resp is None:
        return None
    raw = getattr(resp, "result", None) or getattr(resp, "stdout", None) or getattr(resp, "output", None)
    if isinstance(raw, dict):
        return raw.get("markdown") or raw.get("text")
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data.get("markdown") or data.get("text")
        except json.JSONDecodeError:
            pass
    if isinstance(raw, str) and len(raw.strip()) > 50:
        return raw.strip()
    return None


async def _run_interact_directory(
    app: object,
    scrape_id: str,
    settings,
    page_url: str,
    usage: dict | None,
) -> str | None:
    """
    One code-based interact session: capture text per page and follow Next controls.
    """
    max_pages = max(1, min(int(settings.firecrawl_interact_max_pages), 50))
    code = _INTERACT_NODE_TEMPLATE.replace("__MAX_PAGES__", str(max_pages))
    timeout_sec = max(10, min(int(settings.firecrawl_interact_max_seconds), 300))

    async with _get_interact_semaphore():
        interact_fn = getattr(app, "interact", None)
        if not interact_fn:
            return None
        t0 = time.monotonic()
        text_out: str | None = None
        try:
            resp = await interact_fn(
                scrape_id,
                code=code,
                language="node",
                timeout=timeout_sec,
                origin="district-prospector",
            )
            if usage is not None:
                usage["interact_calls"] = int(usage.get("interact_calls") or 0) + 1
                usage["interact_mode"] = "code"
            raw = _text_from_interact_response(resp)
            if raw and len(raw.strip()) > 80:
                text_out = (
                    "## Staff directory (Firecrawl interact)\n"
                    f"Source: {page_url}\n\n"
                    + raw
                )
            else:
                print(f"[firecrawl] interact returned thin content for {page_url}")
        except Exception as e:
            print(f"[firecrawl] interact failed for {page_url}: {type(e).__name__}: {e}")
        finally:
            elapsed = time.monotonic() - t0
            if usage is not None:
                usage["interact_seconds"] = float(usage.get("interact_seconds") or 0) + elapsed
            try:
                stop_fn = getattr(app, "stop_interaction", None)
                if stop_fn and scrape_id:
                    stop = await stop_fn(scrape_id)
                    cb = getattr(stop, "credits_billed", None)
                    if usage is not None and cb is not None:
                        usage["interact_stop_credits"] = int(
                            usage.get("interact_stop_credits") or 0
                        ) + int(cb)
            except Exception as se:
                print(f"[firecrawl] stop_interaction failed for {page_url}: {se}")
        return text_out

# Staff/leadership-related search terms passed to Firecrawl's map to bias
# URL discovery toward pages that are likely to contain contact information
# for the roles we care about.
_MAP_SEARCH_TERMS = (
    "staff directory leadership superintendent curriculum CTE "
    "administration department contact secondary high school"
)

# URL path segments that strongly suggest the page won't contain target
# contacts — used as a post-filter on map results.
_LOW_VALUE_SEGMENTS = {
    "board", "board_of_education", "board-of-education",
    "policy", "policies", "financial", "finance", "budget",
    "enrollment", "enroll", "calendar", "substitute", "employment", "jobs",
    "bids", "rfp", "proposals", "athletics", "sports",
    "parent", "student", "food", "nutrition", "transportation",
    "elementary", "primary", "k-5", "middle-school", "middle_school",
    "preschool", "pre-k", "news", "pub", "sitemap", "gallery", "photos",
    "new-families", "resources",
    "police", "security", "safety-security", "maintenance", "facilities",
    "facilities-construction", "construction", "purchasing", "fine-arts",
    "corporate-relations", "bond-", "legal-services", "staff-discounts",
}

# URL path extensions / filenames to exclude entirely
_EXCLUDED_EXTENSIONS = {".xml", ".pdf", ".doc", ".docx", ".xls", ".xlsx"}

# URL segments that score higher — pages likely to contain leadership or staff.
_HIGH_VALUE_SEGMENTS = {
    "leadership", "staff", "directory", "administration", "cabinet",
    "superintendent", "curriculum", "cte", "career-tech", "instruction",
    "secondary", "high-school", "academics", "department", "contact",
    "about", "our-district", "district-info",
}


def _score_url(url: str) -> int:
    """Simple heuristic score for a URL's relevance to staff/leadership."""
    lower = url.lower()
    score = 0
    for seg in _HIGH_VALUE_SEGMENTS:
        if seg in lower:
            score += 1
    for seg in _LOW_VALUE_SEGMENTS:
        if seg in lower:
            score -= 2
    return score


def _primary_host_variants(url: str) -> frozenset[str]:
    """
    Hosts treated as the same "main" site as the configured URL: bare domain
    and www only. Excludes other subdomains (e.g. gearup.district.org when
    base is www.district.org).
    """
    netloc = urlparse(url).netloc.lower()
    if not netloc:
        return frozenset()
    if netloc.startswith("www."):
        bare = netloc[4:]
        return frozenset({netloc, bare})
    return frozenset({netloc, f"www.{netloc}"})


def _url_matches_configured_site(url: str, base_url: str) -> bool:
    """True if URL hostname is the same primary site as base_url (www optional)."""
    try:
        u_host = urlparse(url).netloc.lower()
        if not u_host:
            return False
        return u_host in _primary_host_variants(base_url)
    except Exception:
        return False


def _get_async_firecrawl():
    """Return an initialised AsyncFirecrawl client (v4+ SDK)."""
    settings = get_settings()
    if not settings.firecrawl_api_key:
        raise RuntimeError("FIRECRAWL_API_KEY is not set in environment")
    from firecrawl import AsyncFirecrawl
    return AsyncFirecrawl(api_key=settings.firecrawl_api_key)


def _extract_urls_from_map(result: object) -> list[str]:
    """
    Extract URL strings from a Firecrawl map() response.

    v4 SDK returns a typed object whose .links attribute is a list of
    objects/dicts with a "url" key (not plain strings).  Older versions
    may return plain string lists or raw dicts.
    """
    def _url_from_item(item: object) -> str | None:
        if isinstance(item, str):
            return item
        # typed SDK object (e.g. LinkResult) — url may be Pydantic AnyUrl, not str
        if hasattr(item, "url") and item.url is not None:  # type: ignore[union-attr]
            return str(item.url)  # type: ignore[union-attr]
        # plain dict
        if isinstance(item, dict):
            u = item.get("url") or item.get("link") or item.get("href")
            if u is not None:
                return str(u)
        return None

    # v4 typed object with .links attribute
    if hasattr(result, "links"):
        links = result.links  # type: ignore[union-attr]
        if isinstance(links, list):
            return [u for item in links if (u := _url_from_item(item)) is not None]

    # dict shapes (older SDK or raw API response)
    if isinstance(result, dict):
        for key in ("links", "urls", "results"):
            candidates = result.get(key)
            if candidates and isinstance(candidates, list):
                return [u for item in candidates if (u := _url_from_item(item)) is not None]
        # nested under "data"
        data = result.get("data")
        if isinstance(data, list):
            return [u for item in data if (u := _url_from_item(item)) is not None]

    # plain list
    if isinstance(result, list):
        return [u for item in result if (u := _url_from_item(item)) is not None]

    return []


def _normalize_map_url_key(u: str) -> str:
    try:
        p = urlparse((u or "").strip())
        if not p.netloc:
            return (u or "").strip().rstrip("/")
        return f"{p.scheme or 'https'}://{p.netloc.lower()}{p.path}".rstrip("/")
    except Exception:
        return (u or "").strip().rstrip("/")


def _link_item_from_map_entry(item: object) -> dict | None:
    url: str | None = None
    title = ""
    description = ""
    if isinstance(item, str):
        url = item
    elif isinstance(item, dict):
        url = item.get("url") or item.get("link") or item.get("href")
        t = item.get("title")
        d = item.get("description")
        title = (str(t) if t else "")[:300]
        description = (str(d) if d else "")[:400]
    else:
        if hasattr(item, "url") and getattr(item, "url", None) is not None:
            url = str(item.url)
        if hasattr(item, "title") and getattr(item, "title", None) is not None:
            title = str(item.title)[:300]
        if hasattr(item, "description") and getattr(item, "description", None) is not None:
            description = str(item.description)[:400]
    if not url:
        return None
    return {
        "url": _normalize_map_url_key(str(url)),
        "title": title,
        "description": description,
    }


def _link_items_from_map(result: object) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    def consume_links(links: list) -> None:
        for item in links:
            row = _link_item_from_map_entry(item)
            if not row:
                continue
            key = row["url"]
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    if hasattr(result, "links"):
        links = getattr(result, "links", None)
        if isinstance(links, list):
            consume_links(links)
            return rows

    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            links = data.get("links")
            if isinstance(links, list):
                consume_links(links)
                return rows
        for key in ("links", "urls", "results"):
            candidates = result.get(key)
            if isinstance(candidates, list):
                consume_links(candidates)
                return rows
    if isinstance(result, list):
        consume_links(result)
    return rows


async def discover_map_candidates(
    base_url: str,
    usage: dict | None = None,
) -> list[dict]:
    """
    Firecrawl map with a larger limit; return link dicts with url, title, description
    for URL triage. Pre-filter obvious junk and cap list size for LLM context.
    """
    settings = get_settings()
    app = _get_async_firecrawl()
    fetch_limit = max(10, min(int(settings.batch_map_candidate_limit), 200))
    cap = max(10, min(int(settings.batch_triage_max_candidates), 80))

    try:
        result = await app.map(url=base_url, limit=fetch_limit)
        if usage is not None:
            usage["map_calls"] = int(usage.get("map_calls") or 0) + 1

        items = _link_items_from_map(result)
        filtered: list[dict] = []
        for row in items:
            u = row["url"]
            if not _url_matches_configured_site(u, base_url):
                continue
            if any(u.lower().endswith(ext) for ext in _EXCLUDED_EXTENSIONS):
                continue
            if _score_url(u) < -3:
                continue
            filtered.append(row)

        filtered.sort(key=lambda r: (-_score_url(r["url"]), len(urlparse(r["url"]).path)))
        print(
            f"[firecrawl] map candidates {len(filtered)} (from {len(items)} raw links) for {base_url}"
        )
        return filtered[:cap]

    except Exception as e:
        print(f"[firecrawl] map candidates failed for {base_url}: {type(e).__name__}: {e}")
        return []


def _extract_markdown(result: object) -> str | None:
    """
    Extract markdown text from a Firecrawl scrape() response.

    v4 SDK returns a typed object with a .markdown attribute (str).
    Older versions return a dict.
    """
    # v4 typed object
    if hasattr(result, "markdown"):
        md = result.markdown  # type: ignore[union-attr]
        if isinstance(md, str) and md.strip():
            return md

    # dict shape
    if isinstance(result, dict):
        for key in ("markdown", "content", "text"):
            val = result.get(key)
            if isinstance(val, str) and val.strip():
                return val
        data = result.get("data")
        if isinstance(data, dict):
            for key in ("markdown", "content", "text"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    return val

    if isinstance(result, str) and result.strip():
        return result

    return None


def _url_from_search_result_item(item: object) -> str | None:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        u = item.get("url") or item.get("link") or item.get("href")
        return str(u) if u is not None else None
    if hasattr(item, "url") and item.url is not None:  # type: ignore[union-attr]
        return str(item.url)  # type: ignore[union-attr]
    return None


def _extract_search_urls(result: object) -> list[str]:
    """
    Extract URLs from a Firecrawl search() response.

    v4 may return a dict, a Pydantic model, or a typed object with a .web list
    of items that have .url (often Pydantic AnyUrl, not str).
    """
    # Pydantic v2 model → plain dict
    if hasattr(result, "model_dump") and callable(result.model_dump):  # type: ignore[union-attr]
        try:
            result = result.model_dump()  # type: ignore[assignment]
        except Exception:
            pass

    # Typed object: .web list (SearchResult items)
    if hasattr(result, "web"):
        web = getattr(result, "web", None)
        if isinstance(web, list):
            return [u for x in web if (u := _url_from_search_result_item(x)) is not None]

    if isinstance(result, dict):
        web = result.get("web") or []
        if isinstance(web, list) and web:
            return [u for x in web if (u := _url_from_search_result_item(x)) is not None]
        data = result.get("data")
        if isinstance(data, dict):
            web = data.get("web") or []
            if isinstance(web, list) and web:
                return [u for x in web if (u := _url_from_search_result_item(x)) is not None]
        for key in ("data", "results", "links", "urls"):
            candidates = result.get(key)
            if isinstance(candidates, list):
                urls = [_url_from_search_result_item(item) for item in candidates]
                urls = [u for u in urls if u]
                if urls:
                    return urls
    if isinstance(result, list):
        return [u for item in result if (u := _url_from_search_result_item(item)) is not None]
    return []


async def discover_urls(
    base_url: str,
    max_urls: int | None = None,
    usage: dict | None = None,
) -> list[str]:
    """
    Use Firecrawl's map endpoint to discover relevant pages on a district website.

    Returns up to max_urls URLs ranked by relevance, filtering out low-value
    pages (board, policies, enrollment, elementary/middle-school, etc.).
    """
    settings = get_settings()
    limit = max_urls or settings.batch_max_target_pages
    fetch_limit = min(limit * 8, 50)

    app = _get_async_firecrawl()

    try:
        result = await app.map(url=base_url, limit=fetch_limit)
        if usage is not None:
            usage["map_calls"] = int(usage.get("map_calls") or 0) + 1

        urls = _extract_urls_from_map(result)
        raw_repr = repr(result)[:300]
        print(f"[firecrawl] map returned {len(urls)} URLs for {base_url} "
              f"(type={type(result).__name__}, links_attr={hasattr(result, 'links')}, raw={raw_repr})")

        scored = [(url, _score_url(url)) for url in urls if isinstance(url, str)]
        scored.sort(key=lambda x: -x[1])

        # Keep only URLs with a non-negative score; exclude known file types.
        # Within same score tier, prefer shorter (parent) paths over deep sub-pages
        # so the main department page is always selected before its children.
        # Then apply path diversity: cap each second-level branch at 2 URLs so a
        # single department can't consume all available slots.
        scored_clean = [
            (url, score) for url, score in scored
            if score >= 0
            and not any(url.lower().endswith(ext) for ext in _EXCLUDED_EXTENSIONS)
        ]
        # Secondary sort: path length ascending (shorter = parent page first)
        scored_clean.sort(key=lambda x: (-x[1], len(urlparse(x[0]).path)))

        filtered: list[str] = []
        branch_counts: dict[str, int] = {}
        for url, _score in scored_clean:
            parts = urlparse(url).path.strip("/").split("/")
            branch = "/".join(parts[:2]) if len(parts) >= 2 else parts[0] if parts else ""
            if branch_counts.get(branch, 0) >= 2:
                continue
            branch_counts[branch] = branch_counts.get(branch, 0) + 1
            filtered.append(url)

        before_host = len(filtered)
        filtered = [u for u in filtered if _url_matches_configured_site(u, base_url)]
        if before_host > len(filtered):
            print(
                f"[firecrawl] dropped {before_host - len(filtered)} map URLs not on primary host "
                f"of {base_url} (e.g. other subdomains)"
            )

        print(f"[firecrawl] {len(filtered)} URLs after filtering/dedup for {base_url}")
        return filtered[:limit]

    except Exception as e:
        print(f"[firecrawl] map failed for {base_url}: {type(e).__name__}: {e}")
        return []


def _html_to_plaintext_for_agent(html: str, max_chars: int = 80_000) -> str:
    """Strip tags for Claude when Firecrawl times out; staff lists are often in static HTML."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


async def _direct_http_scrape_fallback(url: str) -> str | None:
    """Last resort: fetch HTML and extract visible text (no JS execution)."""
    html = await _fetch_html(url, timeout=45)
    if not html or len(html) < 100:
        return None
    plain = _html_to_plaintext_for_agent(html)
    if len(plain.strip()) < 50:
        return None
    return (
        "[Content via direct HTTP fetch — Firecrawl timed out or failed; "
        "JS-only content may be missing.]\n\n"
        + plain
    )


def _is_scrape_timeout_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    if "timeout" in name or "timeout" in msg:
        return True
    return "timed out" in msg or "requesttimeout" in name


async def scrape_page(
    url: str,
    usage: dict | None = None,
    interact_priority: bool = False,
) -> str | None:
    """
    Scrape a single page via Firecrawl and return clean markdown content.
    On timeout, retries with a longer server-side timeout + short wait_for;
    if still failing, falls back to direct HTTP + plaintext extraction.

    When ``usage`` is provided, increments scrape/interact counters for Slack estimates.
    ``interact_priority``: triage-selected staff directory — allow interact when
    markdown is thin even if path heuristics do not match.
    """
    settings = get_settings()
    app = _get_async_firecrawl()
    timeout_ms = max(1_000, min(int(settings.firecrawl_scrape_timeout_ms), 300_000))
    wait_ms = max(0, int(settings.firecrawl_scrape_wait_for_ms))

    async def _do_scrape(t_ms: int, w_ms: int) -> object:
        kw: dict = {"url": url, "formats": ["markdown"], "timeout": t_ms}
        if w_ms > 0:
            kw["wait_for"] = w_ms
        return await app.scrape(**kw)

    async def _merge_interact(content: str | None, result: object) -> str | None:
        scrape_id = _extract_scrape_id(result)
        if not scrape_id or not _should_run_interact(
            url, content, settings, interact_priority=interact_priority
        ):
            return content
        print(f"[firecrawl] running interact for directory page {url}")
        block = await _run_interact_directory(app, scrape_id, settings, url, usage)
        if not block:
            return content
        return ((content or "").strip() + "\n\n" + block).strip() if content else block

    try:
        result = await _do_scrape(timeout_ms, wait_ms)
        if usage is not None:
            usage["scrape_calls"] = int(usage.get("scrape_calls") or 0) + 1
            _add_scrape_credits_from_document(usage, result)
        content = _extract_markdown(result)
        content = await _merge_interact(content, result)
        if content and len(content.strip()) >= 50:
            return content

        print(f"[firecrawl] scrape returned thin/empty content for {url} (type={type(result).__name__})")
        return content if content else None
    except Exception as e:
        print(f"[firecrawl] scrape failed for {url}: {type(e).__name__}: {e}")
        if _is_scrape_timeout_error(e):
            retry_t = min(300_000, max(timeout_ms * 2, 90_000))
            retry_w = max(wait_ms, 2_000)
            print(
                f"[firecrawl] retrying scrape with timeout={retry_t}ms wait_for={retry_w}ms for {url}"
            )
            try:
                result = await _do_scrape(retry_t, retry_w)
                if usage is not None:
                    usage["scrape_calls"] = int(usage.get("scrape_calls") or 0) + 1
                    _add_scrape_credits_from_document(usage, result)
                content = _extract_markdown(result)
                content = await _merge_interact(content, result)
                if content and len(content.strip()) >= 50:
                    return content
            except Exception as e2:
                print(f"[firecrawl] retry scrape failed for {url}: {type(e2).__name__}: {e2}")

        fb = await _direct_http_scrape_fallback(url)
        if fb:
            print(f"[firecrawl] using direct HTTP fallback for {url} ({len(fb)} chars)")
            return fb
        return None


async def scrape_pages(urls: list[str], usage: dict | None = None) -> list[dict]:
    """
    Scrape multiple pages in parallel.
    Returns list of {"url": str, "content": str} for pages that succeeded.
    """
    import asyncio
    tasks = [scrape_page(url, usage, interact_priority=False) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    pages = []
    dropped_short = 0
    for url, content in zip(urls, results):
        if isinstance(content, Exception):
            print(f"[firecrawl] scrape error for {url}: {content}")
            continue
        if content and len(content) > 30:
            pages.append({"url": url, "content": content})
        elif content:
            dropped_short += 1
    if dropped_short:
        print(f"[firecrawl] dropped {dropped_short} short pages (<31 chars)")
    return pages


async def scrape_pages_with_interact_flags(
    url_flag_pairs: list[tuple[str, bool]],
    usage: dict | None = None,
) -> list[dict]:
    """Parallel scrape with per-URL interact_priority (for directory-first path)."""
    import asyncio
    tasks = [
        scrape_page(url, usage, interact_priority=flag)
        for url, flag in url_flag_pairs
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    pages: list[dict] = []
    for (url, _), content in zip(url_flag_pairs, results):
        if isinstance(content, Exception):
            print(f"[firecrawl] scrape error for {url}: {content}")
            continue
        if content and len(content) > 30:
            pages.append({"url": url, "content": content})
    return pages


def _url_triage_plan_usable(plan: dict) -> bool:
    if not plan:
        return False
    if (plan.get("staff_directory_url") or "").strip():
        return True
    enrich = plan.get("enrichment_urls")
    if isinstance(enrich, list) and len(enrich) > 0:
        return True
    return bool(plan.get("include_homepage", True))


async def _scrape_district_triage_path(
    base_url: str,
    fc_usage: dict,
    plan: dict,
) -> list[dict]:
    """SchoolInsites API, then staff directory (interact-friendly), then enrichment + homepage."""
    settings = get_settings()
    si_text = await _fetch_schoolinsites_directory(base_url)

    dir_url = (plan.get("staff_directory_url") or "").strip()
    if dir_url and not _url_matches_configured_site(dir_url, base_url):
        print(f"[firecrawl] triage directory URL off-domain, discarding: {dir_url}")
        dir_url = ""

    enrich_raw = plan.get("enrichment_urls") or []
    if not isinstance(enrich_raw, list):
        enrich_raw = []
    enrich: list[str] = []
    seen_e: set[str] = set()
    cap = max(0, int(settings.batch_enrichment_url_cap))
    for u in enrich_raw:
        if len(enrich) >= cap:
            break
        s = str(u).strip()
        if not s:
            continue
        key = _normalize_map_url_key(s)
        if key in seen_e:
            continue
        seen_e.add(key)
        if not _url_matches_configured_site(s, base_url):
            continue
        if s.rstrip("/") == base_url.rstrip("/"):
            continue
        if dir_url and s.rstrip("/") == dir_url.rstrip("/"):
            continue
        enrich.append(_normalize_map_url_key(s))

    include_home = bool(plan.get("include_homepage", True))
    base_norm = _normalize_map_url_key(base_url)
    dir_is_home = bool(dir_url and _normalize_map_url_key(dir_url) == base_norm)

    dir_page: dict | None = None
    if dir_url:
        content = await scrape_page(dir_url, fc_usage, interact_priority=True)
        if content and len(content.strip()) > 30:
            dir_page = {"url": dir_url, "content": content}

    rest_specs: list[tuple[str, bool]] = []
    if include_home and not dir_is_home:
        rest_specs.append((base_url, False))
    rest_specs.extend([(u, False) for u in enrich])

    max_total = max(1, int(settings.batch_max_scrape_urls))
    used_dir = 1 if dir_page else 0
    rest_cap = max(0, max_total - used_dir)
    rest_specs = rest_specs[:rest_cap]

    rest_pages = await scrape_pages_with_interact_flags(rest_specs, fc_usage)
    home_pages = [
        p for p in rest_pages
        if _normalize_map_url_key(p.get("url") or "") == _normalize_map_url_key(base_url)
    ]
    other_pages = [
        p for p in rest_pages
        if _normalize_map_url_key(p.get("url") or "") != _normalize_map_url_key(base_url)
    ]

    assembled: list[dict] = []
    if si_text:
        assembled.append({"url": f"{base_url} [SchoolInsites API]", "content": si_text})
    if dir_page:
        assembled.append(dir_page)
    assembled.extend(other_pages)
    assembled.extend(home_pages)
    return assembled


async def _scrape_district_heuristic_fallback(base_url: str, fc_usage: dict) -> list[dict]:
    """Original map → score → parallel scrape pipeline when triage is skipped or fails."""
    settings = get_settings()
    subpage_urls = await discover_urls(
        base_url,
        max_urls=settings.batch_max_target_pages,
        usage=fc_usage,
    )
    all_urls = [base_url] + [
        u for u in subpage_urls
        if u.rstrip("/") != base_url.rstrip("/") and _url_matches_configured_site(u, base_url)
    ]
    pages = await scrape_pages(all_urls, fc_usage)
    print(f"[firecrawl] Scraped {len(pages)} pages for {base_url} (heuristic)")

    if not subpage_urls:
        print(f"[firecrawl] map found no subpages — using HTML fallback for subpage discovery on {base_url}")
        homepage_html = await _fetch_html(base_url)

        if homepage_html and len(homepage_html) > 100:
            discovered = _discover_subpage_urls_from_html(
                homepage_html, base_url, max_urls=settings.batch_max_target_pages
            )
            print(f"[firecrawl] HTML fallback discovered {len(discovered)} candidate subpage URLs for {base_url}")

            if discovered:
                fc_subpages = await scrape_pages(discovered, fc_usage)
                if fc_subpages:
                    print(f"[firecrawl] Firecrawl scraped {len(fc_subpages)} discovered subpages for {base_url}")
                    pages.extend(fc_subpages)
                else:
                    direct_subpages = await _fetch_pages_direct(discovered)
                    print(f"[firecrawl] direct HTTP fetched {len(direct_subpages)} subpages for {base_url}")
                    pages.extend(direct_subpages)

        if not pages:
            homepage_html = homepage_html or await _fetch_html(base_url)
            if homepage_html and len(homepage_html) > 100:
                pages.append({"url": f"{base_url} [direct_fallback]", "content": homepage_html})

    si_text = await _fetch_schoolinsites_directory(base_url)
    if si_text:
        pages.insert(0, {"url": f"{base_url} [SchoolInsites API]", "content": si_text})

    return pages


async def _fetch_schoolinsites_directory(base_url: str) -> str:
    """
    Attempt to detect and call the SchoolInsites /sys/api/directory JSON API
    directly from the homepage HTML. Returns formatted text or empty string.

    SchoolInsites renders staff entirely via JS; Firecrawl's markdown output
    will be near-empty for these widgets. Calling the API directly gets clean
    structured data that is far more useful for Claude.
    """
    try:
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(base_url, headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            })
            resp.raise_for_status()
            raw_html = resp.text

        soup = BeautifulSoup(raw_html, "html.parser")
        widgets = soup.find_all(attrs={"data-module": "widgets/directory"})
        if not widgets:
            return ""

        all_lines: list[str] = []
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            for widget in widgets:
                widget_id = widget.get("data-widget-id")
                page_size = widget.get("data-page-size", "500")
                if not widget_id:
                    continue
                api_url = (
                    f"{origin}/sys/api/directory"
                    f"?widgetId={widget_id}&viewId=directory-photo"
                    f"&pageNum=1&pageSize={page_size}"
                )
                try:
                    r = await client.get(api_url)
                    r.raise_for_status()
                    data = r.json()
                except Exception as e:
                    print(f"[firecrawl] SchoolInsites API failed for widget {widget_id}: {e}")
                    continue

                if not isinstance(data, list):
                    continue

                print(f"[firecrawl] SchoolInsites API returned {len(data)} entries")
                for person in data:
                    name = person.get("NameFirstLast") or ""
                    title = person.get("JobTitle") or ""
                    dept = person.get("Department") or ""
                    phone = person.get("PhoneNumber") or ""
                    email = person.get("EmailAddress") or ""
                    if not name:
                        continue
                    line = name
                    if title:
                        line += f" — {title}"
                    if dept:
                        line += f" ({dept})"
                    if email:
                        line += f" [EMAIL: {email}]"
                    if phone:
                        line += f" [PHONE: {phone}]"
                    all_lines.append(line)

        if not all_lines:
            return ""
        return "## Staff Directory (via SchoolInsites API)\n" + "\n".join(all_lines)

    except Exception as e:
        print(f"[firecrawl] SchoolInsites check failed for {base_url}: {e}")
        return ""


# Domains that are known directories or aggregators — never use these as
# the "official" district website even if they appear first in search results.
_DIRECTORY_DOMAINS = {
    "greatschools.org", "niche.com", "schooldigger.com", "nces.ed.gov",
    "publicschoolreview.com", "usnews.com", "education.com",
    "wikipedia.org", "google.com", "facebook.com", "twitter.com",
    "linkedin.com", "yelp.com", "mapquest.com", "yellowpages.com",
    "homefacts.com", "city-data.com", "har.com", "zillow.com",
}

# TLD / domain patterns that strongly suggest an official district site.
_OFFICIAL_DOMAIN_HINTS = (
    ".k12.", ".edu", ".org", ".us", ".net",
)


def _looks_like_official_website(url: str) -> bool:
    """
    Return True if the URL looks like an official school district website
    rather than a directory, aggregator, or social media page.
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().lstrip("www.")
        if any(d in domain for d in _DIRECTORY_DOMAINS):
            return False
        if any(hint in domain for hint in _OFFICIAL_DOMAIN_HINTS):
            return True
        return True
    except Exception:
        return False


async def discover_district_website(
    district_name: str,
    usage: dict | None = None,
) -> str | None:
    """
    Use Firecrawl search to find the official website for a school district
    when no URL is stored in Pipedrive or the districts table.

    Returns the first result URL that looks like an official domain, or None.
    """
    app = _get_async_firecrawl()
    queries = (
        f'"{district_name}" official school district website',
        f"{district_name} school district official website",
        f"{district_name} USD homepage",
    )

    try:
        for query in queries:
            result = await app.search(query=query, limit=8)
            if usage is not None:
                usage["search_calls"] = int(usage.get("search_calls") or 0) + 1
            urls = _extract_search_urls(result)
            if not urls:
                print(
                    f"[firecrawl] search returned 0 URLs for query={query!r} "
                    f"(type={type(result).__name__}, repr={repr(result)[:400]})"
                )
                continue

            for url in urls:
                if url and _looks_like_official_website(url):
                    parsed = urlparse(url)
                    homepage = f"{parsed.scheme}://{parsed.netloc}"
                    print(f"[firecrawl] Discovered website for '{district_name}': {homepage}")
                    return homepage

            print(
                f"[firecrawl] search had {len(urls)} URLs but none passed official-domain filter "
                f"for '{district_name}' (first: {urls[0] if urls else None})"
            )

        print(f"[firecrawl] Could not find official website for '{district_name}'")
        return None

    except Exception as e:
        print(f"[firecrawl] Website discovery failed for '{district_name}': {type(e).__name__}: {e}")
        return None


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


async def _fetch_html(url: str, timeout: int = 15) -> str | None:
    """Fetch raw HTML from a URL using a browser user-agent."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": _BROWSER_UA})
            resp.raise_for_status()
            return resp.text or None
    except Exception as e:
        print(f"[firecrawl] direct fetch failed for {url}: {type(e).__name__}: {e}")
        return None


def _discover_subpage_urls_from_html(raw_html: str, base_url: str, max_urls: int = 10) -> list[str]:
    """
    Parse homepage HTML for internal links and return the top candidates
    likely to contain staff/leadership contact info. Used as fallback when
    Firecrawl map returns nothing.
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    base_domain = urlparse(base_url).netloc
    links: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if base_domain in parsed.netloc or parsed.netloc in base_domain:
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
            links.add(clean_url)

    scored = [(url, _score_url(url)) for url in links]
    scored.sort(key=lambda x: -x[1])
    filtered = [url for url, score in scored if score >= 0]
    return filtered[:max_urls]


async def _fetch_pages_direct(urls: list[str]) -> list[dict]:
    """Fetch multiple pages via direct httpx and return content dicts."""
    import asyncio
    tasks = [_fetch_html(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    pages = []
    for url, content in zip(urls, results):
        if isinstance(content, Exception):
            continue
        if content and len(content) > 100:
            pages.append({"url": url, "content": content})
    return pages


async def scrape_district(
    base_url: str,
    usage: dict | None = None,
    *,
    district_name: str | None = None,
    district_state: str | None = None,
    batch_agent: BatchExtractionAgent | None = None,
) -> tuple[list[dict], dict, dict | None]:
    """
    Directory-first pipeline when ``district_name`` and ``batch_agent`` are set:
    map (rich metadata) → cheap LLM URL triage → SchoolInsites API + prioritized
    directory scrape (interact-friendly) → enrichment URLs + optional homepage.

    Otherwise uses the legacy heuristic map/score pipeline (same as triage fallback).

    Returns ``(pages, firecrawl_usage, url_triage_meta)``. ``url_triage_meta`` is
    None only if triage was skipped and no summary dict was produced; usually a
    dict with ``used_heuristic``, ``staff_directory_url``, ``enrichment_urls``,
    ``triage_usage``, and ``rationale``.
    """
    settings = get_settings()
    fc_usage = usage if usage is not None else new_firecrawl_usage()
    print(f"[firecrawl] Starting scrape for {base_url}")

    triage_meta: dict | None = None

    def _fill_triage_tokens(meta: dict | None) -> None:
        if not meta:
            return
        tu = meta.get("triage_usage")
        if isinstance(tu, dict):
            meta["triage_in_tokens"] = int(tu.get("input_tokens") or 0)
            meta["triage_out_tokens"] = int(tu.get("output_tokens") or 0)

    name_ok = bool((district_name or "").strip())
    use_triage = (
        batch_agent is not None
        and name_ok
        and bool(settings.anthropic_api_key)
    )

    if use_triage:
        link_items = await discover_map_candidates(base_url, usage=fc_usage)
        if link_items:
            plan, tri_usage = batch_agent.triage_urls_from_map(
                district_name=(district_name or "").strip(),
                district_state=(district_state or "").strip() or None,
                homepage_url=base_url,
                candidates=link_items,
            )
            triage_meta = {
                "used_heuristic": False,
                "staff_directory_url": plan.get("staff_directory_url"),
                "enrichment_urls": list(plan.get("enrichment_urls") or []),
                "include_homepage": bool(plan.get("include_homepage", True)),
                "rationale": (plan.get("rationale") or "")[:500],
                "triage_usage": tri_usage,
            }
            if _url_triage_plan_usable(plan):
                rationale_preview = (plan.get("rationale") or "")[:120]
                print(
                    f"[firecrawl] URL triage: dir={plan.get('staff_directory_url')!r} "
                    f"enrich={len(plan.get('enrichment_urls') or [])} "
                    f"home={plan.get('include_homepage', True)} "
                    f"rationale={rationale_preview!r}"
                )
                pages = await _scrape_district_triage_path(base_url, fc_usage, plan)
                if pages:
                    _fill_triage_tokens(triage_meta)
                    return pages, fc_usage, triage_meta
                print("[firecrawl] Triage path returned no pages — heuristic fallback")
            triage_meta["used_heuristic"] = True
            print("[firecrawl] URL triage plan unusable or empty scrape — heuristic fallback")
        else:
            triage_meta = {
                "used_heuristic": True,
                "staff_directory_url": None,
                "enrichment_urls": [],
                "include_homepage": True,
                "rationale": "no map candidates for triage",
                "triage_usage": {},
            }
            print("[firecrawl] No map candidates — heuristic fallback")

    pages = await _scrape_district_heuristic_fallback(base_url, fc_usage)
    if triage_meta is None:
        triage_meta = {
            "used_heuristic": True,
            "staff_directory_url": None,
            "enrichment_urls": [],
            "include_homepage": True,
            "rationale": "triage skipped (no district name, agent, or Anthropic key)",
            "triage_usage": {},
        }
    _fill_triage_tokens(triage_meta)
    return pages, fc_usage, triage_meta
