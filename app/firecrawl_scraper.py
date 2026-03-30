from __future__ import annotations
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from app.config import get_settings

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


async def discover_urls(base_url: str, max_urls: int | None = None) -> list[str]:
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

        print(f"[firecrawl] {len(filtered)} URLs after filtering/dedup for {base_url}")
        return filtered[:limit]

    except Exception as e:
        print(f"[firecrawl] map failed for {base_url}: {type(e).__name__}: {e}")
        return []


async def scrape_page(url: str) -> str | None:
    """
    Scrape a single page via Firecrawl and return clean markdown content.
    Returns None on failure.
    """
    app = _get_async_firecrawl()
    try:
        result = await app.scrape(url=url, formats=["markdown"])
        content = _extract_markdown(result)
        if content and len(content.strip()) >= 50:
            return content

        print(f"[firecrawl] scrape returned thin/empty content for {url} (type={type(result).__name__})")
        return content if content else None
    except Exception as e:
        print(f"[firecrawl] scrape failed for {url}: {type(e).__name__}: {e}")
        return None


async def scrape_pages(urls: list[str]) -> list[dict]:
    """
    Scrape multiple pages in parallel.
    Returns list of {"url": str, "content": str} for pages that succeeded.
    """
    import asyncio
    tasks = [scrape_page(url) for url in urls]
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


async def discover_district_website(district_name: str) -> str | None:
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


async def scrape_district(base_url: str) -> list[dict]:
    """
    Full pipeline for a single district:
    1. Use Firecrawl map to discover relevant subpage URLs.
    2. Scrape homepage + top subpages in parallel via Firecrawl.
    3. If map found no subpages, fall back to HTML-based link discovery and
       scrape those pages (Firecrawl first, direct HTTP if that also fails).
    4. Supplement with SchoolInsites direct API if detected.

    Returns a list of {"url": str, "content": str} dicts ready to pass
    to the batch extraction agent.
    """
    settings = get_settings()
    print(f"[firecrawl] Starting scrape for {base_url}")

    # Step 1: discover target URLs via Firecrawl map
    subpage_urls = await discover_urls(base_url, max_urls=settings.batch_max_target_pages)

    # Always include homepage; deduplicate
    all_urls = [base_url] + [u for u in subpage_urls if u.rstrip("/") != base_url.rstrip("/")]

    # Step 2: scrape all pages in parallel via Firecrawl
    pages = await scrape_pages(all_urls)
    print(f"[firecrawl] Scraped {len(pages)} pages for {base_url}")

    # Step 3: if Firecrawl map found no subpages (or total pages still < 2),
    # use direct HTML to discover subpages and fetch them. This runs even if the
    # homepage was scraped — we still need subpages to find contacts.
    if not subpage_urls:
        print(f"[firecrawl] map found no subpages — using HTML fallback for subpage discovery on {base_url}")
        homepage_html = await _fetch_html(base_url)

        if homepage_html and len(homepage_html) > 100:
            discovered = _discover_subpage_urls_from_html(
                homepage_html, base_url, max_urls=settings.batch_max_target_pages
            )
            print(f"[firecrawl] HTML fallback discovered {len(discovered)} candidate subpage URLs for {base_url}")

            if discovered:
                fc_subpages = await scrape_pages(discovered)
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

    # Step 4: SchoolInsites supplement (check homepage)
    si_text = await _fetch_schoolinsites_directory(base_url)
    if si_text:
        pages.insert(0, {"url": f"{base_url} [SchoolInsites API]", "content": si_text})

    return pages
