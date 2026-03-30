from __future__ import annotations
import asyncio
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
    "enrollment", "calendar", "substitute", "employment", "jobs",
    "bids", "rfp", "proposals", "athletics", "sports",
    "parent", "student", "food", "nutrition", "transportation",
    "elementary", "primary", "k-5", "middle-school", "middle_school",
    "preschool", "pre-k",
}

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


def _get_firecrawl():
    """Return an initialised Firecrawl client (v4+ Firecrawl class)."""
    settings = get_settings()
    if not settings.firecrawl_api_key:
        raise RuntimeError("FIRECRAWL_API_KEY is not set in environment")
    from firecrawl import Firecrawl
    return Firecrawl(api_key=settings.firecrawl_api_key)


def _extract_urls_from_map(result: object) -> list[str]:
    """
    Extract URL strings from a Firecrawl map() response.

    v4 SDK returns a typed object with a .links attribute (list[str]).
    Older versions return a dict or plain list.
    """
    # v4 typed object
    if hasattr(result, "links"):
        links = result.links  # type: ignore[union-attr]
        if isinstance(links, list):
            return [u for u in links if isinstance(u, str)]

    # dict shapes
    if isinstance(result, dict):
        for key in ("links", "urls", "results"):
            candidates = result.get(key)
            if candidates:
                break
        else:
            candidates = (result.get("data") or []) if isinstance(result.get("data"), list) else []
        urls = []
        for item in (candidates or []):
            if isinstance(item, str):
                urls.append(item)
            elif isinstance(item, dict):
                u = item.get("url") or item.get("link") or item.get("href")
                if isinstance(u, str):
                    urls.append(u)
        return urls

    # plain list
    if isinstance(result, list):
        return [u for u in result if isinstance(u, str)]

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


def _extract_search_urls(result: object) -> list[str]:
    """
    Extract URLs from a Firecrawl search() response.

    v4 returns dict with result.get("web") as list[dict] each with a "url" key.
    """
    if isinstance(result, dict):
        # v4 shape
        web = result.get("web") or []
        if web:
            return [item.get("url") for item in web if isinstance(item, dict) and item.get("url")]
        # fallback keys
        for key in ("data", "results", "links", "urls"):
            candidates = result.get(key)
            if isinstance(candidates, list):
                urls = []
                for item in candidates:
                    if isinstance(item, str):
                        urls.append(item)
                    elif isinstance(item, dict):
                        u = item.get("url") or item.get("link") or item.get("href")
                        if isinstance(u, str):
                            urls.append(u)
                if urls:
                    return urls
    if isinstance(result, list):
        urls = []
        for item in result:
            if isinstance(item, str):
                urls.append(item)
            elif isinstance(item, dict):
                u = item.get("url") or item.get("link")
                if isinstance(u, str):
                    urls.append(u)
        return urls
    return []


async def discover_urls(base_url: str, max_urls: int | None = None) -> list[str]:
    """
    Use Firecrawl's map endpoint with a staff-focused search term to discover
    the most relevant pages on a district website.

    Returns up to max_urls URLs ranked by relevance, filtering out low-value
    pages (board, policies, enrollment, elementary/middle-school, etc.).
    """
    settings = get_settings()
    limit = max_urls or settings.batch_max_target_pages
    fetch_limit = min(limit * 8, 50)

    app = _get_firecrawl()

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: app.map(url=base_url, limit=fetch_limit)
        )

        urls = _extract_urls_from_map(result)
        print(f"[firecrawl] map returned {len(urls)} URLs for {base_url} (type={type(result).__name__}, links_attr={hasattr(result, 'links')})")

        # Filter and score
        scored = [(url, _score_url(url)) for url in urls if isinstance(url, str)]
        scored.sort(key=lambda x: -x[1])

        filtered = [url for url, score in scored if score >= -1]
        print(f"[firecrawl] {len(filtered)} URLs after filtering for {base_url}")
        return filtered[:limit]

    except Exception as e:
        print(f"[firecrawl] map failed for {base_url}: {type(e).__name__}: {e}")
        return []


async def scrape_page(url: str) -> str | None:
    """
    Scrape a single page via Firecrawl and return clean markdown content.
    Returns None on failure.
    """
    app = _get_firecrawl()
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: app.scrape(url=url, formats=["markdown"])
        )
        content = _extract_markdown(result)
        if content and len(content.strip()) >= 50:
            return content

        # If content is empty or very thin, log and return whatever we got.
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
        # Reject known directories
        if any(d in domain for d in _DIRECTORY_DOMAINS):
            return False
        # Prefer official-looking TLDs/patterns
        if any(hint in domain for hint in _OFFICIAL_DOMAIN_HINTS):
            return True
        # Accept .com only as a last resort — many smaller districts use it
        return True
    except Exception:
        return False


async def discover_district_website(district_name: str) -> str | None:
    """
    Use Firecrawl search to find the official website for a school district
    when no URL is stored in Pipedrive or the districts table.

    Searches for "[district name] official school district website" and
    returns the first result URL that looks like an official domain.
    Returns None if nothing suitable is found.

    Uses Firecrawl search credits — much cheaper than a Claude web search call.
    """
    app = _get_firecrawl()
    query = f'"{district_name}" official school district website'

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: app.search(query=query, limit=5)
        )

        for url in _extract_search_urls(result):
            if url and _looks_like_official_website(url):
                parsed = urlparse(url)
                # Return just the homepage (scheme + netloc), not a deep subpage
                homepage = f"{parsed.scheme}://{parsed.netloc}"
                print(f"[firecrawl] Discovered website for '{district_name}': {homepage}")
                return homepage

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
    3. If Firecrawl returned nothing, fall back to direct HTML fetching
       with link-based subpage discovery from the homepage.
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

    # Step 3: if Firecrawl produced nothing, fall back to direct HTML fetching
    if not pages:
        print(f"[firecrawl] Firecrawl returned 0 pages — falling back to direct HTML fetch for {base_url}")
        homepage_html = await _fetch_html(base_url)

        if homepage_html and len(homepage_html) > 100:
            # Discover subpage URLs from homepage links
            discovered = _discover_subpage_urls_from_html(
                homepage_html, base_url, max_urls=settings.batch_max_target_pages
            )
            print(f"[firecrawl] HTML fallback discovered {len(discovered)} candidate subpage URLs for {base_url}")

            # Fetch subpages directly
            if discovered:
                subpages = await _fetch_pages_direct(discovered)
                pages.extend(subpages)
                print(f"[firecrawl] HTML fallback fetched {len(subpages)} subpages for {base_url}")

            # Also include homepage itself
            pages.append({"url": f"{base_url} [direct_fallback]", "content": homepage_html})

    # Step 4: SchoolInsites supplement (check homepage)
    si_text = await _fetch_schoolinsites_directory(base_url)
    if si_text:
        pages.insert(0, {"url": f"{base_url} [SchoolInsites API]", "content": si_text})

    return pages
