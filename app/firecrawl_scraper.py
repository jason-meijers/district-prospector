from __future__ import annotations
import asyncio
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from firecrawl import FirecrawlApp
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


def _get_firecrawl() -> FirecrawlApp:
    settings = get_settings()
    if not settings.firecrawl_api_key:
        raise RuntimeError("FIRECRAWL_API_KEY is not set in environment")
    return FirecrawlApp(api_key=settings.firecrawl_api_key)


async def discover_urls(base_url: str, max_urls: int | None = None) -> list[str]:
    """
    Use Firecrawl's map endpoint with a staff-focused search term to discover
    the most relevant pages on a district website.

    Returns up to max_urls URLs ranked by relevance, filtering out low-value
    pages (board, policies, enrollment, elementary/middle-school, etc.).
    """
    settings = get_settings()
    limit = max_urls or settings.batch_max_target_pages

    app = _get_firecrawl()

    try:
        # Run synchronous Firecrawl SDK call in a thread pool so it doesn't
        # block the async event loop.
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: app.map_url(
                base_url,
                params={
                    "search": _MAP_SEARCH_TERMS,
                    "limit": min(limit * 8, 50),  # fetch extra, then filter
                }
            )
        )

        # Firecrawl returns a list of URLs directly or wrapped in a dict
        if isinstance(result, dict):
            urls = result.get("links") or result.get("urls") or []
        elif isinstance(result, list):
            urls = result
        else:
            urls = []

        # Filter and score
        scored = [(url, _score_url(url)) for url in urls if isinstance(url, str)]
        scored.sort(key=lambda x: -x[1])

        # Take the top N, dropping anything with a very negative score
        filtered = [url for url, score in scored if score >= -1]
        print(f"[firecrawl] map returned {len(urls)} URLs, {len(filtered)} after filtering for {base_url}")
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
            lambda: app.scrape_url(
                url,
                params={
                    "formats": ["markdown"],
                    "onlyMainContent": True,
                }
            )
        )
        if isinstance(result, dict):
            return result.get("markdown") or result.get("content") or None
        return str(result) if result else None
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
    for url, content in zip(urls, results):
        if isinstance(content, Exception):
            print(f"[firecrawl] scrape error for {url}: {content}")
            continue
        if content and len(content) > 100:
            pages.append({"url": url, "content": content})
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
            lambda: app.search(query, params={"limit": 5})
        )

        # Firecrawl search returns a list of result dicts with a "url" key
        results = []
        if isinstance(result, dict):
            results = result.get("data") or result.get("results") or []
        elif isinstance(result, list):
            results = result

        for item in results:
            url = None
            if isinstance(item, dict):
                url = item.get("url") or item.get("link")
            elif isinstance(item, str):
                url = item

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


async def scrape_district(base_url: str) -> list[dict]:
    """
    Full pipeline for a single district:
    1. Use Firecrawl map to discover relevant subpage URLs.
    2. Scrape homepage + top subpages in parallel via Firecrawl.
    3. Supplement with SchoolInsites direct API if detected.

    Returns a list of {"url": str, "content": str} dicts ready to pass
    to the batch extraction agent.
    """
    settings = get_settings()
    print(f"[firecrawl] Starting scrape for {base_url}")

    # Step 1: discover target URLs
    subpage_urls = await discover_urls(base_url, max_urls=settings.batch_max_target_pages)

    # Always include homepage; deduplicate
    all_urls = [base_url] + [u for u in subpage_urls if u.rstrip("/") != base_url.rstrip("/")]

    # Step 2: scrape all pages in parallel
    pages = await scrape_pages(all_urls)
    print(f"[firecrawl] Scraped {len(pages)} pages for {base_url}")

    # Step 3: SchoolInsites supplement (check homepage)
    si_text = await _fetch_schoolinsites_directory(base_url)
    if si_text:
        # Prepend as a synthetic page so Claude sees it first
        pages.insert(0, {"url": f"{base_url} [SchoolInsites API]", "content": si_text})

    return pages
