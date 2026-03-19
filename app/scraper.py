from __future__ import annotations
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import re


# Tags and class/id patterns to strip from HTML
REMOVE_TAGS = {"script", "style", "nav", "footer", "noscript", "iframe", "svg"}
# Be careful not to strip sidebars entirely — many districts put key contacts
# (e.g. superintendent or curriculum director) in a right-hand sidebar.
REMOVE_PATTERNS = re.compile(
    r"(menu|nav|cookie|banner|popup|modal|advertisement|social-share|breadcrumb)",
    re.IGNORECASE,
)

# Common district site platforms put staff in these URL patterns
STAFF_URL_HINTS = [
    "leadership", "staff", "directory", "team", "about", "contact",
    "department", "administration", "superintendent", "cabinet",
    "educational-services", "ed-services", "instruction",
    "career-tech", "cte", "academics", "programs", "curriculum",
    "district",  # e.g. our_district, our-district — often has superintendent welcome
]
# Path segments that suggest the page is NOT staff/leadership (penalize score)
LOW_VALUE_PATH_PARTS = {
    "policies", "policy", "financial", "reports", "enrollment", "substitute",
    "calendar", "payments", "proposals", "rfp", "lead", "bids", "employment",
    "board_of_education", "board",
    "parent", "athletics", "bully", "bullying", "food", "finance",
    "physical", "english", "forms", "final-forms",
}

# Query param names that often indicate pagination
PAGINATION_PARAMS = {"page", "p", "start", "offset", "pg"}

# US phone number pattern — matches (614) 555-1234, 614-555-1234, 614.555.1234, etc.
# Uses (?<![.\d]) instead of \b so the opening parenthesis in (614) is included.
_PHONE_RE = re.compile(
    r'(?<![.\d])(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}(?!\d)'
)


def _annotate_phone_numbers(text: str) -> str:
    """
    Annotate bare phone numbers with [PHONE: ...] markers so Claude can
    reliably extract them even when the site uses plain text instead of tel: links.
    Skips numbers that are already inside a [PHONE: ...] annotation.
    """
    result: list[str] = []
    last = 0
    for m in _PHONE_RE.finditer(text):
        # Skip if already annotated
        preceding = text[max(0, m.start() - 15): m.start()]
        if "PHONE:" in preceding:
            result.append(text[last: m.end()])
            last = m.end()
            continue
        result.append(text[last: m.start()])
        result.append(f"{m.group(0)} [PHONE: {m.group(0)}]")
        last = m.end()
    result.append(text[last:])
    return "".join(result)


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


async def fetch_page(url: str, timeout: int = 15) -> str | None:
    """
    Fetch a page and return raw HTML, or None on failure.

    Retry strategy:
    1. Normal browser UA.
    2. If HTTP and the server disconnects, retry with HTTPS.
    3. If the page loads but is nearly empty (JS-rendered shell), retry with a
       crawler UA — many platforms (e.g. SchoolBlocks) serve pre-rendered HTML
       to bots/crawlers for SEO even though browsers get a JS shell.
    """
    async def _get(target_url: str, ua: str) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                resp = await client.get(target_url, headers={"User-Agent": ua})
                resp.raise_for_status()
                text = resp.text or ""
                print(
                    f"[scraper] Fetched {target_url} "
                    f"(final={resp.url}, status={resp.status_code}, bytes={len(text)})"
                )
                return text
        except httpx.TimeoutException as e:
            print(
                f"[scraper] Timeout fetching {target_url} "
                f"(ua={ua[:32]}..., timeout={timeout}s): {type(e).__name__}: {e}"
            )
            return None
        except httpx.HTTPStatusError as e:
            resp = e.response
            print(
                f"[scraper] HTTP error fetching {target_url} "
                f"(final={resp.url}, status={resp.status_code}, ua={ua[:32]}...): {e}"
            )
            return None
        except httpx.RequestError as e:
            print(
                f"[scraper] Request error fetching {target_url} "
                f"(ua={ua[:32]}...): {type(e).__name__}: {e}"
            )
            return None
        except Exception as e:
            print(
                f"[scraper] Unexpected error fetching {target_url}: "
                f"{type(e).__name__}: {e}"
            )
            return None

    html = await _get(url, _BROWSER_UA)

    # HTTPS upgrade retry for plain-HTTP URLs that drop connections
    if html is None and url.startswith("http://"):
        https_url = "https://" + url[len("http://"):]
        print(f"[scraper] Retrying over HTTPS: {https_url}")
        html = await _get(https_url, _BROWSER_UA)
        if html is None:
            print(f"[scraper] HTTPS retry also failed for {https_url}")
            return None

    return html


def _extract_ssr_content(soup: BeautifulSoup) -> str:
    """
    Extract page content embedded inside SSR data blocks used by modern
    JS frameworks (Nuxt, Next.js, etc.).  Many school-district website
    platforms (Apptegy / Thrillshare, Finalsite, etc.) deliver all visible
    text this way — the <body> itself is almost empty until JS hydrates.

    We pull HTML fragments from these data blocks and parse them into
    clean text with email / phone annotations so that nothing is lost
    when we later strip <script> tags.
    """
    fragments: list[str] = []

    # ── Nuxt 3 payload (<script id="__NUXT_DATA__">) ────────────
    nuxt_data_tag = soup.find("script", id="__NUXT_DATA__")
    if nuxt_data_tag and nuxt_data_tag.string:
        _collect_html_fragments(nuxt_data_tag.string, fragments)

    # ── Nuxt 2 payload (window.__NUXT__=…) ──────────────────────
    for script in soup.find_all("script"):
        text = script.string or ""
        if "__NUXT__" in text or "window.__NUXT__" in text:
            _collect_html_fragments(text, fragments)

    # ── Next.js payload (<script id="__NEXT_DATA__">) ───────────
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag and next_data_tag.string:
        _collect_html_fragments(next_data_tag.string, fragments)

    if not fragments:
        return ""

    # Parse each HTML fragment and extract text + link annotations
    lines: list[str] = []
    for frag in fragments:
        frag_soup = BeautifulSoup(frag, "html.parser")
        _extract_text_lines(frag_soup, lines)

    # Deduplicate adjacent identical lines
    deduped: list[str] = []
    for line in lines:
        if not deduped or line != deduped[-1]:
            deduped.append(line)

    return "\n".join(deduped)


# Regex to find HTML-like strings inside script data blocks.  Matches
# values that contain at least one HTML tag and are long enough to be
# meaningful content (not just a stray <br>).
_HTML_FRAGMENT_RE = re.compile(
    r'(?:<[a-zA-Z][^>]*>.*?</[a-zA-Z]+>)',
    re.DOTALL,
)


def _collect_html_fragments(raw: str, out: list[str]) -> None:
    """
    Scan *raw* (the text content of a <script> block) for embedded HTML
    fragments and append them to *out*.  Works with both JSON-encoded
    strings (\\u003C … \\u003E) and raw angle brackets.
    """
    # Unescape common JSON unicode escapes so regex can match tags
    text = raw.replace("\\u003C", "<").replace("\\u003E", ">")
    text = text.replace("\\u0026", "&")
    # Also unescape \" to " for strings inside JSON
    text = text.replace('\\"', '"')

    for m in _HTML_FRAGMENT_RE.finditer(text):
        fragment = m.group(0)
        # Only keep fragments that look like real content (have text).
        # Threshold is low (5) so short-but-meaningful content like phone
        # numbers that trail a <br> tag inside a <p> aren't discarded.
        stripped = re.sub(r"<[^>]+>", "", fragment).strip()
        if len(stripped) > 4:
            out.append(fragment)


_RECOGNIZED_ELEMENTS = frozenset([
    "p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th",
    "span", "div", "a", "strong", "b", "em",
])


def _extract_text_lines(soup: BeautifulSoup, lines: list[str]) -> None:
    """
    Walk *soup* and append cleaned text lines (with email / phone
    annotations) to *lines*.  Shared helper used by both the SSR
    extractor and the main clean_html path.
    """
    for el in soup.find_all(list(_RECOGNIZED_ELEMENTS)):
        text = el.get_text(separator=" ", strip=True)
        if not text or len(text) < 2:
            continue

        attrs = el.attrs if el.attrs is not None else {}
        if el.name == "a" and attrs.get("href"):
            href = attrs.get("href", "")
            if href.startswith("mailto:"):
                text = f"{text} [EMAIL: {href.replace('mailto:', '')}]"
            elif href.startswith("tel:"):
                text = f"{text} [PHONE: {href.replace('tel:', '')}]"
            elif not href.startswith("#"):
                text = f"{text} [LINK: {href}]"

        lines.append(text)

    # Also walk bare string nodes whose parent is not a recognized element.
    # This catches text like phone numbers that appear after a <br> tag
    # as direct text content of a <p>, which the element loop above misses
    # because the non-greedy SSR fragment regex splits <p> into sub-fragments.
    for string_node in soup.strings:
        text = string_node.strip()
        if not text or len(text) < 2:
            continue
        parent_name = getattr(string_node.parent, "name", None)
        if parent_name not in _RECOGNIZED_ELEMENTS:
            # Skip raw JSON/code artifacts (e.g. SSR data bleed-through)
            if text.startswith(("{", "[", '"', "',")) or "}," in text or '":"' in text:
                continue
            lines.append(text)


def clean_html(raw_html: str) -> str:
    """
    Strip boilerplate from HTML and return cleaned text with link annotations.
    Handles both traditional server-rendered pages AND modern JS-framework
    sites (Nuxt, Next.js, Apptegy, Finalsite) that embed content in <script>
    data blocks.
    """
    soup = BeautifulSoup(raw_html, "html.parser")

    # ── Phase 0: SchoolBlocks person block extraction ────────────
    # SchoolBlocks sites render staff cards via JS but embed name+title in
    # data-filter-string / aria-label attributes on data-blocktype="person"
    # elements.  Extract those before the rest of the HTML is cleaned away.
    sb_lines: list[str] = []
    for person_el in soup.find_all(attrs={"data-blocktype": "person"}):
        filter_str = person_el.get("data-filter-string") or person_el.get("aria-label") or ""
        if filter_str.strip():
            sb_lines.append(filter_str.strip())
        # Also grab any mailto links inside the block
        for a in person_el.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href[len("mailto:"):]
                if email:
                    sb_lines.append(f"Email: {email}")
    sb_text = "\n".join(sb_lines)

    # ── Phase 1: Extract content from SSR / JS-framework data blocks
    #    BEFORE we strip <script> tags, because the actual page content
    #    may live entirely inside those blocks.
    ssr_text = _extract_ssr_content(soup)

    # ── Phase 2: Standard HTML cleaning ─────────────────────────
    # Remove unwanted tags entirely
    for tag in soup.find_all(REMOVE_TAGS):
        tag.decompose()

    # Remove elements whose class or id match boilerplate patterns.
    # Never remove structural page-frame elements — some CMSes (e.g. Finalsite)
    # put words like "menu" or "banner" in the <body> class list, which would
    # otherwise wipe out the entire page content.
    _NEVER_REMOVE = {"html", "body", "main", "article", "section", "head"}
    for el in soup.find_all(True):
        if el.name in _NEVER_REMOVE:
            continue
        attrs = el.attrs if el.attrs is not None else {}
        classes = " ".join(attrs.get("class", []) or [])
        el_id = attrs.get("id", "") or ""
        if REMOVE_PATTERNS.search(classes) or REMOVE_PATTERNS.search(el_id):
            el.decompose()

    # Extract text with link annotations from visible HTML
    lines: list[str] = []
    _extract_text_lines(soup, lines)

    # Deduplicate adjacent identical lines (common with nested elements)
    deduped: list[str] = []
    for line in lines:
        if not deduped or line != deduped[-1]:
            deduped.append(line)

    html_text = "\n".join(deduped)

    # ── Phase 3: Combine SchoolBlocks + SSR content + standard HTML ─
    # SchoolBlocks person blocks go first; SSR content second; HTML last.
    parts = [p for p in (sb_text, ssr_text, html_text) if p.strip()]
    combined = "\n".join(parts)

    # ── Phase 4: Strip UI widget label lines ─────────────────────
    # Navigation widgets (e.g. Apptegy/Thrillshare) inject labels like
    # "Documents Icon", "Events Icon", "Staff Icon" into the SSR data.
    # These are never person names and confuse the contact extractor.
    _UI_LABEL_RE = re.compile(
        r'\b(icon|button|logo|image|widget|toggle|menu|close|open|arrow|chevron)\b',
        re.IGNORECASE,
    )
    filtered_lines = [
        ln for ln in combined.splitlines()
        if not _UI_LABEL_RE.search(ln) or len(ln.strip()) > 60
    ]
    combined = "\n".join(filtered_lines)

    # ── Phase 5: Annotate bare phone numbers ─────────────────────
    # Sites often render phone numbers as plain text with no tel: link.
    # Adding [PHONE: ...] markers gives Claude a reliable extraction signal.
    return _annotate_phone_numbers(combined)


# Regex to find internal paths quoted in script data blocks (SSR / JS navigation).
# Matches "/some/path" but excludes static assets.
_SCRIPT_PATH_RE = re.compile(r'"(/[a-zA-Z][a-zA-Z0-9_./@-]*)"')
_STATIC_EXTENSIONS = {".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg",
                      ".woff", ".woff2", ".ttf", ".eot", ".ico", ".map", ".webp"}


def extract_internal_links(raw_html: str, base_url: str) -> list[str]:
    """
    Extract all internal links from a page, resolved to absolute URLs.
    Checks both standard <a href> tags AND paths embedded in script data
    blocks (SSR / JS navigation used by Apptegy, Finalsite, etc.).
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    base_domain = urlparse(base_url).netloc
    parsed_base = urlparse(base_url)
    base_root = f"{parsed_base.scheme}://{parsed_base.netloc}"
    links = set()

    # ── Standard <a> tags ─────────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        if base_domain in parsed.netloc or parsed.netloc in base_domain:
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
            links.add(clean_url)

    # ── Paths embedded in <script> data blocks ────────────────────
    # JS-rendered sites (Apptegy, Nuxt, Next.js) often embed their
    # navigation structure in JSON inside script tags rather than <a> tags.
    for script in soup.find_all("script"):
        text = script.string or ""
        if len(text) < 50:
            continue
        text = text.replace("\\u003C", "<").replace("\\u003E", ">")
        text = text.replace("\\u0026", "&").replace('\\"', '"')
        for m in _SCRIPT_PATH_RE.finditer(text):
            path = m.group(1)
            if path.startswith("/api/") or path.startswith("/silent"):
                continue
            ext = path[path.rfind("."):] if "." in path.split("/")[-1] else ""
            if ext.lower() in _STATIC_EXTENSIONS:
                continue
            clean_url = f"{base_root}{path}".rstrip("/")
            links.add(clean_url)

    return list(links)


def score_url_for_staff_content(url: str) -> int:
    """
    Heuristic score for how likely a URL is to contain staff directory info.
    Higher = more likely. Penalizes low-value path segments and very deep paths.
    """
    url_lower = url.lower()
    score = 0
    for hint in STAFF_URL_HINTS:
        if hint in url_lower:
            # Give CTE-related URLs a little extra weight since they are
            # especially relevant to our target roles.
            if hint in {"cte", "career-tech"}:
                score += 2
            else:
                score += 1
    # Penalize path segments that usually aren't staff/leadership pages
    parsed = urlparse(url_lower)
    path_segments = [p for p in parsed.path.split("/") if p]
    for segment in path_segments:
        if segment in LOW_VALUE_PATH_PARTS or any(p in segment for p in LOW_VALUE_PATH_PARTS):
            score -= 1
            break
    # Penalize very deep paths (likely leaf pages, not directory indexes)
    path_depth = url_lower.count("/") - 2
    if path_depth > 5:
        score -= 1
    return score


def _path_depth(url: str) -> int:
    """Number of path segments (fewer = more likely a base/section page)."""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    return len(parts)


def get_candidate_urls(raw_html: str, base_url: str, max_urls: int = 10) -> list[str]:
    """
    Extract internal links and return the top candidates most likely
    to contain staff/leadership contact info. Sorted by score (high first),
    then by path depth (shallow first) so base pages like /our_district
    are preferred over /our_district/board_of_education when scores tie.
    """
    links = extract_internal_links(raw_html, base_url)
    scored = [
        (url, score_url_for_staff_content(url), _path_depth(url))
        for url in links
    ]
    # Descending score, then ascending path depth (base paths first)
    scored.sort(key=lambda x: (-x[1], x[2]))
    return [url for url, _s, _d in scored[:max_urls]]


def looks_like_staff_directory(url: str) -> bool:
    """True if the URL path suggests a staff/directory listing (and may be paginated)."""
    url_lower = url.lower()
    return any(hint in url_lower for hint in STAFF_URL_HINTS)


def _normalize_for_comparison(url: str) -> str:
    """Scheme + netloc + path, no fragment, no trailing slash (query kept for pagination)."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _url_has_pagination_query(parsed) -> bool:
    """True if query string has a known pagination param."""
    if not parsed.query:
        return False
    keys = {k.lower() for k in parse_qs(parsed.query, keep_blank_values=True)}
    return bool(keys & PAGINATION_PARAMS)


def _parse_page_number(url: str) -> int | None:
    """Try to extract a page number from query or path for ordering. Returns None if not found."""
    parsed = urlparse(url)
    if parsed.query:
        q = parse_qs(parsed.query, keep_blank_values=True)
        for k, v in q.items():
            if k.lower() in PAGINATION_PARAMS and v and str(v[0]).isdigit():
                return int(v[0])
    path_lower = parsed.path.lower()
    if "/page/" in path_lower:
        segment = path_lower.split("/page/")[-1].split("/")[0]
        if segment.isdigit():
            return int(segment)
    return None


async def fetch_schoolinsites_directory(raw_html: str, base_url: str) -> str:
    """
    SchoolInsites / MyConnectSuite CMS renders staff directories entirely
    via client-side JavaScript ($.getJSON to /sys/api/directory).  The
    initial HTML contains only an empty container div; our httpx-based
    scraper never executes JavaScript so staff data is invisible.

    This function detects the widget container, calls the API directly,
    and returns a formatted text block that can be appended to the page
    content sent to Claude.

    Returns an empty string if the page doesn't use this CMS pattern.
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    parsed_base = urlparse(base_url)
    api_root = f"{parsed_base.scheme}://{parsed_base.netloc}"

    # Find all SchoolInsites directory widgets on the page
    widgets = soup.find_all(attrs={"data-module": "widgets/directory"})
    if not widgets:
        return ""

    all_lines: list[str] = []

    for widget in widgets:
        widget_id = widget.get("data-widget-id")
        page_size = widget.get("data-page-size", "500")
        if not widget_id:
            continue

        api_url = (
            f"{api_root}/sys/api/directory"
            f"?widgetId={widget_id}&viewId=directory-photo"
            f"&pageNum=1&pageSize={page_size}"
        )

        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(api_url, headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                })
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            print(f"[scraper] SchoolInsites API call failed for widget {widget_id}: {e}")
            continue

        if not isinstance(data, list) or not data:
            continue

        print(f"[scraper] SchoolInsites directory API returned {len(data)} staff members")

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


def get_pagination_links(
    raw_html: str, page_url: str, max_next_pages: int = 2
) -> list[str]:
    """
    Find links that look like "next page" of the same listing (same path base,
    pagination query or path). Returns up to max_next_pages URLs to fetch,
    excluding the current page, same domain only.
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    base_domain = urlparse(page_url).netloc
    current_parsed = urlparse(page_url)
    path_base = current_parsed.path.rstrip("/") or "/"

    candidates: list[tuple[str, int]] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        full_url = urljoin(page_url, href)
        parsed = urlparse(full_url)

        if base_domain not in parsed.netloc and parsed.netloc not in base_domain:
            continue

        other_path = parsed.path.rstrip("/") or "/"
        if not (other_path == path_base or other_path.startswith(path_base + "/")):
            continue

        has_query_pagination = _url_has_pagination_query(parsed)
        has_path_pagination = "/page/" in other_path.lower()
        link_text = (a.get_text() or "").strip().lower()
        text_looks_next = link_text in ("next", "»", ">") or link_text.startswith("page ") or (link_text.isdigit() and int(link_text) > 1)

        if not (has_query_pagination or has_path_pagination or text_looks_next):
            continue

        full_no_frag = full_url.split("#")[0].rstrip("/")
        current_no_frag = page_url.split("#")[0].rstrip("/")
        if full_no_frag == current_no_frag:
            continue

        page_num = _parse_page_number(full_url)
        candidates.append((full_url, page_num if page_num is not None else 999))

    seen: set[str] = set()
    unique: list[str] = []
    for url, _ in sorted(candidates, key=lambda x: (x[1], x[0])):
        if url in seen:
            continue
        seen.add(url)
        unique.append(url)
        if len(unique) >= max_next_pages:
            break

    return unique
