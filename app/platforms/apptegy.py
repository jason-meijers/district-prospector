"""
Apptegy / Thrillshare adapter.

Apptegy-built district sites ship a Nuxt SSR payload (``window.__NUXT__``) and
an embedded configuration blob (``window.clientWorkStateTemp``) that names the
Thrillshare CMS API URLs for each content section — staff directory, news,
events, documents, etc. The staff endpoint is public JSON and paginates the
full roster, including leadership roles that are invisible on page 1 of the
rendered site (e.g. Superintendent on the alphabetical page 2).

Why this matters
----------------
Without this adapter the fallback path is Firecrawl + LLM extraction of the
SSR HTML, which only sees the first paginated slice (~20 people). Districts
with a superintendent whose name does not start with 'A' quietly disappear
from triage results. Calling the API directly returns every staff member with
name, title, department, email, and phone in clean JSON.

Fastly challenge
----------------
Apptegy fronts its sites with a Fastly "Client Challenge" that returns a ~3KB
JS shim to default browser UAs. A Googlebot UA bypasses the shim cleanly. The
challenge never fires on the ``thrillshare-cmsv2.services.thrillshare.com``
API host itself, so we only need the UA workaround to grab the section id
once.

Flow
----
1. Detect signals in the homepage HTML. If the HTML is the Fastly shim, retry
   once with a Googlebot UA.
2. Parse ``window.clientWorkStateTemp`` (or a loose regex fallback) to find
   the ``links.staff`` URL and section id.
3. Walk ``?page_no=1..N`` on the Thrillshare directory endpoint and format
   each person into the same ``Name — Title (Department) [EMAIL: …]
   [PHONE: …]`` shape the SchoolInsites adapter produces.
"""

from __future__ import annotations

import json
import re
from typing import Iterable
from urllib.parse import urlparse

import httpx

from app.platforms import PlatformAdapter, PlatformDetection, PlatformPage, register


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
# Googlebot passes Apptegy's Fastly challenge; default browser UAs do not.
_GOOGLEBOT_UA = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
)

_SIGNAL_PATTERNS = [
    re.compile(r"window\.__NUXT__", re.IGNORECASE),
    re.compile(r"thrillshare-spa|thrillshare\.com", re.IGNORECASE),
    re.compile(r"apptegy(\.|\-)", re.IGNORECASE),
    re.compile(r"tsassets\.", re.IGNORECASE),
    re.compile(r"name=\"generator\"\s+content=\"[^\"]*Apptegy", re.IGNORECASE),
    re.compile(r"clientWorkStateTemp", re.IGNORECASE),
]

# Upper bound on pages walked per site. 50 pages × 20 rows/page = 1000 staff —
# well beyond any realistic district roster; the API returns ~20/page.
_MAX_PAGES = 50
# Fastly challenge shim is tiny and titled "Client Challenge". Real Apptegy
# homepages run 500KB+; anything under this with challenge markers is the shim.
_CHALLENGE_MAX_BYTES = 8_000


class ApptegyAdapter:
    name = "apptegy"

    async def detect(
        self,
        homepage_html: str,
        base_url: str,
    ) -> PlatformDetection | None:
        html = homepage_html or ""

        # If we were handed the Fastly challenge shim (or nothing at all),
        # retry once with a Googlebot UA so the Apptegy signals — and the
        # staff API URL — actually show up.
        if _looks_like_fastly_challenge(html) or len(html) < 200:
            retry = await _fetch_with_ua(base_url, _GOOGLEBOT_UA)
            if retry:
                html = retry

        hits = sum(1 for p in _SIGNAL_PATTERNS if p.search(html))
        if hits == 0:
            return None

        staff_api_url = _extract_staff_api_url(html)

        # Strong confidence (fast-path eligible) only when we actually have an
        # API URL to call. Otherwise return a weak hit so the orchestrator
        # falls back to Firecrawl instead of short-circuiting to an empty
        # page list.
        if staff_api_url:
            confidence = min(0.85 + 0.03 * hits, 0.98)
        else:
            confidence = min(0.55 + 0.05 * hits, 0.7)

        return PlatformDetection(
            name=self.name,
            confidence=confidence,
            context={
                "signal_hits": hits,
                "staff_api_url": staff_api_url,
            },
        )

    async def fetch(
        self,
        base_url: str,
        detection: PlatformDetection,
        usage: dict | None = None,
    ) -> list[PlatformPage]:
        staff_api_url = (detection.context or {}).get("staff_api_url")
        if not staff_api_url:
            print(
                f"[platforms.apptegy] no staff API URL for {base_url}; "
                "falling back to Firecrawl pipeline"
            )
            return []

        try:
            people = await _walk_directory(staff_api_url)
        except Exception as e:
            print(
                f"[platforms.apptegy] directory walk failed ({type(e).__name__}: {e}); "
                "falling back to Firecrawl pipeline"
            )
            return []

        if not people:
            return []

        lines = [_format_person(p) for p in people]
        lines = [line for line in lines if line]
        if not lines:
            return []

        print(
            f"[platforms.apptegy] {base_url} via {staff_api_url} — "
            f"{len(lines)} staff entries"
        )
        content = "## Staff Directory (via Apptegy/Thrillshare API)\n" + "\n".join(lines)
        return [
            PlatformPage(
                url=f"{base_url} [Apptegy API]",
                content=content,
            )
        ]


def _looks_like_fastly_challenge(html: str) -> bool:
    if not html or len(html) >= _CHALLENGE_MAX_BYTES:
        return False
    return "Client Challenge" in html and "_fs-ch-" in html


async def _fetch_with_ua(url: str, user_agent: str, timeout: int = 15) -> str | None:
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": user_agent},
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.text or None
    except Exception as e:
        print(f"[platforms.apptegy] fetch {url} failed: {type(e).__name__}: {e}")
        return None


_STAFF_URL_RE = re.compile(
    r'"staff"\s*:\s*"(https?://[^"\s]*thrillshare[^"\s]*/api/v2/s/\d+/directories[^"\s]*)"',
    re.IGNORECASE,
)
_CLIENT_WORK_STATE_RE = re.compile(
    r'clientWorkStateTemp\s*=\s*JSON\.parse\(\s*"((?:\\.|[^"\\])*)"\s*\)',
    re.DOTALL,
)


def _extract_staff_api_url(html: str) -> str | None:
    """
    Prefer parsing the ``clientWorkStateTemp`` JSON.parse literal because it's
    the canonical place Apptegy publishes API URLs. Fall back to a loose
    regex across the whole document (covers SSR payload variants).
    """
    if not html:
        return None

    m = _CLIENT_WORK_STATE_RE.search(html)
    if m:
        raw = m.group(1)
        # The string inside JSON.parse("...") is a JSON string literal — undo
        # the JS string escaping to get valid JSON, then load it.
        try:
            decoded = json.loads('"' + raw + '"')
            state = json.loads(decoded)
            staff = (((state or {}).get("links") or {}).get("staff") or "").strip()
            if staff and "thrillshare" in staff and "/directories" in staff:
                return staff
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[platforms.apptegy] clientWorkStateTemp parse failed: {e}")

    # Fallback: scan the raw (and unescaped) HTML for the staff key. Handles
    # cases where the blob is JSON-encoded elsewhere (e.g. inlined in __NUXT__
    # with different escaping).
    unescaped = html.replace('\\"', '"').replace("\\/", "/")
    m2 = _STAFF_URL_RE.search(unescaped)
    if m2:
        return m2.group(1)

    return None


async def _walk_directory(staff_api_url: str) -> list[dict]:
    """
    Walk the Thrillshare staff directory via ``?page_no=N``. The API uses
    ``meta.links.last`` to advertise the final page number; we parse that
    once on the first request and iterate from there, capped at ``_MAX_PAGES``.
    """
    people: list[dict] = []
    seen_ids: set[int] = set()

    async with httpx.AsyncClient(
        timeout=15,
        follow_redirects=True,
        headers={
            "User-Agent": _BROWSER_UA,
            "Accept": "application/json",
        },
    ) as client:
        first = await _get_page(client, staff_api_url, page_no=1)
        if first is None:
            return []
        _extend_unique(people, seen_ids, first.get("directories") or [])

        last_page = _parse_last_page(first) or 1
        last_page = min(max(last_page, 1), _MAX_PAGES)

        for n in range(2, last_page + 1):
            page = await _get_page(client, staff_api_url, page_no=n)
            if page is None:
                break
            rows = page.get("directories") or []
            if not rows:
                break
            before = len(people)
            _extend_unique(people, seen_ids, rows)
            # Defensive: if the API starts echoing page 1 back (as some
            # section ids do) stop instead of collecting duplicates.
            if len(people) == before:
                break

    return people


async def _get_page(
    client: httpx.AsyncClient,
    staff_api_url: str,
    page_no: int,
) -> dict | None:
    sep = "&" if "?" in staff_api_url else "?"
    url = f"{staff_api_url}{sep}page_no={page_no}"
    try:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[platforms.apptegy] page {page_no} failed: {type(e).__name__}: {e}")
        return None
    if not isinstance(data, dict):
        return None
    return data


def _extend_unique(
    out: list[dict],
    seen_ids: set[int],
    rows: Iterable[dict],
) -> None:
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = row.get("id")
        if isinstance(rid, int):
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
        out.append(row)


_LAST_PAGE_RE = re.compile(r"page_no=(\d+)")


def _parse_last_page(payload: dict) -> int | None:
    meta = payload.get("meta") if isinstance(payload, dict) else None
    links = meta.get("links") if isinstance(meta, dict) else None
    if not isinstance(links, dict):
        return None
    last = links.get("last")
    if not isinstance(last, str):
        return None
    m = _LAST_PAGE_RE.search(last)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _format_person(person: dict) -> str:
    name = (person.get("full_name") or "").strip()
    if not name:
        first = (person.get("first") or "").strip()
        last = (person.get("last") or "").strip()
        name = f"{first} {last}".strip()
    if not name:
        return ""

    title = (person.get("title") or "").strip()
    dept = (person.get("department") or "").strip()
    email = (person.get("email") or "").strip()
    phone = (person.get("phone_number") or "").strip()

    line = name
    if title:
        line += f" — {title}"
    if dept:
        line += f" ({dept})"
    if email:
        line += f" [EMAIL: {email}]"
    if phone:
        line += f" [PHONE: {phone}]"
    return line


# Silence the unused-import lint on urlparse — kept for future use if we
# ever need to validate the staff URL's origin against base_url.
_ = urlparse


register(ApptegyAdapter())
