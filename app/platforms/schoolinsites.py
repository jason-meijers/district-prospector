"""
SchoolInsites / MyConnectSuite adapter.

Detection signal: ``data-module="widgets/directory"`` elements in the
homepage HTML (or anywhere else we already grabbed). When present, the
platform exposes a JSON endpoint at ``/sys/api/directory`` that returns
every staff member with name, title, department, email, and phone — far
cleaner than anything Firecrawl+LLM would recover from the JS-rendered
shell.
"""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.platforms import PlatformAdapter, PlatformDetection, PlatformPage, register


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class SchoolInsitesAdapter:
    name = "schoolinsites"

    async def detect(
        self,
        homepage_html: str,
        base_url: str,
    ) -> PlatformDetection | None:
        soup = BeautifulSoup(homepage_html, "html.parser")
        widgets = soup.find_all(attrs={"data-module": "widgets/directory"})
        if not widgets:
            return None

        widget_specs: list[dict] = []
        for w in widgets:
            wid = w.get("data-widget-id")
            if not wid:
                continue
            widget_specs.append(
                {
                    "widget_id": wid,
                    "page_size": w.get("data-page-size", "500"),
                }
            )
        if not widget_specs:
            return None

        return PlatformDetection(
            name=self.name,
            confidence=0.95,
            context={"widgets": widget_specs},
        )

    async def fetch(
        self,
        base_url: str,
        detection: PlatformDetection,
        usage: dict | None = None,
    ) -> list[PlatformPage]:
        widgets: list[dict] = detection.context.get("widgets") or []
        if not widgets:
            return []

        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        all_lines: list[str] = []
        async with httpx.AsyncClient(
            timeout=15,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            for widget in widgets:
                widget_id = widget["widget_id"]
                page_size = widget.get("page_size", "500")
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
                    print(f"[platforms.schoolinsites] widget {widget_id} failed: {e}")
                    continue

                if not isinstance(data, list):
                    continue

                print(
                    f"[platforms.schoolinsites] widget {widget_id} returned "
                    f"{len(data)} entries"
                )
                for person in data:
                    line = _format_person(person)
                    if line:
                        all_lines.append(line)

        if not all_lines:
            return []

        content = "## Staff Directory (via SchoolInsites API)\n" + "\n".join(all_lines)
        return [
            PlatformPage(
                url=f"{base_url} [SchoolInsites API]",
                content=content,
            )
        ]


def _format_person(person: dict) -> str:
    name = person.get("NameFirstLast") or ""
    if not name:
        return ""
    title = person.get("JobTitle") or ""
    dept = person.get("Department") or ""
    phone = person.get("PhoneNumber") or ""
    email = person.get("EmailAddress") or ""
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


register(SchoolInsitesAdapter())
