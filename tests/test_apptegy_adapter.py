"""
Unit tests for the Apptegy/Thrillshare platform adapter.

Covers the pure parsing helpers (staff URL extraction, pagination math,
formatting, Fastly challenge detection). Network-level integration lives
outside this file.
"""

from __future__ import annotations

import json
import unittest

from app.platforms.apptegy import (
    _extend_unique,
    _extract_staff_api_url,
    _format_person,
    _looks_like_fastly_challenge,
    _parse_last_page,
)


_STAFF_URL = (
    "https://thrillshare-cmsv2.services.thrillshare.com"
    "/api/v2/s/429704/directories"
)


def _clientworkstate_snippet(staff_url: str = _STAFF_URL) -> str:
    """Mimic the real ``window.clientWorkStateTemp = JSON.parse("…")`` blob."""
    state = {
        "host": "www.example.org",
        "org": 25694,
        "parentOrgId": 5131,
        "links": {
            "live_feed": "https://example/live",
            "staff": staff_url,
            "news": "https://example/news",
        },
    }
    inner = json.dumps(state)
    escaped = json.dumps(inner)
    return f'<script>window.clientWorkStateTemp = JSON.parse({escaped});</script>'


class TestExtractStaffApiUrl(unittest.TestCase):
    def test_pulls_staff_url_from_client_work_state(self) -> None:
        html = _clientworkstate_snippet()
        self.assertEqual(_extract_staff_api_url(html), _STAFF_URL)

    def test_falls_back_to_loose_regex_when_blob_missing(self) -> None:
        # No clientWorkStateTemp, but the staff URL appears elsewhere (e.g.
        # inlined in a __NUXT__ payload with escaped slashes).
        html = (
            '<script>window.__NUXT__={"links":{"staff":"'
            + _STAFF_URL.replace("/", "\\/")
            + '"}}</script>'
        )
        self.assertEqual(_extract_staff_api_url(html), _STAFF_URL)

    def test_returns_none_when_no_staff_url(self) -> None:
        html = "<html><body>Nothing here</body></html>"
        self.assertIsNone(_extract_staff_api_url(html))

    def test_ignores_non_thrillshare_staff_keys(self) -> None:
        # A "staff" key pointing somewhere unrelated must not match.
        html = '<script>var x = {"staff":"https://example.com/staff"};</script>'
        self.assertIsNone(_extract_staff_api_url(html))


class TestParseLastPage(unittest.TestCase):
    def test_reads_last_page_from_meta_links(self) -> None:
        payload = {
            "meta": {
                "links": {
                    "first": "https://api/x?page=2&page_no=1",
                    "last": "https://api/x?page=2&page_no=9",
                },
            },
        }
        self.assertEqual(_parse_last_page(payload), 9)

    def test_returns_none_when_meta_missing(self) -> None:
        self.assertIsNone(_parse_last_page({}))
        self.assertIsNone(_parse_last_page({"meta": {}}))
        self.assertIsNone(_parse_last_page({"meta": {"links": {"last": ""}}}))


class TestFormatPerson(unittest.TestCase):
    def test_full_row_renders_all_annotations(self) -> None:
        line = _format_person(
            {
                "full_name": "Clint Jones",
                "title": "Superintendent",
                "department": "District Office",
                "email": "clint.jones@lamarwarriors.org",
                "phone_number": "479-885-3907",
            }
        )
        self.assertIn("Clint Jones", line)
        self.assertIn("— Superintendent", line)
        self.assertIn("(District Office)", line)
        self.assertIn("[EMAIL: clint.jones@lamarwarriors.org]", line)
        self.assertIn("[PHONE: 479-885-3907]", line)

    def test_uses_first_last_fallback_when_full_name_missing(self) -> None:
        line = _format_person(
            {
                "first": "Abby",
                "last": "Pelts",
                "title": "Teacher",
            }
        )
        self.assertTrue(line.startswith("Abby Pelts"))
        self.assertIn("— Teacher", line)

    def test_skips_empty_optional_fields(self) -> None:
        line = _format_person(
            {
                "full_name": "Alicia Palmer",
                "title": "Cafeteria",
                "department": None,
                "email": "",
                "phone_number": "",
            }
        )
        self.assertEqual(line, "Alicia Palmer — Cafeteria")

    def test_returns_empty_when_nameless(self) -> None:
        self.assertEqual(_format_person({"title": "Teacher"}), "")


class TestExtendUnique(unittest.TestCase):
    def test_dedups_by_id(self) -> None:
        out: list[dict] = []
        seen: set[int] = set()
        _extend_unique(out, seen, [{"id": 1, "full_name": "A"}, {"id": 2, "full_name": "B"}])
        _extend_unique(out, seen, [{"id": 2, "full_name": "B"}, {"id": 3, "full_name": "C"}])
        names = [p["full_name"] for p in out]
        self.assertEqual(names, ["A", "B", "C"])

    def test_keeps_rows_without_id(self) -> None:
        out: list[dict] = []
        seen: set[int] = set()
        _extend_unique(out, seen, [{"full_name": "A"}, {"full_name": "A"}])
        # No id ⇒ no dedup by design (avoids dropping legit duplicates that
        # happen to share a display name).
        self.assertEqual(len(out), 2)


class TestFastlyChallengeDetection(unittest.TestCase):
    def test_matches_tiny_challenge_shim(self) -> None:
        shim = (
            '<!DOCTYPE html><html><head><title>Client Challenge</title>'
            '<link href="/_fs-ch-abc/assets/styles.css"></head></html>'
        )
        self.assertTrue(_looks_like_fastly_challenge(shim))

    def test_rejects_real_homepage(self) -> None:
        real = "<html>" + ("x" * 50_000) + "Client Challenge _fs-ch-</html>"
        # Real Apptegy homepages are hundreds of KB; the size gate rules them
        # out even if the literal strings happen to appear somewhere.
        self.assertFalse(_looks_like_fastly_challenge(real))

    def test_rejects_empty_or_unrelated(self) -> None:
        self.assertFalse(_looks_like_fastly_challenge(""))
        self.assertFalse(_looks_like_fastly_challenge("<html>hi</html>"))


if __name__ == "__main__":
    unittest.main()
