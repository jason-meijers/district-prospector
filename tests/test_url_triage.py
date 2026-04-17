"""Unit tests for URL triage plan sanitization (no API calls)."""

import unittest

from app.batch_agent import _sanitize_triage_plan


class TestSanitizeTriagePlan(unittest.TestCase):
    def test_filters_unknown_urls(self) -> None:
        allowed = {"https://school.example.org", "https://school.example.org/staff"}
        home = "https://school.example.org"
        plan = _sanitize_triage_plan(
            {
                "staff_directory_url": "https://evil.example/phish",
                "enrichment_urls": [
                    "https://school.example.org/staff",
                    "https://unknown.example/x",
                ],
                "include_homepage": True,
                "rationale": "test",
            },
            allowed,
            home,
            enrich_cap=5,
        )
        self.assertIsNone(plan["staff_directory_url"])
        self.assertEqual(plan["enrichment_urls"], ["https://school.example.org/staff"])
        self.assertTrue(plan["include_homepage"])

    def test_invalid_json_fallback_shape(self) -> None:
        allowed = {"https://a.org", "https://a.org/b"}
        plan = _sanitize_triage_plan(None, allowed, "https://a.org", 3)
        self.assertEqual(plan["rationale"], "invalid_or_empty_json")
        self.assertIsNone(plan["staff_directory_url"])
        self.assertEqual(plan["enrichment_urls"], [])

    def test_respects_enrichment_cap(self) -> None:
        allowed = {f"https://d.org/p{i}" for i in range(10)} | {"https://d.org"}
        home = "https://d.org"
        plan = _sanitize_triage_plan(
            {
                "staff_directory_url": None,
                "enrichment_urls": [f"https://d.org/p{i}" for i in range(10)],
                "include_homepage": False,
                "rationale": "many",
            },
            allowed,
            home,
            enrich_cap=2,
        )
        self.assertEqual(len(plan["enrichment_urls"]), 2)
        self.assertFalse(plan["include_homepage"])

    def test_skips_duplicate_of_directory(self) -> None:
        allowed = {"https://d.org", "https://d.org/dir", "https://d.org/lead"}
        plan = _sanitize_triage_plan(
            {
                "staff_directory_url": "https://d.org/dir",
                "enrichment_urls": ["https://d.org/dir", "https://d.org/lead"],
                "include_homepage": True,
                "rationale": "x",
            },
            allowed,
            "https://d.org",
            5,
        )
        self.assertEqual(plan["staff_directory_url"], "https://d.org/dir")
        self.assertEqual(plan["enrichment_urls"], ["https://d.org/lead"])


if __name__ == "__main__":
    unittest.main()
