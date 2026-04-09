"""Tests for Firecrawl scrape helpers."""

import unittest
from unittest.mock import MagicMock

from app.firecrawl_scraper import _should_run_interact


class TestShouldRunInteract(unittest.TestCase):
    def test_interact_priority_runs_even_when_markdown_not_thin(self):
        """Triage-selected directory must paginate; page 1 is often long."""
        settings = MagicMock()
        settings.firecrawl_interact_enabled = True
        settings.firecrawl_interact_min_markdown_chars = 400
        thick = "x" * 5000
        self.assertTrue(
            _should_run_interact(
                "https://example.org/directory",
                thick,
                settings,
                interact_priority=True,
            )
        )

    def test_non_priority_still_requires_thin_for_directory_urls(self):
        settings = MagicMock()
        settings.firecrawl_interact_enabled = True
        settings.firecrawl_interact_min_markdown_chars = 400
        thick = "x" * 5000
        self.assertFalse(
            _should_run_interact(
                "https://example.org/directory",
                thick,
                settings,
                interact_priority=False,
            )
        )


if __name__ == "__main__":
    unittest.main()
