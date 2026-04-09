"""Tests for Slack / Pipedrive contact sanitization."""

import unittest

from app.slack import sanitize_email_for_pipedrive, sanitize_phone_for_pipedrive


class TestSanitizePhone(unittest.TestCase):
    def test_strips_tel_and_junk(self):
        self.assertEqual(
            sanitize_phone_for_pipedrive("tel:989-743-3471>"),
            "989-743-3471",
        )

    def test_dedupes_concatenated_us_number(self):
        self.assertEqual(
            sanitize_phone_for_pipedrive("tel:989-743-3471989-743-3471>"),
            "989-743-3471",
        )


class TestSanitizeEmail(unittest.TestCase):
    def test_strips_mailto(self):
        self.assertEqual(
            sanitize_email_for_pipedrive("mailto:jane@school.org"),
            "jane@school.org",
        )


if __name__ == "__main__":
    unittest.main()
