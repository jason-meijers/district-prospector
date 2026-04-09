"""Tests for Slack / Pipedrive contact sanitization."""

import unittest

from app.slack import (
    pipedrive_payload_blocks,
    sanitize_email_for_pipedrive,
    sanitize_phone_for_pipedrive,
)


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


class TestPayloadBlocks(unittest.TestCase):
    def test_fenced_sections_use_verbatim_true(self):
        blocks = pipedrive_payload_blocks(
            header_mrkdwn="*Header*",
            person_body_json='{"x": 1}',
            note_content="note",
        )
        self.assertTrue(blocks[1]["text"]["verbatim"])
        self.assertFalse(blocks[0]["text"]["verbatim"])
        self.assertTrue(blocks[3]["text"]["verbatim"])


if __name__ == "__main__":
    unittest.main()
