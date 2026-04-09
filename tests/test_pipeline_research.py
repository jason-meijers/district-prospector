"""Tests for shared Firecrawl-based Pipedrive research pipeline."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.pipeline_research import (
    reconcile_extracted_contacts,
    run_firecrawl_research,
)


class TestReconcileExtractedContacts(unittest.TestCase):
    def test_reconciles_confirmed_updated_new_missing(self) -> None:
        existing_contacts = [
            {
                "person_id": 1,
                "name": "Jane Smith",
                "job_title": "Superintendent",
                "role_category_id": 482,
                "email": "jane@district.org",
                "phone": "555-111-2222",
            },
            {
                "person_id": 2,
                "name": "Bob Brown",
                "job_title": "Curriculum Director",
                "role_category_id": 472,
                "email": None,
                "phone": None,
            },
        ]
        all_person_names = {"jane smith": 1, "bob brown": 2, "already exists": 44}
        extracted_contacts = [
            {
                "name": "Jane Smith",
                "job_title": "Superintendent",
                "role_category_id": 482,
                "role_category": "Superintendent",
                "email": "jane@district.org",
                "email_confidence": "confirmed",
                "phone": "555-111-2222",
                "source_url": "https://district.org/staff",
                "notes": "",
            },
            {
                "name": "Bob Brown",
                "job_title": "Director of Curriculum & Instruction",
                "role_category_id": 472,
                "role_category": "Curriculum Director",
                "email": "bbrown@district.org",
                "email_confidence": "high",
                "phone": "555-000-9999",
                "source_url": "https://district.org/leadership",
                "notes": "",
            },
            {
                "name": "Already Exists",
                "job_title": "CTE Director",
                "role_category_id": 471,
                "role_category": "CTE Director",
                "email": "existing@district.org",
                "email_confidence": "confirmed",
                "phone": None,
                "source_url": "https://district.org/cte",
                "notes": "",
            },
            {
                "name": "New Person",
                "job_title": "Assistant Superintendent",
                "role_category_id": 468,
                "role_category": "Assistant Superintendent",
                "email": "new@district.org",
                "email_confidence": "confirmed",
                "phone": "555-444-5555",
                "source_url": "https://district.org/admin",
                "notes": "",
            },
        ]

        contacts = reconcile_extracted_contacts(
            extracted_contacts=extracted_contacts,
            existing_contacts=existing_contacts,
            all_person_names=all_person_names,
        )

        self.assertEqual(len(contacts["confirmed"]), 1)
        self.assertEqual(contacts["confirmed"][0]["pipedrive_person_id"], 1)

        self.assertEqual(len(contacts["updated"]), 1)
        self.assertEqual(contacts["updated"][0]["pipedrive_person_id"], 2)
        self.assertIn("title", contacts["updated"][0]["changes"])
        self.assertIn("email", contacts["updated"][0]["changes"])
        self.assertIn("phone", contacts["updated"][0]["changes"])

        self.assertEqual(len(contacts["new"]), 1)
        self.assertEqual(contacts["new"][0]["name"], "New Person")

        self.assertEqual(len(contacts["missing"]), 0)

    def test_marks_missing_when_existing_not_found(self) -> None:
        contacts = reconcile_extracted_contacts(
            extracted_contacts=[],
            existing_contacts=[
                {
                    "person_id": 9,
                    "name": "Absent Person",
                    "job_title": "CTE Director",
                    "role_category_id": 471,
                    "email": None,
                    "phone": None,
                }
            ],
            all_person_names={},
        )

        self.assertEqual(len(contacts["missing"]), 1)
        self.assertEqual(contacts["missing"][0]["pipedrive_person_id"], 9)


class TestRunFirecrawlResearch(unittest.IsolatedAsyncioTestCase):
    @patch("app.pipeline_research.scrape_district")
    @patch("app.pipeline_research.BatchExtractionAgent")
    async def test_returns_expected_payload_shape(self, mock_agent_cls, mock_scrape) -> None:
        mock_agent = mock_agent_cls.return_value
        mock_agent.extract_contacts_raw.return_value = (
            [
                {
                    "name": "Jane Smith",
                    "job_title": "Superintendent",
                    "role_category_id": 482,
                    "role_category": "Superintendent",
                    "email": "jane@district.org",
                    "email_confidence": "confirmed",
                    "phone": "555-111-2222",
                    "source_url": "https://district.org/staff",
                    "notes": "",
                }
            ],
            {"pattern": "firstinitial+last@district.org", "confidence": "high", "examples_found": ["jsmith@district.org"]},
            {"input_tokens": 10, "output_tokens": 20},
        )
        mock_scrape.return_value = (
            [{"url": "https://district.org/staff", "content": "Jane Smith Superintendent"}],
            {"scrape_calls": 2},
            {"used_heuristic": False, "staff_directory_url": "https://district.org/staff", "enrichment_urls": []},
        )

        result = await run_firecrawl_research(
            org_name="Test District",
            website_url="https://district.org",
            existing_contacts=[],
            all_person_names={},
            district_state=None,
        )

        self.assertEqual(result["district_name"], "Test District")
        self.assertIn("contacts", result)
        self.assertIn("confirmed", result["contacts"])
        self.assertIn("usage", result)
        self.assertIn("firecrawl_usage", result)
        self.assertIn("url_triage", result)


if __name__ == "__main__":
    unittest.main()
