"""Tests for deterministic target-role title filtering."""

from __future__ import annotations

import unittest

from app.role_filters import (
    is_excluded_job_title,
    is_obviously_non_target_roster_title,
    is_strong_target_job_title,
    is_target_extracted_contact,
)


class TestExcludedTitles(unittest.TestCase):
    def test_rejects_irrelevant_directors(self) -> None:
        for title in (
            "Director of Transportation",
            "Director of Transport",
            "Director of Human Relations",
            "Director of Community Relations",
            "Director of Facilities",
            "Director of Finance",
            "Director of Technology",
        ):
            with self.subTest(title=title):
                self.assertTrue(is_excluded_job_title(title))

    def test_keeps_curriculum_leaders(self) -> None:
        for title in (
            "Director of Curriculum & Instruction",
            "Director of Teaching and Learning",
            "Assistant Superintendent of Educational Services",
            "CTE Director",
            "Superintendent",
        ):
            with self.subTest(title=title):
                self.assertFalse(is_excluded_job_title(title))


class TestTargetExtractedContact(unittest.TestCase):
    def test_rejects_generic_director_bucket_for_transport(self) -> None:
        contact = {
            "name": "Jane Doe",
            "job_title": "Director of Transportation",
            "role_category_id": 474,
        }
        self.assertFalse(is_target_extracted_contact(contact))

    def test_accepts_curriculum_director_with_matching_role_id(self) -> None:
        contact = {
            "name": "Jane Doe",
            "job_title": "Director of Curriculum & Instruction",
            "role_category_id": 472,
        }
        self.assertTrue(is_target_extracted_contact(contact))

    def test_rejects_other_bucket_without_strong_title(self) -> None:
        contact = {
            "name": "Jane Doe",
            "job_title": "Director of Community Relations",
            "role_category_id": 478,
        }
        self.assertFalse(is_target_extracted_contact(contact))

    def test_roster_prefilter_matches_exclusions(self) -> None:
        self.assertTrue(is_obviously_non_target_roster_title("Director of Human Resources"))
        self.assertFalse(is_obviously_non_target_roster_title("Director of Curriculum"))


class TestStrongTargetTitles(unittest.TestCase):
    def test_strong_matches(self) -> None:
        self.assertTrue(is_strong_target_job_title("Deputy Superintendent"))
        self.assertTrue(is_strong_target_job_title("Career Technical Education Director"))
        self.assertFalse(is_strong_target_job_title("Director of Operations"))


if __name__ == "__main__":
    unittest.main()
