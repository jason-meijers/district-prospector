"""Tests for parsing JSON objects from LLM responses (prose + markdown fences)."""

from __future__ import annotations

import unittest

from app.json_llm import parse_llm_json_object


class TestParseLlmJsonObject(unittest.TestCase):
    def test_plain_object(self) -> None:
        raw = '{"a": 1, "b": "two"}'
        out = parse_llm_json_object(raw)
        self.assertEqual(out, {"a": 1, "b": "two"})

    def test_preamble_then_fenced_json(self) -> None:
        raw = """Looking at the content I can extract:
```json
{
  "district_main_phone": "870-597-2723",
  "contacts": [{"name": "Bill Muse", "job_title": "Superintendent"}]
}
```
"""
        out = parse_llm_json_object(raw)
        self.assertIsNotNone(out)
        assert out is not None
        self.assertEqual(out.get("district_main_phone"), "870-597-2723")
        self.assertEqual(len(out.get("contacts") or []), 1)

    def test_fenced_without_json_tag(self) -> None:
        raw = """Here you go:
```
{"staff_directory_url": "https://x.org/staff", "enrichment_urls": []}
```
"""
        out = parse_llm_json_object(raw)
        self.assertEqual(
            out,
            {"staff_directory_url": "https://x.org/staff", "enrichment_urls": []},
        )

    def test_preamble_then_raw_object(self) -> None:
        raw = 'Sure. {"notes": "ok", "contacts": []} trailing ignored'
        out = parse_llm_json_object(raw)
        self.assertEqual(out, {"notes": "ok", "contacts": []})

    def test_braces_inside_string_values(self) -> None:
        raw = '{"pattern": "a{b}c", "contacts": []}'
        out = parse_llm_json_object(raw)
        self.assertEqual(out, {"pattern": "a{b}c", "contacts": []})

    def test_returns_none_for_garbage(self) -> None:
        self.assertIsNone(parse_llm_json_object("not json"))
        self.assertIsNone(parse_llm_json_object(None))
        self.assertIsNone(parse_llm_json_object("   "))


if __name__ == "__main__":
    unittest.main()
