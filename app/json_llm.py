"""Extract JSON objects from LLM text (leading prose, markdown code fences)."""

from __future__ import annotations

import json
import re


def _first_json_object_slice(text: str) -> str | None:
    """First top-level `{...}` in text, respecting JSON double-quoted strings."""
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    i = start
    in_string = False
    escape_next = False
    while i < len(text):
        c = text[i]
        if in_string:
            if escape_next:
                escape_next = False
            elif c == "\\":
                escape_next = True
            elif c == '"':
                in_string = False
            i += 1
            continue
        if c == '"':
            in_string = True
            i += 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
        i += 1
    return None


def parse_llm_json_object(raw: str | None) -> dict | None:
    """
    Parse a single JSON object from Claude output that may include prose or ``` fences.
    """
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    try:
        o = json.loads(s)
        if isinstance(o, dict):
            return o
    except json.JSONDecodeError:
        pass
    for m in re.finditer(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", s):
        chunk = m.group(1).strip()
        if not chunk:
            continue
        try:
            o = json.loads(chunk)
            if isinstance(o, dict):
                return o
        except json.JSONDecodeError:
            continue
    blob = _first_json_object_slice(s)
    if blob:
        try:
            o = json.loads(blob)
            if isinstance(o, dict):
                return o
        except json.JSONDecodeError:
            pass
    return None
