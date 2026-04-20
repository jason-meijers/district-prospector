"""
ContactHunter — agentic tool-calling loop for discovering contacts that the
pipeline-first pass missed (hybrid mode) or for replacing the fixed pipeline
entirely (full_agent mode).

The loop is intentionally tight and budgeted:

* A small catalog of well-typed tools (map / search / scrape / extract,
  fetch_platform_api, commit_contact, report_coverage).
* Hard caps on tool-calls, output tokens, and wall-clock seconds.
* Trace entries capture every tool call for observability and later
  storage in ``hunter_traces``.

The result shape mirrors what the existing pipeline emits so it can be
merged back into :mod:`app.pipeline_research` without bespoke glue.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

import anthropic

from app.config import (
    ROLE_CATEGORY_OPTIONS,
    get_settings,
)
from app.text_sanitize import sanitize_contact_dict
from app.role_coverage import score_role_coverage


# ── Types ─────────────────────────────────────────────────────────


@dataclass
class Budget:
    """Hard stops for a single hunt invocation."""

    max_tool_calls: int = 8
    max_output_tokens: int = 4096
    max_seconds: float = 120.0


@dataclass
class HuntGoal:
    """What the hunter is being asked to find."""

    district_name: str
    district_state: str | None
    base_url: str
    missing_roles: list[str]
    known_contacts: list[dict] = field(default_factory=list)


@dataclass
class HuntResult:
    """Output of a hunt run."""

    contacts: list[dict]
    stop_reason: str
    tool_calls: int
    elapsed_seconds: float
    token_usage: dict
    trace: list[dict] = field(default_factory=list)


# ── Tool schemas (Anthropic tool-use format) ──────────────────────

_TOOL_SCHEMAS: list[dict] = [
    {
        "name": "firecrawl_map",
        "description": (
            "List URLs discovered via Firecrawl's map endpoint on the district "
            "website, biased toward staff/leadership pages. Use this to find "
            "candidate pages before scraping. Returns up to 25 URLs with titles."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Optional keyword filter (e.g. 'superintendent', "
                        "'curriculum director'). Leave empty to return the "
                        "default staff-biased results."
                    ),
                }
            },
            "required": [],
        },
    },
    {
        "name": "firecrawl_search",
        "description": (
            "Run a Firecrawl web search limited to the district's domain. Use "
            "only when map has been exhausted and a specific name/title is "
            "still missing."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search phrase, e.g. 'Lincoln ISD superintendent'.",
                }
            },
            "required": ["query"],
        },
    },
    {
        "name": "firecrawl_scrape",
        "description": (
            "Fetch a single URL as markdown. Always prefer this over a raw web "
            "fetch — Firecrawl handles JS rendering and applies our scrape/"
            "interact heuristics."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "interact_priority": {
                    "type": "boolean",
                    "description": (
                        "If true (use for directory pages), Firecrawl will "
                        "paginate via interact so later pages aren't lost."
                    ),
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "fetch_platform_api",
        "description": (
            "If the site is on a known CMS (SchoolInsites, etc.) with a "
            "structured staff API, fetch the roster through the adapter. "
            "Returns cleaned markdown ready for extraction."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "commit_contact",
        "description": (
            "Record a single confirmed contact. Commit at least one contact "
            "per missing role before calling report_coverage. Names, emails, "
            "and phones must come verbatim from tool output (no guessing)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "title": {"type": "string"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "role_category": {
                    "type": "string",
                    "enum": list(ROLE_CATEGORY_OPTIONS),
                    "description": "Which target role this person fills.",
                },
                "source_url": {
                    "type": "string",
                    "description": "URL the evidence came from.",
                },
                "evidence": {
                    "type": "string",
                    "description": (
                        "Short (≤200 chars) quote from the source supporting "
                        "the name↔title↔email mapping."
                    ),
                },
            },
            "required": ["name", "title", "role_category", "source_url"],
        },
    },
    {
        "name": "report_coverage",
        "description": (
            "Call when you believe no further useful contacts can be found. "
            "Pass a short status string; this terminates the hunt loop."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": (
                        "One of: 'complete' (all requested roles filled), "
                        "'partial' (some filled), 'exhausted' (no further "
                        "sources to try)."
                    ),
                },
                "notes": {
                    "type": "string",
                    "description": "Optional free-text rationale for humans.",
                },
            },
            "required": ["status"],
        },
    },
]


_SYSTEM_PROMPT = """\
You are ContactHunter — a focused research agent working inside the
District Prospector pipeline. You are given a school district, the roles we
still need, and a toolbox for crawling the district's website.

Rules:
1. Call ONE tool at a time. Read each observation before deciding the next
   step. Do not hallucinate — every contact you commit must be justified by a
   direct quote from tool output, passed back as `evidence`.
2. Prefer `fetch_platform_api` first when a CMS adapter might apply — it is
   cheaper and cleaner than scraping.
3. Use `firecrawl_map` to find candidate pages, then `firecrawl_scrape` the
   most promising one(s). Only use `firecrawl_search` when map+scrape have
   been exhausted.
4. Commit contacts with `commit_contact` as soon as you have solid evidence.
   One person per tool call. You may commit multiple contacts if a page
   shows multiple target roles.
5. When you have committed all reachable roles, OR you've run out of
   productive leads, call `report_coverage` with an appropriate status.
6. Be frugal. You have a hard cap on tool calls and seconds. Do not scrape
   the same URL twice. Skip non-staff pages (board minutes, calendars,
   policies).
"""


# ── Hunter ────────────────────────────────────────────────────────


class ContactHunter:
    """
    Agentic tool-calling loop.

    ``tool_impls`` is a dict mapping tool name → awaitable callable
    implementing the tool. Tools receive the parsed JSON input and must
    return a ``dict`` (JSON-serializable). Implementations live in
    :func:`build_default_tool_impls` but can be overridden for tests.
    """

    def __init__(
        self,
        goal: HuntGoal,
        tool_impls: dict[str, Any],
        budget: Budget | None = None,
        model: str | None = None,
    ):
        self.goal = goal
        self.tool_impls = tool_impls
        self.settings = get_settings()
        self.budget = budget or Budget(
            max_tool_calls=self.settings.hunter_max_tool_calls_gap_fill,
            max_output_tokens=self.settings.hunter_max_output_tokens,
            max_seconds=self.settings.hunter_max_seconds,
        )
        self.model = model or self.settings.hunter_model

        self._client = anthropic.Anthropic(api_key=self.settings.anthropic_api_key)
        self._committed: list[dict] = []
        self._trace: list[dict] = []
        self._token_usage: dict = {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }
        self._tool_calls = 0
        self._visited_urls: set[str] = set()

    async def run(self) -> HuntResult:
        start = time.monotonic()
        stop_reason = "budget_exhausted"

        user_prompt = self._build_initial_user_prompt()
        messages: list[dict] = [{"role": "user", "content": user_prompt}]

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= self.budget.max_seconds:
                stop_reason = "timeout"
                break
            if self._tool_calls >= self.budget.max_tool_calls:
                stop_reason = "max_tool_calls"
                break

            try:
                response = await asyncio.to_thread(
                    self._client.messages.create,
                    model=self.model,
                    max_tokens=self.budget.max_output_tokens,
                    system=_SYSTEM_PROMPT,
                    tools=_TOOL_SCHEMAS,
                    messages=messages,
                )
            except Exception as e:
                print(f"[hunter] LLM call failed: {type(e).__name__}: {e}")
                stop_reason = "llm_error"
                self._trace.append({"event": "llm_error", "error": str(e)})
                break

            self._track_usage(response)

            # Append the assistant message exactly as received so tool_use
            # blocks pair correctly with their tool_result follow-ups.
            assistant_content = self._message_content_as_list(response)
            messages.append({"role": "assistant", "content": assistant_content})

            tool_uses = [b for b in assistant_content if isinstance(b, dict) and b.get("type") == "tool_use"]
            if not tool_uses:
                # No more tools requested — the model gave a final text answer.
                stop_reason = "model_stopped"
                break

            tool_results: list[dict] = []
            hunt_finished = False
            for block in tool_uses:
                self._tool_calls += 1
                tool_name = block.get("name") or ""
                tool_input = block.get("input") or {}
                tool_use_id = block.get("id") or ""

                trace_entry = {
                    "event": "tool_call",
                    "tool": tool_name,
                    "input": tool_input,
                    "call_index": self._tool_calls,
                    "elapsed_ms": int((time.monotonic() - start) * 1000),
                }

                if tool_name == "commit_contact":
                    self._apply_commit(tool_input)
                    result_payload = {"status": "recorded", "committed_count": len(self._committed)}
                elif tool_name == "report_coverage":
                    stop_reason = f"report_coverage:{tool_input.get('status', 'unknown')}"
                    trace_entry["result"] = tool_input
                    self._trace.append(trace_entry)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use_id,
                            "content": json.dumps({"status": "acknowledged"}),
                        }
                    )
                    hunt_finished = True
                    break
                else:
                    impl = self.tool_impls.get(tool_name)
                    if impl is None:
                        result_payload = {"error": f"unknown tool {tool_name!r}"}
                    else:
                        try:
                            result_payload = await self._invoke_tool(impl, tool_name, tool_input)
                        except Exception as e:
                            print(f"[hunter] tool {tool_name} raised {type(e).__name__}: {e}")
                            result_payload = {"error": f"{type(e).__name__}: {e}"}

                trace_entry["result"] = _summarize_result(result_payload)
                self._trace.append(trace_entry)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": json.dumps(result_payload)[:40_000],
                    }
                )

                if self._tool_calls >= self.budget.max_tool_calls:
                    stop_reason = "max_tool_calls"
                    hunt_finished = True
                    break

            if tool_results:
                messages.append({"role": "user", "content": tool_results})
            if hunt_finished:
                break

        elapsed_total = time.monotonic() - start
        return HuntResult(
            contacts=self._committed,
            stop_reason=stop_reason,
            tool_calls=self._tool_calls,
            elapsed_seconds=elapsed_total,
            token_usage=self._token_usage,
            trace=self._trace,
        )

    # ── helpers ──

    def _build_initial_user_prompt(self) -> str:
        known_lines = []
        for c in self.goal.known_contacts[:15]:
            parts = [c.get("name") or "?"]
            if c.get("title"):
                parts.append(c["title"])
            if c.get("role_category"):
                parts.append(f"[{c['role_category']}]")
            known_lines.append(" — ".join(parts))
        known_block = "\n".join(known_lines) if known_lines else "(none yet)"

        state = self.goal.district_state or "unknown"
        missing = ", ".join(self.goal.missing_roles) or "(none — verify only)"

        coverage = score_role_coverage(self.goal.known_contacts)
        covered = len(coverage.covered)
        total_cohorts = covered + len(coverage.missing)
        coverage_ratio = (covered / total_cohorts) if total_cohorts else 0.0

        return (
            f"District: {self.goal.district_name} ({state})\n"
            f"Website: {self.goal.base_url}\n"
            f"Already committed contacts:\n{known_block}\n\n"
            f"Roles still missing (priority order): {missing}\n"
            f"Coverage: {covered}/{total_cohorts} cohorts "
            f"({coverage_ratio:.0%})\n\n"
            "Find the missing contacts using the tool catalog. Commit each "
            "with a direct evidence quote. When you cannot make further "
            "progress, call report_coverage."
        )

    def _track_usage(self, response: Any) -> None:
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        for key in (
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ):
            self._token_usage[key] = self._token_usage.get(key, 0) + int(
                getattr(usage, key, 0) or 0
            )

    def _message_content_as_list(self, response: Any) -> list[dict]:
        out: list[dict] = []
        for block in getattr(response, "content", []) or []:
            if hasattr(block, "model_dump"):
                out.append(block.model_dump())
            elif isinstance(block, dict):
                out.append(block)
        return out

    async def _invoke_tool(self, impl: Any, name: str, tool_input: dict) -> dict:
        if name == "firecrawl_scrape":
            url = str(tool_input.get("url") or "").strip()
            if url in self._visited_urls:
                return {"note": "already scraped — skipped duplicate", "url": url}
            self._visited_urls.add(url)

        result = impl(tool_input)
        if asyncio.iscoroutine(result):
            result = await result
        if not isinstance(result, dict):
            return {"result": result}
        return result

    def _apply_commit(self, payload: dict) -> None:
        name = (payload.get("name") or "").strip()
        if not name:
            return
        ev = (payload.get("evidence") or "")[:500] or None
        contact = sanitize_contact_dict(
            {
                "name": name,
                "title": (payload.get("title") or "").strip(),
                "email": (payload.get("email") or "").strip().lower() or None,
                "phone": (payload.get("phone") or "").strip() or None,
                "role_category": (payload.get("role_category") or "").strip() or None,
                "source_url": (payload.get("source_url") or "").strip() or None,
                "evidence": ev,
                "origin": "contact_hunter",
            }
        )
        self._committed.append(contact)


def _summarize_result(result: Any) -> Any:
    """Compact representation of tool output for trace storage."""
    if not isinstance(result, dict):
        return {"value": str(result)[:500]}
    summary: dict = {}
    for key, val in result.items():
        if isinstance(val, str):
            summary[key] = val[:500]
        elif isinstance(val, list):
            summary[key] = {"list_len": len(val), "sample": val[:3]}
        else:
            summary[key] = val
    return summary


__all__ = [
    "Budget",
    "ContactHunter",
    "HuntGoal",
    "HuntResult",
]
