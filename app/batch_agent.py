from __future__ import annotations
import json
import re
import time
import anthropic
from app.config import get_settings

# ─────────────────────────────────────────────────────────────
# Role options passed directly into the prompt so Claude
# always assigns a valid Pipedrive ID.
# ─────────────────────────────────────────────────────────────

BATCH_ROLE_OPTIONS = """
ID 482 → Superintendent
ID 468 → Assistant Superintendent
ID 472 → Curriculum Director
ID 623 → Curriculum Coordinator
ID 471 → CTE Director
ID 467 → Assistant CTE Director
ID 470 → CTE Coordinator
ID 480 → Principal (secondary/high school only)
ID 474 → Director (use for Director of Secondary Education, Director of Learning and Instruction, and similar senior director roles that don't fit a more specific category above)
ID 478 → Other (last resort only)
"""

# ─────────────────────────────────────────────────────────────
# Simplified extraction prompt — raw contact hunting only,
# no CRM comparison, ~500 tokens vs ~3,000 for the full prompt.
# ─────────────────────────────────────────────────────────────

BATCH_EXTRACTION_SYSTEM = f"""You are a contact research agent. Your job is to extract contact information for specific roles from school district website content.

## Target Roles

Extract people who hold these roles (exact titles and reasonable equivalents):

**Superintendent level:**
- Superintendent, Deputy Superintendent, Assistant Superintendent (any area), Interim Superintendent, Acting Superintendent

**Curriculum / Instruction:**
- Director of Curriculum, Director of Instruction, Director of Curriculum & Instruction, Director of Learning and Instruction, Director of Teaching & Learning, Chief Academic Officer, Assistant/Associate Superintendent of Educational Services, Assistant/Associate Superintendent of Instruction, Curriculum Coordinator, Instructional Services Director

**CTE:**
- CTE Director, CTE Coordinator, Career Technical Education Director, Director of Career & College Readiness, Career Pathways Director, Vocational Education Director, Assistant CTE Director

**Secondary / High School:**
- Principal (HIGH SCHOOL or SECONDARY only), Director of Secondary Education, Director of High School Programs

## Critical Exclusions

SKIP any person whose title explicitly references:
- Elementary, Primary, K-5, Grades 1-6, or Middle School (e.g. "Elementary Principal", "Director of K-5 Curriculum", "Middle School CTE Coordinator")
- Board members or trustees
- Teachers, counselors, coaches, nurses
- HR, Human Resources, Finance, Accounting, Business, Facilities, Operations, Maintenance, IT, Technology, Transportation

## Role Category Assignment

For each contact assign the BEST matching role_category_id from this list:
{BATCH_ROLE_OPTIONS.strip()}

## District main phone (board office / district switchboard)

District sites almost always show one main phone in the header, footer, or contact block (e.g. \"Phone: 555-123-4567\", \"Central Office\", \"District Office\").

1. Set JSON field `district_main_phone` to that single main district/switchboard number in **###-###-####** form (US), or `null` if truly not visible anywhere in the content.
2. For **each contact**, if the site does **not** list a **direct** phone for that person, set `phone` to `district_main_phone` (when not null). If they **do** have their own extension/number, keep that personal number in `phone`.
3. Only use numbers that clearly belong to the **district or central office**, not random numbers from news stories or unrelated sponsors.

## Email Pattern Detection

When you find email addresses, detect the pattern (e.g. firstinitial+last@domain.org). Apply it to contacts without a listed email. Track confidence:
- "confirmed" = email explicitly shown for this person
- "high" = pattern from 2+ confirmed emails, applied here
- "medium" = pattern from 1 confirmed email
- "low" = no pattern found

## Output Format

Return ONLY valid JSON — no markdown, no explanation.

{{
  "district_main_phone": "string ###-###-#### or null — district / central office main line from the site",
  "contacts": [
    {{
      "name": "string — full name as shown on site",
      "job_title": "string — exact title from website",
      "role_category": "string — label matching the ID below",
      "role_category_id": 482,
      "email": "string or null",
      "email_confidence": "confirmed|high|medium|low",
      "phone": "string — personal direct line if shown, otherwise district_main_phone, or null",
      "source_url": "string — URL where this person was found"
    }}
  ],
  "email_pattern": {{
    "pattern": "string — e.g. firstinitial+last@domain.org, or null if not found",
    "confidence": "high|medium|low",
    "examples_found": ["email@domain.org"]
  }},
  "notes": "string — brief observations about site quality or data completeness"
}}

IMPORTANT: Only return real human names. Never treat page labels, navigation items, department names, or image descriptions as person names."""

_PHONE_NEAR_LABEL = re.compile(
    r"(?:phone|telephone|tel\.?|district\s+office|central\s+office|main\s+office|board\s+office)\s*[:\s]+"
    r"\(?([2-9]\d{2})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})",
    re.I,
)


def _normalize_us_phone_str(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        d = digits[1:]
        return f"{d[:3]}-{d[3:6]}-{d[6:]}"
    return None


def _guess_district_phone_from_pages(pages: list[dict]) -> str | None:
    """If Claude omits district_main_phone, find a labeled main line in scraped text."""
    parts: list[str] = []
    for p in pages[:8]:
        parts.append((p.get("content") or "")[:12000])
    chunk = "\n".join(parts)
    if not chunk.strip():
        return None
    m = _PHONE_NEAR_LABEL.search(chunk)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


class BatchExtractionAgent:
    """
    Simplified extraction agent for batch processing.
    Performs raw contact hunting — no Pipedrive comparison.
    """

    def __init__(self):
        self.settings = get_settings()
        self.client = anthropic.Anthropic(api_key=self.settings.anthropic_api_key)

    def _call_claude(self, user_message: str, max_tokens: int = 2048) -> tuple[str | None, dict]:
        """
        Call Claude with the batch extraction system prompt.
        Uses prompt caching on the system prompt — after the first call in a
        session, the system prompt is served from cache at ~10% of input cost.

        Retries up to 3 times on rate-limit errors with exponential backoff.
        """
        max_retries = 3
        retry_delay = 60

        for attempt in range(max_retries):
            try:
                response = self.client.messages.create(
                    model=self.settings.claude_model,
                    max_tokens=max_tokens,
                    system=[{
                        "type": "text",
                        "text": BATCH_EXTRACTION_SYSTEM,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_message}],
                )

                usage: dict = {}
                if getattr(response, "usage", None):
                    u = response.usage
                    usage = {
                        "input_tokens": getattr(u, "input_tokens", 0) or 0,
                        "output_tokens": getattr(u, "output_tokens", 0) or 0,
                        "cache_creation_input_tokens": getattr(u, "cache_creation_input_tokens", 0) or 0,
                        "cache_read_input_tokens": getattr(u, "cache_read_input_tokens", 0) or 0,
                    }

                content = getattr(response, "content", None) or []
                text_parts = []
                for block in content if isinstance(content, list) else []:
                    if block is None:
                        continue
                    t = block.get("text") if isinstance(block, dict) else getattr(block, "text", None)
                    if t:
                        text_parts.append(t)
                text = " ".join(text_parts).strip() or None
                return text, usage

            except anthropic.RateLimitError as e:
                if attempt < max_retries - 1:
                    wait = retry_delay * (2 ** attempt)
                    print(f"[batch_agent] Rate limit — waiting {wait}s (attempt {attempt + 2}/{max_retries}): {e}")
                    time.sleep(wait)
                else:
                    print(f"[batch_agent] Rate limit — all retries exhausted")
                    raise
            except Exception as e:
                print(f"[batch_agent] Claude call failed: {type(e).__name__}: {e}")
                return None, {}

        return None, {}

    def extract_contacts_raw(
        self,
        pages: list[dict],
        org_name: str,
        website_url: str,
    ) -> tuple[list[dict], dict, dict]:
        """
        Extract contacts from scraped page content.

        Args:
            pages: list of {"url": str, "content": str}
            org_name: district name (for context)
            website_url: district homepage URL

        Returns:
            (contacts_list, email_pattern_dict, usage_dict)
            contacts_list is a flat list of contact dicts.
            On failure returns ([], {}, {}).
        """
        if not pages:
            return [], {}, {}

        chars_per_page = self.settings.batch_chars_per_page
        page_sections = []
        for i, page in enumerate(pages, 1):
            content = (page.get("content") or "")[:chars_per_page]
            page_sections.append(f"### Page {i}: {page['url']}\n{content}")

        pages_text = "\n\n".join(page_sections)
        user_message = (
            f"District: {org_name}\n"
            f"Website: {website_url}\n\n"
            f"## Website Content\n\n"
            f"{pages_text}\n\n"
            f"Extract all contacts matching target roles. Return JSON only."
        )

        raw, usage = self._call_claude(user_message)
        if not raw:
            print(f"[batch_agent] Claude returned no text for {org_name}")
            return [], {}, usage

        try:
            cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            result = json.loads(cleaned)
        except json.JSONDecodeError as e:
            print(f"[batch_agent] JSON parse error for {org_name}: {e}")
            print(f"[batch_agent] Raw response (first 500 chars): {raw[:500]}")
            return [], {}, usage

        contacts = result.get("contacts") or []
        email_pattern = result.get("email_pattern") or {}
        notes = result.get("notes") or ""

        # Basic sanity filter: skip anything that looks like a UI label
        _LABEL_WORDS = {
            "icon", "button", "logo", "image", "widget", "toggle",
            "arrow", "chevron", "pdf", "document", "link", "menu",
        }
        clean_contacts = []
        for c in contacts:
            name = (c.get("name") or "").strip()
            if not name:
                continue
            name_lower = name.lower()
            if any(word in name_lower for word in _LABEL_WORDS):
                print(f"[batch_agent] Skipping UI label as name: '{name}'")
                continue
            clean_contacts.append(c)

        district_phone = (result.get("district_main_phone") or "").strip()
        district_phone = _normalize_us_phone_str(district_phone) if district_phone else None
        if not district_phone:
            district_phone = _guess_district_phone_from_pages(pages)

        if district_phone:
            backfill = 0
            for c in clean_contacts:
                if not (c.get("phone") or "").strip():
                    c["phone"] = district_phone
                    backfill += 1
            if backfill:
                print(
                    f"[batch_agent] Filled {backfill} missing phone(s) with district main line {district_phone}"
                )

        print(
            f"[batch_agent] Extracted {len(clean_contacts)} contacts for {org_name} "
            f"(input: {usage.get('input_tokens', 0):,} tokens, "
            f"output: {usage.get('output_tokens', 0):,} tokens)"
        )

        if notes:
            print(f"[batch_agent] Notes for {org_name}: {notes}")

        return clean_contacts, email_pattern, usage
