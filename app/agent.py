from __future__ import annotations
import json
import re
import time
import anthropic
from app.config import get_settings, ROLE_CATEGORY_OPTIONS, ROLE_CATEGORY_BY_LABEL
from app.scraper import fetch_page, clean_html, get_candidate_urls, looks_like_staff_directory, get_pagination_links, fetch_schoolinsites_directory

# ─────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────

URL_IDENTIFICATION_SYSTEM = """You are analyzing a school district homepage to identify which pages are most likely to contain staff contact information for district leadership, department heads, and program directors.

Given the cleaned HTML content of the homepage (including all internal links), return a JSON array of up to {max_urls} URLs most likely to contain:
- District leadership team / superintendent's office
- "Our District" or "About Us" pages (often include superintendent welcome message and contact)
- Department directory or staff directory
- Educational services / academic services department page
- Career Technical Education (CTE) program page
- Contact us page with staff listings

Prioritize "Our District", "About the District", and similar pages when present — they frequently list the superintendent and key contacts.

Return ONLY a valid JSON array of full URLs, ranked by likelihood. No markdown, no explanation, no preamble."""


EXTRACTION_SYSTEM = """You are a contact research agent for Nucleus Courses, an education technology company. Your job is to analyze school district website content and extract contacts that match target roles, then compare them against existing CRM records.

## Your Task

Given:
1. Cleaned text from a school district's website pages
2. A list of existing contacts already in our CRM for this district

Produce a structured JSON analysis identifying:
- CONFIRMED: Existing CRM contacts found on the website, still in their role
- UPDATED: Existing CRM contacts found but with changed information (new title, new email, etc.)
- NEW: People found on the website who match target roles but are NOT in our CRM
- MISSING: CRM contacts who were NOT found anywhere on the website pages provided

CRITICAL — UPDATED vs MISSING:
- UPDATED means the person's NAME explicitly APPEARS on at least one page, with different info (e.g. new title, new email). If you see "Jane Smith" on the site with a new title, she is UPDATED.
- Finding someone's email address in the page source is NOT sufficient to classify them as UPDATED. Email addresses can linger in page source code (old links, hidden metadata, etc.) long after a person has left. The person's NAME must be visibly present on the page as a current contact.
- MISSING means the person's name does NOT appear on any provided page. Put them in MISSING (not UPDATED) when:
  - Their name is absent from all pages, or
  - Someone else is now in their role (e.g. a new Superintendent is named and the previous one is gone) — even if the old person's email still appears somewhere in the page source.
  - We use MISSING to mark people as no longer at the district (Former). Do NOT put someone in UPDATED just because their email was found in source code while their name is absent—put them in MISSING.

## Target Roles

You are looking for people in these categories. Match on exact titles AND reasonable equivalents.

**Superintendent** — includes:
- Superintendent, Deputy Superintendent, Assistant Superintendent (any area), Interim Superintendent, Acting Superintendent

**Curriculum** — includes:
- Director of Curriculum, Director of Instruction, Curriculum & Instruction Director/Coordinator, Director of Teaching & Learning, Assistant/Associate Superintendent of Educational Services, Assistant/Associate Superintendent of Instruction, Chief Academic Officer, Director of Academic Programs, Curriculum Coordinator, Instructional Services Director

**CTE** — includes:
- CTE Director, CTE Coordinator, Career Technical Education Director, Director of Career & College Readiness, CTE Program Manager, Career Pathways Director, Vocational Education Director

IMPORTANT: District org charts vary widely. A title like "Assistant Superintendent of Educational Services" IS a curriculum role even though it doesn't say "curriculum." Use your judgment — if someone oversees curriculum, instruction, or academic programs based on their title and context, include them. Explain your reasoning in the notes field.

CRITICAL: Pages whose URL or heading includes words like "Superintendent", "Office of the Superintendent", "Curriculum", or "Curriculum Director" almost always contain the contacts we want. Even if the information is presented in a welcome letter format or in a right-hand sidebar, you MUST extract the superintendent or curriculum director name, title, email, and phone when they appear.

CRITICAL — Valid person names only: A "name" must be a real human name (e.g. "Jason Longbrake", "Dr. Maria Chen"). NEVER treat UI labels, navigation items, or image descriptions as names. If a string contains words like "Icon", "Button", "Logo", "Image", "Widget", "Arrow", "PDF", "Document", "Link", "Menu", or "Toggle", it is NOT a person — skip it entirely, even if a title like "Superintendent" appears nearby.

Do NOT include: principals, vice principals, teachers, counselors, board members (trustees), business/finance roles, HR roles, facilities/operations roles, or IT/technology roles unless their title explicitly includes curriculum, instruction, or CTE oversight.

Do NOT include: Director of Equity & Inclusion (or similar equity/diversity-only leadership), MTSS Coordinator (or MTSS-only roles), Director of Expeditions (or similar enrichment-only program directors), gifted/talented or GATE program coordinators (e.g. Coordinator of Gifted and Learning). **Executive Director** is acceptable when it clearly supports curriculum, instruction, CTE, or district academic leadership. **Director of [X]** — include only when X is clearly curriculum, instruction, teaching & learning, secondary education, CTE, or academic services — not unrelated director portfolios.

## Role Category Assignment

For each contact, you MUST assign a `role_category_id` from our CRM dropdown. Pick the BEST match from this list:

ID: 482 → "Superintendent"
ID: 468 → "Assistant Superintendent"
ID: 472 → "Curriculum Director"
ID: 623 → "Curriculum Coordinator"
ID: 471 → "CTE Director"
ID: 467 → "Assistant CTE Director"
ID: 470 → "CTE Coordinator"
ID: 474 → "Director" (use when they're a director but don't fit a more specific category)
ID: 481 → "Program Manager"
ID: 478 → "Other" (last resort)

Pick the most specific option that fits. For example:
- "Superintendent" → 482
- "Deputy Superintendent" → 482 (still a superintendent-level role)
- "Assistant Superintendent of Educational Services" → 468
- "Director of Curriculum & Instruction" → 472
- "CTE Coordinator" → 470
- "Career Pathways Director" → 471

## Email Pattern Detection

When you find email addresses on the site, analyze the pattern:
- Look for mailto: links, displayed emails, or email patterns in contact sections
- Common patterns: first.last@domain, firstinitial+last@domain, first_last@domain, flast@domain
- If you can identify a consistent pattern from 2+ confirmed emails, apply it to contacts who don't have a listed email
- Track your confidence level:
  - "confirmed" = email explicitly shown or linked for this person
  - "high" = pattern derived from 2+ confirmed emails on same domain
  - "medium" = pattern derived from only 1 confirmed email, or common convention assumed
  - "low" = no pattern found, only district general email available

## Matching Against Existing CRM Contacts

When comparing website findings to existing CRM contacts:
- Match by name (fuzzy — "Dr. Gregory Nehen" matches "Greg Nehen" matches "Gregory Nehen")
- If the person's NAME appears on the site but with different title/email/phone, this is an UPDATE
- NEVER use an email address alone as the basis for an UPDATE. An email found in page source does not confirm the person is still at the district. Their name must appear visibly on the page as a current contact.
- If the person's name does NOT appear on any provided page → MISSING. This includes when someone else is now in their role (e.g. "Brian Gillis is Superintendent" and Benjamin O'Connor was previously—O'Connor goes in MISSING, not UPDATED), even if O'Connor's email still appears somewhere in the page's source code.
- Do NOT mark someone as MISSING just because they weren't on the leadership page—they might be on another page. Only mark MISSING if they appear on NONE of the pages provided.

## Output Format

Return ONLY valid JSON in this exact structure. No markdown, no explanation, no preamble.

{
  "district_name": "string",
  "website": "string",
  "email_pattern": {
    "pattern": "string — describe the pattern, e.g. 'firstinitial+last@avhsd.org'",
    "confidence": "high|medium|low",
    "examples_found": ["email1@domain.org"]
  },
  "contacts": {
    "confirmed": [
      {
        "name": "string",
        "job_title": "string — exact title as shown on site",
        "role_category_id": 482,
        "role_category_label": "Superintendent",
        "email": "string|null",
        "email_confidence": "confirmed|high|medium|low",
        "phone": "string|null",
        "source_url": "string",
        "pipedrive_person_id": 12345,
        "notes": "string"
      }
    ],
    "updated": [
      {
        "name": "string",
        "job_title": "string — NEW title from website",
        "previous_title": "string — title from CRM",
        "role_category_id": 472,
        "role_category_label": "Curriculum Director",
        "email": "string|null",
        "email_confidence": "confirmed|high|medium|low",
        "phone": "string|null",
        "source_url": "string",
        "pipedrive_person_id": 12345,
        "changes": ["title", "email", "phone"],
        "notes": "string"
      }
    ],
    "new": [
      {
        "name": "string",
        "job_title": "string",
        "role_category_id": 471,
        "role_category_label": "CTE Director",
        "email": "string|null",
        "email_confidence": "confirmed|high|medium|low",
        "phone": "string|null",
        "source_url": "string",
        "notes": "string — MUST explain reasoning if title isn't an exact match"
      }
    ],
    "missing": [
      {
        "name": "string",
        "previous_title": "string — from CRM",
        "pipedrive_person_id": 12345,
        "notes": "string — always note that absence from website doesn't confirm departure"
      }
    ]
  },
  "research_notes": "string — general observations about the site, data completeness, or suggestions"
}"""


# ─────────────────────────────────────────────────────────────
# Agent Logic
# ─────────────────────────────────────────────────────────────

WEBSITE_DISCOVERY_SYSTEM = """You are helping find the official website for a US school district.

Given a district name (and possibly a state), determine the most likely official website URL.

Rules:
- School district websites typically follow patterns like: www.districtname.org, www.districtname.k12.state.us, www.districtnameisd.net, etc.
- Return ONLY the URL as a plain string, no markdown, no explanation.
- If you are not confident, return "UNKNOWN".
- Only return URLs you believe are the official district website, not a third-party directory listing."""


class ExtractionAgent:
    """
    The core agent that orchestrates website fetching, Claude calls,
    and returns structured extraction results.
    """

    def __init__(self):
        self.settings = get_settings()
        self.client = anthropic.Anthropic(api_key=self.settings.anthropic_api_key)

    def _call_claude(self, system: str, user_message: str, max_tokens: int = 4096) -> tuple[str | None, dict]:
        """Call Claude and return (text_response_or_none, usage_dict).

        Automatically retries on rate-limit errors (HTTP 429) with
        exponential backoff: 60 s → 120 s → 240 s (3 attempts total).
        """
        max_retries = 3
        retry_delay = 60  # seconds

        for attempt in range(max_retries):
            try:
                response = self.client.messages.create(
                    model=self.settings.claude_model,
                    max_tokens=max_tokens,
                    system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": user_message}],
                )
                usage = {}
                if getattr(response, "usage", None):
                    u = response.usage
                    usage["input_tokens"] = getattr(u, "input_tokens", 0) or 0
                    usage["output_tokens"] = getattr(u, "output_tokens", 0) or 0
                    usage["cache_creation_input_tokens"] = getattr(u, "cache_creation_input_tokens", 0) or 0
                    usage["cache_read_input_tokens"] = getattr(u, "cache_read_input_tokens", 0) or 0

                content = getattr(response, "content", None)
                if content is None:
                    return None, usage
                text_parts = []
                for block in content if isinstance(content, list) else []:
                    if block is None:
                        continue
                    if isinstance(block, dict):
                        if block.get("type") == "text" and block.get("text"):
                            text_parts.append(block.get("text") or "")
                    else:
                        if getattr(block, "type", None) == "text":
                            text_parts.append(getattr(block, "text", None) or "")
                text = " ".join(text_parts).strip() or None
                return text, usage

            except anthropic.RateLimitError as e:
                if attempt < max_retries - 1:
                    wait = retry_delay * (2 ** attempt)
                    print(f"[agent] Rate limit hit — waiting {wait}s before retry {attempt + 2}/{max_retries}... ({e})")
                    time.sleep(wait)
                else:
                    print(f"[agent] Rate limit hit — all {max_retries} attempts exhausted.")
                    raise
            except AttributeError as e:
                if "NoneType" in str(e) or "get" in str(e).lower():
                    print(f"[agent] Claude API response issue (None/get): {e}")
                    return None, {}
                raise
            except Exception as e:
                if "'NoneType' object has no attribute 'get'" in str(e):
                    print(f"[agent] Claude API response issue: {e}")
                    return None, {}
                raise

        return None, {}  # unreachable, but satisfies the type checker

    def _call_claude_with_search(self, system: str, user_message: str, max_tokens: int = 1024) -> str:
        """Call Claude with web search enabled for website discovery."""
        max_retries = 3
        retry_delay = 60

        for attempt in range(max_retries):
            try:
                response = self.client.messages.create(
                    model=self.settings.claude_model,
                    max_tokens=max_tokens,
                    system=system,
                    messages=[{"role": "user", "content": user_message}],
                    tools=[{"type": "web_search_20250305", "name": "web_search"}],
                )
                text_parts = [block.text for block in response.content if block.type == "text"]
                return " ".join(text_parts).strip()
            except anthropic.RateLimitError as e:
                if attempt < max_retries - 1:
                    wait = retry_delay * (2 ** attempt)
                    print(f"[agent] Rate limit hit (search) — waiting {wait}s before retry {attempt + 2}/{max_retries}... ({e})")
                    time.sleep(wait)
                else:
                    print(f"[agent] Rate limit hit (search) — all {max_retries} attempts exhausted.")
                    raise

        return ""  # unreachable

    async def discover_website(self, org_name: str) -> str | None:
        """
        Use Claude + web search to find a district's official website
        when the Pipedrive record doesn't have one.
        Returns the URL string or None if not found.
        """
        print(f"[agent] No website on file — searching for {org_name}...")

        user_message = (
            f"Find the official website for this school district: {org_name}\n\n"
            f"Search the web and return ONLY the official district homepage URL."
        )

        try:
            raw = self._call_claude_with_search(WEBSITE_DISCOVERY_SYSTEM, user_message)

            # Claude might return a URL embedded in text — extract it
            url = None
            if "UNKNOWN" in raw.upper():
                return None

            # Try to find a URL in the response
            import re
            url_match = re.search(r'https?://[^\s<>"\')\]]+', raw)
            if url_match:
                url = url_match.group(0).rstrip(".,;:")

            if url:
                # Validate it's actually reachable
                test_html = await fetch_page(url, timeout=10)
                if test_html and len(test_html) > 500:
                    print(f"[agent] Discovered website: {url}")
                    return url
                else:
                    print(f"[agent] Discovered URL {url} but couldn't reach it")
                    return None

        except Exception as e:
            print(f"[agent] Website discovery failed: {e}")

        return None

    # Common staff/leadership directory paths used by various school website platforms.
    # These are probed and prepended when the site's links don't include human-readable
    # staff pages (e.g. SchoolBlocks sites that only expose UUID-based internal links).
    _COMMON_STAFF_PATHS = [
        "/en-US/staff",
        "/staff",
        "/staff-directory",
        "/about/staff-directory",
        "/district/staff-directory",
        "/district/staff",
        "/departments/staff-directory",
        "/pages/staff-directory",
        "/about-us/staff-directory",
        "/our-district/staff",
        "/contact/staff-directory",
    ]

    async def _probe_common_staff_urls(self, base_url: str) -> list[str]:
        """
        Probe a fixed set of common staff directory paths against the base URL.
        Returns only paths that actually respond with enough content to be useful.
        Uses a short timeout so it doesn't slow down sites that 404 cleanly.
        """
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        found = []
        for path in self._COMMON_STAFF_PATHS:
            url = origin + path
            html = await fetch_page(url, timeout=8)
            if html and len(html) > 500:
                found.append(url)
                print(f"[agent] Found common staff path: {url}")
        return found

    async def identify_subpages(self, homepage_html: str, base_url: str) -> list[str]:
        """
        Step 3: Use heuristic pre-filtering + Claude to identify the best
        subpages to fetch for staff/leadership contacts.
        """
        # First, get heuristic candidates from link analysis
        candidate_urls = get_candidate_urls(homepage_html, base_url, max_urls=15)
        cleaned_homepage = clean_html(homepage_html)

        # Build the prompt with candidates for Claude to rank/filter
        url_list = "\n".join(f"- {url}" for url in candidate_urls)
        user_message = (
            f"Homepage URL: {base_url}\n\n"
            f"## Homepage Content\n{cleaned_homepage[:6000]}\n\n"
            f"## Candidate Internal Links\n{url_list}\n\n"
            f"Select up to {self.settings.max_subpages} URLs from the candidate list above "
            f"(or from the homepage content) that are most likely to contain district "
            f"leadership, staff directory, or CTE program contact information."
        )

        system = URL_IDENTIFICATION_SYSTEM.format(max_urls=self.settings.max_subpages)

        raw = None
        claude_urls: list[str] = []
        _usage: dict = {}
        try:
            raw, _usage = self._call_claude(system, user_message, max_tokens=1024)
            if raw is None:
                raise ValueError("Claude returned no text")
            print(f"[agent] URL identification raw response: {raw[:500]}")
            # Parse JSON, handling potential markdown fences
            cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            urls = json.loads(cleaned)
            if isinstance(urls, list):
                claude_urls = urls
            elif isinstance(urls, dict):
                list_urls = urls.get("urls") or urls.get("url") or []
                if isinstance(list_urls, list):
                    claude_urls = list_urls
        except (json.JSONDecodeError, Exception) as e:
            print(f"[agent] URL identification failed: {e}")
            print(f"[agent] Raw response was: {raw[:500] if raw else 'None'}")
            claude_urls = candidate_urls  # fallback

        # If Claude only returned UUID/opaque paths, the site likely uses a JS framework
        # (e.g. SchoolBlocks) that hides human-readable URLs from its HTML.
        # Probe common staff directory paths and prepend any that respond.
        import re as _re
        uuid_pattern = _re.compile(r"/pages/[0-9a-f-]{8,}", _re.IGNORECASE)
        all_opaque = claude_urls and all(uuid_pattern.search(u) for u in claude_urls)
        if all_opaque or not claude_urls:
            print("[agent] All candidate URLs appear UUID-based — probing common staff paths...")
            common = await self._probe_common_staff_urls(base_url)
            # Prepend found common paths; keep Claude's UUIDs as fallback
            claude_urls = common + [u for u in claude_urls if u not in common]

        return claude_urls[: self.settings.max_subpages], _usage

    async def fetch_and_clean_pages(
        self, base_url: str, subpage_urls: list[str]
    ) -> list[dict]:
        """
        Fetch each subpage URL and return cleaned content.
        Returns list of {"url": ..., "content": ...} dicts.
        """
        pages = []

        # Always include the homepage
        homepage_html = await fetch_page(base_url)
        if homepage_html:
            cleaned_home = clean_html(homepage_html)
            sis_home = await fetch_schoolinsites_directory(homepage_html, base_url)
            if sis_home:
                cleaned_home = sis_home
            pages.append({"url": base_url, "content": cleaned_home})

        for url in subpage_urls:
            # Skip if it's the same as homepage
            if url.rstrip("/") == base_url.rstrip("/"):
                continue

            raw_html = await fetch_page(url)
            if raw_html:
                cleaned = clean_html(raw_html)

                # Detect SchoolInsites/MyConnectSuite JS-rendered directory widgets
                # and supplement with direct API data (invisible to static scraping).
                sis_data = await fetch_schoolinsites_directory(raw_html, url)
                if sis_data:
                    # The static HTML is mostly empty containers when JS renders
                    # the directory — use only the API data to save tokens.
                    cleaned = sis_data

                # Only include if there's meaningful content
                if len(cleaned) > 100:
                    pages.append({"url": url, "content": cleaned})

                # If this looks like a staff directory, follow pagination (e.g. page 2, 3)
                if looks_like_staff_directory(url):
                    max_extra = getattr(self.settings, "max_directory_pages", 2)
                    pagination_urls = get_pagination_links(raw_html, url, max_next_pages=max_extra)
                    for page_url in pagination_urls:
                        next_html = await fetch_page(page_url)
                        if next_html:
                            next_cleaned = clean_html(next_html)
                            if len(next_cleaned) > 100:
                                pages.append({"url": page_url, "content": next_cleaned})
                                print(f"[agent] Fetched pagination page: {page_url}")

        return pages

    async def extract_contacts(
        self,
        pages: list[dict],
        existing_contacts: list[dict],
        org_name: str,
        website_url: str,
    ) -> dict:
        """
        Step 4: Send all page content + existing CRM contacts to Claude
        for extraction and comparison. Returns parsed JSON result.
        """
        # Build the page content section
        page_sections = []
        for i, page in enumerate(pages, 1):
            # Truncate very long pages to control token usage
            content = page["content"][:8000]
            page_sections.append(f"### Page {i}: {page['url']}\n{content}")

        pages_text = "\n\n".join(page_sections)
        contacts_json = json.dumps(existing_contacts, separators=(",", ":"))

        user_message = (
            f"## District Website Content\n\n"
            f"Organization: {org_name}\n"
            f"Website: {website_url}\n\n"
            f"{pages_text}\n\n"
            f"---\n\n"
            f"## Existing CRM Contacts for This Organization\n\n"
            f"{contacts_json}\n\n"
            f"---\n\n"
            f"Analyze the website content above. Find all contacts matching target roles, "
            f"compare against existing CRM contacts, and return the structured JSON output."
        )

        raw = None
        try:
            raw, _usage = self._call_claude(EXTRACTION_SYSTEM, user_message, max_tokens=4096)
            if raw is None:
                return {"error": "Claude returned no text"}, {}
            cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            result = json.loads(cleaned)
            return (result if result is not None else {"error": "Claude returned empty/null JSON"}), _usage
        except json.JSONDecodeError as e:
            print(f"[agent] JSON parse error: {e}")
            print(f"[agent] Raw response: {raw[:500] if raw else 'None'}")
            return {"error": f"Failed to parse Claude response: {e}"}, {}
        except Exception as e:
            print(f"[agent] Extraction failed: {e}")
            return {"error": str(e)}, {}

    async def run(
        self,
        org_name: str,
        website_url: str,
        existing_contacts: list[dict],
        all_person_names: dict[str, int] | None = None,
    ) -> dict:
        """
        Full agent pipeline: fetch homepage → identify subpages →
        fetch subpages → extract contacts → return results.
        """
        print(f"[agent] Starting research for {org_name} ({website_url})")

        # Step 3a: Fetch homepage
        homepage_html = await fetch_page(website_url)
        if not homepage_html:
            print(f"[agent] Homepage fetch failed for {website_url} (empty/None response)")
            return {"error": f"Could not reach {website_url}"}

        # Step 3b: Identify best subpages
        print("[agent] Identifying relevant subpages...")
        subpage_urls, id_usage = await self.identify_subpages(homepage_html, website_url)
        print(f"[agent] Found {len(subpage_urls)} candidate pages: {subpage_urls}")

        # Step 3c: Fetch and clean all pages
        print("[agent] Fetching and cleaning pages...")
        pages = await self.fetch_and_clean_pages(website_url, subpage_urls)
        print(f"[agent] Cleaned {len(pages)} pages")

        if not pages:
            return {"error": f"No readable pages found on {website_url}"}

        # Step 4: Extract contacts via Claude
        print("[agent] Extracting contacts via Claude...")
        result, ex_usage = await self.extract_contacts(pages, existing_contacts, org_name, website_url)

        if result is None or "error" in result:
            return result if isinstance(result, dict) else {"error": "Extraction returned no result"}

        # Post-process contacts to ensure we promote CONFIRMED records to UPDATED
        # when the website provides an email/phone that is missing in Pipedrive,
        # and to treat major name changes as new people instead of updates.
        contacts = result.get("contacts") or {}
        confirmed = contacts.get("confirmed") or []
        updated = contacts.get("updated") or []
        new = contacts.get("new") or []

        # Index existing CRM contacts by person_id for comparison.
        existing_by_id = {c.get("person_id"): c for c in existing_contacts if c.get("person_id")}

        new_confirmed: list[dict] = []
        for c in confirmed:
            person_id = c.get("pipedrive_person_id")
            existing = existing_by_id.get(person_id)
            if not existing:
                new_confirmed.append(c)
                continue

            had_email = bool(existing.get("email"))
            had_phone = bool(existing.get("phone"))
            has_email_now = bool(c.get("email"))
            has_phone_now = bool(c.get("phone"))

            email_added = (not had_email) and has_email_now
            phone_added = (not had_phone) and has_phone_now

            if email_added or phone_added:
                changes: list[str] = []
                if email_added:
                    changes.append("email")
                if phone_added:
                    changes.append("phone")

                previous_title = existing.get("job_title") or ""
                updated_contact = {
                    "name": c.get("name"),
                    "job_title": c.get("job_title") or previous_title,
                    "previous_title": previous_title,
                    "role_category_id": c.get("role_category_id"),
                    "role_category_label": c.get("role_category_label"),
                    "email": c.get("email"),
                    "email_confidence": c.get("email_confidence"),
                    "phone": c.get("phone"),
                    "source_url": c.get("source_url"),
                    "pipedrive_person_id": person_id,
                    "changes": changes,
                    "notes": c.get("notes") or "",
                }
                updated.append(updated_contact)
            else:
                new_confirmed.append(c)

        contacts["confirmed"] = new_confirmed

        # Helper to decide whether two names are "similar" (likely same person).
        def _names_similar(a: str | None, b: str | None) -> bool:
            if not a or not b:
                return False
            a_norm = a.lower().strip()
            b_norm = b.lower().strip()
            if a_norm == b_norm:
                return True
            a_tokens = set(a_norm.replace(",", "").split())
            b_tokens = set(b_norm.replace(",", "").split())
            # Consider similar if they share at least one token (e.g. same last name)
            return bool(a_tokens & b_tokens)

        # Reclassify UPDATED records that only differ by name and don't look like the same person
        # as NEW contacts instead of PATCHing the existing record's name.
        new_updated: list[dict] = []
        for u in updated:
            changes = u.get("changes") or []
            person_id = u.get("pipedrive_person_id")
            if "name" in changes and person_id in existing_by_id:
                prev = existing_by_id[person_id]
                prev_name = (prev.get("name") or "").strip()
                new_name = (u.get("name") or "").strip()
                if prev_name and new_name and not _names_similar(prev_name, new_name):
                    # Treat as a new person instead of updating the existing record's name
                    new_contact = {
                        "name": new_name,
                        "job_title": u.get("job_title") or prev.get("job_title") or "",
                        "role_category_id": u.get("role_category_id"),
                        "role_category_label": u.get("role_category_label"),
                        "email": u.get("email"),
                        "email_confidence": u.get("email_confidence"),
                        "phone": u.get("phone"),
                        "source_url": u.get("source_url"),
                        "notes": u.get("notes") or "",
                    }
                    new.append(new_contact)
                    continue

            new_updated.append(u)

        contacts["updated"] = new_updated

        # Dedup NEW contacts against every active person in the org (not just
        # those in our target role categories).  Claude only sees the filtered
        # prompt context, so it can recommend adding someone who already exists
        # with a non-target role (e.g. "Director" ID 474, "Other" ID 478).
        if all_person_names:
            deduped_new: list[dict] = []
            for contact in new:
                contact_name = (contact.get("name") or "").strip()
                if not contact_name:
                    deduped_new.append(contact)
                    continue

                # Check for an exact or token-overlap match in the full name index
                matched_id = None
                contact_lower = contact_name.lower()
                contact_tokens = set(contact_lower.replace(",", "").split())

                if contact_lower in all_person_names:
                    matched_id = all_person_names[contact_lower]
                else:
                    for existing_name, pid in all_person_names.items():
                        existing_tokens = set(existing_name.replace(",", "").split())
                        # Require at least 2 tokens to match (first+last), or
                        # an exact single-token match — avoids false positives on
                        # common first names like "Jennifer".
                        shared = contact_tokens & existing_tokens
                        meaningful = {t for t in shared if len(t) > 2}
                        if len(meaningful) >= 2:
                            matched_id = pid
                            break

                if matched_id:
                    print(
                        f"[agent] Skipping NEW recommendation for '{contact_name}' "
                        f"— already exists in Pipedrive (person ID {matched_id})"
                    )
                else:
                    deduped_new.append(contact)
            new = deduped_new

        contacts["new"] = new
        result["contacts"] = contacts

        # Sum token usage from both Claude calls
        def _sum_usage(a: dict, b: dict) -> dict:
            keys = set(a) | set(b)
            return {k: (a.get(k, 0) + b.get(k, 0)) for k in keys}

        result["usage"] = _sum_usage(id_usage, ex_usage)

        print(
            f"[agent] Done. Found: "
            f"{len(result.get('contacts', {}).get('confirmed', []))} confirmed, "
            f"{len(result.get('contacts', {}).get('updated', []))} updated, "
            f"{len(result.get('contacts', {}).get('new', []))} new, "
            f"{len(result.get('contacts', {}).get('missing', []))} missing"
        )

        return result
