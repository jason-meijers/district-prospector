from __future__ import annotations
import json
import re
import time
from urllib.parse import urlparse

import anthropic
from app.config import get_settings
from app.json_llm import parse_llm_json_object

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
ID 474 → Director (**only** curriculum / instruction / teaching & learning / secondary education / CTE leadership — not facilities, finance, HR, technology, security, fine arts, athletics, equity & inclusion, MTSS, expeditions/enrichment-only programs, gifted/GATE-only program roles, or other non-academic departments)
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

**Director titles (important):**
- **Executive Director** is acceptable when it denotes district or academic-program leadership aligned with our targets (e.g. instructional services, CTE, secondary programs).
- **Director of [something]** — include **only** if that something is clearly **curriculum, instruction, teaching & learning, secondary education, CTE, career pathways, or academic services**. Do **not** include directors of unrelated programs (examples below).

## Critical Exclusions

SKIP any person whose title explicitly references:
- Elementary, Primary, K-5, Grades 1-6, or Middle School (e.g. "Elementary Principal", "Director of K-5 Curriculum", "Middle School CTE Coordinator")
- Board members or trustees
- Teachers, counselors, coaches, nurses
- HR, Human Resources, Finance, Accounting, Business, Facilities, Operations, Maintenance, Construction, Custodial, Grounds, IT, Technology, Transportation
- Police, SRO, security, safety (except curriculum/safety coordination that is clearly instructional)
- Purchasing, procurement, vendor management, food service, nutrition
- Fine arts, band, choir, theater, athletics, sports (unless a later product flag explicitly allows arts)
- Corporate relations, partnerships, foundation, communications/marketing **unless** the role is clearly instructional leadership
- **Equity & Inclusion** leadership (e.g. Director of Equity & Inclusion, Chief Diversity Officer) — not a target role
- **MTSS** roles (e.g. MTSS Coordinator, Multi-Tiered System of Supports)
- **Expeditions** or similar enrichment-only program directors (e.g. Director of Expeditions) — not curriculum/CTE/secondary leadership
- **Gifted & talented / GATE / advanced learning** program staff (e.g. Coordinator of Gifted and Learning, Gifted Coordinator) — not a target role

## Role Category Assignment

For each contact assign the BEST matching role_category_id from this list:
{BATCH_ROLE_OPTIONS.strip()}

## District main phone (board office / district switchboard)

District sites almost always show one main phone in the header, footer, or contact block (e.g. \"Phone: 555-123-4567\", \"Central Office\", \"District Office\").

1. Set JSON field `district_main_phone` to that single main district/switchboard number in **###-###-####** form (US), or `null` if truly not visible anywhere in the content.
2. For **each contact**, if the site does **not** list a **direct** phone for that person, set `phone` to `district_main_phone` (when not null). If they **do** have their own extension/number, keep that personal number in `phone`.
3. Only use numbers that clearly belong to the **district or central office**, not random numbers from news stories or unrelated sponsors.

## Email Pattern Detection

In `email_pattern`:
- `examples_found`: list every district-staff email you see in the content (helps verify the rule).
- `confidence`:
  - **high** = the same local-part rule is obvious from **2+** staff emails on the district domain
  - **medium** = **1** clear staff email on the district domain shows the rule
  - **low** = no reliable pattern; do not guess addresses

Per-contact `email` / `email_confidence`:
- **confirmed** = that exact address appears next to this person's name on the site
- **high** or **medium** = you **applied the pattern** to build their address (must match `email_pattern.confidence`)

**Mandatory:** If `email_pattern.confidence` is **high** or **medium**, **no contact may have `email: null`**. You must apply the pattern to build `email` for anyone missing a printed address, and set their `email_confidence` to **high** or **medium** (same tier as the pattern, or **high** when the pattern is **high**).

If you are not sure enough to fill everyone, you must set `email_pattern.confidence` to **low** instead.

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

BATCH_URL_TRIAGE_SYSTEM = """You pick which URLs to scrape for **district leadership and curriculum/CTE contacts** for a U.S. K-12 school district.

You receive the district name, homepage URL, and a **numbered list of candidate URLs** discovered via the site map (each with optional title/description). You must **only** choose URLs whose path appears in that list — copy the `url` value **exactly** as given (same string).

**staff_directory_url**: The single best page that lists **many district staff** (district directory, staff directory, faculty/staff listing, leadership team with multiple names, "our team", etc.). Prefer district-wide directories over a single school. Set to `null` if none of the candidates clearly fit.

**enrichment_urls**: Up to the allowed count of **extra** pages that help find superintendent, curriculum/instruction leaders, CTE, or district office contacts — e.g. leadership, administration, curriculum & instruction, CTE, human resources **only if** it clearly lists district cabinet/leadership contacts, "contact us", about/district administration. Do **not** pick: police, security, athletics-only, elementary-only pages, fine arts, maintenance, facilities, purchasing, board policy-only, calendars, news.

**include_homepage**: `true` if we should also scrape the homepage for superintendent name, main phone, or district office info (default `true` unless the homepage is useless and everything needed is on other chosen URLs).

**rationale**: One short sentence for logs.

Output **only** valid JSON with these keys:
{"staff_directory_url": null or string, "enrichment_urls": [], "include_homepage": true, "rationale": "string"}
No markdown fences."""


def _normalize_triage_url(u: str) -> str:
    try:
        p = urlparse((u or "").strip())
        if not p.netloc:
            return (u or "").strip().rstrip("/")
        return f"{p.scheme or 'https'}://{p.netloc.lower()}{p.path}".rstrip("/")
    except Exception:
        return (u or "").strip().rstrip("/")


def _sanitize_triage_plan(
    parsed: dict | None,
    allowed: set[str],
    homepage_key: str,
    enrich_cap: int,
) -> dict:
    """Keep only URLs present in ``allowed``; enforce enrichment cap."""
    out: dict = {
        "staff_directory_url": None,
        "enrichment_urls": [],
        "include_homepage": True,
        "rationale": "",
    }
    if not parsed or not isinstance(parsed, dict):
        out["rationale"] = "invalid_or_empty_json"
        return out

    out["rationale"] = str(parsed.get("rationale") or "")[:500]

    su = parsed.get("staff_directory_url")
    if su:
        key = _normalize_triage_url(str(su))
        if key in allowed:
            out["staff_directory_url"] = key

    raw_enrich = parsed.get("enrichment_urls")
    if isinstance(raw_enrich, list):
        seen: set[str] = set()
        for item in raw_enrich:
            if len(out["enrichment_urls"]) >= max(0, enrich_cap):
                break
            key = _normalize_triage_url(str(item))
            if not key or key == homepage_key:
                continue
            if out["staff_directory_url"] and key == out["staff_directory_url"]:
                continue
            if key not in allowed:
                continue
            if key in seen:
                continue
            seen.add(key)
            out["enrichment_urls"].append(key)

    ih = parsed.get("include_homepage")
    if ih is False:
        out["include_homepage"] = False
    elif ih is True:
        out["include_homepage"] = True

    if out["staff_directory_url"] and out["staff_directory_url"] == homepage_key:
        # Homepage as directory is valid; avoid duplicating in enrichment list (handled in scraper).
        pass

    return out


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


def _normalize_contact_phone(raw: str | None) -> str | None:
    """
    Normalize phone to ###-###-####; preserve extension as ' x1234' when present.
    Returns None if empty; returns original stripped string if not a US 10/11 digit core.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    ext_m = re.search(r"(?:\s*(?:x|ext\.?)\s*)(\d{1,6})\s*$", s, re.I)
    ext = ext_m.group(1) if ext_m else None
    core = s[: ext_m.start()].strip() if ext_m else s
    normalized = _normalize_us_phone_str(core)
    if not normalized:
        return s
    return f"{normalized} x{ext}" if ext else normalized


def _name_parts_for_email(name: str) -> tuple[str, str]:
    """Given display name, return (first_token, last_token) lowercased, letters only in tokens."""
    s = (name or "").strip()
    s = re.sub(r",\s*(Ph\.?D\.?|Ed\.?D\.?|Jr\.?|Sr\.?|III|II|IV|M\.?D\.?|Esq\.?).*$", "", s, flags=re.I)
    s = re.sub(r"^(Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?)\s+", "", s, flags=re.I)
    parts = re.findall(r"[A-Za-z][A-Za-z'\-]*", s)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0].lower(), ""
    return parts[0].lower(), parts[-1].lower()


def _email_formulas():
    """(label, fn(first,last)->local). Order: try more specific rules when learning from pairs."""

    def fi_l(first: str, last: str) -> str | None:
        return f"{first[0]}{last}" if first and last else None

    def l_fi(first: str, last: str) -> str | None:
        return f"{last}{first[0]}" if first and last else None

    def fi_dot_l(first: str, last: str) -> str | None:
        return f"{first[0]}.{last}" if first and last else None

    def f_dot_l(first: str, last: str) -> str | None:
        return f"{first}.{last}" if first and last else None

    def flast(first: str, last: str) -> str | None:
        return f"{first}{last}" if first and last else None

    def l_dot_fi(first: str, last: str) -> str | None:
        return f"{last}.{first[0]}" if first and last else None

    def l_f2(first: str, last: str) -> str | None:
        return f"{last}{first[0:2]}" if first and last and len(first) >= 2 else None

    def f2_l(first: str, last: str) -> str | None:
        return f"{first[0:2]}{last}" if first and last and len(first) >= 2 else None

    def l_f2i(first: str, last: str) -> str | None:
        """e.g. Jenny Johnson -> johnsonje"""
        return f"{last}{first[0]}{first[1]}" if first and last and len(first) >= 2 else None

    def l_f3(first: str, last: str) -> str | None:
        return f"{last}{first[0:3]}" if first and last and len(first) >= 3 else None

    return (
        ("l_f2i", l_f2i),
        ("l_f3", l_f3),
        ("l_f2", l_f2),
        ("f2_l", f2_l),
        ("fi_l", fi_l),
        ("l_fi", l_fi),
        ("f_dot_l", f_dot_l),
        ("fi_dot_l", fi_dot_l),
        ("l_dot_fi", l_dot_fi),
        ("flast", flast),
    )


def _learn_email_local_formula(
    pairs: list[tuple[str, str]],
    pattern_hint: str | None,
) -> tuple[str, callable] | None:
    """
    Find the one formula that maps every (name, local) pair to the same local part.
    pairs: (display_name, email_local_lower)
    Returns (formula_key, fn(first,last)->str|None).
    """
    if not pairs:
        return None
    hint = (pattern_hint or "").lower()
    formulas = list(_email_formulas())
    matching: list[tuple[str, callable]] = []
    for label, fn in formulas:
        ok = True
        for name, loc in pairs:
            first, last = _name_parts_for_email(name)
            try:
                pred = fn(first, last)
            except Exception:
                ok = False
                break
            if pred is None or pred.lower() != loc.lower():
                ok = False
                break
        if ok:
            matching.append((label, fn))
    if len(matching) == 1:
        return matching[0]
    if len(matching) > 1 and hint:
        for label, fn in matching:
            if label.replace("_", "") in hint.replace("+", "").replace(" ", "") or label in hint:
                return (label, fn)
        if "first" in hint and "initial" in hint and "last" in hint:
            for label, fn in matching:
                if label.startswith("fi_l") or label == "fi_dot_l":
                    return (label, fn)
        if "last" in hint and "first" in hint:
            for label, fn in matching:
                if label.startswith("l_f"):
                    return (label, fn)
    return None


def _email_domain_for_inference(email_pattern: dict, contacts: list[dict]) -> str | None:
    for ex in email_pattern.get("examples_found") or []:
        if isinstance(ex, str) and "@" in ex.strip():
            return ex.strip().split("@", 1)[1].lower()
    for c in contacts:
        e = (c.get("email") or "").strip()
        if "@" in e:
            return e.rsplit("@", 1)[-1].lower()
    return None


def _pairs_from_contacts_for_learning(contacts: list[dict], domain: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    dlow = domain.lower()
    for c in contacts:
        name = (c.get("name") or "").strip()
        e = (c.get("email") or "").strip().lower()
        if not name or "@" not in e:
            continue
        loc, dom = e.rsplit("@", 1)
        if dom != dlow:
            continue
        pairs.append((name, loc))
    return pairs


def _backfill_emails_from_pattern(contacts: list[dict], email_pattern: dict) -> int:
    """
    When Claude set pattern confidence high/medium but left emails null, learn the
    local-part rule from contacts who have an address on the same domain and apply it.
    """
    conf = (email_pattern.get("confidence") or "low").lower()
    examples = email_pattern.get("examples_found") or []
    if conf not in ("high", "medium"):
        return 0
    if conf == "medium" and len(examples) < 1 and not any(
        (c.get("email") or "").strip() for c in contacts
    ):
        return 0

    domain = _email_domain_for_inference(email_pattern, contacts)
    if not domain:
        return 0

    pairs = _pairs_from_contacts_for_learning(contacts, domain)
    learned = _learn_email_local_formula(pairs, email_pattern.get("pattern"))
    if not learned:
        if conf == "high" and len(examples) >= 2:
            print(
                "[batch_agent] email_pattern high but could not learn formula from contacts "
                f"(pairs={len(pairs)}, examples={len(examples)})"
            )
        return 0

    label, fn = learned
    filled = 0
    infer_conf = "high" if conf == "high" or len(pairs) >= 2 else "medium"
    for c in contacts:
        if (c.get("email") or "").strip():
            continue
        first, last = _name_parts_for_email(c.get("name") or "")
        local = fn(first, last)
        if not local:
            continue
        c["email"] = f"{local.lower()}@{domain}"
        c["email_confidence"] = infer_conf
        filled += 1
    if filled:
        print(f"[batch_agent] Inferred {filled} email(s) using learned formula ({label}) @ {domain}")
    return filled


# ─────────────────────────────────────────────────────────────
# Per-page-type adaptive character budgets
# ─────────────────────────────────────────────────────────────

# URL path segments that signal a staff directory / leadership page and
# justify spending more characters on that page's content.
_DIRECTORY_HINT_SEGMENTS = (
    "staff", "directory", "leadership", "administration",
    "cabinet", "superintendent", "departments", "department",
    "curriculum", "cte", "career-tech",
)


def _classify_page_type(url: str, content: str, website_url: str | None) -> str:
    """
    Classify a scraped page so :func:`extract_contacts_raw` can allocate the
    right character budget to it. Returns one of:
      ``"platform_api"`` — full text, no truncation
      ``"directory"``    — big chunk (~30k chars)
      ``"homepage"``     — trimmed (~3k chars)
      ``"enrichment"``   — medium trim (~6k chars)
    """
    u = (url or "").strip()
    lower_url = u.lower()

    if "[schoolinsites api]" in lower_url or "[platform api]" in lower_url:
        return "platform_api"

    if website_url:
        base = website_url.rstrip("/")
        stripped_url = u.split(" [", 1)[0].rstrip("/")
        if stripped_url == base:
            return "homepage"

    if any(seg in lower_url for seg in _DIRECTORY_HINT_SEGMENTS):
        return "directory"

    # Tall pages with many "Name — Title" rows are almost always staff
    # directories even if the URL doesn't name them as such.
    if content:
        sample = content[:20_000]
        hits = len(re.findall(r"\b[A-Z][a-z]+\s+[A-Z][a-zA-Z\-']+\s*[—-]\s*[A-Z][A-Za-z/ ]+", sample))
        if hits >= 8:
            return "directory"

    return "enrichment"


def _budget_for_page_type(page_type: str, default_chars: int) -> int | None:
    """
    Return the char cap for a page. ``None`` means "no truncation".
    ``default_chars`` is the legacy uniform cap — used as a floor for
    directory/homepage so we never shrink below the old behaviour unless
    explicitly configured lower.
    """
    if page_type == "platform_api":
        return None
    if page_type == "directory":
        return max(default_chars, 30_000)
    if page_type == "homepage":
        return min(default_chars, 3_000)
    return min(default_chars, 6_000)


def _split_paragraphs(text: str) -> list[str]:
    """Split page content into paragraphs using blank-line boundaries."""
    if not text:
        return []
    return [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]


def _dedup_paragraphs_across_pages(pages: list[dict]) -> list[dict]:
    """
    Drop paragraphs that repeat across pages (nav blocks, site-wide footers,
    "quick links" menus) before sending content to Claude. Keeps the first
    occurrence so the page that originally surfaced a paragraph retains it.
    """
    if not pages:
        return pages

    seen: set[str] = set()
    deduped: list[dict] = []
    dropped = 0
    for page in pages:
        content = page.get("content") or ""
        paragraphs = _split_paragraphs(content)
        if not paragraphs:
            deduped.append(dict(page))
            continue
        kept: list[str] = []
        for para in paragraphs:
            # Skip very short fragments — they're more likely to be false
            # positives (dates, button labels) than reusable boilerplate.
            if len(para) < 60:
                kept.append(para)
                continue
            key = re.sub(r"\s+", " ", para.lower())[:240]
            if key in seen:
                dropped += 1
                continue
            seen.add(key)
            kept.append(para)
        deduped.append({**page, "content": "\n\n".join(kept)})

    if dropped:
        print(f"[batch_agent] Dropped {dropped} duplicate paragraph(s) across pages")
    return deduped


def _normalize_all_contact_phones(contacts: list[dict]) -> None:
    for c in contacts:
        raw = c.get("phone")
        if raw is None or raw == "":
            continue
        n = _normalize_contact_phone(raw)
        if n:
            c["phone"] = n


def _normalize_all_contact_emails(contacts: list[dict]) -> None:
    for c in contacts:
        e = c.get("email")
        if isinstance(e, str) and e.strip():
            c["email"] = e.strip().lower()


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

    def _call_claude_triage(self, user_message: str, max_tokens: int = 512) -> tuple[str | None, dict]:
        """
        Cheap model URL triage — no prompt caching (small system prompt).
        """
        max_retries = 3
        retry_delay = 60

        for attempt in range(max_retries):
            try:
                response = self.client.messages.create(
                    model=self.settings.batch_url_triage_model,
                    max_tokens=max_tokens,
                    system=BATCH_URL_TRIAGE_SYSTEM,
                    messages=[{"role": "user", "content": user_message}],
                )

                usage: dict = {}
                if getattr(response, "usage", None):
                    u = response.usage
                    usage = {
                        "input_tokens": getattr(u, "input_tokens", 0) or 0,
                        "output_tokens": getattr(u, "output_tokens", 0) or 0,
                        "cache_creation_input_tokens": getattr(
                            u, "cache_creation_input_tokens", 0
                        )
                        or 0,
                        "cache_read_input_tokens": getattr(u, "cache_read_input_tokens", 0)
                        or 0,
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
                    wait = retry_delay * (2**attempt)
                    print(
                        f"[batch_agent] Triage rate limit — waiting {wait}s (attempt {attempt + 2}/{max_retries}): {e}"
                    )
                    time.sleep(wait)
                else:
                    print("[batch_agent] Triage rate limit — all retries exhausted")
                    raise
            except anthropic.NotFoundError:
                raise
            except Exception as e:
                print(f"[batch_agent] Triage Claude call failed: {type(e).__name__}: {e}")
                return None, {}

        return None, {}

    def triage_urls_from_map(
        self,
        district_name: str,
        district_state: str | None,
        homepage_url: str,
        candidates: list[dict],
    ) -> tuple[dict, dict]:
        """
        Select staff directory + enrichment URLs from map metadata via a cheap LLM.
        Only URLs present in ``candidates`` may appear in the result.

        Returns:
            (plan_dict, usage_dict); plan has staff_directory_url, enrichment_urls,
            include_homepage, rationale.
        """
        empty_usage: dict = {}
        if not candidates:
            return {
                "staff_directory_url": None,
                "enrichment_urls": [],
                "include_homepage": True,
                "rationale": "no_candidates",
            }, empty_usage

        home_key = _normalize_triage_url(homepage_url)
        allowed: set[str] = {home_key}
        for row in candidates:
            u = (row.get("url") or "").strip()
            if u:
                allowed.add(_normalize_triage_url(u))

        lines: list[str] = []
        for i, row in enumerate(candidates, 1):
            u = row.get("url") or ""
            t = (row.get("title") or "").replace("\n", " ")[:200]
            d = (row.get("description") or "").replace("\n", " ")[:250]
            lines.append(f"{i}. url: {u}\n   title: {t}\n   desc: {d}")

        loc = f", {district_state}" if district_state else ""
        user_message = (
            f"District: {district_name}{loc}\n"
            f"Homepage: {home_key}\n"
            f"Max enrichment URLs to return (hard cap): {self.settings.batch_enrichment_url_cap}\n\n"
            f"Candidates:\n"
            + "\n\n".join(lines)
            + "\n\nReturn JSON only."
        )

        raw, usage = self._call_claude_triage(user_message)
        if not raw:
            return {
                "staff_directory_url": None,
                "enrichment_urls": [],
                "include_homepage": True,
                "rationale": "triage_no_response",
            }, usage

        parsed = parse_llm_json_object(raw)
        if parsed is None:
            print("[batch_agent] Triage JSON parse error: could not parse JSON object")
            print(f"[batch_agent] Triage raw (first 400 chars): {raw[:400]}")

        cap = max(0, int(self.settings.batch_enrichment_url_cap))
        plan = _sanitize_triage_plan(parsed, allowed, home_key, cap)
        return plan, usage

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

        default_chars = self.settings.batch_chars_per_page
        deduped_pages = _dedup_paragraphs_across_pages(pages)

        page_sections: list[str] = []
        total_chars = 0
        budget_log: list[str] = []
        for i, page in enumerate(deduped_pages, 1):
            url = page.get("url") or ""
            content = page.get("content") or ""
            page_type = _classify_page_type(url, content, website_url)
            cap = _budget_for_page_type(page_type, default_chars)
            trimmed = content if cap is None else content[:cap]
            total_chars += len(trimmed)
            budget_log.append(f"{page_type}:{len(trimmed)}")
            page_sections.append(f"### Page {i}: {url}\n{trimmed}")

        print(
            f"[batch_agent] page budgets for {org_name}: "
            + ", ".join(budget_log)
            + f" (total={total_chars} chars)"
        )

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

        result = parse_llm_json_object(raw)
        if not result:
            print(f"[batch_agent] JSON parse error for {org_name}: could not parse JSON object")
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

        _backfill_emails_from_pattern(clean_contacts, email_pattern)
        _normalize_all_contact_emails(clean_contacts)
        _normalize_all_contact_phones(clean_contacts)

        print(
            f"[batch_agent] Extracted {len(clean_contacts)} contacts for {org_name} "
            f"(input: {usage.get('input_tokens', 0):,} tokens, "
            f"output: {usage.get('output_tokens', 0):,} tokens)"
        )

        if notes:
            print(f"[batch_agent] Notes for {org_name}: {notes}")

        return clean_contacts, email_pattern, usage
