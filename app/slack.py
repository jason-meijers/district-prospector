from __future__ import annotations
import json
import re
import httpx
from app.config import (
    get_settings,
    PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY,
    PIPEDRIVE_SALUTATION_FIELD_KEY,
    ROLE_CATEGORY_OPTIONS,
    SALUTATION_OPTIONS,
)


def format_research_run_summary(result: dict) -> str | None:
    """
    Human-readable breakdown of scrape + extraction strategies for Slack reviewers.

    ``result`` matches the dict returned by :func:`run_firecrawl_research`.
    Returns None if ``result`` is unusable (should not normally happen).
    """
    if not isinstance(result, dict):
        return None

    settings = get_settings()
    extraction_model = getattr(settings, "claude_model", "") or "Claude extraction model"
    triage_model = getattr(settings, "batch_url_triage_model", "") or "Haiku"

    mode_raw = (result.get("research_mode") or "pipeline").lower()
    strategy_lines = {
        "pipeline": (
            "`pipeline` — fixed scrape + extraction only (no ContactHunter)."
        ),
        "hybrid": (
            "`hybrid` — full scrape + extraction first; ContactHunter runs only "
            "if superintendent / curriculum / CTE-style roles are still missing."
        ),
        "full_agent": (
            "`full_agent` — ContactHunter carries the whole research step "
            "(batch map/triage path skipped)."
        ),
    }
    strategy = strategy_lines.get(mode_raw, f"`{mode_raw}`")

    ut = result.get("url_triage")
    ut = ut if isinstance(ut, dict) else {}

    discovery: list[str] = []
    if ut.get("used_platform_adapter"):
        plat = ut.get("platform") if isinstance(ut.get("platform"), dict) else {}
        pname = plat.get("platform") or "unknown CMS"
        conf = plat.get("confidence")
        npg = plat.get("pages_returned")
        suffix = ""
        if conf is not None:
            suffix += f", confidence `{conf}`"
        if npg is not None:
            suffix += f", `{npg}` page(s)"
        discovery.append(
            "• *Where pages came from:* Platform adapter detected "
            f"`{pname}`{suffix}. Structured/directory-style text was fetched directly; "
            "Firecrawl map + LLM URL triage were skipped."
        )
    elif ut.get("used_full_agent"):
        discovery.append(
            "• *Where pages came from:* Skipped for this mode — ContactHunter "
            "tool calls (map/scrape/search/platform) gathered content instead "
            "of the batch directory pipeline."
        )
    elif ut.get("used_heuristic"):
        rationale = (ut.get("rationale") or "").strip()
        discovery.append(
            "• *Where pages came from:* **Heuristic URL fall back** — the normal "
            "step is Firecrawl `map` → shortlist links → Haiku picks directory + "
            "enrichment URLs. That path did not produce usable targets, so the "
            "service fell back to scoring/heuristic URL selection and scraping "
            "those pages instead."
        )
        if rationale:
            discovery.append(f"  _Signal: {rationale}_")
    else:
        discovery.append(
            "• *Where pages came from:* **LLM URL triage** "
            f"(`{triage_model}`) chose staff directory + enrichment URLs from "
            "Firecrawl map results, then those URLs were scraped."
        )

    discovery.append(
        "• *Who extracted contacts:* Batch LLM extraction "
        f"(`{extraction_model}`) on cleaned markdown from each page "
        "(per-page character budgets + cross-page dedup where enabled)."
    )

    hunter = result.get("hunter")
    hunter = hunter if isinstance(hunter, dict) else {}

    if hunter.get("skipped"):
        why = hunter.get("reason") or "n/a"
        discovery.append(
            f"• *ContactHunter (gap-fill):* Skipped — `{why}`."
        )
    elif hunter.get("hunt_id"):
        tc = hunter.get("tool_calls")
        stop = hunter.get("stop_reason") or "n/a"
        elapsed = hunter.get("elapsed_seconds")
        el_s = f", `{elapsed}s`" if elapsed is not None else ""
        mb = hunter.get("missing_before") or []
        mb_s = ""
        if mb:
            mb_s = f" Missing cohorts before hunt: {', '.join(str(x) for x in mb)}."
        discovery.append(
            "• *ContactHunter:* Ran for missing roles — "
            f"`{tc}` tool call(s), stop=`{stop}`{el_s}.{mb_s}"
        )
    elif mode_raw == "hybrid":
        discovery.append(
            "• *ContactHunter:* Did not run (no gap-fill trigger or pipeline-only outcome)."
        )
    elif mode_raw == "pipeline":
        discovery.append(
            "• *ContactHunter:* Not used (pipeline-only mode)."
        )

    lines = [
        "🧩 *How this run got the data*",
        f"• *Strategy:* {strategy}",
        *discovery,
    ]
    return "\n".join(lines)


def slack_plaintext_no_autolink(value: str | None) -> str:
    """
    Slack clients auto-link emails (mailto) and phone numbers (tel), which breaks
    copy/paste into Pipedrive. Inline code (backticks) suppresses that behavior.
    """
    if value is None or value == "":
        return value or ""
    s = str(value)
    if "`" in s:
        s = s.replace("`", "'")
    return f"`{s}`"


def sanitize_email_for_pipedrive(raw: str | None) -> str | None:
    """Strip mailto: / HTML junk from scraped or pasted values."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.rstrip(">").strip()
    low = s.lower()
    if low.startswith("mailto:"):
        s = s[7:].strip()
    if "?" in s and "@" in s:
        s = s.split("?", 1)[0].strip()
    return s if "@" in s else None


def sanitize_phone_for_pipedrive(raw: str | None) -> str | None:
    """
    Strip tel: URIs and fix duplicate US numbers when href + visible text were concatenated.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.rstrip(">").strip()
    if s.lower().startswith("tel:"):
        s = s[4:].strip()
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    # e.g. same 10-digit US number twice: 98974334719897433471
    if len(digits) == 20 and digits[:10] == digits[10:]:
        digits = digits[:10]
    elif len(digits) == 22 and digits[0] == "1" and digits[:11] == digits[11:]:
        digits = digits[:11]
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == "1":
        d = digits[1:]
        return f"{d[:3]}-{d[3:6]}-{d[6:]}"
    # Non-US or extension: return digits-only or lightly cleaned original
    return s


def format_slack_source_url_line(source_url: str | None) -> str:
    """Plain URL in backticks — avoids <url|label> breaking when users copy/paste."""
    u = (source_url or "").strip()
    if not u:
        return "📄 Source: N/A"
    return f"📄 Source: {slack_plaintext_no_autolink(u)}"


def _escape_mrkdwn_code_fence(s: str) -> str:
    """So JSON/note text cannot break out of ``` fences."""
    return s.replace("```", "`\u200b``")


class SlackClient:
    """Post messages and threads to Slack via the Web API."""

    def __init__(self):
        self.settings = get_settings()
        self.token = self.settings.slack_bot_token
        self.channel = self.settings.slack_channel_id
        self.base_url = "https://slack.com/api"

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    async def post_message(self, text: str, blocks: list | None = None) -> str | None:
        """
        Post a message to the channel. Returns the message timestamp (ts)
        which is used as thread_ts for replies.
        """
        payload = {
            "channel": self.channel,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        if blocks:
            payload["blocks"] = blocks

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{self.base_url}/chat.postMessage",
                headers=self._headers(),
                json=payload,
            )
            data = resp.json()
            if not data.get("ok"):
                print(f"[slack] Error posting message: {data.get('error')}")
                return None
            return data.get("ts")

    async def post_thread(
        self, thread_ts: str, text: str, blocks: list | None = None
    ) -> str | None:
        """
        Post a reply in a thread. Use ``blocks`` for Block Kit (e.g. ``verbatim`` code
        sections so Slack does not auto-link emails/phones/URLs inside payloads).
        """
        payload = {
            "channel": self.channel,
            "thread_ts": thread_ts,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        if blocks:
            payload["blocks"] = blocks
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{self.base_url}/chat.postMessage",
                headers=self._headers(),
                json=payload,
            )
            data = resp.json()
            if not data.get("ok"):
                print(f"[slack] Error posting thread: {data.get('error')}")
                return None
            return data.get("ts")

    # ─────────────────────────────────────────────────────────────
    # Message Formatting
    # ─────────────────────────────────────────────────────────────

    def format_parent_message(
        self,
        district_name: str,
        website_url: str,
        org_id: int,
        pipedrive_domain: str,
        confirmed: int,
        updated: int,
        new: int,
        missing: int,
    ) -> str:
        org_url = f"https://{pipedrive_domain}/organization/{org_id}"
        return (
            f"*Research complete for {district_name}*\n"
            f"🔗 {website_url}\n"
            f"🏢 Pipedrive Org: <{org_url}|{district_name}>\n\n"
            f"📊 Results:\n"
            f"• {confirmed} confirmed (still listed)\n"
            f"• {updated} need updates\n"
            f"• {new} new contacts found\n"
            f"• {missing} not found on site"
        )

    def format_usage_message(self, usage: dict) -> str:
        """Format token usage (write, read, cache, total) for a run."""
        write = usage.get("output_tokens", 0)
        read = usage.get("input_tokens", 0)
        cache_write = usage.get("cache_creation_input_tokens", 0)
        cache_read = usage.get("cache_read_input_tokens", 0)
        cache_total = cache_write + cache_read
        total = read + write
        return (
            f"📊 *Token usage (this run)*\n"
            f"• Write (output): {write:,}\n"
            f"• Read (input): {read:,}\n"
            f"• Cache: {cache_total:,} (read: {cache_read:,}, write: {cache_write:,})\n"
            f"• Total: {total:,}"
        )

    def format_new_contact(self, contact: dict, org_id: int, date_str: str) -> str:
        role_field_key = PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY
        email_val = sanitize_email_for_pipedrive(contact.get("email"))
        phone_val = sanitize_phone_for_pipedrive(contact.get("phone"))
        if email_val:
            email_display = (
                f"{slack_plaintext_no_autolink(email_val)} "
                f"_({contact.get('email_confidence', 'low')} confidence)_"
            )
        else:
            email_display = "Not found"
        phone_display = (
            slack_plaintext_no_autolink(phone_val) if phone_val else "Not found"
        )

        # Strip a recognised salutation from the name and map it to the
        # Pipedrive salutation custom field.
        raw_name = contact["name"] or ""
        salutation_id: int | None = None
        clean_name = raw_name
        for prefix, opt_id in SALUTATION_OPTIONS.items():
            # Match "Dr. " or "Dr " at the start of the name (case-insensitive)
            prefix_bare = prefix.rstrip(".")
            if raw_name.lower().startswith(f"{prefix.lower()} ") or raw_name.lower().startswith(f"{prefix_bare.lower()} "):
                salutation_id = opt_id
                clean_name = raw_name[len(prefix):].lstrip(" ").lstrip(". ").strip()
                # Re-strip in case the prefix was without the period (e.g. "Dr ")
                if not clean_name:
                    clean_name = raw_name  # safety fallback
                break

        # Pipedrive v2 POST /api/v2/persons body
        custom_fields: dict = {role_field_key: contact.get("role_category_id")}
        if salutation_id is not None:
            custom_fields[PIPEDRIVE_SALUTATION_FIELD_KEY] = salutation_id

        body = {
            "name": clean_name,
            "org_id": org_id,
            "job_title": contact["job_title"],
            "custom_fields": custom_fields,
        }
        if email_val:
            body["emails"] = [{"value": email_val, "primary": True, "label": "work"}]
        if phone_val:
            body["phones"] = [{"value": phone_val, "primary": True, "label": "work"}]

        note_content = (
            f"Prospecting Bot: {contact['name']} added from district website.\n"
            f"Source: {contact.get('source_url', 'N/A')}\n"
            f"Email confidence: {contact.get('email_confidence', 'N/A')}\n"
            f"Agent notes: {contact.get('notes', '')}\n"
            f"Researched: {date_str}"
        )
        source_line = format_slack_source_url_line(contact.get("source_url"))
        person_body_json = json.dumps(body, indent=2)
        role_label = contact.get("role_category_label") or contact.get("role_category") or "N/A"
        pjson = _escape_mrkdwn_code_fence(person_body_json)
        ntxt = _escape_mrkdwn_code_fence(note_content)

        # Single `text` message (no Block Kit): Zapier and integrations need the full API body
        # in one field; Block Kit sections truncate and show "Show more".
        return (
            f"🆕 *CREATE: {contact['name']}*\n"
            f"📋 Title: {contact['job_title']}\n"
            f"🏷️ Role Category: {role_label}\n"
            f"📧 Email: {email_display}\n"
            f"📞 Phone: {phone_display}\n"
            f"{source_line}\n"
            f"📝 {contact.get('notes', '')}\n\n"
            f"*Action: Create Person*\n"
            f"--Payload Start--\n"
            f"```\n{pjson}\n```\n"
            f"--Payload End--\n\n"
            f"*Action: Add Note*\n"
            f"--Note Start--\n"
            f"```\n{ntxt}\n```\n"
            f"--Note End--"
        )

    def format_updated_contact(self, contact: dict, date_str: str) -> str:
        changes_display = "\n".join(
            f"  • {c}" for c in contact.get("changes", [])
        )

        # Pipedrive v2 PATCH body: job_title, emails, phones, and role category when title changes
        body: dict = {}
        if "title" in contact.get("changes", []):
            body["job_title"] = contact["job_title"]
            # If Claude provided a role_category_id, use it; otherwise choose a best-fit ID.
            role_id = contact.get("role_category_id")
            if not role_id:
                # Heuristic: try to infer from any ROLE_CATEGORY_OPTIONS whose label appears in the title
                title_lower = (contact.get("job_title") or "").lower()
                best_match_id = None
                for opt_id, label in ROLE_CATEGORY_OPTIONS.items():
                    # Skip "Former" here; we handle that in missing-contact flow
                    if label == "Former":
                        continue
                    if label.lower() in title_lower:
                        best_match_id = opt_id
                        break
                # Fallback to "Other" (478) if nothing matches
                role_id = best_match_id or 478
            body.setdefault("custom_fields", {})
            body["custom_fields"][PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY] = role_id
        if "email" in contact.get("changes", []):
            ev = sanitize_email_for_pipedrive(contact.get("email"))
            if ev:
                body["emails"] = [{"value": ev, "primary": True, "label": "work"}]
        if "phone" in contact.get("changes", []):
            pv = sanitize_phone_for_pipedrive(contact.get("phone"))
            if pv:
                body["phones"] = [{"value": pv, "primary": True, "label": "work"}]

        note_content = (
            f"Prospecting Bot: Updated {contact['name']} on {date_str}.\n"
            f"Source: {contact.get('source_url', 'N/A')}\n"
            f"Agent notes: {contact.get('notes', '')}"
        )
        source_line = format_slack_source_url_line(contact.get("source_url"))
        person_body_json = json.dumps(body, indent=2)
        person_id = contact['pipedrive_person_id']
        pjson = _escape_mrkdwn_code_fence(person_body_json)
        ntxt = _escape_mrkdwn_code_fence(note_content)
        return (
            f"✏️ *UPDATE: {contact['name']}* (ID: {person_id})\n"
            f"📋 Changes:\n{changes_display}\n"
            f"{source_line}\n"
            f"📝 {contact.get('notes', '')}\n\n"
            f"*Action: Update Person*\n"
            f"Person ID: {person_id}\n"
            f"--Payload Start--\n"
            f"```\n{pjson}\n```\n"
            f"--Payload End--\n\n"
            f"*Action: Add Note*\n"
            f"--Note Start--\n"
            f"```\n{ntxt}\n```\n"
            f"--Note End--"
        )

    def format_note_payload(self, person_id: int, content: str, action_label: str = "add_note") -> str:
        """Format a standalone note for the notes endpoint (separate thread message)."""
        return (
            f"📎 *Action: Add Note*\n"
            f"Person ID: {person_id}\n"
            f"--Note Start--\n"
            f"```\n{content}\n```\n"
            f"--Note End--"
        )

    def format_confirmed_contact(self, contact: dict) -> str:
        u = (contact.get("source_url") or "").strip()
        if u:
            source_line = f"📄 Confirmed on: {slack_plaintext_no_autolink(u)}"
        else:
            source_line = "📄 Source: N/A"
        return (
            f"✅ *CONFIRMED: {contact['name']}*\n"
            f"📋 Title: {contact['job_title']}\n"
            f"{source_line}\n"
            f"_No action needed — contact verified on current site._"
        )

    def format_former_on_deals(self, contact: dict, deals: list[dict], pipedrive_domain: str) -> str:
        name = contact.get("name") or "Unknown"
        deal_lines = "\n".join(
            f"• <https://{pipedrive_domain}/deal/{d['deal_id']}|{d['title'] or 'Deal #' + str(d['deal_id'])}>"
            for d in deals
        )
        count = len(deals)
        return (
            f"📋 *{name}* is marked Former but is still the main contact on "
            f"{count} open deal{'s' if count != 1 else ''}:\n"
            f"{deal_lines}\n\n"
            f"_Recommend reassigning these deals to an active contact._"
        )

    def format_missing_contact(self, contact: dict, website_url: str, date_str: str) -> str:
        previous_title = (contact.get("previous_title") or "Unknown").strip()

        note_content = (
            f"Prospecting Bot: {contact['name']} was not found on the district "
            f"website ({website_url}). It is likely they are no longer at the district. "
            f"Manual verification recommended."
        )
        # Pipedrive v2 PATCH body: only custom_fields (Role Category → Former); job_title unchanged
        person_body = {
            "custom_fields": {
                PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY: 475,
            },
        }
        person_body_json = json.dumps(person_body, indent=2)
        person_id = contact['pipedrive_person_id']
        pjson = _escape_mrkdwn_code_fence(person_body_json)
        ntxt = _escape_mrkdwn_code_fence(note_content)
        return (
            f"⚠️ *NOT FOUND: {contact['name']}* (ID: {person_id})\n"
            f"📋 Last known title: {previous_title}\n"
            f"📝 Not found on current district website. Recommend updating Role Category → Former (see payload below).\n\n"
            f"*Action: Update Person*\n"
            f"Person ID: {person_id}\n"
            f"--Payload Start--\n"
            f"```\n{pjson}\n```\n"
            f"--Payload End--\n\n"
            f"*Action: Add Note*\n"
            f"--Note Start--\n"
            f"```\n{ntxt}\n```\n"
            f"--Note End--"
        )
