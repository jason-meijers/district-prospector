from __future__ import annotations
import asyncio
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import (
    get_settings,
    PIPEDRIVE_TRIGGER_FIELD_KEY,
    PIPEDRIVE_TRIGGER_OPTION_ID,
    PIPEDRIVE_TRIGGER_DONE_ID,
    PIPEDRIVE_EMAIL_DOMAIN_FIELD_KEY,
    PIPEDRIVE_LAST_RESEARCHED_FIELD_KEY,
    ROLE_CATEGORY_OPTIONS,
    ROLE_PRIORITY,
)
from app.pipedrive import PipedriveClient
from app.slack import SlackClient
from app.agent import ExtractionAgent


# ─────────────────────────────────────────────────────────────
# App Setup
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize clients on startup."""
    app.state.pipedrive = PipedriveClient()
    app.state.slack = SlackClient()
    app.state.agent = ExtractionAgent()
    print("[app] Agent service started")
    yield
    print("[app] Agent service stopped")


app = FastAPI(
    title="District Contact Research Agent",
    description="Pipedrive-triggered agent that researches school district websites for contacts",
    version="1.0.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────

class ManualTrigger(BaseModel):
    """For testing — manually trigger research for an org."""
    org_id: int
    website_url: str | None = None  # Override website if needed


# ─────────────────────────────────────────────────────────────
# Core Pipeline
# ─────────────────────────────────────────────────────────────

async def run_research_pipeline(org_id: int, website_override: str | None = None):
    """
    The main async pipeline. Runs in the background after the webhook
    returns 200. Steps:
    1. Fetch org + existing contacts from Pipedrive
    2. Run the extraction agent
    3. Post results to Slack
    """
    settings = get_settings()
    pipedrive = PipedriveClient()
    slack = SlackClient()
    agent = ExtractionAgent()

    try:
        # ── Step 1: Get context from Pipedrive ──────────────────
        print(f"[pipeline] Fetching org {org_id} from Pipedrive...")
        org_data = await pipedrive.get_organization(org_id)
        if not org_data or not org_data.get("id"):
            await slack.post_message(
                f"❌ *Organization not found*\n"
                f"Pipedrive org ID: {org_id}\n"
                f"The organization may have been deleted or the ID is invalid."
            )
            return
        org_name = org_data.get("name", f"Org #{org_id}")

        website_url = website_override or pipedrive.get_org_website(org_data)
        website_was_discovered = False

        if not website_url:
            # No website on file — try to discover it
            website_url = await agent.discover_website(org_name)

            if website_url:
                website_was_discovered = True
                # Write it back to Pipedrive so we have it next time
                try:
                    await pipedrive.update_org_website(org_id, website_url)
                    print(f"[pipeline] Wrote discovered website to Pipedrive: {website_url}")
                except Exception as e:
                    print(f"[pipeline] Failed to write website to Pipedrive: {e}")
            else:
                await slack.post_message(
                    f"❌ *No website found for {org_name}*\n"
                    f"Pipedrive org ID: {org_id}\n"
                    f"Could not find an official website via search either.\n"
                    f"Add a website URL to the organization record and try again."
                )
                return

        print(f"[pipeline] Fetching existing contacts for {org_name}...")
        raw_persons = await pipedrive.get_org_persons(org_id)
        existing_contacts = pipedrive.format_persons_for_prompt(raw_persons)
        all_person_names = pipedrive.get_all_person_names(raw_persons)
        print(
            f"[pipeline] Found {len(existing_contacts)} role-matched contacts "
            f"and {len(all_person_names)} total active persons in Pipedrive"
        )

        # ── Step 2: Run the extraction agent ────────────────────
        result = await agent.run(org_name, website_url, existing_contacts, all_person_names)

        if "error" in result:
            await slack.post_message(
                f"❌ *Research failed for {org_name}*\n"
                f"🔗 {website_url}\n"
                f"Error: {result['error']}"
            )
            return

        # ── Step 3: Post results to Slack ───────────────────────
        contacts = result.get("contacts", {})
        confirmed = contacts.get("confirmed", [])
        updated = contacts.get("updated", [])
        new = contacts.get("new", [])
        missing = contacts.get("missing", [])

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Parent message
        parent_text = slack.format_parent_message(
            district_name=result.get("district_name", org_name),
            website_url=website_url,
            org_id=org_id,
            pipedrive_domain=settings.pipedrive_domain,
            confirmed=len(confirmed),
            updated=len(updated),
            new=len(new),
            missing=len(missing),
        )
        if website_was_discovered:
            parent_text += f"\n\n🔍 _Website was not on file — discovered via search and saved to Pipedrive._"
        thread_ts = await slack.post_message(parent_text)

        if not thread_ts:
            print("[pipeline] Failed to post parent message to Slack")
            return

        # Small delay between thread messages to maintain order
        delay = 0.5

        # Post confirmed contacts
        for contact in confirmed:
            msg = slack.format_confirmed_contact(contact)
            await slack.post_thread(thread_ts, msg)
            await asyncio.sleep(delay)

        # Post updated contacts (person payload + note in same message)
        for contact in updated:
            msg = slack.format_updated_contact(contact, date_str)
            await slack.post_thread(thread_ts, msg)
            await asyncio.sleep(delay)

        # Post new contacts
        for contact in new:
            msg = slack.format_new_contact(contact, org_id, date_str)
            await slack.post_thread(thread_ts, msg)
            await asyncio.sleep(delay)

        # Post missing contacts (person payload + note in same message)
        for contact in missing:
            msg = slack.format_missing_contact(contact, website_url, date_str)
            await slack.post_thread(thread_ts, msg)
            await asyncio.sleep(delay)

        # ── Deal contact switch recommendations ───────────────
        # If a deal's point of contact is MISSING, recommend switching
        # to the best available replacement from confirmed/updated/new.
        if missing:
            try:
                org_deals = await pipedrive.get_org_deals(org_id, status="open")
                print(f"[pipeline] Found {len(org_deals)} open deals for {org_name}")

                # Build a set of person IDs that are missing
                missing_person_ids: set[int] = set()
                missing_by_pid: dict[int, dict] = {}
                for m in missing:
                    pid = m.get("pipedrive_person_id")
                    if pid:
                        missing_person_ids.add(pid)
                        missing_by_pid[pid] = m

                # Build candidate pool from confirmed + updated + new (in that order).
                # Each candidate needs a person_id and role_category_id.
                candidates: list[dict] = []
                for c in confirmed:
                    if c.get("pipedrive_person_id") and c.get("role_category_id"):
                        candidates.append(c)
                for u in updated:
                    if u.get("pipedrive_person_id") and u.get("role_category_id"):
                        candidates.append(u)
                # NEW contacts don't have a pipedrive_person_id yet — they'd need
                # to be created first, so we skip them for automatic switch
                # recommendations.

                # Group deals by their missing person_id
                deals_by_missing_pid: dict[int, list[dict]] = {}
                for deal in org_deals:
                    dpid = deal.get("person_id")
                    if dpid and dpid in missing_person_ids:
                        deals_by_missing_pid.setdefault(dpid, []).append(deal)

                if deals_by_missing_pid and candidates:
                    priority_index = {rid: i for i, rid in enumerate(ROLE_PRIORITY)}
                    fallback = len(ROLE_PRIORITY)

                    def _candidate_sort_key(c: dict, target_role_id: int | None) -> tuple:
                        cid = c.get("role_category_id")
                        same_role = 0 if cid == target_role_id else 1
                        prio = priority_index.get(cid, fallback)
                        return (same_role, prio)

                    for missing_pid, affected_deals in deals_by_missing_pid.items():
                        old_contact = missing_by_pid[missing_pid]
                        old_name = old_contact.get("name") or "Unknown"
                        old_role_label = old_contact.get("previous_title") or "Unknown"

                        # Determine old role category from existing contacts
                        old_role_id: int | None = None
                        for ec in existing_contacts:
                            if ec.get("person_id") == missing_pid:
                                old_role_id = ec.get("role_category_id")
                                break

                        best = min(
                            candidates,
                            key=lambda c: _candidate_sort_key(c, old_role_id),
                        )
                        best_role_label = (
                            ROLE_CATEGORY_OPTIONS.get(best.get("role_category_id", 0))
                            or best.get("role_category_label")
                            or best.get("job_title")
                            or "N/A"
                        )

                        msg = slack.format_switch_contact(
                            old_name=old_name,
                            new_name=best.get("name") or "Unknown",
                            role_label=best_role_label,
                            deals=affected_deals,
                            new_person_id=best["pipedrive_person_id"],
                            org_name=org_name,
                            date_str=date_str,
                        )
                        await slack.post_thread(thread_ts, msg)
                        await asyncio.sleep(delay)
                        print(
                            f"[pipeline] Switch recommendation: "
                            f"{best.get('name')} for {old_name} on "
                            f"{len(affected_deals)} deal(s)"
                        )
            except Exception as e:
                print(f"[pipeline] Deal switch check failed (non-fatal): {e}")

        # Post research notes if present
        notes = result.get("research_notes")
        if notes:
            await slack.post_thread(
                thread_ts,
                f"🔬 *Research Notes*\n{notes}"
            )

        # Post email pattern info
        email_pattern = result.get("email_pattern", {})
        if email_pattern.get("pattern"):
            await slack.post_thread(
                thread_ts,
                f"📧 *Email Pattern Detected*\n"
                f"Pattern: `{email_pattern['pattern']}`\n"
                f"Confidence: {email_pattern.get('confidence', 'unknown')}\n"
                f"Examples: {', '.join(email_pattern.get('examples_found', [])[:2])}"
            )

        # Post token usage as last thread message
        usage = result.get("usage")
        if usage:
            await slack.post_thread(thread_ts, slack.format_usage_message(usage))

        # ── Step 4: Finalise org record in Pipedrive ────────────
        # Extract email domain from any confirmed example address in the
        # email_pattern block (strip the @ and everything before it).
        email_domain: str | None = None
        email_pattern = result.get("email_pattern", {})
        examples = email_pattern.get("examples_found") or []
        for ex in examples:
            if "@" in ex:
                email_domain = ex.split("@", 1)[1].strip().lower()
                break
        # Fallback: try to parse the domain out of the pattern description
        if not email_domain and email_pattern.get("pattern"):
            import re as _re
            m = _re.search(r"@([\w.\-]+)", email_pattern["pattern"])
            if m:
                email_domain = m.group(1).strip().lower()

        org_updates: dict = {
            PIPEDRIVE_TRIGGER_FIELD_KEY: PIPEDRIVE_TRIGGER_DONE_ID,
            PIPEDRIVE_LAST_RESEARCHED_FIELD_KEY: date_str,
        }
        if email_domain:
            org_updates[PIPEDRIVE_EMAIL_DOMAIN_FIELD_KEY] = email_domain

        try:
            await pipedrive.update_organization(org_id, org_updates)
            print(
                f"[pipeline] Org {org_id} updated — trigger→done, "
                f"date={date_str}"
                + (f", domain={email_domain}" if email_domain else "")
            )
        except Exception as e:
            print(f"[pipeline] Failed to update org fields on completion: {e}")

        print(f"[pipeline] Research complete for {org_name}")

    except Exception as e:
        print(f"[pipeline] Unhandled error for org {org_id}: {e}")
        traceback.print_exc()
        try:
            await slack.post_message(
                f"⚠️ *Unexpected error researching org {org_id}*\n"
                f"```{str(e)[:500]}```\n"
                f"Check server logs for details."
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "district-contact-agent"}


@app.post("/webhook/pipedrive")
async def pipedrive_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receives Pipedrive organization update webhooks.
    Checks if the trigger field was toggled, then runs the pipeline
    in the background.
    If WEBHOOK_SECRET is set, requests must include it via header
    X-Webhook-Secret or query param ?secret=...
    """
    settings = get_settings()

    # Optional auth: require shared secret if configured
    if settings.webhook_secret:
        header_secret = request.headers.get("X-Webhook-Secret")
        query_secret = request.query_params.get("secret")
        if header_secret != settings.webhook_secret and query_secret != settings.webhook_secret:
            return JSONResponse(
                content={"status": "unauthorized", "reason": "invalid or missing webhook secret"},
                status_code=401,
            )

    try:
        body = await request.json()
    except Exception:
        return {"status": "ignored", "reason": "invalid JSON"}

    # Pipedrive webhooks have different structures depending on the event
    current = body.get("current", {})
    previous = body.get("previous", {})
    meta = body.get("meta", {})

    # Only process organization updates
    if meta.get("object") != "organization":
        return {"status": "ignored", "reason": "not an organization event"}

    # Check if the trigger field changed to the "Trigger" option (ID 632)
    current_value = current.get(PIPEDRIVE_TRIGGER_FIELD_KEY)
    previous_value = previous.get(PIPEDRIVE_TRIGGER_FIELD_KEY)

    # Only trigger when the dropdown is set to "Trigger" (option ID 632)
    # Pipedrive sends dropdown values as strings or ints depending on context
    trigger_id = str(PIPEDRIVE_TRIGGER_OPTION_ID)
    current_str = str(current_value) if current_value is not None else ""
    previous_str = str(previous_value) if previous_value is not None else ""

    if current_str != trigger_id:
        return {"status": "ignored", "reason": "trigger field not set to Trigger"}

    if current_str == previous_str:
        return {"status": "ignored", "reason": "trigger field unchanged"}

    org_id = current.get("id")
    if not org_id:
        return {"status": "error", "reason": "no org ID found"}

    # Run in background so we return 200 immediately
    background_tasks.add_task(run_research_pipeline, org_id)

    return {"status": "accepted", "org_id": org_id}


@app.post("/trigger")
async def manual_trigger(payload: ManualTrigger, background_tasks: BackgroundTasks):
    """
    Manual trigger for testing. Pass an org_id and optionally a website_url
    to override the one in Pipedrive.
    """
    background_tasks.add_task(
        run_research_pipeline, payload.org_id, payload.website_url
    )
    return {"status": "accepted", "org_id": payload.org_id}
