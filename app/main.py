from __future__ import annotations
import asyncio
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
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

        # Post deal alerts for missing contacts who are still the main contact on open deals
        if missing:
            try:
                org_deals = await pipedrive.get_org_deals(org_id, status="open")
                missing_pids = {m.get("pipedrive_person_id") for m in missing if m.get("pipedrive_person_id")}
                deals_by_pid: dict[int, list[dict]] = {}
                for deal in org_deals:
                    dpid = deal.get("person_id")
                    if dpid and dpid in missing_pids:
                        deals_by_pid.setdefault(dpid, []).append(deal)

                for contact in missing:
                    pid = contact.get("pipedrive_person_id")
                    affected = deals_by_pid.get(pid)
                    if affected:
                        msg = slack.format_former_on_deals(contact, affected, settings.pipedrive_domain)
                        await slack.post_thread(thread_ts, msg)
                        await asyncio.sleep(delay)
            except Exception as e:
                print(f"[pipeline] Deal check for missing contacts failed (non-fatal): {e}")

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
        print("[webhook] raw body:", body)
    except Exception:
        print("[webhook] invalid JSON payload")
        return {"status": "ignored", "reason": "invalid JSON"}

    # Pipedrive webhooks have different structures depending on the event
    # Support both the older shape {current, previous, meta.object}
    # and the v2 shape {data, previous, meta.entity, data.custom_fields}.
    meta = body.get("meta", {}) or {}

    trigger_field_present_in_current = False
    trigger_field_present_in_previous = False

    if "data" in body:
        # v2 style payload (what we're currently seeing in logs)
        data = body.get("data") or {}
        previous = body.get("previous") or {}
        data_custom = data.get("custom_fields") or {}
        prev_custom = previous.get("custom_fields") or {}
        trigger_field_present_in_current = PIPEDRIVE_TRIGGER_FIELD_KEY in data_custom
        trigger_field_present_in_previous = PIPEDRIVE_TRIGGER_FIELD_KEY in prev_custom
        current_value_raw = data_custom.get(PIPEDRIVE_TRIGGER_FIELD_KEY)
        previous_value_raw = prev_custom.get(PIPEDRIVE_TRIGGER_FIELD_KEY)
        org_id = data.get("id")
        object_type = meta.get("entity")
    else:
        # legacy style payload
        current = body.get("current", {}) or {}
        previous = body.get("previous", {}) or {}
        trigger_field_present_in_current = PIPEDRIVE_TRIGGER_FIELD_KEY in current
        trigger_field_present_in_previous = PIPEDRIVE_TRIGGER_FIELD_KEY in previous
        current_value_raw = current.get(PIPEDRIVE_TRIGGER_FIELD_KEY)
        previous_value_raw = previous.get(PIPEDRIVE_TRIGGER_FIELD_KEY)
        org_id = current.get("id")
        object_type = meta.get("object")

    # Only process organization updates
    if object_type != "organization":
        print("[webhook] ignored: not an organization event, meta:", meta)
        return {"status": "ignored", "reason": "not an organization event"}

    def _extract_option_id(value):
        """Pipedrive may send dropdowns as plain IDs or {id, type} objects."""
        if isinstance(value, dict) and "id" in value:
            return value.get("id")
        return value

    # Only run when we can confirm the trigger FIELD itself changed.
    # Some Pipedrive update payloads can omit previous custom fields for
    # unrelated changes, which otherwise causes false positives if current
    # still equals the trigger option.
    if not trigger_field_present_in_current or not trigger_field_present_in_previous:
        print(
            "[webhook] ignored: trigger field not explicitly present in both current and previous payloads",
            "present_current:", trigger_field_present_in_current,
            "present_previous:", trigger_field_present_in_previous,
        )
        return {"status": "ignored", "reason": "trigger field not explicitly changed"}

    # Check if the trigger field changed to the "Trigger" option (ID 632)
    current_value = _extract_option_id(current_value_raw)
    previous_value = _extract_option_id(previous_value_raw)

    # Only trigger when the dropdown is set to "Trigger" (option ID 632)
    # Pipedrive sends dropdown values as strings or ints depending on context
    trigger_id = str(PIPEDRIVE_TRIGGER_OPTION_ID)
    current_str = str(current_value) if current_value is not None else ""
    previous_str = str(previous_value) if previous_value is not None else ""

    if current_str != trigger_id:
        print(
            "[webhook] ignored: trigger field not set to Trigger",
            "current:", current_value_raw,
            "previous:", previous_value_raw,
        )
        return {"status": "ignored", "reason": "trigger field not set to Trigger"}

    if current_str == previous_str:
        print("[webhook] ignored: trigger field unchanged")
        return {"status": "ignored", "reason": "trigger field unchanged"}

    if not org_id:
        print("[webhook] error: no org ID found, meta/body:", meta, body)
        return {"status": "error", "reason": "no org ID found"}

    # Run in background so we return 200 immediately
    print(f"[webhook] accepted: starting pipeline for org {org_id}")
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


# ─────────────────────────────────────────────────────────────
# Batch Pipeline Endpoints
# ─────────────────────────────────────────────────────────────

class BatchRunRequest(BaseModel):
    limit: int = 50
    run_dedup: bool = True


class DistrictImportRow(BaseModel):
    name: str
    website_url: Optional[str] = None
    state: Optional[str] = None
    pipedrive_org_id: Optional[int] = None


class DistrictImportRequest(BaseModel):
    districts: list[DistrictImportRow]


class ApproveContactsRequest(BaseModel):
    new_contact_ids: list[str]


class BatchTestUrlRequest(BaseModel):
    website_url: str
    org_name: str = "Manual test district"
    run_extract: bool = True


async def _run_batch_background(limit: int, run_dedup: bool) -> None:
    from app.batch_runner import run_batch
    try:
        summary = await run_batch(limit=limit, run_dedup=run_dedup)
        print(f"[batch endpoint] Batch complete: {summary}")
    except Exception as e:
        print(f"[batch endpoint] Batch run failed: {e}")
        traceback.print_exc()


async def _sync_pipedrive_background() -> None:
    from app.sync import sync_pipedrive_contacts
    try:
        summary = await sync_pipedrive_contacts()
        print(f"[sync endpoint] Contacts sync complete: {summary}")
    except Exception as e:
        print(f"[sync endpoint] Contacts sync failed: {e}")
        traceback.print_exc()


async def _sync_districts_background(skip_existing: bool) -> None:
    from app.sync import sync_districts_from_pipedrive
    try:
        summary = await sync_districts_from_pipedrive(skip_existing=skip_existing)
        print(f"[sync endpoint] Districts sync complete: {summary}")
    except Exception as e:
        print(f"[sync endpoint] Districts sync failed: {e}")
        traceback.print_exc()


@app.post("/batch/run")
async def start_batch_run(payload: BatchRunRequest, background_tasks: BackgroundTasks):
    """
    Start a batch run. Claims up to `limit` districts from Supabase
    (status `manual` first — most recently updated; then `pending` FIFO)
    and processes them asynchronously. Returns immediately.
    """
    settings = get_settings()
    if not settings.supabase_url or not settings.firecrawl_api_key:
        raise HTTPException(
            status_code=503,
            detail="Batch pipeline not configured. Set SUPABASE_URL, SUPABASE_SERVICE_KEY, and FIRECRAWL_API_KEY."
        )
    background_tasks.add_task(_run_batch_background, payload.limit, payload.run_dedup)
    return {"status": "accepted", "limit": payload.limit, "run_dedup": payload.run_dedup}


@app.get("/batch/status")
async def batch_status():
    """Return district queue counts by status."""
    try:
        from app.database import get_district_counts
        counts = get_district_counts()
        return {"status": "ok", "districts": counts}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/batch/sync-pipedrive")
async def sync_pipedrive(background_tasks: BackgroundTasks):
    """
    Manually trigger a Pipedrive contacts sync.
    Fetches all persons from Pipedrive and rebuilds the snapshot table.
    """
    settings = get_settings()
    if not settings.supabase_url:
        raise HTTPException(status_code=503, detail="SUPABASE_URL not configured")
    background_tasks.add_task(_sync_pipedrive_background)
    return {"status": "accepted", "message": "Pipedrive sync started in background"}


@app.post("/districts/import")
async def import_districts(payload: DistrictImportRequest):
    """
    Bulk import districts. Each row needs at minimum a name.
    Optional: website_url, state, pipedrive_org_id.
    Imported districts start with status='pending' and join the work queue.
    """
    try:
        from app.database import import_districts as db_import
        rows = [d.model_dump() for d in payload.districts]
        count = db_import(rows)
        return {"status": "ok", "imported": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SyncDistrictsRequest(BaseModel):
    skip_existing: bool = True


@app.post("/districts/sync-from-pipedrive")
async def sync_districts_from_pipedrive(
    payload: SyncDistrictsRequest,
    background_tasks: BackgroundTasks,
):
    """
    Pull all Pipedrive organizations into the districts table.

    - skip_existing=true (default): only adds new orgs, leaves existing rows untouched.
    - skip_existing=false: also updates name and website_url for existing rows.

    This is the easiest way to seed the districts table from your existing
    Pipedrive data. Run once to populate, then add any non-Pipedrive districts
    via /districts/import.
    """
    settings = get_settings()
    if not settings.supabase_url:
        raise HTTPException(status_code=503, detail="SUPABASE_URL not configured")
    background_tasks.add_task(_sync_districts_background, payload.skip_existing)
    return {
        "status": "accepted",
        "message": "Districts sync from Pipedrive started in background",
        "skip_existing": payload.skip_existing,
    }


@app.post("/batch/approve")
async def approve_contacts(payload: ApproveContactsRequest, background_tasks: BackgroundTasks):
    """
    Approve a list of new_contact IDs for Pipedrive write.
    Triggers the Pipedrive person creation for each approved contact.
    """
    if not payload.new_contact_ids:
        return {"status": "ok", "queued": 0}
    background_tasks.add_task(_write_approved_to_pipedrive, payload.new_contact_ids)
    return {"status": "accepted", "queued": len(payload.new_contact_ids)}


@app.post("/batch/test-url")
async def batch_test_url(payload: BatchTestUrlRequest):
    """
    Manual test helper: scrape a single website URL and optionally run
    extraction, without touching districts queue/status tables.
    """
    settings = get_settings()
    if not settings.firecrawl_api_key:
        raise HTTPException(status_code=503, detail="FIRECRAWL_API_KEY not configured")

    try:
        from app.firecrawl_scraper import scrape_district

        pages = await scrape_district(payload.website_url)
        page_urls = [p.get("url") for p in pages if p.get("url")]
        page_lengths = [
            {"url": p.get("url"), "chars": len((p.get("content") or ""))}
            for p in pages
        ]

        if not payload.run_extract:
            return {
                "status": "ok",
                "website_url": payload.website_url,
                "pages_scraped": len(pages),
                "page_urls": page_urls,
                "page_lengths": page_lengths,
                "contacts_found": 0,
                "contacts": [],
                "token_usage": {},
            }

        if not settings.anthropic_api_key:
            raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

        from app.batch_agent import BatchExtractionAgent
        agent = BatchExtractionAgent()
        contacts, email_pattern, usage = agent.extract_contacts_raw(
            pages=pages,
            org_name=payload.org_name,
            website_url=payload.website_url,
        )

        return {
            "status": "ok",
            "website_url": payload.website_url,
            "pages_scraped": len(pages),
            "page_urls": page_urls,
            "page_lengths": page_lengths,
            "contacts_found": len(contacts),
            "contacts": contacts,
            "email_pattern": email_pattern,
            "token_usage": usage,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


async def _write_approved_to_pipedrive(new_contact_ids: list[str]) -> None:
    """
    For each approved new_contact, create the person in Pipedrive and
    mark the row as approved with the created timestamp.
    """
    from app.pipedrive_writer import write_approved_contacts
    try:
        results = await write_approved_contacts(new_contact_ids)
        print(f"[approve] Wrote {results['created']} contacts to Pipedrive, {results['errors']} errors")
    except Exception as e:
        print(f"[approve] Pipedrive write failed: {e}")
        traceback.print_exc()
