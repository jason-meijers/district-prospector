from __future__ import annotations
import asyncio
import traceback
from app.config import get_settings
from app.firecrawl_scraper import scrape_district, discover_district_website
from app.batch_agent import BatchExtractionAgent
from app.database import (
    claim_next_district,
    mark_district_processing,
    mark_district_done,
    mark_district_error,
    save_found_contacts,
    run_dedup_job,
    reset_district_to_pending,
)
from app.slack import SlackClient


async def _process_one_district(
    district: dict,
    agent: BatchExtractionAgent,
    semaphore: asyncio.Semaphore,
) -> dict:
    """
    Process a single district: scrape → extract → save.

    Wrapped in the semaphore so at most `batch_concurrency` districts
    run in parallel at any time. All errors are caught so a single
    district failure doesn't abort the batch.

    Returns a summary dict for logging.
    """
    district_id = district["id"]
    org_name = district["name"]
    website_url = district.get("website_url") or ""
    pipedrive_org_id = district.get("pipedrive_org_id")

    async with semaphore:
        print(f"[batch] Starting: {org_name} ({website_url})")
        mark_district_processing(district_id)

        try:
            async def _persist_discovered_website(discovered_url: str) -> None:
                # Save to districts table.
                from supabase import create_client
                db = create_client(settings.supabase_url, settings.supabase_service_key)
                db.table("districts").update({"website_url": discovered_url}).eq("id", district_id).execute()

                # Write back to Pipedrive if we have an org ID.
                if pipedrive_org_id:
                    try:
                        from app.pipedrive import PipedriveClient
                        pd = PipedriveClient()
                        await pd.update_org_website(pipedrive_org_id, discovered_url)
                        print(f"[batch] Saved discovered website to Pipedrive for {org_name}")
                    except Exception as pd_err:
                        print(f"[batch] Could not write website back to Pipedrive for {org_name}: {pd_err}")

            # Step 1a: Discover website if missing
            if not website_url:
                print(f"[batch] No website for {org_name} — searching via Firecrawl...")
                website_url = await discover_district_website(org_name)

                if website_url:
                    await _persist_discovered_website(website_url)
                else:
                    msg = "No website found via Firecrawl search"
                    print(f"[batch] Skipping {org_name}: {msg}")
                    mark_district_error(district_id, msg)
                    return {"district": org_name, "status": "error", "reason": msg, "contacts": 0}

            # Step 1b: Scrape via Firecrawl
            pages = await scrape_district(website_url)
            if not pages:
                print(f"[batch] {org_name}: No content from listed website ({website_url}) — attempting rediscovery...")
                fallback_url = await discover_district_website(org_name)

                if fallback_url and fallback_url.rstrip("/") != website_url.rstrip("/"):
                    print(f"[batch] {org_name}: trying fallback website {fallback_url}")
                    await _persist_discovered_website(fallback_url)
                    website_url = fallback_url
                    pages = await scrape_district(website_url)

                if not pages:
                    msg = f"No content retrieved from {website_url}"
                    print(f"[batch] {org_name}: {msg}")
                    mark_district_error(district_id, msg)
                    return {"district": org_name, "status": "error", "reason": msg, "contacts": 0}

            used_urls = [p.get("url") for p in pages if p.get("url")]

            # Step 2: Extract contacts via Claude
            contacts, email_pattern, usage = agent.extract_contacts_raw(
                pages, org_name, website_url
            )

            # Step 3: Persist to Supabase
            save_found_contacts(district_id, pipedrive_org_id, contacts)
            mark_district_done(district_id)

            n = len(contacts)
            print(
                f"[batch] Done: {org_name} — {n} contacts found "
                f"(in: {usage.get('input_tokens', 0):,} tok, "
                f"out: {usage.get('output_tokens', 0):,} tok)"
            )
            return {
                "district": org_name,
                "status": "done",
                "contacts": n,
                "usage": usage,
                "website_url": website_url,
                "used_urls": used_urls,
                "contacts_detail": contacts,
            }

        except Exception as e:
            msg = f"{type(e).__name__}: {str(e)[:500]}"
            print(f"[batch] Error processing {org_name}: {msg}")
            traceback.print_exc()
            mark_district_error(district_id, msg)
            return {
                "district": org_name,
                "status": "error",
                "reason": msg,
                "contacts": 0,
                "website_url": website_url,
                "used_urls": [],
                "contacts_detail": [],
            }


def _format_batch_contact_line(contact: dict) -> str:
    name = (contact.get("name") or "").strip() or "Unknown"
    role = (contact.get("job_title") or "").strip() or "Unknown role"
    role_category = contact.get("role_category") or contact.get("role_category_id") or "Unknown"
    email = contact.get("email") or "N/A"
    phone = contact.get("phone") or "N/A"
    return f"• {name} | {email} | {phone} | {role_category} | {role}"


async def _post_batch_result_to_slack(result: dict) -> None:
    try:
        slack = SlackClient()
        district = result.get("district", "Unknown district")
        status = result.get("status")
        website = result.get("website_url") or "N/A"
        used_urls = result.get("used_urls") or []
        contacts = result.get("contacts_detail") or []

        if status == "error":
            reason = result.get("reason") or "Unknown error"
            text = (
                f"❌ *Batch district failed*\n"
                f"District: {district}\n"
                f"Website attempted: {website}\n"
                f"Reason: {reason}"
            )
            await slack.post_message(text)
            return

        parent = (
            f"✅ *Batch district complete*\n"
            f"District: {district}\n"
            f"Website used: {website}\n"
            f"Contacts found: {len(contacts)}"
        )
        thread_ts = await slack.post_message(parent)
        if not thread_ts:
            return

        if used_urls:
            urls_preview = "\n".join(f"• {u}" for u in used_urls[:20])
            more_urls = len(used_urls) - min(len(used_urls), 20)
            if more_urls > 0:
                urls_preview += f"\n• ... and {more_urls} more URL(s)"
            await slack.post_thread(thread_ts, f"🌐 *URLs used*\n{urls_preview}")

        if contacts:
            lines = [_format_batch_contact_line(c) for c in contacts[:30]]
            more_contacts = len(contacts) - min(len(contacts), 30)
            body = "\n".join(lines)
            if more_contacts > 0:
                body += f"\n• ... and {more_contacts} more contact(s)"
            await slack.post_thread(
                thread_ts,
                "👥 *Contacts found* (`name | email | phone | role category | role`)\n" + body,
            )
        else:
            await slack.post_thread(thread_ts, "👥 *Contacts found*\nNone")
    except Exception as e:
        print(f"[batch] Slack post failed (non-fatal): {e}")


async def run_batch(limit: int = 50, run_dedup: bool = True) -> dict:
    """
    Main batch runner. Claims up to `limit` pending districts from Supabase
    and processes them with concurrency controlled by batch_concurrency setting.

    Args:
        limit: max number of districts to process in this run
        run_dedup: if True, runs the dedup job after all districts are processed

    Returns a summary dict with counts and total token usage.
    """
    settings = get_settings()

    if not settings.supabase_url:
        raise RuntimeError("SUPABASE_URL must be set to run batch processing")
    if not settings.firecrawl_api_key:
        raise RuntimeError("FIRECRAWL_API_KEY must be set to run batch processing")

    districts = []
    for _ in range(limit):
        claimed = claim_next_district()
        if not claimed:
            break
        districts.append(claimed)
    if not districts:
        print("[batch] No pending districts in queue")
        return {"processed": 0, "done": 0, "errors": 0, "total_contacts": 0}

    print(f"[batch] Starting batch run: {len(districts)} districts (concurrency={settings.batch_concurrency})")

    agent = BatchExtractionAgent()
    semaphore = asyncio.Semaphore(settings.batch_concurrency)

    tasks = [
        _process_one_district(district, agent, semaphore)
        for district in districts
    ]
    results = await asyncio.gather(*tasks)

    # Post per-district visibility to Slack (non-fatal).
    await asyncio.gather(*[_post_batch_result_to_slack(r) for r in results], return_exceptions=True)

    # Summarise
    done = sum(1 for r in results if r["status"] == "done")
    errors = sum(1 for r in results if r["status"] == "error")
    total_contacts = sum(r.get("contacts", 0) for r in results)
    total_input_tokens = sum(r.get("usage", {}).get("input_tokens", 0) for r in results if isinstance(r.get("usage"), dict))
    total_output_tokens = sum(r.get("usage", {}).get("output_tokens", 0) for r in results if isinstance(r.get("usage"), dict))

    print(
        f"[batch] Run complete — {done} done, {errors} errors, "
        f"{total_contacts} contacts found, "
        f"{total_input_tokens:,} input tokens, {total_output_tokens:,} output tokens"
    )

    # Run dedup to populate new_contacts for review
    new_unique = 0
    if run_dedup and done > 0:
        print("[batch] Running dedup job...")
        try:
            new_unique = run_dedup_job()
        except Exception as e:
            print(f"[batch] Dedup job failed (non-fatal): {e}")

    return {
        "processed": len(districts),
        "done": done,
        "errors": errors,
        "total_contacts": total_contacts,
        "new_unique_contacts": new_unique,
        "token_usage": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
        },
    }


async def reset_stale_processing_districts(max_age_minutes: int = 30) -> int:
    """
    Reset any districts stuck in 'processing' status back to 'pending'.
    This handles cases where a worker crashed mid-run without updating status.
    Returns the number of districts reset.
    """
    from supabase import create_client
    from datetime import datetime, timezone, timedelta

    settings = get_settings()
    db = create_client(settings.supabase_url, settings.supabase_service_key)
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)).isoformat()

    result = (
        db.table("districts")
        .select("id, name")
        .eq("status", "processing")
        .lt("updated_at", cutoff)
        .execute()
    )
    stale = result.data or []
    for d in stale:
        reset_district_to_pending(d["id"])
        print(f"[batch] Reset stale district to pending: {d['name']} ({d['id']})")

    return len(stale)
