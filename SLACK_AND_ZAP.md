# Slack Output Format (Block Kit)

All actions now post as Slack Block Kit messages with interactive buttons
that call this service directly. The legacy emoji + `---PAYLOAD---` format
parsed by Zapier is deprecated (see the "Legacy Zapier flow" section at the
bottom for migration notes).

## Slack Channel

All messages go to a single channel, e.g., `#district-research`.

## Architecture Overview

```
Pipeline → Supabase.pending_actions  (row with full Pipedrive payload)
        ↓
        → Slack.chat.postMessage (Block Kit with Approve / Edit / Skip buttons)
        ↓
User clicks button
        ↓
Slack → POST /slack/interact  (payload contains action_id only, not the body)
        ↓
verify signing secret → claim pending_actions row → call Pipedrive →
response_url updates the original Slack message with ✅ / ❌ / ⏭️
```

The important property: **the Pipedrive payload never appears in the Slack
message**. Slack only carries an opaque `pending_action_<id>` action_id.
Auditing goes through Supabase (`pending_actions` table) which records the
full request body, result, and timestamps.

## Parent Message Format (unchanged)

```
✅ *Research complete for {district_name}*
🔗 {website_url}
🏢 Pipedrive Org: <{pipedrive_org_url}|{org_name}>

📊 Results:
• {confirmed_count} confirmed (still listed)
• {updated_count} need updates
• {new_count} new contacts found
• {missing_count} not found on site
```

## Thread Messages (Block Kit)

Each actionable contact posts as a threaded Block Kit message built by
`app/slack_blocks.py`:

- **NEW contact** — "Create" button (primary), "Skip" button.
- **UPDATED contact** — "Apply update" button, "Skip" button.
- **MISSING contact (mark former)** — "Mark as Former", "Leave as-is" button.
- **CONFIRMED contact** — plain text (no buttons, nothing to approve).

Each button's `action_id` is `pending_action_<uuid>` (approve) or
`pending_action_skip` (cancel). Slack sends that id to `/slack/interact`
which looks up the row in `pending_actions` to get the actual Pipedrive
payload.

## Configuration

Required environment variables:

| Variable | Purpose |
|----------|---------|
| `SLACK_BOT_TOKEN` | Posts messages (existing) |
| `SLACK_CHANNEL_ID` | Target channel (existing) |
| `SLACK_SIGNING_SECRET` | Validates `/slack/interact` requests |
| `SLACK_USE_BLOCK_KIT` | Set `true` to enable Block Kit output |
| `PUBLIC_BASE_URL` | For documentation/logging of the interactivity URL |

### Slack App Interactivity

In the Slack app's **Interactivity & Shortcuts** page:

1. Toggle **Interactivity** on.
2. Set **Request URL** to `https://<PUBLIC_BASE_URL>/slack/interact`.
3. Reinstall the app so the signing secret is in use.

Slack sends the request as `application/x-www-form-urlencoded` with a
`payload` field containing JSON. `POST /slack/interact` handles all of that
(see `app/slack_interact.py`).

## Observability

- `pending_actions` table stores the full action lifecycle: `pending`,
  `executed`, `failed`, `cancelled`, with the Pipedrive request body and
  response.
- The Slack message is updated in place via `response_url` so approvers see
  the outcome without scrolling through replies.

## Legacy Zapier Flow (deprecated)

Before Block Kit, each thread message embedded a `---PAYLOAD---` block and
Zapier watched for ✅ reactions to execute the JSON payload. That flow is
fully retained inside `app/slack.py::SlackClient.format_*` as a fallback
when `SLACK_USE_BLOCK_KIT=false`, but should be disabled once the Block Kit
path is verified:

1. Set `SLACK_USE_BLOCK_KIT=true`.
2. Run one batch end-to-end and confirm buttons work.
3. Turn off the Zapier Zaps that parse `---PAYLOAD---` so contacts aren't
   double-created.
4. Remove the legacy formatter methods from `app/slack.py` once no traffic
   has hit them for a full batch.
