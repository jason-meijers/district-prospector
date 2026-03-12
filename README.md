# District Contact Research Agent

A Pipedrive-triggered agent that researches school district websites to find, verify, and enrich contacts matching target roles. Results post to Slack for human review — approve with an emoji, and a Zap executes the Pipedrive API call.

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Anthropic API key
- Pipedrive API token
- Slack Bot Token (with `chat:write` scope)
- A Slack channel for results

### 2. Setup

```bash
# Clone / navigate to project
cd district-contact-agent

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Copy and fill in environment variables
cp .env.example .env
# Edit .env with your actual keys
```

### 3. Pipedrive Setup

1. **Create a custom field** on Organizations:
   - Name: `Research Contacts`
   - Type: Single option (with one option like "Run") or Checkbox
   - Note the field API key (Settings → Data Fields → Organization)

2. **Create a custom field** on Persons:
   - Name: `Role Category`
   - Type: Single option
   - Options: `Superintendent`, `Curriculum`, `CTE`
   - Note the field API key

3. **Create a webhook** in Pipedrive:
   - Settings → Webhooks → New Webhook
   - Event: `updated.organization`
   - URL: `https://your-deployment-url.com/webhook/pipedrive`

4. **Update `.env`** with the field keys from steps 1-2.

### 4. Slack Setup

1. **Create a Slack app** at https://api.slack.com/apps
2. **Add Bot Token Scopes**: `chat:write`, `chat:write.public`
3. **Install to workspace** and copy the Bot Token (`xoxb-...`)
4. **Create a channel** like `#district-research`
5. **Invite the bot** to the channel
6. **Update `.env`** with the bot token and channel ID

### 5. Run Locally

```bash
uvicorn app.main:app --reload --port 8080
```

### 6. Test

```bash
# Manual trigger (replace with a real Pipedrive org ID)
curl -X POST http://localhost:8080/trigger \
  -H "Content-Type: application/json" \
  -d '{"org_id": 12345, "website_url": "https://www.avdistrict.org"}'
```

### 7. Deploy

**Railway:**
```bash
railway init
railway up
```

**Render:**
- Connect your Git repo
- Set environment variables in the dashboard
- Deploy will auto-detect the Dockerfile

**Google Cloud Run:**
```bash
gcloud builds submit --tag gcr.io/YOUR_PROJECT/district-agent
gcloud run deploy district-agent --image gcr.io/YOUR_PROJECT/district-agent --allow-unauthenticated
```

After deploying, update your Pipedrive webhook URL to point to the live endpoint.

## Architecture

```
Pipedrive webhook → FastAPI endpoint → Background task:
  1. Fetch org + existing contacts from Pipedrive
  2. Fetch district homepage
  3. Claude identifies best subpages to scrape
  4. Fetch + clean subpages
  5. Claude extracts contacts + compares to CRM
  6. Post results to Slack (parent message + threaded actions)

Slack emoji reaction → Zapier → Pipedrive API call
```

## Files

```
district-contact-agent/
├── app/
│   ├── __init__.py
│   ├── main.py          # FastAPI app, endpoints, pipeline orchestration
│   ├── config.py         # Environment config via pydantic-settings
│   ├── agent.py          # Claude prompts + extraction logic
│   ├── pipedrive.py      # Pipedrive API client
│   ├── slack.py          # Slack message formatting + posting
│   └── scraper.py        # HTML fetching, cleaning, link extraction
├── .env.example
├── requirements.txt
├── Dockerfile
├── ARCHITECTURE.md       # Detailed architecture spec
├── PROMPTS.md            # Claude prompt documentation
├── SLACK_AND_ZAP.md      # Slack format + Zapier workflow spec
└── README.md
```

## Cost

~$0.10 per district on Claude Sonnet. At 500 districts: ~$50 total.

## Zapier Setup

See `SLACK_AND_ZAP.md` for the full Zapier workflow spec, including:
- Trigger configuration (emoji reaction listener)
- JavaScript code step for parsing the `---PAYLOAD---` block
- Router paths for create/update/note actions
- Duplicate prevention logic
