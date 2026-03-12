# District Contact Research Agent — Architecture & Prompt Spec

## Overview

An agentic service that, when triggered from Pipedrive, researches a school district's website to find, verify, and enrich contacts matching target roles. Results are posted to Slack for human review, then executed against Pipedrive via emoji-triggered Zaps.

---

## System Flow

```
┌─────────────┐     Webhook      ┌──────────────────┐
│  Pipedrive   │ ──────────────► │  Agent Service    │
│  (field      │   org_id +      │  (FastAPI on      │
│   toggled)   │   website_url   │   Railway/Render) │
└─────────────┘                  └────────┬─────────┘
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                        Pipedrive    District     Anthropic
                        API          Website      API (Sonnet)
                        (get org +   (fetch +     (extract +
                         contacts)    parse)       reason)
                              │           │           │
                              └───────────┼───────────┘
                                          ▼
                                    ┌───────────┐
                                    │   Slack    │
                                    │  Channel   │
                                    └─────┬─────┘
                                          │
                                    emoji reaction
                                          │
                                          ▼
                                    ┌───────────┐
                                    │  Zapier    │
                                    │  (execute  │
                                    │   API call)│
                                    └───────────┘
```

---

## Step-by-Step Flow

### Step 1: Trigger

A custom field on the Pipedrive Organization (e.g., `Research Contacts` — single option or checkbox) is toggled. Pipedrive fires a webhook to the agent's endpoint.

**Webhook payload contains:**
- `current.id` → Pipedrive org ID
- `current.{website_field_key}` → district website URL

The agent endpoint returns `200 OK` immediately and processes asynchronously (background task).

### Step 2: Gather Context from Pipedrive

Agent makes two Pipedrive API calls:

1. **GET /organizations/{id}** → org name, website, any custom fields
2. **GET /organizations/{id}/persons** → all existing Person records linked to this org

For each existing person, extract: name, job title, email, phone, Pipedrive person ID.

### Step 3: Fetch & Clean District Website

Agent fetches the district homepage, strips boilerplate (nav, footer, scripts, styles, CSS), and keeps content + all internal links.

Then uses Claude (Sonnet) to identify which subpages are most likely to contain staff directories, leadership teams, department contacts, or CTE/academic program staff. Claude returns a ranked list of URLs to fetch next.

Agent fetches up to 5 subpages, cleans each one.

**HTML cleaning strategy (Python):**
- Remove `<script>`, `<style>`, `<nav>`, `<footer>` tags
- Remove elements with class/id containing: `menu`, `nav`, `sidebar`, `cookie`, `banner`
- Keep `<main>`, `<article>`, `<section>`, `<table>`, `<div>` with content
- Strip all HTML attributes except `href` on `<a>` tags
- Convert to plain text with link annotations
- This alone cuts token usage ~60-70%

### Step 4: Claude Extracts & Reasons

Send all cleaned page content + existing Pipedrive contacts to Claude Sonnet with the extraction prompt (see below). Claude returns a structured JSON response with:

- **Confirmed contacts** — existing Pipedrive person found on website, still in role
- **Updated contacts** — existing person found but with changed title/phone/email
- **New contacts** — found on website, not in Pipedrive, matches target roles
- **Missing contacts** — in Pipedrive but NOT found anywhere on current website

### Step 5: Post to Slack

**Parent message:**
```
✅ Research complete for {org_name}
🔗 {website_url}
📊 {confirmed_count} confirmed · {updated_count} updates · {new_count} new · {missing_count} not found
```

**Threaded messages (one per action):**

Each thread message follows this exact format so the Zap can parse it:

**For NEW contacts:**
```
🆕 CREATE: {name}
Title: {job_title}
Role Category: {role_category}
Email: {email} ({confidence})
Phone: {phone}
Source: {source_page_url}
Notes: {agent_reasoning}

---PAYLOAD---
{
  "action": "create_person",
  "data": {
    "name": "Chris Grado",
    "org_id": 12345,
    "email": [{"value": "cgrado@avhsd.org", "primary": true}],
    "phone": [{"value": "(661) 948-7655", "primary": true}],
    "job_title": "Asst. Superintendent, Educational Services",
    "ROLE_CATEGORY_FIELD_KEY": "Curriculum",
    "note": "Email inferred from district pattern (firstinitial+last@avhsd.org). Found on Leadership Team page. Title maps to Curriculum/Instruction Director equivalent."
  }
}
```

**For UPDATED contacts:**
```
✏️ UPDATE: {name} (Pipedrive ID: {person_id})
Change: {what_changed}
Before: {old_value}
After: {new_value}
Source: {source_page_url}

---PAYLOAD---
{
  "action": "update_person",
  "person_id": 67890,
  "data": {
    "job_title": "Deputy Superintendent"
  }
}
```

**For CONFIRMED contacts (no action needed, informational only):**
```
✅ CONFIRMED: {name}
Title: {job_title}
Still listed on: {source_page_url}
```
(No payload — no emoji needed)

**For MISSING contacts:**
```
⚠️ NOT FOUND: {name} (Pipedrive ID: {person_id})
Last known title: {job_title}
Notes: Not found on current district website. May have left, or site may have changed. Recommend manual verification.

---PAYLOAD---
{
  "action": "update_person",
  "person_id": 11111,
  "data": {
    "note": "Agent research (DATE): Not found on district website. Recommend verification."
  }
}
```

### Step 6: Zapier Execution

A Zap watches the Slack channel for a specific emoji reaction (e.g., ✅ or 🚀) on threaded messages.

When triggered:
1. Grab the message text
2. Extract everything after `---PAYLOAD---`
3. Parse JSON
4. Route based on `action`:
   - `create_person` → POST /persons + POST /notes
   - `update_person` → PUT /persons/{person_id} (and/or POST /notes)

---

## Target Roles

The agent looks for people matching these titles (and reasonable variations):

| Role Category | Example Titles |
|---------------|----------------|
| **Superintendent** | Superintendent, Deputy Superintendent, Assistant Superintendent |
| **Curriculum** | Director of Curriculum, Director of Instruction, Curriculum & Instruction Director, Curriculum Coordinator, Asst. Superintendent of Educational Services, Director of Teaching & Learning, Chief Academic Officer |
| **CTE** | CTE Director, CTE Coordinator, Career Technical Education Director, Director of Career & College Readiness, CTE Program Manager |

The agent uses fuzzy matching — if a title *implies* oversight of curriculum or CTE programs, it should be included with a note explaining the reasoning.

---

## Email Confidence Levels

| Level | Criteria | Example |
|-------|----------|---------|
| **Confirmed** | Email explicitly listed on website next to the person's name | lschmidt@avhsd.org (linked on contact page) |
| **High** | Email pattern observed from 2+ confirmed emails on the same domain, applied to this person | cgrado@avhsd.org (pattern: firstinitial+last@avhsd.org, confirmed from 3 other staff) |
| **Medium** | Email pattern observed from only 1 confirmed email, or common pattern assumed | gnehen@avhsd.org (only 1 reference email found) |
| **Low** | No email pattern found; generic contact email only | info@avdistrict.org (district general) |

Confidence level is included in the Slack message AND in the note added to the Pipedrive Person record.

---

## Pipedrive Field Mapping

| Agent Output | Pipedrive Field | Notes |
|-------------|-----------------|-------|
| name | `name` | Full name as found |
| job_title | `job_title` | Exact title from website |
| role_category | Custom field (key TBD) | One of: Superintendent, Curriculum, CTE |
| email | `email` | Array format with primary flag |
| phone | `phone` | Array format with primary flag |
| org_id | `org_id` | From the triggering organization |
| confidence + reasoning | Note (via POST /notes) | Added as a note on the Person record |

---

## Cost Estimates Per District

| Component | Tokens | Cost (Sonnet) |
|-----------|--------|---------------|
| Step 3: URL identification prompt | ~4,000 in / ~500 out | ~$0.02 |
| Step 4: Full extraction prompt | ~15,000 in / ~2,000 out | ~$0.08 |
| **Total per district** | **~20K in / ~2.5K out** | **~$0.10** |

At 500 districts: **~$50**

---

## Error Handling

- **Website unreachable**: Post to Slack: "❌ Could not reach {url} — site may be down. Try again later."
- **No relevant pages found**: Post to Slack: "🔍 No staff/leadership pages found on {url}. Site may use a non-standard structure."
- **Claude API error**: Retry once, then post: "⚠️ Processing error for {org_name}. Flagged for manual review."
- **Pipedrive API error on context fetch**: Post: "⚠️ Could not retrieve existing contacts from Pipedrive for {org_name}."

---

## Tech Stack

- **Runtime**: Python 3.11+ / FastAPI
- **Hosting**: Railway, Render, or Google Cloud Run
- **APIs**: Anthropic (Claude Sonnet), Pipedrive v1, Slack Web API
- **HTML parsing**: BeautifulSoup4 for cleaning
- **Async**: `asyncio` + `httpx` for non-blocking fetches
