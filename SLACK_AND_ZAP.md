# Slack Output Format & Zapier Parsing Spec

## Slack Channel

All messages go to a single channel, e.g., `#district-research`

---

## Parent Message Format

Posted to the channel when research completes for a district.

```
✅ *Research complete for {district_name}*
🔗 {website_url}
🏢 Pipedrive Org: <{pipedrive_org_url}|{org_name}>

📊 Results:
• {confirmed_count} confirmed (still listed)
• {updated_count} need updates
• {new_count} new contacts found
• {missing_count} not found on site

React with ✅ on any threaded action to approve it.
```

---

## Thread Message Formats

All action messages are posted as replies to the parent message.
Each actionable message contains a `---PAYLOAD---` delimiter.
The Zap parses everything BELOW that delimiter as JSON.

### NEW Contact

```
🆕 *CREATE: {name}*
📋 Title: {job_title}
🏷️ Role Category: {role_category}
📧 Email: {email} _({email_confidence} confidence)_
📞 Phone: {phone}
📄 Source: {source_page_url}
📝 {notes}

---PAYLOAD---
{
  "action": "create_person",
  "org_id": {org_id},
  "data": {
    "name": "{name}",
    "org_id": {org_id},
    "email": [{"value": "{email}", "primary": true, "label": "work"}],
    "phone": [{"value": "{phone}", "primary": true, "label": "work"}],
    "job_title": "{job_title}",
    "{role_category_field_key}": "{role_category}"
  },
  "note": "Source: {source_page_url}\nEmail confidence: {email_confidence}\nAgent notes: {notes}\nResearched: {date}"
}
```

### UPDATED Contact

```
✏️ *UPDATE: {name}* (ID: {person_id})
📋 Changes:
  • {field}: {old_value} → {new_value}
📄 Source: {source_page_url}
📝 {notes}

---PAYLOAD---
{
  "action": "update_person",
  "person_id": {person_id},
  "data": {
    "{changed_field}": "{new_value}"
  },
  "note": "Updated by research agent on {date}.\nChange: {field} from '{old_value}' to '{new_value}'.\nSource: {source_page_url}"
}
```

### CONFIRMED Contact (informational — no payload)

```
✅ *CONFIRMED: {name}*
📋 Title: {job_title}
📄 Still listed on: {source_page_url}
_No action needed — contact verified on current site._
```

No `---PAYLOAD---` section. No emoji reaction needed.

### MISSING Contact

```
⚠️ *NOT FOUND: {name}* (ID: {person_id})
📋 Last known title: {job_title}
📝 Not found on current district website. This could mean they've moved on, or the site structure changed. Recommend manual verification before making changes.

---PAYLOAD---
{
  "action": "add_note",
  "person_id": {person_id},
  "note": "Research agent ({date}): This contact was not found on the district website ({website_url}). They may have left the district or the site may have been restructured. Manual verification recommended."
}
```

---

## Zapier Workflow Spec

### Trigger
- **App**: Slack
- **Event**: New Reaction Added
- **Channel**: `#district-research`
- **Reaction**: `white_check_mark` (✅) or your chosen emoji

### Step 1: Get Message Text
- Use Slack → "Find Message" using the message timestamp from the trigger

### Step 2: Extract Payload
- Code by Zapier (JavaScript):

```javascript
const message = inputData.message_text;
const delimiter = '---PAYLOAD---';
const payloadIndex = message.indexOf(delimiter);

if (payloadIndex === -1) {
  return { skip: true, reason: 'No payload found' };
}

const jsonString = message.substring(payloadIndex + delimiter.length).trim();
const payload = JSON.parse(jsonString);

return {
  skip: false,
  action: payload.action,
  person_id: payload.person_id || null,
  org_id: payload.org_id || null,
  data: JSON.stringify(payload.data || {}),
  note: payload.note || null
};
```

### Step 3: Router (Paths by action)

**Path A: `create_person`**
1. Pipedrive → Create Person (use parsed `data` fields)
2. Pipedrive → Create Note (use `note` field, link to new person ID)
3. Slack → Add Reaction (🎉 to confirm execution)

**Path B: `update_person`**
1. Pipedrive → Update Person (ID from `person_id`, fields from `data`)
2. Pipedrive → Create Note (use `note` field, link to person ID)
3. Slack → Add Reaction (🎉)

**Path C: `add_note`**
1. Pipedrive → Create Note (use `note` field, link to `person_id`)
2. Slack → Add Reaction (🎉)

### Edge Cases
- If payload JSON is malformed → Zap error handler sends DM to admin
- If Pipedrive API returns 404 (person deleted) → Log error, add ❌ reaction
- If duplicate detected (person already exists) → Zap should search Pipedrive first; if found, skip create and add 🔄 reaction

---

## Preventing Duplicate Approvals

Add a check at the start of the Zap: if the message already has a 🎉 reaction, skip execution. This prevents double-processing if someone accidentally reacts twice.

---

## Field Key Reference

Before deploying, replace these placeholders with actual Pipedrive field keys:

| Placeholder | Description | How to Find |
|-------------|-------------|-------------|
| `{role_category_field_key}` | Custom field key for "Role Category" on Person | Pipedrive → Settings → Data Fields → Person → find key (format: `abc123...`) |
| `{org_id}` | Populated at runtime from the triggering webhook | Comes from Step 2 of agent flow |
| `{person_id}` | Populated at runtime from existing CRM data | Comes from Pipedrive GET /organizations/{id}/persons |
