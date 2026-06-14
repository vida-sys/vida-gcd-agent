# Skill: Batch Personalized Outreach — Get CPR Done
**Version:** 1.1 (HubSpot cross-check + Manae roster)
**Author:** Vida Monroe / Get CPR Done

---

## Architecture Overview

```
Mailchimp (open-not-booked contacts)
  ↓
HubSpot Cross-Check (per contact, by email)
  ↓
  ├─ IS a customer (closed-won deal or lifecycle = customer)
  │     → Manae Roster (held, emailed to Manae on Fridays)
  │
  └─ NOT a customer
        → Claude generates personalized email
        → Vida reviews sample of 5
        → Gmail sends batch (25–50/day, randomized delays)
        → Replies forwarded to Vida as [CPR Lead Reply]
```

---

## Step 1 — Mailchimp Fetch

**Target:** Contacts who meet ALL of:
- `status` = subscribed
- Opened ≥1 campaign (avg_open_rate > 0 or open event in last 180 days)
- Does NOT have tags: `customer`, `booked`, `unsubscribe_requested`, `do_not_contact`
- Not contacted in this session (dedup by email)

**Pull fields:** `email_address`, `firstName`, `lastName`, `merge_fields.COMPANY`, `tags`, `stats.avg_open_rate`, `last_changed`

---

## Step 2 — HubSpot Cross-Check

For each Mailchimp contact, search HubSpot by email.

**Mark as CUSTOMER (→ Manae) if ANY is true:**
- Contact has ≥1 closed-won deal
- `lifecyclestage` = `customer`
- `hs_lead_status` = `IN_PROGRESS` with a closed deal on record

**Mark as PROSPECT (→ Vida) if:**
- Not found in HubSpot at all
- Found in HubSpot but no closed-won deals and lifecycle ≠ customer

**Lookup returns:**
```json
{
  "found": true/false,
  "isCustomer": true/false,
  "name": "string",
  "company": "string",
  "lastDealName": "string",
  "lastDealValue": 0,
  "lastDealDate": "YYYY-MM-DD",
  "lifecycleStage": "string"
}
```

**Performance note:** HubSpot checks run sequentially with no added delay (they're reads, not sends). For 50 contacts expect ~2–3 min for this step.

---

## Step 3A — Vida's Outreach (Prospects)

### Email Generation Prompt (System)
```
You are Vida Monroe, Business Development Associate at Get CPR Done.

Write short, warm, direct reactivation emails. Brand voice: prevention-first,
confidence-building, community-focused. Never fear-based or liability-focused.

Rules:
- Subject: conversational, ≤8 words
- Body: exactly 3 sentences
- Use first name and org name naturally
- No exclamation points, no filler openers
- End with a soft CTA: book, get details, or ask questions
- Sign off: Vida Monroe | Get CPR Done

Output ONLY valid JSON: {"subject":"...","body":"...","personalization_notes":"..."}
```

### Industry Context Map

| Company keyword | Inferred context |
|----------------|-----------------|
| School / Academy / Education | Staff CPR recertification before school year |
| Construction / Industrial | OSHA workplace safety compliance |
| Church / Ministry | Congregation and volunteer safety |
| Hospitality / Restaurant / Hotel | Front-of-house staff safety |
| Healthcare / Medical | Clinical staff renewal |
| Nonprofit / Foundation | Community program safety |
| Aviation / Aerospace | High-stakes emergency preparedness |
| Financial / Bank / Insurance | Employee safety and compliance |
| Law / Legal | Employee safety readiness |
| Default | General workforce CPR readiness |

### Sending Rules
| Setting | Value |
|---------|-------|
| Batch size | 25–50 (user selects) |
| Review sample | 5 random from batch |
| Min delay between sends | 60 seconds |
| Max delay between sends | 180 seconds |
| Daily hard cap | 50 emails |
| Sending identity | Vida Monroe (Gmail) |

### Reply Handling
1. Poll Gmail for replies to sent thread IDs
2. On reply: forward to vida@getcprdone.com
   - Subject: `[CPR Lead Reply] {First Last} — {Org}`
   - Body: their reply + context (when sent, what was sent)
3. Log replied contacts to prevent duplicate forwards

---

## Step 3B — Manae's Roster (Existing Customers)

Customers filtered from the batch are held and compiled into a weekly roster email.

**Roster email:**
- **To:** Manae@GetCPRDone.com
- **Subject:** `[Weekly Roster] N existing customers to follow up — w/e {date}`
- **Sent:** End of each batch run (Friday cadence recommended)
- **Contains:** Per customer: name, company, email, last deal name, value, date

**Rationale:** Existing customers have an established relationship with GCD and respond better to a personal touch from their account owner (Manae) than a cold reactivation sequence.

---

## Fallback Template (Claude generation failure)

```
Subject: Quick question about CPR training

Hi {first_name},

Wanted to check in — is {company} due for CPR/AED training this year?
We work with organizations across the country and can usually schedule
within a few weeks.

Happy to answer any questions or send over details.

Vida Monroe | Get CPR Done
```

---

## Error Handling

| Error | Behavior |
|-------|----------|
| Mailchimp rate limit | Back off 2s between fetches |
| Missing COMPANY field | Use "your team" |
| HubSpot lookup failure | Default to PROSPECT (safe fallback) |
| Gmail send failure | Log + skip, do not retry same session |
| Claude generation failure | Use fallback template |

---

## Routing Decision Tree

```
Mailchimp contact
      │
      ▼
HubSpot lookup by email
      │
  ┌───┴────┐
Found?    Not found
  │           │
  ▼           ▼
Closed-won  → PROSPECT → Vida batch
deal? Yes
  │
  ▼
CUSTOMER → Manae roster
```

---

## Weekly Cadence (Recommended)

| Day | Action |
|-----|--------|
| Monday | Run Batch 1 (25–50 prospects) |
| Wednesday | Run Batch 2 (25–50 prospects) |
| Friday | Trigger Manae roster send for the week |

---

## Future Enhancements

- Auto-create HubSpot contact + deal on reply
- A/B subject line testing across batches
- Tag replied contacts in Mailchimp as `outreach_reply`
- Schedule sends for 9am–5pm local time only
- 7-day follow-up sequence for non-responders
- Slack notification to Vida on each reply (instead of/in addition to email forward)

---

## Production Script (v1.2)

**File:** `outreach_agent.py`  
**Runtime:** Python 3.10+, `anthropic` SDK only  
**Modes:** `daily` | `roster` | `reply_check`  

### Schedule
| Cron | Mode | Effect |
|------|------|--------|
| `0 9 * * 1-5` | `daily` | 100 prospects/day, M–F |
| `5 9 * * 1` | `roster` | Manae's customer roster, Mondays |
| `0 9,11,13,15,17 * * 1-5` | `reply_check` | Reply forwarding, every 2 hrs |

### Failure handling
- Any exception → `notify_chris()` → email to chris@getcprdone.com
- HubSpot lookup failure → defaults to PROSPECT (safe)
- Gmail send failure → logs, skips contact, notifies Chris
- Generation failure → uses fallback template, notifies Chris

### Reply routing
- Forward to: vida@getcprdone.com  
- CC: Manae@GetCPRDone.com  
- Subject: `[CPR Lead Reply] {Name} — {Company}`

### State file
`state.json` persists between runs. Tracks: contacted_emails, sent_thread_ids, manae_roster_pending, last_roster_send, daily_sent_count, forwarded_thread_ids.
