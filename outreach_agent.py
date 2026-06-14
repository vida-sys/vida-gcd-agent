"""
Get CPR Done — Batch Outreach Agent
====================================
Runs M–F only. Pulls Mailchimp open-not-booked contacts across ALL lists,
cross-checks HubSpot, routes customers to Manae's weekly roster (sent every
Monday), and sends personalized reactivation emails from Vida to prospects.

On any failure      → emails Chris.
On any reply        → forwards to Vida + CCs Manae.
On hard bounce      → logs to do_not_contact, queues org for replacement search.
End of day          → summary report to Chris.

Schedule:
  Daily (M–F) 9:00 AM  → python outreach_agent.py --mode daily
  Monday      9:05 AM  → python outreach_agent.py --mode roster
  Every 2 hrs 9AM–5PM  → python outreach_agent.py --mode reply_check

Usage:
  python outreach_agent.py --mode daily
  python outreach_agent.py --mode roster
  python outreach_agent.py --mode reply_check
  python outreach_agent.py --mode daily --dry-run
"""

import argparse
import base64
import fcntl
import json
import logging
import os
import random
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────────────

VIDA_EMAIL    = "vida@getcprdone.com"
MANAE_EMAIL   = "Manae@GetCPRDone.com"
CHRIS_EMAIL   = "chris@getcprdone.com"
SENDING_NAME  = "Vida Monroe"
COMPANY_NAME  = "Get CPR Done"

BATCH_SIZE    = 750
MIN_DELAY_SEC = 30
MAX_DELAY_SEC = 60

STATE_FILE = Path(__file__).parent / "state.json"
LOG_FILE   = Path(__file__).parent / "outreach.log"

# ─── Credentials (env vars override these fallbacks) ─────────────────────────

ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
MAILCHIMP_API_KEY  = os.environ.get("MAILCHIMP_API_KEY", "")
MAILCHIMP_SERVER   = "us15"
HUBSPOT_TOKEN      = os.environ.get("HUBSPOT_TOKEN", "")

INDUSTRY_MAP = [
    (["school","academy","learning","education","elementary","preschool","montessori","kipp","charter"],
     "school or educational organization — staff CPR/AED recertification before the school year"),
    (["construction","contracting","industrial","engineering","trades","builder"],
     "construction or industrial company — OSHA workplace safety compliance"),
    (["church","ministry","congregation","parish","fellowship"],
     "faith-based or community organization — congregation and volunteer safety"),
    (["hospitality","restaurant","hotel","dining","food","catering","bistro"],
     "hospitality or food service business — front-of-house staff safety"),
    (["healthcare","medical","clinic","dental","therapy","wellness","pharmacy"],
     "healthcare organization — clinical staff renewal"),
    (["nonprofit","foundation","shelter","community center"],
     "nonprofit or social services organization — community program safety"),
    (["aviation","aerospace","airline","airport"],
     "aviation organization — high-stakes emergency preparedness"),
    (["financial","finance","bank","insurance","investment","wealth","advisory"],
     "financial services firm — employee safety and compliance"),
    (["law","legal","attorney"],
     "legal firm — employee safety readiness"),
]

# Role/generic address prefixes to skip — not a real person
ROLE_ADDRESS_PREFIXES = {
    "center", "info", "admin", "contact", "office", "main", "general",
    "licensing", "reporting", "mainoffice", "director", "noreply", "no-reply",
    "support", "help", "sales", "billing", "hr", "careers", "jobs",
    "ccc", "hello", "team", "staff",
}

# Placeholder/test addresses to always skip
PLACEHOLDER_ADDRESSES = {
    "email@yourbusiness.com", "test@test.com", "example@example.com",
    "user@example.com", "name@domain.com",
}

# API batch size for email generation (50 contacts per Claude call)
GENERATION_BATCH_SIZE = 50

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("outreach")

# ─── State ────────────────────────────────────────────────────────────────────

LOCK_FILE = Path(__file__).parent / "outreach_agent.lock"

def _acquire_lock():
    """
    Acquire an exclusive file lock so only one instance of the agent
    can run at a time. Returns the lock file handle (keep it open).
    Exits immediately if another process holds the lock.
    """
    lf = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error(
            "Another instance of outreach_agent.py is already running "
            f"(lock held: {LOCK_FILE}). Exiting to prevent duplicate sends."
        )
        sys.exit(1)
    lf.write(str(os.getpid()))
    lf.flush()
    return lf

def _normalize_state(raw: dict) -> dict:
    """
    Migrate legacy key names and normalize all email lists to lowercase.
    Handles the ghost keys 'contacted' and 'daily_counts' that were
    written by older code versions.
    """
    # Canonical schema
    state = {
        "contacted_emails": [],
        "sent_thread_ids": [],
        "forwarded_thread_ids": [],
        "manae_roster_pending": [],
        "last_roster_send": None,
        "daily_sent_count": {},
        "do_not_contact": [],
        "bounce_replacement_queue": [],
    }
    state.update(raw)

    # Merge legacy 'contacted' key into 'contacted_emails'
    legacy_contacted = raw.get("contacted", [])
    if legacy_contacted:
        merged = set(e.lower() for e in state["contacted_emails"])
        merged.update(e.lower() for e in legacy_contacted)
        state["contacted_emails"] = sorted(merged)
        log.info(f"  Migrated {len(legacy_contacted)} entries from legacy 'contacted' key")

    # Merge legacy 'daily_counts' key into 'daily_sent_count'
    legacy_daily = raw.get("daily_counts", {})
    if legacy_daily:
        for k, v in legacy_daily.items():
            if k not in state["daily_sent_count"]:
                state["daily_sent_count"][k] = v

    # Remove ghost keys
    state.pop("contacted", None)
    state.pop("daily_counts", None)

    # Normalize all email lists to lowercase
    state["contacted_emails"] = sorted({e.lower() for e in state["contacted_emails"] if e})
    state["do_not_contact"]   = sorted({e.lower() for e in state["do_not_contact"] if e})

    return state

def load_state():
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text())
            return _normalize_state(raw)
        except Exception as e:
            log.warning(f"Could not parse state.json: {e} — starting fresh")
    return {
        "contacted_emails": [],
        "sent_thread_ids": [],
        "forwarded_thread_ids": [],
        "manae_roster_pending": [],
        "last_roster_send": None,
        "daily_sent_count": {},
        "do_not_contact": [],
        "bounce_replacement_queue": [],
    }

def save_state(state):
    # Always normalize before saving so the file stays clean
    state["contacted_emails"] = sorted({e.lower() for e in state.get("contacted_emails", []) if e})
    state["do_not_contact"]   = sorted({e.lower() for e in state.get("do_not_contact", []) if e})
    # Write atomically: write to tmp file then rename
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

# ─── Name / email hygiene ─────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Title-case all-caps names; leave mixed-case alone."""
    if not name:
        return name
    if name == name.upper() and len(name) > 1:
        return name.title()
    return name

def is_role_address(email: str) -> bool:
    """Return True if the local part looks like a generic/role address."""
    local = email.split("@")[0].lower()
    # strip digits from end (e.g. center578 → center)
    base = re.sub(r"\d+$", "", local)
    return base in ROLE_ADDRESS_PREFIXES

# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_weekday():
    return date.today().weekday() < 5

def is_monday():
    return date.today().weekday() == 0

def today_str():
    return date.today().isoformat()

def infer_industry(company="", tags=None):
    text = (company + " " + " ".join(tags or [])).lower()
    for keywords, context in INDUSTRY_MAP:
        if any(k in text for k in keywords):
            return context
    return "organization — general workforce CPR and First Aid readiness"

def ssl_ctx():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()

def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30, context=ssl_ctx()) as r:
        return json.loads(r.read().decode())

def http_post(url, headers, payload):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=120, context=ssl_ctx()) as r:
        return json.loads(r.read().decode())

def http_post_raw(url, headers, payload):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120, context=ssl_ctx()) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())

# ─── Mailchimp ────────────────────────────────────────────────────────────────

def fetch_all_mailchimp_lists():
    """Return list of {id, name} for every Mailchimp audience."""
    auth = base64.b64encode(f"anystring:{MAILCHIMP_API_KEY}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    all_lists = []
    offset = 0
    while True:
        url = (
            f"https://{MAILCHIMP_SERVER}.api.mailchimp.com/3.0/lists"
            f"?count=100&offset={offset}&fields=lists.id,lists.name,total_items"
        )
        data = http_get(url, headers)
        batch = data.get("lists", [])
        all_lists.extend(batch)
        if len(all_lists) >= data.get("total_items", 0) or not batch:
            break
        offset += len(batch)
    return all_lists

def fetch_mailchimp_contacts(batch_size, do_not_contact=None):
    """Fetch eligible contacts across ALL Mailchimp lists."""
    log.info(f"Fetching up to {batch_size} contacts from all Mailchimp lists...")
    auth = base64.b64encode(f"anystring:{MAILCHIMP_API_KEY}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    do_not_contact = set(e.lower() for e in (do_not_contact or []))

    lists = fetch_all_mailchimp_lists()
    log.info(f"  Found {len(lists)} Mailchimp list(s): {', '.join(l['name'] for l in lists)}")

    blocking_tags = {"customer", "booked", "do_not_contact", "unsubscribe_requested"}
    contacts = []
    seen_emails = set()

    for mc_list in lists:
        list_id   = mc_list["id"]
        list_name = mc_list["name"]
        if len(contacts) >= batch_size:
            break

        needed = batch_size - len(contacts) + 50
        members_url = (
            f"https://{MAILCHIMP_SERVER}.api.mailchimp.com/3.0/lists/{list_id}/members"
            f"?status=subscribed&count={needed}"
            f"&sort_field=last_changed&sort_dir=DESC"
            f"&fields=members.email_address,members.merge_fields,members.tags,members.stats,members.last_changed"
        )
        try:
            data = http_get(members_url, headers)
        except Exception as e:
            log.warning(f"  Could not fetch list {list_name}: {e}")
            continue

        members = data.get("members", [])
        list_count = 0

        for m in members:
            email = m["email_address"].lower()
            if email in seen_emails or email in do_not_contact:
                continue
            if email in PLACEHOLDER_ADDRESSES:
                log.info(f"  Skipping placeholder address: {email}")
                continue
            if is_role_address(email):
                log.info(f"  Skipping role address: {email}")
                do_not_contact.add(email)
                continue
            member_tags = {t["name"].lower() for t in m.get("tags", [])}
            if member_tags & blocking_tags:
                continue
            stats = m.get("stats", {})
            avg_open_rate = stats.get("avg_open_rate", 0)
            open_count    = stats.get("member_rating", 0)  # 1–5 star rating as proxy for engagement
            if avg_open_rate == 0:
                continue
            mf = m.get("merge_fields", {})
            first = normalize_name(mf.get("FNAME", ""))
            last  = normalize_name(mf.get("LNAME", ""))
            last_open_str = m.get("last_changed", "")[:10]
            # Warmth score: recency (days since last open) weighted with open rate
            try:
                days_since = (date.today() - date.fromisoformat(last_open_str)).days if last_open_str else 999
            except ValueError:
                days_since = 999
            # Lower score = warmer: recent high-openers first
            warmth_score = days_since / max(avg_open_rate, 0.01)
            contacts.append({
                "email": m["email_address"],
                "firstName": first,
                "lastName": last,
                "company": mf.get("COMPANY", "") or mf.get("ORG", ""),
                "tags": list(member_tags),
                "lastOpen": last_open_str,
                "avgOpenRate": round(avg_open_rate, 3),
                "openCount": open_count,
                "warmthScore": warmth_score,
                "sourceList": list_name,
            })
            seen_emails.add(email)
            list_count += 1
            if len(contacts) >= batch_size:
                break

        log.info(f"  → {list_count} contacts from '{list_name}'")

    log.info(f"  → {len(contacts)} total eligible contacts across all lists")
    # Sort warmest first: lowest warmth score = most recent + highest open rate
    contacts.sort(key=lambda c: c.get("warmthScore", 999))
    if contacts:
        top = contacts[0]
        log.info(
            f"  → Warmth sort done. Top contact: {top['email']} "
            f"(last open: {top['lastOpen']}, avg open rate: {top['avgOpenRate']:.0%})"
        )
    return contacts

# ─── HubSpot ──────────────────────────────────────────────────────────────────

def check_hubspot(contact):
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{"filters": [{
            "propertyName": "email",
            "operator": "EQ",
            "value": contact["email"],
        }]}],
        "properties": ["email", "firstname", "lastname", "company", "lifecyclestage", "hs_lead_status"],
        "limit": 1,
    }
    try:
        status, data = http_post_raw(search_url, headers, payload)
        if status != 200 or not data.get("results"):
            return {"found": False, "isCustomer": False}

        hs_contact = data["results"][0]
        props = hs_contact.get("properties", {})
        contact_id = hs_contact["id"]
        lifecycle = props.get("lifecyclestage", "")
        is_customer = lifecycle in ("customer", "evangelist")

        deals_url = (
            f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}/associations/deals"
        )
        try:
            deal_assoc = http_get(deals_url, {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
            deal_ids = [r["id"] for r in deal_assoc.get("results", [])]
        except Exception:
            deal_ids = []

        last_deal_name = None
        last_deal_value = 0
        last_deal_date = None

        for deal_id in deal_ids[:5]:
            deal_url = (
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
                f"?properties=dealname,dealstage,amount,closedate"
            )
            try:
                deal_data = http_get(deal_url, {"Authorization": f"Bearer {HUBSPOT_TOKEN}"})
                dp = deal_data.get("properties", {})
                if dp.get("dealstage") == "closedwon":
                    is_customer = True
                    last_deal_name  = dp.get("dealname")
                    last_deal_value = float(dp.get("amount") or 0)
                    last_deal_date  = (dp.get("closedate") or "")[:10]
                    break
            except Exception:
                continue

        return {
            "found": True,
            "isCustomer": is_customer,
            "name": f"{props.get('firstname','')} {props.get('lastname','')}".strip(),
            "company": props.get("company", ""),
            "lastDealName": last_deal_name,
            "lastDealValue": last_deal_value,
            "lastDealDate": last_deal_date,
            "lifecycleStage": lifecycle,
        }
    except Exception as e:
        log.warning(f"  HubSpot lookup failed for {contact['email']}: {e} — defaulting to prospect")
        return {"found": False, "isCustomer": False}

# ─── Bounce recovery — web search for replacement contact ─────────────────────

def search_replacement_contact(bounced_email, state):
    """
    Given a bounced email, search the web for a replacement contact at the same org.
    Also checks HubSpot for any other contacts at that domain.
    Queues result into state['bounce_replacement_queue'] for the next batch.
    """
    domain = bounced_email.split("@")[-1].lower()
    # Skip personal email providers
    personal_domains = {
        "gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com",
        "aol.com","msn.com","ymail.com","me.com","mac.com","sbcglobal.net",
    }
    if domain in personal_domains:
        log.info(f"  Bounce recovery: personal email domain {domain} — skipping replacement search")
        return

    log.info(f"  Bounce recovery: searching for replacement at {domain}...")

    # 1. Check HubSpot for other contacts at same domain
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = {
        "filterGroups": [{"filters": [{
            "propertyName": "email",
            "operator": "CONTAINS_TOKEN",
            "value": f"*@{domain}",
        }]}],
        "properties": ["email", "firstname", "lastname", "jobtitle"],
        "limit": 5,
    }
    try:
        status, data = http_post_raw(search_url, headers, payload)
        if status == 200 and data.get("results"):
            for result in data["results"]:
                props = result.get("properties", {})
                alt_email = props.get("email", "")
                if alt_email and alt_email.lower() != bounced_email.lower():
                    queue_entry = {
                        "originalBounced": bounced_email,
                        "replacementEmail": alt_email,
                        "replacementName": f"{props.get('firstname','')} {props.get('lastname','')}".strip(),
                        "source": "hubspot",
                        "domain": domain,
                        "queuedAt": today_str(),
                    }
                    queue = state.get("bounce_replacement_queue", [])
                    # Don't add duplicates
                    if not any(q.get("replacementEmail") == alt_email for q in queue):
                        queue.append(queue_entry)
                        state["bounce_replacement_queue"] = queue
                        log.info(f"  → Replacement found in HubSpot: {alt_email}")
                    return
    except Exception as e:
        log.warning(f"  HubSpot domain search failed: {e}")

    # 2. Web search for org + CPR/safety contact
    try:
        search_query = f"site:{domain} OR \"{domain}\" CPR training safety director contact"
        search_url_api = (
            f"https://api.anthropic.com/v1/messages"
        )
        api_headers = {
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": "claude-sonnet-4-5",
            "max_tokens": 300,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "system": (
                "You are a research agent. Search for a valid contact email at the given domain "
                "for someone who might handle CPR/First Aid training for their organization. "
                "Look for HR director, office manager, facilities manager, or training coordinator. "
                'Return ONLY JSON: {"found": true/false, "email": "...", "name": "...", "title": "..."} '
                "If no specific person found, return {\"found\": false}. "
                "Never guess or fabricate email addresses."
            ),
            "messages": [{"role": "user", "content": (
                f"Find a valid contact at domain: {domain}\n"
                f"This is for a CPR training company reaching out. "
                f"Search for their website and find an appropriate contact person."
            )}],
        }
        data = http_post(search_url_api, api_headers, payload)
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text = block["text"]
                break
        if text:
            cleaned = text.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("```")[1]
                if cleaned.startswith("json"):
                    cleaned = cleaned[4:]
            result = json.loads(cleaned.strip())
            if result.get("found") and result.get("email"):
                queue_entry = {
                    "originalBounced": bounced_email,
                    "replacementEmail": result["email"],
                    "replacementName": result.get("name", ""),
                    "replacementTitle": result.get("title", ""),
                    "source": "web_search",
                    "domain": domain,
                    "queuedAt": today_str(),
                }
                queue = state.get("bounce_replacement_queue", [])
                if not any(q.get("replacementEmail") == result["email"] for q in queue):
                    queue.append(queue_entry)
                    state["bounce_replacement_queue"] = queue
                    log.info(f"  → Replacement found via web search: {result['email']} ({result.get('name','')})")
                return
    except Exception as e:
        log.warning(f"  Web search replacement failed for {domain}: {e}")

    log.info(f"  → No replacement found for {domain} — flagged for manual review")

# ─── Claude email generation (batch) ─────────────────────────────────────────

FALLBACK_SUBJECT = "Quick question about CPR training"

def fallback_body(contact):
    first = contact.get("firstName") or "there"
    company = contact.get("company") or "your team"
    return (
        f"Hi {first},\n\n"
        f"Wanted to check in — is {company} due for CPR/AED training this year? "
        f"We work with organizations across the country and can usually schedule within a few weeks.\n\n"
        f"Happy to answer any questions or send over details.\n\n"
        f"{SENDING_NAME} | {COMPANY_NAME}"
    )

def _fallback_result(contact):
    return {
        "subject": FALLBACK_SUBJECT,
        "body": fallback_body(contact),
        "personalization_notes": "fallback used",
    }

def generate_emails_batch(contacts):
    """
    Generate personalized emails for a list of contacts in a single Claude API call.
    Returns a list of dicts: [{subject, body, personalization_notes}, ...]
    in the same order as the input contacts.
    """
    if not contacts:
        return []

    system = (
        f"You are {SENDING_NAME}, Business Development Associate at {COMPANY_NAME} — "
        "a national CPR and First Aid training company affiliated with the American Heart Association.\n\n"
        "Write short, warm, direct reactivation emails. Brand voice: prevention-first, "
        "confidence-building, community-focused. Never fear-based or liability-focused.\n\n"
        "Rules for each email:\n"
        "- Subject: conversational, ≤8 words, no clickbait\n"
        "- Body: exactly 3 sentences\n"
        "- Use first name naturally (already properly capitalized — do not alter the case)\n"
        "- Reference org name and context naturally\n"
        "- No exclamation points\n"
        "- No filler openers (do not start with 'I hope this finds you well' etc.)\n"
        f"- Sign off: {SENDING_NAME} | {COMPANY_NAME}\n\n"
        "You will receive a JSON array of contacts. "
        "Return ONLY a JSON array (no markdown, no preamble) with one object per contact "
        "in the SAME ORDER, each with keys: subject, body, personalization_notes.\n"
        "Example output format:\n"
        '[{"subject":"...","body":"...","personalization_notes":"..."},'
        '{"subject":"...","body":"...","personalization_notes":"..."}]'
    )

    contact_list = []
    for i, c in enumerate(contacts):
        industry = infer_industry(c.get("company", ""), c.get("tags", []))
        contact_list.append({
            "index": i,
            "firstName": c.get("firstName") or "there",
            "company": c.get("company") or "your organization",
            "industry": industry,
            "tags": ", ".join(c.get("tags", [])) or "none",
            "sourceList": c.get("sourceList", "unknown"),
            "lastOpen": c.get("lastOpen") or "recently",
            "avgOpenRate": f"{c.get('avgOpenRate', 0):.0%}",
        })

    user_msg = (
        f"Generate reactivation emails for these {len(contacts)} contacts:\n\n"
        + json.dumps(contact_list, indent=2)
    )

    # Scale max_tokens with batch size — ~350 tokens per email is comfortable
    max_tok = min(4096, 350 * len(contacts) + 200)

    payload = {
        "model": "claude-sonnet-4-5",
        "max_tokens": max_tok,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }

    try:
        data = http_post("https://api.anthropic.com/v1/messages", headers, payload)
        text = next((b["text"] for b in data.get("content", []) if b.get("type") == "text"), "")
        cleaned = text.strip()
        # Strip markdown fences if present
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        results = json.loads(cleaned)
        if not isinstance(results, list):
            raise ValueError("Expected JSON array")
        # Pad with fallbacks if model returned fewer than expected
        while len(results) < len(contacts):
            results.append(_fallback_result(contacts[len(results)]))
        return results[:len(contacts)]
    except Exception as e:
        log.warning(f"  Batch generation error: {e} — using fallbacks for all {len(contacts)} contacts")
        return [_fallback_result(c) for c in contacts]


def generate_email(contact):
    """Single-contact wrapper around the batch function (kept for compatibility)."""
    results = generate_emails_batch([contact])
    r = results[0]
    return r.get("subject", FALLBACK_SUBJECT), r.get("body", fallback_body(contact)), r.get("personalization_notes", "")

# ─── Gmail SMTP ───────────────────────────────────────────────────────────────

def send_gmail_smtp(to, subject, body, cc=None):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        log.error("  GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set")
        return {"success": False, "error": "Gmail credentials not configured"}

    msg = MIMEMultipart()
    msg["From"]    = f"{SENDING_NAME} <{gmail_user}>"
    msg["To"]      = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc

    full_body = body if SENDING_NAME in body else body + f"\n\n{SENDING_NAME} | {COMPANY_NAME}"
    msg.attach(MIMEText(full_body, "plain"))

    recipients = [to] + ([cc] if cc else [])
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl_ctx()) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, recipients, msg.as_string())
        return {"success": True, "threadId": ""}
    except smtplib.SMTPRecipientsRefused as e:
        # Hard bounce / 550 — extract error details
        err_detail = str(e)
        return {"success": False, "error": err_detail, "hard_bounce": True}
    except Exception as e:
        err_str = str(e)
        is_hard = "550" in err_str or "5.7.1" in err_str or "does not exist" in err_str
        return {"success": False, "error": err_str, "hard_bounce": is_hard}

send_email = send_gmail_smtp

# ─── Error notification ───────────────────────────────────────────────────────

def notify_chris(error_msg, context=""):
    try:
        subject = f"[Outreach Agent ERROR] {today_str()}"
        body = (
            f"Hi Chris,\n\nThe Get CPR Done Batch Outreach Agent encountered an error.\n\n"
            f"Error: {error_msg}\n\nContext: {context or 'See log file.'}\n\n"
            f"Log: {LOG_FILE}\n\n—Outreach Agent (automated)"
        )
        result = send_email(CHRIS_EMAIL, subject, body)
        if result.get("success"):
            log.info("  → Error notification sent to Chris")
        else:
            log.error(f"  → Failed to notify Chris: {result.get('error')}")
    except Exception as e:
        log.error(f"  → Could not notify Chris: {e}")

# ─── End-of-day report ────────────────────────────────────────────────────────

def send_eod_report(sent_count, bounce_count, bounce_list, replacement_queue, skipped_role, dry_run=False, warmth_breakdown=None):
    """Email chris@getcprdone.com a summary of today's outreach run."""
    subject = f"[Outreach Report] {today_str()} — {sent_count} sent"

    warmth_lines = ""
    if warmth_breakdown:
        warmth_lines = (
            f"\n  Audience warmth:\n"
            f"    Hot  (opened <30 days ago):  {warmth_breakdown.get('hot', 0)}\n"
            f"    Warm (30–180 days):           {warmth_breakdown.get('warm', 0)}\n"
            f"    Cold (180+ days):             {warmth_breakdown.get('cold', 0)}\n"
        )

    bounce_lines = ""
    if bounce_list:
        bounce_lines = "\n\nBounced emails (added to do_not_contact):\n"
        bounce_lines += "\n".join(f"  • {b}" for b in bounce_list)

    replacement_lines = ""
    if replacement_queue:
        replacement_lines = "\n\nReplacement contacts queued for next batch:\n"
        for r in replacement_queue:
            src = r.get("source", "unknown")
            name = r.get("replacementName", "")
            title = r.get("replacementTitle", "")
            replacement_lines += (
                f"  • {r.get('replacementEmail','')} ({name}{' — ' + title if title else ''}) "
                f"[replaces {r.get('originalBounced','')} via {src}]\n"
            )

    skipped_lines = ""
    if skipped_role:
        skipped_lines = f"\n\nSkipped {len(skipped_role)} role/generic addresses (not real people):\n"
        skipped_lines += "\n".join(f"  • {e}" for e in skipped_role[:10])
        if len(skipped_role) > 10:
            skipped_lines += f"\n  ... and {len(skipped_role) - 10} more"

    body = (
        f"Hi Chris,\n\n"
        f"Here's today's outreach summary for {today_str()}:\n\n"
        f"  Emails sent:    {sent_count}\n"
        f"  Hard bounces:   {bounce_count}\n"
        f"  Replacements queued: {len(replacement_queue)}\n"
        f"{warmth_lines}"
        f"{bounce_lines}"
        f"{replacement_lines}"
        f"{skipped_lines}"
        f"\n\n—Outreach Agent (automated)"
    )

    if not dry_run:
        result = send_email(CHRIS_EMAIL, subject, body)
        if result.get("success"):
            log.info("  → End-of-day report sent to Chris")
        else:
            log.error(f"  → EOD report failed: {result.get('error')}")
    else:
        log.info(f"  [DRY RUN] Would send EOD report to Chris: {sent_count} sent, {bounce_count} bounced")

# ─── Reply check ──────────────────────────────────────────────────────────────

def classify_reply(sender, subject, body_text):
    """Classify an inbound email. Returns: 'bounce', 'ooo', 'unsubscribe', or 'genuine'."""
    sender_l  = sender.lower()
    subject_l = subject.lower()
    body_l    = body_text.lower()

    # Hard bounce / delivery failure
    if any(k in sender_l for k in ["mailer-daemon", "postmaster", "mail delivery"]):
        return "bounce"
    if any(k in subject_l for k in [
        "undeliverable", "delivery failed", "delivery status notification",
        "mail delivery failed", "returned mail", "delivery failure",
        "failed to deliver", "unable to deliver",
    ]):
        return "bounce"

    # Out of office / auto-reply
    if any(k in subject_l for k in [
        "out of office", "auto-reply", "automatic reply", "away from",
        "on vacation", "ooo:", "i am out", "i\'m out", "i will be out",
        "currently out", "annual leave", "maternity leave", "on leave",
    ]):
        return "ooo"
    # Some OOOs don't put it in subject — check body first line
    first_lines = body_l[:300]
    if any(k in first_lines for k in [
        "i am currently out", "i\'m currently out", "i am away", "i\'m away",
        "i am on vacation", "i\'m on vacation", "i will be out of the office",
        "i\'m out of the office", "this is an automatic reply",
        "this is an automated response",
    ]):
        return "ooo"

    # Unsubscribe / opt-out
    if any(k in subject_l for k in [
        "unsubscribe", "remove me", "opt out", "opt-out",
        "stop emailing", "please remove", "take me off",
    ]):
        return "unsubscribe"

    return "genuine"


def archive_message(mail, mid):
    """Mark as read and archive (remove from INBOX) without deleting."""
    mail.store(mid, "+FLAGS", "\\Seen")
    # Gmail archiving = remove the \\Inbox label
    mail.store(mid, "-X-GM-LABELS", "\\Inbox")


def check_replies(state, dry_run=False):
    import imaplib
    import email as emaillib

    gmail_user = os.environ.get("GMAIL_ADDRESS", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")

    if not gmail_user or not gmail_pass:
        log.info("Gmail credentials not set — skipping reply check")
        return

    log.info("Checking Gmail inbox for replies / bounces / OOOs...")
    already_forwarded = set(state.get("forwarded_thread_ids", []))
    do_not_contact    = set(state.get("do_not_contact", []))

    try:
        ctx  = ssl_ctx()
        mail = imaplib.IMAP4_SSL("imap.gmail.com", ssl_context=ctx)
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX")

        # Search ALL unread — not just those matching thread IDs,
        # because SMTP sends don't give us real Gmail thread IDs.
        _, msg_ids = mail.search(None, "UNSEEN")
        all_mids = msg_ids[0].split() if msg_ids[0] else []
        log.info(f"  {len(all_mids)} unread messages in inbox")

        genuine_count    = 0
        archived_count   = 0
        unsubscribe_count = 0

        for mid in all_mids:
            try:
                _, msg_data = mail.fetch(mid, "(RFC822)")
                msg = emaillib.message_from_bytes(msg_data[0][1])

                sender  = msg.get("From", "Unknown")
                subject = msg.get("Subject", "")
                body_text = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body_text = part.get_payload(decode=True).decode(errors="replace")
                            break
                else:
                    body_text = msg.get_payload(decode=True).decode(errors="replace")

                kind = classify_reply(sender, subject, body_text)

                if kind == "bounce":
                    log.info(f"  Bounce from {sender} — archiving silently")
                    # Extract the original recipient from the bounce body if possible
                    # and add to do_not_contact
                    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", body_text)
                    if email_match:
                        bounced = email_match.group(0).lower()
                        if bounced not in do_not_contact:
                            do_not_contact.add(bounced)
                            state["do_not_contact"] = list(do_not_contact)
                            log.info(f"    Added {bounced} to do_not_contact")
                    if not dry_run:
                        archive_message(mail, mid)
                    archived_count += 1

                elif kind == "ooo":
                    log.info(f"  OOO from {sender} — archiving silently")
                    if not dry_run:
                        archive_message(mail, mid)
                    archived_count += 1

                elif kind == "unsubscribe":
                    log.info(f"  Unsubscribe request from {sender} — archiving + blocking")
                    # Extract their email and block
                    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", sender)
                    if email_match:
                        unsub_email = email_match.group(0).lower()
                        do_not_contact.add(unsub_email)
                        state["do_not_contact"] = list(do_not_contact)
                    if not dry_run:
                        archive_message(mail, mid)
                    unsubscribe_count += 1
                    archived_count += 1

                elif kind == "genuine":
                    log.info(f"  Genuine reply from {sender} — forwarding to Vida")
                    fwd_subject = f"[CPR Lead Reply] {sender}"
                    fwd_body = (
                        f"Hi Vida,\n\nNew reply to your outreach email.\n\n"
                        f"From: {sender}\n"
                        f"Subject: {subject}\n"
                        f"---\n{body_text[:800]}\n---\n\n"
                        f"—Outreach Agent (automated)"
                    )
                    if not dry_run:
                        send_email(VIDA_EMAIL, fwd_subject, fwd_body)
                        # Mark as read so it doesn't re-appear
                        mail.store(mid, "+FLAGS", "\\Seen")
                    genuine_count += 1

            except Exception as e:
                log.warning(f"  Error processing message {mid}: {e}")
                continue

        mail.logout()

        state["do_not_contact"] = list(do_not_contact)
        save_state(state)

        log.info(
            f"  Reply check done: {genuine_count} genuine (forwarded), "
            f"{archived_count} archived (bounces/OOOs/unsubs incl. {unsubscribe_count} unsubs)"
        )

    except Exception as e:
        log.error(f"Reply check error: {e}")
        notify_chris(f"Reply check failed: {e}")

# ─── Manae roster ─────────────────────────────────────────────────────────────

def send_manae_roster(state, dry_run=False):
    roster = state.get("manae_roster_pending", [])
    if not roster:
        log.info("Manae roster: nothing accumulated. Skipping.")
        return

    if state.get("last_roster_send") == today_str():
        log.info("Manae roster already sent today. Skipping.")
        return

    log.info(f"Sending Manae roster ({len(roster)} customers)...")
    rows = []
    for i, entry in enumerate(roster, 1):
        c  = entry.get("contact", {})
        hs = entry.get("hubspot", {})
        name    = f"{c.get('firstName','')} {c.get('lastName','')}".strip() or c.get("email","")
        company = c.get("company","")
        email   = c.get("email","")
        deal    = hs.get("lastDealName") or "N/A"
        value   = f"${hs.get('lastDealValue',0):,.0f}" if hs.get("lastDealValue") else ""
        ddate   = hs.get("lastDealDate","")
        rows.append(
            f"{i}. {name}{' — ' + company if company else ''} ({email})\n"
            f"   Last deal: {deal}{' · ' + value if value else ''}{' · ' + ddate if ddate else ''}"
        )

    week_end = (date.today() - timedelta(days=1)).strftime("%b %d")
    subject = f"[Weekly Roster] {len(roster)} existing customers to follow up — week ending {week_end}"
    body = (
        f"Hi Manae,\n\n"
        f"Here are {len(roster)} existing Get CPR Done customers from our Mailchimp "
        f"open-not-booked list this week. They were filtered from the cold outreach batch "
        f"because they already have a relationship with us — these are yours to follow up directly.\n\n"
        + "\n\n".join(rows) +
        f"\n\n—Vida Monroe | {COMPANY_NAME} (via automated Outreach Agent)"
    )

    if not dry_run:
        result = send_email(MANAE_EMAIL, subject, body)
        if result.get("success"):
            state["last_roster_send"] = today_str()
            state["manae_roster_pending"] = []
            save_state(state)
            log.info(f"  → Roster sent to Manae ({len(roster)} customers)")
        else:
            err = result.get("error","unknown")
            log.error(f"  → Roster send failed: {err}")
            notify_chris(f"Manae roster send failed: {err}")
    else:
        log.info(f"  [DRY RUN] Would send {len(roster)}-contact roster to Manae")
        state["last_roster_send"] = today_str()
        state["manae_roster_pending"] = []
        save_state(state)

# ─── Main daily run ───────────────────────────────────────────────────────────

def run_daily(dry_run=False):
    state    = load_state()
    today    = today_str()
    daily_counts = state.get("daily_sent_count", {})
    already_sent = daily_counts.get(today, 0)

    if already_sent >= BATCH_SIZE:
        log.info(f"Daily cap reached ({already_sent}/{BATCH_SIZE}). Exiting.")
        return

    remaining = BATCH_SIZE - already_sent
    contacted      = {e.lower() for e in state.get("contacted_emails", [])}
    do_not_contact = {e.lower() for e in state.get("do_not_contact", [])}

    # Track today's stats for EOD report
    today_sent    = 0
    today_bounces = []
    today_skipped_roles = []
    replacement_queue_snapshot = []

    try:
        # 1 — Mailchimp (all lists)
        raw = fetch_mailchimp_contacts(remaining + 20, do_not_contact=do_not_contact)

        # Track any role addresses that were skipped during fetch
        for c in raw:
            if is_role_address(c.get("email", "")):
                today_skipped_roles.append(c["email"])

        fresh = [c for c in raw if c.get("email","").lower() not in {e.lower() for e in contacted}
                 and c.get("email","").lower() not in do_not_contact]
        log.info(f"After dedup: {len(fresh)} fresh contacts")
        if not fresh:
            log.info("No new contacts to process today.")
            # Still send EOD report if we've sent anything today
            if already_sent > 0:
                send_eod_report(already_sent, 0, [], state.get("bounce_replacement_queue", []), today_skipped_roles, dry_run)
            return

        # 2 — HubSpot cross-check
        prospects = []
        customers = []
        for i, c in enumerate(fresh):
            log.info(f"HubSpot check {i+1}/{len(fresh)}: {c.get('email')}")
            hs = check_hubspot(c)
            if hs.get("isCustomer"):
                customers.append({"contact": c, "hubspot": hs})
                log.info(f"  → Customer → Manae roster")
            else:
                prospects.append(c)
                log.info(f"  → Prospect → Vida outreach")

        state["manae_roster_pending"] = state.get("manae_roster_pending", []) + customers
        save_state(state)
        log.info(f"Routing: {len(prospects)} prospects, {len(customers)} added to Manae roster")

        # 3 — Generate in batches, then send
        to_send    = prospects[:remaining]
        thread_ids = list(state.get("sent_thread_ids", []))
        sent_today = already_sent

        # Pre-generate all emails in batches of GENERATION_BATCH_SIZE
        log.info(f"Generating {len(to_send)} emails in batches of {GENERATION_BATCH_SIZE}...")
        generated = []
        for batch_start in range(0, len(to_send), GENERATION_BATCH_SIZE):
            batch = to_send[batch_start:batch_start + GENERATION_BATCH_SIZE]
            batch_end = batch_start + len(batch)
            log.info(f"  Batch API call: contacts {batch_start+1}–{batch_end} of {len(to_send)}")
            try:
                results = generate_emails_batch(batch)
                generated.extend(results)
                log.info(f"  → {len(results)} emails generated")
            except Exception as e:
                log.warning(f"  Batch generation failed: {e} — using fallbacks for this batch")
                generated.extend([_fallback_result(c) for c in batch])

        log.info(f"Generation complete. Sending {len(to_send)} emails...")

        for i, (contact, email_data) in enumerate(zip(to_send, generated)):
            email   = contact.get("email","")
            subject = email_data.get("subject", FALLBACK_SUBJECT)
            body    = email_data.get("body", fallback_body(contact))
            notes   = email_data.get("personalization_notes", "")
            log.info(f"Sending {i+1}/{len(to_send)}: {email}")
            log.info(f"  Subject: {subject} | Notes: {notes}")

            if not dry_run:
                result = send_email(email, subject, body)
                if result.get("success"):
                    contacted.add(email.lower())
                    sent_today += 1
                    today_sent += 1
                    if result.get("threadId"):
                        thread_ids.append(result["threadId"])
                    log.info(f"  ✓ Sent ({sent_today}/{BATCH_SIZE} today)")
                elif result.get("hard_bounce"):
                    err = result.get("error","unknown")
                    log.warning(f"  ✗ Hard bounce for {email}: {err}")
                    # Add to do_not_contact
                    do_not_contact.add(email.lower())
                    contacted.add(email.lower())  # also mark as contacted so we don't retry
                    today_bounces.append(email)
                    # Queue replacement search
                    search_replacement_contact(email, state)
                else:
                    err = result.get("error","unknown")
                    log.error(f"  ✗ Failed: {err}")
                    notify_chris(f"Send failed for {email}", err)
            else:
                log.info(f"  [DRY RUN] Would send: {subject}")
                contacted.add(email.lower())
                sent_today += 1
                today_sent += 1

            state["contacted_emails"]  = list(contacted)
            state["do_not_contact"]    = list(do_not_contact)
            state["sent_thread_ids"]   = thread_ids
            daily_counts[today]        = sent_today
            state["daily_sent_count"]  = daily_counts
            save_state(state)

            if i < len(to_send) - 1 and not dry_run:
                delay = random.randint(MIN_DELAY_SEC, MAX_DELAY_SEC)
                log.info(f"  Waiting {delay}s...")
                time.sleep(delay)

        log.info(f"Daily run complete. {sent_today - already_sent} sent today.")

        # 4 — End-of-day report to Chris
        # Compute warmth breakdown for report
        hot    = [c for c in to_send if c.get("warmthScore", 999) < 30]
        warm   = [c for c in to_send if 30 <= c.get("warmthScore", 999) < 180]
        cold   = [c for c in to_send if c.get("warmthScore", 999) >= 180]
        replacement_queue_snapshot = state.get("bounce_replacement_queue", [])
        send_eod_report(
            sent_count=sent_today,
            bounce_count=len(today_bounces),
            bounce_list=today_bounces,
            replacement_queue=replacement_queue_snapshot,
            skipped_role=today_skipped_roles,
            dry_run=dry_run,
            warmth_breakdown={"hot": len(hot), "warm": len(warm), "cold": len(cold)},
        )

    except Exception as e:
        log.exception(f"Fatal error: {e}")
        notify_chris(str(e), f"Daily run failed on {today}")
        sys.exit(1)

# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily","roster","reply_check"], required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-weekday", action="store_true")
    parser.add_argument("--force", action="store_true")  # alias
    args = parser.parse_args()

    # Acquire exclusive lock — exits immediately if another instance is running
    _lock_fh = _acquire_lock()

    if not args.force_weekday and not args.force and not is_weekday():
        log.info(f"Today is {date.today().strftime('%A')} — agent only runs M–F. Exiting.")
        sys.exit(0)

    if args.dry_run:
        log.info("=== DRY RUN MODE — no emails will be sent ===")

    log.info(f"=== Outreach Agent | mode={args.mode} | {datetime.now().isoformat()} ===")

    state = load_state()

    if args.mode == "daily":
        run_daily(dry_run=args.dry_run)
    elif args.mode == "roster":
        if not is_monday() and not args.force_weekday and not args.force:
            log.info("Roster mode only runs Mondays. Exiting.")
            sys.exit(0)
        send_manae_roster(state, dry_run=args.dry_run)
    elif args.mode == "reply_check":
        check_replies(state, dry_run=args.dry_run)

    log.info(f"=== Complete | {datetime.now().isoformat()} ===\n")

if __name__ == "__main__":
    main()
