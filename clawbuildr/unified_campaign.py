"""
Unified Campaign Engine - LinkedIn + Email sequences in one system.
Uses existing campaign_flows/campaign_flow_messages tables.
flow_type: 'linkedin' | 'email' | 'multi' (both channels in sequence)
"""

import sqlite3
import json
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger("UnifiedCampaign")

DB_PATH = "data/clawbuildr.db"


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


# ─── Campaign CRUD ───────────────────────────────────────────────

def create_campaign(name: str, flow_type: str = "multi", manual_control: int = 1,
                    account_id: str = None, settings: dict = None) -> dict:
    """Create a campaign with default flow structure."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()
    cid = conn.execute(
        "INSERT INTO campaigns (name, manual_control, ai_confidence_threshold, status, created_at, updated_at, account_id) "
        "VALUES (?, ?, 80, 'draft', ?, ?, ?)",
        (name, manual_control, now, now, account_id)
    ).lastrowid
    conn.commit()

    # Create default flow
    flow_id = conn.execute(
        "INSERT INTO campaign_flows (campaign_id, flow_type, is_active, created_at) VALUES (?, ?, 1, ?)",
        (cid, flow_type, now)
    ).lastrowid
    conn.commit()
    conn.close()

    return {"campaign_id": cid, "flow_id": flow_id, "name": name, "flow_type": flow_type}


def add_flow_step(flow_id: int, step_number: int, message_text: str,
                  delay_days: int = 0, channel: str = None) -> dict:
    """Add a step to a campaign flow."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()
    msg_id = conn.execute(
        "INSERT INTO campaign_flow_messages (flow_id, step_number, message_text, delay_days, is_active, created_at) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        (flow_id, step_number, message_text, delay_days, now)
    ).lastrowid
    conn.commit()
    conn.close()
    return {"message_id": msg_id, "flow_id": flow_id, "step": step_number}


def get_campaign(campaign_id: int) -> dict:
    """Get full campaign with flows and steps."""
    conn = _db()
    camp = conn.execute("SELECT * FROM campaigns WHERE campaign_id = ?", (campaign_id,)).fetchone()
    if not camp:
        conn.close()
        return {}

    flows = conn.execute("SELECT * FROM campaign_flows WHERE campaign_id = ?", (campaign_id,)).fetchall()
    result = dict(camp)
    result["flows"] = []

    for f in flows:
        steps = conn.execute(
            "SELECT * FROM campaign_flow_messages WHERE flow_id = ? ORDER BY step_number",
            (f["flow_id"],)
        ).fetchall()
        result["flows"].append({**dict(f), "steps": [dict(s) for s in steps]})

    conn.close()
    return result


def list_campaigns(status: str = None) -> list:
    """List all campaigns."""
    conn = _db()
    if status:
        rows = conn.execute("SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Enrollment ──────────────────────────────────────────────────

def enroll_lead(campaign_id: int, contact_id: str, flow_type: str = "email") -> dict:
    """Enroll a contact in a campaign."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()

    # Check if already enrolled
    existing = conn.execute(
        "SELECT id FROM campaign_leads WHERE campaign_id = ? AND contact_id = ?",
        (campaign_id, contact_id)
    ).fetchone()
    if existing:
        conn.close()
        return {"status": "already_enrolled", "id": existing["id"]}

    lead_id = conn.execute(
        "INSERT INTO campaign_leads (campaign_id, contact_id, flow_state, current_flow, current_step, sequence_active, enrolled_at, updated_at) "
        "VALUES (?, ?, 'pending', ?, 1, 1, ?, ?)",
        (campaign_id, contact_id, flow_type, now, now)
    ).lastrowid
    conn.commit()
    conn.close()
    return {"status": "enrolled", "lead_id": lead_id}


def get_pending_sends(campaign_id: int = None, limit: int = 50) -> list:
    """Get leads that are due for their next message step."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()

    query = """
        SELECT cl.*, c.name as campaign_name, cf.flow_type
        FROM campaign_leads cl
        JOIN campaigns c ON cl.campaign_id = c.campaign_id
        JOIN campaign_flows cf ON cl.campaign_id = cf.campaign_id AND cf.is_active = 1
        WHERE cl.sequence_active = 1
        AND cl.flow_state != 'completed'
        AND cl.flow_state != 'replied'
        AND cl.flow_state != 'meeting_booked'
    """
    params = []
    if campaign_id:
        query += " AND cl.campaign_id = ?"
        params.append(campaign_id)

    query += " ORDER BY cl.next_action_at ASC NULLS FIRST LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def advance_lead(lead_id: int, status: str = "sent") -> dict:
    """Advance a lead to the next step after sending."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()

    lead = conn.execute("SELECT * FROM campaign_leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        conn.close()
        return {"error": "lead not found"}

    # Get next step
    next_step = lead["current_step"] + 1
    flow = conn.execute(
        "SELECT * FROM campaign_flow_messages WHERE flow_id = (SELECT flow_id FROM campaign_flows WHERE campaign_id = ? AND is_active = 1) AND step_number = ?",
        (lead["campaign_id"], next_step)
    ).fetchone()

    if not flow:
        # No more steps - completed
        conn.execute(
            "UPDATE campaign_leads SET flow_state = 'completed', sequence_active = 0, updated_at = ? WHERE id = ?",
            (now, lead_id)
        )
        conn.commit()
        conn.close()
        return {"status": "completed"}

    # Schedule next step
    next_at = (datetime.now(timezone.utc) + timedelta(days=flow["delay_days"])).isoformat()
    conn.execute(
        "UPDATE campaign_leads SET current_step = ?, flow_state = 'waiting', next_action_at = ?, updated_at = ? WHERE id = ?",
        (next_step, next_at, now, lead_id)
    )
    conn.commit()
    conn.close()
    return {"status": "advanced", "next_step": next_step, "next_at": next_at}


def mark_replied(lead_id: int) -> dict:
    """Mark a lead as replied - stop sequence."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE campaign_leads SET flow_state = 'replied', sequence_active = 0, updated_at = ? WHERE id = ?",
        (now, lead_id)
    )
    conn.commit()
    conn.close()
    return {"status": "replied"}


def mark_meeting(lead_id: int) -> dict:
    """Mark a lead as meeting booked - stop sequence."""
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE campaign_leads SET flow_state = 'meeting_booked', sequence_active = 0, updated_at = ? WHERE id = ?",
        (now, lead_id)
    )
    conn.commit()
    conn.close()
    return {"status": "meeting_booked"}


# ─── Template Generation ────────────────────────────────────────

def get_flow_message(flow_id: int, step_number: int) -> Optional[dict]:
    """Get a specific flow message template."""
    conn = _db()
    msg = conn.execute(
        "SELECT * FROM campaign_flow_messages WHERE flow_id = ? AND step_number = ? AND is_active = 1",
        (flow_id, step_number)
    ).fetchone()
    conn.close()
    return dict(msg) if msg else None


# ─── Campaign Send Loop Integration ─────────────────────────────

async def process_pending_sends():
    """Main loop: find pending sends, personalize, send via appropriate channel."""
    from clawbuildr.template_scorer import personalize_template

    pending = get_pending_sends(limit=10)
    if not pending:
        return {"processed": 0}

    processed = 0
    for lead in pending:
        try:
            # Get the flow message template
            flow = _db().execute(
                "SELECT flow_id FROM campaign_flows WHERE campaign_id = ? AND is_active = 1",
                (lead["campaign_id"],)
            ).fetchone()
            if not flow:
                continue

            template = get_flow_message(flow["flow_id"], lead["current_step"])
            if not template:
                continue

            # Get contact and company data for personalization
            conn = _db()
            contact = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (lead["contact_id"],)).fetchone()
            if not contact:
                conn.close()
                continue

            company = None
            if contact["company_id"]:
                company = conn.execute("SELECT * FROM companies WHERE company_id = ?", (contact["company_id"],)).fetchone()
                if company:
                    company = dict(company)
            conn.close()

            # Personalize the template
            personalized = personalize_template(
                template["message_text"],
                contact=dict(contact),
                company=company
            )

            # Send based on flow type
            flow_type = lead.get("flow_type", "email")
            send_result = None

            if flow_type in ("email", "multi"):
                if contact["email"]:
                    from clawbuildr.tools import gmail_send
                    send_result = await gmail_send(
                        to=contact["email"],
                        subject=personalized.get("subject", "Kennismaking"),
                        body=personalized["body"]
                    )

                    # Track the send
                    conn = _db()
                    conn.execute(
                        "INSERT INTO emails (contact_id, direction, status, subject, body, sent_at, created_at) "
                        "VALUES (?, 'outbound', 'SENT', ?, ?, ?, ?)",
                        (lead["contact_id"], personalized.get("subject", ""),
                         personalized["body"], datetime.now(timezone.utc).isoformat())
                    )
                    conn.commit()
                    conn.close()

            if flow_type == "linkedin" and contact.get("linkedin_url"):
                # Queue for LinkedIn send (human approval or auto)
                conn = _db()
                conn.execute(
                    "INSERT INTO campaign_message_queue (contact_id, campaign_id, message_text, channel, status, created_at) "
                    "VALUES (?, ?, ?, 'linkedin', 'queued', ?)",
                    (lead["contact_id"], lead["campaign_id"], personalized["body"],
                     datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
                conn.close()
                send_result = {"status": "queued_for_linkedin"}

            # Advance the lead
            if send_result and send_result.get("status") in ("sent", "queued_for_linkedin"):
                advance_lead(lead["id"])
                processed += 1

        except Exception as e:
            logger.error(f"Error processing lead {lead['id']}: {e}")

    return {"processed": processed}


if __name__ == "__main__":
    # Test: create a campaign with email flow
    result = create_campaign("Test Multi-Channel", flow_type="multi")
    print(f"Created: {result}")

    # Add steps
    add_flow_step(result["flow_id"], 1,
        "Hoi {{first_name}},\n\nIk zag dat {{company_name}} actief is in {{industry}}. "
        "Wij helpen bedrijven zoals het uwe met het vergroten van hun online zichtbaarheid.\n\n"
        "Zou je open staan voor een kort gesprek om te zien of we iets voor je kunnen betekenen?\n\n"
        "Groet,\nMarvin", delay_days=0)

    add_flow_step(result["flow_id"], 2,
        "Hoi {{first_name}},\n\nEven een kort berichtje ter opvolging. "
        "Veel bedrijven in {{industry}} worstelen met het genereren van consistente leads. "
        "Wij hebben een bewezen aanpak die werkt.\n\n"
        "Plan hier een gratis kennismakingsgesprek: [CALENDLY_LINK]\n\n"
        "Groet,\nMarvin", delay_days=3)

    print("Steps added. Campaign ready.")
