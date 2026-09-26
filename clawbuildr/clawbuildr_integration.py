#!/usr/bin/env python3
"""
ClawBuildr Integration — Pipeline orchestration, content learning, lead scoring.
Ties together all modules into a unified workflow.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Integration")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def run_enrichment_pipeline(contact_id: int) -> Dict[str, Any]:
    """Run the full enrichment pipeline for a contact."""
    db = _get_db()
    try:
        contact = db.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if not contact:
            return {"error": "Contact not found"}

        result = {"contact_id": contact_id, "steps": {}}

        try:
            from enrich_lead import research_company
            research = research_company(
                company_name=contact["first_name"] or "",
                linkedin_url=contact["linkedin_url"] or "",
            )
            result["steps"]["research"] = {"status": "done", "data": research}

            db.execute(
                "UPDATE contacts SET research_result = ?, updated_at = ? WHERE contact_id = ?",
                (json.dumps(research), datetime.now(timezone.utc).isoformat(), contact_id),
            )
        except Exception as e:
            result["steps"]["research"] = {"status": "error", "error": str(e)[:200]}

        try:
            from clawbuildr_ai_email import generate_ai_email
            email_result = generate_ai_email(
                contact_id=contact_id,
                company_name=contact["first_name"] or "",
                contact_name=f"{contact['first_name'] or ''} {contact['last_name'] or ''}",
                role=contact["role"] or "",
            )
            result["steps"]["email"] = {"status": "done", "data": email_result}

            if email_result.get("subject") and email_result.get("body"):
                db.execute(
                    "UPDATE contacts SET outreach_draft = ?, updated_at = ? WHERE contact_id = ?",
                    (json.dumps(email_result), datetime.now(timezone.utc).isoformat(), contact_id),
                )
        except Exception as e:
            result["steps"]["email"] = {"status": "error", "error": str(e)[:200]}

        db.commit()
        return result
    finally:
        db.close()


def run_reply_processing() -> Dict[str, Any]:
    """Run reply detection and processing."""
    try:
        from clawbuildr_reply_detector import check_replies
        replies = check_replies(since_hours=24)
        return {"processed": len(replies), "replies": replies}
    except Exception as e:
        return {"error": str(e)[:200]}


def run_followup_processing() -> Dict[str, Any]:
    """Process due follow-ups."""
    try:
        from clawbuildr_scheduler import process_due_followups
        due = process_due_followups()
        return {"due": len(due), "followups": due}
    except Exception as e:
        return {"error": str(e)[:200]}


def score_lead(contact_id: int) -> Dict[str, Any]:
    """Score a lead based on available data."""
    db = _get_db()
    try:
        contact = db.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if not contact:
            return {"error": "Contact not found"}

        score = 50
        reasons = []

        if contact["role"]:
            role_lower = contact["role"].lower()
            if any(r in role_lower for r in ["founder", "ceo", "owner", "directeur", "oprichter"]):
                score += 20
                reasons.append("Decision maker")
            elif any(r in role_lower for r in ["manager", "lead", "head"]):
                score += 10
                reasons.append("Manager role")

        if contact["linkedin_url"]:
            score += 10
            reasons.append("Has LinkedIn")

        if contact["email"]:
            score += 5
            reasons.append("Has email")

        if contact["research_result"]:
            score += 10
            reasons.append("Has research data")

        stage = contact["current_stage"]
        if stage == "REPLIED":
            score += 30
            reasons.append("Replied")
        elif stage == "MEETING_BOOKED":
            score += 50
            reasons.append("Meeting booked")
        elif stage == "ACTIVE_OUTREACH":
            score += 5
            reasons.append("In outreach")

        score = min(score, 100)

        db.execute(
            "UPDATE contacts SET lead_score = ?, updated_at = ? WHERE contact_id = ?",
            (score, datetime.now(timezone.utc).isoformat(), contact_id),
        )
        db.commit()

        return {"contact_id": contact_id, "score": score, "reasons": reasons}
    finally:
        db.close()


def get_pipeline_overview() -> Dict[str, Any]:
    """Get overview of the entire pipeline."""
    db = _get_db()
    try:
        stages = {}
        for row in db.execute(
            "SELECT current_stage, COUNT(*) as cnt FROM contacts GROUP BY current_stage"
        ).fetchall():
            stages[row["current_stage"]] = row["cnt"]

        total_contacts = sum(stages.values())

        emails_sent = db.execute("SELECT COUNT(*) FROM emails WHERE UPPER(direction) = 'OUTBOUND'").fetchone()[0]
        emails_received = db.execute("SELECT COUNT(*) FROM emails WHERE UPPER(direction) = 'INBOUND'").fetchone()[0]
        linkedin_sent = db.execute("SELECT COUNT(*) FROM linkedin_outreach").fetchone()[0]
        linkedin_accepted = db.execute(
            "SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'accepted'"
        ).fetchone()[0]

        reply_rate = round(emails_received / emails_sent * 100, 1) if emails_sent > 0 else 0
        accept_rate = round(linkedin_accepted / linkedin_sent * 100, 1) if linkedin_sent > 0 else 0

        return {
            "total_contacts": total_contacts,
            "stages": stages,
            "emails_sent": emails_sent,
            "emails_received": emails_received,
            "reply_rate_pct": reply_rate,
            "linkedin_sent": linkedin_sent,
            "linkedin_accepted": linkedin_accepted,
            "accept_rate_pct": accept_rate,
        }
    finally:
        db.close()


def get_activity_feed(limit: int = 50) -> List[Dict[str, Any]]:
    """Get recent activity feed."""
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT al.*, c.first_name, c.last_name, c.email
               FROM activity_log al
               LEFT JOIN contacts c ON al.contact_id = c.contact_id
               ORDER BY al.created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


if __name__ == "__main__":
    overview = get_pipeline_overview()
    print(json.dumps(overview, indent=2))
