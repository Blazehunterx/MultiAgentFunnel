#!/usr/bin/env python3
"""
ClawBuildr Lead Scoring — Intelligent lead scoring based on multiple signals.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

logger = logging.getLogger("ClawBuildr.LeadScoring")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


ROLE_SCORES = {
    "founder": 25, "ceo": 25, "owner": 25, "directeur": 25, "oprichter": 25,
    "cto": 20, "cmo": 20, "cfo": 20, "vp": 18, "vice president": 18,
    "manager": 15, "lead": 12, "head": 12, "director": 15,
    "specialist": 8, "analyst": 6, "coordinator": 5, "assistant": 3,
}

STAGE_SCORES = {
    "MEETING_BOOKED": 50, "QUALIFIED": 40, "REPLIED": 30,
    "ACTIVE_OUTREACH": 10, "OUTREACH_DRAFTED": 8, "PENDING_APPROVAL": 5,
    "PRE_QUALIFIED": 5, "OPPORTUNITY_MAPPED": 3, "DELIVERABILITY_VERIFIED": 2,
    "RESEARCHED": 1, "INGESTED": 0,
    "CLOSED_WON": 100, "CLOSED_LOST": -20, "BOUNCED": -15,
    "NURTURE": 2,
}


def score_contact(contact_id: int) -> Dict[str, Any]:
    db = _get_db()
    try:
        contact = db.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if not contact:
            return {"error": "Contact not found"}

        score = 0
        breakdown = {}

        role = (contact["role"] or "").lower()
        role_score = 0
        for keyword, pts in ROLE_SCORES.items():
            if keyword in role:
                role_score = pts
                break
        score += role_score
        breakdown["role"] = role_score

        stage = contact["current_stage"] or "INGESTED"
        stage_score = STAGE_SCORES.get(stage, 0)
        score += stage_score
        breakdown["stage"] = stage_score

        has_email = bool(contact["email"])
        has_linkedin = bool(contact["linkedin_url"])
        has_research = bool(contact["research_result"])

        data_score = (10 if has_email else 0) + (10 if has_linkedin else 0) + (10 if has_research else 0)
        score += data_score
        breakdown["data_completeness"] = data_score

        emails = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE contact_id = ?", (contact_id,)
        ).fetchone()["cnt"]
        activity_score = min(emails * 5, 20)
        score += activity_score
        breakdown["activity"] = activity_score

        score = max(0, min(100, score))

        db.execute(
            "UPDATE contacts SET lead_score = ?, updated_at = ? WHERE contact_id = ?",
            (score, datetime.now(timezone.utc).isoformat(), contact_id),
        )
        db.commit()

        tier = "hot" if score >= 70 else "warm" if score >= 40 else "cold"

        return {
            "contact_id": contact_id,
            "score": score,
            "tier": tier,
            "breakdown": breakdown,
        }
    finally:
        db.close()


def score_all_contacts() -> Dict[str, Any]:
    db = _get_db()
    try:
        contacts = db.execute("SELECT contact_id FROM contacts").fetchall()
        results = []
        for c in contacts:
            result = score_contact(c["contact_id"])
            results.append(result)

        tiers = {"hot": 0, "warm": 0, "cold": 0}
        for r in results:
            if "tier" in r:
                tiers[r["tier"]] += 1

        return {"scored": len(results), "tiers": tiers}
    finally:
        db.close()


def get_top_leads(limit: int = 10) -> List[Dict[str, Any]]:
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT contact_id, first_name, last_name, email, role, current_stage, lead_score
               FROM contacts ORDER BY lead_score DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


if __name__ == "__main__":
    result = score_all_contacts()
    print(f"Scored: {result['scored']}, Tiers: {result['tiers']}")
    top = get_top_leads(5)
    for t in top:
        print(f"  {t['first_name']} {t['last_name']}: {t['lead_score']} ({t['current_stage']})")
