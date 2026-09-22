#!/usr/bin/env python3
"""
ClawBuildr LinkedIn Integration — Campaign management, warming status, learning.
Wraps linkedin_engine.py with higher-level campaign and tracking features.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.LinkedInIntegration")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def get_linkedin_stats() -> Dict[str, Any]:
    db = _get_db()
    try:
        total_outreach = db.execute("SELECT COUNT(*) FROM linkedin_outreach").fetchone()[0]
        accepted = db.execute(
            "SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'accepted'"
        ).fetchone()[0]
        replied = db.execute(
            "SELECT COUNT(*) FROM linkedin_outreach WHERE reply_body IS NOT NULL AND reply_body != ''"
        ).fetchone()[0]
        pending = db.execute(
            "SELECT COUNT(*) FROM linkedin_outreach WHERE connection_status = 'pending'"
        ).fetchone()[0]

        accept_rate = round(accepted / max(total_outreach, 1) * 100, 1)
        reply_rate = round(replied / max(total_outreach, 1) * 100, 1)

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sent_today = db.execute(
            "SELECT COUNT(*) FROM linkedin_outreach WHERE date(timestamp) = ?",
            (today,),
        ).fetchone()[0]

        return {
            "total_outreach": total_outreach,
            "accepted": accepted,
            "replied": replied,
            "pending": pending,
            "accept_rate": accept_rate,
            "reply_rate": reply_rate,
            "sent_today": sent_today,
        }
    finally:
        db.close()


def get_linkedin_campaigns() -> List[Dict[str, Any]]:
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT * FROM linkedin_outreach ORDER BY timestamp DESC LIMIT 50"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_warming_status() -> Dict[str, Any]:
    db = _get_db()
    try:
        row = db.execute("SELECT * FROM warming_state ORDER BY id DESC LIMIT 1").fetchone()
        if row:
            return dict(row)
        return {"status": "not_started", "day": 0, "daily_limit": 5}
    finally:
        db.close()


def record_linkedin_action(contact_id: int, action: str, profile_url: str = "",
                           note: str = "", outcome: str = "") -> int:
    db = _get_db()
    try:
        contact = db.execute(
            "SELECT first_name, last_name FROM contacts WHERE contact_id = ?", (contact_id,)
        ).fetchone()

        cursor = db.execute(
            """INSERT INTO linkedin_outreach
               (contact_id, first_name, last_name, profile_url, note, outcome, timestamp, connection_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (contact_id,
             contact["first_name"] if contact else "",
             contact["last_name"] if contact else "",
             profile_url, note, outcome,
             datetime.now(timezone.utc).isoformat(),
             "pending" if action == "connect" else "sent"),
        )
        db.commit()
        return cursor.lastrowid
    finally:
        db.close()


def get_linkedin_learning() -> Dict[str, Any]:
    db = _get_db()
    try:
        notes = db.execute(
            """SELECT note, COUNT(*) as cnt, outcome
               FROM linkedin_outreach
               WHERE note IS NOT NULL AND note != ''
               GROUP BY note, outcome
               ORDER BY cnt DESC LIMIT 10"""
        ).fetchall()

        return {
            "top_notes": [{"note": r["note"], "count": r["cnt"], "outcome": r["outcome"]} for r in notes],
        }
    finally:
        db.close()


if __name__ == "__main__":
    stats = get_linkedin_stats()
    print(json.dumps(stats, indent=2))
