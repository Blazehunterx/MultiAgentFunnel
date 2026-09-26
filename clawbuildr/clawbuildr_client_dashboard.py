#!/usr/bin/env python3
"""
ClawBuildr Client Dashboard — Client-facing dashboard views with workspace isolation.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.ClientDashboard")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def get_client_overview(workspace_id: int = None) -> Dict[str, Any]:
    db = _get_db()
    try:
        contacts = db.execute("SELECT COUNT(*) as cnt FROM contacts").fetchone()["cnt"]
        emails_sent = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE UPPER(direction) = 'OUTBOUND'"
        ).fetchone()["cnt"]
        emails_received = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE UPPER(direction) = 'INBOUND'"
        ).fetchone()["cnt"]
        linkedin_sent = db.execute("SELECT COUNT(*) as cnt FROM linkedin_outreach").fetchone()["cnt"]
        meetings = db.execute(
            "SELECT COUNT(*) as cnt FROM contacts WHERE current_stage = 'MEETING_BOOKED'"
        ).fetchone()["cnt"]

        reply_rate = round(emails_received / max(emails_sent, 1) * 100, 1)

        stages = {}
        for row in db.execute(
            "SELECT current_stage, COUNT(*) as cnt FROM contacts GROUP BY current_stage"
        ).fetchall():
            stages[row["current_stage"]] = row["cnt"]

        return {
            "contacts": contacts,
            "emails_sent": emails_sent,
            "emails_received": emails_received,
            "reply_rate": reply_rate,
            "linkedin_sent": linkedin_sent,
            "meetings": meetings,
            "stages": stages,
        }
    finally:
        db.close()


def get_client_contacts(workspace_id: int = None, stage: str = None,
                        limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    db = _get_db()
    try:
        query = "SELECT * FROM contacts"
        params = []
        if stage:
            query += " WHERE current_stage = ?"
            params.append(stage)
        query += " ORDER BY lead_score DESC, created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = db.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_client_activity(workspace_id: int = None, limit: int = 30) -> List[Dict[str, Any]]:
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


def get_client_campaigns(workspace_id: int = None) -> List[Dict[str, Any]]:
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT * FROM campaigns ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


if __name__ == "__main__":
    overview = get_client_overview()
    print(json.dumps(overview, indent=2))
