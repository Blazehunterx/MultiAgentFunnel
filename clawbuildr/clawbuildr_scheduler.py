#!/usr/bin/env python3
"""
ClawBuildr Scheduler — Follow-up sequence management.
Runs follow-up emails on a schedule, skipping contacts with positive replies.
"""

import os
import json
import sqlite3
import logging
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Scheduler")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

FOLLOWUP_SEQUENCE = [
    {"step": 1, "delay_hours": 48, "subject_prefix": "Re: ", "tone": "gentle"},
    {"step": 2, "delay_hours": 96, "subject_prefix": "Re: ", "tone": "value_add"},
    {"step": 3, "delay_hours": 168, "subject_prefix": "Re: ", "tone": "breakup"},
]

_DAILY_LIMIT = 50


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_followup_table():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS followup_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id INTEGER NOT NULL,
                step INTEGER NOT NULL DEFAULT 1,
                scheduled_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                sent_at TEXT,
                email_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contact_id) REFERENCES contacts(contact_id)
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_followup_pending
            ON followup_queue(status, scheduled_at)
        """)
        db.commit()
    finally:
        db.close()


def enroll_contact(contact_id: int, campaign_id: int = 1) -> bool:
    """Enroll a contact in the follow-up sequence. Returns True if enrolled."""
    _ensure_followup_table()
    db = _get_db()
    try:
        existing = db.execute(
            "SELECT id FROM followup_queue WHERE contact_id = ? AND status = 'pending'",
            (contact_id,),
        ).fetchone()
        if existing:
            return False

        now = datetime.now(timezone.utc)
        first_delay = timedelta(hours=FOLLOWUP_SEQUENCE[0]["delay_hours"])

        db.execute(
            """INSERT INTO followup_queue (contact_id, step, scheduled_at, status, created_at)
               VALUES (?, 1, ?, 'pending', ?)""",
            (contact_id, (now + first_delay).isoformat(), now.isoformat()),
        )
        db.commit()
        logger.info(f"Enrolled contact {contact_id} in follow-up sequence")
        return True
    finally:
        db.close()


def schedule_next_step(contact_id: int, current_step: int) -> bool:
    """Schedule the next follow-up step. Returns True if scheduled."""
    db = _get_db()
    try:
        next_step = current_step + 1
        if next_step > len(FOLLOWUP_SEQUENCE):
            return False

        now = datetime.now(timezone.utc)
        delay = timedelta(hours=FOLLOWUP_SEQUENCE[next_step - 1]["delay_hours"])

        db.execute(
            """INSERT INTO followup_queue (contact_id, step, scheduled_at, status, created_at)
               VALUES (?, ?, ?, 'pending', ?)""",
            (contact_id, next_step, (now + delay).isoformat(), now.isoformat()),
        )
        db.commit()
        return True
    finally:
        db.close()


def process_due_followups() -> List[Dict[str, Any]]:
    """Process follow-ups that are due. Returns list of sent follow-ups."""
    _ensure_followup_table()
    db = _get_db()
    sent = []

    try:
        now = datetime.now(timezone.utc).isoformat()
        due = db.execute(
            """SELECT fq.*, c.first_name, c.last_name, c.email, c.current_stage, co.domain
               FROM followup_queue fq
               JOIN contacts c ON fq.contact_id = c.contact_id
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE ((fq.status = 'pending' AND fq.scheduled_at <= ?)
                  OR fq.status = 'ready')
                 AND c.current_stage = 'EMAIL_SENT'
                 AND c.email NOT LIKE '%naam@%' AND c.email NOT LIKE '%voorbeeld@%'
                 AND c.email NOT LIKE '%jouw@%' AND c.email NOT LIKE '%test@%'
                 AND c.email NOT LIKE '%example@%' AND c.email NOT LIKE '%demo@%'
                 AND (co.domain LIKE '%.nl' OR co.domain LIKE '%.be' OR co.domain LIKE '%.de')
               ORDER BY fq.scheduled_at ASC
               LIMIT ?""",
            (now, _DAILY_LIMIT),
        ).fetchall()

        today_sent = db.execute(
            "SELECT COUNT(*) FROM followup_queue WHERE status = 'sent' AND date(sent_at) = date('now')"
        ).fetchone()[0]

        remaining = _DAILY_LIMIT - today_sent

        for item in due[:remaining]:
            try:
                contact_stage = item["current_stage"]
                if contact_stage in ("CLOSED_LOST", "MEETING_BOOKED", "CLOSED_WON"):
                    db.execute(
                        "UPDATE followup_queue SET status = 'skipped' WHERE id = ?",
                        (item["id"],),
                    )
                    continue

                if contact_stage == "REPLIED":
                    db.execute(
                        "UPDATE followup_queue SET status = 'skipped' WHERE id = ?",
                        (item["id"],),
                    )
                    continue

                step_info = FOLLOWUP_SEQUENCE[item["step"] - 1]
                logger.info(
                    f"Follow-up step {item['step']} due for {item['email']} "
                    f"(scheduled: {item['scheduled_at']})"
                )

                db.execute(
                    "UPDATE followup_queue SET status = 'ready' WHERE id = ?",
                    (item["id"],),
                )
                db.commit()

                sent.append({
                    "queue_id": item["id"],
                    "contact_id": item["contact_id"],
                    "email": item["email"],
                    "step": item["step"],
                    "tone": step_info["tone"],
                })

            except Exception as e:
                logger.error(f"Error processing follow-up {item['id']}: {e}")
                continue

    finally:
        db.close()

    return sent


def mark_sent(queue_id: int, email_id: int) -> None:
    """Mark a follow-up as sent."""
    db = _get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "UPDATE followup_queue SET status = 'sent', sent_at = ?, email_id = ? WHERE id = ?",
            (now, email_id, queue_id),
        )
        db.commit()
    finally:
        db.close()


def get_pending_followups() -> List[Dict[str, Any]]:
    """Get all pending follow-ups."""
    _ensure_followup_table()
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT fq.*, c.first_name, c.last_name, c.email
               FROM followup_queue fq
               JOIN contacts c ON fq.contact_id = c.contact_id
               WHERE fq.status IN ('pending', 'ready')
               ORDER BY fq.scheduled_at ASC"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_followup_stats() -> Dict[str, Any]:
    """Get follow-up statistics."""
    _ensure_followup_table()
    db = _get_db()
    try:
        stats = {}
        for status in ("pending", "ready", "sent", "skipped"):
            count = db.execute(
                "SELECT COUNT(*) FROM followup_queue WHERE status = ?", (status,)
            ).fetchone()[0]
            stats[status] = count

        by_step = {}
        for row in db.execute(
            "SELECT step, status, COUNT(*) as cnt FROM followup_queue GROUP BY step, status"
        ).fetchall():
            by_step[f"step_{row['step']}_{row['status']}"] = row["cnt"]

        return {"by_status": stats, "by_step": by_step}
    finally:
        db.close()


if __name__ == "__main__":
    _ensure_followup_table()
    due = process_due_followups()
    print(f"Follow-ups due: {len(due)}")
    for f in due:
        print(f"  Step {f['step']} → {f['email']} ({f['tone']})")
