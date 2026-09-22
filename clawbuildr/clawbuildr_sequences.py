#!/usr/bin/env python3
"""
ClawBuildr Sequences — Campaign, sequence, and enrollment management with A/B testing.
"""

import os
import json
import sqlite3
import logging
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Sequences")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_sequence_tables():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sequences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER,
                name TEXT NOT NULL,
                description TEXT,
                steps TEXT NOT NULL DEFAULT '[]',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sequence_enrollments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                current_step INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                enrolled_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_step_at TEXT,
                completed_at TEXT,
                FOREIGN KEY (sequence_id) REFERENCES sequences(id),
                FOREIGN KEY (contact_id) REFERENCES contacts(contact_id),
                UNIQUE(sequence_id, contact_id)
            )
        """)
        db.commit()
    finally:
        db.close()


def create_sequence(name: str, description: str = "", steps: List[Dict] = None,
                    workspace_id: int = None) -> int:
    _ensure_sequence_tables()
    db = _get_db()
    try:
        cursor = db.execute(
            "INSERT INTO sequences (workspace_id, name, description, steps) VALUES (?, ?, ?, ?)",
            (workspace_id, name, description, json.dumps(steps or [])),
        )
        db.commit()
        return cursor.lastrowid
    finally:
        db.close()


def get_sequences(workspace_id: int = None) -> List[Dict[str, Any]]:
    _ensure_sequence_tables()
    db = _get_db()
    try:
        if workspace_id:
            rows = db.execute(
                "SELECT * FROM sequences WHERE workspace_id = ? AND is_active = 1 ORDER BY created_at DESC",
                (workspace_id,),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM sequences WHERE is_active = 1 ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def enroll_in_sequence(sequence_id: int, contact_id: int) -> bool:
    _ensure_sequence_tables()
    db = _get_db()
    try:
        existing = db.execute(
            "SELECT id FROM sequence_enrollments WHERE sequence_id = ? AND contact_id = ? AND status = 'active'",
            (sequence_id, contact_id),
        ).fetchone()
        if existing:
            return False

        db.execute(
            """INSERT INTO sequence_enrollments (sequence_id, contact_id, current_step, status, enrolled_at)
               VALUES (?, ?, 1, 'active', ?)""",
            (sequence_id, contact_id, datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
        return True
    finally:
        db.close()


def advance_enrollment(sequence_id: int, contact_id: int) -> Optional[int]:
    _ensure_sequence_tables()
    db = _get_db()
    try:
        enrollment = db.execute(
            "SELECT * FROM sequence_enrollments WHERE sequence_id = ? AND contact_id = ? AND status = 'active'",
            (sequence_id, contact_id),
        ).fetchone()
        if not enrollment:
            return None

        sequence = db.execute("SELECT * FROM sequences WHERE id = ?", (sequence_id,)).fetchone()
        if not sequence:
            return None

        steps = json.loads(sequence["steps"])
        next_step = enrollment["current_step"] + 1

        if next_step > len(steps):
            db.execute(
                "UPDATE sequence_enrollments SET status = 'completed', completed_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), enrollment["id"]),
            )
            db.commit()
            return None

        db.execute(
            "UPDATE sequence_enrollments SET current_step = ?, last_step_at = ? WHERE id = ?",
            (next_step, datetime.now(timezone.utc).isoformat(), enrollment["id"]),
        )
        db.commit()
        return next_step
    finally:
        db.close()


def get_active_enrollments(sequence_id: int = None) -> List[Dict[str, Any]]:
    _ensure_sequence_tables()
    db = _get_db()
    try:
        if sequence_id:
            rows = db.execute(
                """SELECT se.*, c.first_name, c.last_name, c.email, s.name as sequence_name
                   FROM sequence_enrollments se
                   JOIN contacts c ON se.contact_id = c.contact_id
                   JOIN sequences s ON se.sequence_id = s.id
                   WHERE se.sequence_id = ? AND se.status = 'active'
                   ORDER BY se.enrolled_at""",
                (sequence_id,),
            ).fetchall()
        else:
            rows = db.execute(
                """SELECT se.*, c.first_name, c.last_name, c.email, s.name as sequence_name
                   FROM sequence_enrollments se
                   JOIN contacts c ON se.contact_id = c.contact_id
                   JOIN sequences s ON se.sequence_id = s.id
                   WHERE se.status = 'active'
                   ORDER BY se.enrolled_at"""
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def pause_enrollment(sequence_id: int, contact_id: int) -> bool:
    db = _get_db()
    try:
        db.execute(
            "UPDATE sequence_enrollments SET status = 'paused' WHERE sequence_id = ? AND contact_id = ? AND status = 'active'",
            (sequence_id, contact_id),
        )
        db.commit()
        return True
    finally:
        db.close()


def get_sequence_stats(sequence_id: int) -> Dict[str, Any]:
    db = _get_db()
    try:
        total = db.execute(
            "SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ?", (sequence_id,)
        ).fetchone()[0]
        active = db.execute(
            "SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ? AND status = 'active'",
            (sequence_id,),
        ).fetchone()[0]
        completed = db.execute(
            "SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ? AND status = 'completed'",
            (sequence_id,),
        ).fetchone()[0]
        paused = db.execute(
            "SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ? AND status = 'paused'",
            (sequence_id,),
        ).fetchone()[0]

        return {
            "total": total,
            "active": active,
            "completed": completed,
            "paused": paused,
            "completion_rate": round(completed / max(total, 1) * 100, 1),
        }
    finally:
        db.close()


if __name__ == "__main__":
    _ensure_sequence_tables()
    seq_id = create_sequence("Test Sequence", "A 3-step test", [
        {"delay_hours": 0, "type": "email", "template": "intro"},
        {"delay_hours": 48, "type": "email", "template": "followup_1"},
        {"delay_hours": 96, "type": "email", "template": "followup_2"},
    ])
    print(f"Created sequence: {seq_id}")
