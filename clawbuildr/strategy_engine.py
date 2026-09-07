"""
Strategy Engine — Multi-step outreach sequence management.
Handles advancing leads through strategy steps (email -> LinkedIn -> follow-up -> final).
"""
import sqlite3
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger("StrategyEngine")

import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "clawbuildr.db")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def get_active_strategy(tenant_id: str) -> Optional[Dict]:
    conn = _db()
    row = conn.execute(
        "SELECT * FROM strategies WHERE tenant_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 1",
        (tenant_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_strategy_steps(strategy_id: str) -> list:
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM strategy_steps WHERE strategy_id = ? ORDER BY step_number ASC",
        (strategy_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def enroll_lead_in_strategy(lead_id: str, strategy_id: str):
    conn = _db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO lead_sequences (lead_id, strategy_id, current_step, next_action_at, status, enrolled_at)
            VALUES (?, ?, 1, ?, 'ACTIVE', ?)
            ON CONFLICT(lead_id, strategy_id) DO UPDATE SET
                status = 'ACTIVE', current_step = 1, next_action_at = excluded.next_action_at
        """, (lead_id, strategy_id, now, now))
        conn.commit()
        logger.info(f"[Strategy] Enrolled lead {lead_id} in strategy {strategy_id}")
    finally:
        conn.close()


def pause_sequence(lead_id: str, reason: str):
    conn = _db()
    try:
        conn.execute(
            "UPDATE lead_sequences SET status = 'PAUSED', pause_reason = ? WHERE lead_id = ? AND status = 'ACTIVE'",
            (reason, lead_id)
        )
        conn.commit()
    finally:
        conn.close()


def stop_sequence(lead_id: str, reason: str = "COMPLETED"):
    conn = _db()
    try:
        conn.execute(
            "UPDATE lead_sequences SET status = 'STOPPED', pause_reason = ? WHERE lead_id = ? AND status IN ('ACTIVE', 'PAUSED')",
            (reason, lead_id)
        )
        conn.commit()
    finally:
        conn.close()


def get_leads_due_for_next_step() -> list:
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()
    rows = conn.execute("""
        SELECT ls.*, s.tenant_id FROM lead_sequences ls
        JOIN strategies s ON ls.strategy_id = s.strategy_id
        WHERE ls.status = 'ACTIVE' AND ls.next_action_at <= ?
    """, (now,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def advance_sequence(lead_id: str, strategy_id: str) -> Optional[Dict]:
    conn = _db()
    try:
        seq = conn.execute(
            "SELECT * FROM lead_sequences WHERE lead_id = ? AND strategy_id = ?",
            (lead_id, strategy_id)
        ).fetchone()
        if not seq or seq["status"] != "ACTIVE":
            return None

        current_step_num = seq["current_step"]
        current_step = conn.execute(
            "SELECT * FROM strategy_steps WHERE strategy_id = ? AND step_number = ?",
            (strategy_id, current_step_num)
        ).fetchone()
        if not current_step:
            return None

        contact = conn.execute("SELECT current_stage FROM contacts WHERE contact_id = ?", (lead_id,)).fetchone()
        stop_conditions = json.loads(current_step["stop_if"] or "[]")
        if contact and contact["current_stage"] in stop_conditions:
            conn.execute(
                "UPDATE lead_sequences SET status = 'STOPPED', pause_reason = ? WHERE lead_id = ? AND strategy_id = ?",
                (f"Stop stage: {contact['current_stage']}", lead_id, strategy_id)
            )
            conn.commit()
            return None

        next_step = conn.execute(
            "SELECT * FROM strategy_steps WHERE strategy_id = ? AND step_number = ?",
            (strategy_id, current_step_num + 1)
        ).fetchone()

        if next_step:
            delay_days = next_step["delay_days"] or 0
            next_action_at = (datetime.now(timezone.utc) + timedelta(days=delay_days)).isoformat()
            conn.execute("""
                UPDATE lead_sequences SET current_step = ?, next_action_at = ?, last_step_at = ?
                WHERE lead_id = ? AND strategy_id = ?
            """, (current_step_num + 1, next_action_at, datetime.now(timezone.utc).isoformat(), lead_id, strategy_id))
        else:
            conn.execute(
                "UPDATE lead_sequences SET status = 'COMPLETED', pause_reason = 'All steps done' WHERE lead_id = ? AND strategy_id = ?",
                (lead_id, strategy_id)
            )

        conn.commit()
        return dict(current_step)
    finally:
        conn.close()


def render_template(template_body: str, variables: Dict[str, str]) -> str:
    result = template_body
    for key, value in variables.items():
        result = result.replace("{{" + key + "}}", value or "")
    return result


def get_template_variables(template_body: str) -> list:
    import re
    return list(set(re.findall(r'\{\{(\w+)\}\}', template_body)))
