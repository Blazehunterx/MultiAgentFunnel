#!/usr/bin/env python3
"""
ClawBuildr Learning — Self-learning engine.
Tracks email performance, A/B test optimization, timing optimization, sentiment history.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Learning")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_learning_tables():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS email_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_name TEXT,
                subject_pattern TEXT,
                body_pattern TEXT,
                sent_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                bounce_count INTEGER DEFAULT 0,
                meeting_count INTEGER DEFAULT 0,
                reply_rate REAL DEFAULT 0.0,
                meeting_rate REAL DEFAULT 0.0,
                avg_confidence REAL DEFAULT 0.0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS ab_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_name TEXT NOT NULL,
                variant_a TEXT NOT NULL,
                variant_b TEXT NOT NULL,
                variant_a_sent INTEGER DEFAULT 0,
                variant_b_sent INTEGER DEFAULT 0,
                variant_a_replies INTEGER DEFAULT 0,
                variant_b_replies INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                winner TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ended_at TEXT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS timing_optimization (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_of_day INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                sent_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                reply_rate REAL DEFAULT 0.0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id INTEGER NOT NULL,
                sentiment TEXT NOT NULL,
                confidence REAL DEFAULT 0.0,
                email_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS learnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                insight TEXT NOT NULL,
                evidence TEXT,
                confidence REAL DEFAULT 0.5,
                times_applied INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.commit()
    finally:
        db.close()


def record_email_performance(template_name: str, subject: str, body: str,
                              confidence: float = 0.0) -> int:
    _ensure_learning_tables()
    db = _get_db()
    try:
        cursor = db.execute(
            """INSERT INTO email_performance
               (template_name, subject_pattern, body_pattern, sent_count, avg_confidence)
               VALUES (?, ?, ?, 1, ?)""",
            (template_name, subject[:200], body[:500], confidence),
        )
        db.commit()
        return cursor.lastrowid
    finally:
        db.close()


def record_reply(email_perf_id: int, is_meeting: bool = False):
    db = _get_db()
    try:
        db.execute(
            "UPDATE email_performance SET reply_count = reply_count + 1, updated_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), email_perf_id),
        )
        if is_meeting:
            db.execute(
                "UPDATE email_performance SET meeting_count = meeting_count + 1 WHERE id = ?",
                (email_perf_id,),
            )
        db.execute("""
            UPDATE email_performance
            SET reply_rate = CAST(reply_count AS REAL) / MAX(sent_count, 1),
                meeting_rate = CAST(meeting_count AS REAL) / MAX(sent_count, 1)
            WHERE id = ?
        """, (email_perf_id,))
        db.commit()
    finally:
        db.close()


def create_ab_test(test_name: str, variant_a: str, variant_b: str) -> int:
    _ensure_learning_tables()
    db = _get_db()
    try:
        cursor = db.execute(
            "INSERT INTO ab_tests (test_name, variant_a, variant_b) VALUES (?, ?, ?)",
            (test_name, variant_a, variant_b),
        )
        db.commit()
        return cursor.lastrowid
    finally:
        db.close()


def record_ab_test_result(test_id: int, variant: str, is_reply: bool):
    db = _get_db()
    try:
        if variant == "a":
            db.execute("UPDATE ab_tests SET variant_a_sent = variant_a_sent + 1 WHERE id = ?", (test_id,))
            if is_reply:
                db.execute("UPDATE ab_tests SET variant_a_replies = variant_a_replies + 1 WHERE id = ?", (test_id,))
        else:
            db.execute("UPDATE ab_tests SET variant_b_sent = variant_b_sent + 1 WHERE id = ?", (test_id,))
            if is_reply:
                db.execute("UPDATE ab_tests SET variant_b_replies = variant_b_replies + 1 WHERE id = ?", (test_id,))

        test = db.execute("SELECT * FROM ab_tests WHERE id = ?", (test_id,)).fetchone()
        if test and test["variant_a_sent"] >= 20 and test["variant_b_sent"] >= 20:
            rate_a = test["variant_a_replies"] / max(test["variant_a_sent"], 1)
            rate_b = test["variant_b_replies"] / max(test["variant_b_sent"], 1)
            if rate_a != rate_b:  # ties keep the test running — no evidence yet
                winner = "a" if rate_a > rate_b else "b"
                db.execute(
                    "UPDATE ab_tests SET status = 'completed', winner = ?, ended_at = ? WHERE id = ?",
                    (winner, datetime.now(timezone.utc).isoformat(), test_id),
                )
        db.commit()
    finally:
        db.close()


def get_variant_weights() -> Dict[str, Dict[str, float]]:
    """Variant preference per A/B test, learned from actual replies.

    - completed test -> 100% winner (the learned outcome feeds every future draft)
    - running test with >= 10 sends and a reply-rate gap -> 80/20 lean toward
      the better variant (poor performers get deprioritized before the full
      20+20 threshold); equal rates or no data -> caller stays random
    """
    _ensure_learning_tables()
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT test_name, status, winner,
                      variant_a_sent, variant_b_sent,
                      variant_a_replies, variant_b_replies
               FROM ab_tests"""
        ).fetchall()
        out: Dict[str, Dict[str, float]] = {}
        for r in rows:
            name = r["test_name"]
            if r["status"] == "completed" and r["winner"] in ("a", "b"):
                out[name] = {"a": 1.0, "b": 0.0} if r["winner"] == "a" else {"a": 0.0, "b": 1.0}
                continue
            sent_a = r["variant_a_sent"] or 0
            sent_b = r["variant_b_sent"] or 0
            if max(sent_a, sent_b) < 10:
                continue
            rate_a = (r["variant_a_replies"] or 0) / max(sent_a, 1)
            rate_b = (r["variant_b_replies"] or 0) / max(sent_b, 1)
            if rate_a == rate_b:
                continue
            out[name] = {"a": 0.8, "b": 0.2} if rate_a > rate_b else {"a": 0.2, "b": 0.8}
        return out
    finally:
        db.close()


def record_timing(hour: int, day_of_week: int, is_reply: bool):
    _ensure_learning_tables()
    db = _get_db()
    try:
        existing = db.execute(
            "SELECT id FROM timing_optimization WHERE hour_of_day = ? AND day_of_week = ?",
            (hour, day_of_week),
        ).fetchone()

        if existing:
            db.execute(
                "UPDATE timing_optimization SET sent_count = sent_count + 1, updated_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), existing["id"]),
            )
            if is_reply:
                db.execute(
                    "UPDATE timing_optimization SET reply_count = reply_count + 1 WHERE id = ?",
                    (existing["id"],),
                )
            db.execute("""
                UPDATE timing_optimization
                SET reply_rate = CAST(reply_count AS REAL) / MAX(sent_count, 1)
                WHERE id = ?
            """, (existing["id"],))
        else:
            db.execute(
                """INSERT INTO timing_optimization
                   (hour_of_day, day_of_week, sent_count, reply_count)
                   VALUES (?, ?, 1, ?)""",
                (hour, day_of_week, 1 if is_reply else 0),
            )
        db.commit()
    finally:
        db.close()


def get_best_send_times() -> List[Dict[str, Any]]:
    _ensure_learning_tables()
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT hour_of_day, day_of_week, sent_count, reply_count, reply_rate
               FROM timing_optimization
               WHERE sent_count >= 3
               ORDER BY reply_rate DESC
               LIMIT 10"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def add_learning(category: str, insight: str, evidence: str = "", confidence: float = 0.5):
    _ensure_learning_tables()
    db = _get_db()
    try:
        db.execute(
            """INSERT INTO learnings (category, insight, evidence, confidence)
               VALUES (?, ?, ?, ?)""",
            (category, insight, evidence, confidence),
        )
        db.commit()
    finally:
        db.close()


def get_learnings(category: str = None, limit: int = 20) -> List[Dict[str, Any]]:
    _ensure_learning_tables()
    db = _get_db()
    try:
        if category:
            rows = db.execute(
                "SELECT * FROM learnings WHERE category = ? ORDER BY confidence DESC LIMIT ?",
                (category, limit),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM learnings ORDER BY confidence DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_performance_summary() -> Dict[str, Any]:
    _ensure_learning_tables()
    db = _get_db()
    try:
        total_sent = db.execute("SELECT SUM(sent_count) FROM email_performance").fetchone()[0] or 0
        total_replies = db.execute("SELECT SUM(reply_count) FROM email_performance").fetchone()[0] or 0
        total_meetings = db.execute("SELECT SUM(meeting_count) FROM email_performance").fetchone()[0] or 0
        avg_reply_rate = db.execute("SELECT AVG(reply_rate) FROM email_performance WHERE sent_count > 0").fetchone()[0] or 0

        top_templates = db.execute(
            """SELECT template_name, sent_count, reply_count, reply_rate
               FROM email_performance WHERE sent_count >= 3
               ORDER BY reply_rate DESC LIMIT 5"""
        ).fetchall()

        active_tests = db.execute(
            "SELECT * FROM ab_tests WHERE status = 'running'"
        ).fetchall()

        return {
            "total_sent": total_sent,
            "total_replies": total_replies,
            "total_meetings": total_meetings,
            "avg_reply_rate": round(avg_reply_rate * 100, 1),
            "top_templates": [dict(r) for r in top_templates],
            "active_ab_tests": len(active_tests),
        }
    finally:
        db.close()


if __name__ == "__main__":
    _ensure_learning_tables()
    add_learning("subject_line", "Questions in subject lines get 23% more opens", "A/B test #3", 0.8)
    add_learning("timing", "Tuesday 10am has highest reply rate for NL B2B", "Timing analysis", 0.7)
    learnings = get_learnings()
    print(f"Learnings: {len(learnings)}")
    for l in learnings:
        print(f"  [{l['category']}] {l['insight']} (confidence: {l['confidence']})")
