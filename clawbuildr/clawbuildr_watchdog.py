#!/usr/bin/env python3
"""
ClawBuildr Watchdog — Email quality watchdog.
Monitors sending patterns, detects issues, and enforces rate limits.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

logger = logging.getLogger("ClawBuildr.Watchdog")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

DAILY_SEND_LIMIT = 100
HOURLY_SEND_LIMIT = 20
BOUNCE_RATE_THRESHOLD = 5.0
REPLY_RATE_MINIMUM = 1.0


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn


def check_daily_limit() -> Dict[str, Any]:
    db = _get_db()
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sent = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'outbound' AND status = 'SENT' AND date(sent_at) = ?",
            (today,),
        ).fetchone()["cnt"]

        return {
            "sent_today": sent,
            "limit": DAILY_SEND_LIMIT,
            "remaining": max(0, DAILY_SEND_LIMIT - sent),
            "can_send": sent < DAILY_SEND_LIMIT,
        }
    finally:
        db.close()


def check_hourly_limit() -> Dict[str, Any]:
    db = _get_db()
    try:
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        sent = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'outbound' AND status = 'SENT' AND sent_at > ?",
            (one_hour_ago,),
        ).fetchone()["cnt"]

        return {
            "sent_last_hour": sent,
            "limit": HOURLY_SEND_LIMIT,
            "remaining": max(0, HOURLY_SEND_LIMIT - sent),
            "can_send": sent < HOURLY_SEND_LIMIT,
        }
    finally:
        db.close()


def check_bounce_rate() -> Dict[str, Any]:
    db = _get_db()
    try:
        total = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'outbound'"
        ).fetchone()["cnt"]
        bounced = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'outbound' AND status = 'bounced'"
        ).fetchone()["cnt"]

        rate = (bounced / max(total, 1)) * 100

        return {
            "total_sent": total,
            "bounced": bounced,
            "bounce_rate": round(rate, 2),
            "threshold": BOUNCE_RATE_THRESHOLD,
            "is_healthy": rate < BOUNCE_RATE_THRESHOLD,
        }
    finally:
        db.close()


def check_reply_rate() -> Dict[str, Any]:
    db = _get_db()
    try:
        sent = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'outbound'"
        ).fetchone()["cnt"]
        received = db.execute(
            "SELECT COUNT(*) as cnt FROM emails WHERE direction = 'inbound'"
        ).fetchone()["cnt"]

        rate = (received / max(sent, 1)) * 100

        return {
            "sent": sent,
            "received": received,
            "reply_rate": round(rate, 2),
            "minimum": REPLY_RATE_MINIMUM,
            "is_healthy": rate >= REPLY_RATE_MINIMUM,
        }
    finally:
        db.close()


def get_health_report() -> Dict[str, Any]:
    daily = check_daily_limit()
    hourly = check_hourly_limit()
    bounce = check_bounce_rate()
    reply = check_reply_rate()

    issues = []
    if not daily["can_send"]:
        issues.append("Daily send limit reached")
    if not hourly["can_send"]:
        issues.append("Hourly send limit reached")
    if not bounce["is_healthy"]:
        issues.append(f"Bounce rate too high: {bounce['bounce_rate']}%")
    if not reply["is_healthy"] and bounce["total_sent"] > 20:
        issues.append(f"Reply rate too low: {reply['reply_rate']}%")

    return {
        "status": "healthy" if not issues else "warning",
        "issues": issues,
        "daily_limit": daily,
        "hourly_limit": hourly,
        "bounce_rate": bounce,
        "reply_rate": reply,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def can_send() -> bool:
    daily = check_daily_limit()
    hourly = check_hourly_limit()
    bounce = check_bounce_rate()
    return daily["can_send"] and hourly["can_send"] and bounce["is_healthy"]


if __name__ == "__main__":
    report = get_health_report()
    print(json.dumps(report, indent=2))
