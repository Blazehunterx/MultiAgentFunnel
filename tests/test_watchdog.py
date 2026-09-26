#!/usr/bin/env python3
"""Unit tests for clawbuildr_watchdog (temp DB, no live SMTP)."""

import os
import sys
from datetime import datetime, timezone, timedelta

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import clawbuildr_watchdog as wd


@pytest.fixture()
def tmp_wd_db(tmp_path, monkeypatch):
    db_path = tmp_path / "wd.db"
    monkeypatch.setattr(wd, "DB_PATH", str(db_path))
    monkeypatch.setattr(wd, "DATA_DIR", str(tmp_path))
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE emails (
            email_id TEXT PRIMARY KEY,
            direction TEXT,
            status TEXT,
            subject TEXT,
            sent_at TEXT,
            created_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return str(db_path)


def _insert_email(db_path, direction="OUTBOUND", status="SENT", subject="Hi", sent_at=None, created_at=None):
    import sqlite3

    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO emails (email_id, direction, status, subject, sent_at, created_at) VALUES (?,?,?,?,?,?)",
        (
            f"e{conn.execute('SELECT COUNT(*) FROM emails').fetchone()[0]}",
            direction,
            status,
            subject,
            sent_at or now,
            created_at or now,
        ),
    )
    conn.commit()
    conn.close()


def test_can_send_empty_db(tmp_wd_db):
    assert wd.can_send() is True


def test_daily_limit_blocks(tmp_wd_db, monkeypatch):
    monkeypatch.setattr(wd, "DAILY_SEND_LIMIT", 2)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _insert_email(tmp_wd_db, sent_at=f"{today}T10:00:00+00:00")
    _insert_email(tmp_wd_db, sent_at=f"{today}T11:00:00+00:00")
    daily = wd.check_daily_limit()
    assert daily["can_send"] is False
    assert wd.can_send() is False


def test_hourly_limit_blocks(tmp_wd_db, monkeypatch):
    monkeypatch.setattr(wd, "HOURLY_SEND_LIMIT", 1)
    recent = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    _insert_email(tmp_wd_db, sent_at=recent)
    hourly = wd.check_hourly_limit()
    assert hourly["can_send"] is False
    assert wd.can_send() is False


def test_bounce_notification_excluded_from_rate(tmp_wd_db, monkeypatch):
    monkeypatch.setattr(wd, "BOUNCE_RATE_THRESHOLD", 5.0)
    # 1 real bounce + 9 notification rows → real rate 100%, notifications ignored
    _insert_email(tmp_wd_db, status="BOUNCED", subject="Hello")
    for _ in range(9):
        _insert_email(tmp_wd_db, status="BOUNCED", subject="Bounce notification")
    bounce = wd.check_bounce_rate()
    assert bounce["bounced"] == 1
    assert bounce["total_sent"] == 1
    assert bounce["is_healthy"] is False


def test_bounce_rate_healthy(tmp_wd_db, monkeypatch):
    # Threshold is strict <, so 50% bounce is unhealthy at threshold=50
    monkeypatch.setattr(wd, "BOUNCE_RATE_THRESHOLD", 75.0)
    _insert_email(tmp_wd_db, status="SENT")
    _insert_email(tmp_wd_db, status="BOUNCED")
    bounce = wd.check_bounce_rate()
    assert bounce["bounced"] == 1
    assert bounce["total_sent"] == 2
    assert bounce["is_healthy"] is True


def test_health_report_shape(tmp_wd_db):
    report = wd.get_health_report()
    assert report["status"] in ("healthy", "warning")
    assert "daily_limit" in report
    assert "hourly_limit" in report
    assert "bounce_rate" in report
    assert "issues" in report
