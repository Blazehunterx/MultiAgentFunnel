"""Tests for the LinkedIn outreach rate limits in ``clawbuildr/linkedin_engine.py``.

The engine is the only thing standing between an automated outreach job and a
restricted LinkedIn account, so the limits it enforces are worth pinning down.

Three behaviours are covered:

* ``_get_period_count`` really counts the sends inside the current period. It
  previously used ``strftime('%%H', ...)``, which SQLite reads as a literal
  percent sign rather than the hour, so the cast produced 0 for every row and
  the per-period ceiling was never reached. All 20 daily connection requests
  could fire inside a single hour.
* ``can_send_connection`` refuses outside business hours and once a ceiling is
  hit.
* ``can_send_message`` applies a daily ceiling to messages sent to existing
  connections, which had no limit of its own at all.
"""
import importlib.util
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

# Loaded by file path: clawbuildr/ is not a package, and the dashboard imports
# these names by putting the directory on sys.path rather than importing a
# module. Going through the file directly keeps this a pure unit test.
ROOT = Path(__file__).resolve().parents[1]
ENGINE_PATH = ROOT / "clawbuildr" / "linkedin_engine.py"
_spec = importlib.util.spec_from_file_location("_linkedin_engine_under_test", ENGINE_PATH)
engine = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(engine)


TODAY = datetime.now().date().isoformat()


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """Point the engine at a throwaway database for the duration of a test."""
    path = tmp_path / "clawbuildr.db"
    monkeypatch.setattr(engine, "CLAWBUILDR_DB", str(path))
    engine._get_clawbuildr_db().close()  # creates the schema
    return str(path)


def _seed_connections(db_path, hour, count, outcome="SUCCESS"):
    conn = sqlite3.connect(db_path)
    for minute in range(count):
        conn.execute(
            "INSERT INTO linkedin_outreach (first_name, outcome, timestamp) VALUES (?, ?, ?)",
            ("Lead", outcome, f"{TODAY} {hour:02d}:{minute:02d}:00"),
        )
    conn.commit()
    conn.close()


def _seed_messages(db_path, count, hour=10):
    conn = sqlite3.connect(db_path)
    for minute in range(count):
        conn.execute(
            "INSERT INTO linkedin_followups (profile_url, outcome, timestamp) VALUES (?, ?, ?)",
            ("https://example.invalid/in/lead", "SUCCESS", f"{TODAY} {hour:02d}:{minute % 60:02d}:00"),
        )
    conn.commit()
    conn.close()


def test_period_count_sees_sends_in_the_current_period(db_path):
    """Regression: the hour comparison used to evaluate to 0 for every row."""
    _seed_connections(db_path, hour=9, count=5)

    count, limit, period = engine._get_period_count(now=datetime(2026, 1, 1, 9, 30))

    assert period == "morning"
    assert limit == engine.MORNING_LIMIT
    assert count == 5


def test_period_count_ignores_other_periods(db_path):
    _seed_connections(db_path, hour=9, count=4)   # morning
    _seed_connections(db_path, hour=14, count=3)  # afternoon

    morning, _, _ = engine._get_period_count(now=datetime(2026, 1, 1, 9, 30))
    afternoon, _, _ = engine._get_period_count(now=datetime(2026, 1, 1, 14, 30))

    assert (morning, afternoon) == (4, 3)


def test_period_count_ignores_failed_sends(db_path):
    _seed_connections(db_path, hour=9, count=3, outcome="FAILED")

    count, _, _ = engine._get_period_count(now=datetime(2026, 1, 1, 9, 30))

    assert count == 0


def test_connection_blocked_once_the_period_is_full(db_path):
    _seed_connections(db_path, hour=9, count=engine.MORNING_LIMIT)

    allowed, reason = engine.can_send_connection(now=datetime(2026, 1, 1, 9, 30))

    assert allowed is False
    assert reason == "morning_limit_reached"


def test_connection_allowed_below_the_period_ceiling(db_path):
    _seed_connections(db_path, hour=9, count=engine.MORNING_LIMIT - 1)

    allowed, reason = engine.can_send_connection(now=datetime(2026, 1, 1, 9, 30))

    assert allowed is True
    assert reason == "morning"


@pytest.mark.parametrize("hour", [0, 7, 21, 23])
def test_connection_blocked_outside_business_hours(db_path, hour):
    allowed, reason = engine.can_send_connection(now=datetime(2026, 1, 1, hour, 0))

    assert (allowed, reason) == (False, "off_hours")


def test_message_blocked_once_the_daily_ceiling_is_hit(db_path):
    _seed_messages(db_path, count=engine.DAILY_MESSAGE_LIMIT)

    allowed, reason = engine.can_send_message(now=datetime(2026, 1, 1, 10, 0))

    assert allowed is False
    assert reason == "daily_message_limit_reached"


def test_message_allowed_below_the_daily_ceiling(db_path):
    _seed_messages(db_path, count=engine.DAILY_MESSAGE_LIMIT - 1)

    allowed, reason = engine.can_send_message(now=datetime(2026, 1, 1, 10, 0))

    assert (allowed, reason) == (True, "ok")


@pytest.mark.parametrize("hour", [0, 7, 21, 23])
def test_message_blocked_outside_business_hours(db_path, hour):
    allowed, reason = engine.can_send_message(now=datetime(2026, 1, 1, hour, 0))

    assert (allowed, reason) == (False, "off_hours")
