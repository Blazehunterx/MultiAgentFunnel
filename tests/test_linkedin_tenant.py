#!/usr/bin/env python3
"""Per-tenant LinkedIn session tests (offline: no browser, no network)."""

import json
import os
import sys
from datetime import datetime, timedelta

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import linkedin_engine as li


@pytest.fixture()
def li_env(tmp_path, monkeypatch):
    """Isolate linkedin_engine: temp DB, temp data dir, fresh flag stores."""
    db_path = tmp_path / "li.db"
    monkeypatch.setattr(li, "CLAWBUILDR_DB", str(db_path))
    monkeypatch.setattr(li, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(li, "_flag_state", {
        "flagged": False, "flag_type": None, "flagged_at": None,
        "cooldown_days": 7, "resumed": False,
    })
    monkeypatch.setattr(li, "_flag_states", {})
    monkeypatch.setattr(li, "_session_tenant", {"current": ""})
    db = li._get_clawbuildr_db()  # create schema (warming tables, outreach, ...)
    db.close()
    return str(db_path)


# ---------- A2: cookie storage ----------

def test_legacy_tenant_ids_share_original_cookie_file(li_env):
    for legacy in (None, "", "clawbuildr"):
        assert li.tenant_cookies_path(legacy) == li.COOKIES_PATH
        assert li._is_legacy_tenant(legacy) is True


def test_tenant_gets_own_cookie_file(li_env):
    p = li.tenant_cookies_path("t15")
    assert p != li.COOKIES_PATH
    assert p.endswith("linkedin_cookies_t15.json")
    assert li._is_legacy_tenant("t15") is False


def test_normalize_tenant_id(li_env):
    assert li._normalize_tenant_id(None) == ""
    assert li._normalize_tenant_id("clawbuildr") == ""
    assert li._normalize_tenant_id("t15") == "t15"


# ---------- A4: per-tenant safety state ----------

def test_save_result_scopes_tenant(li_env):
    li._save_result("c1", "Jan", "Jansen", "BV", "https://li/in/x", "note", "SUCCESS", tenant_id="t15")
    li._save_result("c2", "Piet", "De Vries", "BV", "https://li/in/y", "note", "SUCCESS")  # legacy
    import sqlite3
    db = sqlite3.connect(li_env)
    rows = dict(db.execute("SELECT contact_id, IFNULL(tenant_id,'') FROM linkedin_outreach").fetchall())
    db.close()
    assert rows == {"c1": "t15", "c2": ""}


def test_daily_count_scoped_per_tenant(li_env):
    li._save_result("c1", "A", "A", "X", "u1", "n", "SUCCESS", tenant_id="t15")
    li._save_result("c2", "B", "B", "X", "u2", "n", "SUCCESS", tenant_id="t15")
    li._save_result("c3", "C", "C", "X", "u3", "n", "SUCCESS")  # legacy
    assert li._get_daily_count("t15") == 2
    assert li._get_daily_count(None) == 1
    assert li._get_daily_count("t99") == 0


def test_warming_state_isolated_between_tenants(li_env):
    # Legacy account: created 60 days ago -> fully warmed
    created = (datetime.now() - timedelta(days=60)).isoformat()
    import sqlite3
    db = sqlite3.connect(li_env)
    db.execute("INSERT INTO warming_state (id, account_created_at) VALUES (1, ?)", (created,))
    db.commit()
    db.close()
    assert li._get_warming_multiplier(None) == 1.0
    # Fresh tenant: starts protected at 10%
    assert li._get_warming_multiplier("t15") == 0.10
    # And the rows are independent
    db = sqlite3.connect(li_env)
    legacy = db.execute("SELECT account_created_at FROM warming_state WHERE id=1").fetchone()
    tenant = db.execute("SELECT account_created_at FROM warming_state_tenant WHERE tenant_id='t15'").fetchone()
    db.close()
    assert legacy[0] == created
    assert tenant is not None


def test_flag_isolated_between_tenants(li_env):
    li._set_flagged("captcha", "test flag", tenant_id="t15")
    flagged, reason = li._is_flagged("t15")
    assert flagged is True and "captcha" in (reason or "")
    # Legacy + other tenants unaffected
    assert li._is_flagged(None)[0] is False
    assert li._is_flagged("t99")[0] is False


def test_flag_legacy_does_not_block_tenant(li_env):
    li._set_flagged("auth_wall", "legacy flag")
    assert li._is_flagged(None)[0] is True
    assert li._is_flagged("t15")[0] is False


def test_tenant_warming_row_created_on_demand(li_env):
    state = li._get_warming_state(tenant_id="t77")
    assert state["paused"] == 0
    assert state["account_created_at"] is not None
    import sqlite3
    db = sqlite3.connect(li_env)
    row = db.execute("SELECT tenant_id FROM warming_state_tenant WHERE tenant_id='t77'").fetchone()
    db.close()
    assert row == ("t77",)


# ---------- A4: can_send_connection (deterministic) ----------

@pytest.fixture()
def always_active(li_env, monkeypatch):
    monkeypatch.setattr(li, "_is_active_hours", lambda: (True, "active"))
    monkeypatch.setattr(li, "_connection_delay_remaining", lambda: 0.0)
    return li_env


def test_can_send_connection_tenant_flagged_blocks(always_active):
    li._set_flagged("captcha", "x", tenant_id="t15")
    ok, reason = li.can_send_connection(tenant_id="t15")
    assert ok is False
    assert reason.startswith("flagged_")
    # Legacy unaffected
    ok2, _ = li.can_send_connection(tenant_id=None)
    assert ok2 is True


def test_can_send_connection_tenant_daily_limit(always_active):
    # Fresh tenant -> effective limit = max(1, 20 * 10%) = 2
    li._save_result("c1", "A", "A", "X", "u1", "n", "SUCCESS", tenant_id="t15")
    li._save_result("c2", "B", "B", "X", "u2", "n", "SUCCESS", tenant_id="t15")
    ok, reason = li.can_send_connection(tenant_id="t15")
    assert ok is False
    assert "daily_limit_reached" in reason and "(2/2" in reason
    # Legacy still has budget (own count is 0, warmed up below)
    ok2, _ = li.can_send_connection(tenant_id=None)
    assert ok2 is True


# ---------- A3: QR login flow ----------

def test_qr_status_idle_for_fresh_tenant(li_env):
    st = li.linkedin_qr_status("t15")
    assert st["status"] == "idle"


def test_qr_status_rejects_legacy(li_env):
    assert li.linkedin_qr_status("clawbuildr")["status"] == "error"
    assert li.linkedin_qr_start("clawbuildr")["status"] == "error"


def test_qr_state_waiting_while_task_running(li_env):
    li._qr_set_state("t15", "waiting", "scan now")
    with li._qr_tasks_lock:
        li._qr_tasks["t15"] = True
    try:
        st = li.linkedin_qr_status("t15")
        assert st["status"] == "waiting"
    finally:
        with li._qr_tasks_lock:
            li._qr_tasks.pop("t15", None)


def test_qr_waiting_state_falls_back_to_idle_after_restart(li_env):
    # State file says waiting but no thread is running (dashboard restarted)
    li._qr_set_state("t15", "waiting", "scan now")
    st = li.linkedin_qr_status("t15")
    assert st["status"] == "idle"


def test_qr_start_spawns_and_marks_busy_then_completes(li_env, monkeypatch):
    # Replace the worker so no browser is launched
    started = {}

    def fake_worker(tid):
        started["tid"] = tid
        with li._qr_tasks_lock:
            li._qr_tasks[str(tid)] = False

    monkeypatch.setattr(li, "_qr_worker", fake_worker)
    res = li.linkedin_qr_start("t15")
    assert res["status"] == "started"
    # Thread races: wait briefly for the fake worker to run
    import time
    for _ in range(50):
        if started:
            break
        time.sleep(0.02)
    assert started.get("tid") == "t15"


def test_qr_image_path_none_until_captured(li_env):
    assert li.linkedin_qr_image_path("t15") is None
    img, _ = li._qr_paths("t15")
    os.makedirs(os.path.dirname(img), exist_ok=True)
    with open(img, "wb") as f:
        f.write(b"\x89PNG fake")
    assert li.linkedin_qr_image_path("t15") == img


def test_qr_cookie_save_targets_tenant_file(li_env):
    class FakeContext:
        def cookies(self, url):
            return [{
                "name": "li_at", "value": "tok", "domain": ".linkedin.com",
                "path": "/", "secure": True, "httpOnly": True,
                "sameSite": "Lax", "expires": 4102444800,
            }]

    li._save_cookies_from_context(FakeContext(), "t15")
    path = li.tenant_cookies_path("t15")
    assert os.path.exists(path)
    with open(path) as f:
        cookies = json.load(f)
    assert any(c["name"] == "li_at" for c in cookies)
    assert li.linkedin_login_status("t15")["logged_in"] is True
    # Tenant session lives in the tenant file, NOT the shared legacy file
    assert path != li.COOKIES_PATH
    assert os.path.dirname(path) == li.DATA_DIR  # DATA_DIR monkeypatched to tmp


# ---------- A4: sequence passes tenant_id through ----------

def test_sequence_connect_passes_tenant_id(li_env, tmp_path, monkeypatch):
    import clawbuildr_sequence_graph as sg
    db_path = tmp_path / "sg.db"
    monkeypatch.setattr(sg, "DB_PATH", str(db_path))
    import clawbuildr_watchdog as wd
    monkeypatch.setattr(wd, "can_send", lambda: True)
    sg.ensure_tables()

    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS contacts (
            contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT,
            company_id TEXT, current_stage TEXT, linkedin_url TEXT, research_result TEXT,
            lead_score REAL DEFAULT 0, updated_at TEXT, tenant_id TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS companies (
            company_id TEXT PRIMARY KEY, name TEXT, domain TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS linkedin_outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id TEXT, profile_url TEXT,
            connection_status TEXT, accepted_at TEXT, reply_body TEXT, outcome TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS tenant_config (
            tenant_id TEXT PRIMARY KEY, active INTEGER DEFAULT 1, display_name TEXT
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO contacts (contact_id, first_name, last_name, email, current_stage, linkedin_url) VALUES (?,?,?,?,?,?)",
        ("ct1", "Jan", "Jansen", "jan@x.nl", "INGESTED", "https://linkedin.com/in/jan"),
    )
    conn.execute("INSERT OR REPLACE INTO tenant_config (tenant_id, active, display_name) VALUES ('t15', 1, 'T15')")
    conn.commit()
    conn.close()

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return {"outcome": "SUCCESS"}

    monkeypatch.setattr(li, "search_and_connect", fake_connect)
    monkeypatch.setattr(li, "can_send_connection", lambda tenant_id=None: (True, "ok"))

    res = sg._default_linkedin_connect("ct1", {"note": "hi"}, {"tenant_id": "t15"})
    assert res == {"ok": True, "detail": "SUCCESS"}
    assert captured.get("tenant_id") == "t15"
    assert captured.get("profile_url") == "https://linkedin.com/in/jan"


def test_sequence_connect_legacy_stays_legacy(li_env, tmp_path, monkeypatch):
    import clawbuildr_sequence_graph as sg
    db_path = tmp_path / "sg2.db"
    monkeypatch.setattr(sg, "DB_PATH", str(db_path))
    import clawbuildr_watchdog as wd
    monkeypatch.setattr(wd, "can_send", lambda: True)
    sg.ensure_tables()

    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS contacts (
            contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT,
            company_id TEXT, current_stage TEXT, linkedin_url TEXT, research_result TEXT,
            lead_score REAL DEFAULT 0, updated_at TEXT, tenant_id TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS companies (
            company_id TEXT PRIMARY KEY, name TEXT, domain TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS linkedin_outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id TEXT, profile_url TEXT,
            connection_status TEXT, accepted_at TEXT, reply_body TEXT, outcome TEXT
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO contacts (contact_id, first_name, last_name, email, current_stage, linkedin_url) VALUES (?,?,?,?,?,?)",
        ("ct1", "Jan", "Jansen", "jan@x.nl", "INGESTED", "https://linkedin.com/in/jan"),
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return {"outcome": "SUCCESS"}

    monkeypatch.setattr(li, "search_and_connect", fake_connect)
    monkeypatch.setattr(li, "can_send_connection", lambda tenant_id=None: (True, "ok"))

    # Legacy tenant dict (clawbuildr) -> tenant_id normalized to legacy
    res = sg._default_linkedin_connect("ct1", {"note": "hi"}, {"tenant_id": "clawbuildr"})
    assert res == {"ok": True, "detail": "SUCCESS"}
    assert captured.get("tenant_id") == "clawbuildr"
    # linkedin_engine treats it as legacy internally
    assert li._is_legacy_tenant(captured.get("tenant_id")) is True
