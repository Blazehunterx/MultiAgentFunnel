#!/usr/bin/env python3
"""Unit tests for onboarding email connect + tenant-scoped send resolution (no live SMTP/IMAP)."""

import os
import smtplib
import sqlite3
import sys

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import clawbuildr_onboarding as ob
import tools as tl
import clawbuildr_reply_detector as rd


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "clawbuildr.db")
    monkeypatch.setattr(ob, "DB_PATH", db_path)
    monkeypatch.setattr(tl, "CLAWBUILDR_DB", db_path)
    monkeypatch.setattr(rd, "DB_PATH", db_path)
    # Deterministic: no .env fallback unless a test asks for it
    monkeypatch.setattr(rd, "_GMAIL_USER", "")
    monkeypatch.setattr(rd, "_GMAIL_PASSWORD", "")
    ob._ensure_onboarding_tables()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS tenant_config (
            tenant_id TEXT PRIMARY KEY, active INTEGER DEFAULT 0, display_name TEXT,
            sending_email TEXT DEFAULT '', sending_domain TEXT DEFAULT '')"""
    )
    conn.execute("CREATE TABLE IF NOT EXISTS campaigns (campaign_id TEXT PRIMARY KEY, account_id TEXT)")
    conn.execute("INSERT INTO tenant_config (tenant_id, active, display_name) VALUES ('alpha', 1, 'Alpha')")
    conn.execute("INSERT INTO tenant_config (tenant_id, active, display_name) VALUES ('beta', 0, 'Beta')")
    conn.commit()
    conn.close()
    return db_path


def _disable_verify(monkeypatch):
    monkeypatch.setattr(ob, "_verify_smtp", lambda *a, **k: None)
    monkeypatch.setattr(ob, "_verify_imap", lambda *a, **k: True)


def _rows(db_path, tenant=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    sql = "SELECT * FROM email_accounts"
    params = ()
    if tenant is not None:
        sql += " WHERE tenant_id = ?"
        params = (tenant,)
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def test_connect_creates_default_account_and_updates_tenant(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    result = ob.connect_email_account("alpha", "Alpha@Example.com", "app-pass-123")
    assert result["ok"] is True
    assert result["email"] == "alpha@example.com"
    assert result["was_update"] is False

    rows = _rows(tmp_db, "alpha")
    assert len(rows) == 1
    assert rows[0]["email_address"] == "alpha@example.com"
    assert rows[0]["is_default"] == 1
    assert rows[0]["smtp_user"] == "alpha@example.com"
    assert rows[0]["smtp_password"] == "app-pass-123"
    assert rows[0]["tenant_id"] == "alpha"

    conn = sqlite3.connect(tmp_db)
    t = conn.execute("SELECT sending_email, sending_domain FROM tenant_config WHERE tenant_id='alpha'").fetchone()
    conn.close()
    assert t[0] == "alpha@example.com"
    assert t[1] == "example.com"


def test_connect_upsert_demotes_previous_default_same_tenant(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    r1 = ob.connect_email_account("alpha", "one@example.com", "pw1")
    r2 = ob.connect_email_account("alpha", "two@example.com", "pw2")
    assert r1["ok"] and r2["ok"]

    rows = {r["email_address"]: r for r in _rows(tmp_db, "alpha")}
    assert len(rows) == 2
    assert rows["one@example.com"]["is_default"] == 0
    assert rows["two@example.com"]["is_default"] == 1

    # Reconnect first address: must update, not duplicate
    r3 = ob.connect_email_account("alpha", "one@example.com", "pw1-new")
    assert r3["ok"] and r3["was_update"] is True
    rows = {r["email_address"]: r for r in _rows(tmp_db, "alpha")}
    assert len(rows) == 2
    assert rows["one@example.com"]["smtp_password"] == "pw1-new"
    assert rows["one@example.com"]["is_default"] == 1
    assert rows["two@example.com"]["is_default"] == 0


def test_connect_refuses_mailbox_owned_by_other_tenant(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("alpha", "owned@example.com", "pw")["ok"] is True
    result = ob.connect_email_account("beta", "owned@example.com", "pw")
    assert result["ok"] is False
    assert result["stage"] == "input"
    rows = _rows(tmp_db, "alpha")
    assert len(rows) == 1 and rows[0]["tenant_id"] == "alpha"


def test_connect_rejects_invalid_input(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("alpha", "not-an-email", "pw")["ok"] is False
    assert ob.connect_email_account("alpha", "", "pw")["ok"] is False
    bad = ob.connect_email_account("alpha", "x@example.com", "")
    assert bad["ok"] is False and bad["stage"] == "input"
    assert _rows(tmp_db) == []


def test_connect_smtp_failure_is_not_stored(tmp_db, monkeypatch):
    def _fail(*a, **k):
        raise smtplib.SMTPAuthenticationError(535, b"5.7.8 Username and Password not accepted")

    monkeypatch.setattr(ob, "_verify_smtp", _fail)
    result = ob.connect_email_account("alpha", "x@example.com", "bad-pass")
    assert result["ok"] is False
    assert result["stage"] == "smtp"
    assert _rows(tmp_db) == []


def test_resolve_send_kwargs_scopes_to_active_tenant(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("alpha", "a@alpha.test", "pw")["ok"]
    assert ob.connect_email_account("beta", "b@beta.test", "pw")["ok"]

    picked = tl.resolve_send_kwargs()
    assert picked["email_address"] == "a@alpha.test"

    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE tenant_config SET active = 0 WHERE tenant_id = 'alpha'")
    conn.execute("UPDATE tenant_config SET active = 1 WHERE tenant_id = 'beta'")
    conn.commit()
    conn.close()

    picked = tl.resolve_send_kwargs()
    assert picked["email_address"] == "b@beta.test"


def test_resolve_send_kwargs_empty_when_tenant_has_no_accounts(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("beta", "b@beta.test", "pw")["ok"]
    # active tenant is alpha, which has no accounts -> no cross-tenant pick
    assert tl.resolve_send_kwargs() == {}


def test_resolve_send_kwargs_campaign_override_wins(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("alpha", "a@alpha.test", "pw")["ok"]
    beta = ob.connect_email_account("beta", "b@beta.test", "pw")
    conn = sqlite3.connect(tmp_db)
    conn.execute("INSERT INTO campaigns (campaign_id, account_id) VALUES ('c1', ?)", (beta["account_id"],))
    conn.commit()
    conn.close()
    picked = tl.resolve_send_kwargs(campaign_id="c1")
    assert picked["account_id"] == beta["account_id"]


def test_get_imap_accounts_scoping(tmp_db, monkeypatch):
    _disable_verify(monkeypatch)
    assert ob.connect_email_account("alpha", "a@alpha.test", "pw-a")["ok"]
    assert ob.connect_email_account("beta", "b@beta.test", "pw-b")["ok"]

    # No tenant → ALL connected mailboxes (reply detection must cover every
    # tenant regardless of which tenant is currently active).
    accounts = rd.get_imap_accounts()
    assert sorted(a["user"] for a in accounts) == ["a@alpha.test", "b@beta.test"]

    # Explicit tenant → only that tenant's mailbox.
    scoped = rd.get_imap_accounts("alpha")
    assert [a["user"] for a in scoped] == ["a@alpha.test"]
    assert scoped[0]["password"] == "pw-a"
    assert scoped[0]["host"] == "imap.gmail.com"


def test_get_imap_accounts_env_fallback(tmp_db, monkeypatch):
    assert rd.get_imap_accounts() == []
    monkeypatch.setattr(rd, "_GMAIL_USER", "env@fallback.test")
    monkeypatch.setattr(rd, "_GMAIL_PASSWORD", "env-pass")
    accounts = rd.get_imap_accounts()
    assert len(accounts) == 1
    assert accounts[0]["user"] == "env@fallback.test"
    assert accounts[0]["password"] == "env-pass"
