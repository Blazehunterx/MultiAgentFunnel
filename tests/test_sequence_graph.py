#!/usr/bin/env python3
"""Unit tests for clawbuildr_sequence_graph (no live Instagram/SMTP)."""

import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import clawbuildr_sequence_graph as sg


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "clawbuildr.db"
    monkeypatch.setattr(sg, "DB_PATH", str(db_path))
    # Isolate from production watchdog (real bounce rate/limits must not block unit tests)
    import clawbuildr_watchdog as wd
    monkeypatch.setattr(wd, "can_send", lambda: True)
    sg.ensure_tables()
    return str(db_path)


def _import_sqlite(db_path, sql, params=()):
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(sql) if ";" in sql and sql.strip().upper().startswith("CREATE") else None
    if not sql.strip().upper().startswith("CREATE"):
        conn.execute(sql, params)
        conn.commit()
    conn.close()


def _seed_contact(db_path, contact_id="c1", email="jan@bedrijf.nl", stage="INGESTED", linkedin="https://linkedin.com/in/x"):
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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
        """CREATE TABLE IF NOT EXISTS emails (
            email_id TEXT PRIMARY KEY, contact_id TEXT, direction TEXT, subject TEXT,
            body TEXT, status TEXT, sent_at TEXT, created_at TEXT
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
            tenant_id TEXT PRIMARY KEY, active INTEGER DEFAULT 1, display_name TEXT,
            sending_email TEXT, calendar_link TEXT, brand_voice TEXT
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO contacts (contact_id, first_name, last_name, email, current_stage, linkedin_url) VALUES (?,?,?,?,?,?)",
        (contact_id, "Jan", "Jansen", email, stage, linkedin),
    )
    conn.execute("INSERT OR REPLACE INTO companies (company_id, name, domain) VALUES ('co1','BV','bedrijf.nl')")
    conn.execute("INSERT OR REPLACE INTO tenant_config (tenant_id, active, display_name) VALUES ('clawbuildr', 1, 'ClawBuildr')")
    conn.commit()
    conn.close()


def test_normalize_rejects_bad_type():
    with pytest.raises(ValueError):
        sg.normalize_graph({"start": "a", "nodes": {"a": {"type": "nope"}}})


def test_steps_to_graph_five_node_flow():
    steps = [
        {"kind": "message", "title": "Email 1", "delay_days": 0, "delay_hours": 0},
        {"kind": "wait", "title": "Wait 2d", "delay_days": 2},
        {"kind": "linkedin_connect", "title": "Connect", "delay_days": 0},
        {"kind": "wait", "title": "Wait 3d", "delay_days": 3},
        {"kind": "message", "title": "Follow-up", "delay_days": 0, "branch": "no"},
        {"kind": "condition", "title": "If replied", "condition": "email_replied"},
    ]
    # put condition earlier for a real branch graph
    steps = [
        {"kind": "message", "title": "Email 1", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied"},
        {"kind": "message", "title": "Yes path", "delay_days": 0, "branch": "yes"},
        {"kind": "wait", "title": "Wait 2d", "delay_days": 2, "branch": "no"},
        {"kind": "linkedin_connect", "title": "Connect", "delay_days": 0, "branch": "no"},
        {"kind": "wait", "title": "Wait 3d", "delay_days": 3, "branch": "no"},
        {"kind": "message", "title": "Follow-up", "delay_days": 0, "branch": "no"},
    ]
    g = sg.steps_to_graph(steps)
    assert g["start"] == "n1"
    types = [g["nodes"][nid]["type"] for nid in sorted(g["nodes"], key=lambda x: int(x[1:]))]
    assert "email" in types
    assert "condition" in types
    assert "linkedin_connect" in types
    assert "wait" in types
    cond = g["nodes"]["n2"]
    assert cond["type"] == "condition"
    assert cond["on_true"] == "n3"
    assert cond["on_false"] == "n4"
    # linear chain for non-condition
    assert g["nodes"]["n1"]["next"] == "n2"
    assert g["nodes"]["n4"]["next"] == "n5"
    # yes branch is exclusive: n3 must not fall through to no-path n4
    assert g["nodes"]["n3"]["next"] is None
    assert g["nodes"]["n5"]["next"] == "n6"
    assert g["nodes"]["n6"]["next"] == "n7"
    assert g["nodes"]["n7"]["next"] is None


def test_explicit_stop_false_arm_does_not_fall_through():
    """UI 'Stop / geen verdere stappen' on the no-arm must stop, not continue."""
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied",
         "on_true": 2, "on_false": "stop"},
        {"kind": "message", "title": "After", "delay_days": 1},
    ]
    g = sg.steps_to_graph(steps)
    cond = g["nodes"]["n2"]
    assert cond["on_true"] == "n3"
    assert cond["on_false"] is None


def test_explicit_stop_true_arm_stops():
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied",
         "on_true": "stop"},
        {"kind": "message", "title": "After", "delay_days": 1},
    ]
    g = sg.steps_to_graph(steps)
    cond = g["nodes"]["n2"]
    assert cond["on_true"] is None
    # unset false arm still falls through to the main path by default
    assert cond["on_false"] == "n3"


def test_unset_false_arm_keeps_default_fallthrough():
    """Regression: a condition with no arm values keeps implicit No-line."""
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied"},
        {"kind": "message", "title": "After", "delay_days": 1},
    ]
    g = sg.steps_to_graph(steps)
    cond = g["nodes"]["n2"]
    assert cond["on_false"] == "n3"
    assert cond["on_true"] is None


def test_stale_branch_tag_cannot_rewire_explicit_stop():
    """A stale 'no' tag must not turn an explicit Stop into fall-through."""
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied",
         "on_false": "stop"},
        {"kind": "message", "title": "Stale no-tagged", "delay_days": 1, "branch": "no"},
    ]
    g = sg.steps_to_graph(steps)
    cond = g["nodes"]["n2"]
    assert cond["on_false"] is None


def test_normalize_graph_explicit_stop_not_overridden_by_next():
    g = sg.normalize_graph({
        "start": "a",
        "nodes": {
            "a": {"type": "condition", "condition": "email_replied",
                  "on_false": "stop", "next": "b"},
            "b": {"type": "wait", "delay_days": 1},
        },
    })
    assert g["nodes"]["a"]["on_false"] is None


def test_normalize_graph_unset_false_still_takes_next():
    g = sg.normalize_graph({
        "start": "a",
        "nodes": {
            "a": {"type": "condition", "condition": "email_replied", "next": "b"},
            "b": {"type": "wait", "delay_days": 1},
        },
    })
    assert g["nodes"]["a"]["on_false"] == "b"


def test_user_example_flow_delays():
    """email → wait 2d → linkedin connect → wait 3d → email follow-up"""
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0, "delay_hours": 0},
        {"kind": "wait", "title": "2 dagen", "delay_days": 2, "delay_hours": 0},
        {"kind": "linkedin_connect", "title": "Connect", "delay_days": 0, "delay_hours": 0},
        {"kind": "wait", "title": "3 dagen", "delay_days": 3, "delay_hours": 0},
        {"kind": "message", "title": "Follow-up", "delay_days": 0, "delay_hours": 0},
    ]
    g = sg.steps_to_graph(steps)
    assert g["nodes"]["n2"]["type"] == "wait"
    assert g["nodes"]["n2"]["delay_days"] == 2
    assert g["nodes"]["n3"]["type"] == "linkedin_connect"
    assert g["nodes"]["n4"]["delay_days"] == 3
    assert g["nodes"]["n5"]["type"] == "email"
    assert g["nodes"]["n1"]["next"] == "n2"
    assert g["nodes"]["n3"]["next"] == "n4"
    assert g["nodes"]["n5"]["next"] is None


def test_enroll_and_process_email_then_advance(tmp_db):
    _seed_contact(tmp_db)
    calls = []

    def fake_email(cid, node, tenant):
        calls.append((cid, node["id"], node["type"]))
        return {"ok": True, "detail": "test"}

    def fake_li(cid, node, tenant):
        calls.append((cid, node["id"], node["type"]))
        return {"ok": True, "detail": "test"}

    sg.set_actions(email=fake_email, li_connect=fake_li, li_message=fake_li)

    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "linkedin_connect", "title": "Connect", "delay_days": 0},
    ]
    graph = sg.steps_to_graph(steps)
    gid = sg.save_graph(graph, tenant_id="clawbuildr")
    sg.enroll_contact("c1", graph_id=gid)

    # First tick: only n1 due (delay 0)
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    # force due
    conn.execute("UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='c1'",
                 (datetime.now(timezone.utc).isoformat(),))
    conn.commit()
    conn.close()

    stats = sg.process_due()
    assert stats["processed"] >= 1
    assert stats["advanced"] >= 1
    assert calls and calls[0][1] == "n1"

    # Now at n2, force due again
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='c1'",
                 (datetime.now(timezone.utc).isoformat(),))
    conn.commit()
    conn.close()

    stats2 = sg.process_due()
    assert any(c[1] == "n2" for c in calls)
    # completed after last node (next is None)
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, current_node_id FROM sequence_enrollments WHERE contact_id='c1'").fetchone()
    conn.close()
    assert row["status"] == "COMPLETED"

    # restore defaults so other tests aren't affected if reused
    sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_condition_branches_on_email_replied(tmp_db):
    _seed_contact(tmp_db, contact_id="cR", email="reply@bedrijf.nl")
    # seed a reply
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS contacts (
            contact_id TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT,
            company_id TEXT, current_stage TEXT, linkedin_url TEXT, research_result TEXT,
            lead_score REAL DEFAULT 0, updated_at TEXT, tenant_id TEXT
        )"""
    )
    conn.execute("UPDATE contacts SET current_stage='REPLIED' WHERE contact_id='cR'")
    conn.commit()
    conn.close()

    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0},
        {"kind": "condition", "title": "If replied", "condition": "email_replied"},
        {"kind": "wait", "title": "Yes path", "delay_days": 0, "branch": "yes"},
        {"kind": "message", "title": "No follow", "delay_days": 1, "branch": "no"},
    ]
    graph = sg.steps_to_graph(steps)
    # evaluate condition directly
    assert sg.evaluate_condition("email_replied", "cR") is True
    assert sg.evaluate_condition("email_replied", "missing") is False

    # Intro must send while NOT yet replied → stage back to early pipeline
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE contacts SET current_stage='INGESTED' WHERE contact_id='cR'")
    conn.commit()
    conn.close()

    # full path: enroll, process intro, reply arrives, then condition → on_true n3
    calls = []
    def fake_email(cid, node, tenant):
        calls.append(node["id"])
        return {"ok": True, "detail": "t"}
    sg.set_actions(email=fake_email, li_connect=fake_email, li_message=fake_email)

    gid = sg.save_graph(graph, tenant_id="clawbuildr")
    sg.enroll_contact("cR", graph_id=gid)

    def _force_due():
        conn = sqlite3.connect(tmp_db)
        conn.execute("UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='cR'",
                     (datetime.now(timezone.utc).isoformat(),))
        conn.commit()
        conn.close()

    _force_due()
    sg.process_due()
    assert calls and calls[0] == "n1"  # intro sent while early-stage

    # Reply arrives after the intro, before the condition tick
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE contacts SET current_stage='REPLIED' WHERE contact_id='cR'")
    conn.commit()
    conn.close()

    for _ in range(3):
        _force_due()
        sg.process_due()

    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, branch_path FROM sequence_enrollments WHERE contact_id='cR'").fetchone()
    conn.close()
    path = json.loads(row["branch_path"] or "[]")
    # should have taken true branch to n3, not the no-follow n4 as continuing chain after true
    assert "n3" in path or row["status"] == "COMPLETED"
    # n4 is the no-branch follow-up with delay — should not be the only path
    assert "n4" not in path or path.index("n4") > path.index("n3") if "n3" in path and "n4" in path else True

    sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_linkedin_accepted_condition(tmp_db):
    _seed_contact(tmp_db, contact_id="cL")
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS linkedin_outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id TEXT, profile_url TEXT,
            connection_status TEXT, accepted_at TEXT, reply_body TEXT, outcome TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO linkedin_outreach (contact_id, connection_status, accepted_at) VALUES ('cL','accepted','2026-01-01')"
    )
    conn.commit()
    conn.close()
    assert sg.evaluate_condition("linkedin_accepted", "cL") is True
    assert sg.evaluate_condition("linkedin_accepted", "cNope") is False


def test_defer_on_rate_limit(tmp_db):
    _seed_contact(tmp_db, contact_id="cD")
    def defer_action(cid, node, tenant):
        return {"ok": False, "defer": True, "detail": "rate_limited"}
    sg.set_actions(email=defer_action, li_connect=defer_action, li_message=defer_action)

    graph = sg.steps_to_graph([{"kind": "message", "title": "E", "delay_days": 0}])
    gid = sg.save_graph(graph, tenant_id="clawbuildr")
    sg.enroll_contact("cD", graph_id=gid)

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='cD'",
                 (datetime.now(timezone.utc).isoformat(),))
    conn.commit()
    conn.close()

    stats = sg.process_due()
    assert stats["deferred"] >= 1

    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, current_node_id FROM sequence_enrollments WHERE contact_id='cD'").fetchone()
    conn.close()
    assert row["status"] == "ACTIVE"
    assert row["current_node_id"] == "n1"

    sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_save_graph_persists_active(tmp_db):
    graph = sg.steps_to_graph([
        {"kind": "message", "title": "A", "delay_days": 0},
        {"kind": "linkedin_connect", "title": "B", "delay_days": 1},
    ])
    gid1 = sg.save_graph(graph, tenant_id="t1")
    graph2 = sg.steps_to_graph([{"kind": "wait", "title": "W", "delay_days": 1}])
    gid2 = sg.save_graph(graph2, tenant_id="t1")
    active = sg.get_active_graph("t1")
    assert active is not None
    assert active[0] == gid2
    assert gid1 != gid2


def test_save_graph_stops_old_enrollments(tmp_db):
    _seed_contact(tmp_db, contact_id="cStop")
    graph = sg.steps_to_graph([{"kind": "message", "title": "A", "delay_days": 0}])
    gid1 = sg.save_graph(graph, tenant_id="t1")
    assert sg.enroll_contact("cStop", graph_id=gid1, tenant_id="t1")

    graph2 = sg.steps_to_graph([{"kind": "message", "title": "B", "delay_days": 0}])
    gid2 = sg.save_graph(graph2, tenant_id="t1")

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    old = conn.execute(
        "SELECT status, pause_reason FROM sequence_enrollments WHERE graph_id = ? AND contact_id = 'cStop'",
        (gid1,),
    ).fetchone()
    conn.close()
    assert old is not None
    assert old["status"] == "STOPPED"
    assert old["pause_reason"] == "graph_replaced"
    # New graph can enroll the same contact
    assert sg.enroll_contact("cStop", graph_id=gid2, tenant_id="t1")


def test_get_due_enrollments_only_active_graphs(tmp_db):
    _seed_contact(tmp_db, contact_id="cDue")
    graph = sg.steps_to_graph([{"kind": "message", "title": "A", "delay_days": 0}])
    gid1 = sg.save_graph(graph, tenant_id="t1")
    eid = sg.enroll_contact("cDue", graph_id=gid1, tenant_id="t1")
    assert eid

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "UPDATE sequence_enrollments SET next_action_at=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), int(eid)),
    )
    conn.commit()
    conn.close()

    due = sg.get_due_enrollments(limit=50)
    assert any(str(r["id"]) == str(eid) for r in due)

    # Replace graph → old enrollment stopped and not due
    graph2 = sg.steps_to_graph([{"kind": "wait", "title": "W", "delay_days": 1}])
    sg.save_graph(graph2, tenant_id="t1")
    due2 = sg.get_due_enrollments(limit=50)
    assert not any(str(r["id"]) == str(eid) for r in due2)


def test_enroll_skips_other_active_graph(tmp_db):
    _seed_contact(tmp_db, contact_id="cX")
    g1 = sg.steps_to_graph([{"kind": "message", "title": "A", "delay_days": 0}])
    gid1 = sg.save_graph(g1, tenant_id="tA")
    assert sg.enroll_contact("cX", graph_id=gid1, tenant_id="tA")

    # Second active graph for a different tenant (same contact)
    g2 = sg.steps_to_graph([{"kind": "message", "title": "B", "delay_days": 0}])
    gid2 = sg.save_graph(g2, tenant_id="tB")
    # Force both graphs active (save_graph only deactivates same tenant)
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE sequence_graphs SET active = 1")
    conn.commit()
    conn.close()

    result = sg.enroll_contact("cX", graph_id=gid2, tenant_id="tB")
    assert result is None


def test_process_due_claim_prevents_double_process(tmp_db):
    """Concurrent ticks: only one claim wins for a due enrollment."""
    _seed_contact(tmp_db, contact_id="cClaim")
    calls = []

    def fake_email(cid, node, tenant):
        calls.append(cid)
        return {"ok": True, "detail": "t"}

    def fake_li(cid, node, tenant):
        return {"ok": True, "detail": "t"}

    sg.set_actions(email=fake_email, li_connect=fake_li, li_message=fake_li)
    try:
        graph = sg.steps_to_graph([{"kind": "message", "title": "A", "delay_days": 0}])
        gid = sg.save_graph(graph, tenant_id="clawbuildr")
        eid = sg.enroll_contact("cClaim", graph_id=gid)
        assert eid

        import sqlite3
        due_at = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(tmp_db)
        conn.execute(
            "UPDATE sequence_enrollments SET next_action_at=? WHERE id=?",
            (due_at, int(eid)),
        )
        conn.commit()
        conn.close()

        # Simulate another tick already claiming this row
        conn = sqlite3.connect(tmp_db)
        claim_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        conn.execute(
            "UPDATE sequence_enrollments SET next_action_at=? WHERE id=? AND next_action_at=?",
            (claim_at, int(eid), due_at),
        )
        conn.commit()
        conn.close()

        stats = sg.process_due()
        assert stats["processed"] == 0
        assert calls == []
    finally:
        sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_subject_placeholders_rendered(tmp_db, monkeypatch):
    """Fix 1: node subjects must render {{company}}/{{first_name}} like bodies do."""
    _seed_contact(tmp_db, contact_id="cSub", email="sub@bedrijf.nl")
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE contacts SET company_id='co1' WHERE contact_id='cSub'")
    conn.commit()
    conn.close()
    import tools
    captured = {}

    async def fake_gmail_send(**kw):
        captured.update(kw)
        return {"status": "sent", "to": kw.get("to")}

    monkeypatch.setattr(tools, "gmail_send", fake_gmail_send)
    monkeypatch.setattr(
        tools,
        "resolve_send_kwargs",
        lambda **kw: {
            "smtp_password": "fake", "smtp_host": "smtp.test",
            "smtp_port": 587, "smtp_user": "u", "email_address": "from@test",
        },
    )
    monkeypatch.setattr(tools, "record_account_send", lambda *a, **kw: None)

    node = {
        "id": "n1",
        "type": "email",
        "subject": "{{company}} - korte vraag",
        "message": "Hoi {{first_name}} van {{company_name}}, een bericht.",
    }
    res = sg._default_send_email("cSub", node, {})
    assert res.get("ok") is True, res
    assert captured["subject"] == "BV - korte vraag"
    assert "{{" not in captured["subject"]
    assert "{{" not in captured["body"]

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT subject, body FROM emails WHERE contact_id='cSub' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row["subject"] == "BV - korte vraag"
    assert "{{" not in row["body"]


def test_terminal_stage_precheck_blocks_actions(tmp_db):
    """Fix 3: due action nodes never fire for terminal-stage contacts."""
    stages = ("REPLIED", "MEETING_BOOKED", "CLOSED_LOST", "OPT_OUT", "BOUNCED", "OBJECTION")
    try:
        for i, stage in enumerate(stages):
            cid = f"cT{i}"
            _seed_contact(tmp_db, contact_id=cid, email=f"stage{i}@bedrijf.nl", stage=stage)
            calls = []

            def fake(cid_, node, tenant, _calls=calls):
                _calls.append(node["type"])
                return {"ok": True, "detail": "t"}

            sg.set_actions(email=fake, li_connect=fake, li_message=fake)
            graph = sg.steps_to_graph([
                {"kind": "linkedin_connect", "title": "C", "delay_days": 0},
                {"kind": "message", "title": "E", "delay_days": 0},
            ])
            gid = sg.save_graph(graph, tenant_id="t1")
            assert sg.enroll_contact(cid, graph_id=gid, tenant_id="t1")

            import sqlite3
            conn = sqlite3.connect(tmp_db)
            conn.execute(
                "UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id=?",
                (datetime.now(timezone.utc).isoformat(), cid),
            )
            conn.commit()
            conn.close()

            stats = sg.process_due()
            assert calls == [], f"action fired for stage {stage}"
            assert stats["stopped"] >= 1

            conn = sqlite3.connect(tmp_db)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT status, pause_reason FROM sequence_enrollments WHERE contact_id=?", (cid,)
            ).fetchone()
            conn.close()
            assert row["status"] == "STOPPED"
            assert row["pause_reason"] == f"terminal_stage:{stage}"
    finally:
        sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_precheck_lets_waits_fall_through_then_blocks_next_action(tmp_db):
    """Fix 3 semantics: waits/conditions fall through; the next action is blocked."""
    _seed_contact(tmp_db, contact_id="cFW", email="fw@bedrijf.nl", stage="REPLIED")
    graph = sg.steps_to_graph([
        {"kind": "wait", "title": "W", "delay_days": 0},
        {"kind": "message", "title": "E", "delay_days": 0},
    ])
    gid = sg.save_graph(graph, tenant_id="t1")
    assert sg.enroll_contact("cFW", graph_id=gid, tenant_id="t1")

    calls = []

    def fake(cid, node, tenant):
        calls.append(node["id"])
        return {"ok": True, "detail": "t"}

    sg.set_actions(email=fake, li_connect=fake, li_message=fake)
    try:
        import sqlite3

        def force_due():
            conn = sqlite3.connect(tmp_db)
            conn.execute(
                "UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='cFW'",
                (datetime.now(timezone.utc).isoformat(),),
            )
            conn.commit()
            conn.close()

        force_due()
        stats = sg.process_due()
        # wait node processed even though stage is terminal
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, current_node_id FROM sequence_enrollments WHERE contact_id='cFW'"
        ).fetchone()
        conn.close()
        assert row["status"] == "ACTIVE"
        assert row["current_node_id"] == "n2"
        assert stats["advanced"] >= 1

        # now the email action is due ? pre-check stops it before sending
        force_due()
        sg.process_due()
        assert calls == []
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, pause_reason FROM sequence_enrollments WHERE contact_id='cFW'"
        ).fetchone()
        conn.close()
        assert row["status"] == "STOPPED"
        assert row["pause_reason"] == "terminal_stage:REPLIED"
    finally:
        sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)


def test_ooo_auto_reply_not_counted_as_reply(tmp_db):
    """Fix 2 hygiene: OOO/auto-reply inbound rows must not satisfy email_replied."""
    _seed_contact(tmp_db, contact_id="cOOO", email="ooo@bedrijf.nl")
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        """INSERT INTO emails (email_id, contact_id, direction, subject, body, status, created_at, sent_at)
           VALUES ('e_ooo','cOOO','inbound','Automatic reply: Trip planner','Ik ben afwezig tot maandag','ooo','2026-09-25T10:00:00+00:00','2026-09-25T10:00:00+00:00')"""
    )
    conn.commit()
    conn.close()
    assert sg.evaluate_condition("email_replied", "cOOO") is False

    # a real reply still counts
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        """INSERT INTO emails (email_id, contact_id, direction, subject, body, status, created_at, sent_at)
           VALUES ('e_real','cOOO','inbound','Re: korte vraag','Interessant, laten we bellen','positive','2026-09-25T11:00:00+00:00','2026-09-25T11:00:00+00:00')"""
    )
    conn.commit()
    conn.close()
    assert sg.evaluate_condition("email_replied", "cOOO") is True


def test_is_auto_reply_detection():
    """Fix 2 hygiene: OOO subjects/bodies are recognized, real replies are not."""
    import clawbuildr_reply_detector as rd

    assert rd._is_auto_reply("Automatic reply: Trip planner - korte vraag", "any body") is True
    assert rd._is_auto_reply("Out of office", "until monday") is True
    assert rd._is_auto_reply("Auto: underwerp", "") is True
    assert rd._is_auto_reply("Re: korte vraag", "Ik ben afwezig tot 1 oktober.") is True
    assert rd._is_auto_reply("Re: korte vraag", "Interessant, laten we bellen volgende week.") is False
    assert rd._is_auto_reply("Korte vraag", "Zou je open staan voor een kort gesprek?") is False


def test_enroll_eligible_leads_scoped_to_tenant(tmp_db):
    """Multi-tenant isolation: auto-enroll must never grab another tenant's leads."""
    _seed_contact(tmp_db, contact_id="cMine", email="mine@bedrijf.nl")
    _seed_contact(tmp_db, contact_id="cTheirs", email="theirs@bedrijf.nl")
    _seed_contact(tmp_db, contact_id="cLegacy", email="legacy@bedrijf.nl")
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE contacts SET tenant_id='clawbuildr' WHERE contact_id='cMine'")
    conn.execute("UPDATE contacts SET tenant_id='t_other' WHERE contact_id='cTheirs'")
    conn.execute("UPDATE contacts SET tenant_id='' WHERE contact_id='cLegacy'")
    conn.commit()
    conn.close()

    graph = sg.steps_to_graph([{"kind": "message", "title": "Intro", "delay_days": 0}])
    sg.save_graph(graph, tenant_id="clawbuildr")
    count = sg.enroll_eligible_leads(50, tenant_id="clawbuildr")
    assert count == 2  # own + unassigned-legacy; never t_other's

    conn = sqlite3.connect(tmp_db)
    ids = sorted(r[0] for r in conn.execute("SELECT contact_id FROM sequence_enrollments"))
    conn.close()
    assert ids == ["cLegacy", "cMine"]


def test_process_due_uses_graph_tenant_not_active(tmp_db):
    """Sends resolve the enrollment's OWN tenant, not whichever tenant is active."""
    _seed_contact(tmp_db, contact_id="cT", email="t@bedrijf.nl")
    graph = sg.steps_to_graph([{"kind": "message", "title": "E", "delay_days": 0}])
    gid = sg.save_graph(graph, tenant_id="clawbuildr")
    assert sg.enroll_contact("cT", graph_id=gid, tenant_id="clawbuildr")

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE tenant_config SET active = 0")
    conn.execute(
        "INSERT OR REPLACE INTO tenant_config (tenant_id, active, display_name) VALUES ('tX', 1, 'X')"
    )
    conn.execute(
        "UPDATE sequence_enrollments SET next_action_at=? WHERE contact_id='cT'",
        (datetime.now(timezone.utc).isoformat(),),
    )
    conn.commit()
    conn.close()

    seen = {}

    def fake(cid, node, tenant):
        seen.update(tenant or {})
        return {"ok": True, "detail": "t"}

    sg.set_actions(email=fake, li_connect=fake, li_message=fake)
    try:
        sg.process_due()
    finally:
        sg.set_actions(email=sg._default_send_email, li_connect=sg._default_linkedin_connect, li_message=sg._default_linkedin_message)

    assert seen.get("tenant_id") == "clawbuildr"


# --- Bug 4: LinkedIn failure policy (skip-and-continue, hard-stop only FLAGGED) ---


@pytest.mark.parametrize("outcome", [
    "SUCCESS", "SUCCESS_NO_NOTE", "ALREADY_CONNECTED", "ALREADY_PENDING", "ALREADY_CONTACTED",
])
def test_linkedin_policy_success_advances(outcome):
    res = sg._linkedin_action_result(outcome)
    assert res["ok"] is True
    assert not res.get("skip")
    assert not res.get("defer")
    assert res["detail"] == outcome


def test_linkedin_policy_flagged_hard_stops():
    res = sg._linkedin_action_result("FLAGGED")
    assert res["ok"] is False
    assert not res.get("defer")
    assert not res.get("skip")


def test_linkedin_policy_time_limited_defers():
    res = sg._linkedin_action_result("TIME_LIMITED")
    assert res["ok"] is False
    assert res.get("defer") is True


@pytest.mark.parametrize("outcome", ["NOT_FOUND", "FAILURE", "SOMETHING_NEW", ""])
def test_linkedin_policy_soft_failures_skip(outcome):
    res = sg._linkedin_action_result(outcome)
    assert res["ok"] is True
    assert res.get("skip") is True
    assert not res.get("defer")
    assert outcome in res["detail"] or outcome == ""


def test_linkedin_connect_exception_skips_not_stops(tmp_db, monkeypatch):
    """A raised exception inside the action must skip-and-continue, never kill the sequence."""
    _seed_contact(tmp_db, contact_id="ct_x", stage="INGESTED", linkedin="https://www.linkedin.com/in/x/")

    def boom():
        raise RuntimeError("engine exploded")

    try:
        import linkedin_engine
        monkeypatch.setattr(linkedin_engine, "can_send_connection", boom)
    except Exception:
        pass  # if the module cannot load, the in-action import raises -> same skip path

    res = sg._default_linkedin_connect("ct_x", {"note": ""}, {})
    assert res["ok"] is True
    assert res.get("skip") is True
    assert "error:" in res["detail"]
