#!/usr/bin/env python3
"""Tests for product-gap fixes: sequence edit round-trip + A/B learning loop."""

import json
import os
import sqlite3
import sys

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import clawbuildr_sequence_graph as sg
import clawbuildr_learning as learn


# ---------------------------------------------------------------- fixtures

@pytest.fixture()
def sg_db(tmp_path, monkeypatch):
    db_path = tmp_path / "seq.db"
    monkeypatch.setattr(sg, "DB_PATH", str(db_path))
    import clawbuildr_watchdog as wd
    monkeypatch.setattr(wd, "can_send", lambda: True)
    sg.ensure_tables()
    return str(db_path)


@pytest.fixture()
def learn_db(tmp_path, monkeypatch):
    db_path = tmp_path / "learning.db"
    monkeypatch.setattr(learn, "DB_PATH", str(db_path))
    learn._ensure_learning_tables()
    return str(db_path)


def _insert_ab(db_path, **kw):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cols = {
        "test_name": "t", "variant_a": "a", "variant_b": "b",
        "variant_a_sent": 0, "variant_b_sent": 0,
        "variant_a_replies": 0, "variant_b_replies": 0,
        "status": "running", "winner": None,
    }
    cols.update(kw)
    conn.execute(
        """INSERT INTO ab_tests (test_name, variant_a, variant_b,
               variant_a_sent, variant_b_sent, variant_a_replies,
               variant_b_replies, status, winner)
           VALUES (:test_name, :variant_a, :variant_b,
               :variant_a_sent, :variant_b_sent, :variant_a_replies,
               :variant_b_replies, :status, :winner)""",
        cols,
    )
    conn.commit()
    row = conn.execute("SELECT id FROM ab_tests").fetchone()
    conn.close()
    return row["id"]


def _roundtrip(steps, sg_db):
    g1 = sg.steps_to_graph(steps)
    out = sg.graph_to_steps(g1)
    g2 = sg.steps_to_graph(out)
    return g1, out, g2


def _assert_equal_graphs(g1, g2):
    assert json.loads(json.dumps(g1)) == json.loads(json.dumps(g2)), (
        f"graph changed after round-trip:\n{json.dumps(g1, indent=2)}\n"
        f"vs\n{json.dumps(g2, indent=2)}"
    )


# ---------------------------------------------------------------- graph_to_steps

def test_graph_to_steps_empty_graph(sg_db):
    assert sg.graph_to_steps({}) == []
    assert sg.graph_to_steps({"nodes": {}, "start": None}) == []
    assert sg.graph_to_steps({"nodes": {"n1": {"type": "email"}}, "start": "zzz"}) == []


def test_roundtrip_five_node_flow(sg_db):
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0, "delay_hours": 0,
         "tone": "gentle", "subject_prefix": "Re: ",
         "message": "Hi {{first_name}}", "subject": "Quick question"},
        {"kind": "wait", "title": "Wacht 2 dagen", "delay_days": 2, "delay_hours": 0},
        {"kind": "linkedin_connect", "title": "LinkedIn connect", "delay_days": 0,
         "delay_hours": 0, "note": "Hi {{first_name}}"},
        {"kind": "condition", "title": "If replied", "delay_days": 0, "delay_hours": 0,
         "condition": "email_replied"},
        {"kind": "message", "title": "Follow-up", "delay_days": 3, "delay_hours": 0,
         "tone": "value_add", "subject_prefix": "Re: ",
         "message": "Following up", "subject": "Re: Quick question", "branch": "no"},
    ]
    g1, out, g2 = _roundtrip(steps, sg_db)
    _assert_equal_graphs(g1, g2)
    # emitted steps are UI-shaped: no node ids, condition targets are indexes
    for s in out:
        assert "id" not in s
        if s["kind"] == "condition":
            assert s["on_true"] in ("stop",) or isinstance(s["on_true"], int)
            assert isinstance(s["on_false"], int)
    # condition order preserved
    assert [s["kind"] for s in out] == [s["kind"] for s in steps]


def test_roundtrip_yes_arm_branch(sg_db):
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0, "delay_hours": 0,
         "tone": "gentle", "subject_prefix": "Re: ", "message": "Hi", "subject": "S"},
        {"kind": "linkedin_connect", "title": "Connect", "delay_days": 1,
         "delay_hours": 0, "note": "Hi {{first_name}}"},
        {"kind": "condition", "title": "If connected", "delay_days": 0,
         "delay_hours": 0, "condition": "linkedin_accepted"},
        {"kind": "linkedin_message", "title": "Yes DM", "delay_days": 0,
         "delay_hours": 0, "tone": "gentle", "message": "Thanks for connecting!",
         "branch": "yes"},
        {"kind": "message", "title": "Email follow-up", "delay_days": 3,
         "delay_hours": 0, "tone": "value_add", "subject_prefix": "Re: ",
         "message": "Following up", "subject": "Re: S", "branch": "no"},
    ]
    g1, out, g2 = _roundtrip(steps, sg_db)
    _assert_equal_graphs(g1, g2)

    cond_id = g1["start"]  # start is the intro; find condition node
    cond = next(n for n in g1["nodes"].values() if n["type"] == "condition")
    yes_id, no_id = cond["on_true"], cond["on_false"]
    assert g1["nodes"][yes_id]["type"] == "linkedin_message"
    assert g1["nodes"][yes_id].get("next") is None  # yes arm stops after DM
    assert g1["nodes"][no_id]["type"] == "email"  # message steps store as email
    # yes chain tagged on the emitted step for UI re-edit
    yes_steps = [s for s in out if s.get("branch") == "yes"]
    assert len(yes_steps) == 1 and yes_steps[0]["kind"] == "linkedin_message"


def test_roundtrip_explicit_stop_arm(sg_db):
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0, "delay_hours": 0,
         "tone": "gentle", "subject_prefix": "Re: ", "message": "Hi", "subject": "S"},
        {"kind": "condition", "title": "If replied", "delay_days": 0,
         "delay_hours": 0, "condition": "email_replied",
         "on_true": "stop", "on_false": "stop"},
    ]
    g1, out, g2 = _roundtrip(steps, sg_db)
    _assert_equal_graphs(g1, g2)
    cond = next(n for n in g2["nodes"].values() if n["type"] == "condition")
    assert cond["on_true"] is None
    assert cond["on_false"] is None


def test_roundtrip_stable_second_cycle(sg_db):
    """graph -> steps -> graph must be a fixed point (edit, save, re-edit)."""
    steps = [
        {"kind": "message", "title": "Intro", "delay_days": 0, "delay_hours": 0,
         "tone": "gentle", "subject_prefix": "Re: ", "message": "Hi", "subject": "S"},
        {"kind": "condition", "title": "If replied", "delay_days": 0,
         "delay_hours": 0, "condition": "email_replied"},
        {"kind": "message", "title": "Yes path", "delay_days": 0, "delay_hours": 0,
         "tone": "friendly", "subject_prefix": "Re: ", "message": "Great!",
         "subject": "Re: S", "branch": "yes"},
        {"kind": "wait", "title": "Wait", "delay_days": 2, "delay_hours": 0,
         "branch": "no"},
    ]
    g1 = sg.steps_to_graph(steps)
    g2 = sg.steps_to_graph(sg.graph_to_steps(g1))
    g3 = sg.steps_to_graph(sg.graph_to_steps(g2))
    _assert_equal_graphs(g1, g2)
    _assert_equal_graphs(g2, g3)


# ---------------------------------------------------------------- A/B learning loop

def test_completed_winner_is_used(learn_db):
    _insert_ab(learn_db, test_name="win_a", status="completed", winner="a",
               variant_a_sent=25, variant_b_sent=25,
               variant_a_replies=9, variant_b_replies=3)
    w = learn.get_variant_weights()
    assert w["win_a"] == {"a": 1.0, "b": 0.0}


def test_running_gap_leans_to_better_variant(learn_db):
    _insert_ab(learn_db, test_name="leaning", variant_a_sent=12, variant_b_sent=14,
               variant_a_replies=4, variant_b_replies=1)
    w = learn.get_variant_weights()
    assert w["leaning"] == {"a": 0.8, "b": 0.2}


def test_running_tie_stays_random(learn_db):
    _insert_ab(learn_db, test_name="tied", variant_a_sent=12, variant_b_sent=12,
               variant_a_replies=3, variant_b_replies=3)
    w = learn.get_variant_weights()
    assert "tied" not in w


def test_insufficient_data_stays_random(learn_db):
    _insert_ab(learn_db, test_name="fresh", variant_a_sent=3, variant_b_sent=2,
               variant_a_replies=1, variant_b_replies=0)
    w = learn.get_variant_weights()
    assert "fresh" not in w


def test_equal_rates_at_threshold_do_not_complete(learn_db):
    test_id = _insert_ab(learn_db, test_name="tie_full",
                         variant_a_sent=19, variant_b_sent=19,
                         variant_a_replies=5, variant_b_replies=5)
    learn.record_ab_test_result(test_id, "a", is_reply=False)
    learn.record_ab_test_result(test_id, "b", is_reply=False)
    conn = sqlite3.connect(learn_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, winner, variant_a_sent, variant_b_sent FROM ab_tests").fetchone()
    conn.close()
    assert row["variant_a_sent"] == 20 and row["variant_b_sent"] == 20
    assert row["status"] == "running"
    assert row["winner"] is None


def test_higher_rate_completes_with_winner(learn_db):
    test_id = _insert_ab(learn_db, test_name="decide_full",
                         variant_a_sent=19, variant_b_sent=19,
                         variant_a_replies=6, variant_b_replies=2)
    learn.record_ab_test_result(test_id, "a", is_reply=False)
    learn.record_ab_test_result(test_id, "b", is_reply=False)
    conn = sqlite3.connect(learn_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status, winner FROM ab_tests").fetchone()
    conn.close()
    assert row["status"] == "completed"
    assert row["winner"] == "a"
