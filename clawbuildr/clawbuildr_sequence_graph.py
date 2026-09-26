#!/usr/bin/env python3
"""
ClawBuildr Sequence Graph — multi-channel if/else outreach sequences.

Node types (5):
  email             — send an email
  linkedin_connect  — send a LinkedIn connection request
  linkedin_message  — send a LinkedIn DM (1st degree)
  wait              — pure delay
  condition         — if/else branch (email_replied, linkedin_accepted, ...)

Graph JSON shape:
{
  "version": 1,
  "start": "n1",
  "nodes": {
    "n1": {"id":"n1","type":"email","title":"...","delay_days":0,"delay_hours":0,
           "tone":"gentle","subject_prefix":"Re: ","message":"",
           "next":"n2"},
    "n2": {"id":"n2","type":"wait","delay_days":2,"delay_hours":0,"next":"n3"},
    "n3": {"id":"n3","type":"linkedin_connect","delay_days":0,"delay_hours":0,
           "note":"","next":"n4"},
    "n4": {"id":"n4","type":"condition","condition":"email_replied",
           "delay_days":0,"delay_hours":0,
           "on_true":null,   # null => stop (goal reached)
           "on_false":"n5"},
    "n5": {"id":"n5","type":"email","title":"Follow-up", ...}
  }
}
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ClawBuildr.SequenceGraph")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "clawbuildr.db")

ALLOWED_NODE_TYPES = {
    "email",
    "linkedin_connect",
    "linkedin_message",
    "wait",
    "condition",
}

ALLOWED_CONDITIONS = {
    "email_replied",
    "linkedin_accepted",
    "linkedin_replied",
    "always",
}

# Stages that mean the human replied / converted — stop or branch
_STOP_STAGES = {"REPLIED", "MEETING_BOOKED", "CLOSED_WON", "OPT_OUT"}

# Action-node pre-check: never execute sends for contacts in these stages.
# Conditions/waits fall through — they are the graph's own decision points.
_NO_SEND_STAGES = _STOP_STAGES | {"CLOSED_LOST", "BOUNCED", "OBJECTION"}


def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def ensure_tables() -> None:
    db = _db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sequence_graphs (
                graph_id    TEXT PRIMARY KEY,
                tenant_id   TEXT,
                campaign_id TEXT,
                name        TEXT,
                graph_json  TEXT NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sequence_enrollments (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                graph_id        TEXT NOT NULL,
                contact_id      TEXT NOT NULL,
                campaign_id     TEXT,
                current_node_id TEXT,
                status          TEXT NOT NULL DEFAULT 'ACTIVE',
                next_action_at  TEXT,
                last_action_at  TEXT,
                branch_path     TEXT DEFAULT '[]',
                context         TEXT DEFAULT '{}',
                pause_reason    TEXT,
                enrolled_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at    TEXT,
                UNIQUE(graph_id, contact_id)
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_seq_enroll_due
            ON sequence_enrollments(status, next_action_at)
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sequence_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                enrollment_id INTEGER,
                contact_id  TEXT,
                graph_id    TEXT,
                node_id     TEXT,
                node_type   TEXT,
                outcome     TEXT,
                detail      TEXT,
                created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Ensure campaigns can hold a graph pointer
        camp_tables = {r["name"] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='campaigns'"
        ).fetchall()}
        if camp_tables:
            cols = {r["name"] for r in db.execute("PRAGMA table_info(campaigns)").fetchall()}
            if "sequence_graph_json" not in cols:
                db.execute("ALTER TABLE campaigns ADD COLUMN sequence_graph_json TEXT")
        db.commit()
    finally:
        db.close()


# ─── Graph validation / normalization ────────────────────────────────────────

def _is_stop_edge(val: Any) -> bool:
    """True only for the explicit UI/API stop marker (not for unset/null)."""
    return isinstance(val, str) and val.strip().lower() == "stop"


def normalize_graph(raw: Any) -> Dict[str, Any]:
    """Validate and normalize a graph payload. Raises ValueError on bad input."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("graph must be an object")

    nodes_in = raw.get("nodes")
    if isinstance(nodes_in, list):
        node_map: Dict[str, Any] = {}
        for n in nodes_in:
            if not isinstance(n, dict) or not n.get("id"):
                raise ValueError("each node needs an id")
            node_map[str(n["id"])] = n
        nodes_in = node_map
    if not isinstance(nodes_in, dict) or not nodes_in:
        raise ValueError("graph.nodes must be a non-empty object")

    start = str(raw.get("start") or next(iter(nodes_in)))
    if start not in nodes_in:
        raise ValueError(f"start node '{start}' not found")

    nodes: Dict[str, Any] = {}
    for nid, n in nodes_in.items():
        nid = str(nid)
        if not isinstance(n, dict):
            raise ValueError(f"node {nid} must be an object")
        ntype = str(n.get("type") or "email")
        if ntype not in ALLOWED_NODE_TYPES:
            raise ValueError(f"node {nid}: unknown type '{ntype}'")
        try:
            delay_days = max(0, int(n.get("delay_days") or 0))
            delay_hours = max(0, int(n.get("delay_hours") or 0))
        except (TypeError, ValueError):
            raise ValueError(f"node {nid}: invalid delay")

        node: Dict[str, Any] = {
            "id": nid,
            "type": ntype,
            "title": str(n.get("title") or ntype),
            "delay_days": delay_days,
            "delay_hours": delay_hours,
        }

        if ntype == "condition":
            cond = str(n.get("condition") or "email_replied")
            if cond not in ALLOWED_CONDITIONS:
                raise ValueError(f"node {nid}: unknown condition '{cond}'")
            node["condition"] = cond

            def _edge(val):
                if val is None or val == "" or val in ("STOP", "stop"):
                    return None
                return str(val)

            raw_true = n.get("on_true", n.get("true_next"))
            raw_false = n.get("on_false", n.get("false_next"))
            node["on_true"] = _edge(raw_true)
            node["on_false"] = _edge(raw_false)
            # Default false branch: linear next if provided — but an explicit
            # "stop" on the false arm must NOT fall through (bug: arm without
            # target chose Stop, engine kept going).
            nxt = n.get("next")
            if node["on_false"] is None and nxt and not _is_stop_edge(raw_false):
                node["on_false"] = str(nxt)
        else:
            nxt = n.get("next")
            node["next"] = None if nxt in (None, "", "STOP", "stop") else str(nxt)
            if ntype == "email":
                node["tone"] = str(n.get("tone") or "gentle")
                node["subject_prefix"] = str(n.get("subject_prefix") if n.get("subject_prefix") is not None else "Re: ")
                node["message"] = str(n.get("message") or "")
                node["subject"] = str(n.get("subject") or "")
            elif ntype == "linkedin_connect":
                node["note"] = str(n.get("note") or "")
            elif ntype == "linkedin_message":
                node["message"] = str(n.get("message") or "")
                node["tone"] = str(n.get("tone") or "gentle")

        nodes[nid] = node

    # Validate edges
    for nid, node in nodes.items():
        edges = []
        if node["type"] == "condition":
            edges = [node.get("on_true"), node.get("on_false")]
        else:
            edges = [node.get("next")]
        for e in edges:
            if e is not None and e not in nodes:
                raise ValueError(f"node {nid}: edge to unknown node '{e}'")

    return {"version": 1, "start": start, "nodes": nodes}


def steps_to_graph(steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert onboarding UI sequence_steps[] into a validated graph.

    UI step kinds: message (email), linkedin_connect, linkedin_message,
    wait, condition.
    """
    if not steps:
        raise ValueError("sequence has no steps")

    nodes: Dict[str, Any] = {}
    order: List[str] = []

    for i, s in enumerate(steps):
        if not isinstance(s, dict):
            raise ValueError(f"step {i} must be an object")
        nid = str(s.get("id") or f"n{i + 1}")
        raw_kind = str(s.get("kind") or s.get("type") or "message")
        # Map UI kinds → engine types
        if raw_kind in ("message", "email"):
            ntype = "email"
        elif raw_kind in ("linkedin_connect", "li_connect", "connect"):
            ntype = "linkedin_connect"
        elif raw_kind in ("linkedin_message", "li_message", "dm"):
            ntype = "linkedin_message"
        elif raw_kind == "wait":
            ntype = "wait"
        elif raw_kind == "condition":
            ntype = "condition"
        else:
            ntype = raw_kind if raw_kind in ALLOWED_NODE_TYPES else "email"

        base = {
            "id": nid,
            "type": ntype,
            "title": s.get("title") or ntype,
            "delay_days": s.get("delay_days", 0),
            "delay_hours": s.get("delay_hours", 0),
        }
        if ntype == "email":
            base.update({
                "tone": s.get("tone") or "gentle",
                "subject_prefix": s.get("subject_prefix", "Re: "),
                "message": s.get("message") or "",
                "subject": s.get("subject") or "",
            })
        elif ntype == "linkedin_connect":
            base["note"] = s.get("note") or s.get("message") or ""
        elif ntype == "linkedin_message":
            base["message"] = s.get("message") or ""
            base["tone"] = s.get("tone") or "gentle"
        elif ntype == "condition":
            base["condition"] = s.get("condition") or "email_replied"
            # UI may mark branch targets by node id
            base["on_true"] = s.get("on_true")
            base["on_false"] = s.get("on_false")

        nodes[nid] = base
        order.append(nid)

    def _resolve_branch(val):
        """Map UI value (None | int index | node id | 'STOP') to node id or None."""
        if val is None or val == "" or val in ("STOP", "stop"):
            return None
        if isinstance(val, bool):
            return None
        if isinstance(val, int):
            return order[val] if 0 <= val < len(order) else None
        if isinstance(val, str) and val.isdigit():
            idx = int(val)
            return order[idx] if 0 <= idx < len(order) else None
        return str(val)

    # Wire linear next pointers (conditions keep explicit branch targets).
    # branch=yes steps form their own chain and never fall through to no-path.
    step_branches = [str(s.get("branch") or "").lower() for s in steps]

    def _is_yes(idx: int) -> bool:
        return step_branches[idx] in ("yes", "true", "on_true")

    for idx, nid in enumerate(order):
        node = nodes[nid]
        if node["type"] == "condition":
            # default on_false: next non-yes step (skip the true branch body)
            default_false = None
            for j in range(idx + 1, len(order)):
                if not _is_yes(j):
                    default_false = order[j]
                    break
            raw_true = node.get("on_true")
            raw_false = node.get("on_false")
            stop_true = _is_stop_edge(raw_true)
            stop_false = _is_stop_edge(raw_false)
            node["on_true"] = _resolve_branch(raw_true)
            node["on_false"] = _resolve_branch(raw_false)
            if stop_false:
                node["_stop_false"] = True
            if stop_true:
                node["_stop_true"] = True
            # Unset false arm falls through to the main path; an EXPLICIT
            # "stop" choice must stay a stop (no fall-through).
            if not stop_false and node.get("on_false") is None:
                node["on_false"] = default_false
            # on_true defaults to None (stop / goal reached)
            continue

        if node.get("next") is not None:
            continue
        if _is_yes(idx):
            # yes path: only chain to a later yes step; otherwise stop
            nxt_yes = None
            for j in range(idx + 1, len(order)):
                if _is_yes(j):
                    nxt_yes = order[j]
                    break
            node["next"] = nxt_yes
        else:
            # main/no path: next non-yes step (conditions still block? no —
            # a condition on the main path is reached via previous next)
            nxt_main = None
            for j in range(idx + 1, len(order)):
                if not _is_yes(j):
                    nxt_main = order[j]
                    break
            node["next"] = nxt_main

    # Branch tags on steps rewire the nearest prior condition.
    # Only the FIRST yes / FIRST no after a condition set that arm —
    # later same-arm steps are just the linear continuation of that path.
    for i, s in enumerate(steps):
        nid = order[i]
        branch = s.get("branch")
        if branch not in ("yes", "true", "on_true", "no", "false", "on_false"):
            continue
        for j in range(i - 1, -1, -1):
            if nodes[order[j]]["type"] == "condition":
                arm_key = "on_true" if branch in ("yes", "true", "on_true") else "on_false"
                marker = arm_key + "_from_tag"
                # Never rewire an arm the user explicitly set to Stop —
                # a stale branch tag must not turn Stop into fall-through.
                if nodes[order[j]].get("_stop_" + ("true" if arm_key == "on_true" else "false")):
                    break
                if not nodes[order[j]].get(marker):
                    nodes[order[j]][arm_key] = nid
                    nodes[order[j]][marker] = True
                break
    # Strip markers before normalize
    for node in nodes.values():
        node.pop("on_true_from_tag", None)
        node.pop("on_false_from_tag", None)
        node.pop("_stop_true", None)
        node.pop("_stop_false", None)

    graph = {"version": 1, "start": order[0], "nodes": nodes}
    return normalize_graph(graph)


def graph_to_steps(graph: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Reverse of steps_to_graph: stored graph -> onboarding UI sequence_steps[].

    Produces steps the UI can re-edit and steps_to_graph can round-trip:
    branch targets become ARRAY INDEXES (or 'stop'), yes-chain steps carry
    branch='yes'. Node ids are intentionally NOT emitted — the UI strips them
    and steps_to_graph reassigns n1..nN by position, keeping in-flight
    enrollments valid as long as order is unchanged.
    """
    nodes = graph.get("nodes") or {}
    start = graph.get("start")
    if not nodes or start not in nodes:
        return []

    order: List[str] = []
    seen = set()

    def walk(nid: Optional[str]) -> None:
        while nid and nid not in seen:
            node = nodes.get(nid)
            if not node:
                return
            seen.add(nid)
            order.append(nid)
            if node.get("type") == "condition":
                yes = node.get("on_true")
                if yes and yes in nodes:
                    walk(yes)
                nid = node.get("on_false")
                continue
            nxt = node.get("next")
            if not nxt or nxt not in nodes or nxt in seen:
                return
            nid = nxt

    walk(start)
    # Defensive: keep unreachable nodes visible in the editor instead of dropping them
    for nid in sorted(nodes.keys()):
        if nid not in seen:
            order.append(nid)

    branches: Dict[str, str] = {}
    # Re-walk marking yes-chain membership (a step belongs to a yes arm when
    # it is reached via a condition's on_true and never on the main path).
    def mark_yes(nid: Optional[str]) -> None:
        visited = set()
        while nid and nid not in visited:
            visited.add(nid)
            node = nodes.get(nid)
            if not node:
                return
            if node.get("type") == "condition":
                yes = node.get("on_true")
                if yes and yes in nodes:
                    # whole yes arm
                    stack = [yes]
                    while stack:
                        cur = stack.pop()
                        if not cur or cur in visited:
                            continue
                        visited.add(cur)
                        branches[cur] = "yes"
                        cn = nodes.get(cur)
                        if not cn:
                            continue
                        if cn.get("type") == "condition":
                            t = cn.get("on_true")
                            if t:
                                stack.append(t)
                            continue
                        nx = cn.get("next")
                        if nx:
                            stack.append(nx)
                nid = node.get("on_false")
                continue
            nid = node.get("next")

    mark_yes(start)

    index_of = {nid: i for i, nid in enumerate(order)}

    def _target(val: Optional[str]):
        if not val or val not in index_of:
            return "stop"
        return index_of[val]

    steps: List[Dict[str, Any]] = []
    for nid in order:
        n = nodes[nid]
        ntype = n.get("type") or "email"
        kind = "message" if ntype == "email" else ntype
        s: Dict[str, Any] = {
            "kind": kind,
            "title": n.get("title") or kind,
            "delay_days": n.get("delay_days", 0),
            "delay_hours": n.get("delay_hours", 0),
        }
        if branches.get(nid):
            s["branch"] = branches[nid]
        if ntype == "email":
            s.update({
                "subject": n.get("subject") or "",
                "subject_prefix": n.get("subject_prefix", "Re: "),
                "tone": n.get("tone") or "gentle",
                "message": n.get("message") or "",
            })
        elif ntype == "linkedin_connect":
            note = n.get("note") or ""
            s["note"] = note
            s["message"] = note
        elif ntype == "linkedin_message":
            s["message"] = n.get("message") or ""
            s["tone"] = n.get("tone") or "gentle"
        elif ntype == "condition":
            s["condition"] = n.get("condition") or "email_replied"
            s["on_true"] = _target(n.get("on_true"))
            s["on_false"] = _target(n.get("on_false"))
        steps.append(s)
    return steps


def delay_of(node: Dict[str, Any]) -> timedelta:
    return timedelta(days=int(node.get("delay_days") or 0), hours=int(node.get("delay_hours") or 0))


def schedule_from(node: Dict[str, Any], now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return (now + delay_of(node)).isoformat()


# ─── Graph persistence ──────────────────────────────────────────────────────

def save_graph(
    graph: Dict[str, Any],
    tenant_id: str = "clawbuildr",
    campaign_id: Optional[str] = None,
    name: str = "Onboarding sequence",
) -> str:
    ensure_tables()
    graph = normalize_graph(graph)
    graph_id = str(uuid.uuid4())
    db = _db()
    try:
        db.execute(
            """INSERT INTO sequence_graphs (graph_id, tenant_id, campaign_id, name, graph_json, active)
               VALUES (?, ?, ?, ?, ?, 1)""",
            (graph_id, tenant_id, campaign_id, name, json.dumps(graph)),
        )
        if campaign_id:
            db.execute(
                "UPDATE campaigns SET sequence_graph_json = ? WHERE campaign_id = ? OR CAST(campaign_id AS TEXT) = ?",
                (json.dumps(graph), campaign_id, str(campaign_id)),
            )
        # Deactivate other graphs for this tenant and stop their enrollments
        # so replaced graphs can never keep firing (double-send guard).
        old_rows = db.execute(
            "SELECT graph_id FROM sequence_graphs WHERE tenant_id = ? AND graph_id != ?",
            (tenant_id, graph_id),
        ).fetchall()
        for old in old_rows:
            db.execute(
                """UPDATE sequence_enrollments
                   SET status = 'STOPPED', pause_reason = 'graph_replaced'
                   WHERE graph_id = ? AND status IN ('ACTIVE', 'PAUSED')""",
                (old["graph_id"],),
            )
        db.execute(
            "UPDATE sequence_graphs SET active = 0 WHERE tenant_id = ? AND graph_id != ?",
            (tenant_id, graph_id),
        )
        db.commit()
        logger.info("[SeqGraph] Saved graph %s (%d nodes)", graph_id, len(graph["nodes"]))
        return graph_id
    finally:
        db.close()


def get_active_graph(tenant_id: str = "clawbuildr") -> Optional[Tuple[str, Dict[str, Any]]]:
    ensure_tables()
    db = _db()
    try:
        row = db.execute(
            "SELECT graph_id, graph_json FROM sequence_graphs WHERE tenant_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if not row:
            return None
        return row["graph_id"], json.loads(row["graph_json"])
    finally:
        db.close()


def get_graph(graph_id: str) -> Optional[Dict[str, Any]]:
    ensure_tables()
    db = _db()
    try:
        row = db.execute(
            "SELECT graph_json FROM sequence_graphs WHERE graph_id = ?", (graph_id,)
        ).fetchone()
        return json.loads(row["graph_json"]) if row else None
    finally:
        db.close()


# ─── Enrollment ─────────────────────────────────────────────────────────────

def enroll_contact(
    contact_id: str,
    graph_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    tenant_id: str = "clawbuildr",
) -> Optional[str]:
    ensure_tables()
    if not graph_id:
        active = get_active_graph(tenant_id)
        if not active:
            logger.warning("[SeqGraph] No active graph to enroll contact %s", contact_id)
            return None
        graph_id, graph = active
    else:
        graph = get_graph(graph_id)
        if not graph:
            return None

    start = graph["nodes"][graph["start"]]
    now = datetime.now(timezone.utc)
    db = _db()
    try:
        # Idempotent: reactivate if stopped/completed? No — skip if already enrolled
        existing = db.execute(
            "SELECT id, status FROM sequence_enrollments WHERE graph_id = ? AND contact_id = ?",
            (graph_id, str(contact_id)),
        ).fetchone()
        if existing and existing["status"] in ("ACTIVE", "PAUSED"):
            return str(existing["id"])
        # Cross-graph guard: do not enroll if already ACTIVE on another active graph
        other_active = db.execute(
            """SELECT se.id FROM sequence_enrollments se
               JOIN sequence_graphs g ON g.graph_id = se.graph_id
               WHERE se.contact_id = ? AND se.status = 'ACTIVE'
                 AND g.active = 1 AND se.graph_id != ?""",
            (str(contact_id), graph_id),
        ).fetchone()
        if other_active:
            logger.info(
                "[SeqGraph] Contact %s already ACTIVE on another active graph — skip enroll",
                contact_id,
            )
            return None
        next_at = schedule_from(start, now)
        if existing:
            db.execute(
                """UPDATE sequence_enrollments
                   SET status='ACTIVE', current_node_id=?, next_action_at=?,
                       branch_path=?, pause_reason=NULL, completed_at=NULL, last_action_at=?
                   WHERE id=?""",
                (graph["start"], next_at, json.dumps([graph["start"]]), now.isoformat(), existing["id"]),
            )
            eid = str(existing["id"])
        else:
            cur = db.execute(
                """INSERT INTO sequence_enrollments
                   (graph_id, contact_id, campaign_id, current_node_id, status,
                    next_action_at, branch_path, enrolled_at, last_action_at)
                   VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?)""",
                (
                    graph_id,
                    str(contact_id),
                    campaign_id,
                    graph["start"],
                    next_at,
                    json.dumps([graph["start"]]),
                    now.isoformat(),
                    now.isoformat(),
                ),
            )
            eid = str(cur.lastrowid)
        db.commit()
        logger.info("[SeqGraph] Enrolled contact %s at node %s (due %s)", contact_id, graph["start"], next_at)
        return eid
    finally:
        db.close()


def pause_enrollment(contact_id: str, reason: str, graph_id: Optional[str] = None) -> None:
    ensure_tables()
    db = _db()
    try:
        if graph_id:
            db.execute(
                "UPDATE sequence_enrollments SET status='PAUSED', pause_reason=? WHERE contact_id=? AND graph_id=? AND status='ACTIVE'",
                (reason, str(contact_id), graph_id),
            )
        else:
            db.execute(
                "UPDATE sequence_enrollments SET status='PAUSED', pause_reason=? WHERE contact_id=? AND status='ACTIVE'",
                (reason, str(contact_id)),
            )
        db.commit()
    finally:
        db.close()


def get_due_enrollments(limit: int = 25) -> List[Dict[str, Any]]:
    ensure_tables()
    db = _db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        rows = db.execute(
            """SELECT e.*, g.tenant_id AS graph_tenant_id FROM sequence_enrollments e
               JOIN sequence_graphs g ON g.graph_id = e.graph_id
               WHERE e.status = 'ACTIVE'
                 AND e.next_action_at IS NOT NULL
                 AND e.next_action_at <= ?
                 AND g.active = 1
               ORDER BY e.next_action_at ASC LIMIT ?""",
            (now, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def _log_event(enrollment_id, contact_id, graph_id, node_id, node_type, outcome, detail=""):
    db = _db()
    try:
        db.execute(
            """INSERT INTO sequence_events
               (enrollment_id, contact_id, graph_id, node_id, node_type, outcome, detail)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (enrollment_id, str(contact_id), graph_id, node_id, node_type, outcome, str(detail or "")[:500]),
        )
        db.commit()
    except Exception as e:
        logger.warning("[SeqGraph] event log failed: %s", e)
    finally:
        db.close()


def _advance(enrollment_id: Any, next_node_id: Optional[str], graph: Dict[str, Any], branch_path: List[str], status_on_end: str = "COMPLETED"):
    db = _db()
    try:
        now = datetime.now(timezone.utc)
        if not next_node_id:
            db.execute(
                """UPDATE sequence_enrollments
                   SET status=?, current_node_id=NULL, next_action_at=NULL,
                       branch_path=?, last_action_at=?, completed_at=?
                   WHERE id=?""",
                (status_on_end, json.dumps(branch_path), now.isoformat(), now.isoformat(), enrollment_id),
            )
            db.commit()
            return
        node = graph["nodes"].get(next_node_id)
        if node is None:
            db.execute(
                """UPDATE sequence_enrollments
                   SET status='STOPPED', pause_reason=?, next_action_at=NULL,
                       branch_path=?, last_action_at=?, completed_at=?
                   WHERE id=?""",
                (f"missing_node:{next_node_id}", json.dumps(branch_path), now.isoformat(), now.isoformat(), enrollment_id),
            )
            db.commit()
            return
        path = branch_path + [next_node_id]
        db.execute(
            """UPDATE sequence_enrollments
               SET current_node_id=?, next_action_at=?, branch_path=?, last_action_at=?
               WHERE id=?""",
            (next_node_id, schedule_from(node, now), json.dumps(path), now.isoformat(), enrollment_id),
        )
        db.commit()
    finally:
        db.close()


# ─── Condition evaluation ───────────────────────────────────────────────────

def _contact_stage(contact_id: Any) -> Optional[str]:
    db = _db()
    try:
        row = db.execute(
            "SELECT current_stage FROM contacts WHERE CAST(contact_id AS TEXT) = ?",
            (str(contact_id),),
        ).fetchone()
        return row["current_stage"] if row else None
    finally:
        db.close()


def evaluate_condition(condition: str, contact_id: str) -> bool:
    if condition == "always":
        return True
    db = _db()
    try:
        cid = str(contact_id)
        if condition == "email_replied":
            row = db.execute(
                "SELECT current_stage FROM contacts WHERE CAST(contact_id AS TEXT) = ?", (cid,)
            ).fetchone()
            if row and row["current_stage"] in _STOP_STAGES:
                return True
            # Any inbound email? (auto-replies/OOO don't count as replies)
            n = db.execute(
                """SELECT COUNT(*) FROM emails
                   WHERE CAST(contact_id AS TEXT) = ? AND UPPER(direction) IN ('INBOUND', 'IN')
                     AND UPPER(COALESCE(status, '')) NOT IN ('OOO', 'AUTOREPLY', 'AUTO_REPLY', 'AUTO-REPLY')""",
                (cid,),
            ).fetchone()[0]
            return n > 0
        if condition == "linkedin_accepted":
            row = db.execute(
                """SELECT connection_status, accepted_at FROM linkedin_outreach
                   WHERE CAST(contact_id AS TEXT) = ? OR contact_id = ?
                   ORDER BY id DESC LIMIT 1""",
                (cid, cid),
            ).fetchone()
            if not row:
                return False
            return (row["accepted_at"] is not None) or (row["connection_status"] or "").lower() == "accepted"
        if condition == "linkedin_replied":
            row = db.execute(
                """SELECT reply_body FROM linkedin_outreach
                   WHERE CAST(contact_id AS TEXT) = ? OR contact_id = ?
                   ORDER BY id DESC LIMIT 1""",
                (cid, cid),
            ).fetchone()
            return bool(row and row["reply_body"])
        return False
    finally:
        db.close()


# ─── Action executors (injectable for tests) ───────────────────────────────

def _default_send_email(contact_id: str, node: Dict[str, Any], tenant: Dict[str, Any]) -> Dict[str, Any]:
    """Send email via existing tools. Returns {ok, detail}."""
    db = _db()
    try:
        contact = db.execute(
            """SELECT c.*, co.name AS company_name FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE CAST(c.contact_id AS TEXT) = ?""",
            (str(contact_id),),
        ).fetchone()
        if not contact:
            return {"ok": False, "detail": "contact not found"}
        email = contact["email"]
        if not email or "voorbeeld" in email or "example" in email or "test@" in email.lower() or "@" not in email:
            return {"ok": False, "detail": "no usable email"}
        if contact["current_stage"] in ("CLOSED_LOST", "OPT_OUT", "BOUNCED"):
            return {"ok": False, "detail": f"stage {contact['current_stage']}"}

        first = contact["first_name"] or "daar"
        company = contact["company_name"] or ""
        body = node.get("message") or ""
        if not body:
            doctrine = (tenant or {}).get("value_doctrine") or ""
            signature = (tenant or {}).get("signature_block") or (tenant or {}).get("display_name") or "ClawBuildr"
            parts = [f"Hoi {first},"]
            if company:
                parts.append(f"\nIk zag dat je bij {company} werkt — interessant wat jullie doen.")
            else:
                parts.append("\nIk zag je profiel en dacht: interessant wat jullie doen.")
            if doctrine:
                parts.append("\n" + doctrine.strip())
            else:
                parts.append("\nZou je open staan voor een kort gesprek?")
            parts.append("\n\n" + signature.strip())
            body = "\n".join(parts)
        else:
            body = (
                body.replace("{{first_name}}", first)
                .replace("{{naam}}", first)
                .replace("{{company}}", company)
                .replace("{{company_name}}", company)
                .replace("{first_name}", first)
                .replace("{company}", company)
            )
            # Append signature if the template did not include one
            signature = (tenant or {}).get("signature_block") or ""
            if signature and signature.strip() not in body:
                body = body.rstrip() + "\n\n" + signature.strip()
        subject = node.get("subject") or (("Kennismaking met " + company) if company else "Kennismaking")
        subject = (
            subject.replace("{{first_name}}", first)
            .replace("{{naam}}", first)
            .replace("{{company}}", company)
            .replace("{{company_name}}", company)
            .replace("{first_name}", first)
            .replace("{company}", company)
        )
        prefix = node.get("subject_prefix") or ""
        if prefix and not subject.lower().startswith(prefix.lower()):
            # only apply prefix on follow-ups (non-zero delay or explicit Re)
            if int(node.get("delay_days") or 0) + int(node.get("delay_hours") or 0) > 0:
                subject = prefix + subject

        # Prefer async gmail_send via asyncio if no running loop issues — use sync path
        import asyncio
        from tools import gmail_send, resolve_send_kwargs, record_account_send

        campaign_for_send = None
        try:
            crow = db.execute(
                """SELECT campaign_id FROM sequence_enrollments
                   WHERE CAST(contact_id AS TEXT) = ? AND status = 'ACTIVE' AND campaign_id IS NOT NULL
                   ORDER BY id DESC LIMIT 1""",
                (str(contact_id),),
            ).fetchone()
            if crow and crow["campaign_id"]:
                campaign_for_send = crow["campaign_id"]
        except Exception:
            campaign_for_send = None
        send_acc = resolve_send_kwargs(
            campaign_id=campaign_for_send,
            contact_id=str(contact_id),
            tenant_id=(tenant or {}).get("tenant_id"),
        )
        if not send_acc or not send_acc.get("smtp_password"):
            # No usable mailbox for this tenant (not connected / over limit).
            # Defer instead of hard-fail: never fall through to the .env
            # fallback, which would send from a different tenant's mailbox.
            return {"ok": False, "defer": True, "detail": "no_send_account"}

        async def _run():
            return await gmail_send(
                to=email,
                subject=subject,
                body=body,
                from_addr=send_acc.get("email_address"),
                smtp_host=send_acc.get("smtp_host"),
                smtp_port=send_acc.get("smtp_port"),
                smtp_user=send_acc.get("smtp_user"),
                smtp_password=send_acc.get("smtp_password"),
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            # We're inside async executor — run in a fresh loop in thread is handled by caller
            # Fallback: create task not possible here; use nest-free sync SMTP via gmail_send's internals
            # Run a short-lived loop is illegal while one runs; caller should use async path.
            # Use thread-safe approach: execute gmail_send in new event loop via asyncio.run_coroutine_threadsafe
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                res = pool.submit(asyncio.run, _run()).result(timeout=60)
        else:
            res = asyncio.run(_run())

        status = (res or {}).get("status")
        if status == "sent":
            if send_acc.get("account_id"):
                record_account_send(send_acc["account_id"])
            email_id = f"sg_{uuid.uuid4().hex[:12]}"
            db.execute(
                """INSERT INTO emails (email_id, contact_id, direction, subject, body, status, sent_at, created_at)
                   VALUES (?, ?, 'outbound', ?, ?, 'SENT', ?, ?)""",
                (
                    email_id,
                    contact_id,
                    subject,
                    body,
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            db.execute(
                "UPDATE contacts SET current_stage='EMAIL_SENT', updated_at=? WHERE CAST(contact_id AS TEXT)=? AND current_stage NOT IN ('REPLIED','MEETING_BOOKED','CLOSED_WON')",
                (datetime.now(timezone.utc).isoformat(), str(contact_id)),
            )
            db.commit()
            return {"ok": True, "detail": f"sent to {email}"}
        if status == "not_configured":
            # Dev/test: record as queued so graph advances
            db.execute(
                """INSERT INTO emails (email_id, contact_id, direction, subject, body, status, sent_at, created_at)
                   VALUES (?, ?, 'outbound', ?, ?, 'QUEUED_NOT_CONFIGURED', ?, ?)""",
                (
                    f"sg_{uuid.uuid4().hex[:12]}",
                    contact_id,
                    subject,
                    body,
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            db.commit()
            return {"ok": True, "detail": "queued (smtp not configured)"}
        return {"ok": False, "detail": str((res or {}).get("error") or status)}
    except Exception as e:
        logger.exception("[SeqGraph] email send failed")
        return {"ok": False, "detail": str(e)}
    finally:
        db.close()


def _linkedin_action_result(outcome) -> Dict[str, Any]:
    """Map a linkedin_engine outcome to a process_due action result (Bug 4 policy).

    SUCCESS / ALREADY_*  -> ok, advance
    TIME_LIMITED         -> defer (+1h, stays ACTIVE)
    FLAGGED              -> hard-stop the enrollment (account risk)
    anything else        -> skip: log the failure and continue the sequence
    """
    outcome = str(outcome or "FAILURE")
    if outcome in (
        "SUCCESS",
        "SUCCESS_NO_NOTE",
        "ALREADY_CONNECTED",
        "ALREADY_PENDING",
        "ALREADY_CONTACTED",
    ):
        return {"ok": True, "detail": outcome}
    if outcome == "FLAGGED":
        return {"ok": False, "detail": outcome}
    if outcome == "TIME_LIMITED":
        return {"ok": False, "defer": True, "detail": f"rate_limited:{outcome}"}
    return {"ok": True, "skip": True, "detail": f"skip:{outcome}"}


def _default_linkedin_connect(contact_id: str, node: Dict[str, Any], tenant: Dict[str, Any]) -> Dict[str, Any]:
    db = _db()
    try:
        contact = db.execute(
            """SELECT c.*, co.name AS company_name FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE CAST(c.contact_id AS TEXT) = ?""",
            (str(contact_id),),
        ).fetchone()
        if not contact:
            return {"ok": False, "detail": "contact not found"}
        profile = contact["linkedin_url"]
        if not profile:
            # Soft-skip so a missing URL cannot kill a multi-channel sequence
            return {"ok": True, "skip": True, "detail": "no linkedin_url (skipped)"}
        if contact["current_stage"] in ("CLOSED_LOST", "OPT_OUT"):
            return {"ok": False, "detail": f"stage {contact['current_stage']}"}

        from linkedin_engine import search_and_connect, can_send_connection

        tid = (tenant or {}).get("tenant_id")
        can, reason = can_send_connection(tenant_id=tid)
        if not can:
            # Soft-defer: keep enrollment ACTIVE but push next_action_at forward
            return {"ok": False, "defer": True, "detail": f"rate_limited:{reason}"}

        research = None
        if contact["research_result"]:
            try:
                research = json.loads(contact["research_result"])
            except Exception:
                research = None

        note = node.get("note") or ""
        result = search_and_connect(
            first_name=contact["first_name"] or "",
            last_name=contact["last_name"] or "",
            company=contact["company_name"] or "",
            email_body=note,
            contact_id=str(contact_id),
            profile_url=profile,
            research_result=research,
            tenant_id=tid,
        )
        outcome = (result or {}).get("outcome", "FAILURE")
        return _linkedin_action_result(outcome)
    except Exception as e:
        logger.exception("[SeqGraph] linkedin connect failed")
        # Skip-and-continue: an exception must not kill the sequence,
        # and skipping (no retry) means no double-send risk.
        return {"ok": True, "skip": True, "detail": f"error:{str(e)[:150]}"}
    finally:
        db.close()


def _default_linkedin_message(contact_id: str, node: Dict[str, Any], tenant: Dict[str, Any]) -> Dict[str, Any]:
    db = _db()
    try:
        contact = db.execute(
            """SELECT c.*, co.name AS company_name FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE CAST(c.contact_id AS TEXT) = ?""",
            (str(contact_id),),
        ).fetchone()
        if not contact:
            return {"ok": False, "detail": "contact not found"}
        profile = contact["linkedin_url"]
        if not profile:
            # Soft-skip: no LinkedIn URL — advance past this node instead of killing the sequence
            return {"ok": True, "skip": True, "detail": "no linkedin_url (skipped)"}

        from linkedin_engine import can_send_message, _send_followup_message

        tid = (tenant or {}).get("tenant_id")
        can, reason = can_send_message(tenant_id=tid)
        if not can:
            return {"ok": False, "defer": True, "detail": f"rate_limited:{reason}"}

        msg = node.get("message") or ""
        first = contact["first_name"] or ""
        company = contact["company_name"] or ""
        if msg:
            msg = (
                msg.replace("{{first_name}}", first)
                .replace("{{naam}}", first)
                .replace("{{company}}", company)
                .replace("{{company_name}}", company)
                .replace("{first_name}", first)
                .replace("{company}", company)
            )
            outcome = _send_followup_message(profile, first, company, custom_body=msg, tenant_id=tid)
        else:
            outcome = _send_followup_message(profile, first, company, tenant_id=tid)
        return _linkedin_action_result(outcome)
    except Exception as e:
        logger.exception("[SeqGraph] linkedin message failed")
        # Skip-and-continue (Bug 4): do not kill the sequence, do not retry
        return {"ok": True, "skip": True, "detail": f"error:{str(e)[:150]}"}
    finally:
        db.close()


# Injectable actions (tests replace these)
SEND_EMAIL = _default_send_email
SEND_LINKEDIN_CONNECT = _default_linkedin_connect
SEND_LINKEDIN_MESSAGE = _default_linkedin_message


def set_actions(email=None, li_connect=None, li_message=None):
    global SEND_EMAIL, SEND_LINKEDIN_CONNECT, SEND_LINKEDIN_MESSAGE
    if email is not None:
        SEND_EMAIL = email
    if li_connect is not None:
        SEND_LINKEDIN_CONNECT = li_connect
    if li_message is not None:
        SEND_LINKEDIN_MESSAGE = li_message


def _load_tenant(tenant_id: Optional[str] = None) -> Dict[str, Any]:
    """Load tenant config. tenant_id=None → the active tenant (legacy)."""
    db = _db()
    try:
        if tenant_id:
            row = db.execute(
                "SELECT * FROM tenant_config WHERE tenant_id = ?", (tenant_id,)
            ).fetchone()
        else:
            row = db.execute("SELECT * FROM tenant_config WHERE active=1 LIMIT 1").fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}
    finally:
        db.close()


# ─── Main tick ──────────────────────────────────────────────────────────────

def process_due(limit: int = 25) -> Dict[str, Any]:
    """Process all due sequence enrollments. Returns stats."""
    ensure_tables()
    stats = {"processed": 0, "advanced": 0, "stopped": 0, "deferred": 0, "errors": 0}
    due = get_due_enrollments(limit=limit)
    if not due:
        return stats

    for enr in due:
        # Per-enrollment tenant: sends/signature come from the graph's own
        # tenant, NOT whichever tenant happens to be active right now.
        tenant = _load_tenant(enr.get("graph_tenant_id"))
        eid = enr["id"]
        contact_id = enr["contact_id"]
        graph_id = enr["graph_id"]
        # Atomic claim: only one concurrent process_due may claim this row
        # (background loop vs first-tick on campaign create).
        claim_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        claim_db = _db()
        try:
            cur = claim_db.execute(
                """UPDATE sequence_enrollments
                   SET next_action_at = ?
                   WHERE id = ? AND status = 'ACTIVE' AND next_action_at = ?""",
                (claim_at, eid, enr.get("next_action_at")),
            )
            claim_db.commit()
            if cur.rowcount != 1:
                continue
        finally:
            claim_db.close()
        try:
            graph = get_graph(graph_id)
            if not graph:
                db_stop = _db()
                try:
                    db_stop.execute(
                        "UPDATE sequence_enrollments SET status='STOPPED', pause_reason='missing_graph', next_action_at=NULL WHERE id=?",
                        (eid,),
                    )
                    db_stop.commit()
                finally:
                    db_stop.close()
                stats["stopped"] += 1
                continue
            node_id = enr["current_node_id"]
            node = graph["nodes"].get(node_id)
            if not node:
                _advance(eid, None, graph, json.loads(enr.get("branch_path") or "[]"), status_on_end="STOPPED")
                stats["stopped"] += 1
                continue

            path = json.loads(enr.get("branch_path") or "[]")
            ntype = node["type"]

            # Terminal-stage pre-check: never execute sends for contacts that
            # replied / converted / opted out since enrollment. Condition nodes
            # decide for themselves and wait nodes are harmless — both fall through.
            if ntype in ("email", "linkedin_connect", "linkedin_message"):
                stage = _contact_stage(contact_id)
                if stage and stage in _NO_SEND_STAGES:
                    _log_event(eid, contact_id, graph_id, node_id, ntype, "stop", f"terminal_stage:{stage}")
                    db_ts = _db()
                    try:
                        db_ts.execute(
                            """UPDATE sequence_enrollments
                               SET status='STOPPED', pause_reason=?, next_action_at=NULL, completed_at=?
                               WHERE id=?""",
                            (f"terminal_stage:{stage}", datetime.now(timezone.utc).isoformat(), eid),
                        )
                        db_ts.commit()
                    finally:
                        db_ts.close()
                    stats["stopped"] += 1
                    continue

            stats["processed"] += 1

            if ntype == "condition":
                result = evaluate_condition(node.get("condition") or "email_replied", contact_id)
                target = node.get("on_true") if result else node.get("on_false")
                branch = "true" if result else "false"
                _log_event(eid, contact_id, graph_id, node_id, "condition", branch, node.get("condition"))
                if target is None:
                    _advance(eid, None, graph, path, status_on_end="COMPLETED")
                    stats["stopped"] += 1
                else:
                    _advance(eid, target, graph, path)
                    stats["advanced"] += 1
                continue

            if ntype == "wait":
                nxt = node.get("next")
                _log_event(eid, contact_id, graph_id, node_id, "wait", "ok", "")
                if nxt is None:
                    _advance(eid, None, graph, path, status_on_end="COMPLETED")
                    stats["stopped"] += 1
                else:
                    _advance(eid, nxt, graph, path)
                    stats["advanced"] += 1
                continue

            # Actions
            if ntype == "email":
                try:
                    from clawbuildr_watchdog import can_send as _wd_can_send
                    if not _wd_can_send():
                        # Push one hour and stay on same node — over limit / unhealthy
                        db_wd = _db()
                        try:
                            nxt_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
                            db_wd.execute(
                                "UPDATE sequence_enrollments SET next_action_at=?, last_action_at=? WHERE id=?",
                                (nxt_at, datetime.now(timezone.utc).isoformat(), eid),
                            )
                            db_wd.commit()
                        finally:
                            db_wd.close()
                        stats["deferred"] += 1
                        continue
                except Exception:
                    # Fail open on watchdog import/query errors would risk double-send;
                    # fail closed instead — skip this send this cycle.
                    db_wd = _db()
                    try:
                        nxt_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
                        db_wd.execute(
                            "UPDATE sequence_enrollments SET next_action_at=?, last_action_at=? WHERE id=?",
                            (nxt_at, datetime.now(timezone.utc).isoformat(), eid),
                        )
                        db_wd.commit()
                    finally:
                        db_wd.close()
                    stats["deferred"] += 1
                    continue
                res = SEND_EMAIL(contact_id, node, tenant)
                # Human-like pause between sends
                import time as _time
                _time.sleep(1.5)
            elif ntype == "linkedin_connect":
                res = SEND_LINKEDIN_CONNECT(contact_id, node, tenant)
            elif ntype == "linkedin_message":
                res = SEND_LINKEDIN_MESSAGE(contact_id, node, tenant)
            else:
                res = {"ok": False, "detail": f"unknown type {ntype}"}

            _status = (
                "defer" if res.get("defer")
                else "fail" if not res.get("ok")
                else "skip" if res.get("skip")
                else "ok"
            )
            _log_event(eid, contact_id, graph_id, node_id, ntype, _status, res.get("detail"))

            if res.get("defer"):
                # Push one hour and stay on same node
                db = _db()
                try:
                    nxt_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
                    db.execute(
                        "UPDATE sequence_enrollments SET next_action_at=?, last_action_at=? WHERE id=?",
                        (nxt_at, datetime.now(timezone.utc).isoformat(), eid),
                    )
                    db.commit()
                finally:
                    db.close()
                stats["deferred"] += 1
                continue

            if not res.get("ok"):
                # Hard fail on this action — stop sequence to avoid spam loops
                db = _db()
                try:
                    db.execute(
                        "UPDATE sequence_enrollments SET status='STOPPED', pause_reason=?, next_action_at=NULL, completed_at=? WHERE id=?",
                        (f"action_failed:{res.get('detail','')}"[:200], datetime.now(timezone.utc).isoformat(), eid),
                    )
                    db.commit()
                finally:
                    db.close()
                stats["errors"] += 1
                continue

            nxt = node.get("next")
            if nxt is None:
                _advance(eid, None, graph, path, status_on_end="COMPLETED")
                stats["stopped"] += 1
            else:
                _advance(eid, nxt, graph, path)
                stats["advanced"] += 1

        except Exception as e:
            logger.exception("[SeqGraph] error processing enrollment %s", eid)
            stats["errors"] += 1
            try:
                db = _db()
                db.execute(
                    "UPDATE sequence_enrollments SET status='PAUSED', pause_reason=? WHERE id=?",
                    (str(e)[:200], eid),
                )
                db.commit()
                db.close()
            except Exception:
                pass

    return stats


def enroll_eligible_leads(
    limit: int = 50,
    tenant_id: str = "clawbuildr",
    campaign_id: Optional[str] = None,
) -> int:
    """Enroll early-stage leads that are not yet on the active graph. Returns count enrolled."""
    ensure_tables()
    active = get_active_graph(tenant_id)
    if not active:
        return 0
    graph_id, _graph = active
    db = _db()
    try:
        grow = db.execute(
            "SELECT campaign_id FROM sequence_graphs WHERE graph_id = ?", (graph_id,)
        ).fetchone()
        eff_campaign = campaign_id or (grow["campaign_id"] if grow else None)
        if eff_campaign:
            # Graph owns a campaign → only enroll leads explicitly attached to it
            rows = db.execute(
                """SELECT c.contact_id FROM contacts c
                   JOIN campaign_leads cl ON cl.contact_id = CAST(c.contact_id AS TEXT)
                   LEFT JOIN sequence_enrollments se
                     ON se.contact_id = CAST(c.contact_id AS TEXT) AND se.graph_id = ?
                   WHERE cl.campaign_id = ?
                     AND (IFNULL(c.tenant_id,'') = '' OR c.tenant_id = ?)
                     AND c.current_stage IN ('INGESTED','RESEARCHED','DELIVERABILITY_VERIFIED','OPPORTUNITY_MAPPED','PRE_QUALIFIED','ACTIVE_OUTREACH')
                     AND se.id IS NULL
                   ORDER BY c.lead_score DESC
                   LIMIT ?""",
                (graph_id, eff_campaign, tenant_id, int(limit)),
            ).fetchall()
        else:
            rows = db.execute(
                """SELECT c.contact_id FROM contacts c
                   LEFT JOIN sequence_enrollments se
                     ON se.contact_id = CAST(c.contact_id AS TEXT) AND se.graph_id = ?
                   WHERE (IFNULL(c.tenant_id,'') = '' OR c.tenant_id = ?)
                     AND c.current_stage IN ('INGESTED','RESEARCHED','DELIVERABILITY_VERIFIED','OPPORTUNITY_MAPPED','PRE_QUALIFIED','ACTIVE_OUTREACH')
                     AND se.id IS NULL
                   ORDER BY c.lead_score DESC
                   LIMIT ?""",
                (graph_id, tenant_id, int(limit)),
            ).fetchall()
        count = 0
        for r in rows:
            if enroll_contact(str(r["contact_id"]), graph_id=graph_id, campaign_id=eff_campaign, tenant_id=tenant_id):
                count += 1
        if count:
            logger.info("[SeqGraph] Auto-enrolled %d new lead(s) into graph %s", count, graph_id)
        return count
    finally:
        db.close()


def get_stats(graph_id: Optional[str] = None) -> Dict[str, Any]:
    ensure_tables()
    db = _db()
    try:
        if graph_id:
            rows = db.execute(
                "SELECT status, COUNT(*) AS c FROM sequence_enrollments WHERE graph_id=? GROUP BY status",
                (graph_id,),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT status, COUNT(*) AS c FROM sequence_enrollments GROUP BY status"
            ).fetchall()
        out = {r["status"]: r["c"] for r in rows}
        events = db.execute("SELECT COUNT(*) FROM sequence_events").fetchone()[0]
        return {"by_status": out, "events": events}
    finally:
        db.close()


if __name__ == "__main__":
    ensure_tables()
    print("tables ok", get_stats())
