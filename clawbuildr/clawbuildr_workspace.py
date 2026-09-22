#!/usr/bin/env python3
"""
ClawBuildr Workspace — Multi-tenant workspace isolation.
Each client gets their own workspace with isolated data, settings, and quotas.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Workspace")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_workspace_tables():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS workspaces (
                workspace_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                plan TEXT NOT NULL DEFAULT 'free',
                owner_id INTEGER,
                settings TEXT DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER NOT NULL DEFAULT 1
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS workspace_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
                UNIQUE(workspace_id, user_id)
            )
        """)
        db.commit()
    finally:
        db.close()


def create_workspace(name: str, slug: str, plan: str = "free", owner_id: int = None) -> Dict[str, Any]:
    _ensure_workspace_tables()
    db = _get_db()
    try:
        cursor = db.execute(
            "INSERT INTO workspaces (name, slug, plan, owner_id) VALUES (?, ?, ?, ?)",
            (name, slug, plan, owner_id),
        )
        ws_id = cursor.lastrowid
        if owner_id:
            db.execute(
                "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (?, ?, 'owner')",
                (ws_id, owner_id),
            )
        db.commit()
        return {"workspace_id": ws_id, "name": name, "slug": slug, "plan": plan}
    finally:
        db.close()


def get_workspace(workspace_id: int) -> Optional[Dict[str, Any]]:
    db = _get_db()
    try:
        row = db.execute("SELECT * FROM workspaces WHERE workspace_id = ? AND is_active = 1", (workspace_id,)).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


def get_workspace_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    db = _get_db()
    try:
        row = db.execute("SELECT * FROM workspaces WHERE slug = ? AND is_active = 1", (slug,)).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


def list_workspaces() -> List[Dict[str, Any]]:
    _ensure_workspace_tables()
    db = _get_db()
    try:
        rows = db.execute("SELECT * FROM workspaces WHERE is_active = 1 ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def update_workspace_settings(workspace_id: int, settings: Dict[str, Any]) -> bool:
    db = _get_db()
    try:
        db.execute(
            "UPDATE workspaces SET settings = ? WHERE workspace_id = ?",
            (json.dumps(settings), workspace_id),
        )
        db.commit()
        return True
    finally:
        db.close()


def get_workspace_settings(workspace_id: int) -> Dict[str, Any]:
    db = _get_db()
    try:
        row = db.execute("SELECT settings FROM workspaces WHERE workspace_id = ?", (workspace_id,)).fetchone()
        if row and row["settings"]:
            return json.loads(row["settings"])
        return {}
    finally:
        db.close()


def add_workspace_member(workspace_id: int, user_id: int, role: str = "member") -> bool:
    db = _get_db()
    try:
        db.execute(
            "INSERT OR IGNORE INTO workspace_members (workspace_id, user_id, role) VALUES (?, ?, ?)",
            (workspace_id, user_id, role),
        )
        db.commit()
        return True
    finally:
        db.close()


def get_workspace_members(workspace_id: int) -> List[Dict[str, Any]]:
    db = _get_db()
    try:
        rows = db.execute(
            """SELECT wm.*, u.email, u.name
               FROM workspace_members wm
               JOIN users u ON wm.user_id = u.user_id
               WHERE wm.workspace_id = ?""",
            (workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def is_workspace_member(workspace_id: int, user_id: int) -> bool:
    db = _get_db()
    try:
        row = db.execute(
            "SELECT 1 FROM workspace_members WHERE workspace_id = ? AND user_id = ?",
            (workspace_id, user_id),
        ).fetchone()
        return row is not None
    finally:
        db.close()


def get_workspace_stats(workspace_id: int) -> Dict[str, Any]:
    db = _get_db()
    try:
        contacts = db.execute("SELECT COUNT(*) as cnt FROM contacts").fetchone()["cnt"]
        emails = db.execute("SELECT COUNT(*) as cnt FROM emails").fetchone()["cnt"]
        linkedin = db.execute("SELECT COUNT(*) as cnt FROM linkedin_outreach").fetchone()["cnt"]
        campaigns = db.execute("SELECT COUNT(*) as cnt FROM campaigns").fetchone()["cnt"]

        return {
            "contacts": contacts,
            "emails_sent": emails,
            "linkedin_outreach": linkedin,
            "campaigns": campaigns,
        }
    finally:
        db.close()


def deactivate_workspace(workspace_id: int) -> bool:
    db = _get_db()
    try:
        db.execute("UPDATE workspaces SET is_active = 0 WHERE workspace_id = ?", (workspace_id,))
        db.commit()
        return True
    finally:
        db.close()


if __name__ == "__main__":
    _ensure_workspace_tables()
    result = create_workspace("Test Workspace", "test-ws", "pro", 1)
    print("Created:", result)
    ws = get_workspace(result["workspace_id"])
    print("Workspace:", ws)
    stats = get_workspace_stats(result["workspace_id"])
    print("Stats:", stats)
