#!/usr/bin/env python3
"""
ClawBuildr Onboarding — Client signup, email/LinkedIn connection, domain verification, ICP creation.
"""

import os
import json
import sqlite3
import logging
import re
import dns.resolver
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Onboarding")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_onboarding_tables():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS onboarding_checklists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                step TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                completed_at TEXT,
                data TEXT DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, step)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS icp_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                industries TEXT DEFAULT '[]',
                company_sizes TEXT DEFAULT '[]',
                roles TEXT DEFAULT '[]',
                regions TEXT DEFAULT '[]',
                keywords TEXT DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS email_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'gmail',
                imap_host TEXT,
                imap_port INTEGER DEFAULT 993,
                smtp_host TEXT,
                smtp_port INTEGER DEFAULT 587,
                username TEXT,
                password_encrypted TEXT,
                is_verified INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                daily_limit INTEGER DEFAULT 50,
                sent_today INTEGER DEFAULT 0,
                warmup_days INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, email)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS domain_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                domain TEXT NOT NULL,
                spf_valid INTEGER DEFAULT 0,
                dkim_valid INTEGER DEFAULT 0,
                dmarc_valid INTEGER DEFAULT 0,
                mx_valid INTEGER DEFAULT 0,
                verified_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, domain)
            )
        """)
        db.commit()
    finally:
        db.close()


ONBOARDING_STEPS = [
    "signup",
    "workspace_created",
    "email_connected",
    "linkedin_connected",
    "domain_verified",
    "icp_created",
    "first_campaign",
]


def start_onboarding(workspace_id: int) -> Dict[str, Any]:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        for step in ONBOARDING_STEPS:
            db.execute(
                """INSERT OR IGNORE INTO onboarding_checklists (workspace_id, step, status, created_at)
                   VALUES (?, ?, 'pending', ?)""",
                (workspace_id, step, datetime.now(timezone.utc).isoformat()),
            )
        db.commit()
        return {"workspace_id": workspace_id, "steps": ONBOARDING_STEPS, "status": "started"}
    finally:
        db.close()


def complete_step(workspace_id: int, step: str, data: Dict = None) -> bool:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            """UPDATE onboarding_checklists
               SET status = 'completed', completed_at = ?, data = ?
               WHERE workspace_id = ? AND step = ?""",
            (now, json.dumps(data or {}), workspace_id, step),
        )
        db.commit()
        return True
    finally:
        db.close()


def get_onboarding_status(workspace_id: int) -> Dict[str, Any]:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT step, status, completed_at, data FROM onboarding_checklists WHERE workspace_id = ? ORDER BY rowid",
            (workspace_id,),
        ).fetchall()
        steps = []
        completed = 0
        for r in rows:
            steps.append({
                "step": r["step"],
                "status": r["status"],
                "completed_at": r["completed_at"],
                "data": json.loads(r["data"]) if r["data"] else {},
            })
            if r["status"] == "completed":
                completed += 1

        return {
            "workspace_id": workspace_id,
            "steps": steps,
            "completed": completed,
            "total": len(ONBOARDING_STEPS),
            "progress_pct": round(completed / len(ONBOARDING_STEPS) * 100) if ONBOARDING_STEPS else 0,
        }
    finally:
        db.close()


def connect_email_account(workspace_id: int, email: str, provider: str = "gmail", credentials: Dict = None) -> Dict[str, Any]:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        imap_host = credentials.get("imap_host", "imap.gmail.com") if credentials else "imap.gmail.com"
        smtp_host = credentials.get("smtp_host", "smtp.gmail.com") if credentials else "smtp.gmail.com"

        cursor = db.execute(
            """INSERT INTO email_accounts
               (workspace_id, email, provider, imap_host, smtp_host, username, password_encrypted)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (workspace_id, email, provider, imap_host, smtp_host,
             credentials.get("username", email) if credentials else email,
             credentials.get("password", "") if credentials else ""),
        )
        db.commit()
        return {"account_id": cursor.lastrowid, "email": email, "provider": provider}
    finally:
        db.close()


def create_icp(workspace_id: int, name: str, description: str = "",
               industries: List[str] = None, company_sizes: List[str] = None,
               roles: List[str] = None, regions: List[str] = None,
               keywords: List[str] = None) -> Dict[str, Any]:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        cursor = db.execute(
            """INSERT INTO icp_profiles
               (workspace_id, name, description, industries, company_sizes, roles, regions, keywords, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (workspace_id, name, description,
             json.dumps(industries or []), json.dumps(company_sizes or []),
             json.dumps(roles or []), json.dumps(regions or []),
             json.dumps(keywords or []), now, now),
        )
        db.commit()
        return {"icp_id": cursor.lastrowid, "name": name}
    finally:
        db.close()


def get_icps(workspace_id: int) -> List[Dict[str, Any]]:
    _ensure_onboarding_tables()
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT * FROM icp_profiles WHERE workspace_id = ? ORDER BY created_at DESC",
            (workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def verify_domain(workspace_id: int, domain: str) -> Dict[str, Any]:
    _ensure_onboarding_tables()
    results = {"domain": domain, "checks": {}}

    try:
        mx_records = dns.resolver.resolve(domain, "MX")
        results["checks"]["mx"] = {"valid": True, "records": [str(r.exchange) for r in mx_records]}
    except Exception as e:
        results["checks"]["mx"] = {"valid": False, "error": str(e)}

    try:
        txt_records = dns.resolver.resolve(domain, "TXT")
        spf_found = any("v=spf1" in str(r) for r in txt_records)
        results["checks"]["spf"] = {"valid": spf_found}
    except Exception as e:
        results["checks"]["spf"] = {"valid": False, "error": str(e)}

    try:
        dkim_records = dns.resolver.resolve(f"google._domainkey.{domain}", "TXT")
        results["checks"]["dkim"] = {"valid": len(list(dkim_records)) > 0}
    except Exception:
        results["checks"]["dkim"] = {"valid": False}

    try:
        dmarc_records = dns.resolver.resolve(f"_dmarc.{domain}", "TXT")
        results["checks"]["dmarc"] = {"valid": any("v=DMARC1" in str(r) for r in dmarc_records)}
    except Exception:
        results["checks"]["dmarc"] = {"valid": False}

    all_valid = all(c.get("valid", False) for c in results["checks"].values())
    results["all_valid"] = all_valid

    db = _get_db()
    try:
        db.execute(
            """INSERT OR REPLACE INTO domain_configs
               (workspace_id, domain, spf_valid, dkim_valid, dmarc_valid, mx_valid, verified_at, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (workspace_id, domain,
             results["checks"].get("spf", {}).get("valid", False),
             results["checks"].get("dkim", {}).get("valid", False),
             results["checks"].get("dmarc", {}).get("valid", False),
             results["checks"].get("mx", {}).get("valid", False),
             datetime.now(timezone.utc).isoformat() if all_valid else None,
             datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
    finally:
        db.close()

    return results


if __name__ == "__main__":
    _ensure_onboarding_tables()
    result = start_onboarding(1)
    print("Onboarding:", result)
    status = get_onboarding_status(1)
    print("Status:", status)
