#!/usr/bin/env python3
"""
ClawBuildr Onboarding — Client signup, email/LinkedIn connection, domain verification, ICP creation.
"""

import os
import json
import sqlite3
import logging
import re
import uuid
import smtplib
import imaplib
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

try:
    import dns_client
except ImportError:
    from clawbuildr import dns_client

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
        # Real schema — must match tools.resolve_send_kwargs / dashboard expectations.
        db.execute("""
            CREATE TABLE IF NOT EXISTS email_accounts (
                account_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email_address TEXT NOT NULL UNIQUE,
                provider TEXT DEFAULT 'gmail',
                smtp_host TEXT,
                smtp_port INTEGER DEFAULT 587,
                smtp_user TEXT,
                smtp_password TEXT,
                is_active INTEGER DEFAULT 1,
                is_default INTEGER DEFAULT 0,
                daily_send_limit INTEGER DEFAULT 50,
                sends_today INTEGER DEFAULT 0,
                last_send_at TEXT,
                warmup_days INTEGER DEFAULT 0,
                signature TEXT,
                reply_to TEXT,
                tenant_id TEXT DEFAULT '',
                created_at TEXT,
                updated_at TEXT
            )
        """)
        try:
            db.execute("ALTER TABLE email_accounts ADD COLUMN tenant_id TEXT DEFAULT ''")
        except Exception:
            pass
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


def _verify_smtp(email: str, password: str, smtp_host: str = "smtp.gmail.com", smtp_port: int = 587) -> None:
    """Login-test a mailbox over SMTP. Raises on failure."""
    with smtplib.SMTP(smtp_host, int(smtp_port), timeout=15) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(email, password)


def _verify_imap(email: str, password: str, imap_host: str = "imap.gmail.com") -> bool:
    """Login-test a mailbox over IMAP. Returns False instead of raising."""
    server = None
    try:
        server = imaplib.IMAP4_SSL(imap_host, timeout=15)
        server.login(email, password)
        return True
    except Exception:
        return False
    finally:
        if server is not None:
            try:
                server.logout()
            except Exception:
                pass


def connect_email_account(
    tenant_id: str,
    email: str,
    password: str,
    provider: str = "gmail",
    display_name: str = "",
    verify: bool = True,
    daily_send_limit: int = 50,
    smtp_host: str = "smtp.gmail.com",
    smtp_port: int = 587,
) -> Dict[str, Any]:
    """Connect (upsert) a sending mailbox for a tenant.

    Verifies SMTP login first (unless verify=False), stores the account as the
    tenant's default sender and updates tenant_config.sending_email.
    Returns {"ok": True, ...} or {"ok": False, "stage": ..., "error": ...}.
    """
    email = (email or "").strip().lower()
    if not email or "@" not in email or email.startswith("@") or email.endswith("@"):
        return {"ok": False, "stage": "input", "error": "Ongeldig e-mailadres."}
    if not password:
        return {"ok": False, "stage": "input", "error": "App-wachtwoord ontbreekt."}
    tenant_id = tenant_id or "default"

    imap_ok = False
    if verify:
        try:
            _verify_smtp(email, password, smtp_host, smtp_port)
        except smtplib.SMTPAuthenticationError:
            return {
                "ok": False,
                "stage": "smtp",
                "error": "SMTP inloggen mislukt — controleer e-mailadres en app-wachtwoord "
                         "(2-stapsverificatie aan + app-wachtwoord genereren).",
            }
        except Exception as e:
            return {"ok": False, "stage": "smtp", "error": f"SMTP verbinding mislukt: {e}"}
        imap_ok = _verify_imap(email, password)

    _ensure_onboarding_tables()
    db = _get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        domain = email.split("@")[1]
        existing = db.execute(
            "SELECT account_id, tenant_id FROM email_accounts WHERE email_address = ?",
            (email,),
        ).fetchone()

        if existing:
            owner = existing["tenant_id"] or ""
            if owner and owner != tenant_id:
                return {
                    "ok": False,
                    "stage": "input",
                    "error": f"Dit mailbox is al gekoppeld aan een ander account ({owner}).",
                }
            account_id = existing["account_id"]
            db.execute(
                """UPDATE email_accounts
                   SET tenant_id = ?, provider = ?, smtp_host = ?, smtp_port = ?,
                       smtp_user = ?, smtp_password = ?, display_name = ?,
                       is_active = 1, updated_at = ?
                   WHERE account_id = ?""",
                (tenant_id, provider, smtp_host, int(smtp_port), email, password,
                 display_name or email.split("@")[0], now, account_id),
            )
            was_update = True
        else:
            account_id = str(uuid.uuid4())
            db.execute(
                """INSERT INTO email_accounts
                   (account_id, display_name, email_address, provider, smtp_host, smtp_port,
                    smtp_user, smtp_password, is_active, is_default, daily_send_limit,
                    tenant_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?)""",
                (account_id, display_name or email.split("@")[0], email, provider,
                 smtp_host, int(smtp_port), email, password, int(daily_send_limit),
                 tenant_id, now, now),
            )
            was_update = False

        # Exactly one default per tenant — demote the others, keep/raise this one.
        db.execute(
            "UPDATE email_accounts SET is_default = 0, updated_at = ? WHERE tenant_id = ? AND account_id != ?",
            (now, tenant_id, account_id),
        )
        db.execute(
            "UPDATE email_accounts SET is_default = 1, is_active = 1, updated_at = ? WHERE account_id = ?",
            (now, account_id),
        )

        try:
            db.execute(
                "UPDATE tenant_config SET sending_email = ?, sending_domain = ? WHERE tenant_id = ?",
                (email, domain, tenant_id),
            )
        except Exception:
            pass

        db.commit()
    finally:
        db.close()

    logger.info(f"[Onboarding] Email connected for tenant '{tenant_id}': {email} (imap_ok={imap_ok})")
    return {
        "ok": True,
        "account_id": account_id,
        "email": email,
        "imap_ok": imap_ok,
        "was_update": was_update,
    }


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
        mx_records = dns_client.resolve(domain, "MX")
        results["checks"]["mx"] = {"valid": True, "records": [str(r.exchange) for r in mx_records]}
    except Exception as e:
        results["checks"]["mx"] = {"valid": False, "error": str(e)}

    try:
        txt_records = dns_client.resolve(domain, "TXT")
        spf_found = any("v=spf1" in str(r) for r in txt_records)
        results["checks"]["spf"] = {"valid": spf_found}
    except Exception as e:
        results["checks"]["spf"] = {"valid": False, "error": str(e)}

    try:
        dkim_records = dns_client.resolve(f"google._domainkey.{domain}", "TXT")
        results["checks"]["dkim"] = {"valid": len(list(dkim_records)) > 0}
    except Exception:
        results["checks"]["dkim"] = {"valid": False}

    try:
        dmarc_records = dns_client.resolve(f"_dmarc.{domain}", "TXT")
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


# ═══════════════════════════════════════════════════════════════════════════════
# NEW: tenant-based simple onboarding state (used by the Skylead-style wizard)
# ═══════════════════════════════════════════════════════════════════════════════

ONBOARDING_FLOW = [
    "campaign_info",
    "settings",
    "sequence",
    "review",
    "running",
]

LEGACY_ONBOARDING_STEPS = {
    "welcome": "campaign_info",
    "channels": "settings",
    "gmail": "settings",
    "profile": "campaign_info",
    "icp": "settings",
    "message": "campaign_info",
    "sources": "settings",
    "search": "settings",
    "review": "review",
    "goal": "campaign_info",
    "company": "campaign_info",
}


def _ensure_onboarding_state_table():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS onboarding_state (
                tenant_id TEXT PRIMARY KEY,
                completed INTEGER NOT NULL DEFAULT 0,
                current_step TEXT NOT NULL DEFAULT 'campaign_info',
                data TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.commit()
    finally:
        db.close()


def _normalize_step(step: str) -> str:
    step = LEGACY_ONBOARDING_STEPS.get(step, step)
    if step not in ONBOARDING_FLOW:
        return ONBOARDING_FLOW[0]
    return step


def _normalize_completed(steps: Any) -> List[str]:
    normalized: List[str] = []
    for s in steps or []:
        if not isinstance(s, str):
            continue
        mapped = LEGACY_ONBOARDING_STEPS.get(s, s)
        if mapped in ONBOARDING_FLOW and mapped not in normalized:
            normalized.append(mapped)
    return normalized


def _parse_state(row: sqlite3.Row) -> Dict[str, Any]:
    data = {}
    if row["data"]:
        try:
            data = json.loads(row["data"])
        except Exception:
            data = {}
    completed_steps = _normalize_completed(data.get("completed_steps", []))
    data["completed_steps"] = completed_steps
    current_step = _normalize_step(row["current_step"])
    return {
        "tenant_id": row["tenant_id"],
        "completed": bool(row["completed"]),
        "current_step": current_step,
        "data": data,
        "steps": [
            {"step": s, "completed": s in completed_steps}
            for s in ONBOARDING_FLOW
        ],
        "progress_pct": round(len(completed_steps) / len(ONBOARDING_FLOW) * 100),
    }


def get_onboarding_state(tenant_id: str) -> Dict[str, Any]:
    """Get current onboarding state for a tenant."""
    _ensure_onboarding_state_table()
    db = _get_db()
    try:
        row = db.execute(
            "SELECT * FROM onboarding_state WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if not row:
            return _create_onboarding_state(tenant_id)
        return _parse_state(row)
    finally:
        db.close()


def _create_onboarding_state(tenant_id: str) -> Dict[str, Any]:
    _ensure_onboarding_state_table()
    db = _get_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        db.execute(
            """INSERT INTO onboarding_state (tenant_id, completed, current_step, data, updated_at)
               VALUES (?, 0, 'campaign_info', '{}', ?)""",
            (tenant_id, now),
        )
        db.commit()
    finally:
        db.close()
    return get_onboarding_state(tenant_id)


def save_onboarding_step(tenant_id: str, step: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Save progress for one onboarding step and advance to the next."""
    _ensure_onboarding_state_table()
    db = _get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        step = _normalize_step(step)
        state = get_onboarding_state(tenant_id)
        merged = state["data"]
        merged.setdefault("completed_steps", [])
        if step not in merged["completed_steps"]:
            merged["completed_steps"].append(step)
        if data:
            merged.setdefault("step_data", {})
            merged["step_data"][step] = data

        current_index = ONBOARDING_FLOW.index(step)
        next_step = ONBOARDING_FLOW[min(current_index + 1, len(ONBOARDING_FLOW) - 1)]

        db.execute(
            """INSERT INTO onboarding_state (tenant_id, completed, current_step, data, updated_at)
               VALUES (?, 0, ?, ?, ?)
               ON CONFLICT(tenant_id) DO UPDATE SET
                   current_step = excluded.current_step,
                   data = excluded.data,
                   updated_at = excluded.updated_at""",
            (tenant_id, next_step, json.dumps(merged), now),
        )
        db.commit()
        return get_onboarding_state(tenant_id)
    finally:
        db.close()


def complete_onboarding(tenant_id: str) -> Dict[str, Any]:
    """Mark onboarding as complete."""
    _ensure_onboarding_state_table()
    db = _get_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        state = get_onboarding_state(tenant_id)
        data = state["data"]
        data.setdefault("completed_steps", [])
        if "running" not in data["completed_steps"]:
            data["completed_steps"].append("running")
        db.execute(
            """INSERT INTO onboarding_state (tenant_id, completed, current_step, data, updated_at)
               VALUES (?, 1, 'running', ?, ?)
               ON CONFLICT(tenant_id) DO UPDATE SET
                   completed = excluded.completed,
                   current_step = excluded.current_step,
                   data = excluded.data,
                   updated_at = excluded.updated_at""",
            (tenant_id, json.dumps(data), now),
        )
        db.commit()
        return get_onboarding_state(tenant_id)
    finally:
        db.close()


def is_onboarding_complete(tenant_id: str) -> bool:
    state = get_onboarding_state(tenant_id)
    return bool(state.get("completed"))


if __name__ == "__main__":
    _ensure_onboarding_tables()
    result = start_onboarding(1)
    print("Onboarding:", result)
    status = get_onboarding_status(1)
    print("Status:", status)
