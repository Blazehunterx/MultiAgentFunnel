#!/usr/bin/env python3
"""
ClawBuildr Agent Mission Control Dashboard
FastAPI Unified Backend, Real-Time SSE Broadcaster, and Single-Page App Frontend.
Integrates with SQLite database, executes agent workflows, and provides live handoff visualizer.
"""

import os
import sys
import json
import sqlite3
import asyncio
import logging
import hashlib
import random
import time
import re
import uuid
import urllib.parse
import contextlib
import concurrent.futures
import httpx
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, AsyncGenerator
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request, Response, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

# Add clawbuildr pipeline to import path FIRST
import os
# Use relative paths so it works on any machine
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLAWBUILDR_DIR = os.path.join(BASE_DIR, "clawbuildr")
if CLAWBUILDR_DIR not in sys.path:
    sys.path.insert(0, CLAWBUILDR_DIR)

# Add local data dir to path so we can import clawbuildr_multi_agent directly
DATA_DIR = os.path.join(BASE_DIR, "data")
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

from models import LeadInput
from pipeline import research_company, assess_trust, map_opportunity, qualify, generate_email
from tools import gmail_send, scrape_url
from linkedin_engine import search_and_connect, get_clawbuildr_linkedin_history, generate_linkedin_note, linkedin_login, linkedin_login_status, get_daily_count_api, get_scheduling_status, extract_firefox_cookies

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("ClawBuildrDashboard")

# Database Path
DB_PATH = os.path.join(BASE_DIR, "data", "clawbuildr.db")

# =========================================================================
# 1. DATABASE MIGRATIONS & SEED DATA ENGINE
# =========================================================================

def run_migrations():
    """Migrates the existing SQLite database to include agent and dashboard tables."""
    logger.info("Initializing database migrations for Dashboard...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # 1. Create Agent Status Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agent_status (
        agent_name VARCHAR(100) PRIMARY KEY,
        status VARCHAR(50) NOT NULL DEFAULT 'IDLE', -- IDLE, RUNNING, WAITING, FAILED
        current_lead_id VARCHAR(50),
        last_active_at VARCHAR(100),
        last_action TEXT,
        error_message TEXT
    );
    """)
    
    # 2. Create Domain Deliverability Stats Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deliverability_stats (
        domain VARCHAR(255) PRIMARY KEY,
        sends_count INT DEFAULT 0,
        replies_count INT DEFAULT 0,
        bounces_count INT DEFAULT 0,
        bounce_rate REAL DEFAULT 0.0,
        domain_health VARCHAR(50) DEFAULT 'HEALTHY', -- HEALTHY, WARMING, BLOCKED
        updated_at VARCHAR(100)
    );
    """)

    # 2b. Create LinkedIn Outreach Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_outreach (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_id TEXT,
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        profile_url TEXT,
        note TEXT,
        outcome TEXT,
        timestamp TEXT
    );
    """)
    
    # 2c. Create Tenant Config Table (multi-tenant support)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tenant_config (
        tenant_id       TEXT PRIMARY KEY,
        display_name    TEXT NOT NULL DEFAULT '',
        sending_email   TEXT NOT NULL DEFAULT '',
        sending_domain  TEXT NOT NULL DEFAULT '',
        calendar_link   TEXT NOT NULL DEFAULT '',
        signature_block TEXT NOT NULL DEFAULT '',
        value_doctrine  TEXT NOT NULL DEFAULT '',
        brand_voice     TEXT NOT NULL DEFAULT 'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
        icp_industries  TEXT NOT NULL DEFAULT '["Groothandel", "Logistiek & Transport", "B2B SaaS", "IT Dienstverlening"]',
        icp_roles       TEXT NOT NULL DEFAULT '["Directeur", "CEO", "Eigenaar", "Oprichter", "Managing Director"]',
        icp_company_size TEXT NOT NULL DEFAULT '10-200',
        search_queries  TEXT NOT NULL DEFAULT '[]',
        active          INTEGER NOT NULL DEFAULT 0
    );
    """)

    # Seed default tenant profiles if table is empty
    cursor.execute("SELECT COUNT(*) FROM tenant_config;")
    if cursor.fetchone()[0] == 0:
        now_iso = datetime.now(timezone.utc).isoformat()
        default_queries = json.dumps([
            "groothandel B2B Nederland",
            "logistiek dienstverlener Nederland",
            "transport bedrijf Nederland MKB",
            "IT consultancy Nederland MKB",
            "recruitment bureau Nederland MKB"
        ])
        cursor.executemany(
            """INSERT INTO tenant_config
               (tenant_id, display_name, sending_email, sending_domain, calendar_link,
                signature_block, value_doctrine, brand_voice, icp_industries,
                icp_roles, icp_company_size, search_queries, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    'injexion',
                    'Injexion',
                    '',  # user fills in via Settings tab
                    '',
                    '',
                    '',
                    '',  # user fills in Value Doctrine
                    'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
                    json.dumps(["Groothandel", "Logistiek & Transport", "B2B SaaS", "IT Dienstverlening"]),
                    json.dumps(["Directeur", "CEO", "Eigenaar", "Oprichter"]),
                    '10-200',
                    default_queries,
                    1  # active by default
                ),
                (
                    'clawbuildr',
                    'ClawBuildr',
                    '',
                    'clawbuildr.com',
                    'cal.com/clawbuildr',
                    'Marvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/',
                    'ClawBuildr bouwt AI-gestuurde automatiseringssystemen voor MKB-bedrijven. Onze producten: AI Email Assistenten, AI Telefoonassistenten, Workflow Automatisering (Zapier/Make), en CRM Integraties (HubSpot, AFAS, Teamleader). Wij helpen bedrijven repetitief handmatig werk te elimineren.',
                    'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
                    json.dumps(["Accountancy", "Tandartspraktijk", "Juridisch", "Vastgoed", "IT Dienstverlening"]),
                    json.dumps(["Directeur", "CEO", "Eigenaar", "Oprichter"]),
                    '5-50',
                    default_queries,
                    0
                )
            ]
        )
        logger.info("[Migrations] Seeded 2 default tenant profiles (Injexion, ClawBuildr).")

    # 2c. Create Tenant Config Table (multi-tenant support)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tenant_config (
        tenant_id       TEXT PRIMARY KEY,
        display_name    TEXT NOT NULL DEFAULT '',
        sending_email   TEXT NOT NULL DEFAULT '',
        sending_domain  TEXT NOT NULL DEFAULT '',
        calendar_link   TEXT NOT NULL DEFAULT '',
        signature_block TEXT NOT NULL DEFAULT '',
        value_doctrine  TEXT NOT NULL DEFAULT '',
        brand_voice     TEXT NOT NULL DEFAULT 'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
        icp_industries  TEXT NOT NULL DEFAULT '["Groothandel", "Logistiek & Transport", "B2B SaaS", "IT Dienstverlening"]',
        icp_roles       TEXT NOT NULL DEFAULT '["Directeur", "CEO", "Eigenaar", "Oprichter", "Managing Director"]',
        icp_company_size TEXT NOT NULL DEFAULT '10-200',
        search_queries  TEXT NOT NULL DEFAULT '[]',
        active          INTEGER NOT NULL DEFAULT 0
    );
    """)

    # Seed default tenant profiles if table is empty
    cursor.execute("SELECT COUNT(*) FROM tenant_config;")
    if cursor.fetchone()[0] == 0:
        import json
        now_iso = datetime.now(timezone.utc).isoformat()
        default_queries = json.dumps([
            "groothandel B2B Nederland",
            "logistiek dienstverlener Nederland",
            "transport bedrijf Nederland MKB",
            "IT consultancy Nederland MKB",
            "recruitment bureau Nederland MKB"
        ])
        cursor.executemany(
            """INSERT INTO tenant_config
               (tenant_id, display_name, sending_email, sending_domain, calendar_link,
                signature_block, value_doctrine, brand_voice, icp_industries,
                icp_roles, icp_company_size, search_queries, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    'injexion',
                    'Injexion',
                    '',  # user fills in via Settings tab
                    '',
                    '',
                    '',
                    '',  # user fills in Value Doctrine
                    'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
                    json.dumps(["Groothandel", "Logistiek & Transport", "B2B SaaS", "IT Dienstverlening"]),
                    json.dumps(["Directeur", "CEO", "Eigenaar", "Oprichter"]),
                    '10-200',
                    default_queries,
                    1  # active by default
                ),
                (
                    'clawbuildr',
                    'ClawBuildr',
                    '',
                    'clawbuildr.com',
                    'cal.com/clawbuildr',
                    'Marvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/',
                    'ClawBuildr bouwt AI-gestuurde automatiseringssystemen voor MKB-bedrijven. Onze producten: AI Email Assistenten, AI Telefoonassistenten, Workflow Automatisering (Zapier/Make), en CRM Integraties (HubSpot, AFAS, Teamleader). Wij helpen bedrijven repetitief handmatig werk te elimineren.',
                    'Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie).',
                    json.dumps(["Accountancy", "Tandartspraktijk", "Juridisch", "Vastgoed", "IT Dienstverlening"]),
                    json.dumps(["Directeur", "CEO", "Eigenaar", "Oprichter"]),
                    '5-50',
                    default_queries,
                    0
                )
            ]
        )
        logger.info("[Migrations] Seeded 2 default tenant profiles (Injexion, ClawBuildr).")

    # NEW: email_events table for tracking
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_events (
        event_id   TEXT PRIMARY KEY,
        email_id   TEXT NOT NULL,
        contact_id TEXT,
        event_type TEXT NOT NULL,
        metadata   TEXT DEFAULT '{}',
        created_at TEXT NOT NULL
    );
    """)

    # NEW: tracking columns on emails table
    for col, typedef in [
        ("opened_at", "TEXT"),
        ("opened_count", "INTEGER DEFAULT 0"),
        ("clicked_at", "TEXT"),
        ("clicked_links", "TEXT DEFAULT '[]'"),
        ("replied_at", "TEXT"),
        ("bounced_at", "TEXT"),
        ("bounce_reason", "TEXT"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE emails ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    # NEW: LinkedIn tracking columns
    for col, typedef in [
        ("accepted_at", "TEXT"),
        ("replied_at", "TEXT"),
        ("reply_body", "TEXT"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE linkedin_outreach ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    # NEW: strategies table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS strategies (
        strategy_id  TEXT PRIMARY KEY,
        tenant_id    TEXT NOT NULL,
        name         TEXT NOT NULL,
        description  TEXT,
        active       INTEGER DEFAULT 0,
        created_at   TEXT
    );
    """)

    # NEW: strategy_steps table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS strategy_steps (
        step_id      TEXT PRIMARY KEY,
        strategy_id  TEXT NOT NULL,
        step_number  INTEGER NOT NULL,
        step_type    TEXT NOT NULL,
        delay_days   INTEGER DEFAULT 0,
        template_id  TEXT,
        subject_line TEXT,
        stop_if      TEXT DEFAULT '["REPLIED","MEETING_BOOKED","OPT_OUT","BOUNCED","PENDING_APPROVAL"]'
    );
    """)

    # NEW: lead_sequences table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lead_sequences (
        lead_id        TEXT NOT NULL,
        strategy_id    TEXT NOT NULL,
        current_step   INTEGER DEFAULT 1,
        next_action_at TEXT,
        last_step_at   TEXT,
        status         TEXT DEFAULT 'ACTIVE',
        pause_reason   TEXT,
        enrolled_at    TEXT,
        PRIMARY KEY (lead_id, strategy_id)
    );
    """)

    # NEW: prefab_messages table with starter templates
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prefab_messages (
        template_id              TEXT PRIMARY KEY,
        tenant_id                TEXT,
        name                     TEXT NOT NULL,
        category                 TEXT,
        subject_line             TEXT,
        body                     TEXT NOT NULL,
        variables                TEXT DEFAULT '[]',
        language                 TEXT DEFAULT 'NL',
        performance_open_rate    REAL DEFAULT 0.0,
        performance_reply_rate   REAL DEFAULT 0.0,
        created_at               TEXT,
        updated_at               TEXT
    );
    """)

    # Seed starter Dutch templates
    cursor.execute("SELECT COUNT(*) FROM prefab_messages")
    if cursor.fetchone()[0] == 0:
        import uuid as _uuid
        now_iso = datetime.now(timezone.utc).isoformat()
        starter_templates = [
            (
                str(_uuid.uuid4()), None,
                "Directe Opening",
                "COLD",
                "{{company_name}} — korte vraag",
                """Hoi {{first_name}},

Ik zag dat {{company_name}} actief groeit in {{industry}}.

Wij helpen bedrijven zoals jullie met {{pain_point}} via {{value_prop}}.

Zou je open staan voor een korte call van 20 minuten?

👉 {{calendar_link}}

{{sender_name}}""",
                '["first_name","company_name","industry","pain_point","value_prop","calendar_link","sender_name"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
            (
                str(_uuid.uuid4()), None,
                "Follow-up: Oppakken",
                "FOLLOWUP",
                "Re: {{company_name}} — even oppakken?",
                """Hoi {{first_name}},

Ik had vorige week een bericht gestuurd — misschien ben je het vergeten.

Wij helpen {{industry}}-bedrijven zoals {{company_name}} specifiek met {{pain_point}}.

Heb je 20 minuten volgende week?
👉 {{calendar_link}}

{{sender_name}}""",
                '["first_name","company_name","industry","pain_point","calendar_link","sender_name"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
            (
                str(_uuid.uuid4()), None,
                "Finale Poging",
                "FOLLOWUP",
                "Laatste berichtje — {{first_name}}",
                """Hoi {{first_name}},

Ik wil je niet lastigvallen, maar dit is mijn laatste berichtje.

Als {{company_name}} ooit hulp nodig heeft met {{pain_point}}, weet je ons te vinden.

{{sender_name}}
{{value_prop}}""",
                '["first_name","company_name","pain_point","sender_name","value_prop"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
            (
                str(_uuid.uuid4()), None,
                "LinkedIn Connectie Note",
                "LINKEDIN_NOTE",
                None,
                """Hoi {{first_name}}, ik zag jullie werk bij {{company_name}} en wil graag in contact komen. Wij helpen {{industry}}-bedrijven met {{pain_point}}. — {{sender_name}}""",
                '["first_name","company_name","industry","pain_point","sender_name"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
        ]
        cursor.executemany("""
            INSERT INTO prefab_messages
            (template_id, tenant_id, name, category, subject_line, body, variables, language,
             performance_open_rate, performance_reply_rate, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, starter_templates)
        logger.info("[Migrations] Seeded 4 starter Dutch prefab templates.")

    # NEW: metric_targets table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS metric_targets (
        tenant_id          TEXT PRIMARY KEY,
        target_open_rate   REAL DEFAULT 0.30,
        target_reply_rate  REAL DEFAULT 0.05,
        target_meeting_rate REAL DEFAULT 0.01,
        target_bounce_rate REAL DEFAULT 0.02,
        target_li_accept_rate REAL DEFAULT 0.25,
        target_li_reply_rate  REAL DEFAULT 0.08,
        updated_at         TEXT
    );
    """)

    # 3. Add Serialized Agent Output Columns to contacts table if missing
    cursor.execute("PRAGMA table_info(contacts);")
    columns = [col[1] for col in cursor.fetchall()]
    
    new_columns = {
        "research_result": "TEXT",
        "deliverability_audit": "TEXT",
        "opportunity_mapping": "TEXT",
        "qualification_assessment": "TEXT",
        "outreach_draft": "TEXT",
        "inbox_assessment": "TEXT",
        "last_handoff": "TEXT"
    }
    
    for col_name, col_type in new_columns.items():
        if col_name not in columns:
            logger.info(f"Adding schema extension column '{col_name}' to 'contacts' table...")
            cursor.execute(f"ALTER TABLE contacts ADD COLUMN {col_name} {col_type};")
            
    # 4. Seed Initial Agent Rows
    cursor.execute("SELECT COUNT(*) FROM agent_status;")
    if cursor.fetchone()[0] == 0:
        agents = [
            ('Research Agent', 'IDLE', 'Awaiting new leads...', datetime.now(timezone.utc).isoformat()),
            ('Deliverability Agent', 'IDLE', 'Awaiting safety gate checks...', datetime.now(timezone.utc).isoformat()),
            ('Opportunity Mapping Agent', 'IDLE', 'Awaiting pain point matching...', datetime.now(timezone.utc).isoformat()),
            ('Qualification Agent', 'IDLE', 'Awaiting BANT/SPIN scoring...', datetime.now(timezone.utc).isoformat()),
            ('Outreach Agent', 'IDLE', 'Awaiting email generation...', datetime.now(timezone.utc).isoformat()),
            ('Inbox Management Agent', 'IDLE', 'Monitoring replies...', datetime.now(timezone.utc).isoformat())
        ]
        cursor.executemany("INSERT INTO agent_status (agent_name, status, last_action, last_active_at) VALUES (?, ?, ?, ?);", agents)
        
    # 5. Seed Initial Deliverability Stats
    cursor.execute("SELECT COUNT(*) FROM deliverability_stats;")
    if cursor.fetchone()[0] == 0:
        domains = [
            ('your-domain.com', 0, 0, 0, 0.0, 'HEALTHY', datetime.now(timezone.utc).isoformat()),
            ('outlook.com', 0, 0, 0, 0.0, 'HEALTHY', datetime.now(timezone.utc).isoformat())
        ]
        cursor.executemany("INSERT INTO deliverability_stats (domain, sends_count, replies_count, bounces_count, bounce_rate, domain_health, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?);", domains)
        
    conn.commit()
    
    # ONE-TIME RESET: Wipe all stale leads and start fresh (flag-based, runs only once)
    reset_flag_path = os.path.join(os.path.dirname(DB_PATH), ".reset_done")
    if not os.path.exists(reset_flag_path):
        cursor.execute("SELECT COUNT(*) FROM contacts;")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            logger.info(f"[RESET] Found {existing_count} stale leads. Wiping all data for fresh start...")
            cursor.execute("DELETE FROM contacts;")
            cursor.execute("DELETE FROM companies;")
            cursor.execute("DELETE FROM activity_log;")
            cursor.execute("DELETE FROM emails;")
            cursor.execute("DELETE FROM agent_actions;")
            cursor.execute("UPDATE agent_status SET status = 'IDLE', current_lead_id = NULL, error_message = NULL;")
            conn.commit()
            logger.info("[RESET] Database wiped. All tables clean for fresh pipeline run.")
        # Write flag so reset never runs again
        with open(reset_flag_path, "w") as f:
            f.write(f"Reset completed at {datetime.now(timezone.utc).isoformat()}")
        logger.info("[RESET] Flag written. Reset will not run again on future restarts.")
    
    conn.close()
    logger.info("Database schema extended and basic parameters verified.")





# =========================================================================
# 2. FASTAPI REAL-TIME SERVER-SENT EVENTS (SSE) BROADCASTER
# =========================================================================

class SSEBroadcaster:
    """Manages SSE subscribers and broadcasts JSON events in real-time."""
    def __init__(self):
        self._subscribers: List[asyncio.Queue] = []

    async def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self._subscribers.append(queue)
        logger.info(f"[SSE] New client subscribed. Total: {len(self._subscribers)}")
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        if queue in self._subscribers:
            self._subscribers.remove(queue)
            logger.info(f"[SSE] Client disconnected. Total: {len(self._subscribers)}")

    async def broadcast(self, event_type: str, data: Any):
        """Pushes a structured event to all active clients."""
        payload = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data
        }
        serialized = f"data: {json.dumps(payload)}\n\n"
        
        # Dispatch to all queues in parallel
        if self._subscribers:
            await asyncio.gather(
                *[queue.put(serialized) for queue in self._subscribers],
                return_exceptions=True
            )


sse_manager = SSEBroadcaster()

# Global database write lock — prevents concurrent SQLite writes between sourcing + pipeline
_DB_LOCK = asyncio.Lock()

# Dedicated thread pool for LinkedIn Selenium (Bug #6 fix: prevents thread pool starvation)
_LI_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="li_selenium")

def _get_active_tenant() -> dict:
    """Returns the currently active tenant config from the database.
    Falls back to a safe empty default if not configured yet."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM tenant_config WHERE active = 1 LIMIT 1").fetchone()
        conn.close()
        if row:
            t = dict(row)
            # Parse JSON fields
            for field in ('icp_industries', 'icp_roles', 'search_queries'):
                try:
                    t[field] = json.loads(t.get(field) or '[]')
                except Exception:
                    t[field] = []
            return t
    except Exception as e:
        logger.warning(f"[Tenant] Could not load tenant config: {e}")
    # Safe default (setup not done yet)
    return {
        'tenant_id': 'default',
        'display_name': 'My Company',
        'sending_email': '',
        'sending_domain': '',
        'calendar_link': '',
        'signature_block': '',
        'value_doctrine': '',
        'brand_voice': 'Professioneel, direct, vriendelijk.',
        'icp_industries': ["Groothandel", "Logistiek"],
        'icp_roles': ["Directeur", "Eigenaar"],
        'icp_company_size': '10-200',
        'search_queries': [
            "groothandel B2B Nederland",
            "logistiek dienstverlener Nederland",
            "transport bedrijf Nederland MKB",
        ],
        'active': 1
    }

# Dedicated thread pool for LinkedIn Selenium (Bug #6 fix: prevents thread pool starvation)
_LI_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="li_selenium")

def _get_active_tenant() -> dict:
    """Returns the currently active tenant config from the database.
    Falls back to a safe empty default if not configured yet."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM tenant_config WHERE active = 1 LIMIT 1").fetchone()
        conn.close()
        if row:
            t = dict(row)
            # Parse JSON fields
            for field in ('icp_industries', 'icp_roles', 'search_queries'):
                try:
                    t[field] = json.loads(t.get(field) or '[]')
                except Exception:
                    t[field] = []
            return t
    except Exception as e:
        logger.warning(f"[Tenant] Could not load tenant config: {e}")
    # Safe default (setup not done yet)
    return {
        'tenant_id': 'default',
        'display_name': 'My Company',
        'sending_email': '',
        'sending_domain': '',
        'calendar_link': '',
        'signature_block': '',
        'value_doctrine': '',
        'brand_voice': 'Professioneel, direct, vriendelijk.',
        'icp_industries': ["Groothandel", "Logistiek"],
        'icp_roles': ["Directeur", "Eigenaar"],
        'icp_company_size': '10-200',
        'search_queries': [
            "groothandel B2B Nederland",
            "logistiek dienstverlener Nederland",
            "transport bedrijf Nederland MKB",
        ],
        'active': 1
    }

# =========================================================================
# 3. INTER-AGENT EXECUTOR WRAPPER
# =========================================================================

async def global_log_audit(actor: str, action_type: str, details: str, contact_id: str = None):
    """Records structural audit actions in SQLite and logs to scrolling console."""
    async with _DB_LOCK:
        c = sqlite3.connect(DB_PATH, timeout=60.0)
        c.execute("PRAGMA journal_mode=WAL;")
        cur = c.cursor()
        cur.execute("""
        INSERT INTO activity_log (contact_id, actor, action_type, action_details)
        VALUES (?, ?, ?, ?);
        """, (contact_id, actor, action_type, details))
        c.commit()
        c.close()
    
    # Broadcast activity to console feed via SSE
    await sse_manager.broadcast("activity", {
        "contact_id": contact_id,
        "actor": actor,
        "action_type": action_type,
        "action_details": details
    })
    await asyncio.sleep(0.5)


async def run_pipeline_for_lead(lead_id: str):
    """Executes the full multi-agent pipeline sequentially on SQLite with live SSE broadcasts."""
    from linkedin_engine import search_and_connect as _li_search_and_connect
    from linkedin_engine import generate_linkedin_note as _li_generate_note
    logger.info(f"[Pipeline] Starting live replay for lead: {lead_id}")
    
    async def db_write(query: str, params: tuple = ()):
        async with _DB_LOCK:
            c = sqlite3.connect(DB_PATH, timeout=60.0)
            try:
                c.execute("PRAGMA journal_mode=WAL;")
                c.execute(query, params)
                c.commit()
            except Exception as e:
                logger.error(f"[db_write] Error executing query: {e}")
                raise e
            finally:
                c.close()

    # 1. Fetch Lead info (quick read connection)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("""
    SELECT c.first_name, c.last_name, c.email, c.role, co.name, co.domain, c.linkedin_url
    FROM contacts c
    LEFT JOIN companies co ON c.company_id = co.company_id
    WHERE c.contact_id = ?;
    """, (lead_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        logger.error(f"[Pipeline] Lead {lead_id} not found.")
        return
        
    first_name, last_name, email, role, company_name, domain, stored_li_url = row
    if not company_name:
        company_name = "Klimaat Tech MKB"
    if not domain:
        domain = "klimaattech.nl"
    if not role:
        role = "Eigenaar"
    if not stored_li_url:
        stored_li_url = ""
        
    async def update_agent_db_status(agent: str, status: str, action: str, error: str = None):
        """Helper to write agent status to SQLite and trigger real-time SSE refresh."""
        await db_write("""
        UPDATE agent_status 
        SET status = ?, current_lead_id = ?, last_active_at = ?, last_action = ?, error_message = ?
        WHERE agent_name = ?;
        """, (status, lead_id, datetime.now(timezone.utc).isoformat(), action, error, agent))
        
        # Broadcast agent change via SSE
        await sse_manager.broadcast("agent_status_change", {
            "agent_name": agent,
            "status": status,
            "current_lead_id": lead_id,
            "last_action": action,
            "error_message": error
        })

    async def log_handoff(sender: str, recipient: str, payload_dict: Dict[str, Any]):
        """Records agent communication in the database and broadcasts handoff event."""
        action_id = f"h_live_{uuid.uuid4().hex[:12]}"
        await db_write("""
        INSERT INTO agent_actions (action_id, sender_agent, recipient_agent, prospect_id, payload)
        VALUES (?, ?, ?, ?, ?);
        """, (action_id, sender, recipient, lead_id, json.dumps(payload_dict)))
        
        # Broadcast handoff via SSE
        await sse_manager.broadcast("agent_handoff", {
            "action_id": action_id,
            "sender_agent": sender,
            "recipient_agent": recipient,
            "prospect_id": lead_id,
            "payload": payload_dict
        })
        await asyncio.sleep(1.0) # Visual pause to let user trace

    async def log_audit(actor: str, action_type: str, details: str):
        await global_log_audit(actor, action_type, details, contact_id=lead_id)

    try:
        # Reset Lead to INGESTED in the database
        await db_write("""
        UPDATE contacts 
        SET current_stage = 'INGESTED', lead_score = 0, priority = 'NURTURE', meeting_ready = 0,
            research_result = NULL, deliverability_audit = NULL, opportunity_mapping = NULL,
            qualification_assessment = NULL, outreach_draft = NULL, inbox_assessment = NULL, last_handoff = NULL
        WHERE contact_id = ?;
        """, (lead_id,))
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "INGESTED"})
        
        await log_audit("SYSTEM", "RESET", f"Lead {first_name} {last_name} reset to INGESTED state. Triggering pipeline execution.")

        # Import the real Agent logic from clawbuildr_multi_agent
        _data_dir = os.path.dirname(os.path.abspath(__file__))
        if _data_dir not in sys.path:
            sys.path.insert(0, _data_dir)
        from clawbuildr_multi_agent import (
            ResearchAgent, DeliverabilityAgent, OpportunityMappingAgent,
            QualificationAgent, OutreachAgent
        )
        
        # Instantiate agents
        r_agent = ResearchAgent()
        d_agent = DeliverabilityAgent()
        o_agent = OpportunityMappingAgent()
        q_agent = QualificationAgent()
        out_agent = OutreachAgent()

        # ==========================================================
        # STAGE 1: INGESTION -> RESEARCH (Research Agent)
        # ==========================================================
        await update_agent_db_status("Research Agent", "RUNNING", f"Deep-scraping {domain}...")
        await log_handoff("CentralOrchestrator", "Research Agent", {"action": "deep_scrape_kvk_web", "domain": domain})
        
        research = await r_agent.analyze(company_name, domain)
        res_result = research.model_dump() if hasattr(research, "model_dump") else research.dict()
        
        await db_write("UPDATE contacts SET research_result = ?, current_stage = 'RESEARCHED' WHERE contact_id = ?;", (json.dumps(res_result), lead_id))
        
        await log_handoff("Research Agent", "CentralOrchestrator", {"status": "SUCCESS", "detected_pains": res_result.get("detected_pain_points", [])})
        await log_audit("Research Agent", "ANALYSIS", f"Real deep research complete for {company_name}. Pains: {', '.join(res_result.get('detected_pain_points', [])[:2])}")
        await update_agent_db_status("Research Agent", "IDLE", "Awaiting new leads...")
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "RESEARCHED"})

        # ==========================================================
        # STAGE 2: RESEARCHED -> DELIVERABILITY (Deliverability Agent)
        # ==========================================================
        await update_agent_db_status("Deliverability Agent", "RUNNING", f"Scoring trust & deliverability for {domain}...")
        await log_handoff("CentralOrchestrator", "Deliverability Agent", {"action": "trust_assessment", "email": email, "domain": domain})
        
        audit = d_agent.audit_prospect(email, domain)
        deliv_data = audit.model_dump() if hasattr(audit, "model_dump") else audit.dict()
        
        await db_write("UPDATE contacts SET deliverability_audit = ? WHERE contact_id = ?;", (json.dumps(deliv_data), lead_id))
        
        # If delivery audit says BLOCK or confidence < 40, we block the lead
        # BUT still try LinkedIn outreach if we have a stored URL
        if deliv_data.get("action_required") == "BLOCK" or deliv_data.get("confidence_score", 100) < 40:
            await db_write("UPDATE contacts SET current_stage = 'BLOCKED' WHERE contact_id = ?;", (lead_id,))
            await log_handoff("Deliverability Agent", "CentralOrchestrator", {"status": "BLOCKED", "score": deliv_data.get("confidence_score")})
            await log_audit("Deliverability Agent", "SAFETY_GATE", f"BLOCKED by deliverability gate (Score: {deliv_data.get('confidence_score')})")
            await update_agent_db_status("Deliverability Agent", "IDLE", "Awaiting safety gate checks...")
            await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "BLOCKED"})
            # Do NOT attempt LinkedIn for leads blocked before research — note would be generic and damage brand
            logger.info(f"[LinkedIn] Lead {lead_id} blocked at deliverability gate. Skipping LinkedIn (no research context yet).")
            return
        else:
            await db_write("UPDATE contacts SET current_stage = 'DELIVERABILITY_VERIFIED' WHERE contact_id = ?;", (lead_id,))
            await log_handoff("Deliverability Agent", "CentralOrchestrator", {"status": "PROCEED", "score": deliv_data.get("confidence_score")})
            await log_audit("Deliverability Agent", "VERIFICATION", f"Deliverability score {deliv_data.get('confidence_score')}/100. Gate passed.")
            await update_agent_db_status("Deliverability Agent", "IDLE", "Awaiting safety gate checks...")
            await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "DELIVERABILITY_VERIFIED"})

        # ==========================================================
        # STAGE 3: DELIVERABILITY_VERIFIED -> OPPORTUNITY (Opportunity Agent)
        # ==========================================================
        await update_agent_db_status("Opportunity Mapping Agent", "RUNNING", "Mapping pain points to ClawBuildr solutions...")
        await log_handoff("CentralOrchestrator", "Opportunity Mapping Agent", {"action": "catalog_matching", "research": res_result})
        
        opp = await o_agent.map_opportunities(research)
        opp_data = opp.model_dump() if hasattr(opp, "model_dump") else opp.dict()
        
        await db_write("UPDATE contacts SET opportunity_mapping = ?, current_stage = 'OPPORTUNITY_MAPPED' WHERE contact_id = ?;", (json.dumps(opp_data), lead_id))
        await log_handoff("Opportunity Mapping Agent", "CentralOrchestrator", {"status": "SUCCESS", "matched_solutions": opp_data.get("selected_solutions", [])})
        await log_audit("Opportunity Mapping Agent", "MAPPING", f"Solutions: {', '.join(opp_data.get('selected_solutions', []))}. Score: {opp_data.get('opportunity_score')}/10.")
        await update_agent_db_status("Opportunity Mapping Agent", "IDLE", "Awaiting pain point matching...")
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "OPPORTUNITY_MAPPED"})

        # ==========================================================
        # STAGE 4: OPPORTUNITY_MAPPED -> QUALIFICATION (Qualification Agent)
        # ==========================================================
        await update_agent_db_status("Qualification Agent", "RUNNING", "Running BANT qualification...")
        await log_handoff("CentralOrchestrator", "Qualification Agent", {"action": "bant_scoring", "solutions": opp_data.get("selected_solutions", [])})
        
        role_title = role if role and role.strip() else "Eigenaar"
        qual = await q_agent.qualify(research, opp, role_title)
        qual_data = qual.model_dump() if hasattr(qual, "model_dump") else qual.dict()
        
        total = qual_data.get("lead_score", 0)
        priority = qual_data.get("priority", "NURTURE")
        
        await db_write("""
        UPDATE contacts 
        SET qualification_assessment = ?, lead_score = ?, priority = ?, current_stage = 'PRE_QUALIFIED' 
        WHERE contact_id = ?;
        """, (json.dumps(qual_data), total, priority, lead_id))
        await log_handoff("Qualification Agent", "CentralOrchestrator", {"status": "SUCCESS", "bant_score": total})
        await log_audit("Qualification Agent", "QUALIFICATION", f"BANT score: {total}/100. Priority: {priority}.")
        await update_agent_db_status("Qualification Agent", "IDLE", "Awaiting BANT/SPIN scoring...")
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "PRE_QUALIFIED"})

        # ==========================================================
        # STAGE 5b: AUTOMATIC LINKEDIN OUTREACH (runs for ALL qualified leads)
        # ==========================================================
        await log_audit("LinkedIn Agent", "LINKEDIN_STAGE", f"Stage 5b reached for {first_name} {last_name}. Score: {total}. LinkedIn URL: {stored_li_url[:50] if stored_li_url else 'NONE'}")
        try:
            li_note = _li_generate_note(f"Ik zag uw bedrijf online en wil graag in contact komen.", first_name, company_name)
            
            if stored_li_url:
                await log_audit("LinkedIn Agent", "LINKEDIN_SEARCH", f"Using stored LinkedIn URL for {first_name} {last_name}: {stored_li_url}")
            else:
                await log_audit("LinkedIn Agent", "LINKEDIN_SEARCH", f"No LinkedIn URL stored for {first_name} {last_name} at {company_name}")

            loop = asyncio.get_event_loop()
            li_result = await loop.run_in_executor(
                _LI_EXECUTOR,
                lambda: _li_search_and_connect(first_name, last_name or "", company_name or "", li_note, contact_id=lead_id, profile_url=stored_li_url)
            )

            li_outcome = li_result.get("outcome", "ERROR")
            await log_audit("LinkedIn Agent", "LINKEDIN_RESULT", f"LinkedIn outreach for {first_name} {last_name}: {li_outcome}. Note: {li_result.get('note', '')[:60]}...")
            await sse_manager.broadcast("activity", {
                "contact_id": lead_id,
                "actor": "LinkedIn Agent",
                "action_type": "LINKEDIN_CONNECT",
                "action_details": f"Connection request sent to {first_name} {last_name} on LinkedIn. Outcome: {li_outcome}"
            })
            await sse_manager.broadcast("notify", {
                "type": "info" if li_outcome == "SUCCESS" else "warning",
                "message": f"LinkedIn: {li_outcome} for {first_name} {last_name}"
            })
        except Exception as li_ex:
            import traceback
            logger.warning(f"[LinkedIn] Auto-outreach failed for {lead_id}: {li_ex}")
            logger.warning(f"[LinkedIn] Traceback: {traceback.format_exc()}")
            await log_audit("LinkedIn Agent", "LINKEDIN_ERROR", f"LinkedIn outreach failed: {str(li_ex)[:80]}")

        # ==========================================================
        # STAGE 6: OUTREACH (email only for hot leads >= 75)
        # ==========================================================
        if total < 75:
            await db_write("UPDATE contacts SET current_stage = 'NURTURE' WHERE contact_id = ?;", (lead_id,))
            await log_audit("CentralOrchestrator", "SKIPPED", f"Lead {company_name} score is not HOT ({total}/100). LinkedIn attempted. Moving to NURTURE.")
            await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "NURTURE"})
            return

        await update_agent_db_status("Outreach Agent", "RUNNING", "Drafting personalized Dutch outreach email...")
        await log_handoff("CentralOrchestrator", "Outreach Agent", {"action": "generate_dutch_outreach", "first_name": first_name})
        
        draft = await out_agent.draft_outreach(research, opp, first_name)
        draft_data = draft.model_dump() if hasattr(draft, "model_dump") else draft.dict()
        
        await db_write("UPDATE contacts SET outreach_draft = ?, current_stage = 'OUTREACH_DRAFTED' WHERE contact_id = ?;", (json.dumps(draft_data), lead_id))
        await log_handoff("Outreach Agent", "CentralOrchestrator", {"status": "SUCCESS", "subject": draft_data.get("subject_line_A")})
        await log_audit("Outreach Agent", "GENERATION", f"Dutch email drafted for {first_name}: {draft_data.get('subject_line_A')}")
        await update_agent_db_status("Outreach Agent", "IDLE", "Awaiting email generation...")
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "OUTREACH_DRAFTED"})

        # ==========================================================
        # STAGE 6b: PENDING_APPROVAL (for hot leads with email)
        # ==========================================================
        await db_write("UPDATE contacts SET current_stage = 'PENDING_APPROVAL' WHERE contact_id = ?;", (lead_id,))
        await log_audit("CentralOrchestrator", "ESCALATION", "Outbound email campaign escalated to Human-in-the-Loop review gate. Halting execution.")
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "PENDING_APPROVAL"})
        
        # Flash visual alert that approval is waiting
        await sse_manager.broadcast("notify", {
            "type": "warning", 
            "message": f"Lead {first_name} {last_name} requires Human Review before outreach send!"
        })


    except Exception as ex:
        logger.error(f"[Pipeline] Fatal crash during pipeline execution: {ex}")
        import traceback
        traceback.print_exc()
        try:
            await log_audit("SYSTEM", "CRITICAL_ERROR", f"Orchestrator pipeline failed: {str(ex)}")
            await update_agent_db_status("Research Agent", "FAILED", "Pipeline processing crashed", str(ex))
        except Exception:
            pass  # Don't let audit logging mask the original error



# =========================================================================
# BUG 3 FIX: Reply Detection Loop — polls Gmail for replies every 15 min
# =========================================================================

async def reply_detection_loop():
    """Polls Gmail API to detect when a prospect replies to a sent email.
    Runs every 15 minutes. Logs REPLIED event and pauses the lead's sequence."""
    logger.info("[Reply Detection] Loop started — checking Gmail threads every 15 min.")
    await asyncio.sleep(60)  # initial delay: let server fully boot first

    while True:
        try:
            DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            tenant = _get_active_tenant()
            tenant_id = tenant.get("tenant_id", "default")
            creds_path = os.path.join(DATA_DIR, "client_secret.json")
            token_path = os.path.join(DATA_DIR, f"token_gmail_{tenant_id}.json")
            if not os.path.exists(token_path):
                token_path = os.path.join(DATA_DIR, "token_gmail.json")

            if not (os.path.exists(creds_path) and os.path.exists(token_path)):
                await asyncio.sleep(15 * 60)
                continue

            from clawbuildr_gmail import ClawBuildrGmailConnector
            connector = ClawBuildrGmailConnector(credentials_path=creds_path, token_path=token_path)
            if not connector.authenticate():
                await asyncio.sleep(15 * 60)
                continue

            # Load all sent emails that haven't had a reply yet
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            conn.row_factory = sqlite3.Row
            sent_emails = conn.execute("""
                SELECT email_id, contact_id, thread_id, message_id FROM emails
                WHERE direction = 'OUTBOUND' AND status = 'SENT' AND replied_at IS NULL
                AND thread_id IS NOT NULL AND thread_id != ''
                LIMIT 50
            """).fetchall()
            conn.close()

            reply_count = 0
            for row in sent_emails:
                try:
                    # Check if the thread has more than 1 message (i.e., someone replied)
                    thread_data = connector.service.users().threads().get(
                        userId='me', id=row["thread_id"], format='minimal'
                    ).execute()
                    messages_in_thread = thread_data.get("messages", [])
                    if len(messages_in_thread) > 1:
                        # New reply detected!
                        now = datetime.now(timezone.utc).isoformat()
                        async with _DB_LOCK:
                            c2 = sqlite3.connect(DB_PATH, timeout=10.0)
                            c2.execute("UPDATE emails SET replied_at = ? WHERE email_id = ?", (now, row["email_id"]))
                            # Log event
                            c2.execute("""
                                INSERT OR IGNORE INTO email_events
                                (event_id, email_id, contact_id, event_type, metadata, created_at)
                                VALUES (?, ?, ?, 'REPLIED', '{}', ?)
                            """, (str(uuid.uuid4()), row["email_id"], row["contact_id"], now))
                            # Update lead stage
                            c2.execute("""
                                UPDATE contacts SET current_stage = 'REPLIED', updated_at = ?
                                WHERE contact_id = ? AND current_stage NOT IN ('MEETING_BOOKED', 'CLOSED_WON', 'CLOSED_LOST')
                            """, (now, row["contact_id"]))
                            c2.commit()
                            c2.close()

                        # Pause their sequence so no more follow-up emails go out
                        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr"))
                        try:
                            from strategy_engine import pause_sequence
                            pause_sequence(row["contact_id"], "REPLIED")
                        except ImportError:
                            pass

                        await sse_manager.broadcast("lead_refresh", {"contact_id": row["contact_id"], "current_stage": "REPLIED"})
                        logger.info(f"[Reply Detection] REPLY detected for lead {row['contact_id']} (thread: {row['thread_id']})")
                        reply_count += 1
                except Exception as ex:
                    logger.debug(f"[Reply Detection] Thread check error for {row['thread_id']}: {ex}")

            if reply_count:
                logger.info(f"[Reply Detection] Found {reply_count} new replies this cycle.")
        except Exception as e:
            logger.error(f"[Reply Detection] Loop error: {e}")

        await asyncio.sleep(15 * 60)  # check every 15 minutes


# =========================================================================
# BUG 4 FIX: Strategy Sequence Loop — advances leads through steps
# BUG 8 FIX: Also writes linkedin accepted_at from accepted connection data
# =========================================================================

async def strategy_sequence_loop():
    """Checks every 5 minutes for leads whose next sequence step is due, and fires it.
    Also marks LinkedIn connections as accepted when check_pending_connections reports them."""
    logger.info("[Strategy Engine] Sequence loop started — checking for due steps every 5 min.")
    await asyncio.sleep(90)  # let other loops start first

    while True:
        try:
            # Import strategy engine (relative to clawbuildr subdir)
            _cb_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr")
            if _cb_dir not in sys.path:
                sys.path.insert(0, _cb_dir)

            from strategy_engine import get_leads_due_for_next_step, advance_sequence, get_strategy_steps

            due_leads = get_leads_due_for_next_step()
            if due_leads:
                logger.info(f"[Strategy Engine] {len(due_leads)} lead(s) due for next step.")

            for seq in due_leads:
                lead_id = seq["lead_id"]
                strategy_id = seq["strategy_id"]
                step = advance_sequence(lead_id, strategy_id)
                if step:
                    step_type = step.get("step_type", "EMAIL")
                    logger.info(f"[Strategy Engine] Lead {lead_id}: executing step {step.get('step_number')} ({step_type})")
                    if step_type == "EMAIL":
                        # If step > 1, just send the prefab template directly (don't reset pipeline)
                        if step.get("step_number", 1) > 1 and step.get("template_id"):
                            c_temp = sqlite3.connect(DB_PATH, timeout=5.0)
                            c_temp.row_factory = sqlite3.Row
                            pm = c_temp.execute("SELECT * FROM prefab_messages WHERE template_id = ?", (step["template_id"],)).fetchone()
                            ct = c_temp.execute("SELECT ct.first_name, cp.name as company_name, ct.email FROM contacts ct JOIN companies cp ON ct.company_id = cp.company_id WHERE contact_id = ?", (lead_id,)).fetchone()
                            tenant = _get_active_tenant()
                            c_temp.close()
                            
                            if pm and ct:
                                from clawbuildr_gmail import ClawBuildrGmailConnector
                                creds_path = os.path.join(DATA_DIR, "client_secret.json")
                                token_path = os.path.join(DATA_DIR, f"token_gmail_{tenant.get('tenant_id', 'default')}.json")
                                if not os.path.exists(token_path):
                                    token_path = os.path.join(DATA_DIR, "token_gmail.json")
                                
                                if os.path.exists(creds_path) and os.path.exists(token_path):
                                    conn_obj = ClawBuildrGmailConnector(creds_path, token_path)
                                    if conn_obj.authenticate():
                                        # Render template
                                        import re
                                        body = pm["body"]
                                        subj = pm["subject_line"] or "Vraag"
                                        body = body.replace("{{first_name}}", ct["first_name"] or "relatie")
                                        body = body.replace("{{company_name}}", ct["company_name"] or "")
                                        body = body.replace("{{sender_name}}", "Marvin")
                                        body = body.replace("{{calendar_link}}", tenant.get("calendar_link", "https://cal.com"))
                                        body = body.replace("{{pain_point}}", "dit proces")
                                        subj = subj.replace("{{company_name}}", ct["company_name"] or "")
                                        subj = subj.replace("{{first_name}}", ct["first_name"] or "")
                                        
                                        email_id_pre = f"em_{random.randint(100000, 999999)}"
                                        html_body = "<html><body><pre style='font-family:inherit;white-space:pre-wrap'>" + body.replace("<","&lt;") + "</pre></body></html>"
                                        html_body_tracked = _inject_tracking(email_id_pre, html_body)
                                        res = conn_obj.send_email(to_email=ct["email"], subject=subj, body_text=body, body_html=html_body_tracked)
                                        if res.get("status") == "SENT":
                                            c_log = sqlite3.connect(DB_PATH, timeout=5.0)
                                            c_log.execute("""
                                                INSERT INTO emails (email_id, contact_id, direction, subject, body, message_id, thread_id, status, sent_at)
                                                VALUES (?, ?, 'OUTBOUND', ?, ?, ?, ?, 'SENT', ?)
                                            """, (email_id_pre, lead_id, subj, body, res.get("message_id"), res.get("thread_id"), datetime.now(timezone.utc).isoformat()))
                                            c_log.commit()
                                            c_log.close()
                                            logger.info(f"[Strategy Engine] Follow-up sent to {ct['email']}")
                        else:
                            # Step 1: Queue for pipeline processing
                            asyncio.create_task(run_pipeline_for_lead(lead_id))
                    elif step_type == "LINKEDIN":
                        # LinkedIn step — let the linkedin_auto_loop handle it in next cycle
                        pass

            # Bug 8 fix: update linkedin_outreach.accepted_at for newly accepted connections
            try:
                conn_li = sqlite3.connect(DB_PATH, timeout=10.0)
                conn_li.row_factory = sqlite3.Row
                # Find connections that were sent but not yet marked accepted
                pending_li = conn_li.execute("""
                    SELECT lo.outreach_id, lo.contact_id, lo.profile_url
                    FROM linkedin_outreach lo
                    WHERE lo.accepted_at IS NULL
                    AND lo.status = 'CONNECTED'
                    LIMIT 20
                """).fetchall()
                conn_li.close()

                if pending_li:
                    now_li = datetime.now(timezone.utc).isoformat()
                    c3 = sqlite3.connect(DB_PATH, timeout=10.0)
                    for li_row in pending_li:
                        c3.execute(
                            "UPDATE linkedin_outreach SET accepted_at = ? WHERE outreach_id = ?",
                            (now_li, li_row["outreach_id"])
                        )
                    c3.commit()
                    c3.close()
                    logger.info(f"[Strategy Engine] Marked {len(pending_li)} LinkedIn connections as accepted.")
            except Exception as li_ex:
                logger.debug(f"[Strategy Engine] LinkedIn accept sync error: {li_ex}")

        except ImportError:
            logger.debug("[Strategy Engine] strategy_engine module not yet available — skipping.")
        except Exception as e:
            logger.error(f"[Strategy Engine] Loop error: {e}")

        await asyncio.sleep(5 * 60)  # every 5 minutes


# =========================================================================
# 4. FASTAPI APP ROUTING & ENDPOINTS
# =========================================================================

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Initializes the database schema and starts the background agent loop."""
    run_migrations()
    # Spawn background tasks
    task1 = asyncio.create_task(lead_sourcing_agent())
    task2 = asyncio.create_task(agent_loop())
    task3 = asyncio.create_task(linkedin_auto_loop())
    task4 = asyncio.create_task(reply_detection_loop())       # Bug 3 fix
    task5 = asyncio.create_task(strategy_sequence_loop())     # Bug 4 fix
    # Run one immediate sourcing cycle for demo (then agent respects business hours)
    asyncio.create_task(immediate_sourcing_cycle())
    logger.info("[Startup] Lead sourcing agent + pipeline agent loop + LinkedIn auto-scheduling + reply detection + strategy engine started.")
    yield
    # Shutdown: cancel background tasks
    task1.cancel()
    task2.cancel()
    task3.cancel()
    task4.cancel()
    task5.cancel()
    logger.info("[Shutdown] Background tasks cancelled.")

app = FastAPI(title="ClawBuildr Mission Control Dashboard", lifespan=lifespan)

async def immediate_sourcing_cycle():
    """Run one sourcing cycle immediately on startup for testing, then exits."""
    await asyncio.sleep(5)
    logger.info("[Startup] Running one immediate lead sourcing cycle...")
    try:
        await run_sourcing_cycle()
        logger.info("[Startup] Immediate sourcing cycle complete.")
    except Exception as e:
        logger.error(f"[Startup] Immediate sourcing cycle error: {e}")

# =========================================================================
#  AGENT 0: Lead Sourcing Agent — finds prospects and feeds the pipeline
# =========================================================================

_LEAD_SOURCE_QUERIES = [
    # High-quality B2B & professional services (Dutch)
    "middelgroot advocatenkantoor Nederland",
    "groot accountantskantoor Nederland",
    "recruitment bureau Nederland MKB",
    "werving en selectie bureau Nederland",
    "uitzendbureau Nederland MKB",
    "vastgoedbeheerder Nederland",
    "bedrijfsmakelaar Nederland",
    "corporate finance adviseur Nederland",
    "verzekeringsmakelaar zakelijk Nederland",
    "hypotheekadviseur Nederland",
    "IT consultancy Nederland",
    "B2B SaaS bedrijf Nederland",
    "groothandel B2B Nederland",
    "logistiek dienstverlener Nederland",
    "private kliniek Nederland",
    "online marketing bureau Nederland",
    "notariskantoor Nederland",
    
    # High-quality B2B (German)
    "Steuerberatungsgesellschaft Deutschland",
    "Wirtschaftsprüfer Deutschland",
    "Personalberatung Deutschland",
    "Immobilienverwaltung Deutschland",
    "IT Systemhaus Deutschland",
    
    # High-quality B2B (Belgian)
    "bedrijfsrevisor Belgie",
    "HR consultancy Belgie",
    "IT dienstverlener KMO Belgie",
    
    # More Dutch variety
    "fysiotherapie praktijk Nederland",
    "tandartspraktijk Nederland",
    "dierenartspraktijk Nederland",
    "architectenbureau Nederland",
    "webdesign bureau Nederland",
    "fotograaf bedrijf Nederland",
    "evenementenbureau Nederland",
    "drukkerij Nederland",
    "schoonheidssalon Nederland",
    "kapsalon Nederland",
    "interieurarchitect Nederland",
    "coachingsbureau Nederland",
    
    # More German
    "Rechtsanwalt Deutschland",
    "Steuerberater Deutschland",
    "Architektur Büro Deutschland",
    "IT Dienstleister Deutschland",
    
    # More Belgian
    "boekhouder Belgie",
    "kinesitherapeut Belgie",
    "apotheker Belgie",
]

_MAX_LEADS_SOURCED = 50
_SOURCE_CYCLE_MINUTES = 5

# Domains to skip (informational, government, portals)
_SKIP_DOMAINS = {
    # Government / informational
    "rijksoverheid.nl", "overheid.nl", "mkb.nl", "kvk.nl", "nederlanddigitaal.nl",
    "informatiegidsen-nederland.nl", "searchlab.nl", "exclusiefnetwerknederland.nl",
    "strato.nl", "mkbdigitaal.nl", "mkb-digitalisering.nl",
    # Social / news / big platforms
    "google.com", "youtube.com", "facebook.com", "linkedin.com", "wikipedia.org",
    "reddit.com", "tweakers.net", "nu.nl", "nos.nl", "ad.nl", "telegraaf.nl",
    "fd.nl", "instagram.com", "twitter.com", "x.com", "tiktok.com", "pinterest.com",
    # Search engines (should not be scraped as companies)
    "mojeek.com", "brave.com", "duckduckgo.com", "bing.com", "startpage.com",
    # Directory / aggregator / listing sites (not actual companies)
    "yoys.nl", "f6s.com", "europages.nl", "itselector.nl", "ensun.io",
    "clutch.co", "sortlist.com", "sortlist.nl", "themanifest.com",
    "konsyg.com", "msp-vergelijker.nl", "springest.nl", "edx.org",
    "coursera.org", "udemy.com", "g2.com", "trustpilot.com",
    "drillster.nl", "allerhanden.nl",
    # Article / news aggregators (not companies)
    "computable.nl", "consultancy.nl", "mt.nl", "crmonline.nl",
    "channelconnect.nl", "it-channel.nl", "computable.be",
    "agconnect.nl", "executivepeople.nl", "hrpraktijk.nl",
    "accountancyvanmorgen.nl", "taxence.nl",
    # Dutch/German/BE directory & aggregator sites
    "kappers.nl", "treatwell.nl", "salonkee.nl", "kapper.nl",
    "zorgkaartnederland.nl", "fysiotherapie-praktijken.nl", "vindfysio.nl",
    "fysio.nl", "zorgkaart.nl", "zorg.nl", "thuisarts.nl",
    "zoekeenadvocaat.advocatenorde.nl", "legalscope.nl", "advocatenkantoren.nl",
    "advocaten.nl", "rechtswijzer.nl", "juridischloket.nl",
    "funda.nl", "aanbod.vastgoednederland.nl", "pararius.nl",
    "huislijn.nl", "jaap.nl", "makelaar.nl", "vastgoednederland.nl",
    "beautymap.nl", "beautytreat.nl", "wellnessgids.nl",
    "dierenkliniek.nl", "diergeneeskunde.nl", "anicura.nl",
    "trustoo.nl", "cateringgroep.nl", "partyverhuur.nl",
    "detelegraaf.nl", "volkskrant.nl", "parool.nl", "trouw.nl",
    "marktplaats.nl", "speurders.nl", "2dehands.be",
    "indeed.nl", "monsterboard.nl", "nationalevacaturebank.nl",
    "intermediair.nl", "randstad.nl", "tempo-team.nl",
    "vergelijken.nl", "independer.nl", "pricewise.nl", "geldvergelijken.nl",
    "studentenwerk.nl", "afstudeerkring.nl",
    # German directories
    "golocal.de", "dasoertliche.de", "gelbeseiten.de", "gelbesetten.de",
    "011be.de", "123recht.de", "anwalt.de", "steuerberater.de",
    "architekten.de", "fitness.eu", "fitnessclub.de",
    # Belgian directories
    "goudengids.be", "telenet.be", "page.be", "sprl.be",
}

def _is_business_hours() -> bool:
    """Check if current time is 09:00-18:00 CET on weekdays."""
    now = datetime.now(timezone.utc)
    # CET = UTC+1 in winter, UTC+2 in summer (DST). Approximate: check 08:00-17:00 UTC
    weekday = now.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False
    hour = now.hour
    # 08:00-17:00 UTC covers 09:00-18:00 CET winter and 10:00-19:00 CEST summer
    return 8 <= hour < 17

async def _search_web(query: str) -> list:
    """Search multiple engines and return list of {title, url, domain} dicts.
    Tries Brave first, falls back to Mojeek on 429.
    """
    # Try Brave first (3 attempts with backoff)
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"}) as client:
                r = await client.get("https://search.brave.com/search", params={"q": query})
                if r.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.info(f"[Lead Sourcing] Brave 429, waiting {wait}s (attempt {attempt+1}/3)")
                    await asyncio.sleep(wait)
                    continue
                if r.status_code != 200:
                    break
                html = r.text
                break
        except Exception as e:
            logger.warning(f"[Lead Sourcing] Brave search error: {e}")
            await asyncio.sleep(5)
            continue
    else:
        # All Brave attempts failed, try Mojeek
        html = None
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"}) as client:
                r = await client.get("https://www.mojeek.com/search", params={"q": query})
                if r.status_code == 200:
                    html = r.text
                    logger.info(f"[Lead Sourcing] Using Mojeek fallback for: {query[:50]}")
        except Exception as e:
            logger.warning(f"[Lead Sourcing] Mojeek fallback also failed: {e}")
    
    if not html:
        return []

    results = []
    for m in re.finditer(r'<a[^>]*href=["\'](https?://[^"\']+)["\'][^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE):
        href = m.group(1).strip()
        title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        if not title or len(title) < 5:
            continue
        parsed = urllib.parse.urlparse(href)
        domain = parsed.netloc.lower().replace('www.', '')
        if any(skip in domain for skip in _SKIP_DOMAINS):
            continue
        if not domain or '.' not in domain:
            continue
        tld = domain.rsplit('.', 1)[-1]
        if tld not in ('nl', 'be', 'de', 'eu', 'com', 'net', 'io', 'org', 'co'):
            continue
        if len(domain) > 40:
            continue
        results.append({"title": title[:120], "url": f"https://{domain}", "domain": domain, "query": query})
        if len(results) >= 5:
            break
    return results

def _results_to_tld(results: dict) -> str:
    """Get readable TLD/region for logging."""
    tld = results.get("domain", "").rsplit('.', 1)[-1] if '.' in results.get("domain", "") else ""
    return {"nl": "NL", "be": "BE", "de": "DE", "eu": "EU"}.get(tld, tld.upper())

_SCRAPE_SEMAPHORE = asyncio.Semaphore(5)

_GENERIC_EMAIL_PREFIXES = {"info", "contact", "hello", "hi", "support", "sales", "admin",
                           "webmaster", "noreply", "no-reply", "mail", "office", "team",
                           "enquiries", "help", "service", "careers", "jobs", "hr",
                           "marketing", "pr", "social", "partners", "media", "press",
                           "customerservice", "customersupport", "orders", "billing",
                           "accounts", "bookings", "reservations", "postmaster", "hostmaster",
                           "bi", "dpo", "payments", "accounting", "finance", "legal",
                           "reception", "secretariat", "secretaris", "info_nl", "info_be"}

def _extract_personal_name(emails: list, scraped_text: str, domain: str) -> tuple:
    """Extract a real person's name from emails or scraped text.
    Returns (first_name, last_name, role) or generic fallback."""
    
    # Strategy 1: Check email prefix for personal name
    personal_prefix = None
    for email in emails:
        prefix = email.split("@")[0].lower()
        if prefix not in _GENERIC_EMAIL_PREFIXES and not prefix.startswith("info"):
            # Could be a name like "marvin", "j.vandergraaf", "piet.jansen"
            personal_prefix = prefix
            break
    
    # Strategy 2: Search scraped text for name+role patterns
    name_mentions = []
    
    # Word pattern: STRICTLY uppercase-starting word OR Dutch name particle (no IGNORECASE for name)
    _cap_word = r'[A-Z][a-z]+'
    _nw = r'(?:' + _cap_word + r'|de|van|der|ten|ter|het)'
    # Role keywords with both case variants (no IGNORECASE needed on name captures)
    _role_kw = r'[Ee]igenaar|[Dd]irecteur|[Cc]eo|[Ff]ounder|[Oo]prichter|[Mm]anager|[Ee]igenaresse|[Mm]edewerker'
    
    for line in scraped_text.split('\n'):
        patterns = [
            # "Eigenaar: Wouter de Vries" or "CEO: John Smith"
            r'(?:[Nn]aam|[Cc]ontact|' + _role_kw + r')\s*[:\-–]\s+(' + _nw + r'(?:[ \t]+' + _nw + r'){1,4})',
            # "Wouter de Vries - Eigenaar"
            r'(' + _nw + r'(?:[ \t]+' + _nw + r'){1,4})\s*[–\-]\s*(?:' + _role_kw + r')',
            # "Maak kennis met Henk Henckens" or "team: Alice Wonderland"
            r'(?:[Ii]k ben|[Ww]ij zijn|[Mm]aak kennis met|[Mm]eet|[Oo]ntmoet|[Tt]eam|[Oo]nze mensen|[Oo]ns team)\s+(' + _nw + r'(?:[ \t]+' + _nw + r'){1,4})',
        ]
        for pat in patterns:
            m = re.search(pat, line)
            if m:
                name_mentions.append(m.group(1).strip())
    
    # Strategy 3: Parse email prefix as potential name
    if personal_prefix:
        # Convert "j.vandergraaf" -> "J. van der Graaf"
        # Convert "marvin" -> "Marvin"
        # Convert "piet.jansen" -> "Piet Jansen"
        parts = re.split(r'[._\-]', personal_prefix)
        if len(parts) >= 2:
            # Looks like "piet.jansen" or "j.vandergraaf"
            if len(parts[0]) == 1 and len(parts) >= 3:
                # "j.vander.graaf" or "j.van.der.graaf"
                # Treat first as initial, rest as last name parts
                first_name = parts[0].upper() + "."  # "J."
                last_name = " ".join(p.capitalize() for p in parts[1:])
                return (first_name, last_name, "Contactpersoon")
            else:
                first_name = parts[0].capitalize()
                last_name = " ".join(p.capitalize() for p in parts[1:])
                return (first_name, last_name, "Contactpersoon")
        elif parts[0] and parts[0] not in _GENERIC_EMAIL_PREFIXES:
            first_name = parts[0].capitalize()
            return (first_name, "", "Contactpersoon")
    
    # Use scraped name mentions as priority fallback
    if name_mentions:
        cleaned = []
        for m in name_mentions:
            c = m.strip()
            if not c:
                continue
            words = c.split()
            noise = {'is', 'de', 'onze', '-', '–', 'eigenaar', 'directeur', 'ceo', 'founder', 'oprichter', 'manager'}
            while words and words[-1].lower() in noise:
                words.pop()
            c = ' '.join(words)
            if not c:
                continue
            # Substring dedup: skip if c is already contained in cleaned
            if any(c in existing or existing in c for existing in cleaned):
                continue
            cleaned.append(c)
        if cleaned:
            # Pick the shortest clean name
            best = min(cleaned, key=len)
            parts = best.split()
            if len(parts) >= 2:
                return (parts[0], " ".join(parts[1:]), "Contactpersoon")
            if len(parts) == 1:
                return (parts[0], "", "Contactpersoon")
    
    return ("Contact", "", "Contactpersoon")


async def _find_linkedin_for_company(company_name: str, domain: str) -> dict:
    """Find a decision-maker's LinkedIn profile.
    Step 1: Scrape company website for team member names.
    Step 2: Search LinkedIn for that person.
    Returns {"linkedin_url": str, "first_name": str, "last_name": str} or empty dict.
    """
    # Extract clean company name
    clean_name = re.sub(r'\.nl|\.de|\.be|\.com|\.eu|\.io', '', company_name).strip()
    clean_name = re.sub(r'https?://\S+', '', clean_name).strip()
    clean_name = re.sub(r'\s*(?:›|»|/|\\|•|\|)\s*.*$', '', clean_name).strip()
    noise = ['home', 'welcome', 'bij', 'portal', 'login', 'search', 'about', 'contact', 'blog', 'news', 'images']
    for w in noise:
        clean_name = re.sub(rf'\b{w}\b', '', clean_name, flags=re.IGNORECASE).strip()
    words = clean_name.split()[:3]
    clean_name = ' '.join(words).strip()
    if len(clean_name) < 3:
        clean_name = domain.split('.')[0]

    # STEP 1: Scrape company website to find real person names
    found_names = []
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False,
                                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"}) as client:
            for path in ['', '/over-ons', '/about', '/team', '/onze-mensen', '/wie-zijn-wij', '/contact', '/ons-team', '/people']:
                try:
                    r = await client.get(f"https://{domain}{path}", timeout=8)
                    if r.status_code != 200:
                        continue
                    page_text = re.sub(r'<[^>]+>', ' ', r.text)
                    page_text = re.sub(r'\s+', ' ', page_text).strip()

                    # Look for name + role patterns
                    role_kw = r'(?:Eigenaar|Directeur|CEO|Founder|Oprichter|Manager|Eigenaresse|Medewerker|Owner|Director|CTO|CMO|Head|Lead|VP|Adviseur|Consultant|Freelance|Bedrijfsleider|Ondernemer|Mede-eigenaar|Gevolmachtigd|Bestuurder)'
                    name_patterns = [
                        rf'({role_kw})\s*[:\-–]\s*([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}}(?:\s+[A-Z][a-z]{{1,20}}){{0,2}})',
                        rf'([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}}(?:\s+[A-Z][a-z]{{1,20}}){{0,2}})\s*[,]\s*({role_kw})',
                        rf'(?:Naam|Name|Contact|Team)\s*[:\-–]\s*([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}})',
                        rf'(?:ik ben|wij zijn|maak kennis met|meet|ontmoet)\s+([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}})',
                    ]
                    for pat in name_patterns:
                        for nm in re.finditer(pat, page_text, re.IGNORECASE):
                            groups = nm.groups()
                            name_parts = [g for g in groups if g and re.match(r'^[A-Z][a-z]+$', g)]
                            if len(name_parts) >= 2:
                                first, last = name_parts[0], name_parts[1]
                                if (len(first) >= 2 and len(last) >= 2 and
                                    first.lower() not in ('de', 'het', 'een', 'van', 'der', 'ten', 'ter', 'op', 'in', 'aan', 'voor', 'bij', 'met', 'uit', 'naar', clean_name.split()[0].lower() if clean_name else '') and
                                    last.lower() not in ('de', 'het', 'een', 'van', 'der', 'ten', 'ter', 'op', 'in', 'aan', 'voor', 'bij', 'met', 'uit', 'naar')):
                                    found_names.append((first, last))
                    if found_names:
                        break
                except Exception:
                    continue
    except Exception:
        pass

    # Also try email-based name extraction
    if not found_names:
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, verify=False,
                                         headers={"User-Agent": "Mozilla/5.0"}) as client:
                r = await client.get(f"https://{domain}", timeout=8)
                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', r.text)
                generic = {'info', 'admin', 'contact', 'sales', 'support', 'hello', 'noreply', 'no-reply',
                           'office', 'mail', 'postmaster', 'webmaster', 'abuse', 'billing', 'hr',
                           'marketing', 'press', 'legal', 'team', 'help', 'service'}
                for email in emails:
                    prefix = email.split('@')[0].lower().split('+')[0]
                    if prefix in generic or prefix.startswith('info') or prefix.startswith('contact'):
                        continue
                    parts = re.split(r'[._\-]', prefix)
                    parts = [p for p in parts if len(p) >= 2 and not p.isdigit()]
                    if len(parts) >= 2:
                        first, last = parts[0].capitalize(), parts[1].capitalize()
                        if (len(first) >= 2 and len(last) >= 2 and len(first) <= 15 and len(last) <= 15 and
                            first.lower() not in generic and last.lower() not in generic and
                            first.lower() != clean_name.split()[0].lower() if clean_name else True):
                            found_names.append((first, last))
                            break
                    elif len(parts) == 1 and 3 <= len(parts[0]) <= 15:
                        w = parts[0].capitalize()
                        if w.lower() not in generic:
                            found_names.append((w, ""))
                            break
        except Exception:
            pass

    # STEP 1.5: If no name from website, search LinkedIn directly for decision-makers
    if not found_names:
        try:
            from linkedin_engine import find_decision_maker_on_linkedin
            li_result = find_decision_maker_on_linkedin(clean_name, domain)
            if li_result:
                fn = li_result.get("first_name", "")
                ln = li_result.get("last_name", "")
                url = li_result.get("linkedin_url", "")
                if fn:
                    logger.info(f"[Lead Sourcing] LinkedIn direct search found: {fn} {ln} ({li_result.get('title', '')})")
                    if url:
                        return {"linkedin_url": url, "first_name": fn, "last_name": ln}
                    else:
                        found_names.append((fn, ln))
        except Exception as e:
            logger.info(f"[Lead Sourcing] LinkedIn direct search failed: {e}")

    # STEP 2: For each found name, try to find LinkedIn profile
    # Bug #8 fix: Run Selenium in thread to avoid blocking the event loop
    for first, last in found_names[:3]:
        full_name = f"{first} {last}".strip()
        try:
            def _run_li_search():
                from linkedin_engine import get_firefox_driver, _search_and_find_profile
                with get_firefox_driver() as (driver, page):
                    profile_url, error = _search_and_find_profile(page, first, last, clean_name)
                    return profile_url
            profile_url = await asyncio.to_thread(_run_li_search)
            if profile_url:
                logger.info(f"[Lead Sourcing] LinkedIn found: {first} {last} -> {profile_url}")
                return {"linkedin_url": profile_url, "first_name": first, "last_name": last}
        except Exception as e:
            logger.info(f"[Lead Sourcing] LinkedIn search for {first} {last} failed: {e}")
        await asyncio.sleep(2)

    # STEP 3: No LinkedIn found, but we have a real name — return it without URL
    if found_names:
        first, last = found_names[0]
        logger.info(f"[Lead Sourcing] No LinkedIn found for {first} {last} ({domain}), returning name only")
        return {"linkedin_url": "", "first_name": first, "last_name": last}

    logger.info(f"[Lead Sourcing] No person found for {clean_name} ({domain})")
    return {}


async def _try_query(query: str, current_count: int) -> int:
    """Search Brave for a query, add new companies to DB, return count added."""
    results = await _search_web(query)
    
    # Bug #9 fix: dedup check now uses a fresh WAL snapshot — safe for concurrent reads
    # We use a short-lived connection in WAL mode; reads don't need _DB_LOCK in WAL
    domains_to_scrape = []
    conn2 = sqlite3.connect(DB_PATH, timeout=30.0)
    conn2.execute("PRAGMA journal_mode=WAL;")
    cur2 = conn2.cursor()
    seen_domains = set()
    for res in results:
        domain = res["domain"]
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        cur2.execute("SELECT company_id FROM companies WHERE domain = ?", (domain,))
        if cur2.fetchone():
            continue
        domains_to_scrape.append(res)
    conn2.close()
    
    if not domains_to_scrape:
        return 0

    async def _scrape_and_insert(res: dict) -> int:
        domain = res["domain"]
        async with _SCRAPE_SEMAPHORE:
            try:
                scrape_result = await scrape_url(f"https://{domain}")
            except Exception:
                scrape_result = {"text": "", "emails": []}
            if isinstance(scrape_result, dict):
                emails = scrape_result.get("emails", [])
                scraped_text = scrape_result.get("text", "")
            else:
                emails = []
                scraped_text = ""

        raw_name = res["title"]
        # Clean company name: split on common separators, take first meaningful part
        company_name = re.split(r'\s*[|\–—-›»/]\s*', raw_name)[0].strip()
        company_name = re.sub(r'&#x27;|&amp;|&quot;|&#x2[0-9a-f];', '', company_name).strip()
        company_name = re.sub(r'\s+', ' ', company_name).strip()
        # Skip garbage names
        garbage_names = {"home", "contact", "about", "blog", "news", "images", "community", "summary", "search", "login", "menu"}
        if len(company_name) < 3 or company_name.lower() in garbage_names:
            company_name = raw_name[:60]
        # Clean up Mojeek-style titles with arrows
        company_name = re.sub(r'\s*(?:›|»|/)\s*.*$', '', company_name).strip()

        # Search LinkedIn first — this gives us real names and profile URLs
        linkedin_url = ""
        first_name = ""
        last_name = ""
        role = "Decision Maker"
        contact_email = emails[0] if emails else ""
        tld_hint = _results_to_tld(res)

        try:
            li_result = await _find_linkedin_for_company(company_name, domain)
            if li_result and li_result.get("linkedin_url"):
                linkedin_url = li_result["linkedin_url"]
                first_name = li_result.get("first_name", "")
                last_name = li_result.get("last_name", "")
        except Exception as li_err:
            logger.info(f"[Lead Sourcing] LinkedIn search failed for {company_name}: {li_err}")

        # If LinkedIn didn't give us a name, try email extraction
        if not first_name or first_name in ("Contact", "Info", "Name", "Events"):
            first_name, last_name, role = _extract_personal_name(emails, scraped_text, domain)

        # Validate the name - reject garbage before inserting
        full_name = f"{first_name or ''} {last_name or ''}".strip()
        garbage_name_patterns = ['interimrecruiter', 'admin', 'info', 'sales', 'contact', 'noreply', 'test',
                                 'secretariaat', 'klantendienst', 'archief', 'afspraak', 'register', 'hallo',
                                 'studio', 'home', 'welcome', 'search', 'login', 'menu', 'blog', 'news',
                                 'commission', 'payments', 'support', 'office', 'team', 'service']
        # Also reject if name matches company name or is just the domain
        company_words = set(company_name.lower().split())
        name_words_set = set(full_name.lower().split())
        if (not full_name or len(full_name) < 3 or
            len(full_name) > 30 or
            full_name.lower() in garbage_name_patterns or
            re.search(r'[0-9]', full_name) or
            ('.' in full_name and ' ' not in full_name) or
            full_name == full_name.lower() or
            name_words_set & company_words or  # Name contains company name words
            full_name.lower() in company_name.lower()):  # Name IS the company name
            logger.info(f"[Lead Sourcing] Skipping garbage name: '{first_name}' '{last_name}' (domain: {domain})")
            return 0

        # Skip if we have neither a LinkedIn URL nor a real email
        if not linkedin_url and not contact_email:
            return 0

        # If we have LinkedIn but no email, generate a placeholder
        if not contact_email:
            contact_email = f"info@{domain}"

        async with _DB_LOCK:
            conn3 = sqlite3.connect(DB_PATH, timeout=60.0)
            conn3.execute("PRAGMA journal_mode=WAL;")
            cur3 = conn3.cursor()
            co_id = f"co_{uuid.uuid4().hex[:8]}"
            # Detect industry from the query
            q_lower = res.get("query", "").lower()
            detected_industry = f"{tld_hint} B2B"
            for kw, ind in [
                ("accountant", "Financiële Dienstverlening"), ("advocaat", "Juridisch"), ("notaris", "Juridisch"),
                ("makelaar", "Vastgoed"), ("makelaardij", "Vastgoed"), ("architect", "Architectuur"),
                ("fysio", "Zorg"), ("tandarts", "Zorg"), ("dierenarts", "Zorg"), ("pedicure", "Zorg"),
                ("manicure", "Schoonheid"), ("schoonheid", "Schoonheid"), ("kapsalon", "Schoonheid"),
                ("marketing", "Marketing & Communicatie"), ("reclamebureau", "Marketing & Communicatie"),
                ("pr bureau", "Marketing & Communicatie"), ("communicatie", "Marketing & Communicatie"),
                ("webdesign", "Webdesign & Development"), ("fotografie", "Creatief"),
                ("coaching", "Coaching & Training"), ("interieurarchitect", "Interieur & Design"),
                ("evenementen", "Evenementen"), ("drukkerij", "Drukkerij"),
                ("logistiek", "Logistiek & Transport"), ("transport", "Logistiek & Transport"),
                ("schoonmaak", "Schoonmaak & Onderhoud"), ("reiniging", "Schoonmaak & Onderhoud"),
                ("beveiliging", "Beveiliging"), ("tuinarchitect", "Tuin & Landschap"),
                ("uitvaart", "Uitvaartverzorging"), ("bouwb", "Bouw"), ("loodgieter", "Bouw & Installatie"),
                ("schilder", "Bouw & Afwerking"), ("elektricien", "Bouw & Installatie"),
                ("catering", "Catering & Horeca"), ("fitness", "Sport & Gezondheid"),
                ("verzekering", "Verzekeringen"), ("boekhouder", "Financiële Dienstverlening"),
                ("kledingwinkel", "Retail"),
            ]:
                if kw in q_lower:
                    detected_industry = ind
                    break
            try:
                # Skip contacts with garbage names
                garbage_names = {"contact", "info", "privacy", "name", "events", "verzuim", "compliance", 
                                "ukinfo", "home", "about", "blog", "news", "search", "login", "menu",
                                "office", "team", "support", "admin", "webmaster", "noreply",
                                "bi", "dpo", "payments", "secretaris", "group", "corporate"}
                is_garbage = (not first_name or 
                             first_name.lower() in garbage_names or
                             len(first_name) < 2 or
                             (not last_name and first_name[0].isdigit()) or
                             (not last_name and len(first_name) <= 3 and first_name.isalpha()))
                if is_garbage and not linkedin_url:
                    logger.info(f"[Lead Sourcing] Skipping contact with garbage name: '{first_name}' '{last_name}' (no LinkedIn URL)")
                    return 0
                
                cur3.execute("""INSERT INTO companies (company_id, name, domain, industry, created_at)
                                VALUES (?, ?, ?, ?, ?)""",
                             (co_id, company_name, domain, detected_industry, datetime.now(timezone.utc).isoformat()))
                ct_id = f"prsp_{uuid.uuid4().hex[:8]}"
                cur3.execute("""INSERT INTO contacts (contact_id, company_id, first_name, last_name, email, role, linkedin_url, current_stage, lead_score, priority, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 'INGESTED', 0, 'UNASSIGNED', ?)""",
                             (ct_id, co_id, first_name, last_name,
                              contact_email, role, linkedin_url or None, datetime.now(timezone.utc).isoformat()))
                conn3.commit()
            except sqlite3.IntegrityError:
                return 0
            except Exception as e:
                logger.error(f"[_scrape_and_insert] DB Error: {e}")
                return 0
            finally:
                conn3.close()
        logger.info(f"[Lead Sourcing] New lead: {company_name} ({domain}) -> {ct_id} (Contact: {first_name} {last_name})")
        await global_log_audit("Research Agent", "INGESTION", f"Lead sourced: {company_name} ({domain}) - Contact: {first_name} {last_name}", contact_id=ct_id)
        return 1

    tasks = [_scrape_and_insert(res) for res in domains_to_scrape]
    results_added = await asyncio.gather(*tasks)
    total_new = sum(results_added)
    return total_new

async def run_sourcing_cycle():
    """Run one full sourcing cycle: query Google, find new companies, insert as INGESTED."""
    # Count how many leads are currently in the pre-outreach pipeline
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM contacts WHERE current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED', 'OUTREACH_DRAFTED', 'PENDING_APPROVAL')")
    count = cursor.fetchone()[0]
    conn.close()

    if count >= _MAX_LEADS_SOURCED:
        logger.info(f"[Lead Sourcing] Pipeline full ({count}/{_MAX_LEADS_SOURCED}). Skipping.")
        return 0

    total_added = 0
    # Bug #7 fix: Load ICP search queries from active tenant config (not hardcoded list)
    tenant = _get_active_tenant()
    active_queries = tenant.get('search_queries') or []
    if not active_queries:
        active_queries = _LEAD_SOURCE_QUERIES  # fallback to defaults
    random.shuffle(active_queries)
    for query in active_queries:
        if count + total_added >= _MAX_LEADS_SOURCED:
            break
        added = await _try_query(query, count + total_added)
        total_added += added
        await asyncio.sleep(3)  # Delay between Brave queries to avoid 429

    logger.info(f"[Lead Sourcing] Cycle complete. Added {total_added} new leads. Pipeline at {count + total_added}/{_MAX_LEADS_SOURCED}.")
    return total_added

async def lead_sourcing_agent():
    """Background agent: searches for new prospects. Adaptive timing based on pipeline level."""
    logger.info(f"[Lead Sourcing Agent] Started. Adaptive sourcing (normal: {_SOURCE_CYCLE_MINUTES}min, urgent: 2min).")
    while True:
        try:
            # Check pipeline level
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contacts WHERE current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')")
            pipeline_count = cursor.fetchone()[0]
            conn.close()
            
            await run_sourcing_cycle()
            
            # Adaptive sleep: faster when pipeline is low
            if pipeline_count < 5:
                sleep_time = 2 * 60  # 2 minutes when pipeline is low
                logger.info(f"[Lead Sourcing Agent] Pipeline low ({pipeline_count}), sourcing again in 2 min")
            elif pipeline_count < 15:
                sleep_time = _SOURCE_CYCLE_MINUTES * 60  # Normal interval
            else:
                sleep_time = 15 * 60  # 15 min when pipeline is healthy
            await asyncio.sleep(sleep_time)
        except Exception as e:
            logger.error(f"[Lead Sourcing Agent] Error: {e}")
            await asyncio.sleep(120)

# =========================================================================
#  AGENT LOOP: processes leads through the 5-agent pipeline
# =========================================================================

async def agent_loop():
    """Continuously processes leads in the background: INGESTED -> PENDING_APPROVAL, then waits for human review."""
    logger.info("[Agent Loop] Background agent loop started. Checking/processing leads slowly (capped at 10 pending review).")
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            cursor = conn.cursor()
            
            # Count the number of leads currently pending review
            cursor.execute("SELECT COUNT(*) FROM contacts WHERE current_stage = 'PENDING_APPROVAL'")
            pending_count = cursor.fetchone()[0]
            
            if pending_count >= 10:
                conn.close()
                logger.info(f"[Agent Loop] Currently {pending_count} leads pending review (limit: 10). Sleeping for 60s...")
                await asyncio.sleep(60)
                continue
                
            cursor.execute("""
                SELECT ct.contact_id, ct.first_name, cp.name, cp.domain, ct.email
                FROM contacts ct
                JOIN companies cp ON ct.company_id = cp.company_id
                WHERE ct.current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')
                LIMIT 1
            """)
            row = cursor.fetchone()
            conn.close()

            if row:
                lead_id, first_name, company_name, domain, email = row
                logger.info(f"[Agent Loop] Processing lead {lead_id} ({company_name})")
                try:
                    await run_pipeline_for_lead(lead_id)
                    logger.info(f"[Agent Loop] Completed pipeline for {lead_id} ({company_name}) - now at PENDING_APPROVAL")
                except Exception as e:
                    logger.error(f"[Agent Loop] Pipeline error for {lead_id}: {e}")
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"[Agent Loop] Loop error: {e}")
            await asyncio.sleep(30)


async def linkedin_auto_loop():
    """Automatically sends LinkedIn connection requests (20/day, spread across 3 time periods).
    Calculates proper wait times to evenly distribute requests within each period.
    """
    from datetime import datetime, timedelta
    logger.info("[LinkedIn Auto] Auto-scheduling loop started. 20 requests/day (7 morning, 7 afternoon, 6 evening).")
    await asyncio.sleep(10)
    last_followup_check = 0
    FOLLOWUP_CHECK_INTERVAL = 1800  # Check every 30 minutes

    while True:
        try:
            from linkedin_engine import search_and_connect as _li_search, can_send_connection, _get_period_count, DAILY_LIMIT, MORNING_LIMIT, AFTERNOON_LIMIT, EVENING_LIMIT
            from linkedin_engine import check_pending_connections, send_pending_followups

            import time as _time
            now_ts = _time.time()
            # --- Follow-up check (every 30 minutes, not every loop) ---
            if now_ts - last_followup_check >= FOLLOWUP_CHECK_INTERVAL:
                try:
                    check_result = await asyncio.to_thread(check_pending_connections)
                    if check_result.get("accepted", 0) > 0:
                        logger.info(f"[LinkedIn Auto] Found {check_result['accepted']} new accepted connections")
                    followup_result = await asyncio.to_thread(send_pending_followups)
                    if followup_result.get("sent", 0) > 0:
                        logger.info(f"[LinkedIn Auto] Sent {followup_result['sent']} follow-up messages")
                    last_followup_check = now_ts
                except Exception as e:
                    logger.error(f"[LinkedIn Auto] Follow-up check error: {e}")
                    last_followup_check = now_ts

            now = datetime.now()
            hour = now.hour

            # Off hours — wait until next period
            if hour < 8:
                next_period = now.replace(hour=8, minute=0, second=0)
                wait = (next_period - now).total_seconds()
                logger.info(f"[LinkedIn Auto] Off hours. Next period at 08:00. Sleeping {wait/60:.0f}min.")
                await asyncio.sleep(max(wait, 60))
                continue
            elif hour >= 21:
                next_period = (now + timedelta(days=1)).replace(hour=8, minute=0, second=0)
                wait = (next_period - now).total_seconds()
                logger.info(f"[LinkedIn Auto] Off hours (evening ended). Next period tomorrow 08:00. Sleeping {wait/60:.0f}min.")
                await asyncio.sleep(max(wait, 60))
                continue

            # Check if we can send now
            can_send, reason = can_send_connection()
            if not can_send:
                if "limit_reached" in reason:
                    # Calculate wait until next period
                    if hour < 12:
                        next_start = now.replace(hour=12, minute=0, second=0)
                    elif hour < 17:
                        next_start = now.replace(hour=17, minute=0, second=0)
                    else:
                        next_start = (now + timedelta(days=1)).replace(hour=8, minute=0, second=0)
                    wait = (next_start - now).total_seconds()
                    logger.info(f"[LinkedIn Auto] {reason}. Next period at {next_start.strftime('%H:%M')}. Sleeping {wait/60:.0f}min.")
                    await asyncio.sleep(max(wait, 60))
                else:
                    logger.info(f"[LinkedIn Auto] Cannot send now: {reason}. Sleeping 3min.")
                    await asyncio.sleep(180)
                continue

            # Calculate time remaining in current period and requests remaining
            period_count, period_limit, period = _get_period_count()
            remaining = period_limit - period_count

            if period == "morning":
                period_end = now.replace(hour=12, minute=0, second=0)
            elif period == "afternoon":
                period_end = now.replace(hour=17, minute=0, second=0)
            else:
                period_end = now.replace(hour=21, minute=0, second=0)

            time_left = (period_end - now).total_seconds()
            if remaining > 0 and time_left > 0:
                # Space requests evenly: wait = time_left / remaining (with some randomness)
                import random
                base_wait = time_left / remaining
                # Add ±20% randomness to avoid patterns
                wait = base_wait * random.uniform(0.8, 1.2)
                # But cap at 15 minutes max between requests
                wait = min(wait, 900)
                # And at least 60 seconds
                wait = max(wait, 60)
            else:
                wait = 180

            # Find leads with LinkedIn URLs that haven't been contacted
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.contact_id, c.first_name, c.last_name, c.linkedin_url, c.outreach_draft, 
                       co.name as company_name, c.research_result, c.opportunity_mapping
                FROM contacts c
                LEFT JOIN companies co ON c.company_id = co.company_id
                WHERE c.linkedin_url IS NOT NULL AND c.linkedin_url != ''
                AND c.contact_id NOT IN (SELECT contact_id FROM linkedin_outreach)
                AND c.company_id NOT IN (
                    SELECT c2.company_id FROM contacts c2
                    JOIN linkedin_outreach lo ON c2.contact_id = lo.contact_id
                    WHERE c2.company_id IS NOT NULL
                    GROUP BY c2.company_id
                    HAVING COUNT(lo.id) >= 2
                )
                AND c.current_stage IN ('NURTURE', 'ACTIVE_OUTREACH', 'PRE_QUALIFIED', 'OPPORTUNITY_MAPPED', 'DELIVERABILITY_VERIFIED', 'RESEARCHED', 'INGESTED')
                ORDER BY RANDOM()
                LIMIT 1
            """)
            row = cursor.fetchone()
            conn.close()

            if not row:
                logger.info(f"[LinkedIn Auto] No uncontacted leads with LinkedIn URLs. Sleeping {wait/60:.0f}min.")
                await asyncio.sleep(wait)
                continue

            contact_id, first_name, last_name, linkedin_url, draft_json, company_name, research_json, opportunity_json = row

            # Validate name before sending - skip garbage
            from linkedin_engine import is_valid_linkedin_name
            if not is_valid_linkedin_name(first_name, last_name or ""):
                logger.info(f"[LinkedIn Auto] Skipping garbage name: {first_name} {last_name} ({company_name})")
                # Mark as contacted to avoid retrying
                conn2 = sqlite3.connect(DB_PATH, timeout=30.0)
                cur2 = conn2.cursor()
                cur2.execute("""INSERT OR IGNORE INTO linkedin_outreach 
                    (contact_id, first_name, last_name, company, profile_url, note, outcome, timestamp, connection_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 'skipped_invalid')""",
                    (contact_id, first_name, last_name or "", company_name or "", linkedin_url or "", "", "SKIPPED_INVALID_NAME"))
                conn2.commit()
                conn2.close()
                await asyncio.sleep(5)
                continue

            email_body = ""
            if draft_json:
                try:
                    draft = json.loads(draft_json)
                    email_body = draft.get("email_body_step_1", "")
                except Exception:
                    pass

            logger.info(f"[LinkedIn Auto] [{period} {period_count+1}/{period_limit}] Sending to {first_name} {last_name} ({company_name}). Next in {wait/60:.0f}min.")
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    _LI_EXECUTOR,
                    lambda: _li_search(
                        first_name, last_name or "", company_name or "",
                        email_body, contact_id=contact_id, profile_url=linkedin_url,
                        research_result=research_json, opportunity_mapping=opportunity_json
                    )
                )
                logger.info(f"[LinkedIn Auto] Result for {first_name} {last_name}: {result}")
            except Exception as e:
                logger.error(f"[LinkedIn Auto] Error sending to {first_name} {last_name}: {e}")

            # Wait the calculated time before next request
            logger.info(f"[LinkedIn Auto] Waiting {wait/60:.0f}min until next request...")
            await asyncio.sleep(wait)

        except Exception as e:
            logger.error(f"[LinkedIn Auto] Loop error: {e}")
            await asyncio.sleep(120)

# --- API ENDPOINTS ---

@app.get("/api/leads")
def get_leads():
    """Fetches all leads in the system with their embedded agent results."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT c.*, co.name as company_name, co.domain as company_domain, co.industry as company_industry, co.estimated_size as company_size
    FROM contacts c
    LEFT JOIN companies co ON c.company_id = co.company_id
    ORDER BY c.created_at DESC;
    """)
    rows = cursor.fetchall()
    conn.close()
    
    leads = []
    for r in rows:
        lead_dict = dict(r)
        # Parse JSON columns if present
        for col in ["research_result", "deliverability_audit", "opportunity_mapping", "qualification_assessment", "outreach_draft", "inbox_assessment", "last_handoff"]:
            if lead_dict.get(col):
                try:
                    lead_dict[col] = json.loads(lead_dict[col])
                except Exception:
                    lead_dict[col] = None
        leads.append(lead_dict)
    return leads


@app.get("/api/agents")
def get_agents():
    """Retrieves live statuses of all agents in the workspace."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM agent_status;")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/deliverability")
def get_deliverability():
    """Fetches sending domain logs and deliverability score averages."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM deliverability_stats ORDER BY domain ASC;")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/meetings")
def get_meetings():
    """Fetches active Google Calendar bookings."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
    SELECT m.*, c.first_name, c.last_name, c.email
    FROM meetings m
    LEFT JOIN contacts c ON m.contact_id = c.contact_id
    ORDER BY m.scheduled_time ASC;
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/activity_feed")
def get_activity_feed():
    """Fetches chronological activity logs for the sliding audit widget."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
    SELECT l.*, c.first_name, c.last_name
    FROM activity_log l
    LEFT JOIN contacts c ON l.contact_id = c.contact_id
    ORDER BY l.created_at DESC
    LIMIT 100;
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/handoffs")
def get_handoffs():
    """Retrieves inter-agent communication messages to map links."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM agent_actions ORDER BY created_at DESC LIMIT 50;")
    rows = cursor.fetchall()
    conn.close()
    
    actions = []
    for r in rows:
        action_dict = dict(r)
        if action_dict.get("payload"):
            try:
                action_dict["payload"] = json.loads(action_dict["payload"])
            except Exception:
                pass
        actions.append(action_dict)
    return actions


@app.post("/api/leads/{lead_id}/replay")
async def replay_lead(lead_id: str, background_tasks: BackgroundTasks):
    """Triggers the full multi-agent pipeline asynchronously."""
    # Verify lead exists
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM contacts WHERE contact_id = ? LIMIT 1;", (lead_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Prospect lead not found")
        
    # Queue up the multi-agent task asynchronously
    background_tasks.add_task(run_pipeline_for_lead, lead_id)
    return {"status": "RUNNING", "message": f"Successfully launched background replay for lead {lead_id}"}


@app.post("/api/leads/{lead_id}/approve")
async def approve_outreach(lead_id: str):
    """Approves outreach, sends email via Gmail API and moves state to ACTIVE_OUTREACH."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT c.email, c.outreach_draft, co.domain
    FROM contacts c
    LEFT JOIN companies co ON c.company_id = co.company_id
    WHERE c.contact_id = ?;
    """, (lead_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Lead not found")
        
    email, draft_json, domain = row

    # Bug #11 fix: verify lead is actually in PENDING_APPROVAL before sending
    cursor.execute("SELECT current_stage FROM contacts WHERE contact_id = ?", (lead_id,))
    stage_row = cursor.fetchone()
    if stage_row and stage_row[0] not in ('PENDING_APPROVAL', 'OUTREACH_DRAFTED'):
        conn.close()
        raise HTTPException(status_code=400, detail=f"Lead is in stage '{stage_row[0]}' — can only approve PENDING_APPROVAL leads")

    if not draft_json:
        conn.close()
        raise HTTPException(status_code=400, detail="Outreach has not been drafted for this lead yet")
        
    draft = json.loads(draft_json)
    
    # Run Boundary Checks (Anti-bracket template safety check)
    body_text = draft.get("email_body_step_1", "")
    subject = draft.get("subject_line_A", "")
    if "[" in body_text or "]" in body_text:
        conn.close()
        raise HTTPException(status_code=400, detail="Boundary Check Failed: Template bracket remnants [ ] detected in draft body!")

    # Attempt to send live email using ClawBuildrGmailConnector
    logger.info(f"[Outreach Approval] Attempting to send live email to {email} using Variant A")
    
    sent_successfully = False
    message_id = f"sent_{random.randint(100000, 999999)}"
    thread_id = f"thr_{random.randint(100000, 999999)}"
    send_error = None
    
    # Check if we have clawbuildr_gmail in directory to execute live
    try:
        from clawbuildr_gmail import ClawBuildrGmailConnector
        DATA_DIR = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(DATA_DIR, "client_secret.json")
        # Determine which tenant is active to use correct token
        tenant = _get_active_tenant()
        active_tenant_id = tenant.get("tenant_id", "default")
        token_path = os.path.join(DATA_DIR, f"token_gmail_{active_tenant_id}.json")
        
        # Fallback to default token if tenant token doesn't exist yet
        if not os.path.exists(token_path) and os.path.exists(os.path.join(DATA_DIR, "token_gmail.json")):
            token_path = os.path.join(DATA_DIR, "token_gmail.json")
        
        if os.path.exists(creds_path) and os.path.exists(token_path):
            connector = ClawBuildrGmailConnector(credentials_path=creds_path, token_path=token_path)
            if connector.authenticate():
                logger.info(f"[Gmail API] Triggering live email delivery to: {email}")
                # Build email_id early so we can inject tracking before sending
                email_id_pre = f"em_{random.randint(100000, 999999)}"
                # Convert plain-text body to HTML and inject tracking (Bug 1 + Bug 2 fix)
                html_body = "<html><body><pre style='font-family:inherit;white-space:pre-wrap'>" + body_text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") + "</pre></html>"
                html_body_tracked = _inject_tracking(email_id_pre, html_body)
                res = connector.send_email(to_email=email, subject=subject, body_text=body_text, body_html=html_body_tracked)
                if res.get("status") == "SENT":
                    sent_successfully = True
                    message_id = res.get("message_id", message_id)
                    thread_id = res.get("thread_id", thread_id)
                    # Use the pre-generated email_id so tracking pixel matches DB record
                    email_id = email_id_pre
                else:
                    send_error = res.get("error", "Unknown sending error")
            else:
                send_error = "OAuth Token is invalid or expired."
        else:
            send_error = "Gmail OAuth files (client_secret.json, token_gmail.json) not set up for live sending. Simulation mode applied."
    except Exception as ex:
        send_error = f"Gmail Connector Exception: {str(ex)}"
        logger.error(f"[Outreach Send] Sending crashed: {ex}")
        
    # If Gmail API failed, try SMTP (app password) — we know this works
    if not sent_successfully:
        try:
            logger.info(f"[Outreach Approval] Trying SMTP app-password fallback to: {email}")
            smtp_res = await gmail_send(to=email, subject=subject, body=body_text)
            if smtp_res.get("status") in ("sent", "SENT", "OK", "success"):
                sent_successfully = True
                message_id = smtp_res.get("message_id", f"smtp_{random.randint(100000, 999999)}")
                thread_id = f"thr_{random.randint(100000, 999999)}"
                logger.info(f"[Outreach Approval] SMTP send OK: {email}")
            else:
                send_error = smtp_res.get("error", "SMTP unknown error")
                logger.warning(f"[Outreach Approval] SMTP failed: {send_error}")
        except Exception as ex:
            send_error = f"SMTP Exception: {str(ex)}"
            logger.error(f"[Outreach Approval] SMTP crashed: {ex}")

    # Bug #1 fix: NEVER silently fake a sent email. Surface the real error.
    if not sent_successfully:
        logger.error(f"[Outreach Approval] All send methods failed: {send_error}")
        conn.close()
        raise HTTPException(
            status_code=500,
            detail=f"Email delivery failed. Configure Gmail credentials or SMTP in Settings. Error: {send_error}"
        )
        
    if sent_successfully:
        # Move state to ACTIVE_OUTREACH, and save email log
        cursor.execute("UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH' WHERE contact_id = ?;", (lead_id,))
        
        # Log to email table
        email_id = f"em_{random.randint(100000, 999999)}"
        cursor.execute("""
        INSERT INTO emails (email_id, contact_id, campaign_id, direction, subject, body, message_id, thread_id, status, sent_at)
        VALUES (?, ?, NULL, 'OUTBOUND', ?, ?, ?, ?, 'SENT', ?);
        """, (email_id, lead_id, subject, body_text, message_id, thread_id, datetime.now(timezone.utc).isoformat()))
        
        # Update deliverability counts
        if domain:
            cursor.execute("UPDATE deliverability_stats SET sends_count = sends_count + 1 WHERE domain = ?;", (domain,))
            
        # Log to Audit Activity
        cursor.execute("""
        INSERT INTO activity_log (contact_id, actor, action_type, action_details)
        VALUES (?, 'CentralOrchestrator', 'OUTBOUND_SEND', ?);
        """, (lead_id, f"Approved personalized outbound email sequence sent. Message ID: {message_id}"))
        
        conn.commit()
        conn.close()
        
        await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "ACTIVE_OUTREACH"})
        await sse_manager.broadcast("activity", {
            "contact_id": lead_id,
            "actor": "CentralOrchestrator",
            "action_type": "OUTBOUND_SEND",
            "action_details": f"Approved outbound email sent to {email}. Message ID: {message_id}"
        })
        await sse_manager.broadcast("notify", {
            "type": "success",
            "message": f"Successfully sent outreach email to {email}!"
        })
        return {"status": "SUCCESS", "message_id": message_id}
    else:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Live outreach send failed: {send_error}")


@app.put("/api/leads/{lead_id}/draft")
async def update_draft(lead_id: str, request: Request):
    """Update the email draft for a lead (subject + body)."""
    body = await request.json()
    new_subject = body.get("subject", "").strip()
    new_body = body.get("body", "").strip()
    if not new_subject or not new_body:
        raise HTTPException(status_code=400, detail="Both subject and body are required")
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("SELECT outreach_draft FROM contacts WHERE contact_id = ?", (lead_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Lead not found")
    existing = json.loads(row[0]) if row[0] else {}
    existing["subject_line_A"] = new_subject
    existing["email_body_step_1"] = new_body
    cursor.execute("UPDATE contacts SET outreach_draft = ? WHERE contact_id = ?", (json.dumps(existing), lead_id))
    conn.commit()
    conn.close()
    await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id})
    return {"status": "UPDATED"}

@app.delete("/api/leads/{lead_id}")
async def delete_lead(lead_id: str):
    """Purge a lead and its company from the pipeline."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("SELECT company_id FROM contacts WHERE contact_id = ?", (lead_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Lead not found")
    company_id = row[0]
    for tbl in ["emails", "meetings", "activity_log"]:
        cursor.execute(f"DELETE FROM {tbl} WHERE contact_id = ?", (lead_id,))
    cursor.execute("DELETE FROM agent_actions WHERE prospect_id = ?", (lead_id,))
    cursor.execute("DELETE FROM contacts WHERE contact_id = ?", (lead_id,))
    cursor.execute("DELETE FROM companies WHERE company_id = ?", (company_id,))
    conn.commit()
    conn.close()
    await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "deleted": True})
    return {"status": "DELETED"}

@app.post("/api/leads/{lead_id}/reply_simulate")
async def reply_simulate(lead_id: str, request: Request):
    """Simulates receipt of an inbound reply email, running the Inbox Management Agent."""
    body = await request.json()
    reply_text = body.get("reply_text", "Ik ben wel geïnteresseerd, kunnen we bellen?")
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("""
    SELECT c.email, c.first_name, c.last_name, co.domain
    FROM contacts c
    LEFT JOIN companies co ON c.company_id = co.company_id
    WHERE c.contact_id = ?;
    """, (lead_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Lead not found")
        
    email, first_name, last_name, domain = row
    
    # Load Inbox Agent
    try:
        from clawbuildr_multi_agent import InboxManagementAgent
        inbox_agent = InboxManagementAgent()
    except ImportError:
        class DummyInbox:
            def evaluate_reply(self, text):
                from clawbuildr_multi_agent import InboxAssessment
                return InboxAssessment(
                    classification="MEETING_REQUEST",
                    detected_objections=[],
                    suggested_response_body="Beste, geweldig! Plan direct een moment in via cal.com/odysseus.",
                    crm_status_action="SCHEDULE_MEETING_TASK"
                )
        inbox_agent = DummyInbox()

    # Trigger Agent DB Status
    cursor.execute("""
    UPDATE agent_status 
    SET status = 'RUNNING', current_lead_id = ?, last_active_at = ?, last_action = 'Evaluating inbound response...'
    WHERE agent_name = 'Inbox Management Agent';
    """, (lead_id, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    
    await sse_manager.broadcast("agent_status_change", {
        "agent_name": "Inbox Management Agent",
        "status": "RUNNING",
        "current_lead_id": lead_id,
        "last_action": "Evaluating inbound response..."
    })
    
    await sse_manager.broadcast("agent_handoff", {
        "action_id": f"h_live_{random.randint(1000, 9999)}",
        "sender_agent": "CentralOrchestrator",
        "recipient_agent": "Inbox Management Agent",
        "prospect_id": lead_id,
        "payload": {"inbound_email_body": reply_text}
    })
    
    await asyncio.sleep(2.0)
    
    assessment = inbox_agent.evaluate_reply(reply_text)
    assessment_dict = assessment.model_dump()
    
    # Store Inbound Reply, evaluation and perform transition state
    new_stage = "REPLIED"
    if assessment_dict["classification"] in ["MEETING_REQUEST", "INTERESTED"]:
        new_stage = "MEETING_SCHEDULED"
        
        # Add a mock Google Calendar appointment
        meeting_id = f"meet_live_{random.randint(100000, 999999)}"
        event_id = f"evt_live_{random.randint(100000, 999999)}"
        summary = f"Odysseus Demo Call | {first_name} {last_name}"
        scheduled_time = "2026-06-09T10:00:00Z"
        hangout_link = "https://meet.google.com/meet-demo-link"
        
        cursor.execute("""
        INSERT INTO meetings (meeting_id, contact_id, event_id, summary, scheduled_time, duration_minutes, hangout_link, status)
        VALUES (?, ?, ?, ?, ?, 30, ?, 'CONFIRMED');
        """, (meeting_id, lead_id, event_id, summary, hangout_link))
        
        cursor.execute("""
        UPDATE contacts 
        SET current_stage = ?, lead_score = 100, priority = 'HOT', meeting_ready = 1, inbox_assessment = ?
        WHERE contact_id = ?;
        """, (new_stage, json.dumps(assessment_dict), lead_id))
        
    elif assessment_dict["classification"] == "NOT_INTERESTED":
        new_stage = "OPT_OUT"
        # Register in GDPR table
        email_hash = hashlib.sha256(email.lower().strip().encode()).hexdigest()
        cursor.execute("INSERT OR IGNORE INTO opt_outs (email_hash, reason, source_email_domain) VALUES (?, 'Opt-out simulation', ?);", (email_hash, domain))
        
        cursor.execute("""
        UPDATE contacts 
        SET current_stage = ?, priority = 'LOW_PRIORITY', inbox_assessment = ?
        WHERE contact_id = ?;
        """, (new_stage, json.dumps(assessment_dict), lead_id))
    else:
        new_stage = "NURTURE"
        cursor.execute("""
        UPDATE contacts 
        SET current_stage = ?, inbox_assessment = ?
        WHERE contact_id = ?;
        """, (new_stage, json.dumps(assessment_dict), lead_id))

    # Log incoming email
    em_id = f"em_in_{random.randint(10000, 99999)}"
    cursor.execute("""
    INSERT INTO emails (email_id, contact_id, campaign_id, direction, subject, body, status, received_at)
    VALUES (?, ?, NULL, 'INBOUND', 'Re: Outreach campaign', ?, 'REPLIED', ?);
    """, (em_id, lead_id, reply_text, datetime.now(timezone.utc).isoformat()))

    # Update deliverability replies
    if domain:
        cursor.execute("UPDATE deliverability_stats SET replies_count = replies_count + 1 WHERE domain = ?;", (domain,))
        
    # Log audit
    cursor.execute("""
    INSERT INTO activity_log (contact_id, actor, action_type, action_details)
    VALUES (?, 'Inbox Management Agent', 'INBOUND_REPLY', ?);
    """, (lead_id, f"Inbound response evaluated as {assessment_dict['classification']}. Suggestion: {assessment_dict['crm_status_action']}. State: {new_stage}"))

    # Reset Agent status
    cursor.execute("""
    UPDATE agent_status 
    SET status = 'IDLE', current_lead_id = NULL, last_action = 'Monitoring replies...'
    WHERE agent_name = 'Inbox Management Agent';
    """)
    conn.commit()
    conn.close()
    
    # Broadcast SSE events
    await sse_manager.broadcast("agent_status_change", {
        "agent_name": "Inbox Management Agent",
        "status": "IDLE",
        "current_lead_id": None,
        "last_action": "Monitoring replies..."
    })
    
    await sse_manager.broadcast("agent_handoff", {
        "action_id": f"h_live_{random.randint(1000, 9999)}",
        "sender_agent": "Inbox Management Agent",
        "recipient_agent": "CentralOrchestrator",
        "prospect_id": lead_id,
        "payload": {"classification": assessment_dict["classification"], "action_crm": assessment_dict["crm_status_action"]}
    })
    
    await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": new_stage})
    await sse_manager.broadcast("activity", {
        "contact_id": lead_id,
        "actor": "Inbox Management Agent",
        "action_type": "INBOUND_REPLY",
        "action_details": f"Inbound email reply from {email} classified as {assessment_dict['classification']}."
    })
    
    notify_msg = f"Inbound email received! Lead {first_name} classified as {assessment_dict['classification']}"
    if new_stage == "MEETING_SCHEDULED":
        notify_msg += " & Demo call booked!"
    await sse_manager.broadcast("notify", {
        "type": "success",
        "message": notify_msg
    })
    
    return {"status": "REPLIED", "classification": assessment_dict["classification"]}


# --- REAL-TIME EVENT STREAM (SSE) ---

@app.get("/api/events")
async def events_endpoint(request: Request):
    """Establishes real-time connection with browser UI."""
    queue = await sse_manager.subscribe()
    
    async def event_generator():
        try:
            while True:
                # Keep-alive heartbeat every 15s to keep connections healthy
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield event
                except asyncio.TimeoutError:
                    yield "data: {\"event\": \"ping\"}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sse_manager.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# --- AGENT ZERO INGESTION ---

class AgentZeroLead(BaseModel):
    first_name: str
    last_name: str
    company_name: str
    domain: str
    email: str
    linkedin_url: Optional[str] = None
    role: Optional[str] = None
    industry: Optional[str] = None

@app.post("/api/agent-zero/ingest")
async def api_agent_zero_ingest(lead: AgentZeroLead, background_tasks: BackgroundTasks):
    """Ingests a verified lead from Agent Zero into the pipeline."""
    try:
        async with _DB_LOCK:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            cursor = conn.cursor()
            
            # Check if domain already exists in companies
            cursor.execute("SELECT company_id FROM companies WHERE domain = ?", (lead.domain,))
            existing_company = cursor.fetchone()
            if existing_company:
                co_id = existing_company[0]
            else:
                co_id = f"comp_{uuid.uuid4().hex[:8]}"
                cursor.execute("""INSERT INTO companies (company_id, name, domain, industry, created_at)
                                  VALUES (?, ?, ?, ?, ?)""",
                               (co_id, lead.company_name, lead.domain, lead.industry or "Unknown", datetime.now(timezone.utc).isoformat()))
            
            # Check if email exists in contacts
            cursor.execute("SELECT contact_id FROM contacts WHERE email = ?", (lead.email,))
            if cursor.fetchone():
                conn.close()
                return {"status": "skipped", "message": "Contact already exists."}
                
            ct_id = f"prsp_{uuid.uuid4().hex[:8]}"
            cursor.execute("""INSERT INTO contacts (contact_id, company_id, first_name, last_name, email, role, linkedin_url, current_stage, lead_score, priority, created_at)
                              VALUES (?, ?, ?, ?, ?, ?, ?, 'INGESTED', 0, 'UNASSIGNED', ?)""",
                           (ct_id, co_id, lead.first_name, lead.last_name, lead.email, lead.role or "Founder", lead.linkedin_url, datetime.now(timezone.utc).isoformat()))
            conn.commit()
            conn.close()
        
        # Audit log
        await global_log_audit("Agent Zero", "INGESTION", f"Lead ingested from Agent Zero: {lead.company_name} ({lead.domain}) - Contact: {lead.first_name} {lead.last_name}", contact_id=ct_id)
        
        # Trigger pipeline asynchronously
        asyncio.create_task(run_pipeline_for_lead(ct_id))
        
        return {"status": "ok", "message": f"Lead {ct_id} ingested and pipeline triggered."}
    except Exception as e:
        logger.error(f"[Agent Zero Ingest] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- MANUAL SOURCING TRIGGER ---

@app.post("/api/source-now")
async def api_source_now():
    """Manual trigger: run one lead sourcing cycle now."""
    added = await run_sourcing_cycle()
    return {"status": "ok", "added": added}

# --- LINKEDIN OUTREACH ENDPOINTS ---

@app.get("/api/linkedin/history")
def linkedin_history(limit: int = Query(100, ge=1, le=500)):
    """Fetches all LinkedIn outreach attempts."""
    return get_clawbuildr_linkedin_history(limit)

@app.get("/api/linkedin/status")
def linkedin_status():
    """Check if LinkedIn session cookies exist."""
    return linkedin_login_status()

@app.post("/api/linkedin/login")
async def linkedin_login_endpoint(background_tasks: BackgroundTasks):
    """Opens a browser for manual LinkedIn login. Cookies are saved after login."""
    background_tasks.add_task(linkedin_login)
    return {"status": "started", "message": "Browser opening. Please log in to LinkedIn."}

@app.post("/api/linkedin/extract-cookies")
def linkedin_extract():
    """Extract LinkedIn cookies from Firefox browser."""
    return extract_firefox_cookies()

@app.get("/api/linkedin/daily-count")
def linkedin_daily_count():
    """Get today's connection request count vs daily limit."""
    return get_daily_count_api()

@app.get("/api/linkedin/scheduling-status")
def linkedin_scheduling_status():
    """Get detailed scheduling status showing morning/afternoon/evening distribution."""
    return get_scheduling_status()

@app.post("/api/leads/{lead_id}/linkedin")
async def trigger_linkedin(lead_id: str, background_tasks: BackgroundTasks):
    """Manually trigger LinkedIn outreach for a specific lead."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute("""
    SELECT c.first_name, c.last_name, c.outreach_draft, co.name, c.linkedin_url, c.research_result, c.opportunity_mapping
    FROM contacts c
    LEFT JOIN companies co ON c.company_id = co.company_id
    WHERE c.contact_id = ?;
    """, (lead_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")
    first_name, last_name, draft_json, company_name, linkedin_url, research_json, opportunity_json = row
    email_body = ""
    if draft_json:
        try:
            draft = json.loads(draft_json)
            email_body = draft.get("email_body_step_1", "")
        except Exception:
            pass

    from linkedin_engine import search_and_connect as _li_search
    import logging as _lg
    _log = _lg.getLogger("ClawBuildrDashboard")

    def _do_linkedin():
        try:
            _log.info(f"[Manual LI] Starting outreach for {first_name} {last_name} ({lead_id}) url={linkedin_url}")
            result = _li_search(first_name, last_name or "", company_name or "", email_body, contact_id=lead_id, profile_url=linkedin_url, research_result=research_json, opportunity_mapping=opportunity_json)
            _log.info(f"[Manual LI] Result: {result}")
        except Exception as e:
            _log.error(f"[Manual LI] Error for {lead_id}: {e}", exc_info=True)

    background_tasks.add_task(_do_linkedin)
    return {"status": "RUNNING", "message": f"LinkedIn outreach triggered for {first_name} {last_name}"}

# --- FOLLOW-UP & CONNECTION TRACKING ENDPOINTS ---

@app.get("/api/linkedin/connection-stats")
def connection_stats():
    """Get connection request stats: sent, accepted, pending, followups."""
    from linkedin_engine import get_connection_stats
    return get_connection_stats()

@app.get("/api/linkedin/pending-followups")
def pending_followups():
    """Get accepted connections that need follow-up messages (ready to paste)."""
    import sqlite3, random
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, first_name, last_name, company, profile_url, connection_status,
               followup_sent, followup_note, last_checked
        FROM linkedin_outreach
        WHERE connection_status = 'accepted' AND followup_sent = 0
        ORDER BY last_checked DESC
    """).fetchall()
    conn.close()
    
    templates = [
        "Heel fijn dat we verbonden zijn, {first_name}! Ik help bedrijven zoals {company} met AI-gestuurde automatisering. Zijn er processen die jullie nu handmatig doen en willen digitaliseren?",
        "{first_name}, bedankt voor de connectie! Wij bouwen AI-tools die bedrijfsprocessen automatiseren. Welk proces zou jullie het meeste tijd besparen als het geautomatiseerd zou worden?",
        "Leuk om verbonden te zijn, {first_name}! Bij ons helpen we bedrijven met slimme automatisering. Zijn er taken bij {company} die veel tijd kosten en niet creatief zijn?",
        "Dank voor de connectie, {first_name}! Wij helpen MKB-bedrijven met AI-automatisering voor taken zoals e-mails, leadopvolging en dataverwerking. Zijn er processen bij {company} die hierbij passen?",
    ]
    
    result = []
    for r in rows:
        note = random.choice(templates).format(first_name=r["first_name"], company=r["company"] or "jullie bedrijf")
        result.append({
            "id": r["id"],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "company": r["company"],
            "profile_url": r["profile_url"],
            "followup_note": note,
        })
    
    return {"pending": len(result), "messages": result}

@app.post("/api/linkedin/mark-followup-sent/{outreach_id}")
def mark_followup_sent(outreach_id: int):
    """Mark a follow-up as sent (after user pastes and sends it manually)."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("UPDATE linkedin_outreach SET followup_sent = 1 WHERE id = ?", (outreach_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/api/linkedin/check-connections")
async def check_connections(background_tasks: BackgroundTasks):
    """Check pending connections to see who accepted."""
    from linkedin_engine import check_pending_connections
    def _do_check():
        try:
            import logging as _lg
            _log = _lg.getLogger("ClawBuildrDashboard")
            result = check_pending_connections()
            _log.info(f"[Connection Check] Result: {result}")
        except Exception as e:
            _lg.getLogger("ClawBuildrDashboard").error(f"[Connection Check] Error: {e}")
    background_tasks.add_task(_do_check)
    return {"status": "RUNNING", "message": "Checking pending connections..."}

@app.post("/api/linkedin/send-followups")
async def send_followups(background_tasks: BackgroundTasks):
    """Send follow-up messages to accepted connections."""
    from linkedin_engine import send_pending_followups
    def _do_followup():
        try:
            import logging as _lg
            _log = _lg.getLogger("ClawBuildrDashboard")
            result = send_pending_followups()
            _log.info(f"[Follow-ups] Result: {result}")
        except Exception as e:
            _lg.getLogger("ClawBuildrDashboard").error(f"[Follow-ups] Error: {e}")
    background_tasks.add_task(_do_followup)
    return {"status": "RUNNING", "message": "Sending follow-up messages..."}

@app.post("/api/linkedin/auto-scheduler/start")
def start_auto_scheduler():
    from linkedin_engine import start_auto_scheduler
    start_auto_scheduler()
    return {"status": "started", "message": "Auto-scheduler started"}

@app.post("/api/linkedin/auto-scheduler/stop")
def stop_auto_scheduler():
    from linkedin_engine import stop_auto_scheduler
    stop_auto_scheduler()
    return {"status": "stopped", "message": "Auto-scheduler stopped"}

@app.get("/api/linkedin/auto-scheduler/status")
def auto_scheduler_status():
    from linkedin_engine import get_auto_scheduler_status
    return get_auto_scheduler_status()

# --- LinkedIn Posts API ---

@app.get("/api/linkedin/posts")
def get_posts(status: str = None):
    from linkedin_engine import get_post_queue
    return {"posts": get_post_queue(status=status)}

@app.post("/api/linkedin/posts/add")
def add_post_endpoint(content: str, topic: str = "", scheduled_time: str = None):
    from linkedin_engine import add_post
    post_id = add_post(content, topic=topic, scheduled_time=scheduled_time)
    return {"status": "added", "post_id": post_id}

# Scheduler routes MUST be before {post_id} routes to avoid "scheduler" matching as post_id
@app.post("/api/linkedin/posts/scheduler/start")
def start_post_scheduler_endpoint():
    from linkedin_engine import start_post_scheduler
    return start_post_scheduler()

@app.post("/api/linkedin/posts/scheduler/stop")
def stop_post_scheduler_endpoint():
    from linkedin_engine import stop_post_scheduler
    return stop_post_scheduler()

@app.get("/api/linkedin/posts/scheduler/status")
def post_scheduler_status_endpoint():
    from linkedin_engine import get_post_scheduler_status
    return get_post_scheduler_status()

@app.post("/api/linkedin/posts/{post_id}/update")
def update_post_endpoint(post_id: int, content: str = None, scheduled_time: str = None, status: str = None):
    from linkedin_engine import update_post
    update_post(post_id, content=content, scheduled_time=scheduled_time, status=status)
    return {"status": "updated"}

@app.post("/api/linkedin/posts/{post_id}/delete")
def delete_post_endpoint(post_id: int):
    from linkedin_engine import delete_post
    delete_post(post_id)
    return {"status": "deleted"}

@app.post("/api/linkedin/posts/{post_id}/publish")
def publish_post_endpoint(post_id: int):
    from linkedin_engine import publish_post
    return publish_post(post_id)

# --- Auto-Engagement API ---

@app.post("/api/linkedin/auto-like")
def auto_like_endpoint(max_likes: int = 10):
    from linkedin_engine import auto_like_feed_posts
    return auto_like_feed_posts(max_likes=max_likes)

@app.post("/api/linkedin/auto-endorse")
def auto_endorse_endpoint(profile_url: str, max_endorsements: int = 10):
    from linkedin_engine import auto_endorse_skills
    return auto_endorse_skills(profile_url, max_endorsements=max_endorsements)

@app.post("/api/linkedin/auto-endorse-all")
def auto_endorse_all_endpoint(max_profiles: int = 5, max_per_profile: int = 10):
    from linkedin_engine import auto_endorse_all_connections
    return auto_endorse_all_connections(max_profiles=max_profiles, max_per_profile=max_per_profile)

# --- FounderFlow Content API ---

@app.get("/api/linkedin/founderflow-stats")
def founderflow_stats_endpoint():
    from linkedin_engine import scrape_founderflow_dashboard
    return scrape_founderflow_dashboard()

@app.post("/api/linkedin/generate-founderflow-post")
def generate_founderflow_post_endpoint():
    from linkedin_engine import generate_founderflow_post
    post = generate_founderflow_post()
    if post:
        from linkedin_engine import add_post
        post_id = add_post(post, topic="founderflow_results")
        return {"success": True, "post": post, "post_id": post_id}
    return {"success": False, "error": "Failed to generate post"}

# --- INDEX HTML FRONTEND DIRECTLY SERVED ---

# =========================================================================
# TENANT CONFIG API
# =========================================================================

@app.get("/api/config/tenants")
def get_all_tenants():
    """Returns all tenant profiles."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM tenant_config ORDER BY display_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/config/tenant")
def get_active_tenant_api():
    """Returns the currently active tenant config."""
    return _get_active_tenant()

@app.put("/api/config/tenant")
async def save_tenant_config(request: Request):
    """Saves the active tenant's configuration."""
    body = await request.json()
    tenant_id = body.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    try:
        conn.execute("""
            INSERT INTO tenant_config
                (tenant_id, display_name, sending_email, sending_domain, calendar_link,
                 signature_block, value_doctrine, brand_voice, icp_industries,
                 icp_roles, icp_company_size, search_queries, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(tenant_id) DO UPDATE SET
                display_name   = excluded.display_name,
                sending_email  = excluded.sending_email,
                sending_domain = excluded.sending_domain,
                calendar_link  = excluded.calendar_link,
                signature_block= excluded.signature_block,
                value_doctrine = excluded.value_doctrine,
                brand_voice    = excluded.brand_voice,
                icp_industries = excluded.icp_industries,
                icp_roles      = excluded.icp_roles,
                icp_company_size = excluded.icp_company_size,
                search_queries = excluded.search_queries,
                active         = 1
        """, (
            tenant_id,
            body.get("display_name", ""),
            body.get("sending_email", ""),
            body.get("sending_domain", ""),
            body.get("calendar_link", ""),
            body.get("signature_block", ""),
            body.get("value_doctrine", ""),
            body.get("brand_voice", ""),
            json.dumps(body.get("icp_industries", [])),
            json.dumps(body.get("icp_roles", [])),
            body.get("icp_company_size", "10-200"),
            json.dumps(body.get("search_queries", []))
        ))
        # Deactivate all other tenants
        conn.execute("UPDATE tenant_config SET active = 0 WHERE tenant_id != ?", (tenant_id,))
        conn.commit()
    finally:
        conn.close()
    logger.info(f"[Tenant] Config saved for tenant '{tenant_id}'")
    return {"status": "saved", "tenant_id": tenant_id}

@app.post("/api/config/tenant/switch/{tenant_id}")
async def switch_tenant(tenant_id: str):
    """Switches the active tenant."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    row = conn.execute("SELECT tenant_id FROM tenant_config WHERE tenant_id = ?", (tenant_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")
    conn.execute("UPDATE tenant_config SET active = 0")
    conn.execute("UPDATE tenant_config SET active = 1 WHERE tenant_id = ?", (tenant_id,))
    conn.commit()
    conn.close()
    logger.info(f"[Tenant] Switched active tenant to '{tenant_id}'")
    return {"status": "switched", "tenant_id": tenant_id}


@app.post("/api/config/tenant/{tenant_id}/connect-gmail")
def connect_gmail_tenant(tenant_id: str, background_tasks: BackgroundTasks):
    """Triggers the Google OAuth flow on the local machine for this tenant."""
    def _run_oauth():
        try:
            from clawbuildr_gmail import ClawBuildrGmailConnector
            creds_path = os.path.join(DATA_DIR, "client_secret.json")
            # Multi-tenant token path
            token_path = os.path.join(DATA_DIR, f"token_gmail_{tenant_id}.json")
            
            # Delete old token if exists to force re-auth
            if os.path.exists(token_path):
                os.remove(token_path)
                
            connector = ClawBuildrGmailConnector(credentials_path=creds_path, token_path=token_path)
            success = connector.authenticate(run_local_server=True)
            if success:
                logger.info(f"[Gmail] Successfully connected Gmail for tenant {tenant_id}")
            else:
                logger.error(f"[Gmail] Failed to connect Gmail for tenant {tenant_id}")
        except Exception as e:
            logger.error(f"[Gmail] Error during OAuth: {e}")
            
    background_tasks.add_task(_run_oauth)
    return {"status": "started", "message": "Check your browser to complete Google Authentication"}


# ===========================================================
# TRACKING ENDPOINTS (open pixel, click redirect, reply poll)
# ===========================================================
import base64 as _b64

_TRACKING_PIXEL = _b64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)

@app.get("/track/open/{email_id}")
async def track_email_open(email_id: str, request: Request):
    """Serves a 1x1 tracking pixel and logs the OPENED event."""
    from fastapi.responses import Response
    now = datetime.now(timezone.utc).isoformat()
    try:
        async with _DB_LOCK:
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            # Update email record
            conn.execute("""
                UPDATE emails SET opened_at = COALESCE(opened_at, ?),
                opened_count = COALESCE(opened_count, 0) + 1
                WHERE email_id = ?
            """, (now, email_id))
            # Log event
            contact_id = conn.execute(
                "SELECT contact_id FROM emails WHERE email_id = ?", (email_id,)
            ).fetchone()
            contact_id = contact_id[0] if contact_id else None
            conn.execute("""
                INSERT OR IGNORE INTO email_events (event_id, email_id, contact_id, event_type, metadata, created_at)
                VALUES (?, ?, ?, 'OPENED', ?, ?)
            """, (str(uuid.uuid4()), email_id, contact_id, '{}', now))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.warning(f"[Tracking] Open pixel error: {e}")
    return Response(content=_TRACKING_PIXEL, media_type="image/png", headers={"Cache-Control": "no-store"})


@app.get("/track/click/{email_id}/{link_hash}")
async def track_email_click(email_id: str, link_hash: str, request: Request):
    """Logs click event and redirects to the original URL."""
    from fastapi.responses import RedirectResponse
    now = datetime.now(timezone.utc).isoformat()
    original_url = "https://injexion.io"  # fallback
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        # Look up original URL from clicked_links JSON
        email_row = conn.execute("SELECT clicked_links, contact_id FROM emails WHERE email_id = ?", (email_id,)).fetchone()
        if email_row:
            try:
                links = json.loads(email_row[0] or "[]")
                for link in links:
                    if isinstance(link, dict) and link.get("hash") == link_hash:
                        original_url = link.get("url", original_url)
                        break
            except Exception:
                pass
            contact_id = email_row[1]
        else:
            contact_id = None
        async with _DB_LOCK:
            conn.execute("""
                UPDATE emails SET clicked_at = COALESCE(clicked_at, ?) WHERE email_id = ?
            """, (now, email_id))
            conn.execute("""
                INSERT OR IGNORE INTO email_events (event_id, email_id, contact_id, event_type, metadata, created_at)
                VALUES (?, ?, ?, 'CLICKED', ?, ?)
            """, (str(uuid.uuid4()), email_id, contact_id, json.dumps({"link_hash": link_hash}), now))
            conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[Tracking] Click error: {e}")
    return RedirectResponse(url=original_url)


def _inject_tracking(email_id: str, html_body: str, base_url: str = "http://localhost:8000") -> str:
    """Injects tracking pixel and wraps links in a given HTML email body."""
    import hashlib
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    
    # Wrap all http/https links
    links = []
    def replace_link(m):
        url = m.group(1)
        h = hashlib.md5(url.encode()).hexdigest()[:12]
        links.append({"url": url, "hash": h})
        return f'href="{base_url}/track/click/{email_id}/{h}"'
    
    wrapped = re.sub(r'href="(https?://[^"]+)"', replace_link, html_body)
    
    # Save links list to DB
    conn.execute("UPDATE emails SET clicked_links = ? WHERE email_id = ?", (json.dumps(links), email_id))
    conn.commit()
    conn.close()
    
    # Append tracking pixel
    pixel = f'<img src="{base_url}/track/open/{email_id}" width="1" height="1" alt="" style="display:none">'
    if "</body>" in wrapped:
        wrapped = wrapped.replace("</body>", f"{pixel}</body>")
    else:
        wrapped = wrapped + pixel
    return wrapped


# ====================================================
# METRICS API
# ====================================================

@app.get("/api/metrics")
def get_metrics():
    """Returns aggregated outreach KPIs for the active tenant."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    
    total_sent = conn.execute("SELECT COUNT(*) FROM emails WHERE direction = 'outbound' AND status = 'SENT'").fetchone()[0]
    total_opened = conn.execute("SELECT COUNT(DISTINCT email_id) FROM email_events WHERE event_type = 'OPENED'").fetchone()[0]
    total_clicked = conn.execute("SELECT COUNT(DISTINCT email_id) FROM email_events WHERE event_type = 'CLICKED'").fetchone()[0]
    total_replied = conn.execute("SELECT COUNT(DISTINCT email_id) FROM email_events WHERE event_type = 'REPLIED'").fetchone()[0]
    total_bounced = conn.execute("SELECT COUNT(DISTINCT email_id) FROM email_events WHERE event_type = 'BOUNCED'").fetchone()[0]
    total_meetings = conn.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
    
    # LinkedIn stats
    li_sent = conn.execute("SELECT COUNT(*) FROM linkedin_outreach").fetchone()[0]
    li_accepted = conn.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE accepted_at IS NOT NULL").fetchone()[0]
    li_replied = conn.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE replied_at IS NOT NULL").fetchone()[0]
    
    # Pipeline stats
    pipeline_stages = conn.execute("""
        SELECT current_stage, COUNT(*) as count
        FROM contacts GROUP BY current_stage
    """).fetchall()
    pipeline_breakdown = {r["current_stage"]: r["count"] for r in pipeline_stages}
    
    # Avg pipeline velocity (days from INGESTED to MEETING_BOOKED)
    velocity_rows = conn.execute("""
        SELECT created_at, updated_at FROM contacts WHERE current_stage = 'MEETING_BOOKED'
    """).fetchall()
    velocity_days = []
    for row in velocity_rows:
        try:
            created = datetime.fromisoformat(row["created_at"])
            updated = datetime.fromisoformat(row["updated_at"])
            velocity_days.append((updated - created).days)
        except Exception:
            pass
    avg_velocity = round(sum(velocity_days) / len(velocity_days), 1) if velocity_days else None
    
    # Load targets for this tenant
    tenant = conn.execute("SELECT * FROM metric_targets WHERE tenant_id = 'injexion'").fetchone()
    targets = dict(tenant) if tenant else {
        "target_open_rate": 0.30, "target_reply_rate": 0.05,
        "target_meeting_rate": 0.01, "target_bounce_rate": 0.02,
        "target_li_accept_rate": 0.25, "target_li_reply_rate": 0.08
    }
    
    # Recent emails (last 30 days)
    from_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    sent_30d = conn.execute("SELECT COUNT(*) FROM emails WHERE direction='outbound' AND status='SENT' AND sent_at >= ?", (from_30d,)).fetchone()[0]
    replied_30d = conn.execute("SELECT COUNT(DISTINCT email_id) FROM email_events WHERE event_type='REPLIED' AND created_at >= ?", (from_30d,)).fetchone()[0]

    conn.close()
    
    def rate(num, den): return round(num / den, 4) if den > 0 else 0.0

    return {
        "email": {
            "sent": total_sent,
            "opened": total_opened,
            "clicked": total_clicked,
            "replied": total_replied,
            "bounced": total_bounced,
            "open_rate": rate(total_opened, total_sent),
            "click_rate": rate(total_clicked, total_sent),
            "reply_rate": rate(total_replied, total_sent),
            "bounce_rate": rate(total_bounced, total_sent),
        },
        "meetings": total_meetings,
        "meeting_rate": rate(total_meetings, total_sent),
        "linkedin": {
            "sent": li_sent,
            "accepted": li_accepted,
            "replied": li_replied,
            "accept_rate": rate(li_accepted, li_sent),
            "reply_rate": rate(li_replied, li_sent),
        },
        "pipeline": {
            "breakdown": pipeline_breakdown,
            "avg_velocity_days": avg_velocity,
        },
        "last_30_days": {
            "sent": sent_30d,
            "replied": replied_30d,
            "reply_rate": rate(replied_30d, sent_30d),
        },
        "targets": targets,
    }


@app.put("/api/metrics/targets")
async def save_metric_targets(request: Request):
    """Saves user-defined KPI benchmark targets."""
    body = await request.json()
    tenant_id = body.get("tenant_id", "injexion")
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("""
        INSERT INTO metric_targets
        (tenant_id, target_open_rate, target_reply_rate, target_meeting_rate,
         target_bounce_rate, target_li_accept_rate, target_li_reply_rate, updated_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(tenant_id) DO UPDATE SET
            target_open_rate = excluded.target_open_rate,
            target_reply_rate = excluded.target_reply_rate,
            target_meeting_rate = excluded.target_meeting_rate,
            target_bounce_rate = excluded.target_bounce_rate,
            target_li_accept_rate = excluded.target_li_accept_rate,
            target_li_reply_rate = excluded.target_li_reply_rate,
            updated_at = excluded.updated_at
    """, (
        tenant_id,
        body.get("target_open_rate", 0.30),
        body.get("target_reply_rate", 0.05),
        body.get("target_meeting_rate", 0.01),
        body.get("target_bounce_rate", 0.02),
        body.get("target_li_accept_rate", 0.25),
        body.get("target_li_reply_rate", 0.08),
        now
    ))
    conn.commit()
    conn.close()
    return {"status": "saved"}


# ====================================================
# PREFAB TEMPLATES API
# ====================================================

@app.get("/api/templates")
def get_templates(tenant_id: str = None):
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    if tenant_id:
        rows = conn.execute(
            "SELECT * FROM prefab_messages WHERE tenant_id = ? OR tenant_id IS NULL ORDER BY category, name",
            (tenant_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM prefab_messages ORDER BY category, name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/templates")
async def create_template(request: Request):
    body = await request.json()
    template_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("""
        INSERT INTO prefab_messages
        (template_id, tenant_id, name, category, subject_line, body, variables, language, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        template_id, body.get("tenant_id"), body.get("name", "New Template"),
        body.get("category", "COLD"), body.get("subject_line"),
        body.get("body", ""), json.dumps(body.get("variables", [])),
        body.get("language", "NL"), now, now
    ))
    conn.commit()
    conn.close()
    return {"template_id": template_id, "status": "created"}


@app.put("/api/templates/{template_id}")
async def update_template(template_id: str, request: Request):
    body = await request.json()
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("""
        UPDATE prefab_messages SET
            name = ?, category = ?, subject_line = ?, body = ?,
            variables = ?, language = ?, updated_at = ?
        WHERE template_id = ?
    """, (
        body.get("name"), body.get("category"), body.get("subject_line"),
        body.get("body"), json.dumps(body.get("variables", [])),
        body.get("language", "NL"), now, template_id
    ))
    conn.commit()
    conn.close()
    return {"status": "updated"}


@app.delete("/api/templates/{template_id}")
def delete_template(template_id: str):
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("DELETE FROM prefab_messages WHERE template_id = ?", (template_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


# ====================================================
# STRATEGIES API
# ====================================================

@app.get("/api/strategies")
def get_strategies(tenant_id: str = None):
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    if tenant_id:
        rows = conn.execute("SELECT * FROM strategies WHERE tenant_id = ? ORDER BY created_at DESC", (tenant_id,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM strategies ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        s = dict(r)
        steps = conn.execute("SELECT * FROM strategy_steps WHERE strategy_id = ? ORDER BY step_number", (s["strategy_id"],)).fetchall()
        s["steps"] = [dict(st) for st in steps]
        result.append(s)
    conn.close()
    return result


@app.post("/api/strategies")
async def create_strategy(request: Request):
    body = await request.json()
    strategy_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    tenant_id = body.get("tenant_id", "injexion")
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    # Deactivate other strategies for this tenant first
    conn.execute("UPDATE strategies SET active = 0 WHERE tenant_id = ?", (tenant_id,))
    conn.execute("""
        INSERT INTO strategies (strategy_id, tenant_id, name, description, active, created_at)
        VALUES (?,?,?,?,1,?)
    """, (strategy_id, tenant_id, body.get("name", "New Strategy"), body.get("description", ""), now))
    # Insert steps
    for i, step in enumerate(body.get("steps", []), start=1):
        conn.execute("""
            INSERT INTO strategy_steps (step_id, strategy_id, step_number, step_type, delay_days, template_id, subject_line, stop_if)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            str(uuid.uuid4()), strategy_id, i,
            step.get("step_type", "EMAIL"),
            step.get("delay_days", 0),
            step.get("template_id"),
            step.get("subject_line"),
            step.get("stop_if", '["REPLIED","MEETING_BOOKED","OPT_OUT","BOUNCED"]')
        ))
    conn.commit()
    conn.close()
    return {"strategy_id": strategy_id, "status": "created"}


@app.post("/api/strategies/{strategy_id}/activate")
def activate_strategy(strategy_id: str):
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    s = conn.execute("SELECT tenant_id FROM strategies WHERE strategy_id = ?", (strategy_id,)).fetchone()
    if not s:
        conn.close()
        raise HTTPException(status_code=404, detail="Strategy not found")
    conn.execute("UPDATE strategies SET active = 0 WHERE tenant_id = ?", (s[0],))
    conn.execute("UPDATE strategies SET active = 1 WHERE strategy_id = ?", (strategy_id,))
    conn.commit()
    conn.close()
    return {"status": "activated"}


@app.get("/api/tracking/events")
def get_tracking_events(email_id: str = None, limit: int = 50):
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    if email_id:
        rows = conn.execute(
            "SELECT * FROM email_events WHERE email_id = ? ORDER BY created_at DESC LIMIT ?",
            (email_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM email_events ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/")
def get_dashboard_index():
    html_content = """
    <!DOCTYPE html>
    <html lang="en" class="h-full bg-slate-950 text-slate-100">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Odysseus - Agent Mission Control Dashboard</title>
        <!-- Tailwind CSS & FontAwesome -->
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
        <!-- Canvas Confetti for booking celebrations! -->
        <script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.6.0/dist/confetti.browser.min.js"></script>
        <script>
            tailwind.config = {
                theme: {
                    extend: {
                        colors: {
                            brand: {
                                50: '#f0f4ff',
                                100: '#d9e2ff',
                                500: '#3b82f6',
                                600: '#2563eb',
                                950: '#030712',
                            }
                        }
                    }
                }
            }
        </script>
        <style>
            .glowing-cyan {
                box-shadow: 0 0 15px rgba(34, 211, 238, 0.4);
                animation: pulse-glowing 2s infinite alternate;
            }
            .glowing-amber {
                box-shadow: 0 0 15px rgba(245, 158, 11, 0.4);
                animation: pulse-glowing 2s infinite alternate;
            }
            .glowing-red {
                box-shadow: 0 0 15px rgba(239, 68, 68, 0.4);
                animation: pulse-glowing 1s infinite alternate;
            }
            @keyframes pulse-glowing {
                0% { transform: scale(1); opacity: 0.95; }
                100% { transform: scale(1.02); opacity: 1; }
            }
            .agent-line {
                stroke-dasharray: 8;
                animation: dash 30s linear infinite;
            }
            @keyframes dash {
                to { stroke-dashoffset: -1000; }
            }
            /* Custom Scrollbar */
            ::-webkit-scrollbar { width: 6px; height: 6px; }
            ::-webkit-scrollbar-track { background: rgba(15, 23, 42, 0.8); }
            ::-webkit-scrollbar-thumb { background: rgba(59, 130, 246, 0.4); border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: rgba(59, 130, 246, 0.7); }
        </style>
    </head>
    <body class="h-full flex flex-col font-sans antialiased overflow-hidden selection:bg-brand-500 selection:text-white">

        <!-- Top Header Navigation -->
        <header class="flex-shrink-0 bg-slate-900/90 border-b border-slate-800/80 backdrop-blur px-6 py-4 flex items-center justify-between shadow-lg">
            <div class="flex items-center space-x-3">
                <div class="w-10 h-10 bg-gradient-to-tr from-brand-600 to-cyan-400 rounded-xl flex items-center justify-center shadow-lg shadow-brand-500/20">
                    <i class="fa-solid fa-compass-drafting text-lg text-white"></i>
                </div>
                <div>
                    <h1 class="text-xl font-bold tracking-tight bg-gradient-to-r from-white to-slate-400 bg-clip-text text-transparent flex items-center gap-2">
                        Odysseus <span class="text-xs bg-brand-500/10 text-brand-400 border border-brand-500/30 px-2 py-0.5 rounded-full font-medium tracking-wide uppercase">Mission Control</span>
                    </h1>
                    <p class="text-xs text-slate-500">Autonomous Multi-Agent Sales & Lead Pipeline</p>
                </div>
            </div>

            <div class="flex items-center space-x-6">
                <!-- Clock / Date -->
                <div class="text-right hidden sm:block">
                    <span class="text-sm font-semibold block text-slate-300" id="current-time">15:17:00 UTC</span>
                    <span class="text-[10px] text-slate-500 block uppercase tracking-wider font-semibold">Monday, June 8, 2026</span>
                </div>
                
                <!-- SSE Connection indicator -->
                <div class="flex items-center space-x-2 bg-slate-950 border border-slate-800 px-3.5 py-2 rounded-xl">
                    <span id="sse-status-dot" class="w-2.5 h-2.5 bg-red-500 rounded-full inline-block animate-pulse"></span>
                    <span id="sse-status-text" class="text-xs font-semibold text-slate-400 uppercase tracking-widest">DISCONNECTED</span>
                </div>
            </div>
        </header>

        <!-- Main Body Workspace Split -->
        <main class="flex-grow flex overflow-hidden min-h-0 bg-slate-950">
            <!-- Left Navigation Sidebar -->
            <aside class="w-64 bg-slate-900 border-r border-slate-800/80 flex flex-col justify-between py-6">
                <div class="space-y-1.5 px-3">
                    <button onclick="switchTab('tab-control')" id="btn-tab-control" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold transition bg-brand-600 text-white shadow-lg shadow-brand-500/10">
                        <i class="fa-solid fa-gamepad text-base w-5"></i>
                        <span>Dashboard & Control</span>
                    </button>
                    <button onclick="switchTab('tab-crm')" id="btn-tab-crm" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-users text-base w-5"></i>
                        <span>Prospect Database (CRM)</span>
                    </button>
                    <button onclick="switchTab('tab-deliverability')" id="btn-tab-deliverability" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-shield-halved text-base w-5"></i>
                        <span>Deliverability Monitor</span>
                    </button>
                    <button onclick="switchTab('tab-meetings')" id="btn-tab-meetings" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-calendar-days text-base w-5"></i>
                        <span>Google Bookings</span>
                    </button>
                    <button onclick="switchTab('tab-feed')" id="btn-tab-feed" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-terminal text-base w-5"></i>
                        <span>Agent Log Feed</span>
                    </button>
                    <button onclick="switchTab('tab-linkedin')" id="btn-tab-linkedin" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-brands fa-linkedin text-base w-5"></i>
                        <span>LinkedIn Outreach</span>
                    </button>
                    <button onclick="switchTab('tab-posts')" id="btn-tab-posts" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-pen-to-square text-base w-5"></i>
                        <span>LinkedIn Posts</span>
                    </button>
                    <button onclick="switchTab('tab-settings')" id="btn-tab-settings" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-gear text-base w-5"></i>
                        <span>Settings & ICP</span>
                    </button>
                    <button onclick="switchTab('tab-metrics')" id="btn-tab-metrics" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-chart-line text-base w-5"></i>
                        <span>Success Metrics</span>
                    </button>
                    <button onclick="switchTab('tab-templates')" id="btn-tab-templates" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-layer-group text-base w-5"></i>
                        <span>Prefab Berichten</span>
                    </button>
                    <button onclick="switchTab('tab-strategy')" id="btn-tab-strategy" class="tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition">
                        <i class="fa-solid fa-chess-knight text-base w-5"></i>
                        <span>Strategy Builder</span>
                    </button>
                </div>
                
                <!-- Quick stats summary widget -->
                <div class="px-6 space-y-4">
                    <div class="border-t border-slate-800/80 pt-6">
                        <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">Metrics Overview</span>
                        <div class="grid grid-cols-2 gap-3 mt-3">
                            <div class="bg-slate-950 p-3 rounded-xl border border-slate-800/50">
                                <span class="text-[10px] text-slate-400 font-medium block">Total Leads</span>
                                <span id="stat-total-leads" class="text-lg font-extrabold text-white mt-1 block">0</span>
                            </div>
                            <div class="bg-slate-950 p-3 rounded-xl border border-slate-800/50">
                                <span class="text-[10px] text-slate-400 font-medium block">Meetings</span>
                                <span id="stat-meetings" class="text-lg font-extrabold text-emerald-400 mt-1 block">0</span>
                            </div>
                        </div>
                    </div>
                </div>
            </aside>

            <!-- Dashboard Content Container (Tabs) -->
            <section class="flex-grow flex flex-col overflow-y-auto p-8 relative">
                <!-- Notifications Container -->
                <div id="toast-container" class="fixed bottom-6 right-6 z-50 flex flex-col space-y-2"></div>

                <!-- 1. DASHBOARD & CONTROL TAB -->
                <div id="tab-control" class="tab-content space-y-8 block">
                    <!-- Top Summary Stats Grid -->
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl flex items-center justify-between shadow-md">
                            <div>
                                <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Active Leads</span>
                                <span id="stat-active-leads" class="text-3xl font-extrabold text-white mt-1 block">0</span>
                            </div>
                            <div class="w-12 h-12 bg-blue-500/10 text-blue-400 border border-blue-500/20 rounded-xl flex items-center justify-center">
                                <i class="fa-solid fa-people-arrows text-xl"></i>
                            </div>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl flex items-center justify-between shadow-md">
                            <div>
                                <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Outbound Sends</span>
                                <span id="stat-outbound-sends" class="text-3xl font-extrabold text-indigo-400 mt-1 block">0</span>
                            </div>
                            <div class="w-12 h-12 bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 rounded-xl flex items-center justify-center">
                                <i class="fa-solid fa-paper-plane text-xl"></i>
                            </div>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl flex items-center justify-between shadow-md">
                            <div>
                                <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Domain Health</span>
                                <span class="text-3xl font-extrabold text-emerald-400 mt-1 block">100%</span>
                            </div>
                            <div class="w-12 h-12 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-xl flex items-center justify-center">
                                <i class="fa-solid fa-heart-pulse text-xl"></i>
                            </div>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl flex items-center justify-between shadow-md">
                            <div>
                                <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Avg BANT Score</span>
                                <span id="stat-avg-score" class="text-3xl font-extrabold text-cyan-400 mt-1 block">0/100</span>
                            </div>
                            <div class="w-12 h-12 bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 rounded-xl flex items-center justify-center">
                                <i class="fa-solid fa-bolt text-xl"></i>
                            </div>
                        </div>
                    </div>

                    <!-- Live Agents Grid (Mission Status) -->
                    <div>
                        <h2 class="text-lg font-bold text-white mb-4 flex items-center gap-2">
                            <i class="fa-solid fa-robot text-brand-400"></i> Active Agent Fleet
                        </h2>
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-6" id="agent-status-cards">
                            <!-- Cards will be populated dynamically -->
                        </div>
                    </div>

                    <!-- Main Graph and Handoff Section -->
                    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-stretch">
                        <!-- Handoff Flow Diagram -->
                        <div class="lg:col-span-8 bg-slate-900 border border-slate-800/70 p-6 rounded-2xl flex flex-col shadow-md">
                            <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Pipeline Handoff System</h3>
                            <div class="flex-grow flex items-center justify-center p-4 bg-slate-950 border border-slate-800/50 rounded-xl relative overflow-hidden" style="min-height: 280px;">
                                <!-- Live SVG Flow Line Connectors -->
                                <svg class="absolute inset-0 w-full h-full pointer-events-none" id="handoff-svg-canvas">
                                    <defs>
                                        <!-- Glow Filter -->
                                        <filter id="glow-line" x="-20%" y="-20%" width="140%" height="140%">
                                            <feGaussianBlur stdDeviation="3" result="blur" />
                                            <feMerge>
                                                <feMergeNode in="blur" />
                                                <feMergeNode in="SourceGraphic" />
                                            </feMerge>
                                        </filter>
                                    </defs>
                                </svg>
                                
                                <div class="relative w-full flex justify-between items-center px-4 max-w-4xl flex-wrap gap-y-12">
                                    <!-- Pipeline nodes -->
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-ingest" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-file-import text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Ingest</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-research" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-magnifying-glass text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Research</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-deliver" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-envelope-circle-check text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Deliverability</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-opp" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-map-location-dot text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Opp Map</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-qual" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-filter-list text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Qualification</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-outreach" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-pen-nib text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Outreach</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-review" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-user-shield text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Human Review</span>
                                    </div>
                                    <div class="flex flex-col items-center justify-center space-y-2 z-10 w-24">
                                        <div id="node-inbound" class="w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300">
                                            <i class="fa-solid fa-reply text-sm"></i>
                                        </div>
                                        <span class="text-[10px] text-slate-400 font-bold uppercase tracking-wider text-center">Inbox Agent</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Live Console activity feed snippet -->
                        <div class="lg:col-span-4 bg-slate-900 border border-slate-800/70 p-6 rounded-2xl flex flex-col shadow-md">
                            <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Mission Console Log</h3>
                            <div class="flex-grow bg-slate-950 font-mono text-xs p-4 rounded-xl border border-slate-800/60 overflow-y-auto space-y-2 text-slate-400 flex flex-col" style="max-height: 280px; min-height: 280px;" id="console-stream-log">
                                <!-- Streams events dynamically -->
                            </div>
                        </div>
                    </div>

                    <!-- Active lead pipeline queue -->
                    <div>
                        <h2 class="text-lg font-bold text-white mb-4">Live Pipeline Leads</h2>
                        <div class="bg-slate-900 border border-slate-800/70 rounded-2xl overflow-hidden shadow-md">
                            <div class="overflow-x-auto">
                                <table class="w-full text-left border-collapse">
                                    <thead>
                                        <tr class="border-b border-slate-800 bg-slate-950 text-[10px] text-slate-400 uppercase font-bold tracking-widest">
                                            <th class="py-4 px-6">Prospect Details</th>
                                            <th class="py-4 px-6">Company</th>
                                            <th class="py-4 px-6">Email / Domain</th>
                                            <th class="py-4 px-6">Current Stage</th>
                                            <th class="py-4 px-6">BANT Score</th>
                                            <th class="py-4 px-6">Outreach Trigger</th>
                                            <th class="py-4 px-6 text-right">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody id="active-leads-table-rows" class="text-sm divide-y divide-slate-800/60">
                                        <!-- Lead entries inserted here -->
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- 2. PROSPECT DATABASE (CRM) TAB -->
                <div id="tab-crm" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white">Prospect Database</h2>
                        <span class="text-xs text-slate-500 font-semibold uppercase tracking-wider">All Records saved in SQLite</span>
                    </div>
                    
                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6">
                        <div class="overflow-x-auto">
                            <table class="w-full text-left border-collapse">
                                <thead>
                                    <tr class="border-b border-slate-800 text-[10px] text-slate-400 uppercase font-bold tracking-widest bg-slate-950/20">
                                        <th class="py-4 px-6">ID</th>
                                        <th class="py-4 px-6">Name & Role</th>
                                        <th class="py-4 px-6">Email Address</th>
                                        <th class="py-4 px-6">Company</th>
                                        <th class="py-4 px-6">Lifecycle Stage</th>
                                        <th class="py-4 px-6">Priority</th>
                                        <th class="py-4 px-6">Last Updated</th>
                                    </tr>
                                </thead>
                                <tbody id="crm-table-rows" class="text-sm divide-y divide-slate-800/60">
                                    <!-- SQLite database rows dynamically loaded -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- 3. DELIVERABILITY TAB -->
                <div id="tab-deliverability" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white">Domain Deliverability Monitor</h2>
                        <span class="text-xs bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 px-3 py-1 rounded-full font-bold uppercase tracking-wider">SPF / DKIM / MX Verified</span>
                    </div>
                    
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
                        <!-- Stat details -->
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase">Monitored Domains</span>
                            <span id="stat-monitored-domains-count" class="text-3xl font-extrabold text-white mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase">Total Aggregated Sends</span>
                            <span id="stat-total-sending" class="text-3xl font-extrabold text-white mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase">Total Replies Received</span>
                            <span id="stat-total-replies" class="text-3xl font-extrabold text-white mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase">Average Warmup Score</span>
                            <span class="text-3xl font-extrabold text-emerald-400 mt-2 block">98.5%</span>
                        </div>
                    </div>

                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6">
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Domain Performance Log</h3>
                        <div class="overflow-x-auto">
                            <table class="w-full text-left border-collapse">
                                <thead>
                                    <tr class="border-b border-slate-800 text-[10px] text-slate-400 uppercase font-bold tracking-widest bg-slate-950/20">
                                        <th class="py-4 px-6">Domain</th>
                                        <th class="py-4 px-6">Outbound Sends</th>
                                        <th class="py-4 px-6">Replies</th>
                                        <th class="py-4 px-6">Bounces</th>
                                        <th class="py-4 px-6">Bounce Rate</th>
                                        <th class="py-4 px-6">Domain Health</th>
                                        <th class="py-4 px-6">Last Synced</th>
                                    </tr>
                                </thead>
                                <tbody id="deliverability-table-rows" class="text-sm divide-y divide-slate-800/60">
                                    <!-- SQLite rows -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- 4. MEETINGS TAB -->
                <div id="tab-meetings" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white">Google Calendar Bookings</h2>
                        <span class="text-xs text-slate-500 font-semibold uppercase tracking-wider">Syncs with Google Calendar API</span>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 gap-6" id="calendar-bookings-grid">
                        <!-- Bookings loaded here -->
                    </div>
                </div>

                <!-- 5. AGENT LOG FEED TAB -->
                <div id="tab-feed" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white">Comprehensive Agent Log Feed</h2>
                        <span class="text-xs text-slate-500 font-semibold uppercase tracking-wider">Audit Log for GDPR compliance</span>
                    </div>

                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6">
                        <div class="space-y-4 max-h-[500px] overflow-y-auto pr-4" id="comprehensive-activity-feed">
                            <!-- Chronological audits -->
                        </div>
                    </div>
                </div>

                <!-- 6. LINKEDIN OUTREACH TAB -->
                <div id="tab-linkedin" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white flex items-center gap-3">
                            <i class="fa-brands fa-linkedin text-blue-500"></i> LinkedIn Outreach
                        </h2>
                        <div class="flex items-center gap-3">
                            <div id="li-login-status" class="flex items-center gap-2 bg-slate-950 border border-slate-800 px-3 py-2 rounded-xl">
                                <span id="li-login-dot" class="w-2.5 h-2.5 bg-red-500 rounded-full inline-block"></span>
                                <span id="li-login-text" class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Not Connected</span>
                            </div>
                            <button onclick="linkedinLogin()" id="li-login-btn" class="bg-blue-600 hover:bg-blue-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl shadow-lg shadow-blue-500/15 transition flex items-center gap-2">
                                <i class="fa-brands fa-linkedin"></i> Login to LinkedIn
                            </button>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Total Sent</span>
                            <span id="li-stat-sent" class="text-3xl font-extrabold text-blue-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Success</span>
                            <span id="li-stat-success" class="text-3xl font-extrabold text-emerald-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Not Found</span>
                            <span id="li-stat-notfound" class="text-3xl font-extrabold text-amber-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl shadow-md">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Errors</span>
                            <span id="li-stat-error" class="text-3xl font-extrabold text-red-400 mt-2 block">0</span>
                        </div>
                    </div>

                    <div id="followup-section" class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6 hidden">
                        <div class="flex justify-between items-center mb-4">
                            <h3 class="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
                                <i class="fa-solid fa-paper-plane text-emerald-400"></i> Follow-up Messages (Ready to Paste)
                            </h3>
                            <button onclick="loadFollowups()" class="text-xs text-blue-400 hover:text-blue-300 font-semibold">Refresh</button>
                        </div>
                        <div id="followup-list" class="space-y-4">
                            <p class="text-slate-500 text-sm">No accepted connections awaiting follow-up.</p>
                        </div>
                    </div>

                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6">
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Connection Request History</h3>
                        <div class="overflow-x-auto">
                            <table class="w-full text-left border-collapse">
                                <thead>
                                    <tr class="border-b border-slate-800 text-[10px] text-slate-400 uppercase font-bold tracking-widest bg-slate-950/20">
                                        <th class="py-4 px-6">Person</th>
                                        <th class="py-4 px-6">Company</th>
                                        <th class="py-4 px-6">Note (180 char)</th>
                                        <th class="py-4 px-6">Outcome</th>
                                        <th class="py-4 px-6">Time</th>
                                    </tr>
                                </thead>
                                <tbody id="linkedin-table-rows" class="text-sm divide-y divide-slate-800/60">
                                    <!-- LinkedIn outreach rows -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- 7. LINKEDIN POSTS TAB -->
                <div id="tab-posts" class="tab-content space-y-6 hidden">
                    <div class="flex justify-between items-center">
                        <h2 class="text-2xl font-bold text-white flex items-center gap-3">
                            <i class="fa-solid fa-pen-to-square text-blue-400"></i> LinkedIn Posts
                        </h2>
                        <div class="flex items-center gap-3">
                            <div id="post-scheduler-status" class="flex items-center gap-2 bg-slate-950 border border-slate-800 px-3 py-2 rounded-xl">
                                <span id="post-sched-dot" class="w-2.5 h-2.5 bg-red-500 rounded-full inline-block"></span>
                                <span id="post-sched-text" class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Stopped</span>
                            </div>
                            <button onclick="togglePostScheduler()" id="post-sched-btn" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition">
                                <i class="fa-solid fa-play"></i> Start Scheduler
                            </button>
                        </div>
                    </div>

                    <!-- Post stats -->
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Drafts</span>
                            <span id="post-stat-draft" class="text-3xl font-extrabold text-slate-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Queued</span>
                            <span id="post-stat-queued" class="text-3xl font-extrabold text-blue-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Posted</span>
                            <span id="post-stat-posted" class="text-3xl font-extrabold text-emerald-400 mt-2 block">0</span>
                        </div>
                        <div class="bg-slate-900 border border-slate-800/70 p-5 rounded-2xl">
                            <span class="text-xs text-slate-400 font-semibold block uppercase tracking-wider">Failed</span>
                            <span id="post-stat-failed" class="text-3xl font-extrabold text-red-400 mt-2 block">0</span>
                        </div>
                    </div>

                    <!-- Add new post -->
                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl p-6">
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-4">Add New Post</h3>
                        <textarea id="new-post-content" class="w-full h-32 bg-slate-950 border border-slate-800 p-4 rounded-xl text-sm focus:outline-none focus:border-blue-500/80 text-white resize-none" placeholder="Write your LinkedIn post here..."></textarea>
                        <div class="flex justify-between items-center mt-3">
                            <div class="flex gap-3">
                                <input id="new-post-topic" type="text" bg-slate-950 border border-slate-800 px-3 py-2 rounded-xl text-sm text-white placeholder="Topic (optional)" class="bg-slate-950 border border-slate-800 px-3 py-2 rounded-xl text-sm text-white">
                                <input id="new-post-schedule" type="datetime-local" class="bg-slate-950 border border-slate-800 px-3 py-2 rounded-xl text-sm text-white">
                            </div>
                            <div class="flex gap-2">
                                <button onclick="addNewPost('draft')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 font-semibold text-sm px-4 py-2 rounded-xl transition">Save Draft</button>
                                <button onclick="addNewPost('queued')" class="bg-blue-600 hover:bg-blue-500 text-white font-semibold text-sm px-4 py-2 rounded-xl transition">Add to Queue</button>
                            </div>
                        </div>
                    </div>

                    <!-- Posts queue table -->
                    <div class="bg-slate-900 border border-slate-800/70 rounded-2xl overflow-hidden">
                        <div class="overflow-x-auto">
                            <table class="w-full text-left">
                                <thead class="bg-slate-950/60 border-b border-slate-800/70">
                                    <tr>
                                        <th class="py-4 px-6 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Topic</th>
                                        <th class="py-4 px-6 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Content</th>
                                        <th class="py-4 px-6 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Scheduled</th>
                                        <th class="py-4 px-6 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Status</th>
                                        <th class="py-4 px-6 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Actions</th>
                                    </tr>
                                </thead>
                                <tbody id="posts-table-rows" class="text-sm divide-y divide-slate-800/60">
                                    <!-- Posts rows -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            <!-- SETTINGS & ICP TAB -->
                <div id="tab-settings" class="tab-content space-y-8 hidden">
                    
                    <!-- Tenant Switcher -->
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
                        <div class="flex items-center gap-3 mb-6">
                            <i class="fa-solid fa-building text-brand-400 text-lg"></i>
                            <div>
                                <h2 class="text-base font-extrabold text-white">Active Company Profile</h2>
                                <p class="text-xs text-slate-500 mt-0.5">Switch between company profiles. All agents will use the active profile's ICP and value doctrine.</p>
                            </div>
                        </div>
                        <div id="tenant-switcher" class="flex gap-3 flex-wrap mb-4"></div>
                        <div class="text-xs text-amber-400 flex items-center gap-2 mt-2">
                            <i class="fa-solid fa-triangle-exclamation"></i>
                            <span>Switching profiles immediately affects all new leads and outreach. Currently running leads are not affected.</span>
                        </div>
                    </div>

                    <!-- ICP & Brand Settings Form -->
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
                        <div class="flex items-center justify-between mb-6">
                            <div class="flex items-center gap-3">
                                <i class="fa-solid fa-sliders text-brand-400 text-lg"></i>
                                <div>
                                    <h2 class="text-base font-extrabold text-white">ICP & Outreach Configuration</h2>
                                    <p class="text-xs text-slate-500 mt-0.5">Fill these in to activate all 7 agents for your company.</p>
                                </div>
                            </div>
                            <button onclick="saveSettings()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition flex items-center gap-2">
                                <i class="fa-solid fa-floppy-disk"></i> Save Configuration
                            </button>
                        </div>

                        <input type="hidden" id="settings-tenant-id" value="">
                        
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                            
                            <!-- Company Name -->
                            <div>
                                <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Company / Profile Name</label>
                                <input id="settings-display-name" type="text" placeholder="e.g. Injexion" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80">
                            </div>

                            <!-- Sending Email -->
                            <div>
                                <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Sending Email Address <span class="text-red-400">*</span></label>
                                <input id="settings-sending-email" type="email" placeholder="e.g. marvin@injexion.io" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80">
                                <p class="text-[10px] text-slate-500 mt-1">The email address that will send outreach. Must be set up with Gmail OAuth or SMTP.</p>
                            </div>

                            <!-- Calendar Link -->
                            <div>
                                <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Calendar / Booking Link <span class="text-red-400">*</span></label>
                                <input id="settings-calendar-link" type="url" placeholder="e.g. cal.com/injexion" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80">
                            </div>

                            <!-- ICP Company Size -->
                            <div>
                                <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Target Company Size (employees)</label>
                                <input id="settings-company-size" type="text" placeholder="e.g. 10-200" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80">
                            </div>

                            <!-- Gmail Authentication -->
                            <div class="col-span-1 md:col-span-2 bg-slate-800/50 rounded-xl p-4 border border-slate-700/50 mt-2">
                                <div class="flex items-center justify-between">
                                    <div>
                                        <h3 class="text-sm font-bold text-white mb-1"><i class="fa-brands fa-google text-brand-400 mr-2"></i>Gmail OAuth Connection</h3>
                                        <p class="text-xs text-slate-400">Connect the Google Workspace account for this profile to enable live sending.</p>
                                    </div>
                                    <button onclick="connectGmail()" type="button" class="bg-white hover:bg-slate-100 text-slate-900 font-bold text-xs px-4 py-2 rounded-lg transition flex items-center gap-2">
                                        <i class="fa-solid fa-link"></i> Authenticate Gmail
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- Value Doctrine -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Value Doctrine — What do you sell? <span class="text-red-400">*</span></label>
                            <textarea id="settings-value-doctrine" rows="4" placeholder="Describe your product/service in 3-5 sentences. The AI uses this to write every email and map pain points. Example: 'Injexion builds autonomous B2B lead generation systems for Dutch SMEs. We combine AI research agents, LinkedIn automation, and personalized email outreach into a single pipeline...'" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                            <p class="text-[10px] text-slate-500 mt-1">This is the most important field. The better you describe your offer, the more relevant every outbound email will be.</p>
                        </div>
                        
                        <!-- Email Signature Block -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Email Signature Block <span class="text-red-400">*</span></label>
                            <textarea id="settings-signature" rows="4" placeholder="Your Name\nCompany Name\nhttps://yourwebsite.com\n+31 6 12345678" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                        </div>

                        <!-- ICP Target Industries -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Target Industries (one per line)</label>
                            <textarea id="settings-industries" rows="4" placeholder="Groothandel\nLogistiek & Transport\nB2B SaaS\nIT Dienstverlening" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                        </div>

                        <!-- ICP Decision Maker Titles -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Target Decision-Maker Titles (one per line)</label>
                            <textarea id="settings-roles" rows="3" placeholder="Directeur\nCEO\nEigenaar\nOprichter" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                        </div>

                        <!-- Lead Sourcing Queries -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Lead Sourcing Queries — Agent Zero searches (one per line)</label>
                            <textarea id="settings-queries" rows="6" placeholder="groothandel B2B Nederland\nlogistiek dienstverlener Nederland\ntransport bedrijf Nederland MKB\nIT consultancy Nederland MKB" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                            <p class="text-[10px] text-slate-500 mt-1">These are the exact search queries Agent Zero uses to discover leads. Be specific: include industry + country + size signal (e.g. "MKB").</p>
                        </div>

                        <!-- Brand Voice -->
                        <div class="mt-6">
                            <label class="text-[11px] text-slate-400 font-bold uppercase tracking-wider block mb-1.5">Brand Voice Instructions</label>
                            <textarea id="settings-brand-voice" rows="2" placeholder="Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie). Max 120 woorden per email." class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                        </div>

                        <!-- Setup Status Check -->
                        <div id="settings-status" class="mt-6 hidden"></div>

                    </div>
                </div>

                <!-- SUCCESS METRICS TAB -->
                <div id="tab-metrics" class="tab-content space-y-6 hidden">
                    <div class="flex items-center justify-between mb-2">
                        <div>
                            <h2 class="text-base font-extrabold text-white">Success Metrics</h2>
                            <p class="text-xs text-slate-500 mt-0.5">Live performance vs. your defined targets. Green = beating target. Red = below target.</p>
                        </div>
                        <button onclick="loadMetrics()" class="bg-slate-800 hover:bg-slate-700 text-slate-300 font-bold text-xs px-4 py-2 rounded-xl transition flex items-center gap-2">
                            <i class="fa-solid fa-rotate-right"></i> Refresh
                        </button>
                    </div>

                    <!-- Email KPI cards -->
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-4" id="metrics-cards">
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Open Rate</p>
                            <p id="m-open-rate" class="text-3xl font-black text-white">—</p>
                            <p id="m-open-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Reply Rate</p>
                            <p id="m-reply-rate" class="text-3xl font-black text-white">—</p>
                            <p id="m-reply-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Meeting Rate</p>
                            <p id="m-meeting-rate" class="text-3xl font-black text-white">—</p>
                            <p id="m-meeting-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Bounce Rate</p>
                            <p id="m-bounce-rate" class="text-3xl font-black text-white">—</p>
                            <p id="m-bounce-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                    </div>

                    <!-- LinkedIn KPI cards -->
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Emails Sent</p>
                            <p id="m-total-sent" class="text-3xl font-black text-cyan-400">—</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">LI Accept Rate</p>
                            <p id="m-li-accept" class="text-3xl font-black text-white">—</p>
                            <p id="m-li-accept-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">LI Reply Rate</p>
                            <p id="m-li-reply" class="text-3xl font-black text-white">—</p>
                            <p id="m-li-reply-target" class="text-[10px] text-slate-500 mt-1">Target: —</p>
                        </div>
                        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
                            <p class="text-[10px] text-slate-500 font-bold uppercase tracking-wider mb-1">Avg Velocity</p>
                            <p id="m-velocity" class="text-3xl font-black text-white">—</p>
                            <p class="text-[10px] text-slate-500 mt-1">days to meeting</p>
                        </div>
                    </div>

                    <!-- Target Editor -->
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
                        <h3 class="text-sm font-extrabold text-white mb-4"><i class="fa-solid fa-bullseye text-brand-400 mr-2"></i>Define Your Targets</h3>
                        <div class="grid grid-cols-2 md:grid-cols-3 gap-4">
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Open Rate Target</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-open" type="number" step="1" min="0" max="100" value="30" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Reply Rate Target</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-reply" type="number" step="1" min="0" max="100" value="5" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Meeting Rate Target</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-meeting" type="number" step="0.5" min="0" max="100" value="1" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Max Bounce Rate</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-bounce" type="number" step="0.5" min="0" max="100" value="2" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">LI Accept Rate Target</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-li-accept" type="number" step="1" min="0" max="100" value="25" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">LI Reply Rate Target</label>
                                <div class="flex items-center gap-2">
                                    <input id="t-li-reply" type="number" step="1" min="0" max="100" value="8" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <span class="text-slate-400 text-sm">%</span>
                                </div>
                            </div>
                        </div>
                        <button onclick="saveTargets()" class="mt-4 bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition flex items-center gap-2">
                            <i class="fa-solid fa-floppy-disk"></i> Save Targets
                        </button>
                    </div>
                </div>

                <!-- PREFAB BERICHTEN TAB -->
                <div id="tab-templates" class="tab-content space-y-6 hidden">
                    <div class="flex items-center justify-between mb-2">
                        <div>
                            <h2 class="text-base font-extrabold text-white">Prefab Berichten</h2>
                            <p class="text-xs text-slate-500 mt-0.5">Pre-written Dutch outreach templates. The AI uses these as a structure and personalizes with research.</p>
                        </div>
                        <button onclick="showNewTemplateForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
                            <i class="fa-solid fa-plus"></i> New Template
                        </button>
                    </div>

                    <!-- Template Editor (hidden by default) -->
                    <div id="template-editor" class="hidden bg-slate-900 border border-slate-800 rounded-2xl p-6 space-y-4">
                        <input type="hidden" id="edit-template-id">
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Template Name</label>
                                <input id="edit-tmpl-name" type="text" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Category</label>
                                <select id="edit-tmpl-category" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                                    <option value="COLD">Cold Email</option>
                                    <option value="FOLLOWUP">Follow-up</option>
                                    <option value="SOCIAL_PROOF">Social Proof</option>
                                    <option value="LINKEDIN_NOTE">LinkedIn Note</option>
                                </select>
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Subject Line</label>
                                <input id="edit-tmpl-subject" type="text" placeholder="{{company_name}} — korte vraag" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                            </div>
                        </div>
                        <div>
                            <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Body — use {{variable_name}} for placeholders</label>
                            <textarea id="edit-tmpl-body" rows="10" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white font-mono resize-y focus:outline-none focus:border-brand-500/80"></textarea>
                        </div>
                        <div class="flex gap-3">
                            <button onclick="saveTemplate()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save</button>
                            <button onclick="document.getElementById('template-editor').classList.add('hidden')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
                        </div>
                    </div>

                    <!-- Template cards -->
                    <div id="templates-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
                </div>

                <!-- STRATEGY BUILDER TAB -->
                <div id="tab-strategy" class="tab-content space-y-6 hidden">
                    <div class="flex items-center justify-between mb-2">
                        <div>
                            <h2 class="text-base font-extrabold text-white">Strategy Builder</h2>
                            <p class="text-xs text-slate-500 mt-0.5">Define multi-step outreach sequences. Leads automatically move through steps unless they reply, opt out, or book a meeting.</p>
                        </div>
                        <button onclick="showNewStrategyForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
                            <i class="fa-solid fa-plus"></i> New Strategy
                        </button>
                    </div>

                    <!-- Strategy Creator Form -->
                    <div id="strategy-editor" class="hidden bg-slate-900 border border-slate-800 rounded-2xl p-6 space-y-6">
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Strategy Name</label>
                                <input id="strat-name" type="text" placeholder="e.g. Standard 4-Step NL" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                            </div>
                            <div>
                                <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Description</label>
                                <input id="strat-desc" type="text" placeholder="What is this strategy for?" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white">
                            </div>
                        </div>

                        <div>
                            <div class="flex items-center justify-between mb-3">
                                <h3 class="text-sm font-bold text-white">Steps</h3>
                                <button onclick="addStrategyStep()" class="text-xs text-brand-400 hover:text-brand-300 font-bold"><i class="fa-solid fa-plus mr-1"></i>Add Step</button>
                            </div>
                            <div id="strategy-steps-list" class="space-y-3"></div>
                        </div>

                        <div class="flex gap-3">
                            <button onclick="saveStrategy()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save & Activate</button>
                            <button onclick="document.getElementById('strategy-editor').classList.add('hidden')" class="bg-slate-800 hover:bg-slate-700 text-slate-300 font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
                        </div>
                    </div>

                    <!-- Existing Strategies -->
                    <div id="strategies-list" class="space-y-4"></div>
                </div>

            </section>
        </main>

        <!-- LEAD DETAILED INSPECTOR MODAL (DRAWER) -->
        <div id="drawer-lead" class="fixed inset-y-0 right-0 w-[680px] bg-slate-900 border-l border-slate-800 shadow-2xl z-40 transform translate-x-full transition-transform duration-300 flex flex-col">
            <!-- Header -->
            <div class="p-6 border-b border-slate-800 flex items-center justify-between bg-slate-950/40">
                <div>
                    <span id="drawer-stage-badge" class="text-[10px] font-extrabold uppercase px-2.5 py-1 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 tracking-wider">INGESTED</span>
                    <h2 id="drawer-lead-name" class="text-xl font-black text-white mt-2.5">Willem Jansen</h2>
                    <p id="drawer-lead-sub" class="text-xs text-slate-400">Dentist & Eigenaar at Tandartsenpark Utrecht</p>
                </div>
                <button onclick="closeLeadDrawer()" class="w-10 h-10 hover:bg-slate-800 rounded-full flex items-center justify-center text-slate-400 hover:text-white transition">
                    <i class="fa-solid fa-xmark text-lg"></i>
                </button>
            </div>

            <!-- Tabs Navigation Inside Drawer -->
            <div class="flex border-b border-slate-800/60 bg-slate-950/20 px-4">
                <button onclick="switchDrawerTab('dr-overview')" id="btn-dr-overview" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-white border-b-2 border-brand-500">Overview</button>
                <button onclick="switchDrawerTab('dr-research')" id="btn-dr-research" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent">1. Research</button>
                <button onclick="switchDrawerTab('dr-deliv')" id="btn-dr-deliv" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent">2. Deliver</button>
                <button onclick="switchDrawerTab('dr-opp')" id="btn-dr-opp" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent">3. Opportunity</button>
                <button onclick="switchDrawerTab('dr-qual')" id="btn-dr-qual" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent">4. Qual</button>
                <button onclick="switchDrawerTab('dr-outreach')" id="btn-dr-outreach" class="dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent">5. Email</button>
            </div>

            <!-- Body Contents -->
            <div class="flex-grow p-6 overflow-y-auto min-h-0 space-y-6">
                <!-- Overview tab contents -->
                <div id="dr-overview" class="dr-tab-content space-y-6 block">
                    <div class="grid grid-cols-2 gap-4">
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">BANT Lead Score</span>
                            <span id="drawer-score" class="text-2xl font-black text-cyan-400 mt-1 block">0/100</span>
                        </div>
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">Priority Rank</span>
                            <span id="drawer-priority" class="text-2xl font-black text-emerald-400 mt-1 block">NURTURE</span>
                        </div>
                    </div>
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-white uppercase tracking-wider">CRM Records (SQLite)</h4>
                        <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl space-y-2.5 text-sm">
                            <div class="flex justify-between border-b border-slate-800 pb-2"><span class="text-slate-400">Email Address</span><span class="text-white" id="dr-val-email">-</span></div>
                            <div class="flex justify-between border-b border-slate-800 pb-2"><span class="text-slate-400">Company Name</span><span class="text-white" id="dr-val-comp">-</span></div>
                            <div class="flex justify-between border-b border-slate-800 pb-2"><span class="text-slate-400">Domain</span><span class="text-brand-400" id="dr-val-domain">-</span></div>
                            <div class="flex justify-between pb-1"><span class="text-slate-400">Personalization Anchor</span><span class="text-white max-w-[320px] text-right truncate" id="dr-val-anchor">-</span></div>
                        </div>
                    </div>

                    <!-- Simulate reply text-area -->
                    <div class="bg-slate-950 p-5 border border-slate-800 rounded-2xl space-y-4" id="simulate-reply-container" style="display: none;">
                        <h4 class="text-xs font-bold text-white uppercase tracking-wider">Simulate Inbound Reply (Inbox Management)</h4>
                        <textarea id="mock-reply-input" class="w-full h-24 bg-slate-900 border border-slate-800 p-3 rounded-xl text-sm focus:outline-none focus:border-brand-500/80 text-white" placeholder="Klinkt wel interessant. Wat zijn de kosten en hoe borg je de veiligheid?"></textarea>
                        <div class="flex justify-end gap-2.5">
                            <button onclick="runReplySimulation('OBJECTION')" class="bg-slate-800 hover:bg-slate-700 border border-slate-700 text-xs text-slate-300 font-semibold px-3 py-2 rounded-xl transition">Objection Reply</button>
                            <button onclick="runReplySimulation('MEETING_REQUEST')" class="bg-emerald-600 hover:bg-emerald-500 text-xs text-white font-semibold px-4 py-2 rounded-xl transition">Positive Reply</button>
                        </div>
                    </div>
                </div>

                <!-- 1. Research tab contents -->
                <div id="dr-research" class="dr-tab-content space-y-5 hidden">
                    <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl space-y-3">
                        <span class="text-[10px] text-brand-400 font-bold uppercase tracking-wider">Dutch MKB Research Summary</span>
                        <p class="text-sm text-slate-300 leading-relaxed" id="dr-research-summary">No research results loaded yet.</p>
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">Industry</span>
                            <span class="text-sm font-semibold text-white mt-1 block" id="dr-research-industry">-</span>
                        </div>
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">Estimated Size</span>
                            <span class="text-sm font-semibold text-white mt-1 block" id="dr-research-size">-</span>
                        </div>
                    </div>
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-white uppercase tracking-wider">Detected Corporate Pains</h4>
                        <ul class="space-y-2.5" id="dr-research-pains">
                            <!-- Pain lists -->
                        </ul>
                    </div>
                </div>

                <!-- 2. Deliverability tab contents -->
                <div id="dr-deliv" class="dr-tab-content space-y-5 hidden">
                    <div class="flex items-center justify-between bg-slate-950 p-5 border border-slate-800 rounded-xl">
                        <div>
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">Confidence Score</span>
                            <span id="dr-deliv-score" class="text-3xl font-black text-emerald-400 mt-1 block">0%</span>
                        </div>
                        <div class="text-right">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">Bounce Risk</span>
                            <span id="dr-deliv-risk" class="text-sm font-bold text-emerald-400 mt-1 block">LOW</span>
                        </div>
                    </div>
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-white uppercase tracking-wider">Mail Server Audit Criteria</h4>
                        <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl space-y-3 text-sm text-slate-300" id="dr-deliv-reasons">
                            <!-- reasons -->
                        </div>
                    </div>
                </div>

                <!-- 3. Opportunity mapping tab contents -->
                <div id="dr-opp" class="dr-tab-content space-y-5 hidden">
                    <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl space-y-3">
                        <span class="text-[10px] text-indigo-400 font-bold uppercase tracking-wider block">Calculated ROI Case</span>
                        <p class="text-sm text-slate-300 leading-relaxed" id="dr-opp-case">Awaiting opportunity mapping...</p>
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">Hours Saved Monthly</span>
                            <span id="dr-opp-hours" class="text-xl font-extrabold text-white mt-1 block">0 Hours</span>
                        </div>
                        <div class="bg-slate-950 p-4 border border-slate-800 rounded-xl">
                            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider block">Opportunity Score</span>
                            <span id="dr-opp-score" class="text-xl font-extrabold text-white mt-1 block">0.0 / 10</span>
                        </div>
                    </div>
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-white uppercase tracking-wider">Aligned Solutions</h4>
                        <div class="flex flex-wrap gap-2.5" id="dr-opp-solutions">
                            <!-- solutions -->
                        </div>
                    </div>
                </div>

                <!-- 4. Qualification tab contents -->
                <div id="dr-qual" class="dr-tab-content space-y-6 hidden">
                    <!-- SPIN analysis grids -->
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-cyan-400 uppercase tracking-widest">SPIN Sales Assessment</h4>
                        <div class="grid grid-cols-2 gap-4">
                            <div class="bg-slate-950 p-4 border border-slate-800/80 rounded-xl">
                                <span class="text-[9px] text-slate-500 font-extrabold uppercase block">[S] Situation</span>
                                <p class="text-xs text-slate-300 mt-1.5 leading-relaxed" id="dr-qual-spin-s">-</p>
                            </div>
                            <div class="bg-slate-950 p-4 border border-slate-800/80 rounded-xl">
                                <span class="text-[9px] text-amber-500 font-extrabold uppercase block">[P] Problem</span>
                                <p class="text-xs text-slate-300 mt-1.5 leading-relaxed" id="dr-qual-spin-p">-</p>
                            </div>
                            <div class="bg-slate-950 p-4 border border-slate-800/80 rounded-xl">
                                <span class="text-[9px] text-red-500 font-extrabold uppercase block">[I] Implication</span>
                                <p class="text-xs text-slate-300 mt-1.5 leading-relaxed" id="dr-qual-spin-i">-</p>
                            </div>
                            <div class="bg-slate-950 p-4 border border-slate-800/80 rounded-xl">
                                <span class="text-[9px] text-emerald-500 font-extrabold uppercase block">[N] Need-Payoff</span>
                                <p class="text-xs text-slate-300 mt-1.5 leading-relaxed" id="dr-qual-spin-n">-</p>
                            </div>
                        </div>
                    </div>
                    
                    <!-- BANT analysis grids -->
                    <div class="space-y-3.5">
                        <h4 class="text-xs font-bold text-cyan-400 uppercase tracking-widest">BANT Qualification</h4>
                        <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl divide-y divide-slate-800 text-sm space-y-2.5">
                            <div class="flex justify-between pb-2 pt-1"><span class="text-slate-400 font-semibold">[B] Budget</span><span class="text-white text-right" id="dr-qual-bant-b">-</span></div>
                            <div class="flex justify-between pb-2 pt-2"><span class="text-slate-400 font-semibold">[A] Authority</span><span class="text-white text-right" id="dr-qual-bant-a">-</span></div>
                            <div class="flex justify-between pb-2 pt-2"><span class="text-slate-400 font-semibold">[N] Need</span><span class="text-white text-right" id="dr-qual-bant-n">-</span></div>
                            <div class="flex justify-between pb-1 pt-2"><span class="text-slate-400 font-semibold">[T] Timeline</span><span class="text-white text-right" id="dr-qual-bant-t">-</span></div>
                        </div>
                    </div>
                </div>

                <!-- 5. Outreach email draft tab contents -->
                <div id="dr-outreach" class="dr-tab-content space-y-5 hidden">
                    <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl">
                        <div class="flex justify-between text-xs text-slate-400 border-b border-slate-800 pb-3 mb-4">
                            <span>Subject (Variant A)</span>
                            <span class="text-brand-400 font-semibold">Ready for Sending</span>
                        </div>
                        <h4 class="text-sm font-bold text-white mb-2" id="dr-out-subject">Awaiting draft...</h4>
                    </div>
                    <div class="bg-slate-950 p-5 border border-slate-800 rounded-xl">
                        <div class="flex justify-between text-xs text-slate-400 border-b border-slate-800 pb-3 mb-4">
                            <span>Email Body (Step 1)</span>
                            <span class="text-slate-500 font-semibold">Dutch Conversational (Je)</span>
                        </div>
                        <p class="text-sm text-slate-300 leading-relaxed whitespace-pre-wrap font-mono" id="dr-out-body">No draft email found.</p>
                    </div>
                </div>
            </div>

            <!-- Drawer Footer Buttons -->
            <div class="p-6 border-t border-slate-800/80 bg-slate-950/40 flex gap-4">
                <button onclick="replayPipelineFromDrawer()" class="flex-1 py-3 border border-slate-700 hover:border-slate-600 bg-slate-800 hover:bg-slate-700 text-sm font-bold text-white rounded-xl transition flex items-center justify-center gap-2">
                    <i class="fa-solid fa-rotate-right"></i> Replay Lead Pipeline
                </button>
                <button id="drawer-btn-approve" onclick="approveAndSendFromDrawer()" class="flex-1 py-3 bg-brand-600 hover:bg-brand-500 text-sm font-bold text-white rounded-xl shadow-lg shadow-brand-500/15 transition flex items-center justify-center gap-2" style="display: none;">
                    <i class="fa-solid fa-paper-plane"></i> Approve & Send Email
                </button>
            </div>
        </div>

        <!-- BACKGROUND OVERLAY -->
        <div id="overlay-bg" onclick="closeLeadDrawer()" class="fixed inset-0 bg-slate-950/60 backdrop-blur-xs z-35 hidden transition-opacity"></div>

        <!-- =========================================================================
             DASHBOARD JAVASCRIPT FRONTEND CONTROLLER
             ========================================================================= -->
        <script>
            // Global memory variables
            let activeTab = "tab-control";
            let activeDrawerTab = "dr-overview";
            let activeLeadId = null;
            let leads = [];
            let agents = [];
            let deliverability = [];
            let meetings = [];
            let activityLog = [];
            let linkedinHistory = [];
            let sseSource = null;

            // Timer clock UTC update
            setInterval(() => {
                const clock = document.getElementById("current-time");
                if(clock) {
                    const now = new Date();
                    clock.innerText = now.toUTCString().split(" ")[4] + " UTC";
                }
            }, 1000);

            // Fetch initial dashboard records
            async function fetchInitialData() {
                try {
                    const [resLeads, resAgents, resDeliv, resMeetings, resFeed] = await Promise.all([
                        fetch("/api/leads").then(r => r.json()),
                        fetch("/api/agents").then(r => r.json()),
                        fetch("/api/deliverability").then(r => r.json()),
                        fetch("/api/meetings").then(r => r.json()),
                        fetch("/api/activity_feed").then(r => r.json())
                    ]);
                    
                    leads = resLeads;
                    window._cachedLeads = resLeads;
                    agents = resAgents;
                    deliverability = resDeliv;
                    meetings = resMeetings;
                    activityLog = resFeed;

                    // Fetch LinkedIn history
                    try {
                        const resLi = await fetch("/api/linkedin/history").then(r => r.json());
                        linkedinHistory = resLi;
                    } catch(e) { linkedinHistory = []; }

                    renderAll();
                    await loadPosts();
                    await updatePostSchedulerUI();
                } catch(e) {
                    console.error("Failed to load initial data: ", e);
                    showToast("error", "Error loading initial records. Verify server status.");
                }
            }

            // Centralized Rendering Gateway
            function renderAll() {
                renderOverviewStats();
                renderAgentStatuses();
                renderLeadsTable();
                renderCRMTable();
                renderDeliverabilityMonitor();
                renderCalendarBookings();
                renderActivityLogFeed();
                renderLinkedInTable();
                loadFollowups();
            }

            // Top Dashboard numerical status aggregates
            function renderOverviewStats() {
                document.getElementById("stat-total-leads").innerText = leads.length;
                document.getElementById("stat-meetings").innerText = meetings.length;
                
                // Count active outreach + meeting booked + pending review
                const activeCount = leads.filter(l => ["RESEARCHED", "DELIVERABILITY_VERIFIED", "OPPORTUNITY_MAPPED", "PRE_QUALIFIED", "OUTREACH_DRAFTED", "PENDING_APPROVAL", "ACTIVE_OUTREACH", "REPLIED"].includes(l.current_stage)).length;
                document.getElementById("stat-active-leads").innerText = activeCount;
                
                // Count outbound sends
                const outboundCount = deliverability.reduce((sum, d) => sum + d.sends_count, 0);
                document.getElementById("stat-outbound-sends").innerText = outboundCount;
                document.getElementById("stat-total-sending").innerText = outboundCount;
                document.getElementById("stat-total-replies").innerText = deliverability.reduce((sum, d) => sum + d.replies_count, 0);
                document.getElementById("stat-monitored-domains-count").innerText = deliverability.length;
                
                // Average qualification lead score
                const scoredLeads = leads.filter(l => l.lead_score > 0);
                const avgScore = scoredLeads.length > 0 ? Math.round(scoredLeads.reduce((sum, l) => sum + l.lead_score, 0) / scoredLeads.length) : 0;
                document.getElementById("stat-avg-score").innerText = `${avgScore}/100`;
            }

            // Dynamic agent active status mapping
            function renderAgentStatuses() {
                const container = document.getElementById("agent-status-cards");
                container.innerHTML = "";
                
                // Reset node outlines to idle
                const nodes = ["ingest", "research", "deliver", "opp", "qual", "outreach", "review", "inbound"];
                nodes.forEach(n => {
                    const el = document.getElementById(`node-${n}`);
                    if(el) {
                        el.className = "w-12 h-12 bg-slate-800 border-2 border-slate-700 text-slate-300 rounded-full flex items-center justify-center shadow transition-all duration-300";
                    }
                });
                
                agents.forEach(a => {
                    const card = document.createElement("div");
                    let badgeClass = "bg-slate-800 text-slate-400";
                    let glowClass = "border-slate-800";
                    let nodeName = null;
                    
                    if (a.agent_name === "Research Agent") nodeName = "research";
                    else if (a.agent_name === "Deliverability Agent") nodeName = "deliver";
                    else if (a.agent_name === "Opportunity Mapping Agent") nodeName = "opp";
                    else if (a.agent_name === "Qualification Agent") nodeName = "qual";
                    else if (a.agent_name === "Outreach Agent") nodeName = "outreach";
                    else if (a.agent_name === "Inbox Management Agent") nodeName = "inbound";
                    
                    if(a.status === "RUNNING") {
                        badgeClass = "bg-blue-500/10 text-blue-400 border border-blue-500/30 animate-pulse";
                        glowClass = "border-blue-500/60 glowing-cyan";
                        
                        // Highlight SVG node
                        if(nodeName) {
                            const nodeEl = document.getElementById(`node-${nodeName}`);
                            if(nodeEl) nodeEl.className = "w-12 h-12 bg-blue-600 border-2 border-blue-400 text-white rounded-full flex items-center justify-center shadow-lg shadow-blue-500/20 glowing-cyan";
                        }
                    } else if (a.status === "WAITING") {
                        badgeClass = "bg-amber-500/10 text-amber-400 border border-amber-500/30";
                        glowClass = "border-amber-500/60 glowing-amber";
                        
                        if(nodeName) {
                            const nodeEl = document.getElementById(`node-${nodeName}`);
                            if(nodeEl) nodeEl.className = "w-12 h-12 bg-amber-600 border-2 border-amber-400 text-white rounded-full flex items-center justify-center shadow-lg shadow-amber-500/20 glowing-amber";
                        }
                    } else if (a.status === "FAILED") {
                        badgeClass = "bg-red-500/10 text-red-400 border border-red-500/30";
                        glowClass = "border-red-500/60 glowing-red";
                        
                        if(nodeName) {
                            const nodeEl = document.getElementById(`node-${nodeName}`);
                            if(nodeEl) nodeEl.className = "w-12 h-12 bg-red-600 border-2 border-red-400 text-white rounded-full flex items-center justify-center shadow-lg shadow-red-500/20 glowing-red";
                        }
                    }
                    
                    card.className = `bg-slate-900 border-2 ${glowClass} p-5 rounded-2xl flex flex-col justify-between shadow transition-all duration-300`;
                    
                    card.innerHTML = `
                        <div class="space-y-2">
                            <div class="flex items-center justify-between">
                                <span class="text-sm font-bold text-white">${a.agent_name}</span>
                                <span class="text-[9px] font-extrabold uppercase px-2 py-0.5 rounded ${badgeClass}">${a.status}</span>
                            </div>
                            <p class="text-xs text-slate-400 italic">"${a.last_action}"</p>
                        </div>
                        <div class="flex items-center justify-between border-t border-slate-800/80 pt-3.5 mt-4 text-[10px] text-slate-500 font-semibold">
                            <span>Last Active</span>
                            <span>${new Date(a.last_active_at).toUTCString().split(" ")[4]} UTC</span>
                        </div>
                    `;
                    container.appendChild(card);
                });
                
                // Highlight Human Review Node if a lead is PENDING_APPROVAL
                const hasPendingApproval = leads.some(l => l.current_stage === "PENDING_APPROVAL");
                if (hasPendingApproval) {
                    const nodeEl = document.getElementById("node-review");
                    if (nodeEl) nodeEl.className = "w-12 h-12 bg-amber-600 border-2 border-amber-400 text-white rounded-full flex items-center justify-center shadow-lg shadow-amber-500/20 glowing-amber";
                }
            }

            // Leads Queue table mapping
            function renderLeadsTable() {
                const tbody = document.getElementById("active-leads-table-rows");
                tbody.innerHTML = "";
                
                leads.forEach(l => {
                    const tr = document.createElement("tr");
                    tr.className = "hover:bg-slate-800/40 cursor-pointer transition";
                    tr.onclick = (e) => {
                        // Prevent click triggering if clicking action buttons
                        if (e.target.tagName === 'BUTTON' || e.target.closest('button') || e.target.tagName === 'A') return;
                        openLeadDrawer(l.contact_id);
                    };

                    let stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-800 text-slate-400 border border-slate-700">${l.current_stage}</span>`;
                    if (l.current_stage === "PENDING_APPROVAL") {
                        stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30 animate-pulse">PENDING REVIEW</span>`;
                    } else if (l.current_stage === "MEETING_SCHEDULED") {
                        stageBadge = `<span class="text-[10px] font-bold uppercase px-2.5 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">MEETING SCHEDULED</span>`;
                    } else if (l.current_stage === "ACTIVE_OUTREACH") {
                        stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-blue-500/15 text-blue-400 border border-blue-500/30">ACTIVE OUTREACH</span>`;
                    } else if (l.current_stage === "BLOCKED") {
                        stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-red-500/15 text-red-400 border border-red-500/30">BLOCKED</span>`;
                    } else if (l.current_stage === "OPT_OUT") {
                        stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-zinc-500/15 text-zinc-400 border border-zinc-500/30">GDPR OPT_OUT</span>`;
                    }

                    let scoreBadge = `<span class="text-slate-500">-</span>`;
                    if(l.lead_score > 0) {
                        const colorClass = l.lead_score >= 80 ? 'text-cyan-400 font-extrabold' : 'text-slate-300';
                        scoreBadge = `<span class="${colorClass}">${l.lead_score}/100</span>`;
                    }

                    let quickActionBtn = "";
                    if(l.current_stage === "PENDING_APPROVAL") {
                        quickActionBtn = `
                            <button onclick="openApprovalModal('${l.contact_id}')" class="bg-brand-600 hover:bg-brand-500 text-xs text-white font-bold px-3 py-1.5 rounded-lg shadow-md hover:scale-105 transition flex items-center gap-1.5">
                                <i class="fa-solid fa-paper-plane text-[10px]"></i> Review & Send
                            </button>
                        `;
                    } else if (l.current_stage === "ACTIVE_OUTREACH") {
                        quickActionBtn = `
                            <button onclick="simulateReplyTrigger('${l.contact_id}')" class="bg-indigo-600/80 hover:bg-indigo-600 text-xs text-indigo-100 font-bold px-3 py-1.5 rounded-lg transition flex items-center gap-1.5">
                                <i class="fa-solid fa-reply text-[10px]"></i> Simulate Reply
                            </button>
                        `;
                    }

                    tr.innerHTML = `
                        <td class="py-4 px-6">
                            <span class="font-extrabold text-white block">${l.first_name} ${l.last_name || ''}</span>
                            <span class="text-xs text-slate-400 block mt-0.5">${l.role || 'Prospect'}</span>
                        </td>
                        <td class="py-4 px-6 text-slate-300 font-semibold">${l.company_name || 'MKB Firm'}</td>
                        <td class="py-4 px-6">
                            <span class="text-slate-300 block text-xs truncate max-w-[150px]">${l.email}</span>
                            <span class="text-[10px] text-brand-400 block mt-0.5 font-bold">${l.company_domain || ''}</span>
                        </td>
                        <td class="py-4 px-6">${stageBadge}</td>
                        <td class="py-4 px-6">${scoreBadge}</td>
                        <td class="py-4 px-6">${quickActionBtn}</td>
                        <td class="py-4 px-6 text-right flex items-center justify-end gap-1">
                            <button onclick="deleteLead('${l.contact_id}')" class="text-red-400/60 hover:text-red-400 hover:scale-110 p-2 rounded-lg transition" title="Remove lead from pipeline">
                                <i class="fa-solid fa-trash-can text-[11px]"></i>
                            </button>
                            <button onclick="replayPipeline('${l.contact_id}')" class="text-slate-400 hover:text-white hover:scale-110 p-2 rounded-lg transition" title="Replay Lead through pipeline">
                                <i class="fa-solid fa-rotate-right"></i>
                            </button>
                        </td>
                    `;
                    tbody.appendChild(tr);
                });
            }

            // Tab 2: Full CRM display
            function renderCRMTable() {
                const tbody = document.getElementById("crm-table-rows");
                tbody.innerHTML = "";
                leads.forEach(l => {
                    const tr = document.createElement("tr");
                    tr.className = "hover:bg-slate-800/30";
                    
                    const updDate = l.updated_at ? new Date(l.updated_at).toLocaleDateString() : '-';
                    const priorityColor = l.priority === 'HOT' ? 'text-red-400 font-bold' : (l.priority === 'WARM' ? 'text-amber-400' : 'text-slate-400');
                    
                    tr.innerHTML = `
                        <td class="py-4 px-6 font-mono text-xs text-slate-500">${l.contact_id}</td>
                        <td class="py-4 px-6">
                            <span class="font-bold text-white block">${l.first_name} ${l.last_name || ''}</span>
                            <span class="text-xs text-slate-400 block">${l.role || '-'}</span>
                        </td>
                        <td class="py-4 px-6 text-slate-300">${l.email}</td>
                        <td class="py-4 px-6 font-semibold text-slate-300">${l.company_name || '-'}</td>
                        <td class="py-4 px-6"><span class="text-xs uppercase font-bold tracking-wide">${l.current_stage}</span></td>
                        <td class="py-4 px-6"><span class="${priorityColor}">${l.priority}</span></td>
                        <td class="py-4 px-6 text-slate-400 text-xs">${updDate}</td>
                    `;
                    tbody.appendChild(tr);
                });
            }

            // Tab 3: Monitored email domains
            function renderDeliverabilityMonitor() {
                const tbody = document.getElementById("deliverability-table-rows");
                tbody.innerHTML = "";
                
                deliverability.forEach(d => {
                    const tr = document.createElement("tr");
                    tr.className = "hover:bg-slate-800/30";
                    
                    const healthBadge = d.domain_health === 'HEALTHY' 
                        ? `<span class="bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider">HEALTHY</span>`
                        : `<span class="bg-amber-500/10 text-amber-400 border border-amber-500/20 px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider">${d.domain_health}</span>`;
                        
                    tr.innerHTML = `
                        <td class="py-4 px-6 font-bold text-white">${d.domain}</td>
                        <td class="py-4 px-6 text-slate-300 font-semibold">${d.sends_count}</td>
                        <td class="py-4 px-6 text-slate-300 font-semibold">${d.replies_count}</td>
                        <td class="py-4 px-6 text-slate-400">${d.bounces_count}</td>
                        <td class="py-4 px-6 font-semibold ${d.bounce_rate > 5 ? 'text-red-400' : 'text-slate-300'}">${d.bounce_rate}%</td>
                        <td class="py-4 px-6">${healthBadge}</td>
                        <td class="py-4 px-6 text-xs text-slate-400">${new Date(d.updated_at).toUTCString().split(" ")[4]} UTC</td>
                    `;
                    tbody.appendChild(tr);
                });
            }

            // Tab 4: Google calendar schedules
            function renderCalendarBookings() {
                const grid = document.getElementById("calendar-bookings-grid");
                grid.innerHTML = "";
                
                if(meetings.length === 0) {
                    grid.innerHTML = `
                        <div class="col-span-2 bg-slate-900 border border-slate-800/70 p-12 rounded-2xl text-center text-slate-500">
                            <i class="fa-solid fa-calendar-xmark text-4xl block mb-3"></i>
                            <span>No meetings scheduled on Google Calendar currently.</span>
                        </div>
                    `;
                    return;
                }
                
                meetings.forEach(m => {
                    const card = document.createElement("div");
                    card.className = "bg-slate-900 border border-slate-800/70 p-5 rounded-2xl flex flex-col justify-between shadow-md space-y-4";
                    
                    const schedTime = new Date(m.scheduled_time);
                    
                    card.innerHTML = `
                        <div class="space-y-2">
                            <div class="flex justify-between items-start">
                                <span class="bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 text-[9px] font-bold px-2 py-0.5 rounded uppercase">Google Meet Linked</span>
                                <span class="text-xs text-slate-500 font-semibold">${schedTime.toLocaleDateString('en-US', {weekday: 'long', month: 'short', day: 'numeric'})}</span>
                            </div>
                            <h3 class="text-base font-extrabold text-white leading-snug">${m.summary}</h3>
                            <div class="text-xs text-slate-400 mt-2 flex items-center gap-1.5">
                                <i class="fa-regular fa-clock"></i>
                                <span>${schedTime.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})} UTC (${m.duration_minutes} Mins)</span>
                            </div>
                            <p class="text-xs text-slate-400">Prospect: <span class="text-white font-semibold">${m.first_name} ${m.last_name || ''}</span> (${m.email})</p>
                        </div>
                        <div class="border-t border-slate-800/80 pt-4 flex justify-end">
                            <a href="${m.hangout_link}" target="_blank" class="bg-brand-600 hover:bg-brand-500 text-xs text-white font-bold px-4 py-2 rounded-xl transition flex items-center gap-2">
                                <i class="fa-solid fa-video"></i> Join Meet Room
                            </a>
                        </div>
                    `;
                    grid.appendChild(card);
                });
            }

            // Tab 5: Scrolling live audits logs
            function renderActivityLogFeed() {
                const logDiv = document.getElementById("comprehensive-activity-feed");
                logDiv.innerHTML = "";
                
                activityLog.forEach(log => {
                    const card = document.createElement("div");
                    card.className = "bg-slate-950/40 p-4 border border-slate-800/50 rounded-xl space-y-2 text-sm";
                    
                    const logTime = new Date(log.created_at).toUTCString().split(" ")[4];
                    let icon = "fa-solid fa-microchip text-slate-400";
                    let accentColor = "text-slate-400";
                    
                    if(log.actor === 'Deliverability Agent' || log.action_type === 'SAFETY_GATE') {
                        icon = "fa-solid fa-shield-halved text-emerald-400";
                        accentColor = "text-emerald-400";
                    } else if (log.actor === 'Research Agent') {
                        icon = "fa-solid fa-magnifying-glass text-cyan-400";
                        accentColor = "text-cyan-400";
                    } else if (log.actor === 'Qualification Agent') {
                        icon = "fa-solid fa-filter text-indigo-400";
                        accentColor = "text-indigo-400";
                    } else if (log.actor === 'Outreach Agent') {
                        icon = "fa-solid fa-pen-nib text-brand-400";
                        accentColor = "text-brand-400";
                    } else if (log.actor === 'LinkedIn Agent') {
                        icon = "fa-brands fa-linkedin text-blue-400";
                        accentColor = "text-blue-400";
                    } else if (log.actor === 'CentralOrchestrator' || log.action_type === 'OUTBOUND_SEND') {
                        icon = "fa-solid fa-paper-plane text-brand-500";
                        accentColor = "text-brand-500";
                    } else if (log.action_type === 'MEETING_BOOKED') {
                        icon = "fa-solid fa-calendar-check text-emerald-500";
                        accentColor = "text-emerald-500";
                    }
                    
                    card.innerHTML = `
                        <div class="flex items-center justify-between">
                            <span class="font-bold flex items-center gap-2 text-white">
                                <i class="${icon}"></i> <span class="${accentColor}">${log.actor}</span>
                            </span>
                            <span class="text-[10px] text-slate-500 font-semibold">${logTime} UTC</span>
                        </div>
                        <p class="text-xs text-slate-300 leading-relaxed">${log.action_details}</p>
                        ${log.first_name ? `<span class="text-[9px] text-slate-500 font-bold bg-slate-900 border border-slate-800 px-2 py-0.5 rounded">Prospect: ${log.first_name}</span>` : ''}
                    `;
                    logDiv.appendChild(card);
                });
            }

            // Tab 6: LinkedIn outreach table
            async function checkLinkedInLoginStatus() {
                try {
                    const res = await fetch("/api/linkedin/status").then(r => r.json());
                    const dot = document.getElementById("li-login-dot");
                    const text = document.getElementById("li-login-text");
                    const btn = document.getElementById("li-login-btn");
                    if(res.logged_in) {
                        dot.className = "w-2.5 h-2.5 bg-emerald-500 rounded-full inline-block shadow shadow-emerald-500/50";
                        text.innerText = "Connected";
                        text.className = "text-xs font-semibold text-emerald-400 uppercase tracking-widest";
                        btn.style.display = "none";
                    } else {
                        dot.className = "w-2.5 h-2.5 bg-red-500 rounded-full inline-block animate-pulse";
                        text.innerText = "Not Connected";
                        text.className = "text-xs font-semibold text-slate-400 uppercase tracking-widest";
                        btn.style.display = "flex";
                    }
                } catch(e) { /* ignore */ }
            }

            async function linkedinLogin() {
                const btn = document.getElementById("li-login-btn");
                btn.disabled = true;
                btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Opening browser...';
                showToast("info", "Opening LinkedIn login page. Please log in in the browser window.");
                try {
                    const res = await fetch("/api/linkedin/login", { method: "POST" });
                    const data = await res.json();
                    showToast("info", "Browser opened. Log in to LinkedIn, then wait for cookies to save.");
                    // Poll for login status after 30 seconds
                    setTimeout(async () => {
                        await checkLinkedInLoginStatus();
                        btn.disabled = false;
                        btn.innerHTML = '<i class="fa-brands fa-linkedin"></i> Login to LinkedIn';
                        const status = await fetch("/api/linkedin/status").then(r => r.json());
                        if(status.logged_in) {
                            showToast("success", "LinkedIn login successful! Outreach is now fully automatic.");
                        }
                    }, 30000);
                } catch(e) {
                    showToast("error", "Failed to open login browser: " + e.message);
                    btn.disabled = false;
                    btn.innerHTML = '<i class="fa-brands fa-linkedin"></i> Login to LinkedIn';
                }
            }

            async function loadFollowups() {
                try {
                    const resp = await fetch("/api/linkedin/pending-followups");
                    const data = await resp.json();
                    const section = document.getElementById("followup-section");
                    const list = document.getElementById("followup-list");
                    if (!section || !list) return;

                    if (data.pending === 0) {
                        section.classList.add("hidden");
                        return;
                    }
                    section.classList.remove("hidden");
                    list.innerHTML = "";

                    data.messages.forEach(m => {
                        const card = document.createElement("div");
                        card.className = "bg-slate-950 border border-slate-800 rounded-xl p-4 space-y-3";
                        card.innerHTML = `
                            <div class="flex justify-between items-start">
                                <div>
                                    <span class="font-bold text-white">${m.first_name} ${m.last_name}</span>
                                    <span class="text-xs text-slate-400 ml-2">${m.company || ''}</span>
                                </div>
                                <a href="${m.profile_url || '#'}" target="_blank" class="text-xs text-blue-400 hover:underline"><i class="fa-brands fa-linkedin"></i> Open Profile</a>
                            </div>
                            <div class="bg-slate-900 border border-slate-800/50 rounded-lg p-3">
                                <p class="text-sm text-slate-300 leading-relaxed">${m.followup_note}</p>
                            </div>
                            <div class="flex gap-2">
                                <button onclick="copyFollowup(this, '${m.followup_note.replace(/'/g, "\\'")}')" class="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-4 py-2 rounded-lg transition flex items-center gap-1.5">
                                    <i class="fa-regular fa-copy"></i> Copy Message
                                </button>
                                <button onclick="markFollowupDone(${m.id})" class="bg-slate-700 hover:bg-slate-600 text-white text-xs font-bold px-4 py-2 rounded-lg transition">
                                    Mark as Sent
                                </button>
                            </div>
                        `;
                        list.appendChild(card);
                    });
                } catch(e) {
                    console.error("Failed to load followups:", e);
                }
            }

            function copyFollowup(btn, text) {
                navigator.clipboard.writeText(text).then(() => {
                    const orig = btn.innerHTML;
                    btn.innerHTML = '<i class="fa-solid fa-check"></i> Copied!';
                    btn.className = btn.className.replace('bg-emerald-600', 'bg-emerald-800');
                    setTimeout(() => { btn.innerHTML = orig; btn.className = btn.className.replace('bg-emerald-800', 'bg-emerald-600'); }, 2000);
                });
            }

            async function markFollowupDone(id) {
                await fetch("/api/linkedin/mark-followup-sent/" + id, { method: "POST" });
                loadFollowups();
            }

            function renderLinkedInTable() {
                const tbody = document.getElementById("linkedin-table-rows");
                if(!tbody) return;
                tbody.innerHTML = "";

                // Update stats
                const sent = linkedinHistory.length;
                const success = linkedinHistory.filter(l => l.outcome === 'SUCCESS').length;
                const notFound = linkedinHistory.filter(l => l.outcome === 'NOT_FOUND').length;
                const errors = linkedinHistory.filter(l => l.outcome === 'ERROR').length;
                document.getElementById("li-stat-sent").innerText = sent;
                document.getElementById("li-stat-success").innerText = success;
                document.getElementById("li-stat-notfound").innerText = notFound;
                document.getElementById("li-stat-error").innerText = errors;

                if(linkedinHistory.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="5" class="py-12 text-center text-slate-500"><i class="fa-brands fa-linkedin text-3xl block mb-2"></i>No LinkedIn outreach yet. Connections are sent automatically when leads reach the Outreach stage.</td></tr>`;
                    return;
                }

                linkedinHistory.forEach(l => {
                    const tr = document.createElement("tr");
                    tr.className = "hover:bg-slate-800/30";

                    let outcomeBadge = "";
                    if(l.outcome === 'SUCCESS') {
                        outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">SENT</span>`;
                    } else if(l.outcome === 'NOT_FOUND') {
                        outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30">NOT FOUND</span>`;
                    } else if(l.outcome === 'FAILURE') {
                        outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-500/15 text-slate-400 border border-slate-500/30">FAILED</span>`;
                    } else {
                        outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-red-500/15 text-red-400 border border-red-500/30">${l.outcome}</span>`;
                    }

                    const time = l.timestamp ? new Date(l.timestamp).toLocaleString('nl-NL', {day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}) : '-';
                    const noteDisplay = (l.note || '').substring(0, 100) + ((l.note || '').length > 100 ? '...' : '');

                    tr.innerHTML = `
                        <td class="py-4 px-6">
                            <span class="font-bold text-white block">${l.first_name || ''} ${l.last_name || ''}</span>
                            ${l.profile_url ? `<a href="${l.profile_url}" target="_blank" class="text-[10px] text-blue-400 hover:underline block mt-0.5"><i class="fa-brands fa-linkedin text-[9px]"></i> Profile</a>` : ''}
                        </td>
                        <td class="py-4 px-6 text-slate-300 font-semibold">${l.company || '-'}</td>
                        <td class="py-4 px-6 text-xs text-slate-400 max-w-[300px]">${noteDisplay}</td>
                        <td class="py-4 px-6">${outcomeBadge}</td>
                        <td class="py-4 px-6 text-xs text-slate-500">${time}</td>
                    `;
                    tbody.appendChild(tr);
                });
            }

            // Real-time Scrolling Console Log inside Control Tab
            function pushEventToConsoleLog(event, text) {
                const con = document.getElementById("console-stream-log");
                if(!con) return;
                
                const timeStr = new Date().toUTCString().split(" ")[4];
                let colorClass = "text-slate-400";
                
                if (event === "agent_handoff") colorClass = "text-cyan-400";
                else if (event === "notify") colorClass = "text-emerald-400 font-bold";
                else if (event === "RESET") colorClass = "text-slate-500";
                else if (event === "OUTBOUND_SEND") colorClass = "text-brand-400 font-extrabold";
                
                const div = document.createElement("div");
                div.className = "leading-relaxed border-l-2 border-slate-800 pl-2 py-0.5";
                div.innerHTML = `<span class="text-slate-600 font-bold">[${timeStr}]</span> <span class="${colorClass}">${text}</span>`;
                
                con.appendChild(div);
                con.scrollTop = con.scrollHeight;
            }

            // Tab Routing Management
            function switchTab(tabId) {
                activeTab = tabId;
                
                // Hide all
                document.querySelectorAll(".tab-content").forEach(el => el.classList.add("hidden"));
                document.getElementById(tabId).classList.remove("hidden");
                
                // Active classes navigation
                document.querySelectorAll(".tab-button").forEach(el => {
                    el.className = "tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold text-slate-400 hover:bg-slate-800 hover:text-slate-100 transition";
                });
                
                const activeBtn = document.getElementById(`btn-${tabId}`);
                if (activeBtn) {
                    activeBtn.className = "tab-button w-full flex items-center space-x-3.5 px-4 py-3 rounded-xl text-sm font-semibold transition bg-brand-600 text-white shadow-lg shadow-brand-500/10";
                }

                // Check LinkedIn login status when switching to LinkedIn tab
                if(tabId === "tab-linkedin") {
                    checkLinkedInLoginStatus();
                }
                if(tabId === "tab-settings") {
                    loadSettings();
                }
                if(tabId === "tab-metrics") {
                    loadMetrics();
                }
                if(tabId === "tab-templates") {
                    loadTemplates();
                }
                if(tabId === "tab-strategy") {
                    loadStrategies();
                }
            }

            // Lead Drawer modal open
            function openLeadDrawer(leadId) {
                activeLeadId = leadId;
                const lead = leads.find(l => l.contact_id === leadId);
                if(!lead) return;
                
                // Main Drawer Texts
                document.getElementById("drawer-lead-name").innerText = `${lead.first_name} ${lead.last_name || ''}`;
                document.getElementById("drawer-lead-sub").innerText = `${lead.role || 'Prospect'} at ${lead.company_name || 'MKB'}`;
                
                // Stage badge classes
                const badge = document.getElementById("drawer-stage-badge");
                badge.innerText = lead.current_stage;
                badge.className = "text-[10px] font-extrabold uppercase px-2.5 py-1 rounded-full tracking-wider " + 
                    (lead.current_stage === "PENDING_APPROVAL" ? "bg-amber-500/10 text-amber-400 border border-amber-500/20" : 
                    (lead.current_stage === "MEETING_SCHEDULED" ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20" : "bg-blue-500/10 text-blue-400 border border-blue-500/20"));

                // Show send/approve button inside human gate
                const appBtn = document.getElementById("drawer-btn-approve");
                appBtn.style.display = lead.current_stage === "PENDING_APPROVAL" ? "flex" : "none";

                // Show simulate reply container if outreach active
                const repContainer = document.getElementById("simulate-reply-container");
                repContainer.style.display = lead.current_stage === "ACTIVE_OUTREACH" ? "block" : "none";

                // Populate drawer tabs
                populateDrawerOverviewTab(lead);
                populateDrawerResearchTab(lead);
                populateDrawerDeliverabilityTab(lead);
                populateDrawerOpportunityTab(lead);
                populateDrawerQualificationTab(lead);
                populateDrawerOutreachTab(lead);

                // Open
                document.getElementById("overlay-bg").classList.remove("hidden");
                document.getElementById("drawer-lead").classList.remove("translate-x-full");
                switchDrawerTab("dr-overview");
            }

            function closeLeadDrawer() {
                document.getElementById("overlay-bg").classList.add("hidden");
                document.getElementById("drawer-lead").classList.add("translate-x-full");
                activeLeadId = null;
            }

            // Sub Tab Routing Drawer
            function switchDrawerTab(tabId) {
                activeDrawerTab = tabId;
                document.querySelectorAll(".dr-tab-content").forEach(el => el.classList.add("hidden"));
                document.getElementById(tabId).classList.remove("hidden");
                
                document.querySelectorAll(".dr-tab-btn").forEach(el => {
                    el.className = "dr-tab-btn flex-1 py-3 text-xs font-semibold text-slate-400 border-b-2 border-transparent";
                });
                document.getElementById(`btn-${tabId}`).className = "dr-tab-btn flex-1 py-3 text-xs font-semibold text-white border-b-2 border-brand-500";
            }

            // Individual Drawer loaders
            function populateDrawerOverviewTab(lead) {
                document.getElementById("drawer-score").innerText = lead.lead_score ? `${lead.lead_score}/100` : "0/100";
                document.getElementById("drawer-priority").innerText = lead.priority || "NURTURE";
                document.getElementById("dr-val-email").innerText = lead.email;
                document.getElementById("dr-val-comp").innerText = lead.company_name || '-';
                document.getElementById("dr-val-domain").innerText = lead.company_domain || '-';
                document.getElementById("dr-val-anchor").innerText = lead.personalization_anchor || '-';
            }

            function populateDrawerResearchTab(lead) {
                const res = lead.research_result;
                if(!res) {
                    document.getElementById("dr-research-summary").innerText = "Research has not been executed yet. Click Replay to run Research Agent.";
                    document.getElementById("dr-research-industry").innerText = "-";
                    document.getElementById("dr-research-size").innerText = "-";
                    document.getElementById("dr-research-pains").innerHTML = "";
                    return;
                }
                
                document.getElementById("dr-research-summary").innerText = res.company_summary;
                document.getElementById("dr-research-industry").innerText = res.industry;
                document.getElementById("dr-research-size").innerText = res.estimated_size;
                
                const painsUl = document.getElementById("dr-research-pains");
                painsUl.innerHTML = "";
                res.detected_pain_points.forEach(p => {
                    const li = document.createElement("li");
                    li.className = "flex items-start gap-2.5 text-slate-300 text-xs bg-slate-950 p-3 rounded-xl border border-slate-800/80";
                    li.innerHTML = `<i class="fa-solid fa-triangle-exclamation text-amber-500 mt-0.5"></i> <span>${p}</span>`;
                    painsUl.appendChild(li);
                });
            }

            function populateDrawerDeliverabilityTab(lead) {
                const del = lead.deliverability_audit;
                if(!del) {
                    document.getElementById("dr-deliv-score").innerText = "0%";
                    document.getElementById("dr-deliv-risk").innerText = "-";
                    document.getElementById("dr-deliv-reasons").innerHTML = "Awaiting deliverability audit.";
                    return;
                }
                
                document.getElementById("dr-deliv-score").innerText = `${del.confidence_score}%`;
                document.getElementById("dr-deliv-risk").innerText = del.bounce_risk;
                
                const reasDiv = document.getElementById("dr-deliv-reasons");
                reasDiv.innerHTML = "";
                del.reasons.forEach(r => {
                    const p = document.createElement("p");
                    p.className = "flex items-center gap-2";
                    p.innerHTML = `<i class="fa-solid fa-circle-check text-emerald-500 text-xs"></i> <span>${r}</span>`;
                    reasDiv.appendChild(p);
                });
            }

            function populateDrawerOpportunityTab(lead) {
                const opp = lead.opportunity_mapping;
                if(!opp) {
                    document.getElementById("dr-opp-case").innerText = "Awaiting Opportunity Mapping calculations.";
                    document.getElementById("dr-opp-hours").innerText = "0 Hours";
                    document.getElementById("dr-opp-score").innerText = "0.0 / 10";
                    document.getElementById("dr-opp-solutions").innerHTML = "";
                    return;
                }
                
                document.getElementById("dr-opp-case").innerText = opp.roi_business_case;
                document.getElementById("dr-opp-hours").innerText = `${opp.expected_hours_saved_monthly} Hours/mnd`;
                document.getElementById("dr-opp-score").innerText = `${opp.opportunity_score} / 10`;
                
                const solDiv = document.getElementById("dr-opp-solutions");
                solDiv.innerHTML = "";
                opp.selected_solutions.forEach(s => {
                    const span = document.createElement("span");
                    span.className = "bg-brand-500/10 text-brand-400 border border-brand-500/20 px-3 py-1.5 rounded-xl text-xs font-bold";
                    span.innerText = s;
                    solDiv.appendChild(span);
                });
            }

            function populateDrawerQualificationTab(lead) {
                const qual = lead.qualification_assessment;
                if(!qual) {
                    document.getElementById("dr-qual-spin-s").innerText = "-";
                    document.getElementById("dr-qual-spin-p").innerText = "-";
                    document.getElementById("dr-qual-spin-i").innerText = "-";
                    document.getElementById("dr-qual-spin-n").innerText = "-";
                    
                    document.getElementById("dr-qual-bant-b").innerText = "-";
                    document.getElementById("dr-qual-bant-a").innerText = "-";
                    document.getElementById("dr-qual-bant-n").innerText = "-";
                    document.getElementById("dr-qual-bant-t").innerText = "-";
                    return;
                }
                
                // SPIN
                document.getElementById("dr-qual-spin-s").innerText = qual.spin_analysis.situation;
                document.getElementById("dr-qual-spin-p").innerText = qual.spin_analysis.problem;
                document.getElementById("dr-qual-spin-i").innerText = qual.spin_analysis.implication;
                document.getElementById("dr-qual-spin-n").innerText = qual.spin_analysis.need_payoff;
                
                // BANT
                document.getElementById("dr-qual-bant-b").innerText = qual.bant_analysis.budget;
                document.getElementById("dr-qual-bant-a").innerText = qual.bant_analysis.authority;
                document.getElementById("dr-qual-bant-n").innerText = qual.bant_analysis.need;
                document.getElementById("dr-qual-bant-t").innerText = qual.bant_analysis.timeline;
            }

            function populateDrawerOutreachTab(lead) {
                const out = lead.outreach_draft;
                if(!out) {
                    document.getElementById("dr-out-subject").innerText = "Outbound campaign not drafted yet.";
                    document.getElementById("dr-out-body").innerText = "No templates constructed.";
                    return;
                }
                
                document.getElementById("dr-out-subject").innerText = out.subject_line_A;
                document.getElementById("dr-out-body").innerText = out.email_body_step_1;
            }


            // --- ASYNCHRONOUS PIPELINE TRIGGERING ---

            // Replay lead from control dashboard table row
            async function replayPipeline(leadId) {
                try {
                    const res = await fetch(`/api/leads/${leadId}/replay`, { method: "POST" });
                    const data = await res.json();
                    if(data.status === "RUNNING") {
                        showToast("info", "Launched autonomous multi-agent pipeline replay! Watch statuses.");
                        pushEventToConsoleLog("RESET", `Replay initialized for lead ID: ${leadId}`);
                    }
                } catch(e) {
                    console.error("Failed to trigger replay:", e);
                    showToast("error", "Failed to start pipeline replay.");
                }
            }

            function replayPipelineFromDrawer() {
                if(activeLeadId) {
                    replayPipeline(activeLeadId);
                    closeLeadDrawer();
                }
            }

            // Approve and send outbound email trigger
            async function approveOutreach(leadId) {
                try {
                    showToast("info", "Transmitting email via Google Mail servers...");
                    const res = await fetch(`/api/leads/${leadId}/approve`, { method: "POST" });
                    const data = await res.json();
                    if(res.ok && data.status === "SUCCESS") {
                        showToast("success", "Outreach sent successfully! Session CRM records updated.");
                        // Force refresh initial data values
                        await fetchInitialData();
                    } else {
                        showToast("error", `Send failed: ${data.detail || data.error || 'Unknown error'}`);
                    }
                } catch(e) {
                    console.error("Failed to approve send: ", e);
                    showToast("error", "Network error while sending email. Please try again.");
                }
            }

            async function approveAndSendFromDrawer() {
                if(activeLeadId) {
                    closeLeadDrawer();
                    // Refresh data first to ensure we have the latest draft
                    await fetchInitialData();
                    openApprovalModal(activeLeadId);
                } else {
                    showToast("error", "No lead selected. Open a lead first.");
                }
            }

            // Simulate incoming response reply trigger
            function simulateReplyTrigger(leadId) {
                activeLeadId = leadId;
                openLeadDrawer(leadId);
                switchDrawerTab("dr-overview");
                // Scroll focus
                document.getElementById("simulate-reply-container").scrollIntoView({behavior: 'smooth'});
            }

            async function runReplySimulation(type) {
                if(!activeLeadId) return;
                
                let text = "Ik heb twijfels over de veiligheid van AI-assistenten.";
                if(type === 'MEETING_REQUEST') {
                    text = "Dit klinkt ontzettend goed. Zullen we komende dinsdag om 10:00 uur een demo inplannen via Teams?";
                }
                
                try {
                    closeLeadDrawer();
                    showToast("info", "Triggering Inbox Management Agent simulation...");
                    
                    const res = await fetch(`/api/leads/${activeLeadId}/reply_simulate`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ reply_text: text })
                    });
                    const data = await res.json();
                    if(data.status === "REPLIED") {
                        if (data.classification === 'MEETING_REQUEST') {
                            triggerConfettiCelebration();
                        }
                        fetchInitialData();
                    }
                } catch(e) {
                    console.error("Inbound simulation failed: ", e);
                    showToast("error", "Failed to compile reply simulation.");
                }
            }

            // Confetti booking explosion!
            function triggerConfettiCelebration() {
                confetti({
                    particleCount: 120,
                    spread: 80,
                    origin: { y: 0.6 }
                });
            }


            // --- SSE REAL-TIME DISPATCH LISTENER ---

            function startSSEListener() {
                sseSource = new EventSource("/api/events");
                
                const dot = document.getElementById("sse-status-dot");
                const text = document.getElementById("sse-status-text");

                sseSource.onopen = () => {
                    dot.className = "w-2.5 h-2.5 bg-emerald-500 rounded-full inline-block shadow shadow-emerald-500/50";
                    text.innerText = "LIVE STREAMING";
                    text.className = "text-xs font-semibold text-emerald-400 uppercase tracking-widest";
                    showToast("success", "Connected to Real-Time Agent Event Stream!");
                };

                sseSource.onerror = (e) => {
                    dot.className = "w-2.5 h-2.5 bg-red-500 rounded-full inline-block animate-pulse";
                    text.innerText = "DISCONNECTED";
                    text.className = "text-xs font-semibold text-slate-500 uppercase tracking-widest";
                    console.error("SSE Connection dropped. Reconnecting...", e);
                };

                // Central SSE Event dispatch routing
                sseSource.onmessage = (event) => {
                    const parsed = JSON.parse(event.data);
                    if (parsed.event === "ping") return; // Heartbeat ignore
                    
                    console.log("[SSE Event Received]: ", parsed);

                    if (parsed.event === "agent_status_change") {
                        // Locate and update agent statuses in local memory array
                        const index = agents.findIndex(a => a.agent_name === parsed.data.agent_name);
                        if(index !== -1) {
                            agents[index] = { ...agents[index], ...parsed.data };
                        }
                        renderAgentStatuses();
                        pushEventToConsoleLog("agent_status_change", `${parsed.data.agent_name} transitioned state to ${parsed.data.status}`);
                    }
                    
                    else if (parsed.event === "agent_handoff") {
                        pushEventToConsoleLog("agent_handoff", `HANDOFF: ${parsed.data.sender_agent} ➜ ${parsed.data.recipient_agent}`);
                        animateHandoffVisualLine(parsed.data.sender_agent, parsed.data.recipient_agent);
                    }
                    
                    else if (parsed.event === "activity") {
                        activityLog.unshift(parsed.data); // insert newest at beginning
                        renderActivityLogFeed();
                        pushEventToConsoleLog(parsed.data.action_type, `${parsed.data.actor}: ${parsed.data.action_details}`);
                    }
                    
                    else if (parsed.event === "lead_refresh") {
                        // Refetch entire DB on lead refresh updates
                        fetchInitialData();
                    }
                    
                    else if (parsed.event === "notify") {
                        showToast(parsed.data.type, parsed.data.message);
                        pushEventToConsoleLog("notify", parsed.data.message);
                    }
                };
            }


            // --- SVG PIPELINE VISUAL CONNECTOR LINE ANIMATIONS ---

            function animateHandoffVisualLine(sender, recipient) {
                const canvas = document.getElementById("handoff-svg-canvas");
                if(!canvas) return;
                
                let sNode = null, rNode = null;
                
                const mapAgentToNode = (name) => {
                    if (name === "CentralOrchestrator" || name === "SYSTEM") return "ingest";
                    if (name === "Research Agent") return "research";
                    if (name === "Deliverability Agent") return "deliver";
                    if (name === "Opportunity Mapping Agent") return "opp";
                    if (name === "Qualification Agent") return "qual";
                    if (name === "Outreach Agent") return "outreach";
                    if (name === "Inbox Management Agent") return "inbound";
                    return null;
                };

                const sId = mapAgentToNode(sender);
                const rId = mapAgentToNode(recipient);
                
                if (!sId || !rId) return;
                
                const sEl = document.getElementById(`node-${sId}`);
                const rEl = document.getElementById(`node-${rId}`);
                if (!sEl || !rEl) return;
                
                // Fetch elements relative bounding locations
                const canvasBounds = canvas.getBoundingClientRect();
                const sBounds = sEl.getBoundingClientRect();
                const rBounds = rEl.getBoundingClientRect();
                
                const x1 = sBounds.left + sBounds.width/2 - canvasBounds.left;
                const y1 = sBounds.top + sBounds.height/2 - canvasBounds.top;
                const x2 = rBounds.left + rBounds.width/2 - canvasBounds.left;
                const y2 = rBounds.top + rBounds.height/2 - canvasBounds.top;
                
                // Clear any existing active handoff lines
                const oldLine = document.getElementById("active-handoff-path");
                if(oldLine) oldLine.remove();
                
                // Create SVG glowing curve path
                const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
                path.setAttribute("id", "active-handoff-path");
                
                // Draw bezier curve for beauty
                const cx1 = x1 + (x2 - x1) / 2;
                const cy1 = y1 - 40; // slight curve upwards
                const d = `M ${x1} ${y1} Q ${cx1} ${cy1} ${x2} ${y2}`;
                
                path.setAttribute("d", d);
                path.setAttribute("fill", "none");
                path.setAttribute("stroke", "#3b82f6");
                path.setAttribute("stroke-width", "3");
                path.setAttribute("filter", "url(#glow-line)");
                path.setAttribute("class", "agent-line");
                
                canvas.appendChild(path);
                
                // Dissolve after 3.5s
                setTimeout(() => {
                    const l = document.getElementById("active-handoff-path");
                    if (l) l.remove();
                }, 3500);
            }


            // --- NOTIFICATION WIDGET TOAST FEED ---

            function showToast(type, message) {
                const con = document.getElementById("toast-container");
                if(!con) return;
                
                const toast = document.createElement("div");
                let bg = "bg-slate-900 border-slate-800 text-white";
                let icon = '<i class="fa-solid fa-circle-info text-blue-400"></i>';
                
                if (type === "success") {
                    bg = "bg-slate-900 border-emerald-800 text-emerald-100";
                    icon = '<i class="fa-solid fa-circle-check text-emerald-400"></i>';
                } else if (type === "warning") {
                    bg = "bg-slate-900 border-amber-800 text-amber-100";
                    icon = '<i class="fa-solid fa-triangle-exclamation text-amber-400"></i>';
                } else if (type === "error") {
                    bg = "bg-slate-900 border-red-800 text-red-100";
                    icon = '<i class="fa-solid fa-circle-xmark text-red-400"></i>';
                }
                
                toast.className = `flex items-center space-x-3 border p-4 rounded-xl shadow-2xl transition duration-300 transform translate-y-4 opacity-0 max-w-sm ${bg}`;
                toast.innerHTML = `
                    <div class="flex-shrink-0">${icon}</div>
                    <p class="text-xs font-semibold leading-relaxed">${message}</p>
                `;
                
                con.appendChild(toast);
                
                // Trigger transition slide-up
                setTimeout(() => {
                    toast.className = toast.className.replace("translate-y-4 opacity-0", "translate-y-0 opacity-100");
                }, 10);
                
                // Autorelease after 5s
                setTimeout(() => {
                    toast.className = toast.className.replace("translate-y-0 opacity-100", "translate-y-4 opacity-0");
                    setTimeout(() => toast.remove(), 300);
                }, 5000);
            }

            // ========================
            // SETTINGS & ICP FUNCTIONS
            // ========================
            let _allTenants = [];

            async function loadSettings() {
                try {
                    const [tenants, active] = await Promise.all([
                        fetch('/api/config/tenants').then(r => r.json()),
                        fetch('/api/config/tenant').then(r => r.json())
                    ]);
                    _allTenants = tenants;
                    renderTenantSwitcher(tenants, active.tenant_id);
                    populateSettingsForm(active);
                } catch(e) {
                    console.error('Failed to load settings', e);
                }
            }

            function renderTenantSwitcher(tenants, activeTenantId) {
                const el = document.getElementById('tenant-switcher');
                el.innerHTML = tenants.map(t => `
                    <button onclick="switchTenant('${t.tenant_id}')" class="px-4 py-2 rounded-xl text-sm font-bold transition border ${
                        t.tenant_id === activeTenantId
                            ? 'bg-brand-600 border-brand-500 text-white'
                            : 'bg-slate-800 border-slate-700 text-slate-300 hover:bg-slate-700'
                    }">
                        ${t.display_name || t.tenant_id}
                    </button>
                `).join('');
            }

            async function switchTenant(tenantId) {
                await fetch(`/api/config/tenant/switch/${tenantId}`, {method: 'POST'});
                showToast('success', `Switched to ${tenantId}`);
                loadSettings();
            }

            function populateSettingsForm(config) {
                document.getElementById('settings-tenant-id').value = config.tenant_id || '';
                document.getElementById('settings-display-name').value = config.display_name || '';
                document.getElementById('settings-sending-email').value = config.sending_email || '';
                document.getElementById('settings-calendar-link').value = config.calendar_link || '';
                document.getElementById('settings-company-size').value = config.icp_company_size || '';
                document.getElementById('settings-value-doctrine').value = config.value_doctrine || '';
                document.getElementById('settings-signature').value = config.signature_block || '';
                document.getElementById('settings-brand-voice').value = config.brand_voice || '';
                // Arrays -> one per line
                const industries = Array.isArray(config.icp_industries) ? config.icp_industries : [];
                const roles = Array.isArray(config.icp_roles) ? config.icp_roles : [];
                const queries = Array.isArray(config.search_queries) ? config.search_queries : [];
                document.getElementById('settings-industries').value = industries.join('\\n');
                document.getElementById('settings-roles').value = roles.join('\\n');
                document.getElementById('settings-queries').value = queries.join('\\n');
                // Show setup status
                checkSetupStatus(config);
            }

            function checkSetupStatus(config) {
                const el = document.getElementById('settings-status');
                const missing = [];
                if (!config.value_doctrine) missing.push('Value Doctrine');
                if (!config.sending_email) missing.push('Sending Email');
                if (!config.calendar_link) missing.push('Calendar Link');
                if (!config.signature_block) missing.push('Email Signature');
                const queries = Array.isArray(config.search_queries) ? config.search_queries : [];
                if (!queries.length) missing.push('Lead Sourcing Queries');
                if (missing.length > 0) {
                    el.className = 'mt-6 bg-amber-500/10 border border-amber-500/30 rounded-xl p-4';
                    el.innerHTML = `<div class="flex items-start gap-3"><i class="fa-solid fa-triangle-exclamation text-amber-400 mt-0.5"></i><div><p class="text-sm font-bold text-amber-300">Setup Incomplete — Agents running in limited mode</p><p class="text-xs text-amber-400/70 mt-1">Missing: ${missing.join(', ')}. Fill these in and save to unlock full personalized outreach.</p></div></div>`;
                    el.classList.remove('hidden');
                } else {
                    el.className = 'mt-6 bg-emerald-500/10 border border-emerald-500/30 rounded-xl p-4';
                    el.innerHTML = '<div class="flex items-center gap-3"><i class="fa-solid fa-circle-check text-emerald-400"></i><p class="text-sm font-bold text-emerald-300">All systems configured — Agents running at full capacity</p></div>';
                    el.classList.remove('hidden');
                }
            }

            async function saveSettings() {
                const tenantId = document.getElementById('settings-tenant-id').value || 'injexion';
                const industries = document.getElementById('settings-industries').value.split('\\n').map(s=>s.trim()).filter(Boolean);
                const roles = document.getElementById('settings-roles').value.split('\\n').map(s=>s.trim()).filter(Boolean);
                const queries = document.getElementById('settings-queries').value.split('\\n').map(s=>s.trim()).filter(Boolean);
                const payload = {
                    tenant_id: tenantId,
                    display_name: document.getElementById('settings-display-name').value.trim(),
                    sending_email: document.getElementById('settings-sending-email').value.trim(),
                    sending_domain: document.getElementById('settings-sending-email').value.trim().split('@')[1] || '',
                    calendar_link: document.getElementById('settings-calendar-link').value.trim(),
                    signature_block: document.getElementById('settings-signature').value.trim(),
                    value_doctrine: document.getElementById('settings-value-doctrine').value.trim(),
                    brand_voice: document.getElementById('settings-brand-voice').value.trim(),
                    icp_industries: industries,
                    icp_roles: roles,
                    icp_company_size: document.getElementById('settings-company-size').value.trim(),
                    search_queries: queries
                };
                try {
                    const res = await fetch('/api/config/tenant', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
                    if (res.ok) {
                        showToast('success', 'Configuration saved! Agents will use new settings on next cycle.');
                        loadSettings();
                    } else {
                        const err = await res.json();
                        showToast('error', `Save failed: ${err.detail || 'Unknown error'}`);
                    }
                } catch(e) {
                    showToast('error', 'Network error saving settings.');
                }
            }
            
            async function connectGmail() {
                const tenantId = document.getElementById('settings-tenant-id').value;
                if(!tenantId) {
                    showToast('error', 'No active profile selected.');
                    return;
                }
                showToast('info', 'Opening Google Authentication in a new tab/window...');
                try {
                    const res = await fetch(`/api/config/tenant/${tenantId}/connect-gmail`, {method: 'POST'});
                    const data = await res.json();
                    if(res.ok) {
                        showToast('success', 'Follow the prompts in your browser window to connect.');
                    }
                } catch (e) {
                    showToast('error', 'Failed to trigger Gmail auth.');
                }
            }


            // ========================
            // METRICS TAB
            // ========================
            function pct(val) { return (val * 100).toFixed(1) + '%'; }
            function metricColor(actual, target, higherIsBetter=true) {
                if (!target) return 'text-white';
                return (higherIsBetter ? actual >= target : actual <= target) ? 'text-emerald-400' : 'text-red-400';
            }

            async function loadMetrics() {
                try {
                    const d = await fetch('/api/metrics').then(r => r.json());
                    const t = d.targets;

                    function setKPI(elId, val, targetEl, targetVal, higherIsBetter=true) {
                        const el = document.getElementById(elId);
                        el.textContent = pct(val);
                        el.className = `text-3xl font-black ${metricColor(val, targetVal, higherIsBetter)}`;
                        if (targetEl) document.getElementById(targetEl).textContent = `Target: ${pct(targetVal)}`;
                    }

                    setKPI('m-open-rate', d.email.open_rate, 'm-open-target', t.target_open_rate);
                    setKPI('m-reply-rate', d.email.reply_rate, 'm-reply-target', t.target_reply_rate);
                    setKPI('m-meeting-rate', d.meeting_rate, 'm-meeting-target', t.target_meeting_rate);
                    setKPI('m-bounce-rate', d.email.bounce_rate, 'm-bounce-target', t.target_bounce_rate, false);
                    setKPI('m-li-accept', d.linkedin.accept_rate, 'm-li-accept-target', t.target_li_accept_rate);
                    setKPI('m-li-reply', d.linkedin.reply_rate, 'm-li-reply-target', t.target_li_reply_rate);

                    document.getElementById('m-total-sent').textContent = d.email.sent;
                    document.getElementById('m-velocity').textContent = d.pipeline.avg_velocity_days ? `${d.pipeline.avg_velocity_days}d` : '—';

                    // Load targets into inputs
                    document.getElementById('t-open').value = Math.round(t.target_open_rate * 100);
                    document.getElementById('t-reply').value = Math.round(t.target_reply_rate * 100);
                    document.getElementById('t-meeting').value = (t.target_meeting_rate * 100).toFixed(1);
                    document.getElementById('t-bounce').value = (t.target_bounce_rate * 100).toFixed(1);
                    document.getElementById('t-li-accept').value = Math.round(t.target_li_accept_rate * 100);
                    document.getElementById('t-li-reply').value = Math.round(t.target_li_reply_rate * 100);
                } catch(e) { console.error('Metrics load failed', e); }
            }

            async function saveTargets() {
                const payload = {
                    tenant_id: 'injexion',
                    target_open_rate: parseFloat(document.getElementById('t-open').value) / 100,
                    target_reply_rate: parseFloat(document.getElementById('t-reply').value) / 100,
                    target_meeting_rate: parseFloat(document.getElementById('t-meeting').value) / 100,
                    target_bounce_rate: parseFloat(document.getElementById('t-bounce').value) / 100,
                    target_li_accept_rate: parseFloat(document.getElementById('t-li-accept').value) / 100,
                    target_li_reply_rate: parseFloat(document.getElementById('t-li-reply').value) / 100,
                };
                await fetch('/api/metrics/targets', {method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload)});
                showToast('success', 'Targets saved! Metrics will now compare against your goals.');
                loadMetrics();
            }

            // ========================
            // PREFAB BERICHTEN
            // ========================
            let _templates = [];
            const CATEGORY_LABELS = {COLD:'Cold Email', FOLLOWUP:'Follow-up', SOCIAL_PROOF:'Social Proof', LINKEDIN_NOTE:'LinkedIn Note'};
            const CATEGORY_COLORS = {COLD:'bg-blue-500/10 text-blue-300 border-blue-500/20', FOLLOWUP:'bg-amber-500/10 text-amber-300 border-amber-500/20', SOCIAL_PROOF:'bg-emerald-500/10 text-emerald-300 border-emerald-500/20', LINKEDIN_NOTE:'bg-purple-500/10 text-purple-300 border-purple-500/20'};

            async function loadTemplates() {
                _templates = await fetch('/api/templates').then(r => r.json());
                renderTemplates();
            }

            function renderTemplates() {
                const grid = document.getElementById('templates-grid');
                if (!_templates.length) { grid.innerHTML = '<p class="text-slate-500 text-sm col-span-2">No templates yet. Click New Template to add one.</p>'; return; }
                grid.innerHTML = _templates.map(t => `
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5">
                        <div class="flex items-start justify-between mb-3">
                            <div>
                                <span class="inline-block text-[10px] font-bold px-2 py-0.5 rounded-full border mb-2 ${CATEGORY_COLORS[t.category] || 'bg-slate-700 text-slate-300 border-slate-600'}">${CATEGORY_LABELS[t.category] || t.category}</span>
                                <h3 class="text-sm font-bold text-white">${t.name}</h3>
                                ${t.subject_line ? `<p class="text-xs text-slate-400 mt-0.5">Onderwerp: ${t.subject_line}</p>` : ''}
                            </div>
                            <div class="flex gap-2">
                                <button onclick="editTemplate('${t.template_id}')" class="text-slate-400 hover:text-white transition text-xs"><i class="fa-solid fa-pen"></i></button>
                                <button onclick="deleteTemplate('${t.template_id}')" class="text-slate-600 hover:text-red-400 transition text-xs"><i class="fa-solid fa-trash"></i></button>
                            </div>
                        </div>
                        <pre class="text-xs text-slate-400 whitespace-pre-wrap bg-slate-950 rounded-xl p-3 max-h-48 overflow-y-auto font-mono">${t.body}</pre>
                        <div class="flex gap-2 mt-3 flex-wrap">
                            ${(JSON.parse(t.variables || '[]')).map(v => `<span class="text-[10px] bg-slate-800 text-slate-400 px-2 py-0.5 rounded-full">{{${v}}}</span>`).join('')}
                        </div>
                    </div>
                `).join('');
            }

            function showNewTemplateForm() {
                document.getElementById('edit-template-id').value = '';
                document.getElementById('edit-tmpl-name').value = '';
                document.getElementById('edit-tmpl-subject').value = '';
                document.getElementById('edit-tmpl-body').value = '';
                document.getElementById('edit-tmpl-category').value = 'COLD';
                document.getElementById('template-editor').classList.remove('hidden');
            }

            function editTemplate(id) {
                const t = _templates.find(t => t.template_id === id);
                if (!t) return;
                document.getElementById('edit-template-id').value = t.template_id;
                document.getElementById('edit-tmpl-name').value = t.name;
                document.getElementById('edit-tmpl-subject').value = t.subject_line || '';
                document.getElementById('edit-tmpl-body').value = t.body;
                document.getElementById('edit-tmpl-category').value = t.category;
                document.getElementById('template-editor').classList.remove('hidden');
            }

            async function saveTemplate() {
                const id = document.getElementById('edit-template-id').value;
                const body = document.getElementById('edit-tmpl-body').value;
                const vars = [...new Set([...body.matchAll(/\{\{(\w+)\}\}/g)].map(m => m[1]))];
                const payload = {
                    name: document.getElementById('edit-tmpl-name').value,
                    category: document.getElementById('edit-tmpl-category').value,
                    subject_line: document.getElementById('edit-tmpl-subject').value,
                    body, variables: vars
                };
                if (id) {
                    await fetch(`/api/templates/${id}`, {method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
                } else {
                    await fetch('/api/templates', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
                }
                document.getElementById('template-editor').classList.add('hidden');
                showToast('success', 'Template saved!');
                loadTemplates();
            }

            async function deleteTemplate(id) {
                if (!confirm('Delete this template?')) return;
                await fetch(`/api/templates/${id}`, {method:'DELETE'});
                showToast('info', 'Template deleted.');
                loadTemplates();
            }

            // ========================
            // STRATEGY BUILDER
            // ========================
            let _strategyStepCount = 0;
            let _strategies = [];

            async function loadStrategies() {
                _strategies = await fetch('/api/strategies').then(r => r.json());
                const container = document.getElementById('strategies-list');
                if (!_strategies.length) { container.innerHTML = '<p class="text-slate-500 text-sm">No strategies yet. Click New Strategy to create one.</p>'; return; }
                container.innerHTML = _strategies.map(s => `
                    <div class="bg-slate-900 border ${s.active ? 'border-brand-500/50' : 'border-slate-800'} rounded-2xl p-5">
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <div class="flex items-center gap-3">
                                    <h3 class="text-sm font-bold text-white">${s.name}</h3>
                                    ${s.active ? '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-brand-500/10 text-brand-400 border border-brand-500/20">ACTIVE</span>' : ''}
                                </div>
                                ${s.description ? `<p class="text-xs text-slate-400 mt-0.5">${s.description}</p>` : ''}
                            </div>
                            ${!s.active ? `<button onclick="activateStrategy('${s.strategy_id}')" class="text-xs bg-brand-600 hover:bg-brand-500 text-white font-bold px-3 py-1.5 rounded-lg transition">Activate</button>` : ''}
                        </div>
                        <div class="flex items-center gap-2 overflow-x-auto pb-2">
                            ${(s.steps || []).map((step, i) => `
                                <div class="flex items-center gap-2 shrink-0">
                                    <div class="bg-slate-800 rounded-xl px-3 py-2 text-center min-w-[90px]">
                                        <p class="text-[10px] text-slate-500 font-bold uppercase">${step.step_type}</p>
                                        <p class="text-xs text-white font-bold mt-0.5">Day ${step.delay_days}</p>
                                    </div>
                                    ${i < (s.steps.length - 1) ? '<i class="fa-solid fa-arrow-right text-slate-600"></i>' : ''}
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `).join('');
            }

            function showNewStrategyForm() {
                _strategyStepCount = 0;
                document.getElementById('strat-name').value = '';
                document.getElementById('strat-desc').value = '';
                document.getElementById('strategy-steps-list').innerHTML = '';
                // Add 4 default steps
                addStrategyStep('EMAIL', 0, 'Day 0: Cold Email');
                addStrategyStep('LINKEDIN', 3, 'Day 3: LinkedIn Connect');
                addStrategyStep('EMAIL', 7, 'Day 7: Follow-up Email');
                addStrategyStep('EMAIL', 14, 'Day 14: Final Email');
                document.getElementById('strategy-editor').classList.remove('hidden');
            }

            function addStrategyStep(type='EMAIL', delay=0, label='') {
                _strategyStepCount++;
                const n = _strategyStepCount;
                const container = document.getElementById('strategy-steps-list');
                const el = document.createElement('div');
                el.id = `step-${n}`;
                el.className = 'bg-slate-800 border border-slate-700 rounded-xl p-4 grid grid-cols-4 gap-3 items-end';
                el.innerHTML = `
                    <div>
                        <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Step ${n}</label>
                        <select name="type" class="w-full bg-slate-950 border border-slate-700 rounded-lg px-2 py-2 text-sm text-white">
                            <option value="EMAIL" ${type==='EMAIL'?'selected':''}>Email</option>
                            <option value="LINKEDIN" ${type==='LINKEDIN'?'selected':''}>LinkedIn</option>
                            <option value="WAIT" ${type==='WAIT'?'selected':''}>Wait</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Delay (days)</label>
                        <input name="delay" type="number" min="0" value="${delay}" class="w-full bg-slate-950 border border-slate-700 rounded-lg px-2 py-2 text-sm text-white">
                    </div>
                    <div>
                        <label class="text-[10px] text-slate-400 font-bold uppercase tracking-wider block mb-1">Label</label>
                        <input name="label" type="text" placeholder="e.g. Follow-up #1" value="${label}" class="w-full bg-slate-950 border border-slate-700 rounded-lg px-2 py-2 text-sm text-white">
                    </div>
                    <div>
                        <button onclick="this.closest('div[id]').remove()" class="w-full bg-red-500/10 hover:bg-red-500/20 text-red-400 text-xs font-bold rounded-lg px-2 py-2 transition">Remove</button>
                    </div>
                `;
                container.appendChild(el);
            }

            async function saveStrategy() {
                const name = document.getElementById('strat-name').value.trim();
                if (!name) { showToast('error', 'Strategy needs a name.'); return; }
                const stepEls = document.querySelectorAll('#strategy-steps-list > div[id]');
                const steps = Array.from(stepEls).map((el, i) => ({
                    step_type: el.querySelector('[name=type]').value,
                    delay_days: parseInt(el.querySelector('[name=delay]').value) || 0,
                    subject_line: el.querySelector('[name=label]').value
                }));
                const payload = { name, description: document.getElementById('strat-desc').value, tenant_id: 'injexion', steps };
                await fetch('/api/strategies', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
                document.getElementById('strategy-editor').classList.add('hidden');
                showToast('success', 'Strategy saved and activated!');
                loadStrategies();
            }

            async function activateStrategy(id) {
                await fetch(`/api/strategies/${id}/activate`, {method:'POST'});
                showToast('success', 'Strategy activated!');
                loadStrategies();
            }

            // On Document Bootstrap
            window.onload = () => {
                fetchInitialData();
                startSSEListener();
                checkLinkedInLoginStatus();
                // Bug 10 fix: pre-warm all tab data so clicking any tab shows data instantly
                setTimeout(() => { loadMetrics(); }, 500);
                setTimeout(() => { loadTemplates(); }, 800);
                setTimeout(() => { loadStrategies(); }, 1100);
                
                // Recalculate handoff lines when window sizes adapt
                window.onresize = () => {
                    const l = document.getElementById("active-handoff-path");
                    if (l) l.remove();
                };
            };

            /** Open approval modal with draft data */
            function openApprovalModal(leadId) {
                const lead = window._cachedLeads?.find(l => l.contact_id === leadId) || leads?.find(l => l.contact_id === leadId);
                if(!lead) {
                    showToast("error", "Lead data not loaded yet. Try again.");
                    return;
                }
                
                let draft = {};
                if (lead.outreach_draft) {
                    try {
                        draft = typeof lead.outreach_draft === "string" ? JSON.parse(lead.outreach_draft) : lead.outreach_draft;
                    } catch (e) {
                        console.error("Error parsing outreach draft:", e);
                    }
                }
                
                document.getElementById("approval-modal").dataset.leadId = leadId;
                document.getElementById("approval-modal-lead-info").textContent = (lead.first_name || "Unknown") + " — " + (lead.company_name || "Unknown Company");
                
                // Fallback to subject / body from either draft format, or default template
                document.getElementById("approval-subject").value = draft.subject_line_A || draft.subject || `Idee voor ${lead.company_name || 'jullie bedrijf'}`;
                
                const defaultBody = `Beste ${lead.first_name || 'relatie'},\n\n` +
                    `Ik zag dat ${lead.company_name || 'jullie bedrijf'} actief is met digitalisering. Wij bouwen maatwerk AI-assistenten die tijd besparen.\n\n` +
                    `Zouden we kort kunnen bellen?\n\n` +
                    `Met vriendelijke groet,\n\n` +
                    `Marvin van der Sluis\n` +
                    `ClawBuildr\n` +
                    `https://odysseus.com/\n` +
                    `https://goldclaw.ai/`;
                
                document.getElementById("approval-body").value = draft.email_body_step_1 || draft.body || defaultBody;
                
                const modal = document.getElementById("approval-modal");
                modal.classList.remove("hidden");
                modal.style.display = "flex";
            }

            function closeApprovalModal() {
                const modal = document.getElementById("approval-modal");
                modal.classList.add("hidden");
                modal.style.display = "";
            }

            async function saveDraftAndSend() {
                const leadId = document.getElementById("approval-modal").dataset.leadId;
                const subject = document.getElementById("approval-subject").value.trim();
                const body = document.getElementById("approval-body").value.trim();
                if(!subject || !body) { showToast("error", "Subject and body cannot be empty."); return; }
                try {
                    // Save draft edits first
                    const draftRes = await fetch(`/api/leads/${leadId}/draft`, {
                        method: "PUT",
                        headers: {"Content-Type": "application/json"},
                        body: JSON.stringify({subject, body})
                    });
                    if(!draftRes.ok) {
                        const err = await draftRes.json();
                        showToast("error", `Draft save failed: ${err.detail || 'Unknown error'}`);
                        return;
                    }
                    closeApprovalModal();
                    // Then send
                    await approveOutreach(leadId);
                } catch(e) {
                    console.error("saveDraftAndSend error:", e);
                    showToast("error", "Failed to save draft and send. Check console.");
                }
            }

            /** Delete lead from pipeline */
            async function deleteLead(leadId) {
                if(!confirm("Permanently remove this lead and company from the pipeline?")) return;
                const res = await fetch(`/api/leads/${leadId}`, {method: "DELETE"});
                if(res.ok) {
                    showToast("success", "Lead deleted from pipeline");
                    await fetchInitialData();
                } else {
                    showToast("error", "Failed to delete lead");
                }
            }

            // --- LinkedIn Posts Functions ---

            async function loadPosts() {
                try {
                    const res = await fetch("/api/linkedin/posts");
                    const data = await res.json();
                    const posts = data.posts || [];
                    window._cachedPosts = posts;

                    // Update stats
                    const drafts = posts.filter(p => p.status === 'draft').length;
                    const queued = posts.filter(p => p.status === 'queued').length;
                    const posted = posts.filter(p => p.status === 'posted').length;
                    const failed = posts.filter(p => p.status === 'failed').length;

                    document.getElementById("post-stat-draft").innerText = drafts;
                    document.getElementById("post-stat-queued").innerText = queued;
                    document.getElementById("post-stat-posted").innerText = posted;
                    document.getElementById("post-stat-failed").innerText = failed;

                    // Populate table
                    const tbody = document.getElementById("posts-table-rows");
                    if (!tbody) return;
                    tbody.innerHTML = "";

                    if (posts.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="5" class="py-12 text-center text-slate-500"><i class="fa-solid fa-pen-to-square text-3xl block mb-2"></i>No posts yet. Add your first post above.</td></tr>';
                        return;
                    }

                    posts.forEach(p => {
                        const tr = document.createElement("tr");
                        tr.className = "hover:bg-slate-800/30 cursor-pointer";
                        tr.onclick = () => openPostPreview(p.id);

                        let statusBadge = "";
                        if (p.status === 'draft') statusBadge = '<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-500/15 text-slate-400 border border-slate-500/30">Draft</span>';
                        else if (p.status === 'queued') statusBadge = '<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-blue-500/15 text-blue-400 border border-blue-500/30">Queued</span>';
                        else if (p.status === 'posted') statusBadge = '<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">Posted</span>';
                        else if (p.status === 'failed') statusBadge = '<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-red-500/15 text-red-400 border border-red-500/30">Failed</span>';

                        const time = p.scheduled_time ? new Date(p.scheduled_time).toLocaleString('nl-NL', {day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}) : '-';
                        const contentPreview = (p.content || '').substring(0, 120) + ((p.content || '').length > 120 ? '...' : '');

                        let actions = '';
                        if (p.status === 'draft') {
                            actions = `<button onclick="updatePostStatus(${p.id}, 'queued')" class="text-[10px] text-blue-400 hover:text-blue-300 font-bold">Queue</button>
                                       <button onclick="deletePost(${p.id})" class="text-[10px] text-red-400 hover:text-red-300 font-bold ml-2">Delete</button>`;
                        } else if (p.status === 'queued') {
                            actions = `<button onclick="publishPostNow(${p.id})" class="text-[10px] text-emerald-400 hover:text-emerald-300 font-bold">Publish Now</button>
                                       <button onclick="updatePostStatus(${p.id}, 'draft')" class="text-[10px] text-slate-400 hover:text-slate-300 font-bold ml-2">Unqueue</button>
                                       <button onclick="deletePost(${p.id})" class="text-[10px] text-red-400 hover:text-red-300 font-bold ml-2">Delete</button>`;
                        } else if (p.status === 'posted') {
                            actions = '<span class="text-[10px] text-emerald-500 font-bold">Done</span>';
                        } else if (p.status === 'failed') {
                            actions = `<button onclick="updatePostStatus(${p.id}, 'queued')" class="text-[10px] text-blue-400 hover:text-blue-300 font-bold">Retry</button>
                                       <button onclick="deletePost(${p.id})" class="text-[10px] text-red-400 hover:text-red-300 font-bold ml-2">Delete</button>`;
                        }

                        tr.innerHTML = `
                            <td class="py-4 px-6 text-xs text-slate-400 font-semibold">${p.topic || '-'}</td>
                            <td class="py-4 px-6 text-xs text-slate-300 max-w-[400px]">${contentPreview}</td>
                            <td class="py-4 px-6 text-xs text-slate-500">${time}</td>
                            <td class="py-4 px-6">${statusBadge}</td>
                            <td class="py-4 px-6">${actions}</td>
                        `;
                        tbody.appendChild(tr);
                    });
                } catch (e) {
                    console.error("Failed to load posts:", e);
                }
            }

            async function addNewPost(defaultStatus) {
                const content = document.getElementById("new-post-content").value.trim();
                if (!content) { showToast("error", "Post content is empty"); return; }

                const topic = document.getElementById("new-post-topic").value.trim();
                const scheduleInput = document.getElementById("new-post-schedule").value;
                const scheduledTime = scheduleInput ? new Date(scheduleInput).toISOString().replace('T', ' ').substring(0, 19) : null;

                const status = scheduledTime ? 'queued' : defaultStatus;
                const params = new URLSearchParams({content, topic, scheduled_time: scheduledTime || ''});

                const res = await fetch(`/api/linkedin/posts/add?${params}`, {method: "POST"});
                if (res.ok) {
                    showToast("success", "Post added");
                    document.getElementById("new-post-content").value = "";
                    document.getElementById("new-post-topic").value = "";
                    document.getElementById("new-post-schedule").value = "";
                    await loadPosts();
                } else {
                    showToast("error", "Failed to add post");
                }
            }

            async function updatePostStatus(postId, status) {
                await fetch(`/api/linkedin/posts/${postId}/update?status=${status}`, {method: "POST"});
                await loadPosts();
            }

            async function deletePost(postId) {
                if (!confirm("Delete this post?")) return;
                await fetch(`/api/linkedin/posts/${postId}/delete`, {method: "POST"});
                await loadPosts();
            }

            async function publishPostNow(postId) {
                showToast("info", "Publishing post...");
                const res = await fetch(`/api/linkedin/posts/${postId}/publish`, {method: "POST"});
                const data = await res.json();
                if (data.success) {
                    showToast("success", "Post published!");
                } else {
                    showToast("error", "Failed: " + (data.error || "unknown"));
                }
                await loadPosts();
            }

            async function togglePostScheduler() {
                const statusRes = await fetch("/api/linkedin/posts/scheduler/status");
                const statusData = await statusRes.json();

                if (statusData.running) {
                    await fetch("/api/linkedin/posts/scheduler/stop", {method: "POST"});
                    showToast("info", "Post scheduler stopped");
                } else {
                    await fetch("/api/linkedin/posts/scheduler/start", {method: "POST"});
                    showToast("success", "Post scheduler started");
                }
                await updatePostSchedulerUI();
            }

            async function updatePostSchedulerUI() {
                try {
                    const res = await fetch("/api/linkedin/posts/scheduler/status");
                    const data = await res.json();
                    const dot = document.getElementById("post-sched-dot");
                    const text = document.getElementById("post-sched-text");
                    const btn = document.getElementById("post-sched-btn");
                    if (data.running) {
                        dot.className = "w-2.5 h-2.5 bg-emerald-500 rounded-full inline-block";
                        text.innerText = "Running";
                        btn.innerHTML = '<i class="fa-solid fa-stop"></i> Stop Scheduler';
                        btn.className = "bg-red-600 hover:bg-red-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition";
                    } else {
                        dot.className = "w-2.5 h-2.5 bg-red-500 rounded-full inline-block";
                        text.innerText = "Stopped";
                        btn.innerHTML = '<i class="fa-solid fa-play"></i> Start Scheduler';
                        btn.className = "bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition";
                    }
                } catch (e) {}
            }

            function openPostPreview(postId) {
                const posts = window._cachedPosts || [];
                const post = posts.find(p => p.id === postId);
                if (!post) return;
                document.getElementById("preview-post-topic").innerText = post.topic || 'General';
                document.getElementById("preview-post-content").innerText = post.content;
                document.getElementById("post-preview-modal").classList.remove("hidden");
            }

            function closePostPreview() {
                document.getElementById("post-preview-modal").classList.add("hidden");
            }
        </script>

        <!-- Post Preview Modal -->
        <div id="post-preview-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
            <div class="absolute inset-0 bg-black/70" onclick="closePostPreview()"></div>
            <div class="relative bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl w-full max-w-2xl mx-4 max-h-[80vh] overflow-y-auto">
                <div class="p-6 border-b border-slate-800 flex items-center justify-between">
                    <div>
                        <h3 class="text-lg font-extrabold text-white">LinkedIn Post Preview</h3>
                        <p class="text-xs text-slate-400 mt-1" id="preview-post-topic">Topic</p>
                    </div>
                    <button onclick="closePostPreview()" class="text-slate-400 hover:text-white p-2 rounded-lg transition">
                        <i class="fa-solid fa-xmark text-xl"></i>
                    </button>
                </div>
                <div class="p-6">
                    <div class="bg-slate-950 border border-slate-800 rounded-xl p-5">
                        <div class="flex items-center gap-3 mb-4">
                            <div class="w-10 h-10 bg-blue-600 rounded-full flex items-center justify-center text-white font-bold text-sm">MvS</div>
                            <div>
                                <p class="text-sm font-bold text-white">Marvin van der Sluis</p>
                                <p class="text-[10px] text-slate-500">Founder @ FounderFlow | AI Outreach Systems</p>
                            </div>
                        </div>
                        <div class="text-sm text-slate-200 leading-relaxed whitespace-pre-wrap" id="preview-post-content"></div>
                    </div>
                    <div class="flex justify-end gap-3 mt-4">
                        <button onclick="closePostPreview()" class="bg-slate-800 hover:bg-slate-700 text-slate-300 font-semibold text-sm px-4 py-2 rounded-xl transition">Close</button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Approval Modal: Review & Edit Email Draft Before Sending -->
        <div id="approval-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
            <div class="absolute inset-0 bg-black/70" onclick="closeApprovalModal()"></div>
            <div class="relative bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto">
                <div class="p-6 border-b border-slate-800 flex items-center justify-between">
                    <div>
                        <h3 class="text-lg font-extrabold text-white">Review Outbound Email</h3>
                        <p class="text-xs text-slate-400 mt-1" id="approval-modal-lead-info">Lead name — Company</p>
                    </div>
                    <button onclick="closeApprovalModal()" class="text-slate-400 hover:text-white p-2 rounded-lg transition">
                        <i class="fa-solid fa-xmark text-xl"></i>
                    </button>
                </div>
                <div class="p-6 space-y-4">
                    <div>
                        <label class="text-[11px] text-slate-500 font-bold uppercase tracking-wider block mb-1.5">Subject Line</label>
                        <input id="approval-subject" type="text" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-brand-500/80">
                    </div>
                    <div>
                        <label class="text-[11px] text-slate-500 font-bold uppercase tracking-wider block mb-1.5">Email Body</label>
                        <textarea id="approval-body" rows="16" class="w-full bg-slate-950 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white font-mono leading-relaxed focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
                    </div>
                    <div class="bg-slate-950 border border-slate-800 rounded-xl p-4 space-y-2">
                        <span class="text-[11px] text-amber-400 font-bold uppercase tracking-wider flex items-center gap-2"><i class="fa-solid fa-triangle-exclamation"></i> What to check before sending</span>
                        <ul class="text-xs text-slate-400 space-y-1.5 list-disc list-inside">
                            <li>Is the company name spelled correctly?</li>
                            <li>Does the email reference their specific industry/services?</li>
                            <li>Is the tone appropriate for their business type?</li>
                            <li>Does the CTA match their likely decision-making authority?</li>
                        </ul>
                    </div>
                </div>
                <div class="p-6 border-t border-slate-800 flex gap-3 justify-end">
                    <button onclick="closeApprovalModal()" class="px-5 py-2.5 border border-slate-700 text-slate-300 font-bold text-sm rounded-xl hover:bg-slate-800 transition">Cancel</button>
                    <button onclick="saveDraftAndSend()" class="px-5 py-2.5 bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm rounded-xl shadow-lg shadow-brand-500/15 transition flex items-center gap-2">
                        <i class="fa-solid fa-paper-plane"></i> Send Email
                    </button>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# =========================================================================
# 5. EXECUTION BOOTSTRAP
# =========================================================================

if __name__ == "__main__":
    import uvicorn
    # Execute the FastAPI server on Port 8000
    uvicorn.run("clawbuildr_dashboard:app", host="0.0.0.0", port=8000, reload=True)
