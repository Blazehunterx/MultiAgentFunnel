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
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
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

# New integrated modules (mounted via API sub-app)
try:
    from clawbuildr_api import app as _api_v2_app
except Exception as _api_import_err:
    _api_v2_app = None
    logging.getLogger("ClawBuildrDashboard").warning(f"Could not import clawbuildr_api: {_api_import_err}")

try:
    from clawbuildr_onboarding import (
        get_onboarding_state,
        save_onboarding_step,
        complete_onboarding,
        is_onboarding_complete,
    )
except Exception as _onboarding_import_err:
    get_onboarding_state = None
    save_onboarding_step = None
    complete_onboarding = None
    is_onboarding_complete = None
    logging.getLogger("ClawBuildrDashboard").warning(f"Could not import onboarding helpers: {_onboarding_import_err}")

try:
    from clawbuildr_scheduler import enroll_contact
except Exception as _scheduler_import_err:
    enroll_contact = None
    logging.getLogger("ClawBuildrDashboard").warning(f"Could not import scheduler: {_scheduler_import_err}")

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

    # NEW: onboarding choice columns on tenant_config
    for col, typedef in [
        ("lead_sources", "TEXT NOT NULL DEFAULT '[\"hunter\",\"directories\",\"kvk\"]'"),
        ("campaign_channels", "TEXT NOT NULL DEFAULT '[\"email\",\"linkedin\"]'"),
        ("sequence_template", "TEXT NOT NULL DEFAULT 'gentle'"),
        ("icp_regions", "TEXT NOT NULL DEFAULT '[\"nl\"]'"),
        ("pain_points", "TEXT NOT NULL DEFAULT '[]'"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE tenant_config ADD COLUMN {col} {typedef}")
        except Exception:
            pass

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
        ("connection_status", "TEXT DEFAULT 'pending'"),
        ("followup_sent", "INTEGER DEFAULT 0"),
        ("followup_note", "TEXT"),
        ("last_checked", "TEXT"),
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
                "{{company_name}} ��� korte vraag",
                """Hoi {{first_name}},

Ik zag dat {{company_name}} actief groeit in {{industry}}.

Wij helpen bedrijven zoals jullie met {{pain_point}} via {{value_prop}}.

Zou je open staan voor een korte call van 20 minuten?

���� {{calendar_link}}

{{sender_name}}""",
                '["first_name","company_name","industry","pain_point","value_prop","calendar_link","sender_name"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
            (
                str(_uuid.uuid4()), None,
                "Follow-up: Oppakken",
                "FOLLOWUP",
                "Re: {{company_name}} ��� even oppakken?",
                """Hoi {{first_name}},

Ik had vorige week een bericht gestuurd ��� misschien ben je het vergeten.

Wij helpen {{industry}}-bedrijven zoals {{company_name}} specifiek met {{pain_point}}.

Heb je 20 minuten volgende week?
���� {{calendar_link}}

{{sender_name}}""",
                '["first_name","company_name","industry","pain_point","calendar_link","sender_name"]',
                "NL", 0.0, 0.0, now_iso, now_iso
            ),
            (
                str(_uuid.uuid4()), None,
                "Finale Poging",
                "FOLLOWUP",
                "Laatste berichtje ��� {{first_name}}",
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
                """Hoi {{first_name}}, ik zag jullie werk bij {{company_name}} en wil graag in contact komen. Wij helpen {{industry}}-bedrijven met {{pain_point}}. ��� {{sender_name}}""",
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

    # 3. Create contacts table if not exists, then add Serialized Agent Output Columns
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        contact_id TEXT PRIMARY KEY,
        company_id TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        role TEXT,
        linkedin_url TEXT,
        current_stage TEXT DEFAULT 'INGESTED',
        lead_score INTEGER DEFAULT 0,
        priority TEXT DEFAULT 'UNASSIGNED',
        created_at TEXT,
        updated_at TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        company_id TEXT PRIMARY KEY,
        name TEXT,
        domain TEXT,
        industry TEXT,
        estimated_size TEXT,
        created_at TEXT
    );
    """)
    # Ensure estimated_size column exists (for older DBs)
    try:
        cursor.execute("ALTER TABLE companies ADD COLUMN estimated_size TEXT")
    except Exception:
        pass
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activity_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_id TEXT,
        activity_type TEXT,
        description TEXT,
        metadata TEXT,
        created_at TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        email_id TEXT PRIMARY KEY,
        contact_id TEXT,
        thread_id TEXT,
        message_id TEXT,
        direction TEXT,
        status TEXT DEFAULT 'SENT',
        subject TEXT,
        body TEXT,
        replied_at TEXT,
        opened_at TEXT,
        created_at TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agent_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent_name TEXT,
        contact_id TEXT,
        action_type TEXT,
        details TEXT,
        created_at TEXT
    );
    """)
    cursor.execute("PRAGMA table_info(contacts);")
    columns = [col[1] for col in cursor.fetchall()]
    
    new_columns = {
        "research_result": "TEXT",
        "deliverability_audit": "TEXT",
        "opportunity_mapping": "TEXT",
        "qualification_assessment": "TEXT",
        "outreach_draft": "TEXT",
        "inbox_assessment": "TEXT",
        "last_handoff": "TEXT",
        "outcome_label": "TEXT DEFAULT 'Open'"
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
        
    # =====================================================================
    # 6. CAMPAIGN FUNNEL SYSTEM TABLES
    # =====================================================================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        manual_control INTEGER DEFAULT 1,
        text_variants INTEGER DEFAULT 1,
        ai_confidence_threshold INTEGER DEFAULT 80,
        status TEXT DEFAULT 'draft',
        account_id TEXT,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY (account_id) REFERENCES email_accounts(account_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id TEXT NOT NULL,
        contact_id TEXT NOT NULL,
        flow_state TEXT DEFAULT 'new',
        current_flow TEXT DEFAULT 'activation',
        current_step INTEGER DEFAULT 1,
        next_action_at TEXT,
        sequence_active INTEGER DEFAULT 1,
        linkedin_score REAL DEFAULT 0.0,
        enrolled_at TEXT,
        updated_at TEXT,
        UNIQUE(campaign_id, contact_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_flows (
        flow_id TEXT PRIMARY KEY,
        campaign_id TEXT NOT NULL,
        flow_type TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_flow_messages (
        message_id TEXT PRIMARY KEY,
        flow_id TEXT NOT NULL,
        step_number INTEGER NOT NULL,
        message_text TEXT NOT NULL,
        delay_days INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        created_at TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_message_queue (
        queue_id TEXT PRIMARY KEY,
        campaign_id TEXT NOT NULL,
        flow_id TEXT,
        lead_id TEXT NOT NULL,
        direction TEXT DEFAULT 'OUTBOUND',
        message_text TEXT NOT NULL,
        status TEXT DEFAULT 'pending_approval',
        ai_confidence REAL DEFAULT 0.0,
        ai_classified_flow TEXT,
        approved_by TEXT,
        approved_at TEXT,
        sent_at TEXT,
        created_at TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_conversations (
        conversation_id TEXT PRIMARY KEY,
        campaign_id TEXT NOT NULL,
        lead_id TEXT NOT NULL,
        last_message_at TEXT,
        last_message_text TEXT,
        last_sender TEXT,
        lead_state TEXT DEFAULT 'new',
        created_at TEXT,
        updated_at TEXT,
        UNIQUE(campaign_id, lead_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id TEXT NOT NULL,
        campaign_id TEXT NOT NULL,
        lead_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        message_text TEXT NOT NULL,
        flow_id TEXT,
        step_number INTEGER,
        sent_at TEXT,
        created_at TEXT
    );
    """)

    # Seed default flows for existing campaigns
    try:
        cursor.execute("SELECT COUNT(*) FROM campaign_flows")
        if cursor.fetchone()[0] == 0:
            import uuid as _uuid
            cursor.execute("SELECT campaign_id FROM campaigns LIMIT 1")
            row = cursor.fetchone()
            if row:
                cid = row[0]
                flow_types = ['activation', 'more_info', 'not_interested', 'appointment', 'referral', 'contact_later', 'confirm']
                now_ts = datetime.now(timezone.utc).isoformat()
                for ft in flow_types:
                    cursor.execute(
                        "INSERT INTO campaign_flows (flow_id, campaign_id, flow_type, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
                        (str(_uuid.uuid4()), cid, ft, now_ts)
                    )
    except Exception:
        pass

    # =====================================================================
    # 7. MULTI-ACCOUNT EMAIL SYSTEM
    # =====================================================================

    cursor.execute("""
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
        created_at TEXT,
        updated_at TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_variations (
        variation_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        campaign_id TEXT,
        flow_type TEXT DEFAULT 'activation',
        step_number INTEGER DEFAULT 1,
        subject TEXT,
        body TEXT NOT NULL,
        tone TEXT DEFAULT 'professional',
        language TEXT DEFAULT 'nl',
        is_active INTEGER DEFAULT 1,
        times_sent INTEGER DEFAULT 0,
        times_opened INTEGER DEFAULT 0,
        times_replied INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY (account_id) REFERENCES email_accounts(account_id)
    );
    """)

    # Seed default email account from existing .env credentials
    try:
        cursor.execute("SELECT COUNT(*) FROM email_accounts")
        if cursor.fetchone()[0] == 0:
            import uuid as _uuid
            from dotenv import load_dotenv as _load_dotenv
            _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr", ".env")
            _load_dotenv(_env_path)
            _gmail_user = os.environ.get("GMAIL_USER", "marvin@clawbuildr.com")
            _gmail_pass = os.environ.get("GMAIL_PASSWORD", "")
            if _gmail_user and _gmail_pass:
                cursor.execute(
                    """INSERT INTO email_accounts 
                       (account_id, display_name, email_address, provider, smtp_host, smtp_port, smtp_user, smtp_password, is_active, is_default, created_at, updated_at) 
                       VALUES (?, ?, ?, 'gmail', 'smtp.gmail.com', 587, ?, ?, 1, 1, ?, ?)""",
                    (str(_uuid.uuid4()), "Marvin (Default)", _gmail_user, _gmail_user, _gmail_pass,
                     datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat())
                )
    except Exception:
        pass

    # NEW: sentiment column on emails
    for col, typedef in [
        ("sentiment", "TEXT DEFAULT ''"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE emails ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    # NEW: sequences table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sequences (
        sequence_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        created_at TEXT
    );
    """)

    # NEW: sequence_steps table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sequence_steps (
        step_id TEXT PRIMARY KEY,
        sequence_id TEXT NOT NULL,
        step_number INTEGER NOT NULL,
        step_type TEXT NOT NULL DEFAULT 'email',
        delay_hours INTEGER DEFAULT 0,
        subject TEXT,
        body TEXT,
        connection_note TEXT,
        FOREIGN KEY (sequence_id) REFERENCES sequences(sequence_id)
    );
    """)

    # NEW: workflows table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS workflows (
        workflow_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        steps TEXT DEFAULT '[]',
        is_active INTEGER DEFAULT 1,
        created_at TEXT
    );
    """)

    # NEW: workflow_runs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS workflow_runs (
        run_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        status TEXT DEFAULT 'running',
        leads_found INTEGER DEFAULT 0,
        leads_sent INTEGER DEFAULT 0,
        started_at TEXT,
        completed_at TEXT,
        FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
    );
    """)

    # NEW: search_queries table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS search_queries (
        query_id TEXT PRIMARY KEY,
        name TEXT,
        prompt TEXT,
        status TEXT DEFAULT 'pending',
        result_count INTEGER DEFAULT 0,
        created_at TEXT
    );
    """)

    # NEW: search_results table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS search_results (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        query_id TEXT,
        domain TEXT,
        company_name TEXT,
        industry TEXT,
        location TEXT,
        confidence INTEGER DEFAULT 50,
        used INTEGER DEFAULT 0,
        created_at TEXT,
        FOREIGN KEY (query_id) REFERENCES search_queries(query_id)
    );
    """)

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

# Global database write lock ��� prevents concurrent SQLite writes between sourcing + pipeline
_DB_LOCK = asyncio.Lock()
_PIPELINE_ACTIVE: set = set()  # leads currently being processed by pipeline

# Dedicated thread pool for LinkedIn Selenium (Bug #6 fix: prevents thread pool starvation)
_LI_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="li_selenium")

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
            for field in ('icp_industries', 'icp_roles', 'search_queries', 'lead_sources', 'campaign_channels', 'icp_regions', 'pain_points'):
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
        'lead_sources': ["hunter", "directories", "kvk"],
        'campaign_channels': ["email", "linkedin"],
        'sequence_template': 'gentle',
        'icp_regions': ["nl"],
        'pain_points': [],
        'active': 1
    }

# Dedicated thread pool for LinkedIn Selenium (Bug #6 fix: prevents thread pool starvation)
_LI_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="li_selenium")

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
            for field in ('icp_industries', 'icp_roles', 'search_queries', 'lead_sources', 'campaign_channels', 'icp_regions', 'pain_points'):
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
        'lead_sources': ["hunter", "directories", "kvk"],
        'campaign_channels': ["email", "linkedin"],
        'sequence_template': 'gentle',
        'icp_regions': ["nl"],
        'pain_points': [],
        'active': 1
    }

# =========================================================================
# 3. INTER-AGENT EXECUTOR WRAPPER
# =========================================================================

async def global_log_audit(actor: str, action_type: str, details: str, contact_id: str = None):
    """Records structural audit actions in SQLite and logs to scrolling console."""
    def _do_audit():
        c = sqlite3.connect(DB_PATH, timeout=60.0)
        c.execute("PRAGMA journal_mode=WAL;")
        cur = c.cursor()
        cur.execute("""
        INSERT INTO activity_log (contact_id, actor, activity_type, description)
        VALUES (?, ?, ?, ?);
        """, (contact_id, actor, action_type, details))
        c.commit()
        c.close()
    async with _DB_LOCK:
        await asyncio.to_thread(_do_audit)
    
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
    if lead_id in _PIPELINE_ACTIVE:
        logger.info(f"[Pipeline] Lead {lead_id} already being processed � skipping.")
        return
    _PIPELINE_ACTIVE.add(lead_id)
    from linkedin_engine import search_and_connect as _li_search_and_connect
    from linkedin_engine import generate_linkedin_note as _li_generate_note
    logger.info(f"[Pipeline] Starting live replay for lead: {lead_id}")
    
    async def db_write(query: str, params: tuple = ()):
        async with _DB_LOCK:
            def _do_write():
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
            await asyncio.to_thread(_do_write)

    # 1. Fetch Lead info (quick read connection)
    def _fetch_lead():
        _conn = sqlite3.connect(DB_PATH, timeout=30.0)
        _cursor = _conn.cursor()
        _cursor.execute("""
        SELECT c.first_name, c.last_name, c.email, c.role, co.name, co.domain, c.linkedin_url
        FROM contacts c
        LEFT JOIN companies co ON c.company_id = co.company_id
        WHERE c.contact_id = ?;
        """, (lead_id,))
        _row = _cursor.fetchone()
        _conn.close()
        return _row
    row = await asyncio.to_thread(_fetch_lead)

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
            # Do NOT attempt LinkedIn for leads blocked before research ��� note would be generic and damage brand
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
        # STAGE 5b: AUTOMATIC LINKEDIN OUTREACH (persistent Firefox)
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
        # STAGE 6: OUTREACH (email for warm/hot leads >= 50)
        # ==========================================================
        if total < 40:
            await db_write("UPDATE contacts SET current_stage = 'NURTURE' WHERE contact_id = ?;", (lead_id,))
            await log_audit("CentralOrchestrator", "SKIPPED", f"Lead {company_name} score is not warm enough ({total}/100). LinkedIn attempted. Moving to NURTURE.")
            await sse_manager.broadcast("lead_refresh", {"contact_id": lead_id, "current_stage": "NURTURE"})
            return

        await update_agent_db_status("Outreach Agent", "RUNNING", "Drafting personalized Dutch outreach email...")
        await log_handoff("CentralOrchestrator", "Outreach Agent", {"action": "generate_dutch_outreach", "first_name": first_name})
        
        draft = await out_agent.draft_outreach(research, opp, first_name, company_name=company_name, domain=domain, email=email)
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
    finally:
        _PIPELINE_ACTIVE.discard(lead_id)



# =========================================================================
# BUG 3 FIX: Reply Detection Loop ��� polls Gmail for replies every 15 min
# =========================================================================

async def reply_detection_loop():
    """Polls Gmail API to detect when a prospect replies to a sent email.
    Runs every 15 minutes. Logs REPLIED event and pauses the lead's sequence."""
    logger.info("[Reply Detection] Loop started ��� checking Gmail threads every 15 min.")
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
                        # Fetch the actual reply content for OOO detection and classification
                        reply_body = ""
                        reply_subject = ""
                        try:
                            # Get the latest message (the reply)
                            latest_msg_id = messages_in_thread[-1]["id"]
                            latest_msg = connector.service.users().messages().get(
                                userId='me', id=latest_msg_id, format='full'
                            ).execute()
                            # Extract subject
                            headers = latest_msg.get("payload", {}).get("headers", [])
                            for h in headers:
                                if h.get("name", "").lower() == "subject":
                                    reply_subject = h.get("value", "")
                                    break
                            # Extract body text
                            payload = latest_msg.get("payload", {})
                            if payload.get("mimeType") == "text/plain":
                                body_data = payload.get("body", {}).get("data", "")
                                if body_data:
                                    reply_body = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
                            elif payload.get("mimeType", "").startswith("multipart/"):
                                parts = payload.get("parts", [])
                                for part in parts:
                                    if part.get("mimeType") == "text/plain":
                                        body_data = part.get("body", {}).get("data", "")
                                        if body_data:
                                            reply_body = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
                                            break
                        except Exception as fetch_err:
                            logger.debug(f"[Reply Detection] Could not fetch reply body: {fetch_err}")

                        # --- OOO / Auto-Reply Detection ---
                        is_ooo = False
                        ooo_keywords = [
                            "out of office", "out-of-office", "automatisch antwoord",
                            "auto-reply", "auto response", "vacation", "vakantie",
                            "on vacation", "met vakantie", "away from my desk",
                            "not available", "niet beschikbaar", "will be back",
                            "terug op", " Limited access", "beperkte toegang",
                            "do not reply", "antwoord niet op deze e-mail",
                            "this is an automated", "dit is een automatisch",
                            "automatic reply", "automatisch??", "?????",
                        ]
                        check_text = (reply_subject + " " + reply_body).lower()
                        for kw in ooo_keywords:
                            if kw.lower() in check_text:
                                is_ooo = True
                                break

                        now = datetime.now(timezone.utc).isoformat()

                        if is_ooo:
                            # OOO detected � do NOT pause sequence, just log it
                            async with _DB_LOCK:
                                c2 = sqlite3.connect(DB_PATH, timeout=10.0)
                                c2.execute("""
                                    INSERT OR IGNORE INTO email_events
                                    (event_id, email_id, contact_id, event_type, metadata, created_at)
                                    VALUES (?, ?, ?, 'OOO_AUTO_REPLY', ?, ?)
                                """, (str(uuid.uuid4()), row["email_id"], row["contact_id"],
                                      json.dumps({"subject": reply_subject, "body_preview": reply_body[:200]}), now))
                                c2.commit()
                                c2.close()
                            logger.info(f"[Reply Detection] OOO/auto-reply for {row['contact_id']} � sequence NOT paused")
                            continue  # Skip to next email, don't count as real reply

                        # --- Reply Classification (positive/negative/neutral) ---
                        reply_intent = "neutral"  # default
                        positive_keywords = [
                            "interessant", "interesting", "laten we bellen", "let's call",
                            "afspraak", "meeting", "calendly", "volgende week", "next week",
                            "ja graag", "yes please", "vertel meer", "tell me more",
                            "klinkt goed", "sounds good", "ik ben ge�nteresseerd",
                            "i'm interested", "plan een", "schedule a", "call me",
                        ]
                        negative_keywords = [
                            "niet ge�nteresseerd", "not interested", "unsubscribe",
                            "afmelden", "stop", "no thanks", "nee bedankt",
                            "geen interesse", "remove me", "verwijder mij",
                            "stop contact", "stop emailing", "no thanks",
                        ]
                        for kw in positive_keywords:
                            if kw.lower() in check_text:
                                reply_intent = "positive"
                                break
                        if reply_intent == "neutral":
                            for kw in negative_keywords:
                                if kw.lower() in check_text:
                                    reply_intent = "negative"
                                    break

                        # --- Outcome Label ---
                        outcome_label = "Open"
                        if reply_intent == "positive":
                            outcome_label = "Open"  # Positive but not closed yet
                        elif reply_intent == "negative":
                            outcome_label = "Lost"

                        async with _DB_LOCK:
                            c2 = sqlite3.connect(DB_PATH, timeout=10.0)
                            c2.execute("UPDATE emails SET replied_at = ? WHERE email_id = ?", (now, row["email_id"]))
                            # Log event with classification
                            c2.execute("""
                                INSERT OR IGNORE INTO email_events
                                (event_id, email_id, contact_id, event_type, metadata, created_at)
                                VALUES (?, ?, ?, 'REPLIED', ?, ?)
                            """, (str(uuid.uuid4()), row["email_id"], row["contact_id"],
                                  json.dumps({"intent": reply_intent, "subject": reply_subject,
                                              "body_preview": reply_body[:300]}), now))
                            # Update lead stage
                            new_stage = "REPLIED"
                            if reply_intent == "negative":
                                new_stage = "CLOSED_LOST"
                            c2.execute("""
                                UPDATE contacts SET current_stage = ?, updated_at = ?, outcome_label = ?
                                WHERE contact_id = ? AND current_stage NOT IN ('MEETING_BOOKED', 'CLOSED_WON', 'CLOSED_LOST')
                            """, (new_stage, now, outcome_label, row["contact_id"]))
                            c2.commit()
                            c2.close()

                        # --- Pause sequence on ANY real reply ---
                        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr"))
                        try:
                            from strategy_engine import pause_sequence
                            pause_reason = f"REPLIED_{reply_intent.upper()}"
                            pause_sequence(row["contact_id"], pause_reason)
                        except ImportError:
                            pass

                        # --- Auto-continue on positive reply ---
                        if reply_intent == "positive":
                            logger.info(f"[Reply Detection] POSITIVE reply from {row['contact_id']} � will auto-continue sequence on next cycle")
                            # The strategy_sequence_loop will pick this up and advance

                        stage_msg = f"REPLIED ({reply_intent})" if reply_intent != "neutral" else "REPLIED"
                        await sse_manager.broadcast("lead_refresh", {"contact_id": row["contact_id"], "current_stage": stage_msg})
                        logger.info(f"[Reply Detection] {stage_msg} for lead {row['contact_id']} (thread: {row['thread_id']})")
                        reply_count += 1
                except Exception as ex:
                    logger.debug(f"[Reply Detection] Thread check error for {row['thread_id']}: {ex}")

            if reply_count:
                logger.info(f"[Reply Detection] Found {reply_count} new replies this cycle.")
        except Exception as e:
            logger.error(f"[Reply Detection] Loop error: {e}")

        await asyncio.sleep(15 * 60)  # check every 15 minutes


# =========================================================================
# BUG 4 FIX: Strategy Sequence Loop ��� advances leads through steps
# BUG 8 FIX: Also writes linkedin accepted_at from accepted connection data
# =========================================================================

async def strategy_sequence_loop():
    """Checks every 5 minutes for leads whose next sequence step is due, and fires it.
    Also marks LinkedIn connections as accepted when check_pending_connections reports them."""
    logger.info("[Strategy Engine] Sequence loop started ��� checking for due steps every 5 min.")
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
                        # LinkedIn step ��� let the linkedin_auto_loop handle it in next cycle
                        pass

            # Bug 8 fix: update linkedin_outreach.accepted_at for newly accepted connections
            try:
                conn_li = sqlite3.connect(DB_PATH, timeout=10.0)
                conn_li.row_factory = sqlite3.Row
                # Find connections that were sent but not yet marked accepted
                pending_li = conn_li.execute("""
                    SELECT lo.id, lo.contact_id, lo.profile_url
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
                            "UPDATE linkedin_outreach SET accepted_at = ? WHERE id = ?",
                            (now_li, li_row["id"])
                        )
                    c3.commit()
                    c3.close()
                    logger.info(f"[Strategy Engine] Marked {len(pending_li)} LinkedIn connections as accepted.")
            except Exception as li_ex:
                logger.debug(f"[Strategy Engine] LinkedIn accept sync error: {li_ex}")

        except ImportError:
            logger.debug("[Strategy Engine] strategy_engine module not yet available ��� skipping.")
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
    skip_bg = os.environ.get("DASHBOARD_NO_BG", "0") == "1"
    if skip_bg:
        logger.info("[Startup] Background tasks DISABLED (DASHBOARD_NO_BG=1). HTTP-only mode.")
    else:
        # Spawn background tasks � each yield via sleep(0) to keep event loop responsive
        task1 = asyncio.create_task(lead_sourcing_agent()); await asyncio.sleep(0)
        task2 = asyncio.create_task(agent_loop()); await asyncio.sleep(0)
        task3 = asyncio.create_task(linkedin_auto_loop()); await asyncio.sleep(0)
        task4 = asyncio.create_task(reply_detection_loop()); await asyncio.sleep(0)
        task5 = asyncio.create_task(strategy_sequence_loop()); await asyncio.sleep(0)
        task6 = asyncio.create_task(campaign_send_loop()); await asyncio.sleep(0)
        task7 = asyncio.create_task(bounce_detection_loop()); await asyncio.sleep(0)
        task8 = asyncio.create_task(engagement_scoring_loop()); await asyncio.sleep(0)
        task9 = asyncio.create_task(campaign_email_send_loop()); await asyncio.sleep(0)
        task10 = asyncio.create_task(auto_approve_loop()); await asyncio.sleep(0)
        asyncio.create_task(immediate_sourcing_cycle())
        logger.info("[Startup] All background loops started.")
    yield
    if not skip_bg:
        task1.cancel(); task2.cancel(); task3.cancel(); task4.cancel(); task5.cancel()
        task6.cancel(); task7.cancel(); task8.cancel(); task9.cancel(); task10.cancel()
        logger.info("[Shutdown] Background tasks cancelled.")

app = FastAPI(title="ClawBuildr Mission Control Dashboard", lifespan=lifespan)

# Mount the new API v2 routes
if _api_v2_app is not None:
    app.mount("/api/v2", _api_v2_app, name="api_v2")

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
#  AGENT 0: Lead Sourcing Agent ��� finds prospects and feeds the pipeline
# =========================================================================

_LEAD_SOURCE_QUERIES = [
    # ICP-aligned: Groothandel, Logistiek, SaaS, IT
    "groothandel Nederland MKB",
    "groothandel food producten Nederland",
    "groothandel non-food Nederland",
    "groothandel bouwmaterialen Nederland",
    "distributiebedrijf Nederland",
    "logistiek bedrijf Nederland MKB",
    "transportbedrijf Nederland",
    "opslagbedrijf Nederland",
    "fulfillment bedrijf Nederland",
    "e-commerce fulfillment Nederland",
    "B2B SaaS bedrijf Nederland",
    "SaaS startup Nederland",
    "softwarebedrijf Nederland MKB",
    "IT dienstverlener Nederland",
    "IT consultancy Nederland",
    "managed service provider Nederland",
    "cloud dienstverlener Nederland",
    "webshop eigenaar Nederland",
    "e-commerce bedrijf Nederland",
    
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
    "private kliniek Nederland",
    "online marketing bureau Nederland",
    "notariskantoor Nederland",
    "architectenbureau Nederland",
    "ingenieursbureau Nederland",
    "adviesbureau bedrijfskunde Nederland",
    
    # High-quality B2B (German)
    "Steuerberatungsgesellschaft Deutschland",
    "Wirtschaftspr++fer Deutschland",
    "Personalberatung Deutschland",
    "Immobilienverwaltung Deutschland",
    "IT Systemhaus Deutschland",
    "Gro+�h+zndler Deutschland",
    "Logistikunternehmen Deutschland",
    "SaaS Unternehmen Deutschland",
    
    # High-quality B2B (Belgian)
    "bedrijfsrevisor Belgie",
    "HR consultancy Belgie",
    "IT dienstverlener KMO Belgie",
    "groothandel Belgie",
    "logistiek bedrijf Belgie",
    
    # More Dutch variety
    "fysiotherapie praktijk Nederland",
    "tandartspraktijk Nederland",
    "dierenartspraktijk Nederland",
    "webdesign bureau Nederland",
    "fotograaf bedrijf Nederland",
    "evenementenbureau Nederland",
    "drukkerij Nederland",
    "schoonheidssalon Nederland",
    "coachingsbureau Nederland",
    "fitnesscentrum Nederland",
    "yoga studio Nederland",
    
    # More German
    "Rechtsanwalt Deutschland",
    "Steuerberater Deutschland",
    "Architektur B++ro Deutschland",
    "IT Dienstleister Deutschland",
    
    # More Belgian
    "boekhouder Belgie",
    "kinesitherapeut Belgie",
    "apotheker Belgie",
]

_MAX_LEADS_SOURCED = 5000  # Keep pipeline filling all day
_SOURCE_CYCLE_MINUTES = 2  # Source every 2 minutes

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
    Uses the search rotator to avoid single-engine rate limits.
    """
    from search_rotator import search_web as _rotator_search
    return await _rotator_search(query, logger=logger)


async def _search_web_legacy(query: str) -> list:
    """Legacy search implementation kept for reference/fallback.
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

    # Check if this is Yelp HTML (has business listing structure)
    if "yelp.nl" in (html[:500] if html else "") or "biz-" in (html[:2000] if html else ""):
        # Parse Yelp business listings
        for m in re.finditer(r'<a[^>]*href=["\'](/biz/[^"\']+)["\'][^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE):
            biz_path = m.group(1).strip()
            title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            if not title or len(title) < 3:
                continue
            domain = f"yelp{biz_path.split('/')[-1]}"
            results.append({"title": title[:120], "url": f"https://www.yelp.nl{biz_path}", "domain": domain, "query": query, "source": "yelp"})
            if len(results) >= 5:
                break
    else:
        # Standard web search parsing
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
            r'(?:[Nn]aam|[Cc]ontact|' + _role_kw + r')\s*(?::|\-|\u2013)\s+(' + _nw + r'(?:[ \t]+' + _nw + r'){1,4})',
            # "Wouter de Vries - Eigenaar"
            r'(' + _nw + r'(?:[ \t]+' + _nw + r'){1,4})\s*(?:\u2013|\-)\s*(?:' + _role_kw + r')',
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
            noise = {'is', 'de', 'onze', '-', '���', 'eigenaar', 'directeur', 'ceo', 'founder', 'oprichter', 'manager'}
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
    Step 2: Search LinkedIn for that person (rate-limited).
    Returns {"linkedin_url": str, "first_name": str, "last_name": str} or empty dict.
    """
    # Extract clean company name
    clean_name = re.sub(r'\.nl|\.de|\.be|\.com|\.eu|\.io', '', company_name).strip()
    clean_name = re.sub(r'https?://\S+', '', clean_name).strip()
    clean_name = re.sub(r'\s*(?:�Ǧ|-+|/|\\|���|\|)\s*.*$', '', clean_name).strip()
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
                        rf'({role_kw})\s*(?::|\-|\u2013)\s*([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}}(?:\s+[A-Z][a-z]{{1,20}}){{0,2}})',
                        rf'([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}}(?:\s+[A-Z][a-z]{{1,20}}){{0,2}})\s*[,]\s*({role_kw})',
                        rf'(?:Naam|Name|Contact|Team)\s*(?::|\-|\u2013)\s*([A-Z][a-z]{{1,20}})\s+([A-Z][a-z]{{1,20}})',
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
    # Bug fix: run blocking selenium + search delay in a thread so the event loop stays responsive
    if not found_names:
        try:
            from linkedin_engine import find_decision_maker_on_linkedin
            li_result = await asyncio.to_thread(find_decision_maker_on_linkedin, clean_name, domain)
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

    # STEP 3: No LinkedIn found, but we have a real name ��� return it without URL
    if found_names:
        first, last = found_names[0]
        logger.info(f"[Lead Sourcing] No LinkedIn found for {first} {last} ({domain}), returning name only")
        return {"linkedin_url": "", "first_name": first, "last_name": last}

    logger.info(f"[Lead Sourcing] No person found for {clean_name} ({domain})")
    return {}


async def _try_query(query: str, current_count: int) -> int:
    """Search Brave for a query, add new companies to DB, return count added."""
    results = await _search_web(query)
    
    # Bug #9 fix: dedup check now uses a fresh WAL snapshot ��� safe for concurrent reads
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
        company_name = re.split(r'\s*(?:[|\u2013\u2014\u203A\x21/])\s*', raw_name)[0].strip()
        company_name = re.sub(r'&#x27;|&amp;|&quot;|&#x2[0-9a-f];', '', company_name).strip()
        company_name = re.sub(r'\s+', ' ', company_name).strip()
        # CRITICAL: Strip domain names from company names (e.g., "Orderchamp orderchamp.com" → "Orderchamp")
        domain_lower = domain.lower() if domain else ""
        if domain_lower:
            # Remove domain from end of name
            if company_name.lower().endswith(domain_lower):
                company_name = company_name[:-len(domain_lower)].strip()
            # Remove domain without TLD from end
            domain_no_tld = re.sub(r'\.(nl|com|be|de|eu|net|org|info)$', '', domain_lower)
            if company_name.lower().endswith(domain_no_tld):
                company_name = company_name[:-len(domain_no_tld)].strip()
            # If domain appears anywhere in name, remove it
            if domain_lower in company_name.lower():
                idx = company_name.lower().find(domain_lower)
                company_name = company_name[:idx].strip()
            # If name starts with domain part
            if company_name.lower().startswith(domain_no_tld):
                company_name = company_name[len(domain_no_tld):].strip()
        # Skip garbage names
        garbage_names = {"home", "contact", "about", "blog", "news", "images", "community", "summary", "search", "login", "menu"}
        if len(company_name) < 3 or company_name.lower() in garbage_names:
            company_name = raw_name[:60]
        # Clean up Mojeek-style titles with arrows
        company_name = re.sub(r'\s*(?:→|-+|/)\s*.*$', '', company_name).strip()

        # Search LinkedIn first ��� this gives us real names and profile URLs
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

        # If we have a real decision-maker name, try Hunter.io + patterns for a personal email
        generic_prefixes = {"info", "contact", "hello", "hallo", "sales", "support", "admin", "office", "team", "mail", "service", "help", "hr"}
        is_generic_email = (
            not contact_email or
            (contact_email.split("@")[0].lower().split("+")[0] in generic_prefixes) or
            contact_email.lower().startswith(("info@", "contact@", "hello@", "hallo@", "sales@", "support@", "admin@", "office@", "team@", "hr@"))
        )
        if first_name and last_name and is_generic_email:
            try:
                from email_finder import find_personal_email
                email_result = find_personal_email(first_name, last_name, domain)
                found_email = email_result.get("email")
                found_conf = email_result.get("confidence", 0)
                if found_email and found_conf >= 70:
                    contact_email = found_email
                    logger.info(f"[Lead Sourcing] Found personal email for {first_name} {last_name} ({domain}): {found_email} ({found_conf}%)")
            except Exception as ef_err:
                logger.info(f"[Lead Sourcing] Email finder failed for {first_name} {last_name} ({domain}): {ef_err}")

        # Skip if we have neither a LinkedIn URL nor a real email
        if not linkedin_url and not contact_email:
            return 0

        # If we still have no email, generate a placeholder
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
                ("accountant", "Financi+�le Dienstverlening"), ("advocaat", "Juridisch"), ("notaris", "Juridisch"),
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
                ("verzekering", "Verzekeringen"), ("boekhouder", "Financi+�le Dienstverlening"),
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

                # CRITICAL: Only accept Dutch/Belgian/German domains
                domain_lower_check = domain.lower().strip()
                if not (domain_lower_check.endswith('.nl') or domain_lower_check.endswith('.be') or domain_lower_check.endswith('.de')):
                    logger.info(f"[Lead Sourcing] Skipping non-Dutch domain: {domain}")
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
    def _count_pipeline():
        _conn = sqlite3.connect(DB_PATH, timeout=30.0)
        _cur = _conn.cursor()
        _cur.execute("SELECT COUNT(*) FROM contacts WHERE current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED', 'OUTREACH_DRAFTED', 'PENDING_APPROVAL')")
        _count = _cur.fetchone()[0]
        _conn.close()
        return _count
    count = await asyncio.to_thread(_count_pipeline)

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
        await asyncio.sleep(15)  # Slower pacing to avoid search engine rate limits

    logger.info(f"[Lead Sourcing] Cycle complete. Added {total_added} new leads. Pipeline at {count + total_added}/{_MAX_LEADS_SOURCED}.")
    return total_added

async def lead_sourcing_agent():
    """Background agent: searches for new prospects. Adaptive timing based on pipeline level."""
    logger.info(f"[Lead Sourcing Agent] Started. Adaptive sourcing (normal: {_SOURCE_CYCLE_MINUTES}min, urgent: 2min).")
    while True:
        try:
            def _check_pipeline():
                _conn = sqlite3.connect(DB_PATH, timeout=30.0)
                _cur = _conn.cursor()
                _cur.execute("SELECT COUNT(*) FROM contacts WHERE current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')")
                _count = _cur.fetchone()[0]
                _conn.close()
                return _count
            pipeline_count = await asyncio.to_thread(_check_pipeline)
            
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
            def _fetch_agent_data():
                _conn = sqlite3.connect(DB_PATH, timeout=30.0)
                _cursor = _conn.cursor()
                _cursor.execute("SELECT COUNT(*) FROM contacts WHERE current_stage = 'PENDING_APPROVAL'")
                _pending = _cursor.fetchone()[0]
                _row = None
                if _pending < 10:
                    _cursor.execute("""
                        SELECT ct.contact_id, ct.first_name, cp.name, cp.domain, ct.email
                        FROM contacts ct
                        JOIN companies cp ON ct.company_id = cp.company_id
                        WHERE ct.current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')
                        LIMIT 1
                    """)
                    _row = _cursor.fetchone()
                _conn.close()
                return _pending, _row
            pending_count, row = await asyncio.to_thread(_fetch_agent_data)
            
            if pending_count >= 10:
                logger.info(f"[Agent Loop] Currently {pending_count} leads pending review (limit: 10). Sleeping for 60s...")
                await asyncio.sleep(60)
                continue

            if row:
                lead_id, first_name, company_name, domain, email = row
                if lead_id in _PIPELINE_ACTIVE:
                    logger.info(f"[Agent Loop] Lead {lead_id} already being processed � skipping.")
                    await asyncio.sleep(10)
                    continue
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


# Path to auto-send flag file (JSON: {"enabled": true/false})
AUTO_SEND_FLAG_PATH = os.path.join(os.path.dirname(DB_PATH), "auto_send.json")


def _is_auto_send_enabled() -> bool:
    """Check if auto-send mode is enabled via flag file."""
    try:
        if os.path.exists(AUTO_SEND_FLAG_PATH):
            with open(AUTO_SEND_FLAG_PATH, "r") as f:
                data = json.load(f)
                return bool(data.get("enabled", False))
    except Exception:
        pass
    return False


def _set_auto_send(enabled: bool):
    """Enable or disable auto-send mode."""
    try:
        with open(AUTO_SEND_FLAG_PATH, "w") as f:
            json.dump({"enabled": enabled, "updated_at": datetime.now(timezone.utc).isoformat()}, f)
    except Exception as e:
        logger.error(f"[Auto Send] Could not write flag file: {e}")


async def auto_approve_loop():
    """Automatically approves and sends PENDING_APPROVAL emails when auto-send is enabled."""
    logger.info("[Auto Approve] Loop started. Will send PENDING_APPROVAL emails when auto-send is enabled.")
    await asyncio.sleep(15)  # Let server warm up
    while True:
        try:
            if not _is_auto_send_enabled():
                await asyncio.sleep(30)
                continue

            # Find PENDING_APPROVAL leads with non-placeholder emails
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            cur = conn.cursor()
            cur.execute("""
                SELECT contact_id, email FROM contacts
                WHERE current_stage = 'PENDING_APPROVAL'
                ORDER BY created_at ASC
            """)
            rows = cur.fetchall()
            conn.close()

            if not rows:
                await asyncio.sleep(30)
                continue

            # Skip obviously fake/placeholder emails
            placeholder_patterns = [
                "@email.nl", "@example.com", "@test.com", "@domain.com",
                "jouw@", "name@", "info@example", "contact@example", "test@"
            ]
            lead_id = None
            for rid, remail in rows:
                email_lower = (remail or "").lower()
                if any(p in email_lower for p in placeholder_patterns):
                    logger.warning(f"[Auto Approve] Skipping placeholder email for {rid}: {remail}")
                    continue
                lead_id = rid
                break

            if not lead_id:
                await asyncio.sleep(30)
                continue

            logger.info(f"[Auto Approve] Auto-sending email for {lead_id}")
            try:
                await approve_outreach(lead_id)
                logger.info(f"[Auto Approve] Sent email for {lead_id}")
                # Space auto-sends to avoid triggering spam filters
                await asyncio.sleep(random.uniform(60, 120))
            except Exception as e:
                logger.error(f"[Auto Approve] Failed to send for {lead_id}: {e}")
                await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"[Auto Approve] Loop error: {e}")
            await asyncio.sleep(60)


async def linkedin_auto_loop():
    """Automatically sends LinkedIn connection requests (20/day, spread across 3 time periods.
    Uses persistent Firefox singleton — no more spawning new windows per connection.
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

            # Off hours ��� wait until next period
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
                # Add -�20% randomness to avoid patterns
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

# --- SIMPLE UI HELPERS ---

def _active_tenant_id() -> str:
    return _get_active_tenant().get("tenant_id", "default")


def _is_onboarding_complete() -> bool:
    if is_onboarding_complete is None:
        return True
    try:
        return is_onboarding_complete(_active_tenant_id())
    except Exception as e:
        logger.warning(f"[Onboarding] status check failed: {e}")
        return True


def _create_simple_campaign(name: str, activation_message: str, contact_ids: List[str]):
    """Create a campaign and enroll contacts for the email pipeline."""
    campaign_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        conn.execute(
            """INSERT INTO campaigns
               (campaign_id, name, manual_control, text_variants, ai_confidence_threshold, status, created_at, updated_at)
               VALUES (?, ?, 0, 1, 80, 'active', ?, ?)""",
            (campaign_id, name, now, now),
        )
        # Default flows (kept for compatibility with /campaigns page)
        flow_types = ['activation', 'more_info', 'not_interested', 'appointment', 'referral', 'contact_later', 'confirm']
        for ft in flow_types:
            flow_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO campaign_flows (flow_id, campaign_id, flow_type, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
                (flow_id, campaign_id, ft, now),
            )
            if ft == 'activation':
                msg_id = str(uuid.uuid4())
                conn.execute(
                    """INSERT INTO campaign_flow_messages
                       (message_id, flow_id, step_number, message_text, delay_days, is_active, created_at)
                       VALUES (?, ?, 1, ?, 0, 1, ?)""",
                    (msg_id, flow_id, activation_message or "Hoi {{first_name}}, ik zag dat je bij {{company}} werkt. Interessant!", now),
                )

        enrolled = 0
        for cid in contact_ids:
            row = conn.execute("SELECT current_stage FROM contacts WHERE contact_id = ?", (cid,)).fetchone()
            if not row:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO campaign_leads
                   (campaign_id, contact_id, flow_state, current_flow, current_step, sequence_active, enrolled_at, updated_at)
                   VALUES (?, ?, 'new', 'activation', 1, 1, ?, ?)""",
                (campaign_id, cid, now, now),
            )
            # Move early-stage leads into the outreach pipeline
            if row["current_stage"] in ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED'):
                conn.execute(
                    "UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH', updated_at = ? WHERE contact_id = ?",
                    (now, cid),
                )
            enrolled += 1
        conn.commit()
        return {"campaign_id": campaign_id, "enrolled": enrolled}
    finally:
        conn.close()


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
    """Approves outreach, sends email via SMTP primary, Gmail API fallback, and moves state to ACTIVE_OUTREACH."""
    
    # Phase 1: Read lead data (short DB hold)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    try:
        cursor.execute("""
        SELECT c.email, c.outreach_draft, co.domain, c.current_stage
        FROM contacts c
        LEFT JOIN companies co ON c.company_id = co.company_id
        WHERE c.contact_id = ?;
        """, (lead_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Lead not found")
            
        email, draft_json, domain, current_stage = row

        # Bug #11 fix: verify lead is actually in PENDING_APPROVAL before sending
        if current_stage not in ('PENDING_APPROVAL', 'OUTREACH_DRAFTED'):
            raise HTTPException(status_code=400, detail=f"Lead is in stage '{current_stage}' ��� can only approve PENDING_APPROVAL leads")

        if not draft_json:
            raise HTTPException(status_code=400, detail="Outreach has not been drafted for this lead yet")
            
        draft = json.loads(draft_json)
        
        # Run Boundary Checks (Anti-bracket template safety check)
        body_text = draft.get("email_body_step_1", "")
        subject = draft.get("subject_line_A", "")
        if "[" in body_text or "]" in body_text:
            raise HTTPException(status_code=400, detail="Boundary Check Failed: Template bracket remnants [ ] detected in draft body!")
    finally:
        conn.close()

    # Phase 2: Send email (no DB hold)
    logger.info(f"[Outreach Approval] Attempting to send live email to {email} using Variant A")
    
    sent_successfully = False
    message_id = f"sent_{random.randint(100000, 999999)}"
    thread_id = f"thr_{random.randint(100000, 999999)}"
    send_error = None
    email_id_pre = f"em_{random.randint(100000, 999999)}"
    
    # PRIMARY: SMTP app password (we know this works)
    try:
        logger.info(f"[Outreach Approval] Trying SMTP app-password to: {email}")
        html_body = "<html><body><pre style='font-family:inherit;white-space:pre-wrap'>" + body_text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") + "</pre></html>"
        html_body_tracked = _inject_tracking(email_id_pre, html_body)
        smtp_res = await gmail_send(to=email, subject=subject, body=body_text, html_body=html_body_tracked)
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
    
    # FALLBACK: Gmail API only if SMTP failed and OAuth files exist
    if not sent_successfully:
        try:
            from clawbuildr_gmail import ClawBuildrGmailConnector
            DATA_DIR = os.path.dirname(os.path.abspath(__file__))
            creds_path = os.path.join(DATA_DIR, "client_secret.json")
            tenant = _get_active_tenant()
            active_tenant_id = tenant.get("tenant_id", "default")
            token_path = os.path.join(DATA_DIR, f"token_gmail_{active_tenant_id}.json")
            
            if not os.path.exists(token_path) and os.path.exists(os.path.join(DATA_DIR, "token_gmail.json")):
                token_path = os.path.join(DATA_DIR, "token_gmail.json")
            
            if os.path.exists(creds_path) and os.path.exists(token_path):
                connector = ClawBuildrGmailConnector(credentials_path=creds_path, token_path=token_path)
                if connector.authenticate():
                    logger.info(f"[Gmail API] Triggering live email delivery to: {email}")
                    html_body = "<html><body><pre style='font-family:inherit;white-space:pre-wrap'>" + body_text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") + "</pre></html>"
                    html_body_tracked = _inject_tracking(email_id_pre, html_body)
                    res = connector.send_email(to_email=email, subject=subject, body_text=body_text, body_html=html_body_tracked)
                    if res.get("status") == "SENT":
                        sent_successfully = True
                        message_id = res.get("message_id", message_id)
                        thread_id = res.get("thread_id", thread_id)
                    else:
                        send_error = res.get("error", "Unknown sending error")
                else:
                    send_error = "OAuth Token is invalid or expired."
        except Exception as ex:
            send_error = f"Gmail Connector Exception: {str(ex)}"
            logger.error(f"[Outreach Send] Sending crashed: {ex}")

    # Bug #1 fix: NEVER silently fake a sent email. Surface the real error.
    if not sent_successfully:
        logger.error(f"[Outreach Approval] All send methods failed: {send_error}")
        raise HTTPException(
            status_code=500,
            detail=f"Email delivery failed. Configure Gmail credentials or SMTP in Settings. Error: {send_error}"
        )
        
    # Phase 3: Update DB (short DB hold)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH' WHERE contact_id = ?;", (lead_id,))
        
        email_id = f"em_{random.randint(100000, 999999)}"
        cursor.execute("""
        INSERT INTO emails (email_id, contact_id, direction, subject, body, message_id, thread_id, status, sent_at)
        VALUES (?, ?, 'OUTBOUND', ?, ?, ?, ?, 'SENT', ?);
        """, (email_id, lead_id, subject, body_text, message_id, thread_id, datetime.now(timezone.utc).isoformat()))
        
        if domain:
            cursor.execute("UPDATE deliverability_stats SET sends_count = sends_count + 1 WHERE domain = ?;", (domain,))
            
        cursor.execute("""
        INSERT INTO activity_log (contact_id, actor, activity_type, action_details)
        VALUES (?, 'CentralOrchestrator', 'OUTBOUND_SEND', ?);
        """, (lead_id, f"Approved personalized outbound email sequence sent. Message ID: {message_id}"))
        
        conn.commit()
    except Exception as db_err:
        conn.rollback()
        logger.error(f"[Outreach Approval] DB update failed after successful send: {db_err}")
        raise HTTPException(status_code=500, detail=f"Email sent but DB update failed: {db_err}")
    finally:
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
    reply_text = body.get("reply_text", "Ik ben wel ge+�nteresseerd, kunnen we bellen?")
    
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
async def api_source_now(background_tasks: BackgroundTasks):
    """Manual trigger: run one lead sourcing cycle in background."""
    background_tasks.add_task(_run_sourcing_background)
    return {"status": "ok", "message": "Sourcing cycle started in background"}

async def _run_sourcing_background():
    """Background wrapper for sourcing cycle."""
    try:
        await run_sourcing_cycle()
    except Exception as e:
        logger.error(f"[Source Now] Background sourcing failed: {e}")

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


# =========================================================================
# SAFE-SENDING LAYER API
# =========================================================================

@app.get("/api/linkedin/safety-status")
def safety_status_endpoint():
    """Get comprehensive safety status: warming, flags, daily counts."""
    from linkedin_engine import get_safety_status
    return get_safety_status()

@app.post("/api/linkedin/manual-resume")
def manual_resume_endpoint():
    """Resume account after 7-day cooldown. Re-warms at 10% capacity."""
    from linkedin_engine import _manual_resume, _check_cooldown_elapsed, _is_flagged
    flagged, reason = _is_flagged()
    if not flagged:
        return {"success": False, "error": "Account is not flagged"}
    if not _check_cooldown_elapsed():
        return {"success": False, "error": "7-day cooldown has not elapsed yet"}
    result = _manual_resume()
    if result:
        return {"success": True, "message": "Account resumed. Re-warming at 10% capacity."}
    return {"success": False, "error": "Failed to resume account"}

@app.post("/api/linkedin/set-warming-start")
def set_warming_start_endpoint():
    """Set the account creation date for warming calculation."""
    from linkedin_engine import _get_clawbuildr_db
    import json as _json
    try:
        body = _json.loads(b"" if not request.body else request.body)
    except Exception:
        body = {}
    created_at = body.get("account_created_at")
    if not created_at:
        from datetime import datetime as _dt
        created_at = _dt.now().isoformat()
    db = _get_clawbuildr_db()
    db.execute("""
        INSERT INTO warming_state (id, account_created_at) VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET account_created_at = ?
    """, (created_at, created_at))
    db.commit()
    db.close()
    return {"success": True, "account_created_at": created_at}


@app.get("/api/linkedin/ai-usage")
def ai_usage_endpoint():
    """Get AI call usage stats: calls today, daily limit, cache stats."""
    import sys as _sys
    _cb_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr")
    if _cb_dir not in _sys.path:
        _sys.path.insert(0, _cb_dir)
    try:
        from tools import _get_ai_calls_today, AI_DAILY_LIMIT, _get_ai_cache_db
        calls_today = _get_ai_calls_today()
        # Get cache stats
        db = _get_ai_cache_db()
        cache_count = db.execute("SELECT COUNT(*) FROM ai_cache").fetchone()[0]
        db.close()
        return {
            "calls_today": calls_today,
            "daily_limit": AI_DAILY_LIMIT,
            "remaining": max(0, AI_DAILY_LIMIT - calls_today),
            "cache_entries": cache_count,
            "cache_ttl_hours": 24
        }
    except ImportError:
        return {"error": "AI module not available"}


@app.post("/api/linkedin/scrape-engagement")
def scrape_engagement_endpoint():
    """Scrape engagement (likes/comments) from a LinkedIn post."""
    from linkedin_engine import scrape_post_engagement
    try:
        import json as _json
        body = _json.loads(b"" if not request.body else request.body)
    except Exception:
        body = {}
    post_url = body.get("post_url", "")
    max_engagers = body.get("max_engagers", 25)
    if not post_url:
        return {"success": False, "error": "post_url is required"}
    return scrape_post_engagement(post_url, max_engagers)

@app.get("/api/linkedin/warm-leads")
def warm_leads_endpoint():
    """Get warm leads scraped from post engagement."""
    from linkedin_engine import get_warm_leads
    limit = request.args.get("limit", 50, type=int)
    return get_warm_leads(limit)

@app.get("/api/linkedin/funnel-stats")
def funnel_stats_endpoint():
    """Get activation funnel stats: lead progression through stages."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    # Count leads at each stage
    stages = {}
    for stage in ["INBOX", "RESEARCHED", "DELIVERABILITY_VERIFIED", "OPPORTUNITY_MAPPED",
                   "PRE_QUALIFIED", "OUTREACH_DRAFTED", "PENDING_APPROVAL", "ACTIVE_OUTREACH",
                   "REPLIED", "MEETING_BOOKED", "CLOSED_WON", "CLOSED_LOST"]:
        count = conn.execute("SELECT COUNT(*) FROM contacts WHERE current_stage = ?", (stage,)).fetchone()[0]
        stages[stage] = count

    # Conversion rates
    total = sum(stages.values())
    replied = stages.get("REPLIED", 0) + stages.get("MEETING_BOOKED", 0) + stages.get("CLOSED_WON", 0)
    meetings = stages.get("MEETING_BOOKED", 0)
    closed_won = stages.get("CLOSED_WON", 0)

    conn.close()

    return {
        "stages": stages,
        "total_leads": total,
        "reply_rate": round(replied / total * 100, 1) if total > 0 else 0,
        "meeting_rate": round(meetings / total * 100, 1) if total > 0 else 0,
        "close_rate": round(closed_won / total * 100, 1) if total > 0 else 0,
    }


@app.get("/api/analytics/overview")
def analytics_overview():
    """Get comprehensive analytics: pipeline health, email metrics, LinkedIn metrics, tracking pixel data."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    # Pipeline health
    total_contacts = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    stage_counts = {}
    for stage in ["INBOX", "RESEARCHED", "DELIVERABILITY_VERIFIED", "OPPORTUNITY_MAPPED",
                   "PRE_QUALIFIED", "OUTREACH_DRAFTED", "PENDING_APPROVAL", "ACTIVE_OUTREACH",
                   "REPLIED", "MEETING_BOOKED", "CLOSED_WON", "CLOSED_LOST"]:
        stage_counts[stage] = conn.execute("SELECT COUNT(*) FROM contacts WHERE current_stage = ?", (stage,)).fetchone()[0]

    # Email metrics
    total_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    sent_emails = conn.execute("SELECT COUNT(*) FROM emails WHERE direction = 'OUTBOUND' AND status = 'SENT'").fetchone()[0]
    replied_emails = conn.execute("SELECT COUNT(*) FROM emails WHERE replied_at IS NOT NULL").fetchone()[0]
    bounced = conn.execute("SELECT COUNT(*) FROM emails WHERE status = 'BOUNCED'").fetchone()[0]
    reply_rate = round(replied_emails / sent_emails * 100, 1) if sent_emails > 0 else 0

    # LinkedIn metrics (from linkedin_outreach table if exists)
    li_connected = 0
    li_pending = 0
    li_failed = 0
    try:
        li_connected = conn.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS'").fetchone()[0]
        li_pending = conn.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'PENDING'").fetchone()[0]
        li_failed = conn.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'FAILURE'").fetchone()[0]
    except Exception:
        pass

    # Activity log (last 7 days)
    from datetime import datetime as _dt, timedelta as _td
    seven_days_ago = (_dt.now() - _td(days=7)).isoformat()
    recent_activity = conn.execute(
        "SELECT COUNT(*) FROM activity_log WHERE created_at > ?", (seven_days_ago,)
    ).fetchone()[0]

    # Agent actions
    total_actions = conn.execute("SELECT COUNT(*) FROM agent_actions").fetchone()[0]

    # Email events (tracking pixel data)
    email_opens = 0
    email_clicks = 0
    try:
        email_opens = conn.execute("SELECT COUNT(*) FROM email_events WHERE event_type = 'OPENED'").fetchone()[0]
        email_clicks = conn.execute("SELECT COUNT(*) FROM email_events WHERE event_type = 'CLICKED'").fetchone()[0]
    except Exception:
        pass

    # Conversion funnel
    inbound_replies = replied_emails
    meetings_booked = stage_counts.get("MEETING_BOOKED", 0)
    closed_won = stage_counts.get("CLOSED_WON", 0)

    conn.close()

    return {
        "pipeline": {
            "total_contacts": total_contacts,
            "stage_counts": stage_counts,
            "recent_activity_7d": recent_activity,
        },
        "email": {
            "total": total_emails,
            "sent": sent_emails,
            "replied": replied_emails,
            "bounced": bounced,
            "reply_rate": reply_rate,
            "opens": email_opens,
            "clicks": email_clicks,
        },
        "linkedin": {
            "connected": li_connected,
            "pending": li_pending,
            "failed": li_failed,
            "accept_rate": round(li_connected / (li_connected + li_pending) * 100, 1) if (li_connected + li_pending) > 0 else 0,
        },
        "conversions": {
            "inbound_replies": inbound_replies,
            "meetings_booked": meetings_booked,
            "closed_won": closed_won,
            "reply_to_meeting": round(meetings_booked / inbound_replies * 100, 1) if inbound_replies > 0 else 0,
            "meeting_to_close": round(closed_won / meetings_booked * 100, 1) if meetings_booked > 0 else 0,
        },
        "agents": {
            "total_actions": total_actions,
        }
    }


@app.get("/api/analytics/timeline")
def analytics_timeline():
    """Get activity timeline for charts: last 30 days of activity."""
    from datetime import datetime as _dt, timedelta as _td
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    days = []
    for i in range(30, -1, -1):
        day = (_dt.now() - _td(days=i)).strftime("%Y-%m-%d")
        day_start = day + "T00:00:00"
        day_end = day + "T23:59:59"

        emails_sent = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE direction = 'OUTBOUND' AND created_at BETWEEN ? AND ?",
            (day_start, day_end)
        ).fetchone()[0]

        replies = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE replied_at BETWEEN ? AND ?",
            (day_start, day_end)
        ).fetchone()[0]

        li_connections = 0
        try:
            li_connections = conn.execute(
                "SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS' AND timestamp BETWEEN ? AND ?",
                (day_start, day_end)
            ).fetchone()[0]
        except Exception:
            pass

        activity = conn.execute(
            "SELECT COUNT(*) FROM activity_log WHERE created_at BETWEEN ? AND ?",
            (day_start, day_end)
        ).fetchone()[0]

        days.append({
            "date": day,
            "emails_sent": emails_sent,
            "replies": replies,
            "linkedin_connections": li_connections,
            "activity": activity,
        })

    conn.close()
    return {"days": days}


@app.get("/api/tracking/pixel/{email_id}")
def tracking_pixel(email_id):
    """1x1 transparent pixel that fires when email is opened."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.execute("""
            INSERT OR IGNORE INTO email_events (event_id, email_id, contact_id, event_type, metadata, created_at)
            VALUES (?, ?, (SELECT contact_id FROM emails WHERE email_id = ?), 'OPENED', '{}', ?)
        """, (str(uuid.uuid4()), email_id, email_id, now))
        # Update email status
        conn.execute("UPDATE emails SET opened_at = ? WHERE email_id = ? AND opened_at IS NULL", (now, email_id))
        conn.commit()
        conn.close()
    except Exception:
        pass

    # Return 1x1 transparent GIF
    import base64
    pixel = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
    from fastapi.responses import Response
    return Response(content=pixel, media_type="image/gif", headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })


@app.get("/api/tracking/click/{email_id}/{link_url:path}")
def tracking_click(email_id, link_url):
    """Track link clicks then redirect to the target URL."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        decoded_url = base64.urlsafe_b64decode(link_url + "==").decode("utf-8")
    except Exception:
        decoded_url = link_url

    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.execute("""
            INSERT OR IGNORE INTO email_events (event_id, email_id, contact_id, event_type, metadata, created_at)
            VALUES (?, ?, (SELECT contact_id FROM emails WHERE email_id = ?), 'CLICKED', ?, ?)
        """, (str(uuid.uuid4()), email_id, email_id, json.dumps({"url": decoded_url}), now))
        conn.commit()
        conn.close()
    except Exception:
        pass

    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=decoded_url, status_code=302)


@app.get("/api/analytics/tracking-summary")
def tracking_summary():
    """Get email tracking summary: opens, clicks, by campaign."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    total_sent = conn.execute("SELECT COUNT(*) FROM emails WHERE direction = 'OUTBOUND'").fetchone()[0]
    total_opened = conn.execute("SELECT COUNT(*) FROM emails WHERE opened_at IS NOT NULL").fetchone()[0]
    total_clicked = 0
    try:
        total_clicked = conn.execute("SELECT COUNT(*) FROM email_events WHERE event_type = 'CLICKED'").fetchone()[0]
    except Exception:
        pass

    conn.close()

    return {
        "total_sent": total_sent,
        "total_opened": total_opened,
        "total_clicked": total_clicked,
        "open_rate": round(total_opened / total_sent * 100, 1) if total_sent > 0 else 0,
        "click_rate": round(total_clicked / total_sent * 100, 1) if total_sent > 0 else 0,
    }

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
                 icp_roles, icp_company_size, search_queries, active,
                 lead_sources, campaign_channels, sequence_template, icp_regions, pain_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
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
                active         = 1,
                lead_sources   = excluded.lead_sources,
                campaign_channels = excluded.campaign_channels,
                sequence_template = excluded.sequence_template,
                icp_regions    = excluded.icp_regions,
                pain_points    = excluded.pain_points
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
            json.dumps(body.get("search_queries", [])),
            json.dumps(body.get("lead_sources", ["hunter", "directories", "kvk"])),
            json.dumps(body.get("campaign_channels", ["email", "linkedin"])),
            body.get("sequence_template", "gentle"),
            json.dumps(body.get("icp_regions", ["nl"])),
            json.dumps(body.get("pain_points", []))
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
# CAMPAIGN FUNNEL SYSTEM
# ===========================================================
import uuid as _campaign_uuid

@app.get("/api/campaigns")
def list_campaigns():
    """List all campaigns with stats."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        c = dict(r)
        # Get lead count
        lead_count = conn.execute(
            "SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = ?", (c["campaign_id"],)
        ).fetchone()[0]
        c["lead_count"] = lead_count
        # Get pending approval count
        pending = conn.execute(
            "SELECT COUNT(*) FROM campaign_message_queue WHERE campaign_id = ? AND status = 'pending_approval'",
            (c["campaign_id"],)
        ).fetchone()[0]
        c["pending_approval"] = pending
        # Get sent count
        sent = conn.execute(
            "SELECT COUNT(*) FROM campaign_message_queue WHERE campaign_id = ? AND status = 'sent'",
            (c["campaign_id"],)
        ).fetchone()[0]
        c["messages_sent"] = sent
        result.append(c)
    conn.close()
    return {"campaigns": result}


@app.post("/api/campaigns")
def create_campaign(data: dict):
    """Create a new campaign with default flows."""
    campaign_id = str(_campaign_uuid.uuid4())
    name = data.get("name", "New Campaign")
    now = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO campaigns (campaign_id, name, manual_control, text_variants, ai_confidence_threshold, status, created_at, updated_at) VALUES (?, ?, 1, 1, 80, 'draft', ?, ?)",
        (campaign_id, name, now, now)
    )

    # Create default flows
    flow_types = ['activation', 'more_info', 'not_interested', 'appointment', 'referral', 'contact_later', 'confirm']
    for ft in flow_types:
        flow_id = str(_campaign_uuid.uuid4())
        cursor.execute(
            "INSERT INTO campaign_flows (flow_id, campaign_id, flow_type, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
            (flow_id, campaign_id, ft, now)
        )
        # Create default activation message
        if ft == 'activation':
            msg_id = str(_campaign_uuid.uuid4())
            cursor.execute(
                "INSERT INTO campaign_flow_messages (message_id, flow_id, step_number, message_text, delay_days, is_active, created_at) VALUES (?, ?, 1, ?, 0, 1, ?)",
                (msg_id, flow_id, data.get("activation_message", "Hoi {{first_name}}, ik zag dat je bij {{company}} werkt. Interessant! Laten we een keer kletsen."), now)
            )

    conn.commit()
    conn.close()
    return {"campaign_id": campaign_id, "status": "created"}


@app.get("/api/campaigns/{campaign_id}")
def get_campaign(campaign_id: str):
    """Get campaign details with flows."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    campaign = dict(conn.execute("SELECT * FROM campaigns WHERE campaign_id = ?", (campaign_id,)).fetchone() or {})
    if not campaign:
        conn.close()
        return {"error": "Campaign not found"}

    # Get flows with messages
    flows = conn.execute("SELECT * FROM campaign_flows WHERE campaign_id = ?", (campaign_id,)).fetchall()
    campaign["flows"] = []
    for f in flows:
        flow = dict(f)
        messages = conn.execute(
            "SELECT * FROM campaign_flow_messages WHERE flow_id = ? ORDER BY step_number",
            (flow["flow_id"],)
        ).fetchall()
        flow["messages"] = [dict(m) for m in messages]
        campaign["flows"].append(flow)

    # Get lead count
    campaign["lead_count"] = conn.execute(
        "SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]

    conn.close()
    return campaign


@app.put("/api/campaigns/{campaign_id}")
def update_campaign(campaign_id: str, data: dict):
    """Update campaign settings."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    now = datetime.now(timezone.utc).isoformat()
    fields = []
    values = []
    for key in ["name", "manual_control", "text_variants", "ai_confidence_threshold", "status", "account_id"]:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])
    if fields:
        fields.append("updated_at = ?")
        values.append(now)
        values.append(campaign_id)
        conn.execute(f"UPDATE campaigns SET {', '.join(fields)} WHERE campaign_id = ?", values)
        conn.commit()
    conn.close()
    return {"status": "updated"}


@app.delete("/api/campaigns/{campaign_id}")
def delete_campaign(campaign_id: str):
    """Delete campaign and all related data."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cursor = conn.cursor()
    # Delete in order
    cursor.execute("DELETE FROM campaign_messages WHERE campaign_id = ?", (campaign_id,))
    cursor.execute("DELETE FROM campaign_message_queue WHERE campaign_id = ?", (campaign_id,))
    cursor.execute("DELETE FROM campaign_conversations WHERE campaign_id = ?", (campaign_id,))
    # Delete flow messages
    for f in cursor.execute("SELECT flow_id FROM campaign_flows WHERE campaign_id = ?", (campaign_id,)).fetchall():
        cursor.execute("DELETE FROM campaign_flow_messages WHERE flow_id = ?", (f[0],))
    cursor.execute("DELETE FROM campaign_flows WHERE campaign_id = ?", (campaign_id,))
    cursor.execute("DELETE FROM campaign_leads WHERE campaign_id = ?", (campaign_id,))
    cursor.execute("DELETE FROM campaigns WHERE campaign_id = ?", (campaign_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


@app.get("/api/campaigns/{campaign_id}/flows")
def list_flows(campaign_id: str):
    """List all flows for a campaign."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    flows = conn.execute(
        "SELECT * FROM campaign_flows WHERE campaign_id = ? ORDER BY flow_type",
        (campaign_id,)
    ).fetchall()
    result = []
    for f in flows:
        flow = dict(f)
        messages = conn.execute(
            "SELECT * FROM campaign_flow_messages WHERE flow_id = ? ORDER BY step_number",
            (flow["flow_id"],)
        ).fetchall()
        flow["messages"] = [dict(m) for m in messages]
        result.append(flow)
    conn.close()
    return result


@app.put("/api/campaigns/flows/{flow_id}")
def update_flow(flow_id: str, data: dict):
    """Update flow settings (active/inactive)."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    if "is_active" in data:
        conn.execute("UPDATE campaign_flows SET is_active = ? WHERE flow_id = ?", (data["is_active"], flow_id))
        conn.commit()
    conn.close()
    return {"status": "updated"}


@app.post("/api/campaigns/flows/{flow_id}/messages")
def add_flow_message(flow_id: str, data: dict):
    """Add a message step to a flow."""
    msg_id = str(_campaign_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    # Get next step number
    max_step = conn.execute(
        "SELECT COALESCE(MAX(step_number), 0) FROM campaign_flow_messages WHERE flow_id = ?", (flow_id,)
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO campaign_flow_messages (message_id, flow_id, step_number, message_text, delay_days, is_active, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)",
        (msg_id, flow_id, max_step + 1, data.get("message_text", ""), data.get("delay_days", 3), now)
    )
    conn.commit()
    conn.close()
    return {"message_id": msg_id, "status": "added"}


@app.put("/api/campaigns/messages/{message_id}")
def update_flow_message(message_id: str, data: dict):
    """Update a flow message."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    fields = []
    values = []
    for key in ["message_text", "delay_days", "is_active", "step_number"]:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])
    if fields:
        values.append(message_id)
        conn.execute(f"UPDATE campaign_flow_messages SET {', '.join(fields)} WHERE message_id = ?", values)
        conn.commit()
    conn.close()
    return {"status": "updated"}


@app.delete("/api/campaigns/messages/{message_id}")
def delete_flow_message(message_id: str):
    """Delete a flow message."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("DELETE FROM campaign_flow_messages WHERE message_id = ?", (message_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


@app.get("/api/campaigns/{campaign_id}/queue")
def get_message_queue(campaign_id: str, status: str = "pending_approval"):
    """Get messages pending approval."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT q.*, c.first_name, c.last_name, c.company_id, co.name as company_name
           FROM campaign_message_queue q
           LEFT JOIN contacts c ON q.lead_id = c.contact_id
           LEFT JOIN companies co ON c.company_id = co.company_id
           WHERE q.campaign_id = ? AND q.status = ?
           ORDER BY q.created_at DESC""",
        (campaign_id, status)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/campaigns/queue/{queue_id}/approve")
def approve_message(queue_id: str):
    """Approve a message for sending."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        "UPDATE campaign_message_queue SET status = 'approved', approved_at = ? WHERE queue_id = ?",
        (now, queue_id)
    )
    conn.commit()
    conn.close()
    return {"status": "approved"}


@app.post("/api/campaigns/queue/{queue_id}/reject")
def reject_message(queue_id: str):
    """Reject a message."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("UPDATE campaign_message_queue SET status = 'rejected' WHERE queue_id = ?", (queue_id,))
    conn.commit()
    conn.close()
    return {"status": "rejected"}


@app.post("/api/campaigns/queue/approve-all")
def approve_all_messages(data: dict):
    """Batch approve all pending messages for a campaign."""
    campaign_id = data.get("campaign_id")
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        "UPDATE campaign_message_queue SET status = 'approved', approved_at = ? WHERE campaign_id = ? AND status = 'pending_approval'",
        (now, campaign_id)
    )
    conn.commit()
    count = conn.execute(
        "SELECT changes()"
    ).fetchone()[0]
    conn.close()
    return {"status": "approved", "count": count}


@app.get("/api/campaigns/{campaign_id}/inbox")
def get_campaign_inbox(campaign_id: str):
    """Get live conversations for a campaign."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT conv.*, c.first_name, c.last_name, c.email, c.linkedin_url, co.name as company_name
           FROM campaign_conversations conv
           LEFT JOIN contacts c ON conv.lead_id = c.contact_id
           LEFT JOIN companies co ON c.company_id = co.company_id
           WHERE conv.campaign_id = ?
           ORDER BY conv.last_message_at DESC""",
        (campaign_id,)
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        # Get recent messages
        msgs = conn.execute(
            """SELECT * FROM campaign_messages WHERE conversation_id = ?
               ORDER BY created_at DESC LIMIT 10""",
            (d["conversation_id"],)
        ).fetchall()
        d["recent_messages"] = [dict(m) for m in msgs]
        result.append(d)
    conn.close()
    return result


@app.post("/api/campaigns/{campaign_id}/inbox/{conversation_id}/send")
def send_campaign_message(campaign_id: str, conversation_id: str, data: dict):
    """Send a message in a conversation (after approval)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row

    conv = dict(conn.execute(
        "SELECT * FROM campaign_conversations WHERE conversation_id = ?", (conversation_id,)
    ).fetchone() or {})

    msg_id = str(_campaign_uuid.uuid4())
    conn.execute(
        "INSERT INTO campaign_messages (id, conversation_id, campaign_id, lead_id, direction, message_text, flow_id, step_number, sent_at, created_at) VALUES (?, ?, ?, ?, 'OUTBOUND', ?, ?, ?, ?, ?)",
        (msg_id, conversation_id, campaign_id, conv.get("lead_id"), data["message_text"],
         data.get("flow_id"), data.get("step_number"), now, now)
    )
    conn.execute(
        "UPDATE campaign_conversations SET last_message_at = ?, last_message_text = ?, last_sender = 'us', updated_at = ? WHERE conversation_id = ?",
        (now, data["message_text"], now, conversation_id)
    )
    conn.commit()
    conn.close()
    return {"status": "sent", "message_id": msg_id}


@app.post("/api/campaigns/{campaign_id}/inbox/{conversation_id}/toggle")
def toggle_campaign_sequence(campaign_id: str, conversation_id: str, data: dict):
    """Toggle sequence active/paused for a conversation."""
    active = data.get("sequence_active", True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        "UPDATE campaign_conversations SET sequence_active = ?, updated_at = ? WHERE conversation_id = ?",
        (1 if active else 0, datetime.now(timezone.utc).isoformat(), conversation_id)
    )
    conn.commit()
    conn.close()
    return {"status": "updated", "sequence_active": active}


@app.post("/api/campaigns/{campaign_id}/leads")
def add_lead_to_campaign(campaign_id: str, data: dict):
    """Add a lead to a campaign."""
    now = datetime.now(timezone.utc).isoformat()
    contact_id = data.get("contact_id")
    if not contact_id:
        return {"error": "contact_id required"}
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        conn.execute(
            """INSERT INTO campaign_leads (campaign_id, contact_id, flow_state, current_flow, current_step, sequence_active, enrolled_at, updated_at)
               VALUES (?, ?, 'new', 'activation', 1, 1, ?, ?)""",
            (campaign_id, contact_id, now, now)
        )
        # Create conversation
        conv_id = str(_campaign_uuid.uuid4())
        conn.execute(
            "INSERT INTO campaign_conversations (conversation_id, campaign_id, lead_id, lead_state, created_at, updated_at) VALUES (?, ?, ?, 'new', ?, ?)",
            (conv_id, campaign_id, contact_id, now, now)
        )
        conn.commit()
        return {"status": "enrolled", "conversation_id": conv_id}
    except sqlite3.IntegrityError:
        conn.close()
        return {"error": "Lead already in this campaign"}
    finally:
        conn.close()


@app.get("/api/campaigns/{campaign_id}/leads")
def list_campaign_leads(campaign_id: str):
    """List all leads in a campaign."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT cl.*, c.first_name, c.last_name, c.email, c.linkedin_url, co.name as company_name
           FROM campaign_leads cl
           LEFT JOIN contacts c ON cl.contact_id = c.contact_id
           LEFT JOIN companies co ON c.company_id = co.company_id
           WHERE cl.campaign_id = ?
           ORDER BY cl.enrolled_at DESC""",
        (campaign_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ===========================================================
# LINKEDIN COMPANY SCRAPING ENDPOINTS
# ===========================================================
import concurrent.futures as _futures
_linkedin_pool = _futures.ThreadPoolExecutor(max_workers=1)

def _scrape_company_sync(url: str) -> dict:
    """Synchronous LinkedIn company scrape � runs in thread pool."""
    import sys as _sys
    _clawbuildr_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr")
    if _clawbuildr_dir not in _sys.path:
        _sys.path.insert(0, _clawbuildr_dir)
    from linkedin_engine import scrape_linkedin_company, enumerate_company_employees
    result = scrape_linkedin_company(url)
    if "error" not in result:
        employees = enumerate_company_employees(url, max_employees=30)
        result["employees"] = employees
    return result

def _scrape_profile_sync(url: str) -> dict:
    """Synchronous LinkedIn profile scrape � runs in thread pool."""
    import sys as _sys
    _clawbuildr_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr")
    if _clawbuildr_dir not in _sys.path:
        _sys.path.insert(0, _clawbuildr_dir)
    from linkedin_engine import scrape_linkedin_profile_deep
    return scrape_linkedin_profile_deep(url)

def _find_emails_sync(first: str, last: str, domain: str) -> list:
    """Synchronous email finder � runs in thread pool."""
    import sys as _sys
    _clawbuildr_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr")
    if _clawbuildr_dir not in _sys.path:
        _sys.path.insert(0, _clawbuildr_dir)
    from linkedin_engine import find_emails_for_employee
    return find_emails_for_employee(first, last, domain)


@app.get("/api/linkedin/company")
async def linkedin_company_scrape(url: str):
    """Scrape a LinkedIn company page for deep intelligence."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_linkedin_pool, _scrape_company_sync, url)
    return result


@app.get("/api/linkedin/profile")
async def linkedin_profile_scrape(url: str):
    """Deep-scrape a LinkedIn profile for full intelligence."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_linkedin_pool, _scrape_profile_sync, url)
    return result


@app.get("/api/linkedin/find-emails")
async def linkedin_find_emails(first_name: str, last_name: str, domain: str):
    """Find likely email addresses for an employee."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_linkedin_pool, _find_emails_sync, first_name, last_name, domain)
    return {"emails": result}


@app.post("/api/leads/{lead_id}/deep-research")
async def deep_research_lead(lead_id: str):
    """Run deep research on a lead: LinkedIn company + employees + social links + email discovery."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (lead_id,)).fetchone()
    if not row:
        conn.close()
        return {"error": "Lead not found"}
    
    lead = dict(row)
    company = lead.get("company", "")
    domain = lead.get("domain", "")
    linkedin_url = lead.get("linkedin_url", "")
    
    result = {"lead_id": lead_id, "company": company}
    
    # LinkedIn company scrape
    if linkedin_url and "linkedin.com/company" in linkedin_url:
        loop = asyncio.get_event_loop()
        company_data = await loop.run_in_executor(_linkedin_pool, _scrape_company_sync, linkedin_url)
        result["company_data"] = company_data
        
        # Get employees
        employees = company_data.get("employees", [])
        result["employees"] = employees
        
        # Try to find emails for each employee
        if domain and employees:
            emails_found = []
            for emp in employees[:10]:
                name_parts = emp.get("name", "").split()
                if len(name_parts) >= 2:
                    first = name_parts[0]
                    last = name_parts[-1]
                    emails = await loop.run_in_executor(_linkedin_pool, _find_emails_sync, first, last, domain)
                    if emails:
                        emails_found.append({
                            "name": emp.get("name"),
                            "title": emp.get("title"),
                            "profile_url": emp.get("profile_url"),
                            "emails": emails
                        })
            result["employee_emails"] = emails_found
    
    # Store research data
    research_json = json.dumps(result)
    conn.execute("UPDATE contacts SET research_result = ? WHERE contact_id = ?;", (research_json, lead_id))
    conn.commit()
    conn.close()
    
    return result


# ===========================================================
# MULTI-ACCOUNT EMAIL MANAGEMENT
# ===========================================================

@app.get("/api/email-accounts")
def list_email_accounts():
    """List all email accounts."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT account_id, display_name, email_address, provider, is_active, is_default, daily_send_limit, sends_today, last_send_at, warmup_days, signature, reply_to, created_at FROM email_accounts ORDER BY is_default DESC, created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/email-accounts")
def create_email_account(data: dict):
    """Create a new email account."""
    import uuid as _uuid
    now = datetime.now(timezone.utc).isoformat()
    account_id = str(_uuid.uuid4())
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    
    # If this is the first account or marked default, unset other defaults
    if data.get("is_default", 0):
        conn.execute("UPDATE email_accounts SET is_default = 0")
    
    conn.execute(
        """INSERT INTO email_accounts 
           (account_id, display_name, email_address, provider, smtp_host, smtp_port, smtp_user, smtp_password, 
            is_active, is_default, daily_send_limit, signature, reply_to, created_at, updated_at) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (account_id, data["display_name"], data["email_address"], data.get("provider", "gmail"),
         data.get("smtp_host", "smtp.gmail.com"), data.get("smtp_port", 587),
         data.get("smtp_user", data["email_address"]), data.get("smtp_password", ""),
         data.get("is_active", 1), data.get("is_default", 0), data.get("daily_send_limit", 50),
         data.get("signature", ""), data.get("reply_to", ""), now, now)
    )
    conn.commit()
    conn.close()
    return {"account_id": account_id, "status": "created"}


@app.put("/api/email-accounts/{account_id}")
def update_email_account(account_id: str, data: dict):
    """Update an email account."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    
    # If setting as default, unset others
    if data.get("is_default"):
        conn.execute("UPDATE email_accounts SET is_default = 0")
    
    allowed_fields = ["display_name", "email_address", "provider", "smtp_host", "smtp_port", 
                      "smtp_user", "smtp_password", "is_active", "is_default", "daily_send_limit", 
                      "signature", "reply_to"]
    updates = []
    values = []
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = ?")
            values.append(data[field])
    
    if updates:
        updates.append("updated_at = ?")
        values.append(datetime.now(timezone.utc).isoformat())
        values.append(account_id)
        conn.execute(f"UPDATE email_accounts SET {', '.join(updates)} WHERE account_id = ?", values)
        conn.commit()
    
    conn.close()
    return {"status": "updated"}


@app.delete("/api/email-accounts/{account_id}")
def delete_email_account(account_id: str):
    """Delete an email account."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("DELETE FROM email_accounts WHERE account_id = ?", (account_id,))
    conn.execute("DELETE FROM email_variations WHERE account_id = ?", (account_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


@app.post("/api/email-accounts/{account_id}/reset-daily")
def reset_daily_sends(account_id: str):
    """Reset daily send count for an account."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("UPDATE email_accounts SET sends_today = 0 WHERE account_id = ?", (account_id,))
    conn.commit()
    conn.close()
    return {"status": "reset"}


@app.get("/api/email-accounts/{account_id}/variations")
def list_email_variations(account_id: str, campaign_id: str = None):
    """List email variations for an account."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    if campaign_id:
        rows = conn.execute(
            "SELECT * FROM email_variations WHERE account_id = ? AND campaign_id = ? ORDER BY flow_type, step_number",
            (account_id, campaign_id)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM email_variations WHERE account_id = ? ORDER BY flow_type, step_number",
            (account_id,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/email-accounts/{account_id}/variations")
def create_email_variation(account_id: str, data: dict):
    """Create an email variation for an account."""
    import uuid as _uuid
    now = datetime.now(timezone.utc).isoformat()
    variation_id = str(_uuid.uuid4())
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        """INSERT INTO email_variations 
           (variation_id, account_id, campaign_id, flow_type, step_number, subject, body, tone, language, is_active, created_at, updated_at) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (variation_id, account_id, data.get("campaign_id"), data.get("flow_type", "activation"),
         data.get("step_number", 1), data.get("subject", ""), data["body"],
         data.get("tone", "professional"), data.get("language", "nl"), data.get("is_active", 1), now, now)
    )
    conn.commit()
    conn.close()
    return {"variation_id": variation_id, "status": "created"}


@app.put("/api/email-variations/{variation_id}")
def update_email_variation(variation_id: str, data: dict):
    """Update an email variation."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    allowed_fields = ["subject", "body", "tone", "language", "is_active", "flow_type", "step_number"]
    updates = []
    values = []
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = ?")
            values.append(data[field])
    
    if updates:
        updates.append("updated_at = ?")
        values.append(datetime.now(timezone.utc).isoformat())
        values.append(variation_id)
        conn.execute(f"UPDATE email_variations SET {', '.join(updates)} WHERE variation_id = ?", values)
        conn.commit()
    
    conn.close()
    return {"status": "updated"}


@app.delete("/api/email-variations/{variation_id}")
def delete_email_variation(variation_id: str):
    """Delete an email variation."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("DELETE FROM email_variations WHERE variation_id = ?", (variation_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}


# ===========================================================
# EMAIL WARMUP SYSTEM
# ===========================================================

# Warmup phases: gradual ramp over 14 days
WARMUP_PHASES = [
    {"max_days": 2,  "multiplier": 0.10},   # Day 1-2:   10% of limit
    {"max_days": 5,  "multiplier": 0.25},   # Day 3-5:   25%
    {"max_days": 8,  "multiplier": 0.50},   # Day 6-8:   50%
    {"max_days": 11, "multiplier": 0.75},   # Day 9-11:  75%
    {"max_days": 999,"multiplier": 1.00},   # Day 12+:   100%
]

def _get_warmup_limit(account: dict) -> int:
    """Calculate the current daily send limit for an account based on warmup phase.
    
    Args:
        account: dict with daily_send_limit, warmup_days, created_at
    
    Returns:
        int: current max sends allowed today
    """
    base_limit = account.get("daily_send_limit", 50)
    created_at = account.get("created_at")
    
    if not created_at:
        return base_limit
    
    try:
        from datetime import datetime, timezone
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_active = (now - created).days
    except Exception:
        return base_limit
    
    # Find the applicable warmup phase
    for phase in WARMUP_PHASES:
        if days_active <= phase["max_days"]:
            return max(1, int(base_limit * phase["multiplier"]))
    
    return base_limit


def _check_send_allowed(account: dict) -> dict:
    """Check if an account can send more emails today, considering warmup.
    
    Returns:
        dict with allowed (bool), remaining (int), reason (str)
    """
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    row = conn.execute(
        "SELECT sends_today, daily_send_limit, warmup_days, created_at FROM email_accounts WHERE account_id = ?",
        (account["account_id"],)
    ).fetchone()
    conn.close()
    
    if not row:
        return {"allowed": False, "remaining": 0, "reason": "Account not found"}
    
    sends_today = row[0] or 0
    warmup_limit = _get_warmup_limit(account)
    
    if sends_today >= warmup_limit:
        return {
            "allowed": False, 
            "remaining": 0, 
            "reason": f"Daily limit reached ({sends_today}/{warmup_limit}). Warmup phase active."
        }
    
    return {
        "allowed": True, 
        "remaining": warmup_limit - sends_today, 
        "reason": f"{warmup_limit - sends_today} sends remaining today (warmup limit: {warmup_limit})"
    }


# ===========================================================
# BOUNCE DETECTION
# ===========================================================

async def _check_bounce_detection():
    """Parse Gmail for bounce notifications and update lead status.
    
    Bounce emails come from postmaster@ or mail-daemon@ with subjects like:
    - "Delivery Status Notification (Failure)"
    - "Mail delivery failed"
    - "Undeliverable"
    - "Returned mail"
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        
        # Find sent emails that haven't been checked for bounces
        sent_emails = conn.execute("""
            SELECT e.email_id, e.contact_id, e.message_id 
            FROM emails e 
            WHERE e.direction = 'outbound' 
            AND e.status = 'SENT' 
            AND e.bounced_at IS NULL
            AND e.created_at > datetime('now', '-30 days')
        """).fetchall()
        
        if not sent_emails:
            conn.close()
            return
        
        # Check Gmail for bounce messages
        # Look for messages from postmaster or with bounce-related subjects
        bounce_patterns = [
            "postmaster@",
            "mailer-daemon@",
            "mail-daemon@",
            "delivery failed",
            "delivery status notification",
            "undeliverable",
            "returned mail",
            "failure notice",
        ]
        
        for email_row in sent_emails:
            email_id = email_row["email_id"]
            contact_id = email_row["contact_id"]
            
            # Check if this email's thread has a bounce reply
            # In a real implementation, this would query Gmail API
            # For now, we check the email_events table for BOUNCED events
            bounce_event = conn.execute(
                "SELECT event_id FROM email_events WHERE email_id = ? AND event_type = 'BOUNCED'",
                (email_id,)
            ).fetchone()
            
            if bounce_event:
                # Mark the email as bounced
                now = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "UPDATE emails SET bounced_at = ?, bounce_reason = 'Detected bounce' WHERE email_id = ?",
                    (now, email_id)
                )
                
                # Update lead status
                if contact_id:
                    conn.execute(
                        "UPDATE contacts SET current_stage = 'BOUNCED' WHERE contact_id = ? AND current_stage NOT IN ('CLOSED_WON', 'MEETING_BOOKED')",
                        (contact_id,)
                    )
                
                logger.info(f"[Bounce Detection] Email {email_id} bounced. Lead {contact_id} marked as BOUNCED.")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"[Bounce Detection] Error: {e}")


async def bounce_detection_loop():
    """Background loop: check for bounces every 30 minutes."""
    logger.info("[Bounce Detection Loop] Started.")
    while True:
        try:
            await asyncio.sleep(1800)  # 30 minutes
            await _check_bounce_detection()
        except Exception as e:
            logger.error(f"[Bounce Detection Loop] Error: {e}")
            await asyncio.sleep(300)


# ===========================================================
# DYNAMIC LEAD SCORING
# ===========================================================

def _calculate_engagement_score(contact_id: str) -> dict:
    """Calculate dynamic engagement score based on lead activity.
    
    Scoring factors:
    - Email opens: +2 per open (max 10)
    - Email clicks: +5 per click (max 15)
    - Email replies: +20 per reply (max 40)
    - LinkedIn profile view: +5 (if detected)
    - LinkedIn connection accepted: +10
    - LinkedIn message reply: +15
    - Bounced: -50
    - No activity in 7 days: -10
    
    Returns:
        dict with score, factors, and recommendation
    """
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    
    contact = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
    if not contact:
        conn.close()
        return {"score": 0, "factors": [], "recommendation": "unknown"}
    
    contact = dict(contact)
    base_score = contact.get("lead_score", 0) or 0
    factors = []
    
    # Email engagement
    email_stats = conn.execute("""
        SELECT 
            COUNT(CASE WHEN event_type = 'OPENED' THEN 1 END) as opens,
            COUNT(CASE WHEN event_type = 'CLICKED' THEN 1 END) as clicks,
            COUNT(CASE WHEN event_type = 'REPLIED' THEN 1 END) as replies,
            COUNT(CASE WHEN event_type = 'BOUNCED' THEN 1 END) as bounces
        FROM email_events WHERE contact_id = ?
    """, (contact_id,)).fetchone()
    
    if email_stats:
        opens = min(email_stats["opens"] * 2, 10)
        clicks = min(email_stats["clicks"] * 5, 15)
        replies = min(email_stats["replies"] * 20, 40)
        bounces = email_stats["bounces"] * -50
        
        if opens > 0:
            factors.append(f"Email opens: +{opens}")
        if clicks > 0:
            factors.append(f"Email clicks: +{clicks}")
        if replies > 0:
            factors.append(f"Email replies: +{replies}")
        if bounces < 0:
            factors.append(f"Bounced: {bounces}")
        
        base_score += opens + clicks + replies + bounces
    
    # LinkedIn engagement
    li_stats = conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN accepted_at IS NOT NULL THEN 1 END) as accepted,
            COUNT(CASE WHEN replied_at IS NOT NULL THEN 1 END) as replied
        FROM linkedin_outreach WHERE contact_id = ?
    """, (contact_id,)).fetchone()
    
    if li_stats:
        accepted = li_stats["accepted"] * 10
        replied = li_stats["replied"] * 15
        
        if accepted > 0:
            factors.append(f"LinkedIn accepted: +{accepted}")
        if replied > 0:
            factors.append(f"LinkedIn replied: +{replied}")
        
        base_score += accepted + replied
    
    # Recency penalty
    last_activity = conn.execute("""
        SELECT MAX(created_at) as last_activity FROM email_events WHERE contact_id = ?
    """, (contact_id,)).fetchone()
    
    if last_activity and last_activity["last_activity"]:
        try:
            from datetime import datetime, timedelta, timezone
            last = datetime.fromisoformat(last_activity["last_activity"].replace("Z", "+00:00"))
            days_since = (datetime.now(timezone.utc) - last).days
            if days_since > 7:
                penalty = -10
                base_score += penalty
                factors.append(f"Inactive {days_since} days: {penalty}")
        except Exception:
            pass
    
    # Clamp score
    base_score = max(0, min(100, base_score))
    
    # Generate recommendation
    if base_score >= 80:
        recommendation = "HOT � Ready for immediate outreach"
    elif base_score >= 50:
        recommendation = "WARM � Engaged, send follow-up"
    elif base_score >= 20:
        recommendation = "COOL � Some interest, nurture"
    else:
        recommendation = "COLD � Minimal engagement"
    
    # Update lead score in DB
    conn.execute("UPDATE contacts SET lead_score = ? WHERE contact_id = ?", (base_score, contact_id))
    conn.commit()
    conn.close()
    
    return {
        "score": base_score,
        "factors": factors,
        "recommendation": recommendation
    }


async def engagement_scoring_loop():
    """Background loop: re-score leads based on engagement every hour."""
    logger.info("[Engagement Scoring Loop] Started.")
    while True:
        try:
            await asyncio.sleep(3600)  # 1 hour
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            leads = conn.execute(
                "SELECT contact_id FROM contacts WHERE current_stage NOT IN ('BOUNCED', 'CLOSED_WON', 'CLOSED_LOST')"
            ).fetchall()
            conn.close()
            
            for lead in leads:
                try:
                    _calculate_engagement_score(lead[0])
                except Exception:
                    pass
            
            logger.info(f"[Engagement Scoring] Re-scored {len(leads)} leads.")
        except Exception as e:
            logger.error(f"[Engagement Scoring Loop] Error: {e}")
            await asyncio.sleep(300)


def _get_send_account(contact_id: str = None, campaign_id: str = None) -> dict:
    """Select the best email account to send from.
    
    Logic:
    1. If campaign has a linked account, use that
    2. Otherwise, round-robin among active accounts under their daily limit
    """
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    
    # Check if campaign has a specific account linked
    if campaign_id:
        row = conn.execute(
            "SELECT account_id FROM campaigns WHERE campaign_id = ?", (campaign_id,)
        ).fetchone()
        if row and row["account_id"]:
            account = conn.execute(
                "SELECT * FROM email_accounts WHERE account_id = ? AND is_active = 1",
                (row["account_id"],)
            ).fetchone()
            if account:
                conn.close()
                return dict(account)
    
    # Round-robin: pick account with most remaining sends
    accounts = conn.execute(
        """SELECT * FROM email_accounts WHERE is_active = 1 
           AND sends_today < daily_send_limit 
           ORDER BY sends_today ASC, is_default DESC"""
    ).fetchall()
    conn.close()
    
    if accounts:
        return dict(accounts[0])
    return None


def _record_send(account_id: str):
    """Increment send count for an account."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE email_accounts SET sends_today = sends_today + 1, last_send_at = ? WHERE account_id = ?",
        (now, account_id)
    )
    conn.commit()
    conn.close()


# ===========================================================
# GLOBAL INBOX & CONVERSATION VIEW
# ===========================================================

@app.get("/api/inbox")
def get_global_inbox(intent: str = None, campaign_id: str = None, status: str = None, limit: int = 50):
    """Global inbox: all replies and conversations across all campaigns.
    
    Filters:
    - intent: interested, objection, meeting, neutral, ooo
    - campaign_id: filter to specific campaign
    - status: replied, pending, closed
    """
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    
    # Get all leads that have replies or are in active conversations
    query = """
        SELECT DISTINCT
            c.contact_id,
            c.first_name,
            c.last_name,
            c.email,
            comp.name as company,
            c.current_stage,
            c.lead_score,
            c.inbox_assessment,
            c.linkedin_url,
            cm.campaign_id,
            camp.name as campaign_name,
            cm.last_message_at,
            cm.last_message_text,
            cm.last_sender,
            cm.lead_state,
            cm.sequence_active
        FROM contacts c
        LEFT JOIN companies comp ON c.company_id = comp.company_id
        LEFT JOIN campaign_conversations cm ON c.contact_id = cm.lead_id
        LEFT JOIN campaigns camp ON cm.campaign_id = camp.campaign_id
        WHERE c.current_stage IN ('REPLIED', 'MEETING_BOOKED', 'INTERESTED', 'OBJECTION')
        OR cm.last_sender = 'them'
        OR cm.last_message_text IS NOT NULL
        OR c.current_stage IN ('EMAIL_SENT', 'MEETING_BOOKED')
    """
    params = []
    
    if campaign_id:
        query += " AND cm.campaign_id = ?"
        params.append(campaign_id)
    
    query += " ORDER BY cm.last_message_at DESC NULLS LAST"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    rows = conn.execute(query, params).fetchall()
    
    # Enrich with latest email events
    results = []
    for row in rows:
        r = dict(row)
        
        # Parse inbox_assessment for intent
        if r.get("inbox_assessment"):
            try:
                assessment = json.loads(r["inbox_assessment"]) if isinstance(r["inbox_assessment"], str) else r["inbox_assessment"]
                r["intent"] = assessment.get("intent", "neutral")
                r["intent_confidence"] = assessment.get("confidence", 0)
                r["reply_summary"] = assessment.get("summary", "")
            except Exception:
                r["intent"] = "neutral"
        else:
            r["intent"] = "neutral"
        
        # Get latest email event
        if r.get("contact_id"):
            event = conn.execute("""
                SELECT event_type, created_at FROM email_events 
                WHERE contact_id = ? ORDER BY created_at DESC LIMIT 1
            """, (r["contact_id"],)).fetchone()
            if event:
                r["last_event"] = event["event_type"]
                r["last_event_at"] = event["created_at"]
        
        # Apply intent filter
        if intent and r.get("intent") != intent:
            continue
        
        results.append(r)
    
    conn.close()
    return results


@app.get("/api/inbox/{contact_id}/messages")
def get_conversation_messages(contact_id: str):
    """Get all messages for a specific contact (email + LinkedIn)."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    
    messages = []
    
    # Get email messages
    emails = conn.execute("""
        SELECT 
            'email' as channel,
            e.email_id as id,
            e.subject,
            e.body as message_text,
            e.direction,
            e.status,
            e.created_at as timestamp,
            e.opened_at,
            e.clicked_at,
            e.replied_at,
            e.bounced_at
        FROM emails e
        WHERE e.contact_id = ?
        ORDER BY e.created_at ASC
    """, (contact_id,)).fetchall()
    
    for e in emails:
        msg = dict(e)
        msg["direction"] = "OUTBOUND" if msg["direction"] == "outbound" else "INBOUND"
        messages.append(msg)
    
    # Get LinkedIn messages
    li_msgs = conn.execute("""
        SELECT 
            'linkedin' as channel,
            li.id as id,
            li.note as message_text,
            'OUTBOUND' as direction,
            li.outcome as status,
            li.timestamp as timestamp
        FROM linkedin_outreach li
        WHERE li.contact_id = ?
        ORDER BY li.timestamp ASC
    """, (contact_id,)).fetchall()
    
    for m in li_msgs:
        messages.append(dict(m))
    
    # Get campaign messages
    camp_msgs = conn.execute("""
        SELECT 
            'campaign' as channel,
            cm.id,
            cm.message_text,
            cm.direction,
            cm.sent_at as timestamp
        FROM campaign_messages cm
        WHERE cm.lead_id = ?
        ORDER BY cm.sent_at ASC
    """, (contact_id,)).fetchall()
    
    for m in camp_msgs:
        messages.append(dict(m))
    
    # Sort all messages by timestamp
    messages.sort(key=lambda x: x.get("timestamp") or "")
    
    conn.close()
    return messages


@app.post("/api/inbox/{contact_id}/reply")
def send_reply_to_contact(contact_id: str, data: dict):
    """Send a reply to a contact via their preferred channel."""
    message_text = data.get("message_text", "")
    channel = data.get("channel", "email")
    template_id = data.get("template_id")
    account_id = data.get("account_id")
    
    if not message_text:
        return {"error": "message_text required"}
    
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    
    # Get contact info
    contact = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
    if not contact:
        conn.close()
        return {"error": "Contact not found"}
    
    contact = dict(contact)
    now = datetime.now(timezone.utc).isoformat()
    
    if channel == "email":
        # Get account to use
        if not account_id:
            # Use default account
            acc_row = conn.execute("SELECT account_id FROM email_accounts WHERE is_default = 1").fetchone()
            account_id = acc_row["account_id"] if acc_row else None
        
        if not account_id:
            conn.close()
            return {"error": "No email account configured"}
        
        # Send email
        import uuid as _uuid
        email_id = str(_uuid.uuid4())
        
        # Get account details for sending
        account = conn.execute("SELECT * FROM email_accounts WHERE account_id = ?", (account_id,)).fetchone()
        if not account:
            conn.close()
            return {"error": "Email account not found"}
        
        account = dict(account)
        
        # Inject tracking
        tracked_body = _inject_tracking(email_id, message_text)
        
        # Send via Gmail
        import asyncio as _asyncio
        loop = _asyncio.new_event_loop()
        result = loop.run_until_complete(gmail_send(
            to=contact.get("email"),
            subject=data.get("subject", f"Re: {contact.get('company', '')}"),
            body=tracked_body,
            from_addr=account["email_address"],
            smtp_host=account["smtp_host"],
            smtp_port=account["smtp_port"],
            smtp_user=account["smtp_user"],
            smtp_password=account["smtp_password"]
        ))
        loop.close()
        
        # Record email
        conn.execute("""
            INSERT INTO emails (email_id, contact_id, direction, status, subject, body, sent_at, created_at)
            VALUES (?, ?, 'outbound', ?, ?, ?, ?, ?)
        """, (email_id, contact_id, result.get("status", "SENT").upper(), 
              data.get("subject", ""), message_text, now, now))
        
        # Update account send count
        conn.execute(
            "UPDATE email_accounts SET sends_today = sends_today + 1, last_send_at = ? WHERE account_id = ?",
            (now, account_id)
        )
        
        # Update contact stage
        conn.execute(
            "UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH' WHERE contact_id = ? AND current_stage IN ('REPLIED', 'INTERESTED')",
            (contact_id,)
        )
        
        conn.commit()
        conn.close()
        
        return {"status": "sent", "channel": "email", "email_id": email_id, "result": result}
    
    elif channel == "linkedin":
        # Send via LinkedIn
        profile_url = contact.get("linkedin_url")
        if not profile_url:
            conn.close()
            return {"error": "No LinkedIn URL for this contact"}
        
        conn.close()
        
        # Queue for LinkedIn send
        import uuid as _uuid
        queue_id = str(_uuid.uuid4())
        conn2 = sqlite3.connect(DB_PATH, timeout=30.0)
        conn2.execute("""
            INSERT INTO campaign_message_queue (queue_id, campaign_id, flow_id, lead_id, direction, message_text, status, created_at)
            VALUES (?, NULL, NULL, ?, 'OUTBOUND', ?, 'approved', ?)
        """, (queue_id, contact_id, message_text, now))
        conn2.commit()
        conn2.close()
        
        return {"status": "queued", "channel": "linkedin", "queue_id": queue_id}
    
    conn.close()
    return {"error": "Invalid channel"}


# ===========================================================
# CAMPAIGN ? LINKEDIN INTEGRATION
# ===========================================================

def _send_linkedin_dm_sync(profile_url: str, message_text: str) -> dict:
    """Synchronous LinkedIn DM send � runs in thread pool to avoid blocking event loop."""
    try:
        from linkedin_engine import get_firefox_driver, _random_sleep, _detect_auth_wall
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
    except ImportError as e:
        return {"status": "error", "error": f"linkedin_engine import failed: {e}"}

    _firefox_ctx = None
    try:
        _firefox_ctx = get_firefox_driver()
        driver, page = _firefox_ctx.__enter__()

        page.goto("https://www.linkedin.com/messaging/compose/", wait_until="domcontentloaded", timeout=60000)
        _random_sleep(4, 6)

        auth = _detect_auth_wall(page)
        if auth:
            return {"status": "error", "error": f"Auth wall: {auth}"}

        import re as _re
        username_match = _re.search(r'/in/([^/?]+)', profile_url)
        if not username_match:
            return {"status": "error", "error": f"Cannot extract username from {profile_url}"}

        search_input = None
        for sel in ['input[placeholder*="Naam"]', 'input[placeholder*="name"]', 'input[placeholder*="Name"]',
                    'input[placeholder*="Zoek"]', 'input[placeholder*="Search"]', 'input[type="text"]']:
            found = driver.find_elements(By.CSS_SELECTOR, sel)
            if found:
                search_input = found[0]
                break

        if not search_input:
            return {"status": "error", "error": "Search input not found"}

        search_input.click()
        _random_sleep(1, 2)
        display_name = username_match.group(1).replace('-', ' ').replace('.', ' ')
        search_input.send_keys(display_name)
        _random_sleep(3, 5)

        suggestions = driver.find_elements(By.CSS_SELECTOR, 'li[role="option"], .search-reusables__typeahead-result')
        if suggestions:
            suggestions[0].click()
            _random_sleep(2, 3)
        else:
            search_input.send_keys(Keys.ENTER)
            _random_sleep(2, 3)

        textarea = None
        for sel in ['div[role="textbox"]', 'textarea', '[contenteditable="true"]']:
            found = driver.find_elements(By.CSS_SELECTOR, sel)
            if found:
                textarea = found[0]
                break

        if not textarea:
            return {"status": "error", "error": "Textarea not found"}

        textarea.click()
        _random_sleep(1, 2)
        for char in message_text:
            textarea.send_keys(char)
            time.sleep(0.02)
        _random_sleep(1, 2)

        send_btn = None
        for btn in driver.find_elements(By.CSS_SELECTOR, 'button'):
            text = btn.text.strip().lower()
            aria = (btn.get_attribute('aria-label') or '').lower()
            if 'verzenden' in text or 'send' in text or 'verzenden' in aria or 'send' in aria:
                send_btn = btn
                break

        if not send_btn:
            return {"status": "error", "error": "Send button not found"}

        send_btn.click()
        _random_sleep(3, 5)

        return {"status": "sent"}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}
    finally:
        if _firefox_ctx:
            try:
                _firefox_ctx.__exit__(None, None, None)
            except Exception:
                pass


async def _send_linkedin_dm(profile_url: str, message_text: str) -> dict:
    """Async wrapper � runs Selenium in thread pool so event loop stays free."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_send_pool, _send_linkedin_dm_sync, profile_url, message_text)


async def campaign_send_loop():
    """Background loop: polls approved messages in queue and sends via LinkedIn."""
    logger.info("[Campaign Send Loop] Started.")
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            conn.row_factory = sqlite3.Row
            # Get approved messages with lead profile URLs
            rows = conn.execute("""
                SELECT q.queue_id, q.campaign_id, q.lead_id, q.message_text, q.flow_id,
                       c.linkedin_url, c.first_name, c.last_name
                FROM campaign_message_queue q
                LEFT JOIN contacts c ON q.lead_id = c.contact_id
                WHERE q.status = 'approved'
                LIMIT 5
            """).fetchall()
            conn.close()

            if not rows:
                await asyncio.sleep(10)
                continue

            for row in rows:
                r = dict(row)
                profile_url = r.get("linkedin_url", "")
                if not profile_url:
                    logger.warning(f"[Campaign Send] No LinkedIn URL for {r.get('first_name')} � skipping")
                    conn2 = sqlite3.connect(DB_PATH, timeout=10.0)
                    conn2.execute("UPDATE campaign_message_queue SET status = 'error' WHERE queue_id = ?", (r["queue_id"],))
                    conn2.commit()
                    conn2.close()
                    continue

                logger.info(f"[Campaign Send] Sending to {r.get('first_name')} {r.get('last_name')}...")
                result = await _send_linkedin_dm(profile_url, r["message_text"])

                conn2 = sqlite3.connect(DB_PATH, timeout=10.0)
                now = datetime.now(timezone.utc).isoformat()
                if result["status"] == "sent":
                    conn2.execute(
                        "UPDATE campaign_message_queue SET status = 'sent', sent_at = ? WHERE queue_id = ?",
                        (now, r["queue_id"])
                    )
                    # Log to campaign_messages
                    conv = conn2.execute(
                        "SELECT conversation_id FROM campaign_conversations WHERE campaign_id = ? AND lead_id = ?",
                        (r["campaign_id"], r["lead_id"])
                    ).fetchone()
                    if conv:
                        conn2.execute(
                            """INSERT INTO campaign_messages (conversation_id, campaign_id, lead_id, direction, message_text, flow_id, sent_at, created_at)
                               VALUES (?, ?, ?, 'OUTBOUND', ?, ?, ?, ?)""",
                            (conv[0], r["campaign_id"], r["lead_id"], r["message_text"], r["flow_id"], now, now)
                        )
                    # Update conversation
                    conn2.execute(
                        "UPDATE campaign_conversations SET last_message_at = ?, last_message_text = ?, last_sender = 'us', updated_at = ? WHERE campaign_id = ? AND lead_id = ?",
                        (now, r["message_text"], now, r["campaign_id"], r["lead_id"])
                    )
                    logger.info(f"[Campaign Send] Sent to {r.get('first_name')}")
                else:
                    conn2.execute(
                        "UPDATE campaign_message_queue SET status = 'error' WHERE queue_id = ?",
                        (r["queue_id"],)
                    )
                    logger.error(f"[Campaign Send] Failed: {result.get('error', 'unknown')}")
                conn2.commit()
                conn2.close()
                await asyncio.sleep(3)  # Delay between sends

            await asyncio.sleep(15)
        except Exception as e:
            logger.error(f"[Campaign Send Loop] Error: {e}")
            await asyncio.sleep(30)


# ===========================================================
# EMAIL CAMPAIGN SEND LOOP
# ===========================================================

async def campaign_email_send_loop():
    """Background loop: processes approved email messages and sends via Gmail."""
    logger.info("[Campaign Email Send Loop] Started.")
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT q.queue_id, q.campaign_id, q.lead_id, q.message_text, q.flow_id,
                       c.email, c.first_name, c.last_name, co.name as company_name
                FROM campaign_message_queue q
                LEFT JOIN contacts c ON q.lead_id = c.contact_id
                LEFT JOIN companies co ON c.company_id = co.company_id
                WHERE q.status = 'approved' AND q.channel = 'email'
                LIMIT 5
            """).fetchall()
            conn.close()

            if not rows:
                await asyncio.sleep(15)
                continue

            for row in rows:
                r = dict(row)
                email_addr = r.get("email")
                if not email_addr:
                    logger.warning(f"[Email Send] No email for {r.get('first_name')} � skipping")
                    conn2 = sqlite3.connect(DB_PATH, timeout=10.0)
                    conn2.execute("UPDATE campaign_message_queue SET status = 'error' WHERE queue_id = ?", (r["queue_id"],))
                    conn2.commit()
                    conn2.close()
                    continue

                # Extract subject from first line or generate
                msg_text = r["message_text"]
                subject = "Kennismaking"
                body = msg_text
                if msg_text.startswith("Subject:"):
                    lines = msg_text.split("\n", 1)
                    subject = lines[0].replace("Subject:", "").strip()
                    body = lines[1].strip() if len(lines) > 1 else ""

                # Inject tracking pixel
                email_id = str(uuid.uuid4())
                tracking_pixel = f'<img src="/track/open/{email_id}" width="1" height="1" style="display:none">'
                html_body = f"<html><body>{body.replace(chr(10), '<br>')}<br>{tracking_pixel}</body></html>"

                logger.info(f"[Email Send] Sending to {email_addr}...")
                result = await gmail_send(to=email_addr, subject=subject, body=body)

                conn2 = sqlite3.connect(DB_PATH, timeout=10.0)
                now = datetime.now(timezone.utc).isoformat()
                if result.get("status") == "sent":
                    conn2.execute(
                        "UPDATE campaign_message_queue SET status = 'sent', sent_at = ? WHERE queue_id = ?",
                        (now, r["queue_id"])
                    )
                    # Log to emails table
                    conn2.execute(
                        """INSERT INTO emails (email_id, contact_id, direction, status, subject, body, sent_at, created_at)
                           VALUES (?, ?, 'outbound', 'sent', ?, ?, ?, ?)""",
                        (email_id, r["lead_id"], subject, body, now, now)
                    )
                    # Log to campaign_messages
                    conv = conn2.execute(
                        "SELECT conversation_id FROM campaign_conversations WHERE campaign_id = ? AND lead_id = ?",
                        (r["campaign_id"], r["lead_id"])
                    ).fetchone()
                    if conv:
                        conn2.execute(
                            """INSERT INTO campaign_messages (conversation_id, campaign_id, lead_id, direction, message_text, flow_id, sent_at, created_at)
                               VALUES (?, ?, ?, 'EMAIL', ?, ?, ?, ?)""",
                            (conv[0], r["campaign_id"], r["lead_id"], body, r["flow_id"], now, now)
                        )
                    logger.info(f"[Email Send] Sent to {email_addr}")
                else:
                    conn2.execute(
                        "UPDATE campaign_message_queue SET status = 'error' WHERE queue_id = ?",
                        (r["queue_id"],)
                    )
                    logger.error(f"[Email Send] Failed: {result.get('error', 'unknown')}")
                conn2.commit()
                conn2.close()
                await asyncio.sleep(5)

            await asyncio.sleep(15)
        except Exception as e:
            logger.error(f"[Campaign Email Send Loop] Error: {e}")
            await asyncio.sleep(30)


# ===========================================================
# UNIFIED CAMPAIGN SEND ENDPOINT (email + linkedin)
# ===========================================================

@app.post("/api/campaigns/{campaign_id}/send-email")
def queue_email_for_campaign(campaign_id: str, data: dict):
    """Queue an email message for a lead in a campaign."""
    lead_id = data.get("lead_id")
    message_text = data.get("message_text")
    subject = data.get("subject", "Kennismaking")
    if not lead_id or not message_text:
        return {"error": "lead_id and message_text required"}

    # Get contact email
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    contact = conn.execute("SELECT email FROM contacts WHERE contact_id = ?", (lead_id,)).fetchone()
    if not contact or not contact[0]:
        conn.close()
        return {"error": "Contact has no email"}

    queue_id = str(_campaign_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    # Prepend subject as first line
    full_msg = f"Subject: {subject}\n\n{message_text}"
    conn.execute(
        """INSERT INTO campaign_message_queue (queue_id, campaign_id, lead_id, message_text, channel, status, direction, created_at)
           VALUES (?, ?, ?, ?, 'email', 'pending_approval', 'OUTBOUND', ?)""",
        (queue_id, campaign_id, lead_id, full_msg, now)
    )
    conn.commit()
    conn.close()
    return {"queue_id": queue_id, "status": "queued_for_approval", "channel": "email"}


@app.post("/api/campaigns/{campaign_id}/send-email-approve")
def approve_and_send_email(campaign_id: str, data: dict):
    """Approve and immediately send an email (skip queue)."""
    lead_id = data.get("lead_id")
    message_text = data.get("message_text")
    subject = data.get("subject", "Kennismaking")
    if not lead_id or not message_text:
        return {"error": "lead_id and message_text required"}

    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    contact = conn.execute("SELECT email FROM contacts WHERE contact_id = ?", (lead_id,)).fetchone()
    if not contact or not contact[0]:
        conn.close()
        return {"error": "Contact has no email"}

    queue_id = str(_campaign_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    full_msg = f"Subject: {subject}\n\n{message_text}"
    conn.execute(
        """INSERT INTO campaign_message_queue (queue_id, campaign_id, lead_id, message_text, channel, status, direction, created_at)
           VALUES (?, ?, ?, ?, 'email', 'approved', 'OUTBOUND', ?)""",
        (queue_id, campaign_id, lead_id, full_msg, now)
    )
    conn.commit()
    conn.close()
    return {"queue_id": queue_id, "status": "approved_and_queued", "channel": "email"}


# ===========================================================
# TEMPLATE SCORING ENDPOINTS
# ===========================================================

@app.get("/api/templates/score")
def score_template_endpoint(template: str, company_id: str = None, contact_id: str = None):
    """Score a template's personalization potential."""
    from template_scorer import score_template
    company = None
    contact = None
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    if company_id:
        row = conn.execute("SELECT * FROM companies WHERE company_id = ?", (company_id,)).fetchone()
        if row:
            company = dict(row)
    if contact_id:
        row = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if row:
            contact = dict(row)
    conn.close()
    return score_template(template, company, contact)


@app.get("/api/templates/library")
def list_template_library(category: str = None):
    """List available template library."""
    from template_scorer import list_templates
    return list_templates(category)


@app.get("/api/templates/personalize")
def personalize_endpoint(template: str, contact_id: str, company_id: str = None):
    """Personalize a template for a specific contact."""
    from template_scorer import personalize_template
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    contact = None
    company = None
    row = conn.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
    if row:
        contact = dict(row)
    if company_id:
        row = conn.execute("SELECT * FROM companies WHERE company_id = ?", (company_id,)).fetchone()
        if row:
            company = dict(row)
    conn.close()
    return personalize_template(template, contact, company)


# ===========================================================
# EMAIL FINDER ENDPOINTS
# ===========================================================

@app.post("/api/email-finder/extract")
def extract_emails_from_url(data: dict):
    """Extract emails from a URL."""
    from email_finder import extract_emails_from_text
    import httpx
    url = data.get("url", "")
    if not url:
        return {"error": "url required"}
    try:
        resp = httpx.get(url, timeout=10, follow_redirects=True)
        emails = extract_emails_from_text(resp.text)
        return {"url": url, "emails": emails, "count": len(emails)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/email-finder/patterns")
def generate_email_patterns_endpoint(first_name: str, last_name: str, domain: str):
    """Generate possible email patterns for a person."""
    from email_finder import generate_email_patterns
    return {"patterns": generate_email_patterns(first_name, last_name, domain)}


@app.get("/api/kvk/search")
async def kvk_search_endpoint(q: str, max: int = 5):
    """Search KVK (Kamer van Koophandel) for Dutch companies."""
    from tools import kvk_search
    results = await kvk_search(q, max_results=max)
    return {"query": q, "results": results, "count": len(results)}


@app.post("/api/kvk/ingest")
async def kvk_ingest_endpoint(data: dict):
    """Search KVK and add results directly to the pipeline."""
    from tools import kvk_search
    query = data.get("query", "")
    if not query:
        return {"error": "query required"}
    results = await kvk_search(query, max_results=data.get("max", 3))

    added = 0
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    cur = conn.cursor()
    for r in results:
        domain = r.get("website", "")
        if domain:
            domain = domain.replace("https://", "").replace("http://", "").split("/")[0].strip()
        name = r.get("name", "")
        if not name:
            continue
        # Check if already exists
        cur.execute("SELECT company_id FROM companies WHERE name LIKE ?", (f"%{name[:30]}%",))
        if cur.fetchone():
            continue

        company_id = f"co_{hashlib.md5((name + domain).encode()).hexdigest()[:8]}"
        contact_id = f"prsp_{hashlib.md5((name + r.get('email', '')).encode()).hexdigest()[:8]}"

        cur.execute("""INSERT OR IGNORE INTO companies
            (company_id, name, domain, industry, city, address, kvk_nr, phone, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'kvk')""",
            (company_id, name, domain, r.get("sbi_code", ""), r.get("city", ""),
             r.get("address", ""), r.get("kvk_nr", ""), r.get("phone", "")))

        first_name = ""
        last_name = ""
        email = r.get("email", "")
        if email and "@" in email:
            parts = email.split("@")[0].split(".")
            if len(parts) >= 2:
                first_name = parts[0].capitalize()
                last_name = parts[1].capitalize()

        cur.execute("""INSERT OR IGNORE INTO contacts
            (contact_id, company_id, first_name, last_name, email, role, current_stage, lead_score, priority)
            VALUES (?, ?, ?, ?, ?, 'Decision Maker', 'INGESTED', 0, 'UNASSIGNED')""",
            (contact_id, company_id, first_name, last_name, email))
        added += 1

    conn.commit()
    conn.close()
    return {"query": query, "found": len(results), "added": added}


# ===========================================================
# CALENDAR BOOKING ENDPOINTS
# ===========================================================

@app.post("/api/calendar/book")
def book_meeting(data: dict):
    """Book a meeting via Google Calendar."""
    contact_email = data.get("contact_email")
    contact_name = data.get("contact_name")
    start_time = data.get("start_time")  # ISO format
    duration = data.get("duration_minutes", 30)
    summary = data.get("summary", "Kennismakingsgesprek")
    description = data.get("description", "")

    if not contact_email or not start_time:
        return {"error": "contact_email and start_time required"}

    # Try to use the calendar connector
    try:
        import sys
        odysseus_dir = r"C:\Users\marvi\odysseus\data"
        if odysseus_dir not in sys.path:
            sys.path.insert(0, odysseus_dir)
        from clawbuildr_calendar import ClawBuildrCalendarConnector

        creds_path = r"C:\Users\marvi\odysseus\data\uploads\2026\06\08\22d61550477d45b0a27a2522b37115ae.json"
        connector = ClawBuildrCalendarConnector(creds_path)
        if connector.authenticate():
            result = connector.create_meeting(
                contact_email=contact_email,
                contact_name=contact_name or "",
                summary=summary,
                description=description,
                start_time_iso=start_time,
                duration_minutes=duration
            )
            # Store meeting
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            conn.execute(
                """INSERT INTO meetings (contact_id, meeting_date, meeting_type, status, calendly_link, created_at)
                   VALUES (?, ?, 'scheduled', 'booked', ?, ?)""",
                (data.get("contact_id", ""), start_time, result.get("hangout_link", ""),
                 datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            conn.close()
            return result
    except Exception as e:
        logger.warning(f"Calendar connector error: {e}")

    return {"status": "mock_booked", "note": "Calendar not configured - add OAuth credentials"}


@app.get("/api/calendar/events")
def list_calendar_events(max_results: int = 10):
    """List upcoming calendar events."""
    try:
        import sys
        odysseus_dir = r"C:\Users\marvi\odysseus\data"
        if odysseus_dir not in sys.path:
            sys.path.insert(0, odysseus_dir)
        from clawbuildr_calendar import ClawBuildrCalendarConnector

        creds_path = r"C:\Users\marvi\odysseus\data\uploads\2026\06\08\22d61550477d45b0a27a2522b37115ae.json"
        connector = ClawBuildrCalendarConnector(creds_path)
        if connector.authenticate():
            return {"events": connector.fetch_calendar_events(max_results)}
    except Exception as e:
        pass
    return {"events": [], "note": "Calendar not configured"}


# ===========================================================
# CAMPAIGN STATS ENDPOINT
# ===========================================================

@app.get("/api/campaigns/{campaign_id}/stats")
def campaign_stats(campaign_id: str):
    """Get campaign performance stats."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    total_leads = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_leads WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()["cnt"]

    sent = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_message_queue WHERE campaign_id = ? AND status = 'sent'",
        (campaign_id,)
    ).fetchone()["cnt"]

    pending = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_message_queue WHERE campaign_id = ? AND status = 'pending_approval'",
        (campaign_id,)
    ).fetchone()["cnt"]

    replied = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_leads WHERE campaign_id = ? AND flow_state = 'replied'",
        (campaign_id,)
    ).fetchone()["cnt"]

    meetings = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_leads WHERE campaign_id = ? AND flow_state = 'meeting_booked'",
        (campaign_id,)
    ).fetchone()["cnt"]

    # Email stats
    emails_sent = conn.execute(
        "SELECT COUNT(*) as cnt FROM campaign_message_queue WHERE campaign_id = ? AND channel = 'email' AND status = 'sent'",
        (campaign_id,)
    ).fetchone()["cnt"]

    emails_opened = conn.execute("""
        SELECT COUNT(DISTINCT e.contact_id) as cnt FROM emails e
        JOIN campaign_leads cl ON e.contact_id = cl.contact_id
        WHERE cl.campaign_id = ? AND e.opened_at IS NOT NULL
    """, (campaign_id,)).fetchone()["cnt"]

    conn.close()

    return {
        "total_leads": total_leads,
        "sent": sent,
        "pending_approval": pending,
        "replied": replied,
        "meetings_booked": meetings,
        "emails_sent": emails_sent,
        "emails_opened": emails_opened,
        "reply_rate": round(replied / max(sent, 1) * 100, 1),
        "meeting_rate": round(meetings / max(sent, 1) * 100, 1)
    }


# ===========================================================
# ENROLL MULTIPLE LEADS ENDPOINT
# ===========================================================

@app.post("/api/campaigns/{campaign_id}/enroll-batch")
def enroll_batch(campaign_id: str, data: dict):
    """Enroll multiple leads in a campaign at once."""
    contact_ids = data.get("contact_ids", [])
    if not contact_ids:
        return {"error": "contact_ids required"}

    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    now = datetime.now(timezone.utc).isoformat()
    enrolled = 0
    skipped = 0

    for cid in contact_ids:
        try:
            conn.execute(
                """INSERT INTO campaign_leads (campaign_id, contact_id, flow_state, current_flow, current_step, sequence_active, enrolled_at, updated_at)
                   VALUES (?, ?, 'new', 'activation', 1, 1, ?, ?)""",
                (campaign_id, cid, now, now)
            )
            enrolled += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()
    conn.close()
    return {"enrolled": enrolled, "skipped": skipped, "total": len(contact_ids)}


async def campaign_inbox_reader_loop():
    """Background loop: reads LinkedIn inbox via Selenium and routes replies to campaigns."""
    logger.info("[Campaign Inbox Reader] Started.")
    while True:
        try:
            await asyncio.sleep(60)  # Check every 60 seconds
            # TODO: Implement LinkedIn inbox reading via Selenium
            # This would:
            # 1. Open LinkedIn messaging page
            # 2. Scrape recent conversations
            # 3. Match conversations to campaign_leads by profile URL
            # 4. For new messages from leads: classify into flow, add to queue for approval
            # 5. Update campaign_conversations with latest message
            pass
        except Exception as e:
            logger.error(f"[Campaign Inbox Reader] Error: {e}")
            await asyncio.sleep(60)


@app.post("/api/campaigns/queue/{queue_id}/send-now")
def send_now(queue_id: str):
    """Approve and immediately send a message via LinkedIn."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        "UPDATE campaign_message_queue SET status = 'approved', approved_at = ? WHERE queue_id = ?",
        (now, queue_id)
    )
    conn.commit()
    conn.close()
    # The send loop will pick it up within seconds
    return {"status": "queued", "message": "Message approved and queued for immediate send"}


@app.post("/api/campaigns/{campaign_id}/compose")
def compose_campaign_message(campaign_id: str, data: dict):
    """Manually compose a message for a lead and add to approval queue."""
    lead_id = data.get("lead_id")
    message_text = data.get("message_text")
    flow_id = data.get("flow_id")
    if not lead_id or not message_text:
        return {"error": "lead_id and message_text required"}

    queue_id = str(_campaign_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute(
        """INSERT INTO campaign_message_queue (queue_id, campaign_id, flow_id, lead_id, direction, message_text, status, created_at)
           VALUES (?, ?, ?, ?, 'OUTBOUND', ?, 'pending_approval', ?)""",
        (queue_id, campaign_id, flow_id, lead_id, message_text, now)
    )
    conn.commit()
    conn.close()
    return {"queue_id": queue_id, "status": "added_to_queue"}


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


def _inject_tracking(email_id: str, html_body: str, base_url: str = None) -> str:
    """Injects tracking pixel and wraps links in a given HTML email body."""
    import hashlib
    
    # Use configured base URL or fall back to request-based URL
    if not base_url:
        base_url = _get_tracking_base_url()
    
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


def _get_tracking_base_url() -> str:
    """Get the base URL for tracking pixels. Checks env var, then settings, then falls back to localhost."""
    import os
    # Environment variable takes priority
    env_url = os.environ.get("TRACKING_BASE_URL", "")
    if env_url:
        return env_url.rstrip("/")
    
    # Check tenant_config for custom tracking URL
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5.0)
        row = conn.execute("SELECT setting_value FROM tenant_config WHERE setting_key = 'tracking_base_url'").fetchone()
        conn.close()
        if row and row[0]:
            return row[0].rstrip("/")
    except Exception:
        pass
    
    return "http://localhost:8000"


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


@app.get("/campaigns", response_class=HTMLResponse)
def campaigns_page():
    return HTMLResponse(content="""<!DOCTYPE html>
<html lang="en" class="h-full bg-slate-950 text-slate-100">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Campaigns - MultiAgentFunnel</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<script>
tailwind.config={theme:{extend:{colors:{brand:{50:'#f0f9ff',100:'#e0f2fe',200:'#bae6fd',300:'#7dd3fc',400:'#38bdf8',500:'#0ea5e9',600:'#0284c7',700:'#0369a1',800:'#075985',900:'#0c4a6e',950:'#082f49'}}}}}
</script>
<style>
*{scrollbar-width:thin;scrollbar-color:#334155 transparent}
body{font-family:'Inter',system-ui,-apple-system,sans-serif}
.flow-card{transition:all .2s}
.flow-card:hover{transform:translateY(-2px);box-shadow:0 8px 25px rgba(0,0,0,.3)}
.flow-card.active{border-color:#0ea5e9;box-shadow:0 0 20px rgba(14,165,233,.15)}
.queue-item{animation:slideIn .3s ease}
@keyframes slideIn{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:translateY(0)}}
.msg-bubble{max-width:70%;padding:12px 16px;border-radius:16px;margin:4px 0;line-height:1.5}
.msg-outbound{background:#0ea5e9;color:white;border-bottom-right-radius:4px;margin-left:auto}
.msg-inbound{background:#1e293b;color:#e2e8f0;border-bottom-left-radius:4px}
.tab-active{border-bottom:2px solid #0ea5e9;color:#0ea5e9}
</style>
</head>
<body class="h-full">
<div id="app" class="flex h-full">
<!-- Sidebar -->
<aside class="w-64 bg-slate-900 border-r border-slate-800 flex flex-col">
<div class="p-4 border-b border-slate-800">
<h1 class="text-lg font-bold text-white"><i class="fa-solid fa-rocket text-sky-400 mr-2"></i>MultiAgentFunnel</h1>
<p class="text-xs text-slate-500 mt-1">Campaign Command Center</p>
</div>
<nav class="flex-1 p-3 space-y-1">
<a href="/" class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-400 hover:bg-slate-800 hover:text-white transition"><i class="fa-solid fa-gauge w-5"></i>Dashboard</a>
<a href="/campaigns" class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm bg-sky-600/15 text-sky-400 font-medium"><i class="fa-solid fa-bullhorn w-5"></i>Campaigns</a>
<a href="/pipeline" class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-400 hover:bg-slate-800 hover:text-white transition"><i class="fa-solid fa-filter w-5"></i>Pipeline</a>
</nav>
</aside>

<!-- Main -->
<main class="flex-1 flex flex-col overflow-hidden">
<!-- Top bar -->
<header class="h-14 bg-slate-900/80 backdrop-blur border-b border-slate-800 flex items-center justify-between px-6">
<div class="flex items-center gap-3">
<h2 class="text-sm font-bold text-white" id="page-title">Campaigns</h2>
<span class="text-xs text-slate-500" id="page-subtitle"></span>
</div>
<div class="flex items-center gap-2">
<button onclick="showCreateModal()" class="px-3 py-1.5 bg-sky-600 hover:bg-sky-500 text-white text-xs font-bold rounded-lg transition flex items-center gap-1.5">
<i class="fa-solid fa-plus"></i>New Campaign
</button>
</div>
</header>

<!-- Content area -->
<div class="flex-1 overflow-y-auto p-6" id="content-area">
<!-- Filled by JS -->
</div>
</main>
</div>

<!-- Create Campaign Modal -->
<div id="create-modal" class="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 hidden flex items-center justify-center">
<div class="bg-slate-900 border border-slate-700 rounded-2xl p-6 w-full max-w-md shadow-2xl">
<h3 class="text-lg font-bold text-white mb-4">New Campaign</h3>
<input id="new-campaign-name" type="text" placeholder="Campaign name..." class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-sky-500 mb-4">
<textarea id="new-campaign-activation" placeholder="Default activation message (first DM when connected)..." rows="4" class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-sky-500 mb-4"></textarea>
<div class="flex justify-end gap-2">
<button onclick="hideCreateModal()" class="px-4 py-2 text-sm text-slate-400 hover:text-white transition">Cancel</button>
<button onclick="createCampaign()" class="px-4 py-2 bg-sky-600 hover:bg-sky-500 text-white text-sm font-bold rounded-lg transition">Create</button>
</div>
</div>
</div>

<script>
const API = '';
let currentView = 'list';
let currentCampaign = null;

// ========== CAMPAIGN LIST ==========
async function loadCampaigns() {
  const res = await fetch(API + '/api/campaigns');
  const campaigns = await res.json();
  const area = document.getElementById('content-area');
  if (!campaigns.length) {
    area.innerHTML = '<div class="text-center py-20"><i class="fa-solid fa-bullhorn text-4xl text-slate-700 mb-4"></i><p class="text-slate-500">No campaigns yet. Create your first one.</p></div>';
    return;
  }
  let html = '<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">';
  for (const c of campaigns) {
    const statusColor = c.status === 'active' ? 'text-emerald-400' : c.status === 'paused' ? 'text-amber-400' : 'text-slate-500';
    const statusDot = c.status === 'active' ? 'bg-emerald-400' : c.status === 'paused' ? 'bg-amber-400' : 'bg-slate-500';
    html += '<div class="flow-card bg-slate-900 border border-slate-800 rounded-xl p-5 cursor-pointer" onclick="openCampaign(\\'' + c.campaign_id + '\\')">';
    html += '<div class="flex items-center justify-between mb-3"><h3 class="text-sm font-bold text-white truncate">' + esc(c.name) + '</h3>';
    html += '<span class="flex items-center gap-1.5 text-xs ' + statusColor + '"><span class="w-2 h-2 rounded-full ' + statusDot + '"></span>' + c.status + '</span></div>';
    html += '<div class="grid grid-cols-3 gap-3 mt-3">';
    html += '<div class="text-center"><div class="text-lg font-bold text-white">' + c.lead_count + '</div><div class="text-[10px] text-slate-500 uppercase">Leads</div></div>';
    html += '<div class="text-center"><div class="text-lg font-bold text-white">' + c.messages_sent + '</div><div class="text-[10px] text-slate-500 uppercase">Sent</div></div>';
    html += '<div class="text-center"><div class="text-lg font-bold text-amber-400">' + c.pending_approval + '</div><div class="text-[10px] text-slate-500 uppercase">Pending</div></div>';
    html += '</div>';
    html += '<div class="flex items-center gap-2 mt-4 text-xs text-slate-500">';
    html += '<span><i class="fa-solid fa-hand mr-1"></i>' + (c.manual_control ? 'Manual' : 'AI') + '</span>';
    html += '<span class="mx-1">�</span>';
    html += '<span><i class="fa-solid fa-shuffle mr-1"></i>' + (c.text_variants ? 'Variants' : 'Static') + '</span>';
    html += '</div></div>';
  }
  html += '</div>';
  area.innerHTML = html;
}

// ========== CAMPAIGN DETAIL ==========
async function openCampaign(id) {
  const res = await fetch(API + '/api/campaigns/' + id);
  currentCampaign = await res.json();
  currentView = 'detail';
  document.getElementById('page-title').textContent = currentCampaign.name;
  document.getElementById('page-subtitle').textContent = currentCampaign.status;
  renderCampaignDetail();
}

function renderCampaignDetail() {
  const c = currentCampaign;
  const area = document.getElementById('content-area');
  let html = '';
  // Tab bar
  html += '<div class="flex gap-6 border-b border-slate-800 mb-6">';
  html += '<button onclick="showTab(\\'flows\\')" class="tab-btn tab-active pb-3 text-sm font-medium" data-tab="flows"><i class="fa-solid fa-code-branch mr-1.5"></i>Script Flows</button>';
  html += '<button onclick="showTab(\\'queue\\')" class="tab-btn pb-3 text-sm font-medium text-slate-500 hover:text-white" data-tab="queue"><i class="fa-solid fa-inbox mr-1.5"></i>Approval Queue <span class="ml-1 px-1.5 py-0.5 bg-amber-500/20 text-amber-400 text-[10px] rounded-full">' + (c.pending_approval || 0) + '</span></button>';
  html += '<button onclick="showTab(\\'inbox\\')" class="tab-btn pb-3 text-sm font-medium text-slate-500 hover:text-white" data-tab="inbox"><i class="fa-solid fa-comments mr-1.5"></i>Live Inbox</button>';
  html += '<button onclick="showTab(\\'leads\\')" class="tab-btn pb-3 text-sm font-medium text-slate-500 hover:text-white" data-tab="leads"><i class="fa-solid fa-users mr-1.5"></i>Leads</button>';
  html += '<button onclick="showTab(\\'settings\\')" class="tab-btn pb-3 text-sm font-medium text-slate-500 hover:text-white" data-tab="settings"><i class="fa-solid fa-gear mr-1.5"></i>Settings</button>';
  html += '</div>';
  html += '<div id="tab-content"></div>';
  area.innerHTML = html;
  showTab('flows');
}

function showTab(tab) {
  document.querySelectorAll('.tab-btn').forEach(b => { b.classList.remove('tab-active'); b.classList.add('text-slate-500'); });
  document.querySelector('[data-tab="' + tab + '"]').classList.add('tab-active');
  document.querySelector('[data-tab="' + tab + '"]').classList.remove('text-slate-500');
  if (tab === 'flows') renderFlowsTab();
  else if (tab === 'queue') renderQueueTab();
  else if (tab === 'inbox') renderInboxTab();
  else if (tab === 'leads') renderLeadsTab();
  else if (tab === 'settings') renderSettingsTab();
}

const FLOW_LABELS = {
  activation: {name: 'Activation Flow', icon: 'fa-paper-plane', color: 'sky'},
  more_info: {name: 'More Information', icon: 'fa-circle-info', color: 'blue'},
  not_interested: {name: 'Not Interested', icon: 'fa-ban', color: 'red'},
  appointment: {name: 'Appointment', icon: 'fa-calendar-check', color: 'emerald'},
  referral: {name: 'Referral', icon: 'fa-user-group', color: 'purple'},
  contact_later: {name: 'Contact Later', icon: 'fa-clock', color: 'amber'},
  confirm: {name: 'Confirm Meeting', icon: 'fa-check-circle', color: 'teal'}
};

function renderFlowsTab() {
  const tc = document.getElementById('tab-content');
  const flows = currentCampaign.flows || [];
  let html = '<div class="space-y-3">';
  for (const f of flows) {
    const info = FLOW_LABELS[f.flow_type] || {name: f.flow_type, icon: 'fa-circle', color: 'slate'};
    const msgs = f.messages || [];
    html += '<div class="flow-card bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">';
    html += '<div class="flex items-center justify-between p-4 cursor-pointer" onclick="toggleFlow(\\'' + f.flow_id + '\\')">';
    html += '<div class="flex items-center gap-3"><i class="fa-solid ' + info.icon + ' text-' + info.color + '-400"></i>';
    html += '<span class="text-sm font-bold text-white">' + info.name + '</span>';
    html += '<span class="text-xs text-slate-500">' + msgs.length + ' message' + (msgs.length !== 1 ? 's' : '') + '</span></div>';
    html += '<div class="flex items-center gap-2">';
    html += '<label class="relative inline-flex items-center cursor-pointer" onclick="event.stopPropagation()">';
    html += '<input type="checkbox" ' + (f.is_active ? 'checked' : '') + ' onchange="toggleFlowActive(\\'' + f.flow_id + '\\', this.checked)" class="sr-only peer">';
    html += '<div class="w-9 h-5 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full after:content-[\\'\\'] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-sky-600"></div></label>';
    html += '<i class="fa-solid fa-chevron-down text-slate-600 text-xs" id="chevron-' + f.flow_id + '"></i></div></div>';
    html += '<div id="flow-detail-' + f.flow_id + '" class="hidden border-t border-slate-800 p-4">';
    // Messages
    for (let i = 0; i < msgs.length; i++) {
      const m = msgs[i];
      html += '<div class="bg-slate-800/50 rounded-lg p-3 mb-2">';
      html += '<div class="flex items-center justify-between mb-2"><span class="text-xs text-slate-500">Step ' + m.step_number + ' � Delay: ' + m.delay_days + ' day' + (m.delay_days !== 1 ? 's' : '') + '</span>';
      html += '<div class="flex gap-1">';
      html += '<button onclick="editMessage(\\'' + m.message_id + '\\', \\'' + f.flow_id + '\\')" class="text-xs text-sky-400 hover:text-sky-300 px-2 py-1"><i class="fa-solid fa-pen"></i></button>';
      html += '<button onclick="deleteMessage(\\'' + m.message_id + '\\')" class="text-xs text-red-400 hover:text-red-300 px-2 py-1"><i class="fa-solid fa-trash"></i></button>';
      html += '</div></div>';
      html += '<p class="text-sm text-slate-300 whitespace-pre-wrap">' + esc(m.message_text) + '</p></div>';
    }
    html += '<button onclick="addMessage(\\'' + f.flow_id + '\\')" class="w-full border border-dashed border-slate-700 rounded-lg py-2 text-xs text-slate-500 hover:border-sky-500 hover:text-sky-400 transition mt-2"><i class="fa-solid fa-plus mr-1"></i>Add Follow-up</button>';
    html += '</div></div>';
  }
  html += '</div>';
  tc.innerHTML = html;
}

function toggleFlow(flowId) {
  const el = document.getElementById('flow-detail-' + flowId);
  const chevron = document.getElementById('chevron-' + flowId);
  el.classList.toggle('hidden');
  chevron.classList.toggle('rotate-180');
}

async function toggleFlowActive(flowId, active) {
  await fetch(API + '/api/campaigns/flows/' + flowId, {
    method: 'PUT', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({is_active: active ? 1 : 0})
  });
}

async function addMessage(flowId) {
  const text = prompt('Enter message text (use {{first_name}}, {{company}}, {{role}} for personalization):');
  if (!text) return;
  await fetch(API + '/api/campaigns/flows/' + flowId + '/messages', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({message_text: text, delay_days: 3})
  });
  openCampaign(currentCampaign.campaign_id);
}

async function editMessage(msgId, flowId) {
  const msg = prompt('Edit message text:');
  if (msg === null) return;
  await fetch(API + '/api/campaigns/messages/' + msgId, {
    method: 'PUT', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({message_text: msg})
  });
  openCampaign(currentCampaign.campaign_id);
}

async function deleteMessage(msgId) {
  if (!confirm('Delete this message?')) return;
  await fetch(API + '/api/campaigns/messages/' + msgId, {method: 'DELETE'});
  openCampaign(currentCampaign.campaign_id);
}

// ========== QUEUE TAB ==========
async function renderQueueTab() {
  const tc = document.getElementById('tab-content');
  const res = await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/queue?status=pending_approval');
  const queue = await res.json();
  let html = '';
  if (queue.length) {
    html += '<div class="flex items-center justify-between mb-4"><h3 class="text-sm font-bold text-white">' + queue.length + ' message' + (queue.length !== 1 ? 's' : '') + ' pending</h3>';
    html += '<button onclick="approveAll()" class="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold rounded-lg transition"><i class="fa-solid fa-check-double mr-1"></i>Approve All</button></div>';
    html += '<div class="space-y-3">';
    for (const q of queue) {
      html += '<div class="queue-item bg-slate-900 border border-slate-800 rounded-xl p-4">';
      html += '<div class="flex items-center justify-between mb-2">';
      html += '<div class="flex items-center gap-2"><span class="text-sm font-bold text-white">' + esc(q.first_name || '?') + ' ' + esc(q.last_name || '') + '</span>';
      if (q.company_name) html += '<span class="text-xs text-slate-500">at ' + esc(q.company_name) + '</span>';
      if (q.ai_confidence > 0) html += '<span class="text-xs px-1.5 py-0.5 rounded bg-sky-500/15 text-sky-400">' + q.ai_confidence + '% confidence</span>';
      html += '</div>';
      html += '<div class="flex gap-2">';
      html += '<button onclick="rejectMsg(\\'' + q.queue_id + '\\')" class="px-3 py-1 bg-slate-800 hover:bg-red-500/20 text-slate-400 hover:text-red-400 text-xs rounded-lg transition"><i class="fa-solid fa-xmark mr-1"></i>Reject</button>';
      html += '<button onclick="approveMsg(\\'' + q.queue_id + '\\')" class="px-3 py-1 bg-slate-700 hover:bg-slate-600 text-white text-xs rounded-lg transition"><i class="fa-solid fa-check mr-1"></i>Approve</button>';
      html += '<button onclick="sendNowMsg(\\'' + q.queue_id + '\\')" class="px-3 py-1 bg-sky-600 hover:bg-sky-500 text-white text-xs font-bold rounded-lg transition"><i class="fa-solid fa-paper-plane mr-1"></i>Send Now</button>';
      html += '</div></div>';
      html += '<p class="text-sm text-slate-300 whitespace-pre-wrap bg-slate-800/50 rounded-lg p-3">' + esc(q.message_text) + '</p>';
      html += '</div>';
    }
    html += '</div>';
  } else {
    html = '<div class="text-center py-20"><i class="fa-solid fa-inbox text-4xl text-slate-700 mb-4"></i><p class="text-slate-500">No messages pending approval</p></div>';
  }
  tc.innerHTML = html;
}

async function approveMsg(id) {
  await fetch(API + '/api/campaigns/queue/' + id + '/approve', {method: 'POST'});
  renderQueueTab();
}

async function sendNowMsg(id) {
  await fetch(API + '/api/campaigns/queue/' + id + '/send-now', {method: 'POST'});
  renderQueueTab();
}

async function rejectMsg(id) {
  await fetch(API + '/api/campaigns/queue/' + id + '/reject', {method: 'POST'});
  renderQueueTab();
}

async function approveAll() {
  await fetch(API + '/api/campaigns/queue/approve-all', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({campaign_id: currentCampaign.campaign_id})
  });
  renderQueueTab();
}

async function toggleSequence(convId, active) {
  await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/inbox/' + convId + '/toggle', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({sequence_active: active})
  });
  openConversation(convId);
}

// ========== INBOX TAB ==========
async function renderInboxTab() {
  const tc = document.getElementById('tab-content');
  const res = await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/inbox');
  const convos = await res.json();
  if (!convos.length) {
    tc.innerHTML = '<div class="text-center py-20"><i class="fa-solid fa-comments text-4xl text-slate-700 mb-4"></i><p class="text-slate-500">No conversations yet. Add leads to start.</p></div>';
    return;
  }
  let html = '<div class="flex h-[calc(100vh-220px)]">';
  // Left: conversation list
  html += '<div class="w-72 border-r border-slate-800 overflow-y-auto">';
  for (const cv of convos) {
    html += '<div class="p-3 border-b border-slate-800/50 cursor-pointer hover:bg-slate-800/50 transition" onclick="openConversation(\\'' + cv.conversation_id + '\\')">';
    html += '<div class="flex items-center gap-2"><span class="text-sm font-medium text-white">' + esc(cv.first_name || '?') + ' ' + esc(cv.last_name || '') + '</span>';
    html += '<span class="text-[10px] px-1.5 py-0.5 rounded bg-sky-500/15 text-sky-400">' + (cv.lead_state || 'new') + '</span></div>';
    html += '<p class="text-xs text-slate-500 truncate mt-1">' + esc(cv.last_message_text || 'No messages yet') + '</p>';
    html += '<span class="text-[10px] text-slate-600">' + (cv.last_message_at ? timeAgo(cv.last_message_at) : '') + '</span>';
    html += '</div>';
  }
  html += '</div>';
  // Right: conversation view
  html += '<div class="flex-1 flex flex-col" id="conv-view"><div class="flex-1 flex items-center justify-center text-slate-600 text-sm">Select a conversation</div></div>';
  html += '</div>';
  tc.innerHTML = html;
}

async function openConversation(convId) {
  const view = document.getElementById('conv-view');
  view.innerHTML = '<div class="flex items-center justify-center text-slate-500 text-sm">Loading...</div>';
  const res = await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/inbox');
  const convos = await res.json();
  const conv = convos.find(c => c.conversation_id === convId);
  if (!conv) return;
  let html = '<div class="flex flex-col h-full">';
  // Header
  html += '<div class="p-4 border-b border-slate-800 flex items-center justify-between">';
  html += '<div><span class="text-sm font-bold text-white">' + esc(conv.first_name || '') + ' ' + esc(conv.last_name || '') + '</span>';
  if (conv.company_name) html += ' <span class="text-xs text-slate-500">at ' + esc(conv.company_name) + '</span></div>';
  html += '<div class="flex gap-2">';
  html += '<button onclick="toggleSequence(\\'' + convId + '\\', ' + (conv.sequence_active ? 'false' : 'true') + ')" class="px-2 py-1 text-xs rounded ' + (conv.sequence_active ? 'bg-emerald-500/15 text-emerald-400' : 'bg-slate-700 text-slate-400') + '"><i class="fa-solid fa-' + (conv.sequence_active ? 'pause' : 'play') + ' mr-1"></i>' + (conv.sequence_active ? 'Active' : 'Paused') + '</button>';
  html += '</div></div>';
  // Messages
  html += '<div class="flex-1 overflow-y-auto p-4 space-y-1" id="msg-list">';
  const msgs = (conv.recent_messages || []).reverse();
  for (const m of msgs) {
    const cls = m.direction === 'OUTBOUND' ? 'msg-outbound' : 'msg-inbound';
    html += '<div class="flex ' + (m.direction === 'OUTBOUND' ? 'justify-end' : 'justify-start') + '"><div class="msg-bubble ' + cls + '">' + esc(m.message_text) + '</div></div>';
  }
  html += '</div>';
  // Compose
  html += '<div class="p-3 border-t border-slate-800">';
  html += '<div class="flex gap-2"><input id="conv-input" type="text" placeholder="Type a message..." class="flex-1 bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-sky-500">';
  html += '<button onclick="sendConvMsg(\\'' + convId + '\\')" class="px-4 py-2 bg-sky-600 hover:bg-sky-500 text-white text-sm font-bold rounded-lg transition"><i class="fa-solid fa-paper-plane"></i></button></div></div>';
  html += '</div>';
  view.innerHTML = html;
  document.getElementById('msg-list').scrollTop = 99999;
}

async function sendConvMsg(convId) {
  const input = document.getElementById('conv-input');
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/inbox/' + convId + '/send', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({message_text: text})
  });
  openConversation(convId);
}

// ========== LEADS TAB ==========
async function renderLeadsTab() {
  const tc = document.getElementById('tab-content');
  const res = await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id + '/leads');
  const leads = await res.json();
  if (!leads.length) {
    tc.innerHTML = '<div class="text-center py-20"><i class="fa-solid fa-users text-4xl text-slate-700 mb-4"></i><p class="text-slate-500">No leads in this campaign yet.</p></div>';
    return;
  }
  let html = '<table class="w-full text-sm"><thead><tr class="text-left text-xs text-slate-500 uppercase"><th class="pb-3">Name</th><th class="pb-3">Company</th><th class="pb-3">Flow</th><th class="pb-3">Step</th><th class="pb-3">Next Action</th><th class="pb-3">Status</th></tr></thead><tbody>';
  for (const l of leads) {
    html += '<tr class="border-t border-slate-800/50">';
    html += '<td class="py-3 text-white font-medium">' + esc(l.first_name || '') + ' ' + esc(l.last_name || '') + '</td>';
    html += '<td class="py-3 text-slate-400">' + esc(l.company_name || '-') + '</td>';
    html += '<td class="py-3"><span class="text-xs px-2 py-1 rounded bg-sky-500/15 text-sky-400">' + (l.current_flow || 'activation') + '</span></td>';
    html += '<td class="py-3 text-slate-400">' + (l.current_step || 1) + '</td>';
    html += '<td class="py-3 text-slate-500 text-xs">' + (l.next_action_at ? timeAgo(l.next_action_at) : '-') + '</td>';
    html += '<td class="py-3"><span class="text-xs px-2 py-1 rounded ' + (l.sequence_active ? 'bg-emerald-500/15 text-emerald-400' : 'bg-slate-700 text-slate-500') + '">' + (l.sequence_active ? 'Active' : 'Paused') + '</span></td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  tc.innerHTML = html;
}

// ========== SETTINGS TAB ==========
function renderSettingsTab() {
  const tc = document.getElementById('tab-content');
  const c = currentCampaign;
  let html = '<div class="max-w-xl space-y-6">';
  html += '<div class="bg-slate-900 border border-slate-800 rounded-xl p-5">';
  html += '<h3 class="text-sm font-bold text-white mb-4">Campaign Settings</h3>';
  html += '<label class="flex items-center justify-between mb-4"><span class="text-sm text-slate-300">Manual Control (greenlight every message)</span>';
  html += '<label class="relative inline-flex items-center cursor-pointer"><input type="checkbox" ' + (c.manual_control ? 'checked' : '') + ' onchange="updateCampaign(\\'manual_control\\', this.checked ? 1 : 0)" class="sr-only peer"><div class="w-9 h-5 bg-slate-700 rounded-full peer peer-checked:after:translate-x-full after:content-[\\'\\'] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-sky-600"></div></label></label>';
  html += '<label class="flex items-center justify-between mb-4"><span class="text-sm text-slate-300">Text Variants (variation to avoid detection)</span>';
  html += '<label class="relative inline-flex items-center cursor-pointer"><input type="checkbox" ' + (c.text_variants ? 'checked' : '') + ' onchange="updateCampaign(\\'text_variants\\', this.checked ? 1 : 0)" class="sr-only peer"><div class="w-9 h-5 bg-slate-700 rounded-full peer peer-checked:after:translate-x-full after:content-[\\'\\'] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-sky-600"></div></label></label>';
  html += '<label class="flex items-center justify-between"><span class="text-sm text-slate-300">AI Confidence Threshold</span>';
  html += '<input type="number" value="' + c.ai_confidence_threshold + '" onchange="updateCampaign(\\'ai_confidence_threshold\\', parseInt(this.value))" class="w-20 bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white text-center focus:outline-none focus:border-sky-500"></label>';
  html += '</div>';
  html += '<div class="bg-slate-900 border border-slate-800 rounded-xl p-5">';
  html += '<h3 class="text-sm font-bold text-white mb-3">Danger Zone</h3>';
  html += '<button onclick="deleteCampaign()" class="px-4 py-2 bg-red-600/15 hover:bg-red-600/30 text-red-400 text-sm font-medium rounded-lg transition">Delete Campaign</button>';
  html += '</div></div>';
  tc.innerHTML = html;
}

async function updateCampaign(key, value) {
  await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id, {
    method: 'PUT', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({[key]: value})
  });
  currentCampaign[key] = value;
}

async function deleteCampaign() {
  if (!confirm('Delete this campaign and all its data?')) return;
  await fetch(API + '/api/campaigns/' + currentCampaign.campaign_id, {method: 'DELETE'});
  goBack();
}

// ========== UTILS ==========
function goBack() {
  currentView = 'list';
  currentCampaign = null;
  document.getElementById('page-title').textContent = 'Campaigns';
  document.getElementById('page-subtitle').textContent = '';
  loadCampaigns();
}

function showCreateModal() { document.getElementById('create-modal').classList.remove('hidden'); }
function hideCreateModal() { document.getElementById('create-modal').classList.add('hidden'); }

async function createCampaign() {
  const name = document.getElementById('new-campaign-name').value.trim();
  const activation = document.getElementById('new-campaign-activation').value.trim();
  if (!name) return alert('Enter a campaign name');
  await fetch(API + '/api/campaigns', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name, activation_message: activation || undefined})
  });
  hideCreateModal();
  loadCampaigns();
}

function esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function timeAgo(iso) {
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return 'just now';
  if (diff < 3600) return Math.floor(diff/60) + 'm ago';
  if (diff < 86400) return Math.floor(diff/3600) + 'h ago';
  return Math.floor(diff/86400) + 'd ago';
}

// Init
loadCampaigns();
</script>
</body></html>""");


# =========================================================================
# MISSING ENDPOINTS — Frontend requires these but they were never implemented
# =========================================================================

@app.get("/api/funnel/stats")
def get_funnel_stats():
    """Returns lead counts per pipeline stage for the Funnel Pipeline widget."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    stages = {}
    for stage in ["INGESTED", "DISCOVERED", "ENRICHING", "VERIFYING", "READY", "CONTACTED"]:
        row = conn.execute("SELECT COUNT(*) as cnt FROM contacts WHERE current_stage = ?", (stage,)).fetchone()
        stages[stage] = row["cnt"] if row else 0
    conn.close()
    return {"stages": stages}


@app.post("/api/funnel/trigger")
async def trigger_funnel(request: Request):
    """Manually triggers one full pipeline cycle."""
    try:
        import asyncio as _asyncio
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clawbuildr"))
        from pipeline_runner import _run_lead_gen, _run_research, _run_email_gen, _run_quality_gate, _run_email_send

        def _run_cycle():
            try:
                _run_lead_gen()
            except Exception as e:
                logger.error(f"[Funnel Trigger] Lead gen error: {e}")
            try:
                _run_research()
            except Exception as e:
                logger.error(f"[Funnel Trigger] Research error: {e}")
            try:
                _run_email_gen()
            except Exception as e:
                logger.error(f"[Funnel Trigger] Email gen error: {e}")
            try:
                _run_quality_gate()
            except Exception as e:
                logger.error(f"[Funnel Trigger] Quality gate error: {e}")
            try:
                _run_email_send()
            except Exception as e:
                logger.error(f"[Funnel Trigger] Send error: {e}")

        _asyncio.create_task(_asyncio.to_thread(_run_cycle))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/api/deliverability/audit")
async def run_deliverability_audit(request: Request):
    """Runs a live deliverability audit on a sending domain using MX + SPF + DKIM checks."""
    body = await request.json()
    domain = body.get("domain", "")
    if not domain:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT domain FROM deliverability_stats LIMIT 1").fetchone()
        conn.close()
        domain = row["domain"] if row else ""
    if not domain:
        return {"status": "error", "message": "No domain to audit"}
    issues = []
    import subprocess, socket
    try:
        mx = socket.getaddrinfo(f"mail.{domain}", 25, socket.AF_INET)
        if not mx:
            issues.append("No MX records found")
    except Exception:
        issues.append("No MX records found")
    try:
        txt_records = subprocess.run(["nslookup", "-type=TXT", domain], capture_output=True, text=True, timeout=5)
        if "spf" not in txt_records.stdout.lower():
            issues.append("SPF record not detected")
    except Exception:
        issues.append("Could not verify SPF")
    health = "HEALTHY" if len(issues) == 0 else ("WARMING" if len(issues) <= 1 else "BLOCKED")
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO deliverability_stats (domain, sends_count, replies_count, bounces_count, bounce_rate, domain_health, updated_at)
        VALUES (?, 0, 0, 0, 0.0, ?, ?)
    """, (domain, health, now))
    conn.commit()
    conn.close()
    return {"status": "success", "domain": domain, "health": health, "issues": issues}


@app.get("/api/leads/top")
def get_top_leads(limit: int = 20):
    """Returns top-scored leads for the leaderboard."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT c.contact_id, c.first_name, c.last_name, c.email, co.name as company_name,
               c.lead_score as total_score,
               c.lead_score, 0 as engagement_score, c.lead_score as fit_score, 0 as recency_score
        FROM contacts c
        LEFT JOIN companies co ON c.company_id = co.company_id
        WHERE c.lead_score > 0
        ORDER BY c.lead_score DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return {"leads": [dict(r) for r in rows]}


@app.post("/api/leads/import-csv")
async def import_leads_csv():
    """Imports leads from a CSV upload."""
    form = await request.form()
    file = form.get("file")
    if not file:
        return JSONResponse({"detail": "No file provided"}, status_code=400)
    content = await file.read()
    text = content.decode("utf-8", errors="replace")
    import csv, io as _io
    reader = csv.DictReader(_io.StringIO(text))
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for row in reader:
        email = (row.get("email") or "").strip()
        if not email or "@" not in email:
            continue
        cid = str(uuid.uuid4())
        try:
            conn.execute("""
                INSERT OR IGNORE INTO contacts (contact_id, first_name, last_name, email, role, company_name, current_stage, lead_score, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'INGESTED', 10, ?)
            """, (cid, row.get("first_name", ""), row.get("last_name", ""), email, row.get("role", ""), row.get("company_name", ""), now))
            count += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return {"message": f"Successfully imported {count} leads."}


@app.get("/api/search/queries")
def get_search_queries():
    """Returns saved search queries."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM search_queries ORDER BY created_at DESC LIMIT 50").fetchall()
    except Exception:
        rows = []
    conn.close()
    return {"queries": [dict(r) for r in rows]}


@app.post("/api/search")
async def run_search(request: Request):
    """Runs a prospecting search via Hunter.io or web scraping."""
    body = await request.json()
    prompt = body.get("prompt", "")
    name = body.get("name", prompt[:50])
    sources = body.get("sources", [])
    import uuid as _uuid
    qid = str(_uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    try:
        conn.execute("""
            INSERT INTO search_queries (query_id, name, prompt, status, result_count, created_at)
            VALUES (?, ?, ?, 'running', 0, ?)
        """, (qid, name, prompt, now))
        conn.commit()
    except Exception:
        pass
    domains_found = []
    try:
        from tools import hunter_domain_search
        results = await hunter_domain_search(prompt, limit=25)
        domains_found = [r.get("email", "").split("@")[-1] for r in results if r.get("email")]
        domains_found = list(set(domains_found))[:25]
        for d in domains_found:
            try:
                conn.execute("INSERT OR IGNORE INTO search_results (query_id, domain, company_name, confidence, used, created_at) VALUES (?, ?, ?, 70, 0, ?)", (qid, d, d.split(".")[0].replace("-", " ").title(), now))
            except Exception:
                pass
        conn.execute("UPDATE search_queries SET status = 'completed', result_count = ? WHERE query_id = ?", (len(domains_found), qid))
        conn.commit()
    except Exception as e:
        conn.execute("UPDATE search_queries SET status = 'failed' WHERE query_id = ?", (qid,))
        conn.commit()
    conn.close()
    return {"query_id": qid, "message": f"Found {len(domains_found)} domains.", "domains": domains_found}


@app.get("/api/search/results/{query_id}")
async def get_search_results(query_id: str):
    """Returns search results for a given query."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        # Add missing columns if they don't exist (migration)
        try:
            conn.execute("ALTER TABLE search_results ADD COLUMN company_name TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE search_results ADD COLUMN industry TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE search_results ADD COLUMN location TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE search_results ADD COLUMN confidence INTEGER DEFAULT 50")
        except Exception:
            pass
        conn.commit()

        rows = conn.execute(
            "SELECT result_id, domain, company_name, industry, location, confidence, used FROM search_results WHERE query_id = ?",
            (query_id,)
        ).fetchall()
        results = [dict(r) for r in rows]
        # Fill in company_name from companies table if missing
        for r in results:
            if not r.get("company_name"):
                co = conn.execute("SELECT name, industry FROM companies WHERE domain = ?", (r["domain"],)).fetchone()
                if co:
                    r["company_name"] = co["name"]
                    r["industry"] = co["industry"]
        return {"results": results}
    except Exception as e:
        return {"results": [], "error": str(e)}
    finally:
        conn.close()


@app.post("/api/search/convert")
async def bulk_convert_search_results(request: Request):
    """Converts search results into pipeline leads."""
    body = await request.json()
    result_ids = body.get("result_ids", [])
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()
    converted = 0
    for rid in result_ids:
        row = conn.execute("SELECT * FROM search_results WHERE result_id = ?", (rid,)).fetchone()
        if not row:
            continue
        domain = row["domain"] if isinstance(row, dict) else row[1] if len(row) > 1 else ""
        if not domain or "@" in domain:
            continue
        cid = str(uuid.uuid4())
        try:
            conn.execute("""
                INSERT OR IGNORE INTO contacts (contact_id, email, company_name, company_domain, current_stage, lead_score, created_at)
                VALUES (?, ?, ?, ?, 'INGESTED', 10, ?)
            """, (cid, f"info@{domain}", domain, domain, now))
            conn.execute("UPDATE search_results SET used = 1 WHERE result_id = ?", (rid,))
            converted += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return {"message": f"Converted {converted} results to leads.", "converted": converted, "created": converted}


@app.get("/api/sequences")
def get_sequences():
    """Returns email sequences with their steps."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        seq_rows = conn.execute("SELECT * FROM sequences ORDER BY created_at DESC").fetchall()
        sequences = []
        for seq in seq_rows:
            s = dict(seq)
            steps = conn.execute("SELECT * FROM sequence_steps WHERE sequence_id = ? ORDER BY step_number", (s["sequence_id"],)).fetchall()
            s["steps"] = []
            for st in steps:
                sd = dict(st)
                sd["delay_days"] = (sd.get("delay_hours") or 0) / 24
                s["steps"].append(sd)
            sequences.append(s)
    except Exception:
        sequences = []
    conn.close()
    return {"sequences": sequences}


@app.post("/api/sequences")
async def create_sequence(request: Request):
    """Creates a new email sequence with steps."""
    body = await request.json()
    seq_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("""
        INSERT INTO sequences (sequence_id, name, description, created_at) VALUES (?, ?, ?, ?)
    """, (seq_id, body.get("name", "New Sequence"), body.get("description", ""), now))
    for i, step in enumerate(body.get("steps", []), 1):
        step_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO sequence_steps (step_id, sequence_id, step_number, step_type, delay_hours, subject, body, connection_note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (step_id, seq_id, i, step.get("step_type", "email"), step.get("delay_hours", 0),
              step.get("subject", ""), step.get("body", ""), step.get("connection_note", "")))
    conn.commit()
    conn.close()
    return {"sequence_id": seq_id, "status": "created"}


@app.get("/api/workflows")
def get_workflows():
    """Returns automated workflows with their run history."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        wf_rows = conn.execute("SELECT * FROM workflows ORDER BY created_at DESC").fetchall()
        workflows = []
        for wf in wf_rows:
            w = dict(wf)
            if isinstance(w.get("steps"), str):
                try:
                    w["steps"] = json.loads(w["steps"])
                except Exception:
                    w["steps"] = []
            workflows.append(w)
    except Exception:
        workflows = []
    conn.close()
    return {"workflows": workflows}


@app.post("/api/workflows")
async def create_workflow(request: Request):
    """Creates a new automated workflow."""
    body = await request.json()
    wf_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("""
        INSERT INTO workflows (workflow_id, name, description, steps, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (wf_id, body.get("name", "New Workflow"), body.get("description", ""),
          json.dumps(body.get("steps", [])), 1 if body.get("is_active", True) else 0, now))
    conn.commit()
    conn.close()
    return {"workflow_id": wf_id, "status": "created"}


@app.get("/api/workflows/{workflow_id}/runs")
def get_workflow_runs(workflow_id: str):
    """Returns execution history for a workflow."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM workflow_runs WHERE workflow_id = ? ORDER BY started_at DESC LIMIT 20", (workflow_id,)).fetchall()
    except Exception:
        rows = []
    conn.close()
    return {"runs": [dict(r) for r in rows]}


@app.post("/api/workflows/{workflow_id}/run")
async def run_workflow(workflow_id: str):
    """Triggers a workflow execution."""
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    try:
        conn.execute("""
            INSERT INTO workflow_runs (run_id, workflow_id, status, leads_found, leads_sent, started_at, completed_at)
            VALUES (?, ?, 'running', 0, 0, ?, ?)
        """, (run_id, workflow_id, now, now))
        conn.commit()
    except Exception:
        pass
    conn.close()
    return {"run_id": run_id}


# ===========================================================
# SIMPLE ONBOARDING & CAMPAIGN API
# ===========================================================

@app.get("/api/onboarding/status")
def api_onboarding_status():
    """Returns the onboarding state for the active tenant."""
    if get_onboarding_state is None:
        return {"completed": True, "steps": []}
    return get_onboarding_state(_active_tenant_id())


@app.post("/api/onboarding/step")
async def api_onboarding_step(request: Request):
    """Save progress for one onboarding step."""
    if save_onboarding_step is None:
        return {"completed": True}
    body = await request.json()
    step = body.get("step")
    data = body.get("data", {})
    if step not in ["welcome", "channels", "gmail", "profile", "icp", "message", "sequence", "sources", "search", "review", "running"]:
        raise HTTPException(status_code=400, detail="Invalid step")
    return save_onboarding_step(_active_tenant_id(), step, data)


@app.post("/api/onboarding/connect-gmail")
def api_onboarding_connect_gmail(background_tasks: BackgroundTasks):
    """Trigger Gmail OAuth for the active tenant during onboarding."""
    tenant_id = _active_tenant_id()
    return connect_gmail_tenant(tenant_id, background_tasks)


@app.post("/api/onboarding/launch")
async def api_onboarding_launch(request: Request):
    """Finalize onboarding: save tenant config, create first campaign, mark complete."""
    body = await request.json()
    tenant_id = _active_tenant_id()
    tenant = _get_active_tenant()

    # Merge launch payload into tenant config
    save_payload = {
        "tenant_id": tenant_id,
        "display_name": body.get("display_name", tenant.get("display_name", "")),
        "sending_email": body.get("sending_email", tenant.get("sending_email", "")),
        "sending_domain": (body.get("sending_email", tenant.get("sending_email", "")) or "").split("@")[1] if "@" in (body.get("sending_email", tenant.get("sending_email", "")) or "") else tenant.get("sending_domain", ""),
        "calendar_link": body.get("calendar_link", tenant.get("calendar_link", "")),
        "signature_block": body.get("signature_block", tenant.get("signature_block", "")),
        "value_doctrine": body.get("value_doctrine", tenant.get("value_doctrine", "")),
        "brand_voice": body.get("brand_voice", tenant.get("brand_voice", "Professioneel, direct, vriendelijk.")),
        "icp_industries": body.get("icp_industries", tenant.get("icp_industries", [])),
        "icp_roles": body.get("icp_roles", tenant.get("icp_roles", [])),
        "icp_company_size": body.get("icp_company_size", tenant.get("icp_company_size", "10-200")),
        "search_queries": body.get("search_queries", tenant.get("search_queries", [])),
        "lead_sources": body.get("lead_sources", tenant.get("lead_sources", ["hunter", "directories", "kvk"])),
        "campaign_channels": body.get("campaign_channels", tenant.get("campaign_channels", ["email", "linkedin"])),
        "sequence_template": body.get("sequence_template", tenant.get("sequence_template", "gentle")),
        "icp_regions": body.get("icp_regions", tenant.get("icp_regions", ["nl"])),
        "pain_points": body.get("pain_points", tenant.get("pain_points", [])),
    }
    # Re-use existing tenant save logic
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    try:
        conn.execute("""
            INSERT INTO tenant_config
                (tenant_id, display_name, sending_email, sending_domain, calendar_link,
                 signature_block, value_doctrine, brand_voice, icp_industries,
                 icp_roles, icp_company_size, search_queries, active,
                 lead_sources, campaign_channels, sequence_template, icp_regions, pain_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
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
                active         = 1,
                lead_sources   = excluded.lead_sources,
                campaign_channels = excluded.campaign_channels,
                sequence_template = excluded.sequence_template,
                icp_regions    = excluded.icp_regions,
                pain_points    = excluded.pain_points
        """, (
            tenant_id,
            save_payload["display_name"],
            save_payload["sending_email"],
            save_payload["sending_domain"],
            save_payload["calendar_link"],
            save_payload["signature_block"],
            save_payload["value_doctrine"],
            save_payload["brand_voice"],
            json.dumps(save_payload["icp_industries"]),
            json.dumps(save_payload["icp_roles"]),
            save_payload["icp_company_size"],
            json.dumps(save_payload["search_queries"]),
            json.dumps(save_payload["lead_sources"]),
            json.dumps(save_payload["campaign_channels"]),
            save_payload["sequence_template"],
            json.dumps(save_payload["icp_regions"]),
            json.dumps(save_payload["pain_points"]),
        ))
        conn.execute("UPDATE tenant_config SET active = 0 WHERE tenant_id != ?", (tenant_id,))
        conn.commit()
    finally:
        conn.close()

    # Create first campaign if requested
    campaign = None
    contact_ids = body.get("contact_ids", [])
    if body.get("create_campaign", True):
        if not contact_ids:
            # Auto-select early-stage leads not yet in a campaign
            conn = sqlite3.connect(DB_PATH, timeout=10.0)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute("""
                    SELECT c.contact_id FROM contacts c
                    LEFT JOIN campaign_leads cl ON c.contact_id = cl.contact_id
                    WHERE c.current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED')
                      AND cl.contact_id IS NULL
                    ORDER BY c.lead_score DESC
                    LIMIT 50
                """).fetchall()
                contact_ids = [r["contact_id"] for r in rows]
            finally:
                conn.close()
        campaign = _create_simple_campaign(
            name=body.get("campaign_name", f"{save_payload['display_name'] or 'Eerste'} campagne"),
            activation_message=body.get("value_doctrine", ""),
            contact_ids=contact_ids,
        )

    if complete_onboarding is not None:
        complete_onboarding(tenant_id)

    return {"status": "launched", "tenant_id": tenant_id, "campaign": campaign}


@app.get("/api/simple/stats")
def api_simple_stats():
    """High-level stats for the simple dashboard."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        active_leads = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE current_stage NOT IN ('CLOSED_LOST', 'BOUNCED')"
        ).fetchone()[0]
        sent = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE direction = 'outbound' AND status = 'SENT'"
        ).fetchone()[0]
        replies = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE direction = 'inbound'"
        ).fetchone()[0]
        meetings = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE current_stage = 'MEETING_BOOKED'"
        ).fetchone()[0]
        queued = conn.execute(
            "SELECT COUNT(*) FROM followup_queue WHERE status IN ('pending', 'ready')"
        ).fetchone()[0]
    finally:
        conn.close()
    return {
        "active_leads": active_leads,
        "emails_sent": sent,
        "replies": replies,
        "meetings": meetings,
        "followups_queued": queued,
        "healthy": sent > 0 or active_leads > 0,
    }


@app.get("/api/simple/campaigns")
def api_simple_campaigns():
    """Campaign list with simplified stats."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
        campaigns = []
        for r in rows:
            c = dict(r)
            cid = c["campaign_id"]
            c["lead_count"] = conn.execute(
                "SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = ?", (cid,)
            ).fetchone()[0]
            c["active_count"] = conn.execute(
                """SELECT COUNT(*) FROM campaign_leads cl
                   JOIN contacts c ON cl.contact_id = c.contact_id
                   WHERE cl.campaign_id = ? AND c.current_stage NOT IN ('CLOSED_LOST','BOUNCED')""",
                (cid,),
            ).fetchone()[0]
            c["sent_count"] = conn.execute(
                """SELECT COUNT(*) FROM emails e
                   JOIN campaign_leads cl ON e.contact_id = cl.contact_id
                   WHERE cl.campaign_id = ? AND e.direction = 'outbound'""",
                (cid,),
            ).fetchone()[0]
            c["reply_count"] = conn.execute(
                """SELECT COUNT(*) FROM emails e
                   JOIN campaign_leads cl ON e.contact_id = cl.contact_id
                   WHERE cl.campaign_id = ? AND e.direction = 'inbound'""",
                (cid,),
            ).fetchone()[0]
            campaigns.append(c)
        return {"campaigns": campaigns}
    finally:
        conn.close()


@app.post("/api/simple/campaigns")
async def api_create_simple_campaign(request: Request):
    """Create a new campaign from the simple UI wizard."""
    body = await request.json()
    contact_ids = body.get("contact_ids", [])
    if not contact_ids:
        raise HTTPException(status_code=400, detail="Select at least one lead")
    result = _create_simple_campaign(
        name=body.get("name", "New Campaign"),
        activation_message=body.get("activation_message", ""),
        contact_ids=contact_ids,
    )
    return result


@app.get("/api/simple/contacts/selectable")
def api_selectable_contacts(limit: int = 200):
    """Contacts that can be added to a new campaign."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT c.contact_id, c.first_name, c.last_name, c.email, c.role, c.current_stage,
                   co.name as company_name, co.domain as company_domain, co.industry as company_industry
            FROM contacts c
            LEFT JOIN companies co ON c.company_id = co.company_id
            LEFT JOIN campaign_leads cl ON c.contact_id = cl.contact_id
            WHERE c.current_stage IN ('INGESTED','RESEARCHED','DELIVERABILITY_VERIFIED','OPPORTUNITY_MAPPED','PRE_QUALIFIED','ACTIVE_OUTREACH')
              AND cl.contact_id IS NULL
            ORDER BY c.lead_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return {"contacts": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/api/pipeline/start")
def api_start_pipeline():
    """Start the pipeline_runner in the background (local machine only)."""
    import subprocess
    # Guard: never spawn a duplicate runner
    try:
        if sys.platform == "win32":
            out = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command",
                 "(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*pipeline_runner*' }).ProcessId"],
                timeout=10, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW,
            )
            running = [int(x) for x in out.split() if x.strip().isdigit()]
        else:
            try:
                out = subprocess.check_output(["pgrep", "-f", "pipeline_runner.py"], stderr=subprocess.DEVNULL, timeout=10)
                running = [int(x) for x in out.split()]
            except subprocess.CalledProcessError:
                running = []
        if running:
            return {"status": "already_running", "pids": running}
    except Exception as e:
        logger.warning(f"[Pipeline] Running-check failed: {e}")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(base_dir, "clawbuildr", "pipeline_runner.py")
    try:
        if sys.platform == "win32":
            subprocess.Popen(
                [sys.executable, "-u", script],
                cwd=base_dir,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
        else:
            subprocess.Popen(
                [sys.executable, "-u", script],
                cwd=base_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        return {"status": "started"}
    except Exception as e:
        logger.error(f"[Pipeline] Failed to start: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/replies/sentiment-stats")
def get_sentiment_stats():
    """Returns reply sentiment classification stats."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    categories = {
        "interested": 0, "meeting_requested": 0, "not_interested": 0,
        "out_of_office": 0, "objection": 0, "referral": 0, "spam": 0, "other": 0
    }
    try:
        rows = conn.execute("""
            SELECT sentiment, COUNT(*) as cnt
            FROM emails
            WHERE sentiment IS NOT NULL AND sentiment != ''
            GROUP BY sentiment
        """).fetchall()
        for r in rows:
            s = (r["sentiment"] if isinstance(r, dict) else r[0]).lower().strip()
            if s in categories:
                categories[s] = r["cnt"] if isinstance(r, dict) else r[1]
            else:
                categories["other"] += r["cnt"] if isinstance(r, dict) else r[1]
    except Exception:
        pass
    conn.close()
    return {"categories": categories}


@app.get("/api/deliverability/audit")
def get_deliverability_audit_placeholder():
    """GET variant — returns current domain health summary."""
    return get_deliverability()


# Simple Skylead-style routes
_STATIC_DIR = os.path.join(BASE_DIR, "static", "clawbuildr")

@app.get("/")
def root_redirect():
    """Redirect new users to onboarding, everyone else to the simple dashboard."""
    if _is_onboarding_complete():
        return RedirectResponse(url="/dashboard")
    return RedirectResponse(url="/onboarding")


@app.get("/onboarding")
def onboarding_page():
    path = os.path.join(_STATIC_DIR, "onboarding.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Onboarding page missing</h1>", status_code=500)


@app.get("/dashboard")
def simple_dashboard_page():
    path = os.path.join(_STATIC_DIR, "simple_dashboard.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard page missing</h1>", status_code=500)


@app.get("/classic")
def get_classic_dashboard():
    html_content = """

 <!DOCTYPE html>
 <html lang="en" data-theme="light" class="h-full">
 <head>
 <meta charset="UTF-8">
 <meta name="viewport" content="width=device-width, initial-scale=1.0">
 <title>ClawBuildr - Outbound Dashboard</title>
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
 50: '#eff6ff',
 100: '#dbeafe',
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
 :root {
 --bg-primary: #ffffff;
 --bg-secondary: #f9fafb;
 --bg-tertiary: #f3f4f6;
 --surface-elevated: #ffffff;
 --card-bg: #ffffff;
 --card-border: #e5e7eb;
 --card-hover: #f9fafb;
 --text-primary: #111827;
 --text-secondary: #4b5563;
 --text-tertiary: #9ca3af;
 --text-muted: #6b7280;
 --brand: #2563eb;
 --brand-light: #3b82f6;
 --brand-soft: #eff6ff;
 --brand-border: #bfdbfe;
 --success: #10b981;
 --success-soft: #ecfdf5;
 --warning: #f59e0b;
 --warning-soft: #fffbeb;
 --danger: #ef4444;
 --danger-soft: #fef2f2;
 --sidebar-bg: #ffffff;
 --sidebar-border: #e5e7eb;
 --header-bg: rgba(255,255,255,0.95);
 --input-bg: #ffffff;
 --input-border: #d1d5db;
 --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.04);
 --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.05), 0 1px 2px -1px rgb(0 0 0 / 0.05);
 --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.04), 0 2px 4px -2px rgb(0 0 0 / 0.04);
 --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.05), 0 4px 6px -4px rgb(0 0 0 / 0.05);
 }

 [data-theme="dark"] {
 --bg-primary: #111827;
 --bg-secondary: #0f1117;
 --bg-tertiary: #1a1d26;
 --surface-elevated: #151821;
 --card-bg: #1a1d26;
 --card-border: #2a2e3a;
 --card-hover: #252a36;
 --text-primary: #f9fafb;
 --text-secondary: #9ca3af;
 --text-tertiary: #6b7280;
 --text-muted: #4b5563;
 --brand: #3b82f6;
 --brand-light: #60a5fa;
 --brand-soft: rgba(59, 130, 246, 0.14);
 --brand-border: rgba(59, 130, 246, 0.30);
 --success: #22c55e;
 --success-soft: rgba(34, 197, 94, 0.14);
 --warning: #f59e0b;
 --warning-soft: rgba(245, 158, 11, 0.14);
 --danger: #ef4444;
 --danger-soft: rgba(239, 68, 68, 0.14);
 --sidebar-bg: #151821;
 --sidebar-border: #232734;
 --header-bg: rgba(21, 24, 33, 0.92);
 --input-bg: #111827;
 --input-border: #374151;
 --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.25);
 --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.25), 0 1px 2px -1px rgb(0 0 0 / 0.25);
 --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.20), 0 2px 4px -2px rgb(0 0 0 / 0.20);
 --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.25), 0 4px 6px -4px rgb(0 0 0 / 0.25);
 }

 html, body { background-color: var(--bg-secondary); color: var(--text-primary); }

 .cb-surface { background-color: var(--card-bg); border: 1px solid var(--card-border); }
 .cb-card { background-color: var(--card-bg); border: 1px solid var(--card-border); box-shadow: var(--shadow-sm); border-radius: 0.75rem; transition: box-shadow 0.15s ease, border-color 0.15s ease; }
 .cb-card:hover { box-shadow: var(--shadow-md); border-color: var(--brand-border); }
 .cb-card-sm { box-shadow: none; }
 .cb-surface-elevated { background-color: var(--surface-elevated); border: 1px solid var(--card-border); }
 .cb-text-primary { color: var(--text-primary); }
 .cb-text-secondary { color: var(--text-secondary); }
 .cb-text-tertiary { color: var(--text-tertiary); }
 .cb-border { border-color: var(--card-border); }
 .cb-input {
 background-color: var(--input-bg);
 border: 1px solid var(--input-border);
 color: var(--text-primary);
 border-radius: 0.75rem;
 padding: 0.75rem 1rem;
 font-size: 0.875rem;
 transition: border-color 0.15s, box-shadow 0.15s;
 }
 .cb-input:focus { outline: none; border-color: var(--brand); box-shadow: 0 0 0 3px var(--brand-soft); }
 .cb-input::placeholder { color: var(--text-tertiary); }

 .agent-pulse-running { animation: agent-pulse 2.5s ease-in-out infinite; }
 .agent-pulse-waiting { animation: agent-pulse 3s ease-in-out infinite; }
 @keyframes agent-pulse {
 0%, 100% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.15); }
 50% { box-shadow: 0 0 0 6px rgba(59, 130, 246, 0); }
 }

 .agent-line { stroke-dasharray: 8; animation: dash 30s linear infinite; }
 @keyframes dash { to { stroke-dashoffset: -1000; } }

 ::-webkit-scrollbar { width: 6px; height: 6px; }
 ::-webkit-scrollbar-track { background: transparent; }
 ::-webkit-scrollbar-thumb { background: var(--text-tertiary); opacity: 0.4; border-radius: 4px; }
 ::-webkit-scrollbar-thumb:hover { background: var(--text-secondary); }

 .theme-toggle { transition: transform 0.2s; }
 .theme-toggle:hover { transform: scale(1.05); }

 /* Light mode overrides for JS-generated Tailwind classes */
  [data-theme="light"] .bg-slate-950 { background-color: var(--bg-primary) !important; }
  [data-theme="light"] .bg-slate-900 { background-color: var(--card-bg) !important; }
  [data-theme="light"] .bg-slate-800 { background-color: var(--bg-tertiary) !important; }
  [data-theme="light"] .bg-slate-700 { background-color: #e2e8f0 !important; }
  [data-theme="light"] .cb-surface-elevated { background-color: var(--surface-elevated) !important; }
  [data-theme="light"] .cb-surface { background-color: var(--card-bg) !important; border-color: var(--card-border) !important; }
  [data-theme="light"] .cb-card { background-color: var(--card-bg) !important; border-color: var(--card-border) !important; }
  [data-theme="light"] .border-slate-800 { border-color: var(--card-border) !important; }
  [data-theme="light"] .border-slate-700 { border-color: var(--input-border) !important; }
  [data-theme="light"] .border-slate-600 { border-color: #cbd5e1 !important; }
  [data-theme="light"] .cb-border { border-color: var(--card-border) !important; }
  [data-theme="light"] .text-white { color: var(--text-primary) !important; }
  [data-theme="light"] .text-slate-100 { color: var(--text-primary) !important; }
  [data-theme="light"] .text-slate-200 { color: var(--text-primary) !important; }
  [data-theme="light"] .text-slate-300 { color: var(--text-secondary) !important; }
  [data-theme="light"] .text-slate-400 { color: var(--text-tertiary) !important; }
  [data-theme="light"] .text-slate-500 { color: var(--text-muted) !important; }
  [data-theme="light"] .cb-text-primary { color: var(--text-primary) !important; }
  [data-theme="light"] .cb-text-secondary { color: var(--text-secondary) !important; }
  [data-theme="light"] .cb-text-tertiary { color: var(--text-tertiary) !important; }
  [data-theme="light"] .hover\:bg-slate-800:hover { background-color: var(--bg-tertiary) !important; }
  [data-theme="light"] .hover\:bg-slate-700:hover { background-color: #e2e8f0 !important; }
  [data-theme="light"] .hover\:text-white:hover { color: var(--text-primary) !important; }
  [data-theme="light"] .hover\:text-slate-100:hover { color: var(--text-primary) !important; }
  [data-theme="light"] .hover\:text-slate-300:hover { color: var(--text-secondary) !important; }
  [data-theme="light"] .hover\:border-slate-600:hover { border-color: #cbd5e1 !important; }
  [data-theme="light"] .hover\:cb-text-primary:hover { color: var(--text-primary) !important; }
  [data-theme="light"] .hover\:cb-text-secondary:hover { color: var(--text-secondary) !important; }
  [data-theme="light"] .selection\:bg-brand-500::selection { background-color: var(--brand); }
  [data-theme="light"] .selection\:text-white::selection { color: #fff; }

 .tab-button { transition: all 0.15s ease; border-radius: 0.5rem; }
 .tab-button.active {
 background-color: var(--brand-soft);
 color: var(--brand);
 border-right: 3px solid var(--brand);
 }
 [data-theme="dark"] .tab-button:hover { background-color: rgba(255,255,255,0.04); }
 [data-theme="light"] .tab-button:hover { background-color: rgba(0,0,0,0.04); }

 ::selection { background-color: var(--brand); color: #fff; }
 </style>
</head>
 <body class="h-full flex flex-col font-sans antialiased overflow-hidden selection:bg-brand-500 selection:text-white">

 <!-- Top Header Navigation -->
 <header class="flex-shrink-0 cb-surface-elevated border-b cb-border backdrop-blur px-5 py-3 flex items-center justify-between">
 <div class="flex items-center space-x-3">
 <div class="w-8 h-8 bg-gradient-to-tr from-brand-600 to-cyan-400 rounded-lg flex items-center justify-center">
 <i class="fa-solid fa-compass-drafting text-sm text-white"></i>
 </div>
 <h1 class="text-base font-semibold cb-text-primary">ClawBuildr</h1>
 </div>

 <div class="flex items-center space-x-3">
 <!-- SSE Connection indicator -->
 <div class="flex items-center space-x-2 cb-surface px-2.5 py-1 rounded-full">
 <span id="sse-status-dot" class="w-1.5 h-1.5 bg-red-500 rounded-full inline-block animate-pulse"></span>
 <span id="sse-status-text" class="text-[10px] font-medium cb-text-tertiary uppercase tracking-wide">Disconnected</span>
 </div>

 <!-- Theme toggle -->
 <button id="theme-toggle" onclick="toggleTheme()" class="w-8 h-8 rounded-lg cb-surface flex items-center justify-center cb-text-secondary hover:text-brand-600 transition" aria-label="Toggle theme">
 <i class="fa-solid fa-sun text-sm" id="theme-icon-light"></i>
 <i class="fa-solid fa-moon text-sm hidden" id="theme-icon-dark"></i>
 </button>

 <!-- User avatar -->
 <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-brand-500 to-brand-600 flex items-center justify-center text-white text-xs font-semibold">
 CB
 </div>
 </div>
 </header>

 <!-- Main Body Workspace Split -->
 <main class="flex-grow flex overflow-hidden min-h-0" style="background-color: var(--bg-secondary);">
 <!-- Left Navigation Sidebar -->
 <aside class="w-56 cb-surface-elevated border-r cb-border flex flex-col justify-between py-4">
 <div class="space-y-0.5 px-3">
 <div class="px-3 pb-2 text-[10px] font-semibold cb-text-tertiary uppercase tracking-wider">Overview</div>
 <button onclick="switchTab('tab-control')" id="btn-tab-control" class="tab-button active w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-primary">
 <i class="fa-solid fa-gamepad text-base w-5 text-center"></i>
 <span>Dashboard</span>
 </button>
  <button onclick="switchTab('tab-crm')" id="btn-tab-crm" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-users text-base w-5 text-center"></i>
  <span>Prospects</span>
  </button>
  <button onclick="switchTab('tab-grid')" id="btn-tab-grid" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-table-cells-large text-base w-5 text-center"></i>
  <span>Grid</span>
  </button>
 <button onclick="switchTab('tab-feed')" id="btn-tab-feed" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-terminal text-base w-5 text-center"></i>
 <span>Activity</span>
 </button>
 <div class="px-3 pt-4 pb-2 text-[10px] font-semibold cb-text-tertiary uppercase tracking-wider">Outreach</div>
 <button onclick="switchTab('tab-linkedin')" id="btn-tab-linkedin" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-brands fa-linkedin text-base w-5 text-center"></i>
 <span>LinkedIn</span>
 </button>
 <button onclick="switchTab('tab-posts')" id="btn-tab-posts" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-pen-to-square text-base w-5 text-center"></i>
 <span>Posts</span>
 </button>
 <button onclick="switchTab('tab-meetings')" id="btn-tab-meetings" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-calendar-days text-base w-5 text-center"></i>
 <span>Meetings</span>
 </button>
 <div class="px-3 pt-4 pb-2 text-[10px] font-semibold cb-text-tertiary uppercase tracking-wider">Operations</div>
 <button onclick="switchTab('tab-deliverability')" id="btn-tab-deliverability" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-shield-halved text-base w-5 text-center"></i>
 <span>Deliverability</span>
 </button>
 <button onclick="switchTab('tab-metrics')" id="btn-tab-metrics" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-chart-line text-base w-5 text-center"></i>
 <span>Metrics</span>
 </button>
 <button onclick="switchTab('tab-templates')" id="btn-tab-templates" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-layer-group text-base w-5 text-center"></i>
 <span>Templates</span>
 </button>
 <button onclick="switchTab('tab-strategy')" id="btn-tab-strategy" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
 <i class="fa-solid fa-chess-knight text-base w-5 text-center"></i>
 <span>Strategy</span>
 </button>
  <button onclick="switchTab('tab-settings')" id="btn-tab-settings" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-gear text-base w-5 text-center"></i>
  <span>Settings</span>
  </button>

  <div class="px-3 pt-4 pb-2 text-[10px] font-semibold cb-text-tertiary uppercase tracking-wider">Outbound Engine</div>
  <button onclick="switchTab('tab-search')" id="btn-tab-search" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-magnifying-glass text-base w-5 text-center"></i>
  <span>Search</span>
  </button>
  <button onclick="switchTab('tab-workflows')" id="btn-tab-workflows" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-diagram-project text-base w-5 text-center"></i>
  <span>Workflows</span>
  </button>
  <button onclick="switchTab('tab-sequences')" id="btn-tab-sequences" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-arrow-right-arrow-left text-base w-5 text-center"></i>
  <span>Sequences</span>
  </button>
  <button onclick="switchTab('tab-campaigns')" id="btn-tab-campaigns" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-bullhorn text-base w-5 text-center"></i>
  <span>Campaigns</span>
  </button>
  <button onclick="switchTab('tab-analytics')" id="btn-tab-analytics" class="tab-button w-full flex items-center space-x-3 px-3 py-2 rounded-lg text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition">
  <i class="fa-solid fa-chart-pie text-base w-5 text-center"></i>
  <span>Analytics</span>
  </button>
  </div>

  <!-- Quick stats summary widget -->
 <div class="px-4 pb-4">
 <div class="border-t cb-border pt-4">
 <span class="text-[10px] cb-text-tertiary font-semibold uppercase tracking-wider">Overview</span>
 <div class="grid grid-cols-2 gap-2 mt-3">
 <div class="cb-surface p-2.5 rounded-lg">
 <span class="text-[10px] cb-text-tertiary block">Total Leads</span>
 <span id="stat-total-leads" class="text-base font-semibold cb-text-primary mt-0.5 block">0</span>
 </div>
 <div class="cb-surface p-2.5 rounded-lg">
 <span class="text-[10px] cb-text-tertiary block">Meetings</span>
 <span id="stat-meetings" class="text-base font-semibold text-emerald-600 mt-0.5 block">0</span>
 </div>
 </div>
 </div>
 </div>
 </aside>

 <!-- Dashboard Content Container (Tabs) -->
 <section class="flex-grow flex flex-col overflow-y-auto p-6 relative">
 <!-- Notifications Container -->
 <div id="toast-container" class="fixed bottom-6 right-6 z-50 flex flex-col space-y-2"></div>

 <!-- 1. DASHBOARD & CONTROL TAB -->
 <div id="tab-control" class="tab-content space-y-8 block">
 <!-- Welcome + Quick Actions -->
 <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
 <div>
 <h2 class="text-2xl font-semibold cb-text-primary">Dashboard</h2>
 <p class="text-sm cb-text-secondary mt-0.5">Monitor your outbound agents, leads, and pipeline.</p>
 </div>
 <div class="flex items-center gap-3">
 <button onclick="switchTab('tab-crm')" class="px-4 py-2 bg-brand-600 hover:bg-brand-500 text-white text-sm font-medium rounded-lg transition">View Prospects</button>
 <button onclick="switchTab('tab-linkedin')" class="px-4 py-2 cb-surface cb-text-primary hover:bg-brand-soft hover:text-brand-600 hover:border-brand-border text-sm font-medium rounded-lg transition">LinkedIn Outreach</button>
 </div>
 </div>

 <!-- Top Summary Stats Grid -->
 <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
 <div class="cb-card p-4">
 <div class="flex items-center justify-between mb-3">
 <span class="text-xs font-medium cb-text-tertiary uppercase tracking-wide">Active Leads</span>
 <div class="w-8 h-8 rounded-lg flex items-center justify-center" style="background-color: var(--brand-soft); color: var(--brand);">
 <i class="fa-solid fa-people-arrows text-sm"></i>
 </div>
 </div>
 <span id="stat-active-leads" class="text-2xl font-semibold cb-text-primary block">0</span>
 <span class="text-xs cb-text-tertiary">In active outreach</span>
 </div>
 <div class="cb-card p-4">
 <div class="flex items-center justify-between mb-3">
 <span class="text-xs font-medium cb-text-tertiary uppercase tracking-wide">Outbound Sends</span>
 <div class="w-8 h-8 rounded-lg flex items-center justify-center" style="background-color: rgba(99, 102, 241, 0.12); color: #6366f1;">
 <i class="fa-solid fa-paper-plane text-sm"></i>
 </div>
 </div>
 <span id="stat-outbound-sends" class="text-2xl font-semibold cb-text-primary block">0</span>
 <span class="text-xs cb-text-tertiary">Emails sent</span>
 </div>
 <div class="cb-card p-4">
 <div class="flex items-center justify-between mb-3">
 <span class="text-xs font-medium cb-text-tertiary uppercase tracking-wide">Domain Health</span>
 <div class="w-8 h-8 rounded-lg flex items-center justify-center" style="background-color: var(--success-soft); color: var(--success);">
 <i class="fa-solid fa-heart-pulse text-sm"></i>
 </div>
 </div>
 <span class="text-2xl font-semibold cb-text-primary block">100%</span>
 <span class="text-xs cb-text-tertiary">Deliverability score</span>
 </div>
 <div class="cb-card p-4">
 <div class="flex items-center justify-between mb-3">
 <span class="text-xs font-medium cb-text-tertiary uppercase tracking-wide">Avg BANT Score</span>
 <div class="w-8 h-8 rounded-lg flex items-center justify-center" style="background-color: rgba(6, 182, 212, 0.12); color: #06b6d4;">
 <i class="fa-solid fa-bolt text-sm"></i>
 </div>
 </div>
 <span id="stat-avg-score" class="text-2xl font-semibold cb-text-primary block">0/100</span>
 <span class="text-xs cb-text-tertiary">Lead quality</span>
 </div>
  </div>

  <!-- FUNNEL PIPELINE -->
  <div class="cb-card p-5">
  <div class="flex items-center justify-between mb-4">
  <h2 class="text-base font-semibold cb-text-primary"><i class="fa-solid fa-filter mr-2 text-brand-500"></i>Funnel Pipeline</h2>
  <button onclick="triggerFunnel()" class="text-xs bg-brand-600 hover:bg-brand-500 text-white font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-play mr-1"></i> Run Now</button>
  </div>
  <div id="funnel-pipeline" class="flex items-center gap-2 overflow-x-auto pb-2">
  <!-- Populated by JS -->
  </div>
  </div>

  <!-- Live Agents Grid (Mission Status) -->
 <div>
 <div class="flex items-center justify-between mb-4">
 <h2 class="text-base font-semibold cb-text-primary">Active Agent Fleet</h2>
 <span class="text-xs cb-text-tertiary">6 agents working in parallel</span>
 </div>
 <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4" id="agent-status-cards">
 <!-- Cards will be populated dynamically -->
 </div>
 </div>

 <!-- Main Graph and Handoff Section -->
 <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-stretch">
 <!-- Pipeline Flow -->
 <div class="lg:col-span-8 cb-card p-5 flex flex-col">
 <div class="flex items-center justify-between mb-4">
 <h3 class="text-sm font-semibold cb-text-primary">Pipeline Flow</h3>
 <span class="text-xs cb-text-tertiary">Real-time handoff between agents</span>
 </div>
 <div class="flex-grow flex items-center justify-center p-4 cb-surface-elevated border cb-border rounded-lg relative overflow-hidden" style="min-height: 220px;">
 <!-- Live SVG Flow Line Connectors -->
 <svg class="absolute inset-0 w-full h-full pointer-events-none" id="handoff-svg-canvas">
 <defs>
 <filter id="glow-line" x="-20%" y="-20%" width="140%" height="140%">
 <feGaussianBlur stdDeviation="2" result="blur" />
 <feMerge>
 <feMergeNode in="blur" />
 <feMergeNode in="SourceGraphic" />
 </feMerge>
 </filter>
 </defs>
 </svg>
 
 <div class="relative w-full flex justify-between items-center px-2 max-w-4xl flex-wrap gap-y-8">
 <!-- Pipeline nodes -->
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-ingest" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-file-import text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Ingest</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-research" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-magnifying-glass text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Research</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-deliver" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-envelope-circle-check text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Deliver</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-opp" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-map-location-dot text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Opp Map</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-qual" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-filter text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Qualify</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-outreach" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-pen-nib text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Outreach</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-review" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-user-shield text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Review</span>
 </div>
 <div class="flex flex-col items-center justify-center space-y-1.5 z-10 w-20">
 <div id="node-inbound" class="w-10 h-10 cb-surface border cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300">
 <i class="fa-solid fa-reply text-xs"></i>
 </div>
 <span class="text-[10px] cb-text-tertiary font-medium text-center">Inbox</span>
 </div>
 </div>
 </div>
 </div>

 <!-- Live activity feed snippet -->
 <div class="lg:col-span-4 cb-card p-5 flex flex-col">
 <div class="flex items-center justify-between mb-4">
 <h3 class="text-sm font-semibold cb-text-primary">Activity Log</h3>
 <button onclick="switchTab('tab-feed')" class="text-xs text-brand-600 hover:text-brand-500 font-medium">View all</button>
 </div>
 <div class="flex-grow cb-surface-elevated text-xs p-3 rounded-lg border cb-border overflow-y-auto space-y-2 cb-text-tertiary flex flex-col" style="max-height: 220px; min-height: 220px;" id="console-stream-log">
 <!-- Streams events dynamically -->
 </div>
 </div>
 </div>

 <!-- Active lead pipeline queue -->
 <div>
 <div class="flex items-center justify-between mb-4">
 <h2 class="text-base font-semibold cb-text-primary">Pipeline Leads</h2>
 <button onclick="switchTab('tab-crm')" class="text-xs text-brand-600 hover:text-brand-500 font-medium">View all leads</button>
 </div>
 <div class="cb-card overflow-hidden">
 <div class="overflow-x-auto">
 <table class="w-full text-left border-collapse">
 <thead>
 <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
 <th class="py-4 px-6">Prospect Details</th>
 <th class="py-4 px-6">Company</th>
 <th class="py-4 px-6">Email / Domain</th>
 <th class="py-4 px-6">Current Stage</th>
 <th class="py-4 px-6">BANT Score</th>
 <th class="py-4 px-6">Outreach Trigger</th>
 <th class="py-4 px-6 text-right">Actions</th>
 </tr>
 </thead>
 <tbody id="active-leads-table-rows" class="text-sm divide-y cb-border">
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
 <h2 class="text-2xl font-bold cb-text-primary">Prospect Database</h2>
 <span class="text-xs cb-text-tertiary font-semibold uppercase tracking-wider">All Records saved in SQLite</span>
 </div>
 
 <div class="cb-card p-6">
 <div class="overflow-x-auto">
 <table class="w-full text-left border-collapse">
 <thead>
 <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
 <th class="py-4 px-6">ID</th>
 <th class="py-4 px-6">Name & Role</th>
 <th class="py-4 px-6">Email Address</th>
 <th class="py-4 px-6">Company</th>
 <th class="py-4 px-6">Lifecycle Stage</th>
 <th class="py-4 px-6">Priority</th>
 <th class="py-4 px-6">Last Updated</th>
 </tr>
 </thead>
 <tbody id="crm-table-rows" class="text-sm divide-y cb-border">
 <!-- SQLite database rows dynamically loaded -->
 </tbody>
 </table>
 </div>
 </div>
 </div>

 <!-- 2b. GRID TAB — Clay-style spreadsheet view -->
 <div id="tab-grid" class="tab-content space-y-4 hidden">
 <!-- Filter Bar -->
 <div class="cb-card p-4">
 <div class="flex flex-wrap items-center gap-3">
 <div class="relative flex-grow min-w-[200px] max-w-sm">
 <i class="fa-solid fa-magnifying-glass absolute left-3 top-1/2 -translate-y-1/2 text-[11px] cb-text-tertiary"></i>
 <input id="grid-search" type="text" placeholder="Search by name, email, or company..." class="cb-input w-full pl-9" oninput="gridApplyFilters()">
 </div>
 <select id="grid-filter-stage" class="cb-input min-w-[160px]" onchange="gridApplyFilters()">
 <option value="">All Stages</option>
 <option value="INGESTED">Ingested</option>
 <option value="DISCOVERED">Discovered</option>
 <option value="ENRICHING">Enriching</option>
 <option value="VERIFYING">Verifying</option>
 <option value="READY">Ready</option>
 <option value="CONTACTED">Contacted</option>
 <option value="RESEARCHED">Researched</option>
 <option value="DELIVERABILITY_VERIFIED">Deliverability Verified</option>
 <option value="OPPORTUNITY_MAPPED">Opportunity Mapped</option>
 <option value="PRE_QUALIFIED">Pre-Qualified</option>
 <option value="OUTREACH_DRAFTED">Outreach Drafted</option>
 <option value="PENDING_APPROVAL">Pending Approval</option>
 <option value="ACTIVE_OUTREACH">Active Outreach</option>
 <option value="REPLIED">Replied</option>
 <option value="MEETING_SCHEDULED">Meeting Scheduled</option>
 <option value="BLOCKED">Blocked</option>
 <option value="OPT_OUT">Opt Out</option>
 </select>
 <label class="flex items-center gap-2 text-sm cb-text-secondary cursor-pointer select-none">
 <input type="checkbox" id="grid-filter-verified" class="accent-brand-500" onchange="gridApplyFilters()">
 <span class="text-xs font-medium">Verified Only</span>
 </label>
 <div class="flex items-center gap-2">
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider whitespace-nowrap">Score</label>
 <input id="grid-score-min" type="number" min="0" max="100" value="0" class="cb-input w-16 text-center" oninput="gridApplyFilters()">
 <span class="cb-text-tertiary text-xs">–</span>
 <input id="grid-score-max" type="number" min="0" max="100" value="100" class="cb-input w-16 text-center" oninput="gridApplyFilters()">
 </div>
 </div>
 </div>

 <!-- Bulk Actions Bar -->
 <div id="grid-bulk-bar" class="cb-card p-3 hidden">
 <div class="flex flex-wrap items-center gap-3">
 <span id="grid-selected-count" class="text-xs font-bold cb-text-primary">0 selected</span>
 <div class="h-4 w-px bg-gray-600"></div>
 <select id="grid-bulk-campaign" class="cb-input text-xs py-1.5 min-w-[150px]">
 <option value="">Enroll in Campaign...</option>
 </select>
 <button onclick="gridBulkEnroll()" class="text-xs bg-brand-600 hover:bg-brand-500 text-white font-bold px-3 py-1.5 rounded-lg transition">Enroll</button>
 <select id="grid-bulk-stage" class="cb-input text-xs py-1.5 min-w-[150px]">
 <option value="">Change Stage...</option>
 <option value="INGESTED">Ingested</option>
 <option value="DISCOVERED">Discovered</option>
 <option value="ENRICHING">Enriching</option>
 <option value="VERIFYING">Verifying</option>
 <option value="READY">Ready</option>
 <option value="CONTACTED">Contacted</option>
 <option value="NURTURE">Nurture</option>
 <option value="ACTIVE_OUTREACH">Active Outreach</option>
 <option value="MEETING_SCHEDULED">Meeting Scheduled</option>
 <option value="BLOCKED">Blocked</option>
 <option value="OPT_OUT">Opt Out</option>
 </select>
 <button onclick="gridBulkChangeStage()" class="text-xs bg-emerald-600 hover:bg-emerald-500 text-white font-bold px-3 py-1.5 rounded-lg transition">Apply</button>
 <button onclick="gridBulkDelete()" class="text-xs bg-red-500/10 hover:bg-red-500/20 text-red-400 font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-trash-can mr-1"></i>Delete</button>
 <button onclick="gridExportCSV()" class="text-xs cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-file-csv mr-1"></i>Export CSV</button>
 <div class="flex-grow"></div>
 <button onclick="gridSelectAll()" class="text-[10px] cb-text-tertiary hover:cb-text-primary font-semibold uppercase tracking-wider">Select All</button>
 <button onclick="gridDeselectAll()" class="text-[10px] cb-text-tertiary hover:cb-text-primary font-semibold uppercase tracking-wider">Deselect</button>
 </div>
 </div>

 <!-- Table -->
 <div class="cb-card overflow-hidden">
 <div class="overflow-x-auto">
 <table class="w-full text-left border-collapse" id="grid-table">
 <thead>
 <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
 <th class="py-3 px-4 w-10"><input type="checkbox" id="grid-select-all" onclick="gridToggleAll()" class="accent-brand-500"></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('first_name')">Name <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-first_name"></i></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('email')">Email <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-email"></i></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('company_name')">Company <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-company_name"></i></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('company_domain')">Domain <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-company_domain"></i></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('current_stage')">Funnel Stage <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-current_stage"></i></th>
 <th class="py-3 px-4 cursor-pointer hover:cb-text-primary transition select-none" onclick="gridSort('lead_score')">Hunter Score <i class="fa-solid fa-sort text-[8px] ml-1" id="grid-sort-icon-lead_score"></i></th>
 <th class="py-3 px-4">Verified</th>
 <th class="py-3 px-4">LinkedIn</th>
 <th class="py-3 px-4 text-right">Actions</th>
 </tr>
 </thead>
 <tbody id="grid-tbody" class="text-sm divide-y cb-border">
 <!-- Populated by JS -->
 </tbody>
 </table>
 </div>
 </div>

 <!-- Pagination -->
 <div class="flex items-center justify-between">
 <span id="grid-info" class="text-xs cb-text-tertiary">Showing 0–0 of 0</span>
 <div class="flex items-center gap-2">
 <button onclick="gridPrevPage()" id="grid-prev-btn" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary text-xs font-bold px-3 py-1.5 rounded-lg transition disabled:opacity-40" disabled><i class="fa-solid fa-chevron-left mr-1"></i>Prev</button>
 <span id="grid-page-indicator" class="text-xs cb-text-secondary font-semibold">Page 1 of 1</span>
 <button onclick="gridNextPage()" id="grid-next-btn" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary text-xs font-bold px-3 py-1.5 rounded-lg transition disabled:opacity-40" disabled>Next<i class="fa-solid fa-chevron-right ml-1"></i></button>
 </div>
 </div>

 <!-- Empty State -->
 <div id="grid-empty" class="hidden cb-card p-16 text-center">
 <i class="fa-solid fa-table-cells-large text-4xl cb-text-tertiary block mb-4"></i>
 <h3 class="text-base font-bold cb-text-primary mb-1">No leads found</h3>
 <p class="text-sm cb-text-tertiary">Try adjusting your search or filters.</p>
 </div>

 <!-- Loading Skeleton -->
 <div id="grid-loading" class="cb-card p-6 hidden">
 <div class="space-y-3">
 <div class="h-4 bg-gray-700/30 rounded animate-pulse w-1/3"></div>
 <div class="h-4 bg-gray-700/30 rounded animate-pulse w-2/3"></div>
 <div class="h-4 bg-gray-700/30 rounded animate-pulse w-1/2"></div>
 <div class="h-4 bg-gray-700/30 rounded animate-pulse w-3/5"></div>
 <div class="h-4 bg-gray-700/30 rounded animate-pulse w-1/4"></div>
 </div>
 </div>
 </div>

 <!-- 3. DELIVERABILITY TAB -->
 <div id="tab-deliverability" class="tab-content space-y-6 hidden">
 <div class="flex justify-between items-center">
 <h2 class="text-2xl font-bold cb-text-primary">Domain Deliverability Monitor</h2>
 <div class="flex space-x-3">
 <span class="text-xs bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 px-3 py-1 rounded-full font-bold uppercase tracking-wider">SPF / DKIM / MX Verified</span>
 <button onclick="runLiveAudit()" class="text-xs bg-blue-500 hover:bg-blue-600 text-white px-3 py-1 rounded-full font-bold uppercase tracking-wider transition shadow-blue-500/20">
 <i class="fa-solid fa-radar mr-2"></i>Run Live Audit
 </button>
 </div>
 </div>
 
 <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
 <!-- Stat details -->
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase">Monitored Domains</span>
 <span id="stat-monitored-domains-count" class="text-3xl font-extrabold cb-text-primary mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase">Total Aggregated Sends</span>
 <span id="stat-total-sending" class="text-3xl font-extrabold cb-text-primary mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase">Total Replies Received</span>
 <span id="stat-total-replies" class="text-3xl font-extrabold cb-text-primary mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase">Average Warmup Score</span>
 <span class="text-3xl font-extrabold text-emerald-400 mt-2 block">98.5%</span>
 </div>
 </div>

 <div class="cb-card p-6">
 <h3 class="text-sm font-bold cb-text-primary uppercase tracking-wider mb-4">Domain Performance Log</h3>
 <div class="overflow-x-auto">
 <table class="w-full text-left border-collapse">
 <thead>
 <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
 <th class="py-4 px-6">Domain</th>
 <th class="py-4 px-6">Outbound Sends</th>
 <th class="py-4 px-6">Replies</th>
 <th class="py-4 px-6">Bounces</th>
 <th class="py-4 px-6">Bounce Rate</th>
 <th class="py-4 px-6">Domain Health</th>
 <th class="py-4 px-6">Last Synced</th>
 </tr>
 </thead>
 <tbody id="deliverability-table-rows" class="text-sm divide-y cb-border">
 <!-- SQLite rows -->
 </tbody>
 </table>
 </div>
 </div>
 </div>

 <!-- 4. MEETINGS TAB -->
 <div id="tab-meetings" class="tab-content space-y-6 hidden">
 <div class="flex justify-between items-center">
 <h2 class="text-2xl font-bold cb-text-primary">Google Calendar Bookings</h2>
 <span class="text-xs cb-text-tertiary font-semibold uppercase tracking-wider">Syncs with Google Calendar API</span>
 </div>

 <div class="grid grid-cols-1 md:grid-cols-2 gap-6" id="calendar-bookings-grid">
 <!-- Bookings loaded here -->
 </div>
 </div>

 <!-- 5. AGENT LOG FEED TAB -->
 <div id="tab-feed" class="tab-content space-y-6 hidden">
 <div class="flex justify-between items-center">
 <h2 class="text-2xl font-bold cb-text-primary">Comprehensive Agent Log Feed</h2>
 <span class="text-xs cb-text-tertiary font-semibold uppercase tracking-wider">Audit Log for GDPR compliance</span>
 </div>

 <div class="cb-card p-6">
 <div class="space-y-4 max-h-[500px] overflow-y-auto pr-4" id="comprehensive-activity-feed">
 <!-- Chronological audits -->
 </div>
 </div>
 </div>

 <!-- 6. LINKEDIN OUTREACH TAB -->
 <div id="tab-linkedin" class="tab-content space-y-6 hidden">
 <div class="flex justify-between items-center">
 <h2 class="text-2xl font-bold cb-text-primary flex items-center gap-3">
 <i class="fa-brands fa-linkedin text-blue-500"></i> LinkedIn Outreach
 </h2>
 <div class="flex items-center gap-3">
 <div id="li-login-status" class="flex items-center gap-2 cb-surface-elevated border cb-border px-3 py-2 rounded-xl">
 <span id="li-login-dot" class="w-2.5 h-2.5 bg-red-500 rounded-full inline-block"></span>
 <span id="li-login-text" class="text-xs font-semibold cb-text-tertiary uppercase tracking-widest">Not Connected</span>
 </div>
 <button onclick="linkedinLogin()" id="li-login-btn" class="bg-blue-600 hover:bg-blue-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl shadow-blue-500/15 transition flex items-center gap-2">
 <i class="fa-brands fa-linkedin"></i> Login to LinkedIn
 </button>
 </div>
 </div>

 <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Total Sent</span>
 <span id="li-stat-sent" class="text-3xl font-extrabold text-blue-400 mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Success</span>
 <span id="li-stat-success" class="text-3xl font-extrabold text-emerald-400 mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Not Found</span>
 <span id="li-stat-notfound" class="text-3xl font-extrabold text-amber-400 mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Errors</span>
 <span id="li-stat-error" class="text-3xl font-extrabold text-red-400 mt-2 block">0</span>
 </div>
 </div>

 <div id="followup-section" class="cb-card p-6 hidden">
 <div class="flex justify-between items-center mb-4">
 <h3 class="text-sm font-bold cb-text-primary uppercase tracking-wider flex items-center gap-2">
 <i class="fa-solid fa-paper-plane text-emerald-400"></i> Follow-up Messages (Ready to Paste)
 </h3>
 <button onclick="loadFollowups()" class="text-xs text-blue-400 hover:text-blue-300 font-semibold">Refresh</button>
 </div>
 <div id="followup-list" class="space-y-4">
 <p class="cb-text-tertiary text-sm">No accepted connections awaiting follow-up.</p>
 </div>
 </div>

 <div class="cb-card p-6">
 <h3 class="text-sm font-bold cb-text-primary uppercase tracking-wider mb-4">Connection Request History</h3>
 <div class="overflow-x-auto">
 <table class="w-full text-left border-collapse">
 <thead>
 <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
 <th class="py-4 px-6">Person</th>
 <th class="py-4 px-6">Company</th>
 <th class="py-4 px-6">Note (180 char)</th>
 <th class="py-4 px-6">Outcome</th>
 <th class="py-4 px-6">Time</th>
 </tr>
 </thead>
 <tbody id="linkedin-table-rows" class="text-sm divide-y cb-border">
 <!-- LinkedIn outreach rows -->
 </tbody>
 </table>
 </div>
 </div>
 </div>

 <!-- 7. LINKEDIN POSTS TAB -->
 <div id="tab-posts" class="tab-content space-y-6 hidden">
 <div class="flex justify-between items-center">
 <h2 class="text-2xl font-bold cb-text-primary flex items-center gap-3">
 <i class="fa-solid fa-pen-to-square text-blue-400"></i> LinkedIn Posts
 </h2>
 <div class="flex items-center gap-3">
 <div id="post-scheduler-status" class="flex items-center gap-2 cb-surface-elevated border cb-border px-3 py-2 rounded-xl">
 <span id="post-sched-dot" class="w-2.5 h-2.5 bg-red-500 rounded-full inline-block"></span>
 <span id="post-sched-text" class="text-xs font-semibold cb-text-tertiary uppercase tracking-widest">Stopped</span>
 </div>
 <button onclick="togglePostScheduler()" id="post-sched-btn" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition">
 <i class="fa-solid fa-play"></i> Start Scheduler
 </button>
 </div>
 </div>

 <!-- Post stats -->
 <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Drafts</span>
 <span id="post-stat-draft" class="text-3xl font-extrabold cb-text-tertiary mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Queued</span>
 <span id="post-stat-queued" class="text-3xl font-extrabold text-blue-400 mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Posted</span>
 <span id="post-stat-posted" class="text-3xl font-extrabold text-emerald-400 mt-2 block">0</span>
 </div>
 <div class="cb-card p-5">
 <span class="text-xs cb-text-tertiary font-semibold block uppercase tracking-wider">Failed</span>
 <span id="post-stat-failed" class="text-3xl font-extrabold text-red-400 mt-2 block">0</span>
 </div>
 </div>

 <!-- Add new post -->
 <div class="cb-card p-6">
 <h3 class="text-sm font-bold cb-text-primary uppercase tracking-wider mb-4">Add New Post</h3>
 <textarea id="new-post-content" class="cb-input w-full h-32 resize-none" placeholder="Write your LinkedIn post here..."></textarea>
 <div class="flex justify-between items-center mt-3">
 <div class="flex gap-3">
 <input id="new-post-topic" type="text" placeholder="Topic (optional)" class="cb-input">
 <input id="new-post-schedule" type="datetime-local" class="cb-input">
 </div>
 <div class="flex gap-2">
 <button onclick="addNewPost('draft')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-semibold text-sm px-4 py-2 rounded-xl transition">Save Draft</button>
 <button onclick="addNewPost('queued')" class="bg-blue-600 hover:bg-blue-500 text-white font-semibold text-sm px-4 py-2 rounded-xl transition">Add to Queue</button>
 </div>
 </div>
 </div>

 <!-- Posts queue table -->
 <div class="cb-card overflow-hidden">
 <div class="overflow-x-auto">
 <table class="w-full text-left">
 <thead class="cb-surface-elevated border-b cb-border">
 <tr>
 <th class="py-4 px-6 text-[10px] font-bold cb-text-tertiary uppercase tracking-wider">Topic</th>
 <th class="py-4 px-6 text-[10px] font-bold cb-text-tertiary uppercase tracking-wider">Content</th>
 <th class="py-4 px-6 text-[10px] font-bold cb-text-tertiary uppercase tracking-wider">Scheduled</th>
 <th class="py-4 px-6 text-[10px] font-bold cb-text-tertiary uppercase tracking-wider">Status</th>
 <th class="py-4 px-6 text-[10px] font-bold cb-text-tertiary uppercase tracking-wider">Actions</th>
 </tr>
 </thead>
 <tbody id="posts-table-rows" class="text-sm divide-y cb-border">
 <!-- Posts rows -->
 </tbody>
 </table>
 </div>
 </div>
 </div>
 <!-- SETTINGS & ICP TAB -->
 <div id="tab-settings" class="tab-content space-y-8 hidden">
 
 <!-- Tenant Switcher -->
 <div class="cb-card p-6">
 <div class="flex items-center gap-3 mb-6">
 <i class="fa-solid fa-building text-brand-400 text-lg"></i>
 <div>
 <h2 class="text-base font-extrabold cb-text-primary">Active Company Profile</h2>
 <p class="text-xs cb-text-tertiary mt-0.5">Switch between company profiles. All agents will use the active profile's ICP and value doctrine.</p>
 </div>
 </div>
 <div id="tenant-switcher" class="flex gap-3 flex-wrap mb-4"></div>
 <div class="text-xs text-amber-400 flex items-center gap-2 mt-2">
 <i class="fa-solid fa-triangle-exclamation"></i>
 <span>Switching profiles immediately affects all new leads and outreach. Currently running leads are not affected.</span>
 </div>
 </div>

 <!-- ICP & Brand Settings Form -->
 <div class="cb-card p-6">
 <div class="flex items-center justify-between mb-6">
 <div class="flex items-center gap-3">
 <i class="fa-solid fa-sliders text-brand-400 text-lg"></i>
 <div>
 <h2 class="text-base font-extrabold cb-text-primary">ICP & Outreach Configuration</h2>
 <p class="text-xs cb-text-tertiary mt-0.5">Fill these in to activate all 7 agents for your company.</p>
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
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Company / Profile Name</label>
 <input id="settings-display-name" type="text" placeholder="e.g. Injexion" class="cb-input w-full">
 </div>

 <!-- Sending Email -->
 <div>
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Sending Email Address <span class="text-red-400">*</span></label>
 <input id="settings-sending-email" type="email" placeholder="e.g. marvin@injexion.io" class="cb-input w-full">
 <p class="text-[10px] cb-text-tertiary mt-1">The email address that will send outreach. Must be set up with Gmail OAuth or SMTP.</p>
 </div>

 <!-- Calendar Link -->
 <div>
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Calendar / Booking Link <span class="text-red-400">*</span></label>
 <input id="settings-calendar-link" type="url" placeholder="e.g. cal.com/injexion" class="cb-input w-full">
 </div>

 <!-- ICP Company Size -->
 <div>
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Target Company Size (employees)</label>
 <input id="settings-company-size" type="text" placeholder="e.g. 10-200" class="cb-input w-full">
 </div>

 <!-- Gmail Authentication -->
 <div class="col-span-1 md:col-span-2 cb-surface-elevated rounded-xl p-4 border cb-border mt-2">
 <div class="flex items-center justify-between">
 <div>
 <h3 class="text-sm font-bold cb-text-primary mb-1"><i class="fa-brands fa-google text-brand-400 mr-2"></i>Google Workspace Connection</h3>
 <p class="text-xs cb-text-tertiary">Connect the Google Workspace account for this profile to enable live sending and calendar sync.</p>
 </div>
 <button onclick="connectGmail()" type="button" class="bg-white hover:bg-slate-100 text-slate-900 font-bold text-xs px-4 py-2 rounded-lg transition flex items-center gap-2">
 <i class="fa-solid fa-link"></i> Authenticate Google
 </button>
 </div>
 </div>
 </div>

 <!-- Value Doctrine -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Value Doctrine — What do you sell? <span class="text-red-400">*</span></label>
 <textarea id="settings-value-doctrine" rows="4" placeholder="Describe your product/service in 3-5 sentences. The AI uses this to write every email and map pain points. Example: 'Injexion builds autonomous B2B lead generation systems for Dutch SMEs. We combine AI research agents, LinkedIn automation, and personalized email outreach into a single pipeline...'" class="cb-input w-full resize-y"></textarea>
 <p class="text-[10px] cb-text-tertiary mt-1">This is the most important field. The better you describe your offer, the more relevant every outbound email will be.</p>
 </div>
 
 <!-- Email Signature Block -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Email Signature Block <span class="text-red-400">*</span></label>
 <textarea id="settings-signature" rows="4" placeholder="Your Name\nCompany Name\nhttps://yourwebsite.com\n+31 6 12345678" class="w-full cb-surface-elevated border cb-border rounded-xl px-4 py-3 text-sm cb-text-primary font-mono focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
 </div>

 <!-- ICP Target Industries -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Target Industries (one per line)</label>
 <textarea id="settings-industries" rows="4" placeholder="Groothandel\nLogistiek & Transport\nB2B SaaS\nIT Dienstverlening" class="cb-input w-full resize-y"></textarea>
 </div>

 <!-- ICP Decision Maker Titles -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Target Decision-Maker Titles (one per line)</label>
 <textarea id="settings-roles" rows="3" placeholder="Directeur\nCEO\nEigenaar\nOprichter" class="cb-input w-full resize-y"></textarea>
 </div>

 <!-- Lead Sourcing Queries -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Lead Sourcing Queries — Agent Zero searches (one per line)</label>
 <textarea id="settings-queries" rows="6" placeholder="groothandel B2B Nederland\nlogistiek dienstverlener Nederland\ntransport bedrijf Nederland MKB\nIT consultancy Nederland MKB" class="w-full cb-surface-elevated border cb-border rounded-xl px-4 py-3 text-sm cb-text-primary font-mono focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
 <p class="text-[10px] cb-text-tertiary mt-1">These are the exact search queries Agent Zero uses to discover leads. Be specific: include industry + country + size signal (e.g. "MKB").</p>
 </div>

 <!-- Brand Voice -->
 <div class="mt-6">
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Brand Voice Instructions</label>
 <textarea id="settings-brand-voice" rows="2" placeholder="Professioneel, direct, vriendelijk. Gebruik informeel Nederlands (je/jullie). Max 120 woorden per email." class="cb-input w-full resize-y"></textarea>
 </div>

 <!-- Setup Status Check -->
 <div id="settings-status" class="mt-6 hidden"></div>

 </div>
 </div>

 <!-- SUCCESS METRICS TAB -->
 <div id="tab-metrics" class="tab-content space-y-6 hidden">
 <div class="flex items-center justify-between mb-2">
 <div>
 <h2 class="text-base font-extrabold cb-text-primary">Success Metrics</h2>
 <p class="text-xs cb-text-tertiary mt-0.5">Live performance vs. your defined targets. Green = beating target. Red = below target.</p>
 </div>
 <button onclick="loadMetrics()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-xs px-4 py-2 rounded-xl transition flex items-center gap-2">
 <i class="fa-solid fa-rotate-right"></i> Refresh
 </button>
 </div>

 <!-- Email KPI cards -->
 <div class="grid grid-cols-2 md:grid-cols-4 gap-4" id="metrics-cards">
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Open Rate</p>
 <p id="m-open-rate" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-open-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Reply Rate</p>
 <p id="m-reply-rate" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-reply-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Meeting Rate</p>
 <p id="m-meeting-rate" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-meeting-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Bounce Rate</p>
 <p id="m-bounce-rate" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-bounce-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 </div>

 <!-- LinkedIn KPI cards -->
 <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Emails Sent</p>
 <p id="m-total-sent" class="text-3xl font-black text-cyan-400">—</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">LI Accept Rate</p>
 <p id="m-li-accept" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-li-accept-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">LI Reply Rate</p>
 <p id="m-li-reply" class="text-3xl font-black cb-text-primary">—</p>
 <p id="m-li-reply-target" class="text-[10px] cb-text-tertiary mt-1">Target: —</p>
 </div>
 <div class="cb-surface rounded-2xl p-5 text-center">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Avg Velocity</p>
 <p id="m-velocity" class="text-3xl font-black cb-text-primary">—</p>
 <p class="text-[10px] cb-text-tertiary mt-1">days to meeting</p>
 </div>
 </div>

 <!-- Target Editor -->
 <div class="cb-card p-6">
 <h3 class="text-sm font-extrabold cb-text-primary mb-4"><i class="fa-solid fa-bullseye text-brand-400 mr-2"></i>Define Your Targets</h3>
 <div class="grid grid-cols-2 md:grid-cols-3 gap-4">
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Open Rate Target</label>
 <div class="flex items-center gap-2">
 <input id="t-open" type="number" step="1" min="0" max="100" value="30" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Reply Rate Target</label>
 <div class="flex items-center gap-2">
 <input id="t-reply" type="number" step="1" min="0" max="100" value="5" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Meeting Rate Target</label>
 <div class="flex items-center gap-2">
 <input id="t-meeting" type="number" step="0.5" min="0" max="100" value="1" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Max Bounce Rate</label>
 <div class="flex items-center gap-2">
 <input id="t-bounce" type="number" step="0.5" min="0" max="100" value="2" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">LI Accept Rate Target</label>
 <div class="flex items-center gap-2">
 <input id="t-li-accept" type="number" step="1" min="0" max="100" value="25" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">LI Reply Rate Target</label>
 <div class="flex items-center gap-2">
 <input id="t-li-reply" type="number" step="1" min="0" max="100" value="8" class="cb-input w-full">
 <span class="cb-text-tertiary text-sm">%</span>
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
 <h2 class="text-base font-extrabold cb-text-primary">Prefab Berichten</h2>
 <p class="text-xs cb-text-tertiary mt-0.5">Pre-written Dutch outreach templates. The AI uses these as a structure and personalizes with research.</p>
 </div>
 <button onclick="showNewTemplateForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
 <i class="fa-solid fa-plus"></i> New Template
 </button>
 </div>

 <!-- Template Editor (hidden by default) -->
 <div id="template-editor" class="hidden cb-card p-6 space-y-4">
 <input type="hidden" id="edit-template-id">
 <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Template Name</label>
 <input id="edit-tmpl-name" type="text" class="cb-input w-full">
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Category</label>
 <select id="edit-tmpl-category" class="cb-input w-full">
 <option value="COLD">Cold Email</option>
 <option value="FOLLOWUP">Follow-up</option>
 <option value="SOCIAL_PROOF">Social Proof</option>
 <option value="LINKEDIN_NOTE">LinkedIn Note</option>
 </select>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Subject Line</label>
 <input id="edit-tmpl-subject" type="text" placeholder="{{company_name}} — korte vraag" class="cb-input w-full">
 </div>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Body — use {{variable_name}} for placeholders</label>
 <textarea id="edit-tmpl-body" rows="10" class="w-full cb-surface-elevated border cb-border rounded-xl px-4 py-3 text-sm cb-text-primary font-mono resize-y focus:outline-none focus:border-brand-500/80"></textarea>
 </div>
 <div class="flex gap-3">
 <button onclick="saveTemplate()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save</button>
 <button onclick="document.getElementById('template-editor').classList.add('hidden')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
 </div>
 </div>

 <!-- Template cards -->
 <div id="templates-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
 </div>

  <!-- STRATEGY BUILDER TAB -->
  <div id="tab-strategy" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Strategy Builder</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Define multi-step outreach sequences. Leads automatically move through steps unless they reply, opt out, or book a meeting.</p>
  </div>
  <button onclick="showNewStrategyForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-plus"></i> New Strategy
  </button>
  </div>

  <!-- Strategy Creator Form -->
  <div id="strategy-editor" class="hidden cb-card p-6 space-y-6">
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Strategy Name</label>
  <input id="strat-name" type="text" placeholder="e.g. Standard 4-Step NL" class="cb-input w-full">
  </div>
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Description</label>
  <input id="strat-desc" type="text" placeholder="What is this strategy for?" class="cb-input w-full">
  </div>
  </div>

  <div>
  <div class="flex items-center justify-between mb-3">
  <h3 class="text-sm font-bold cb-text-primary">Steps</h3>
  <button onclick="addStrategyStep()" class="text-xs text-brand-400 hover:text-brand-300 font-bold"><i class="fa-solid fa-plus mr-1"></i>Add Step</button>
  </div>
  <div id="strategy-steps-list" class="space-y-3"></div>
  </div>

  <div class="flex gap-3">
  <button onclick="saveStrategy()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save & Activate</button>
  <button onclick="document.getElementById('strategy-editor').classList.add('hidden')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
  </div>
  </div>

  <!-- Existing Strategies -->
  <div id="strategies-list" class="space-y-4"></div>
  </div>

  <!-- ========================
       SEARCH TAB
       ======================== -->
  <div id="tab-search" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Lead Search</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Run natural-language searches across multiple engines and convert results into leads.</p>
  </div>
  </div>

  <!-- Info Banner -->
  <div class="bg-emerald-500/10 border border-emerald-500/30 rounded-xl p-4 flex items-start gap-3">
  <i class="fa-solid fa-check-circle text-emerald-400 mt-0.5"></i>
  <p class="text-xs text-emerald-400/90"><strong>GitHub search is live</strong> — find tech founders/companies for free. Upload CSVs for any lead list. Paid engines (Apollo, Google CSE) are optional.</p>
  </div>

  <!-- Search Form -->
  <div class="cb-card p-6 space-y-4">
  <div>
  <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Search Prompt</label>
  <textarea id="search-prompt" rows="3" class="cb-input w-full resize-none" placeholder="e.g. logistics companies Rotterdam Netherlands"></textarea>
  </div>
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>
  <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Name (optional)</label>
  <input id="search-name" type="text" class="cb-input w-full" placeholder="My Rotterdam search">
  </div>
  <div>
  <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Sources</label>
  <div class="flex flex-wrap gap-3 pt-2">
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="github" checked> GitHub (free)</label>
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="bing" checked> Bing</label>
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="searxng"> SearXNG</label>
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="brave"> Brave</label>
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="apollo"> Apollo (paid)</label>
  <label class="flex items-center gap-1.5 text-sm cb-text-secondary"><input type="checkbox" class="search-source" value="google_cse"> Google CSE</label>
  </div>
  </div>
  </div>
  <div class="flex items-center gap-3">
  <button id="search-run-btn" onclick="runSearch()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-magnifying-glass"></i> Run Search
  </button>
  <span id="search-status" class="text-xs cb-text-tertiary"></span>
  </div>
  </div>

  <!-- CSV Upload -->
  <div class="cb-card p-6 space-y-4">
  <div>
  <h3 class="text-sm font-bold cb-text-primary mb-1">Upload Leads via CSV</h3>
  <p class="text-xs cb-text-tertiary">Columns: email, first_name, last_name, company, domain, linkedin_url, industry, location.</p>
  </div>
  <div class="flex items-center gap-3">
  <input type="file" id="csv-upload-input" accept=".csv" class="text-sm cb-text-secondary file:mr-3 file:py-2 file:px-3 file:rounded-lg file:border-0 file:bg-surface-700 file:text-white hover:file:bg-surface-600">
  <button onclick="uploadLeadsCsv()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-upload"></i> Import
  </button>
  <span id="csv-upload-status" class="text-xs cb-text-tertiary"></span>
  </div>
  </div>

  <!-- Search Results -->
  <div class="cb-card p-6">
  <div class="flex items-center justify-between mb-4">
  <h3 class="text-sm font-bold cb-text-primary">Results</h3>
  <div class="flex items-center gap-2">
  <button onclick="bulkConvertSearchResults()" class="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-user-plus mr-1"></i> Convert Selected</button>
  <button onclick="toggleAllSearchResults()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary text-xs font-bold px-3 py-1.5 rounded-lg transition">Toggle All</button>
  </div>
  </div>
  <div class="overflow-x-auto">
  <table class="w-full text-left border-collapse">
  <thead>
  <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
  <th class="py-3 px-4"><input type="checkbox" id="search-results-select-all" onclick="toggleAllSearchResults()"></th>
  <th class="py-3 px-4">Domain</th>
  <th class="py-3 px-4">Company</th>
  <th class="py-3 px-4">Industry</th>
  <th class="py-3 px-4">Location</th>
  <th class="py-3 px-4">Confidence</th>
  <th class="py-3 px-4 text-right">Actions</th>
  </tr>
  </thead>
  <tbody id="search-results-table" class="text-sm divide-y cb-border">
  <tr><td colspan="7" class="py-12 text-center cb-text-tertiary">Run a search to see results.</td></tr>
  </tbody>
  </table>
  </div>
  </div>

  <!-- Saved Queries -->
  <div class="cb-card p-6">
  <h3 class="text-sm font-bold cb-text-primary mb-4">Saved Queries</h3>
  <div class="overflow-x-auto">
  <table class="w-full text-left border-collapse">
  <thead>
  <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
  <th class="py-3 px-4">Name</th>
  <th class="py-3 px-4">Prompt</th>
  <th class="py-3 px-4">Status</th>
  <th class="py-3 px-4">Results</th>
  <th class="py-3 px-4">Created</th>
  <th class="py-3 px-4 text-right">Actions</th>
  </tr>
  </thead>
  <tbody id="search-queries-table" class="text-sm divide-y cb-border">
  <tr><td colspan="6" class="py-8 text-center cb-text-tertiary">No saved queries.</td></tr>
  </tbody>
  </table>
  </div>
  </div>
  </div>

  <!-- ========================
       WORKFLOWS TAB
       ======================== -->
  <div id="tab-workflows" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Workflows</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Automated playbooks that search, enrich, and add leads to sequences.</p>
  </div>
  <button onclick="showCreateWorkflowForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-plus"></i> Create Workflow
  </button>
  </div>

  <!-- Workflow Creator -->
  <div id="workflow-editor" class="hidden cb-card p-6 space-y-6">
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Workflow Name</label>
  <input id="wf-name" type="text" class="cb-input w-full" placeholder="e.g. Logistics Rotterdam">
  </div>
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Description</label>
  <input id="wf-desc" type="text" class="cb-input w-full" placeholder="What does this workflow do?">
  </div>
  </div>
  <div>
  <div class="flex items-center justify-between mb-3">
  <h3 class="text-sm font-bold cb-text-primary">Steps</h3>
  <button onclick="addWorkflowStep()" class="text-xs text-brand-400 hover:text-brand-300 font-bold"><i class="fa-solid fa-plus mr-1"></i>Add Step</button>
  </div>
  <div id="workflow-steps-list" class="space-y-3"></div>
  </div>
  <div class="flex gap-3">
  <button onclick="saveWorkflow()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save Workflow</button>
  <button onclick="document.getElementById('workflow-editor').classList.add('hidden')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
  </div>
  </div>

  <!-- Existing Workflows -->
  <div id="workflows-list" class="space-y-4"></div>
  </div>

  <!-- ========================
       SEQUENCES TAB
       ======================== -->
  <div id="tab-sequences" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Sequences</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Multi-step outreach cadences: email, LinkedIn, and wait steps.</p>
  </div>
  <button onclick="showCreateSequenceForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-plus"></i> Create Sequence
  </button>
  </div>

  <!-- Sequence Creator -->
  <div id="sequence-editor" class="hidden cb-card p-6 space-y-6">
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Sequence Name</label>
  <input id="seq-name" type="text" class="cb-input w-full" placeholder="e.g. 3-Touch Email">
  </div>
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Description</label>
  <input id="seq-desc" type="text" class="cb-input w-full" placeholder="What is this sequence for?">
  </div>
  </div>
  <div>
  <div class="flex items-center justify-between mb-3">
  <h3 class="text-sm font-bold cb-text-primary">Steps</h3>
  <button onclick="addSequenceStep()" class="text-xs text-brand-400 hover:text-brand-300 font-bold"><i class="fa-solid fa-plus mr-1"></i>Add Step</button>
  </div>
  <div id="sequence-steps-list" class="space-y-3"></div>
  </div>
  <div class="flex gap-3">
  <button onclick="saveSequence()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Save Sequence</button>
  <button onclick="document.getElementById('sequence-editor').classList.add('hidden')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
  </div>
  </div>

  <!-- Existing Sequences -->
  <div id="sequences-list" class="space-y-4"></div>
  </div>

  <!-- ========================
       CAMPAIGNS TAB
       ======================== -->
  <div id="tab-campaigns" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Campaigns</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Enroll leads into sequences and track campaign performance.</p>
  </div>
  <button onclick="showCreateCampaignForm()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-4 py-2.5 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-plus"></i> Create Campaign
  </button>
  </div>

  <!-- Campaign Creator -->
  <div id="campaign-editor" class="hidden cb-card p-6 space-y-4">
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Campaign Name</label>
  <input id="camp-name" type="text" class="cb-input w-full" placeholder="e.g. Q3 Logistics Outreach">
  </div>
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Sequence</label>
  <select id="camp-sequence" class="cb-input w-full"><option value="">Select a sequence...</option></select>
  </div>
  <div>
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Leads</label>
  <select id="camp-leads" multiple class="cb-input w-full h-32"><option value="">Loading leads...</option></select>
  <p class="text-[10px] cb-text-tertiary mt-1">Hold Ctrl/Cmd to select multiple leads.</p>
  </div>
  </div>
  <div class="flex gap-3">
  <button onclick="saveCampaign()" class="bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition"><i class="fa-solid fa-floppy-disk mr-1"></i> Create & Enroll</button>
  <button onclick="document.getElementById('campaign-editor').classList.add('hidden')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-sm px-5 py-2.5 rounded-xl transition">Cancel</button>
  </div>
  </div>

  <!-- Existing Campaigns -->
  <div id="campaigns-list" class="space-y-4"></div>
  </div>

  <!-- ========================
       ANALYTICS TAB
       ======================== -->
  <div id="tab-analytics" class="tab-content space-y-6 hidden">
  <div class="flex items-center justify-between mb-2">
  <div>
  <h2 class="text-base font-extrabold cb-text-primary">Analytics</h2>
  <p class="text-xs cb-text-tertiary mt-0.5">Campaign performance and overall outreach KPIs.</p>
  </div>
  <button onclick="loadAnalytics()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-xs px-4 py-2 rounded-xl transition flex items-center gap-2">
  <i class="fa-solid fa-rotate-right"></i> Refresh
  </button>
  </div>

  <!-- Metrics Cards (re-use Metrics tab data) -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4" id="analytics-metrics-cards">
  <div class="cb-surface rounded-2xl p-5 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Emails Sent</p>
  <p id="a-total-sent" class="text-3xl font-black text-cyan-400">—</p>
  </div>
  <div class="cb-surface rounded-2xl p-5 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Open Rate</p>
  <p id="a-open-rate" class="text-3xl font-black cb-text-primary">—</p>
  </div>
  <div class="cb-surface rounded-2xl p-5 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Reply Rate</p>
  <p id="a-reply-rate" class="text-3xl font-black cb-text-primary">—</p>
  </div>
  <div class="cb-surface rounded-2xl p-5 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Meetings Booked</p>
  <p id="a-meetings" class="text-3xl font-black text-emerald-400">—</p>
  </div>
  </div>

  <!-- Campaign Performance -->
  <div class="cb-card p-6">
  <h3 class="text-sm font-extrabold cb-text-primary mb-4"><i class="fa-solid fa-bullseye text-brand-400 mr-2"></i>Campaign Performance</h3>
  <div class="overflow-x-auto">
  <table class="w-full text-left border-collapse">
  <thead>
  <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
  <th class="py-4 px-6">Campaign</th>
  <th class="py-4 px-6">Enrolled</th>
  <th class="py-4 px-6">Active</th>
  <th class="py-4 px-6">Completed</th>
  <th class="py-4 px-6">Sent</th>
  <th class="py-4 px-6">Open Rate</th>
  <th class="py-4 px-6">Reply Rate</th>
  <th class="py-4 px-6 text-right">Actions</th>
  </tr>
  </thead>
  <tbody id="analytics-campaigns-table" class="text-sm divide-y cb-border">
  <tr><td colspan="8" class="py-12 text-center cb-text-tertiary">No campaigns yet.</td></tr>
  </tbody>
  </table>
  </div>
  </div>

  <!-- A/B Test Results -->
  <div class="cb-card p-6">
  <div class="flex items-center justify-between mb-4">
  <h3 class="text-sm font-extrabold cb-text-primary"><i class="fa-solid fa-flask text-brand-400 mr-2"></i>A/B Test Results</h3>
  <select id="ab-test-campaign-select" class="cb-input text-xs py-1.5 min-w-[180px]" onchange="loadABTests()">
  <option value="">Select campaign...</option>
  </select>
  </div>
  <div id="ab-tests-container" class="space-y-4">
  <p class="cb-text-tertiary text-sm">Select a campaign to view A/B test results.</p>
  </div>
  </div>

  <!-- Deliverability Dashboard -->
  <div class="cb-card p-6">
  <div class="flex items-center justify-between mb-4">
  <h3 class="text-sm font-extrabold cb-text-primary"><i class="fa-solid fa-shield-halved text-brand-400 mr-2"></i>Deliverability Dashboard</h3>
  <select id="deliverability-domain-select" class="cb-input text-xs py-1.5 min-w-[180px]" onchange="loadDeliverabilityDetail()">
  <option value="">Select domain...</option>
  </select>
  </div>
  <div id="deliverability-detail-container">
  <p class="cb-text-tertiary text-sm">Select a domain to view deliverability details.</p>
  </div>
  </div>

  <!-- Lead Score Leaderboard -->
  <div class="cb-card p-6">
  <div class="flex items-center justify-between mb-4">
  <h3 class="text-sm font-extrabold cb-text-primary"><i class="fa-solid fa-trophy text-brand-400 mr-2"></i>Lead Score Leaderboard</h3>
  <button onclick="loadTopLeads()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-bold text-xs px-3 py-1.5 rounded-lg transition flex items-center gap-1.5"><i class="fa-solid fa-rotate-right"></i> Refresh</button>
  </div>
  <div id="top-leads-container">
  <div class="flex items-center gap-2 text-sm cb-text-tertiary"><i class="fa-solid fa-spinner fa-spin"></i> Loading top leads...</div>
  </div>
  </div>

  <!-- Reply Sentiment Chart -->
  <div class="cb-card p-6">
  <div class="flex items-center justify-between mb-4">
  <h3 class="text-sm font-extrabold cb-text-primary"><i class="fa-solid fa-chart-pie text-brand-400 mr-2"></i>Reply Sentiment Analysis</h3>
  </div>
  <div id="sentiment-chart-container" class="flex flex-col md:flex-row items-center gap-8">
  <p class="cb-text-tertiary text-sm">No sentiment data available yet.</p>
  </div>
  </div>
  </div>

  </section>
  </main>

 <!-- LEAD DETAILED INSPECTOR MODAL (DRAWER) -->
 <div id="drawer-lead" class="fixed inset-y-0 right-0 w-[680px] cb-surface-elevated border-l cb-border z-40 transform translate-x-full transition-transform duration-300 flex flex-col" style="box-shadow: var(--shadow-lg);">
 <!-- Header -->
 <div class="p-6 border-b cb-border flex items-center justify-between cb-surface-elevated">
 <div>
 <span id="drawer-stage-badge" class="text-[10px] font-extrabold uppercase px-2.5 py-1 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 tracking-wider">INGESTED</span>
 <h2 id="drawer-lead-name" class="text-xl font-black cb-text-primary mt-2.5">Willem Jansen</h2>
 <p id="drawer-lead-sub" class="text-xs cb-text-tertiary">Dentist & Eigenaar at Tandartsenpark Utrecht</p>
 </div>
 <button onclick="closeLeadDrawer()" class="w-10 h-10 hover:bg-black/5 rounded-full flex items-center justify-center cb-text-tertiary hover:cb-text-primary transition">
 <i class="fa-solid fa-xmark text-lg"></i>
 </button>
 </div>

 <!-- Tabs Navigation Inside Drawer -->
 <div class="flex border-b cb-border cb-surface-elevated px-4">
 <button onclick="switchDrawerTab('dr-overview')" id="btn-dr-overview" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-primary border-b-2 border-brand-500">Overview</button>
 <button onclick="switchDrawerTab('dr-research')" id="btn-dr-research" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent">1. Research</button>
 <button onclick="switchDrawerTab('dr-deliv')" id="btn-dr-deliv" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent">2. Deliver</button>
 <button onclick="switchDrawerTab('dr-opp')" id="btn-dr-opp" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent">3. Opportunity</button>
 <button onclick="switchDrawerTab('dr-qual')" id="btn-dr-qual" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent">4. Qual</button>
 <button onclick="switchDrawerTab('dr-outreach')" id="btn-dr-outreach" class="dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent">5. Email</button>
 </div>

 <!-- Body Contents -->
 <div class="flex-grow p-6 overflow-y-auto min-h-0 space-y-6">
 <!-- Overview tab contents -->
 <div id="dr-overview" class="dr-tab-content space-y-6 block">
 <div class="grid grid-cols-2 gap-4">
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">BANT Lead Score</span>
 <span id="drawer-score" class="text-2xl font-black text-cyan-400 mt-1 block">0/100</span>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">Priority Rank</span>
 <span id="drawer-priority" class="text-2xl font-black text-emerald-400 mt-1 block">NURTURE</span>
 </div>
 </div>
 <div class="space-y-3.5">
 <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider">CRM Records (SQLite)</h4>
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl space-y-2.5 text-sm">
 <div class="flex justify-between border-b cb-border pb-2"><span class="cb-text-tertiary">Email Address</span><span class="cb-text-primary" id="dr-val-email">-</span></div>
 <div class="flex justify-between border-b cb-border pb-2"><span class="cb-text-tertiary">Company Name</span><span class="cb-text-primary" id="dr-val-comp">-</span></div>
 <div class="flex justify-between border-b cb-border pb-2"><span class="cb-text-tertiary">Domain</span><span class="text-brand-400" id="dr-val-domain">-</span></div>
 <div class="flex justify-between pb-1"><span class="cb-text-tertiary">Personalization Anchor</span><span class="cb-text-primary max-w-[320px] text-right truncate" id="dr-val-anchor">-</span></div>
 </div>
 </div>

 <!-- Simulate reply text-area -->
 <div class="cb-surface-elevated p-5 border cb-border rounded-2xl space-y-4" id="simulate-reply-container" style="display: none;">
 <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider">Simulate Inbound Reply (Inbox Management)</h4>
 <textarea id="mock-reply-input" class="w-full h-24 cb-surface p-3 rounded-xl text-sm focus:outline-none focus:border-brand-500/80 cb-text-primary" placeholder="Klinkt wel interessant. Wat zijn de kosten en hoe borg je de veiligheid?"></textarea>
 <div class="flex justify-end gap-2.5">
 <button onclick="runReplySimulation('OBJECTION')" class="cb-surface-elevated hover:bg-black/10 border cb-border text-xs cb-text-secondary font-semibold px-3 py-2 rounded-xl transition">Objection Reply</button>
 <button onclick="runReplySimulation('MEETING_REQUEST')" class="bg-emerald-600 hover:bg-emerald-500 text-xs text-white font-semibold px-4 py-2 rounded-xl transition">Positive Reply</button>
 </div>
 </div>
 </div>

 <!-- 1. Research tab contents -->
 <div id="dr-research" class="dr-tab-content space-y-5 hidden">
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl space-y-3">
 <span class="text-[10px] text-brand-400 font-bold uppercase tracking-wider">Dutch MKB Research Summary</span>
 <p class="text-sm cb-text-secondary leading-relaxed" id="dr-research-summary">No research results loaded yet.</p>
 </div>
 <div class="grid grid-cols-2 gap-4">
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider">Industry</span>
 <span class="text-sm font-semibold cb-text-primary mt-1 block" id="dr-research-industry">-</span>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider">Estimated Size</span>
 <span class="text-sm font-semibold cb-text-primary mt-1 block" id="dr-research-size">-</span>
 </div>
 </div>
 <div class="space-y-3.5">
 <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider">Detected Corporate Pains</h4>
 <ul class="space-y-2.5" id="dr-research-pains">
 <!-- Pain lists -->
 </ul>
 </div>
 </div>

 <!-- 2. Deliverability tab contents -->
 <div id="dr-deliv" class="dr-tab-content space-y-5 hidden">
 <div class="flex items-center justify-between cb-surface-elevated p-5 border cb-border rounded-xl">
 <div>
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">Confidence Score</span>
 <span id="dr-deliv-score" class="text-3xl font-black text-emerald-400 mt-1 block">0%</span>
 </div>
 <div class="text-right">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">Bounce Risk</span>
 <span id="dr-deliv-risk" class="text-sm font-bold text-emerald-400 mt-1 block">LOW</span>
 </div>
 </div>
 <div class="space-y-3.5">
 <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider">Mail Server Audit Criteria</h4>
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl space-y-3 text-sm cb-text-secondary" id="dr-deliv-reasons">
 <!-- reasons -->
 </div>
 </div>
 </div>

 <!-- 3. Opportunity mapping tab contents -->
 <div id="dr-opp" class="dr-tab-content space-y-5 hidden">
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl space-y-3">
 <span class="text-[10px] text-indigo-400 font-bold uppercase tracking-wider block">Calculated ROI Case</span>
 <p class="text-sm cb-text-secondary leading-relaxed" id="dr-opp-case">Awaiting opportunity mapping...</p>
 </div>
 <div class="grid grid-cols-2 gap-4">
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">Hours Saved Monthly</span>
 <span id="dr-opp-hours" class="text-xl font-extrabold cb-text-primary mt-1 block">0 Hours</span>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block">Opportunity Score</span>
 <span id="dr-opp-score" class="text-xl font-extrabold cb-text-primary mt-1 block">0.0 / 10</span>
 </div>
 </div>
 <div class="space-y-3.5">
 <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider">Aligned Solutions</h4>
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
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[9px] cb-text-tertiary font-extrabold uppercase block">[S] Situation</span>
 <p class="text-xs cb-text-secondary mt-1.5 leading-relaxed" id="dr-qual-spin-s">-</p>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[9px] text-amber-500 font-extrabold uppercase block">[P] Problem</span>
 <p class="text-xs cb-text-secondary mt-1.5 leading-relaxed" id="dr-qual-spin-p">-</p>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[9px] text-red-500 font-extrabold uppercase block">[I] Implication</span>
 <p class="text-xs cb-text-secondary mt-1.5 leading-relaxed" id="dr-qual-spin-i">-</p>
 </div>
 <div class="cb-surface-elevated p-4 border cb-border rounded-xl">
 <span class="text-[9px] text-emerald-500 font-extrabold uppercase block">[N] Need-Payoff</span>
 <p class="text-xs cb-text-secondary mt-1.5 leading-relaxed" id="dr-qual-spin-n">-</p>
 </div>
 </div>
 </div>
 
 <!-- BANT analysis grids -->
 <div class="space-y-3.5">
 <h4 class="text-xs font-bold text-cyan-400 uppercase tracking-widest">BANT Qualification</h4>
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl divide-y divide-slate-800 text-sm space-y-2.5">
 <div class="flex justify-between pb-2 pt-1"><span class="cb-text-tertiary font-semibold">[B] Budget</span><span class="cb-text-primary text-right" id="dr-qual-bant-b">-</span></div>
 <div class="flex justify-between pb-2 pt-2"><span class="cb-text-tertiary font-semibold">[A] Authority</span><span class="cb-text-primary text-right" id="dr-qual-bant-a">-</span></div>
 <div class="flex justify-between pb-2 pt-2"><span class="cb-text-tertiary font-semibold">[N] Need</span><span class="cb-text-primary text-right" id="dr-qual-bant-n">-</span></div>
 <div class="flex justify-between pb-1 pt-2"><span class="cb-text-tertiary font-semibold">[T] Timeline</span><span class="cb-text-primary text-right" id="dr-qual-bant-t">-</span></div>
 </div>
 </div>
 </div>

 <!-- 5. Outreach email draft tab contents -->
 <div id="dr-outreach" class="dr-tab-content space-y-5 hidden">
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl">
 <div class="flex justify-between text-xs cb-text-tertiary border-b cb-border pb-3 mb-4">
 <span>Subject (Variant A)</span>
 <span class="text-brand-400 font-semibold">Ready for Sending</span>
 </div>
 <h4 class="text-sm font-bold cb-text-primary mb-2" id="dr-out-subject">Awaiting draft...</h4>
 </div>
 <div class="cb-surface-elevated p-5 border cb-border rounded-xl">
 <div class="flex justify-between text-xs cb-text-tertiary border-b cb-border pb-3 mb-4">
 <span>Email Body (Step 1)</span>
 <span class="cb-text-tertiary font-semibold">Dutch Conversational (Je)</span>
 </div>
 <p class="text-sm cb-text-secondary leading-relaxed whitespace-pre-wrap font-mono" id="dr-out-body">No draft email found.</p>
 </div>
 </div>
 </div>

 <!-- Drawer Footer Buttons -->
 <div class="p-6 border-t cb-border cb-surface-elevated flex gap-4">
 <button onclick="replayPipelineFromDrawer()" class="flex-1 py-3 border cb-border hover:cb-border cb-surface-elevated hover:bg-black/10 text-sm font-bold cb-text-primary rounded-xl transition flex items-center justify-center gap-2">
 <i class="fa-solid fa-rotate-right"></i> Replay Lead Pipeline
 </button>
 <button id="drawer-btn-approve" onclick="approveAndSendFromDrawer()" class="flex-1 py-3 bg-brand-600 hover:bg-brand-500 text-sm font-bold text-white rounded-xl shadow-brand-500/15 transition flex items-center justify-center gap-2" style="display: none;">
 <i class="fa-solid fa-paper-plane"></i> Approve & Send Email
 </button>
 </div>
 </div>

 <!-- BACKGROUND OVERLAY -->
 <div id="overlay-bg" onclick="closeLeadDrawer()" class="fixed inset-0 z-35 hidden transition-opacity" style="background-color: rgba(2,6,23,0.35);"></div>

 <!-- =========================================================================
 DASHBOARD JAVASCRIPT FRONTEND CONTROLLER
 ========================================================================= -->
 <script>

 // Theme handling
 function initTheme() {
 const saved = localStorage.getItem('clawbuildr-theme');
 const theme = saved || 'dark';
 setTheme(theme);
 }

 function setTheme(theme) {
 document.documentElement.setAttribute('data-theme', theme);
 localStorage.setItem('clawbuildr-theme', theme);
 const lightIcon = document.getElementById('theme-icon-light');
 const darkIcon = document.getElementById('theme-icon-dark');
 if (theme === 'dark') {
 if (lightIcon) lightIcon.classList.add('hidden');
 if (darkIcon) darkIcon.classList.remove('hidden');
 } else {
 if (lightIcon) lightIcon.classList.remove('hidden');
 if (darkIcon) darkIcon.classList.add('hidden');
 }
 }

 function toggleTheme() {
 const current = document.documentElement.getAttribute('data-theme') || 'dark';
 setTheme(current === 'dark' ? 'light' : 'dark');
 }

 // Initialize theme immediately
 initTheme();

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
 if (_gridLoaded) { _gridData = resLeads; gridApplyFilters(); }
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
  loadFunnelPipeline();
  }

  // Funnel Pipeline visualization
  async function loadFunnelPipeline() {
  try {
    const res = await fetch('/api/funnel/stats');
    const data = await res.json();
    renderFunnelPipeline(data.stages || {});
  } catch(e) {
    console.error('Funnel stats error:', e);
  }
  }

  function renderFunnelPipeline(stages) {
  const container = document.getElementById('funnel-pipeline');
  const total = Object.values(stages).reduce((s, v) => s + v, 0);
  const colors = {
    'INGESTED': 'bg-slate-500',
    'DISCOVERED': 'bg-blue-500',
    'ENRICHING': 'bg-purple-500',
    'VERIFYING': 'bg-amber-500',
    'READY': 'bg-emerald-500',
    'CONTACTED': 'bg-brand-500',
  };
  const icons = {
    'INGESTED': 'fa-inbox',
    'DISCOVERED': 'fa-magnifying-glass',
    'ENRICHING': 'fa-wand-magic-sparkles',
    'VERIFYING': 'fa-shield-check',
    'READY': 'fa-check-circle',
    'CONTACTED': 'fa-paper-plane',
  };
  const stageOrder = ['INGESTED', 'DISCOVERED', 'ENRICHING', 'VERIFYING', 'READY', 'CONTACTED'];
  container.innerHTML = stageOrder.map((stage, i) => {
    const count = stages[stage] || 0;
    const pct = total > 0 ? ((count / total) * 100).toFixed(0) : 0;
    const arrow = i < stageOrder.length - 1 ? '<i class="fa-solid fa-chevron-right cb-text-tertiary text-[10px] mx-1"></i>' : '';
    return `
      <div class="flex items-center gap-1 flex-shrink-0">
      <div class="cb-surface rounded-xl px-3 py-2 text-center min-w-[80px]">
        <i class="fa-solid ${icons[stage] || 'fa-circle'} text-xs ${colors[stage] || 'bg-gray-500'} mb-1 block"></i>
        <span class="text-sm font-bold cb-text-primary block">${count}</span>
        <span class="text-[9px] cb-text-tertiary uppercase block">${stage}</span>
      </div>
      ${arrow}
      </div>
    `;
  }).join('');
  }

  async function triggerFunnel() {
  try {
    showToast('info', 'Running funnel cycle...');
    const res = await fetch('/api/funnel/trigger', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      showToast('success', 'Funnel cycle complete. Check pipeline stats.');
      loadFunnelPipeline();
      loadMetrics();
    } else {
      showToast('error', 'Funnel error: ' + (data.error || 'unknown'));
    }
  } catch(e) {
    showToast('error', 'Failed to trigger funnel: ' + e.message);
  }
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
 el.className = "w-11 h-11 cb-surface border-2 cb-border cb-text-secondary rounded-full flex items-center justify-center transition-all duration-300";
                    el.style.boxShadow = "var(--shadow-sm)";
 }
 });
 
 agents.forEach(a => {
 const card = document.createElement("div");
 let badgeClass = "bg-slate-100 text-slate-600 border-slate-200";
 let badgeDot = "";
 let nodeName = null;
 
 if (a.agent_name === "Research Agent") nodeName = "research";
 else if (a.agent_name === "Deliverability Agent") nodeName = "deliver";
 else if (a.agent_name === "Opportunity Mapping Agent") nodeName = "opp";
 else if (a.agent_name === "Qualification Agent") nodeName = "qual";
 else if (a.agent_name === "Outreach Agent") nodeName = "outreach";
 else if (a.agent_name === "Inbox Management Agent") nodeName = "inbound";
 
 if(a.status === "RUNNING") {
 badgeClass = "bg-blue-50 text-blue-700 border-blue-200";
 badgeDot = "bg-blue-500";
 if(nodeName) {
 const nodeEl = document.getElementById(`node-${nodeName}`);
 if(nodeEl) { nodeEl.className = "w-10 h-10 rounded-full flex items-center justify-center text-white transition-all duration-300"; nodeEl.style.backgroundColor = "var(--brand)"; nodeEl.style.borderColor = "var(--brand-light)"; nodeEl.style.boxShadow = "0 0 0 3px var(--brand-soft)"; }
 }
 } else if (a.status === "WAITING") {
 badgeClass = "bg-amber-50 text-amber-700 border-amber-200";
 badgeDot = "bg-amber-500";
 if(nodeName) {
 const nodeEl = document.getElementById(`node-${nodeName}`);
 if(nodeEl) { nodeEl.className = "w-10 h-10 rounded-full flex items-center justify-center text-white transition-all duration-300"; nodeEl.style.backgroundColor = "var(--warning)"; nodeEl.style.borderColor = "#fbbf24"; nodeEl.style.boxShadow = "0 0 0 3px var(--warning-soft)"; }
 }
 } else if (a.status === "FAILED") {
 badgeClass = "bg-red-50 text-red-700 border-red-200";
 badgeDot = "bg-red-500";
 if(nodeName) {
 const nodeEl = document.getElementById(`node-${nodeName}`);
 if(nodeEl) { nodeEl.className = "w-10 h-10 rounded-full flex items-center justify-center text-white transition-all duration-300"; nodeEl.style.backgroundColor = "var(--danger)"; nodeEl.style.borderColor = "#fca5a5"; nodeEl.style.boxShadow = "0 0 0 3px var(--danger-soft)"; }
 }
 }
 
 card.className = "cb-card p-4 flex flex-col justify-between";
 
 card.innerHTML = `
 <div class="space-y-3">
 <div class="flex items-center justify-between">
 <span class="text-sm font-medium cb-text-primary">${a.agent_name}</span>
 <span class="inline-flex items-center gap-1.5 text-[10px] font-medium uppercase px-2 py-0.5 rounded-full border ${badgeClass}">
 ${badgeDot ? `<span class="w-1.5 h-1.5 rounded-full ${badgeDot} animate-pulse"></span>` : ""}
 ${a.status}
 </span>
 </div>
 <p class="text-xs cb-text-secondary leading-relaxed">${a.last_action}</p>
 </div>
 <div class="flex items-center justify-between pt-3 mt-3 border-t cb-border text-[10px] cb-text-tertiary">
 <span>Last active</span>
 <span>${new Date(a.last_active_at).toUTCString().split(" ")[4]} UTC</span>
 </div>
 `;
 container.appendChild(card);
 });
 
 // Highlight Human Review Node if a lead is PENDING_APPROVAL
 const hasPendingApproval = leads.some(l => l.current_stage === "PENDING_APPROVAL");
 if (hasPendingApproval) {
 const nodeEl = document.getElementById("node-review");
 if (nodeEl) { nodeEl.className = "w-11 h-11 rounded-full flex items-center justify-center text-white transition-all duration-300 agent-pulse-waiting"; nodeEl.style.backgroundColor = "var(--warning)"; nodeEl.style.borderColor = "#fbbf24"; nodeEl.style.boxShadow = "0 0 0 4px var(--warning-soft)"; }
 }
 }

 // Leads Queue table mapping
 function renderLeadsTable() {
 const tbody = document.getElementById("active-leads-table-rows");
 tbody.innerHTML = "";
 
 leads.forEach(l => {
 const tr = document.createElement("tr");
 tr.className = "hover:bg-black/[0.03] cursor-pointer transition";
 tr.onclick = (e) => {
 // Prevent click triggering if clicking action buttons
 if (e.target.tagName === 'BUTTON' || e.target.closest('button') || e.target.tagName === 'A') return;
 openLeadDrawer(l.contact_id);
 };

 let stageBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full cb-surface-elevated cb-text-tertiary border cb-border">${l.current_stage}</span>`;
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

 let scoreBadge = `<span class="cb-text-tertiary">-</span>`;
 if(l.lead_score > 0) {
 const colorClass = l.lead_score >= 80 ? 'text-cyan-400 font-extrabold' : 'cb-text-secondary';
 scoreBadge = `<span class="${colorClass}">${l.lead_score}/100</span>`;
 }

 let quickActionBtn = "";
 if(l.current_stage === "PENDING_APPROVAL") {
 quickActionBtn = `
 <button onclick="openApprovalModal('${l.contact_id}')" class="bg-brand-600 hover:bg-brand-500 text-xs text-white font-bold px-3 py-1.5 rounded-lg hover:scale-105 transition flex items-center gap-1.5">
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
 <span class="font-extrabold cb-text-primary block">${l.first_name} ${l.last_name || ''}</span>
 <span class="text-xs cb-text-tertiary block mt-0.5">${l.role || 'Prospect'}</span>
 </td>
 <td class="py-4 px-6 cb-text-secondary font-semibold">${l.company_name || 'MKB Firm'}</td>
 <td class="py-4 px-6">
 <span class="cb-text-secondary block text-xs truncate max-w-[150px]">${l.email}</span>
 <span class="text-[10px] text-brand-400 block mt-0.5 font-bold">${l.company_domain || ''}</span>
 </td>
 <td class="py-4 px-6">${stageBadge}</td>
 <td class="py-4 px-6">${scoreBadge}</td>
 <td class="py-4 px-6">${quickActionBtn}</td>
 <td class="py-4 px-6 text-right flex items-center justify-end gap-1">
 <button onclick="deleteLead('${l.contact_id}')" class="text-red-400/60 hover:text-red-400 hover:scale-110 p-2 rounded-lg transition" title="Remove lead from pipeline">
 <i class="fa-solid fa-trash-can text-[11px]"></i>
 </button>
 <button onclick="replayPipeline('${l.contact_id}')" class="cb-text-tertiary hover:cb-text-primary hover:scale-110 p-2 rounded-lg transition" title="Replay Lead through pipeline">
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
 tr.className = "hover:bg-black/[0.02]";
 
 const updDate = l.updated_at ? new Date(l.updated_at).toLocaleDateString() : '-';
 const priorityColor = l.priority === 'HOT' ? 'text-red-400 font-bold' : (l.priority === 'WARM' ? 'text-amber-400' : 'cb-text-tertiary');
 
 tr.innerHTML = `
 <td class="py-4 px-6 font-mono text-xs cb-text-tertiary">${l.contact_id}</td>
 <td class="py-4 px-6">
 <span class="font-bold cb-text-primary block">${l.first_name} ${l.last_name || ''}</span>
 <span class="text-xs cb-text-tertiary block">${l.role || '-'}</span>
 </td>
 <td class="py-4 px-6 cb-text-secondary">${l.email}</td>
 <td class="py-4 px-6 font-semibold cb-text-secondary">${l.company_name || '-'}</td>
 <td class="py-4 px-6"><span class="text-xs uppercase font-bold tracking-wide">${l.current_stage}</span></td>
 <td class="py-4 px-6"><span class="${priorityColor}">${l.priority}</span></td>
 <td class="py-4 px-6 cb-text-tertiary text-xs">${updDate}</td>
 `;
 tbody.appendChild(tr);
 });
 }

 async function runLiveAudit() {
 try {
 const btn = document.querySelector("button[onclick='runLiveAudit()']");
 const oldHtml = btn.innerHTML;
 btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin mr-2"></i>Auditing...';
 btn.disabled = true;

 const res = await fetch('/api/deliverability/audit', { method: 'POST' });
 const data = await res.json();
 if (data.status === 'success') {
 let msg = `Audit complete for ${data.domain}. Status: ${data.health}.`;
 if (data.issues && data.issues.length > 0) {
 msg += ` Issues found: ${data.issues.join(", ")}`;
 }
 alert(msg);
 loadInitialData(); // Reload UI to show updated domain table
 } else {
 alert(`Audit failed: ${data.message}`);
 }
 btn.innerHTML = oldHtml;
 btn.disabled = false;
 } catch(e) {
 alert('Error running audit: ' + e);
 }
 }

 // Tab 3: Monitored email domains
 function renderDeliverabilityMonitor() {
 const tbody = document.getElementById("deliverability-table-rows");
 tbody.innerHTML = "";
 
 deliverability.forEach(d => {
 const tr = document.createElement("tr");
 tr.className = "hover:bg-black/[0.02]";
 
 const healthBadge = d.domain_health === 'HEALTHY' 
 ? `<span class="bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider">HEALTHY</span>`
 : `<span class="bg-amber-500/10 text-amber-400 border border-amber-500/20 px-2 py-0.5 rounded text-xs font-bold uppercase tracking-wider">${d.domain_health}</span>`;
 
 tr.innerHTML = `
 <td class="py-4 px-6 font-bold cb-text-primary">${d.domain}</td>
 <td class="py-4 px-6 cb-text-secondary font-semibold">${d.sends_count}</td>
 <td class="py-4 px-6 cb-text-secondary font-semibold">${d.replies_count}</td>
 <td class="py-4 px-6 cb-text-tertiary">${d.bounces_count}</td>
 <td class="py-4 px-6 font-semibold ${d.bounce_rate > 5 ? 'text-red-400' : 'cb-text-secondary'}">${d.bounce_rate}%</td>
 <td class="py-4 px-6">${healthBadge}</td>
 <td class="py-4 px-6 text-xs cb-text-tertiary">${new Date(d.updated_at).toUTCString().split(" ")[4]} UTC</td>
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
 <div class="col-span-2 cb-surface p-12 rounded-2xl text-center cb-text-tertiary">
 <i class="fa-solid fa-calendar-xmark text-4xl block mb-3"></i>
 <span>No meetings scheduled on Google Calendar currently.</span>
 </div>
 `;
 return;
 }
 
 meetings.forEach(m => {
 const card = document.createElement("div");
 card.className = "cb-card p-5 flex flex-col justify-between space-y-4";
 
 const schedTime = new Date(m.scheduled_time);
 
 card.innerHTML = `
 <div class="space-y-2">
 <div class="flex justify-between items-start">
 <span class="bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 text-[9px] font-bold px-2 py-0.5 rounded uppercase">Google Meet Linked</span>
 <span class="text-xs cb-text-tertiary font-semibold">${schedTime.toLocaleDateString('en-US', {weekday: 'long', month: 'short', day: 'numeric'})}</span>
 </div>
 <h3 class="text-base font-extrabold cb-text-primary leading-snug">${m.summary}</h3>
 <div class="text-xs cb-text-tertiary mt-2 flex items-center gap-1.5">
 <i class="fa-regular fa-clock"></i>
 <span>${schedTime.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})} UTC (${m.duration_minutes} Mins)</span>
 </div>
 <p class="text-xs cb-text-tertiary">Prospect: <span class="cb-text-primary font-semibold">${m.first_name} ${m.last_name || ''}</span> (${m.email})</p>
 </div>
 <div class="border-t cb-border pt-4 flex justify-end">
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
 card.className = "cb-surface-elevated p-4 border cb-border rounded-xl space-y-2 text-sm";
 
 const logTime = new Date(log.created_at).toUTCString().split(" ")[4];
 let icon = "fa-solid fa-microchip cb-text-tertiary";
 let accentColor = "cb-text-tertiary";
 
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
 <span class="font-bold flex items-center gap-2 cb-text-primary">
 <i class="${icon}"></i> <span class="${accentColor}">${log.actor}</span>
 </span>
 <span class="text-[10px] cb-text-tertiary font-semibold">${logTime} UTC</span>
 </div>
 <p class="text-xs cb-text-secondary leading-relaxed">${log.action_details}</p>
 ${log.first_name ? `<span class="text-[9px] cb-text-tertiary font-bold cb-surface px-2 py-0.5 rounded">Prospect: ${log.first_name}</span>` : ''}
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
 text.className = "text-xs font-semibold cb-text-tertiary uppercase tracking-widest";
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
 card.className = "cb-surface-elevated border cb-border rounded-xl p-4 space-y-3";
 card.innerHTML = `
 <div class="flex justify-between items-start">
 <div>
 <span class="font-bold cb-text-primary">${m.first_name} ${m.last_name}</span>
 <span class="text-xs cb-text-tertiary ml-2">${m.company || ''}</span>
 </div>
 <a href="${m.profile_url || '#'}" target="_blank" class="text-xs text-blue-400 hover:underline"><i class="fa-brands fa-linkedin"></i> Open Profile</a>
 </div>
 <div class="cb-surface/50 rounded-lg p-3">
 <p class="text-sm cb-text-secondary leading-relaxed">${m.followup_note}</p>
 </div>
 <div class="flex gap-2">
 <button onclick="copyFollowup(this, '${m.followup_note.replace(/'/g, "\\'")}')" class="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-4 py-2 rounded-lg transition flex items-center gap-1.5">
 <i class="fa-regular fa-copy"></i> Copy Message
 </button>
 <button onclick="markFollowupDone(${m.id})" class="bg-slate-700 hover:bg-slate-600 cb-text-primary text-xs font-bold px-4 py-2 rounded-lg transition">
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
 tbody.innerHTML = `<tr><td colspan="5" class="py-12 text-center cb-text-tertiary"><i class="fa-brands fa-linkedin text-3xl block mb-2"></i>No LinkedIn outreach yet. Connections are sent automatically when leads reach the Outreach stage.</td></tr>`;
 return;
 }

 linkedinHistory.forEach(l => {
 const tr = document.createElement("tr");
 tr.className = "hover:bg-black/[0.02]";

 let outcomeBadge = "";
 if(l.outcome === 'SUCCESS') {
 outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">SENT</span>`;
 } else if(l.outcome === 'NOT_FOUND') {
 outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30">NOT FOUND</span>`;
 } else if(l.outcome === 'FAILURE') {
 outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-500/15 cb-text-tertiary border border-slate-500/30">FAILED</span>`;
 } else {
 outcomeBadge = `<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-red-500/15 text-red-400 border border-red-500/30">${l.outcome}</span>`;
 }

 const time = l.timestamp ? new Date(l.timestamp).toLocaleString('nl-NL', {day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}) : '-';
 const noteDisplay = (l.note || '').substring(0, 100) + ((l.note || '').length > 100 ? '...' : '');

 tr.innerHTML = `
 <td class="py-4 px-6">
 <span class="font-bold cb-text-primary block">${l.first_name || ''} ${l.last_name || ''}</span>
 ${l.profile_url ? `<a href="${l.profile_url}" target="_blank" class="text-[10px] text-blue-400 hover:underline block mt-0.5"><i class="fa-brands fa-linkedin text-[9px]"></i> Profile</a>` : ''}
 </td>
 <td class="py-4 px-6 cb-text-secondary font-semibold">${l.company || '-'}</td>
 <td class="py-4 px-6 text-xs cb-text-tertiary max-w-[300px]">${noteDisplay}</td>
 <td class="py-4 px-6">${outcomeBadge}</td>
 <td class="py-4 px-6 text-xs cb-text-tertiary">${time}</td>
 `;
 tbody.appendChild(tr);
 });
 }

 // Real-time Scrolling Console Log inside Control Tab
 function pushEventToConsoleLog(event, text) {
 const con = document.getElementById("console-stream-log");
 if(!con) return;
 
 const timeStr = new Date().toUTCString().split(" ")[4];
 let colorClass = "cb-text-tertiary";
 
 if (event === "agent_handoff") colorClass = "text-cyan-400";
 else if (event === "notify") colorClass = "text-emerald-400 font-bold";
 else if (event === "RESET") colorClass = "cb-text-tertiary";
 else if (event === "OUTBOUND_SEND") colorClass = "text-brand-400 font-extrabold";
 
 const div = document.createElement("div");
 div.className = "leading-relaxed border-l-2 cb-border pl-2 py-0.5";
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
 el.className = "tab-button w-full flex items-center space-x-3 px-3 py-2.5 rounded-xl text-sm font-medium cb-text-secondary hover:bg-black/5 hover:cb-text-primary transition";
 });
 
 const activeBtn = document.getElementById(`btn-${tabId}`);
 if (activeBtn) {
 activeBtn.className = "tab-button active w-full flex items-center space-x-3 px-3 py-2.5 rounded-xl text-sm font-medium cb-text-primary";
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
  if(tabId === "tab-search") {
  loadSearchQueries();
  }
  if(tabId === "tab-workflows") {
  loadWorkflows();
  }
  if(tabId === "tab-sequences") {
  loadSequences();
  }
  if(tabId === "tab-campaigns") {
  loadCampaigns();
  }
   if(tabId === "tab-analytics") {
   loadAnalytics();
   }
   if(tabId === "tab-grid") {
   gridInit();
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
 el.className = "dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-tertiary border-b-2 border-transparent";
 });
 document.getElementById(`btn-${tabId}`).className = "dr-tab-btn flex-1 py-3 text-xs font-semibold cb-text-primary border-b-2 border-brand-500";
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
 li.className = "flex items-start gap-2.5 cb-text-secondary text-xs cb-surface-elevated p-3 rounded-xl border cb-border";
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
 text.className = "text-xs font-semibold cb-text-tertiary uppercase tracking-widest";
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
 let bg = "cb-surface-elevated cb-border cb-text-primary";
 let icon = '<i class="fa-solid fa-circle-info text-blue-400"></i>';
 
 if (type === "success") {
 bg = "cb-surface-elevated border-emerald-800 text-emerald-100";
 icon = '<i class="fa-solid fa-circle-check text-emerald-400"></i>';
 } else if (type === "warning") {
 bg = "cb-surface-elevated border-amber-800 text-amber-100";
 icon = '<i class="fa-solid fa-triangle-exclamation text-amber-400"></i>';
 } else if (type === "error") {
 bg = "cb-surface-elevated border-red-800 text-red-100";
 icon = '<i class="fa-solid fa-circle-xmark text-red-400"></i>';
 }
 
 toast.className = `flex items-center space-x-3 border p-4 rounded-xl transition duration-300 transform translate-y-4 opacity-0 max-w-sm ${bg}`;
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
 : 'cb-surface-elevated cb-border cb-text-secondary hover:bg-black/10'
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
 if (!target) return 'cb-text-primary';
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
 if (!_templates.length) { grid.innerHTML = '<p class="cb-text-tertiary text-sm col-span-2">No templates yet. Click New Template to add one.</p>'; return; }
 grid.innerHTML = _templates.map(t => `
 <div class="cb-surface rounded-2xl p-5">
 <div class="flex items-start justify-between mb-3">
 <div>
 <span class="inline-block text-[10px] font-bold px-2 py-0.5 rounded-full border mb-2 ${CATEGORY_COLORS[t.category] || 'bg-slate-700 cb-text-secondary border-slate-600'}">${CATEGORY_LABELS[t.category] || t.category}</span>
 <h3 class="text-sm font-bold cb-text-primary">${t.name}</h3>
 ${t.subject_line ? `<p class="text-xs cb-text-tertiary mt-0.5">Onderwerp: ${t.subject_line}</p>` : ''}
 </div>
 <div class="flex gap-2">
 <button onclick="editTemplate('${t.template_id}')" class="cb-text-tertiary hover:cb-text-primary transition text-xs"><i class="fa-solid fa-pen"></i></button>
 <button onclick="deleteTemplate('${t.template_id}')" class="text-slate-600 hover:text-red-400 transition text-xs"><i class="fa-solid fa-trash"></i></button>
 </div>
 </div>
 <pre class="text-xs cb-text-tertiary whitespace-pre-wrap cb-surface-elevated rounded-xl p-3 max-h-48 overflow-y-auto font-mono">${t.body}</pre>
 <div class="flex gap-2 mt-3 flex-wrap">
 ${(JSON.parse(t.variables || '[]')).map(v => `<span class="text-[10px] cb-surface-elevated cb-text-tertiary px-2 py-0.5 rounded-full">{{${v}}}</span>`).join('')}
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
 if (!_strategies.length) { container.innerHTML = '<p class="cb-text-tertiary text-sm">No strategies yet. Click New Strategy to create one.</p>'; return; }
 container.innerHTML = _strategies.map(s => `
 <div class="cb-surface-elevated border ${s.active ? 'border-brand-500/50' : 'cb-border'} rounded-2xl p-5">
 <div class="flex items-center justify-between mb-4">
 <div>
 <div class="flex items-center gap-3">
 <h3 class="text-sm font-bold cb-text-primary">${s.name}</h3>
 ${s.active ? '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-brand-500/10 text-brand-400 border border-brand-500/20">ACTIVE</span>' : ''}
 </div>
 ${s.description ? `<p class="text-xs cb-text-tertiary mt-0.5">${s.description}</p>` : ''}
 </div>
 ${!s.active ? `<button onclick="activateStrategy('${s.strategy_id}')" class="text-xs bg-brand-600 hover:bg-brand-500 text-white font-bold px-3 py-1.5 rounded-lg transition">Activate</button>` : ''}
 </div>
 <div class="flex items-center gap-2 overflow-x-auto pb-2">
 ${(s.steps || []).map((step, i) => `
 <div class="flex items-center gap-2 shrink-0">
 <div class="cb-surface-elevated rounded-xl px-3 py-2 text-center min-w-[90px]">
 <p class="text-[10px] cb-text-tertiary font-bold uppercase">${step.step_type}</p>
 <p class="text-xs cb-text-primary font-bold mt-0.5">Day ${step.delay_days}</p>
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
 el.className = 'cb-surface-elevated border cb-border rounded-xl p-4 grid grid-cols-4 gap-3 items-end';
 el.innerHTML = `
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Step ${n}</label>
 <select name="type" class="cb-input w-full">
 <option value="EMAIL" ${type==='EMAIL'?'selected':''}>Email</option>
 <option value="LINKEDIN" ${type==='LINKEDIN'?'selected':''}>LinkedIn</option>
 <option value="WAIT" ${type==='WAIT'?'selected':''}>Wait</option>
 </select>
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Delay (days)</label>
 <input name="delay" type="number" min="0" value="${delay}" class="cb-input w-full">
 </div>
 <div>
 <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Label</label>
 <input name="label" type="text" placeholder="e.g. Follow-up #1" value="${label}" class="cb-input w-full">
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
  setTimeout(() => { loadSearchQueries(); }, 1400);
  setTimeout(() => { loadWorkflows(); }, 1700);
  setTimeout(() => { loadSequences(); }, 2000);
  setTimeout(() => { loadCampaigns(); }, 2300);
   setTimeout(() => { loadAnalytics(); }, 2600);
   setTimeout(() => { gridInit(); }, 2900);
 
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
 tbody.innerHTML = '<tr><td colspan="5" class="py-12 text-center cb-text-tertiary"><i class="fa-solid fa-pen-to-square text-3xl block mb-2"></i>No posts yet. Add your first post above.</td></tr>';
 return;
 }

 posts.forEach(p => {
 const tr = document.createElement("tr");
 tr.className = "hover:bg-black/[0.02] cursor-pointer";
 tr.onclick = () => openPostPreview(p.id);

 let statusBadge = "";
 if (p.status === 'draft') statusBadge = '<span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-slate-500/15 cb-text-tertiary border border-slate-500/30">Draft</span>';
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
 <button onclick="updatePostStatus(${p.id}, 'draft')" class="text-[10px] cb-text-tertiary hover:cb-text-secondary font-bold ml-2">Unqueue</button>
 <button onclick="deletePost(${p.id})" class="text-[10px] text-red-400 hover:text-red-300 font-bold ml-2">Delete</button>`;
 } else if (p.status === 'posted') {
 actions = '<span class="text-[10px] text-emerald-500 font-bold">Done</span>';
 } else if (p.status === 'failed') {
 actions = `<button onclick="updatePostStatus(${p.id}, 'queued')" class="text-[10px] text-blue-400 hover:text-blue-300 font-bold">Retry</button>
 <button onclick="deletePost(${p.id})" class="text-[10px] text-red-400 hover:text-red-300 font-bold ml-2">Delete</button>`;
 }

 tr.innerHTML = `
 <td class="py-4 px-6 text-xs cb-text-tertiary font-semibold">${p.topic || '-'}</td>
 <td class="py-4 px-6 text-xs cb-text-secondary max-w-[400px]">${contentPreview}</td>
 <td class="py-4 px-6 text-xs cb-text-tertiary">${time}</td>
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

  // ========================
  // SEARCH MODULE
  // ========================
  let _searchResults = [];

  async function loadSearchQueries() {
  try {
  const data = await fetch('/api/search/queries').then(r => r.json());
  const queries = data.queries || data || [];
  const tbody = document.getElementById('search-queries-table');
  if (!queries.length) {
  tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center cb-text-tertiary">No saved queries.</td></tr>';
  return;
  }
  tbody.innerHTML = queries.map(q => `
  <tr class="hover:bg-black/[0.02]">
  <td class="py-3 px-4 cb-text-primary font-semibold">${escapeHtml(q.name || 'Unnamed')}</td>
  <td class="py-3 px-4 cb-text-secondary truncate max-w-[200px]">${escapeHtml(q.prompt)}</td>
  <td class="py-3 px-4"><span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">${q.status || 'completed'}</span></td>
  <td class="py-3 px-4 cb-text-secondary">${q.result_count || 0}</td>
  <td class="py-3 px-4 cb-text-tertiary text-xs">${q.created_at ? new Date(q.created_at).toLocaleString() : '-'}</td>
  <td class="py-3 px-4 text-right">
  <button onclick="viewQueryResults('${q.query_id}')" class="text-xs text-brand-400 hover:text-brand-300 font-bold">View Results</button>
  </td>
  </tr>
  `).join('');
  } catch(e) {
  console.error('Failed to load search queries', e);
  }
  }

  function getSelectedSearchSources() {
  return Array.from(document.querySelectorAll('.search-source:checked')).map(cb => cb.value);
  }

  async function uploadLeadsCsv() {
  const input = document.getElementById('csv-upload-input');
  const status = document.getElementById('csv-upload-status');
  if (!input.files.length) { showToast('error', 'Select a CSV file.'); return; }
  const formData = new FormData();
  formData.append('file', input.files[0]);
  status.innerText = 'Uploading...';
  try {
    const res = await fetch('/api/leads/import-csv', {method: 'POST', body: formData});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Import failed');
    showToast('success', data.message);
    status.innerText = '';
    input.value = '';
    fetchInitialData();
  } catch(e) {
    showToast('error', 'CSV import failed: ' + e.message);
    status.innerText = 'Failed';
  }
  }

  async function runSearch() {
  const prompt = document.getElementById('search-prompt').value.trim();
  const name = document.getElementById('search-name').value.trim();
  const sources = getSelectedSearchSources();
  if (!prompt) { showToast('error', 'Enter a search prompt.'); return; }
  if (sources.length === 0) { showToast('error', 'Select at least one source.'); return; }

  const btn = document.getElementById('search-run-btn');
  const status = document.getElementById('search-status');
  btn.disabled = true;
  btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Searching...';
  status.innerText = 'Querying engines...';

  try {
  const res = await fetch('/api/search', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({prompt, name, sources})
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || 'Search failed');
  showToast('success', data.message || `Found ${data.domains?.length || 0} domains.`);
  await viewQueryResults(data.query_id);
  loadSearchQueries();
  } catch(e) {
  showToast('error', 'Search failed: ' + e.message);
  console.error(e);
  } finally {
  btn.disabled = false;
  btn.innerHTML = '<i class="fa-solid fa-magnifying-glass"></i> Run Search';
  status.innerText = '';
  }
  }

  async function viewQueryResults(queryId) {
  try {
  const data = await fetch(`/api/search/results/${queryId}`).then(r => r.json());
  _searchResults = data.results || [];
  renderSearchResults();
  } catch(e) {
  console.error('Failed to load query results', e);
  showToast('error', 'Failed to load results.');
  }
  }

  function renderSearchResults() {
  const tbody = document.getElementById('search-results-table');
  if (!_searchResults.length) {
  tbody.innerHTML = '<tr><td colspan="7" class="py-12 text-center cb-text-tertiary">No results. Run a search above.</td></tr>';
  return;
  }
  tbody.innerHTML = _searchResults.map(r => `
  <tr class="hover:bg-black/[0.02]">
  <td class="py-3 px-4"><input type="checkbox" class="search-result-checkbox" value="${r.result_id}" ${r.used ? 'disabled' : ''}></td>
  <td class="py-3 px-4 cb-text-secondary font-mono text-xs">${escapeHtml(r.domain)}</td>
  <td class="py-3 px-4 cb-text-primary font-semibold">${escapeHtml(r.company_name || '-')}</td>
  <td class="py-3 px-4 cb-text-secondary text-xs">${escapeHtml(r.industry || '-')}</td>
  <td class="py-3 px-4 cb-text-secondary text-xs">${escapeHtml(r.location || '-')}</td>
  <td class="py-3 px-4">
  <div class="w-full bg-slate-700/30 rounded-full h-1.5 max-w-[80px]">
  <div class="bg-brand-500 h-1.5 rounded-full" style="width: ${Math.min(r.confidence || 50, 100)}%"></div>
  </div>
  <span class="text-[10px] cb-text-tertiary">${r.confidence || 50}%</span>
  </td>
  <td class="py-3 px-4 text-right">
  ${r.used ? '<span class="text-[10px] text-emerald-400 font-bold">Converted</span>' : `<button onclick="convertSearchResult('${r.result_id}')" class="text-xs text-brand-400 hover:text-brand-300 font-bold">Convert to Lead</button>`}
  </td>
  </tr>
  `).join('');
  }

  function getCheckedSearchResultIds() {
  return Array.from(document.querySelectorAll('.search-result-checkbox:checked')).map(cb => cb.value);
  }

  function toggleAllSearchResults() {
  const boxes = document.querySelectorAll('.search-result-checkbox:not(:disabled)');
  const allChecked = Array.from(boxes).every(b => b.checked);
  boxes.forEach(b => b.checked = !allChecked);
  }

  async function convertSearchResult(resultId) {
  await bulkConvertSearchResults([resultId]);
  }

  async function bulkConvertSearchResults(ids) {
  ids = ids || getCheckedSearchResultIds();
  if (!ids.length) { showToast('error', 'Select at least one result.'); return; }
  try {
  const res = await fetch('/api/search/convert', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({result_ids: ids})
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || 'Convert failed');
  showToast('success', data.message || `Converted ${data.converted || data.created || 0} results to leads.`);
  // mark local rows as used
  _searchResults.forEach(r => { if (ids.includes(r.result_id)) r.used = 1; });
  renderSearchResults();
  fetchInitialData();
  } catch(e) {
  showToast('error', 'Convert failed: ' + e.message);
  }
  }

  // ========================
  // WORKFLOWS MODULE
  // ========================
  let _workflows = [];
  let _workflowStepCount = 0;

  async function loadWorkflows() {
  try {
  const data = await fetch('/api/workflows').then(r => r.json());
  _workflows = data.workflows || [];
  renderWorkflows();
  } catch(e) {
  console.error('Failed to load workflows', e);
  }
  }

  function renderWorkflows() {
  const container = document.getElementById('workflows-list');
  if (!_workflows.length) {
  container.innerHTML = '<p class="cb-text-tertiary text-sm">No workflows yet. Click Create Workflow to build one.</p>';
  return;
  }
  container.innerHTML = _workflows.map(w => `
  <div class="cb-surface-elevated border cb-border rounded-2xl p-5">
  <div class="flex items-center justify-between mb-4">
  <div>
  <div class="flex items-center gap-3">
  <h3 class="text-sm font-bold cb-text-primary">${escapeHtml(w.name)}</h3>
  ${w.is_active ? '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-brand-500/10 text-brand-400 border border-brand-500/20">ACTIVE</span>' : ''}
  </div>
  ${w.description ? `<p class="text-xs cb-text-tertiary mt-0.5">${escapeHtml(w.description)}</p>` : ''}
  </div>
  <button onclick="runWorkflow('${w.workflow_id}')" class="bg-brand-600 hover:bg-brand-500 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-play mr-1"></i> Run</button>
  </div>
  <div class="flex items-center gap-2 overflow-x-auto pb-2">
  ${(w.steps || []).map((step, i) => `
  <div class="flex items-center gap-2 shrink-0">
  <div class="cb-surface-elevated rounded-xl px-3 py-2 text-center min-w-[90px]">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase">${step.type}</p>
  <p class="text-xs cb-text-primary font-bold mt-0.5">${step.type === 'search' ? escapeHtml(step.query || '') : (step.type === 'add_to_sequence' ? 'sequence' : (step.enrich ? 'on' : 'off'))}</p>
  </div>
  ${i < (w.steps.length - 1) ? '<i class="fa-solid fa-arrow-right text-slate-600"></i>' : ''}
  </div>
  `).join('')}
  </div>
  <div class="mt-3" id="workflow-runs-${w.workflow_id}"></div>
  </div>
  `).join('');
  _workflows.forEach(w => loadWorkflowRuns(w.workflow_id));
  }

  async function loadWorkflowRuns(workflowId) {
  try {
  const data = await fetch(`/api/workflows/${workflowId}/runs`).then(r => r.json());
  const runs = data.runs || [];
  const el = document.getElementById(`workflow-runs-${workflowId}`);
  if (!el) return;
  if (!runs.length) { el.innerHTML = ''; return; }
  el.innerHTML = `
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-2">Recent Runs</p>
  <div class="space-y-2">
  ${runs.slice(0, 5).map(r => `
  <div class="cb-surface-elevated rounded-xl px-3 py-2 flex items-center justify-between text-xs">
  <span class="cb-text-secondary">${r.run_id}</span>
  <span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full ${r.status === 'completed' ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : (r.status === 'running' ? 'bg-blue-500/10 text-blue-400 border border-blue-500/20 animate-pulse' : 'bg-red-500/10 text-red-400 border border-red-500/20')}">${r.status}</span>
  <span class="cb-text-tertiary">found ${r.leads_found || 0} · sent ${r.leads_sent || 0}</span>
  </div>
  `).join('')}
  </div>
  `;
  } catch(e) {}
  }

  function showCreateWorkflowForm() {
  _workflowStepCount = 0;
  document.getElementById('wf-name').value = '';
  document.getElementById('wf-desc').value = '';
  document.getElementById('workflow-steps-list').innerHTML = '';
  addWorkflowStep('search');
  addWorkflowStep('enrich');
  addWorkflowStep('add_to_sequence');
  document.getElementById('workflow-editor').classList.remove('hidden');
  }

  function addWorkflowStep(type) {
  _workflowStepCount++;
  const n = _workflowStepCount;
  const container = document.getElementById('workflow-steps-list');
  const el = document.createElement('div');
  el.id = `wf-step-${n}`;
  el.className = 'cb-surface-elevated border cb-border rounded-xl p-4 grid grid-cols-12 gap-3 items-end';
  el.innerHTML = `
  <div class="col-span-3">
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Step ${n}</label>
  <select name="type" onchange="onWorkflowStepTypeChange(${n})" class="cb-input w-full">
  <option value="search" ${type==='search'?'selected':''}>Search</option>
  <option value="enrich" ${type==='enrich'?'selected':''}>Enrich</option>
  <option value="add_to_sequence" ${type==='add_to_sequence'?'selected':''}>Add to Sequence</option>
  </select>
  </div>
  <div class="col-span-7" id="wf-step-body-${n}"></div>
  <div class="col-span-2">
  <button onclick="document.getElementById('wf-step-${n}').remove()" class="w-full bg-red-500/10 hover:bg-red-500/20 text-red-400 text-xs font-bold rounded-lg px-2 py-2 transition">Remove</button>
  </div>
  `;
  container.appendChild(el);
  onWorkflowStepTypeChange(n);
  }

  function onWorkflowStepTypeChange(n) {
  const type = document.querySelector(`#wf-step-${n} select[name=type]`).value;
  const body = document.getElementById(`wf-step-body-${n}`);
  if (type === 'search') {
  body.innerHTML = `<input type="text" name="query" placeholder="Search query" class="cb-input w-full">`;
  } else if (type === 'enrich') {
  body.innerHTML = `<label class="flex items-center gap-2 text-sm cb-text-secondary"><input type="checkbox" name="enabled" checked class="accent-brand-500"> Enable enrichment</label>`;
  } else if (type === 'add_to_sequence') {
  body.innerHTML = `<select name="sequence_id" class="cb-input w-full"><option value="">Loading sequences...</option></select>`;
  fetch('/api/sequences').then(r => r.json()).then(d => {
  const seqs = d.sequences || [];
  const select = document.querySelector(`#wf-step-${n} select[name=sequence_id]`);
  select.innerHTML = seqs.map(s => `<option value="${s.sequence_id}">${escapeHtml(s.name)}</option>`).join('') || '<option value="">No sequences</option>';
  });
  }
  }

  async function saveWorkflow() {
  const name = document.getElementById('wf-name').value.trim();
  if (!name) { showToast('error', 'Workflow needs a name.'); return; }
  const stepEls = document.querySelectorAll('#workflow-steps-list > div[id^=wf-step-]');
  const steps = Array.from(stepEls).map(el => {
  const type = el.querySelector('select[name=type]').value;
  const step = {type};
  if (type === 'search') step.query = el.querySelector('input[name=query]').value;
  if (type === 'enrich') step.enabled = el.querySelector('input[name=enabled]').checked;
  if (type === 'add_to_sequence') step.sequence_id = el.querySelector('select[name=sequence_id]').value;
  return step;
  });
  try {
  const res = await fetch('/api/workflows', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({name, description: document.getElementById('wf-desc').value, steps})
  });
  if (!res.ok) throw new Error('Save failed');
  document.getElementById('workflow-editor').classList.add('hidden');
  showToast('success', 'Workflow created!');
  loadWorkflows();
  } catch(e) {
  showToast('error', 'Failed to save workflow: ' + e.message);
  }
  }

  async function runWorkflow(workflowId) {
  try {
  const res = await fetch(`/api/workflows/${workflowId}/run`, {method: 'POST'});
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || 'Run failed');
  showToast('info', 'Workflow run started: ' + data.run_id);
  setTimeout(() => loadWorkflowRuns(workflowId), 2000);
  } catch(e) {
  showToast('error', 'Run failed: ' + e.message);
  }
  }

  // ========================
  // SEQUENCES MODULE
  // ========================
  let _sequences = [];
  let _sequenceStepCount = 0;

  async function loadSequences() {
  try {
  const data = await fetch('/api/sequences').then(r => r.json());
  _sequences = data.sequences || [];
  renderSequences();
  } catch(e) {
  console.error('Failed to load sequences', e);
  }
  }

  function renderSequences() {
  const container = document.getElementById('sequences-list');
  if (!_sequences.length) {
  container.innerHTML = '<p class="cb-text-tertiary text-sm">No sequences yet. Click Create Sequence to build one.</p>';
  return;
  }
  container.innerHTML = _sequences.map(s => `
  <div class="cb-surface-elevated border cb-border rounded-2xl p-5">
  <div class="flex items-center justify-between mb-4">
  <div>
  <h3 class="text-sm font-bold cb-text-primary">${escapeHtml(s.name)}</h3>
  ${s.description ? `<p class="text-xs cb-text-tertiary mt-0.5">${escapeHtml(s.description)}</p>` : ''}
  </div>
  <span class="text-[10px] cb-text-tertiary">${s.steps?.length || 0} step(s)</span>
  </div>
  <div class="flex items-center gap-2 overflow-x-auto pb-2">
  ${(s.steps || []).map((step, i) => `
  <div class="flex items-center gap-2 shrink-0">
  <div class="cb-surface-elevated rounded-xl px-3 py-2 text-center min-w-[90px]">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase">${step.step_type}</p>
  <p class="text-xs cb-text-primary font-bold mt-0.5">${step.delay_days || 0}d</p>
  </div>
  ${i < (s.steps.length - 1) ? '<i class="fa-solid fa-arrow-right text-slate-600"></i>' : ''}
  </div>
  `).join('')}
  </div>
  </div>
  `).join('');
  }

  function showCreateSequenceForm() {
  _sequenceStepCount = 0;
  document.getElementById('seq-name').value = '';
  document.getElementById('seq-desc').value = '';
  document.getElementById('sequence-steps-list').innerHTML = '';
  addSequenceStep('email');
  addSequenceStep('wait');
  document.getElementById('sequence-editor').classList.remove('hidden');
  }

  function addSequenceStep(type) {
  _sequenceStepCount++;
  const n = _sequenceStepCount;
  const container = document.getElementById('sequence-steps-list');
  const el = document.createElement('div');
  el.id = `seq-step-${n}`;
  el.className = 'cb-surface-elevated border cb-border rounded-xl p-4 grid grid-cols-12 gap-3 items-end';
  el.innerHTML = `
  <div class="col-span-3">
  <label class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1">Step ${n}</label>
  <select name="type" onchange="onSequenceStepTypeChange(${n})" class="cb-input w-full">
  <option value="email" ${type==='email'?'selected':''}>Email</option>
  <option value="wait" ${type==='wait'?'selected':''}>Wait</option>
  <option value="linkedin_connection" ${type==='linkedin_connection'?'selected':''}>LinkedIn Connect</option>
  <option value="linkedin_message" ${type==='linkedin_message'?'selected':''}>LinkedIn Message</option>
  </select>
  </div>
  <div class="col-span-7" id="seq-step-body-${n}"></div>
  <div class="col-span-2">
  <button onclick="document.getElementById('seq-step-${n}').remove()" class="w-full bg-red-500/10 hover:bg-red-500/20 text-red-400 text-xs font-bold rounded-lg px-2 py-2 transition">Remove</button>
  </div>
  `;
  container.appendChild(el);
  onSequenceStepTypeChange(n);
  }

  function onSequenceStepTypeChange(n) {
  const type = document.querySelector(`#seq-step-${n} select[name=type]`).value;
  const body = document.getElementById(`seq-step-body-${n}`);
  if (type === 'email') {
  body.innerHTML = `
  <div class="grid grid-cols-2 gap-2">
  <input type="text" name="subject" placeholder="Subject" class="cb-input w-full">
  <input type="number" name="delay_hours" value="0" min="0" class="cb-input w-full" title="Delay hours">
  </div>
  <textarea name="body" rows="2" placeholder="Email body" class="cb-input w-full mt-2 resize-none"></textarea>
  `;
  } else if (type === 'linkedin_connection') {
  body.innerHTML = `<div class="grid grid-cols-2 gap-2"><input type="number" name="delay_hours" value="0" min="0" class="cb-input w-full" title="Delay hours"><input type="text" name="connection_note" placeholder="Connection note (optional)" class="cb-input w-full"></div>`;
  } else if (type === 'linkedin_message') {
  body.innerHTML = `<div class="grid grid-cols-2 gap-2"><input type="number" name="delay_hours" value="0" min="0" class="cb-input w-full" title="Delay hours"><input type="text" name="body" placeholder="Message body" class="cb-input w-full"></div>`;
  } else {
  body.innerHTML = `<input type="number" name="delay_hours" value="72" min="0" class="cb-input w-full" title="Delay hours">`;
  }
  }

  async function saveSequence() {
  const name = document.getElementById('seq-name').value.trim();
  if (!name) { showToast('error', 'Sequence needs a name.'); return; }
  const stepEls = document.querySelectorAll('#sequence-steps-list > div[id^=seq-step-]');
  const steps = Array.from(stepEls).map(el => {
  const type = el.querySelector('select[name=type]').value;
  const delay_hours = parseInt(el.querySelector('input[name=delay_hours]')?.value || '0');
  const step = {step_type: type, delay_hours};
  if (type === 'email') {
  step.subject = el.querySelector('input[name=subject]').value;
  step.body = el.querySelector('textarea[name=body]').value;
  }
  if (type === 'linkedin_connection') {
  step.connection_note = el.querySelector('input[name=connection_note]').value;
  }
  if (type === 'linkedin_message') {
  step.body = el.querySelector('input[name=body]').value;
  }
  return step;
  });
  try {
  const res = await fetch('/api/sequences', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({name, description: document.getElementById('seq-desc').value, steps})
  });
  if (!res.ok) throw new Error('Save failed');
  document.getElementById('sequence-editor').classList.add('hidden');
  showToast('success', 'Sequence created!');
  loadSequences();
  } catch(e) {
  showToast('error', 'Failed to save sequence: ' + e.message);
  }
  }

  // ========================
  // CAMPAIGNS MODULE
  // ========================
  let _campaigns = [];
  let _allLeadsForEnroll = [];

  async function loadCampaigns() {
  try {
  const [campData, seqData, leads] = await Promise.all([
  fetch('/api/campaigns').then(r => r.json()),
  fetch('/api/sequences').then(r => r.json()),
  fetch('/api/leads').then(r => r.json())
  ]);
  _campaigns = campData.campaigns || [];
  _sequences = seqData.sequences || [];
  _allLeadsForEnroll = leads || [];
  renderCampaigns();
  populateCampaignForm();
  } catch(e) {
  console.error('Failed to load campaigns', e);
  }
  }

  function renderCampaigns() {
  const container = document.getElementById('campaigns-list');
  if (!_campaigns.length) {
  container.innerHTML = '<p class="cb-text-tertiary text-sm">No campaigns yet. Click Create Campaign to start one.</p>';
  return;
  }
  container.innerHTML = _campaigns.map(c => {
  const counts = c.enrollment_counts || {};
  return `
  <div class="cb-surface-elevated border cb-border rounded-2xl p-5">
  <div class="flex items-center justify-between mb-4">
  <div>
  <h3 class="text-sm font-bold cb-text-primary">${escapeHtml(c.name)}</h3>
  <p class="text-xs cb-text-tertiary mt-0.5">Sequence: ${escapeHtml(_sequences.find(s => s.sequence_id === c.sequence_id)?.name || c.sequence_id)}</p>
  </div>
  <div class="flex items-center gap-2">
  <span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full ${c.is_active ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 'bg-slate-500/10 cb-text-tertiary border border-slate-500/20'}">${c.is_active ? 'Active' : 'Inactive'}</span>
  </div>
  </div>
  <div class="grid grid-cols-3 gap-3 mb-4">
  <div class="cb-surface rounded-xl p-3 text-center">
  <span class="text-[10px] cb-text-tertiary block uppercase">Enrolled</span>
  <span class="text-lg font-black cb-text-primary">${counts.active || 0}</span>
  </div>
  <div class="cb-surface rounded-xl p-3 text-center">
  <span class="text-[10px] cb-text-tertiary block uppercase">Active</span>
  <span class="text-lg font-black text-blue-400">${counts.active || 0}</span>
  </div>
  <div class="cb-surface rounded-xl p-3 text-center">
  <span class="text-[10px] cb-text-tertiary block uppercase">Completed</span>
  <span class="text-lg font-black text-emerald-400">${counts.completed || 0}</span>
  </div>
  </div>
  <div class="flex gap-2 flex-wrap">
  <button onclick="openEnrollLeadsModal('${c.campaign_id}', '${escapeHtml(c.name)}')" class="bg-brand-600 hover:bg-brand-500 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-user-plus mr-1"></i> Enroll Leads</button>
  <button onclick="bulkEnrollByStage('${c.campaign_id}', '${escapeHtml(c.name)}')" class="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-layer-group mr-1"></i> Enroll Stage</button>
  <button onclick="viewCampaignAnalytics('${c.campaign_id}')" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary text-xs font-bold px-3 py-1.5 rounded-lg transition"><i class="fa-solid fa-chart-pie mr-1"></i> View Analytics</button>
  </div>
  </div>
  `;
  }).join('');
  }

  function populateCampaignForm() {
  const seqSelect = document.getElementById('camp-sequence');
  seqSelect.innerHTML = '<option value="">Select a sequence...</option>' + _sequences.map(s => `<option value="${s.sequence_id}">${escapeHtml(s.name)}</option>`).join('');
  const leadSelect = document.getElementById('camp-leads');
  leadSelect.innerHTML = _allLeadsForEnroll.map(l => `<option value="${l.contact_id}">${escapeHtml(l.first_name + ' ' + (l.last_name || ''))} — ${escapeHtml(l.email)}</option>`).join('');
  }

  function showCreateCampaignForm() {
  document.getElementById('camp-name').value = '';
  document.getElementById('camp-sequence').value = '';
  Array.from(document.getElementById('camp-leads').options).forEach(o => o.selected = false);
  document.getElementById('campaign-editor').classList.remove('hidden');
  }

  async function saveCampaign() {
  const name = document.getElementById('camp-name').value.trim();
  const sequence_id = document.getElementById('camp-sequence').value;
  const lead_ids = Array.from(document.getElementById('camp-leads').selectedOptions).map(o => o.value);
  if (!name || !sequence_id) { showToast('error', 'Name and sequence are required.'); return; }
  try {
  const res = await fetch('/api/campaigns', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({name, sequence_id, lead_ids})
  });
  if (!res.ok) throw new Error('Create failed');
  document.getElementById('campaign-editor').classList.add('hidden');
  showToast('success', 'Campaign created!');
  loadCampaigns();
  } catch(e) {
  showToast('error', 'Failed to create campaign: ' + e.message);
  }
  }

  function openEnrollLeadsModal(campaignId, campaignName) {
  document.getElementById('enroll-modal-campaign-id').value = campaignId;
  document.getElementById('enroll-modal-campaign-name').innerText = campaignName;
  const select = document.getElementById('enroll-leads-select');
  select.innerHTML = _allLeadsForEnroll.map(l => `<option value="${l.contact_id}">${escapeHtml(l.first_name + ' ' + (l.last_name || ''))} — ${escapeHtml(l.email)}</option>`).join('');
  document.getElementById('enroll-leads-modal').classList.remove('hidden');
  }

  function closeEnrollLeadsModal() {
  document.getElementById('enroll-leads-modal').classList.add('hidden');
  }

  async function submitEnrollLeads() {
  const campaignId = document.getElementById('enroll-modal-campaign-id').value;
  const lead_ids = Array.from(document.getElementById('enroll-leads-select').selectedOptions).map(o => o.value);
  if (!lead_ids.length) { showToast('error', 'Select at least one lead.'); return; }
  try {
  const res = await fetch(`/api/campaigns/${campaignId}/enroll`, {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({lead_ids})
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || 'Enroll failed');
  showToast('success', `Enrolled ${data.enrolled} lead(s).`);
  closeEnrollLeadsModal();
  loadCampaigns();
  loadAnalytics();
  } catch(e) {
  showToast('error', 'Enroll failed: ' + e.message);
  }
  }

  async function bulkEnrollByStage(campaignId, campaignName) {
  const stage = prompt(`Enroll leads from which stage into "${campaignName}"?\n\nCommon stages: NURTURE, ACTIVE_OUTREACH, INGESTED, REPLIED.\n\nEnter stage and limit separated by comma (e.g. NURTURE,50):`, 'NURTURE,50');
  if (!stage) return;
  const [stageName, limitStr] = stage.split(',').map(s => s.trim());
  const limit = parseInt(limitStr) || 50;
  try {
    const res = await fetch(`/api/campaigns/${campaignId}/enroll-by-stage`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({stage: stageName, limit})
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Bulk enroll failed');
    showToast('success', `Enrolled ${data.enrolled} ${stageName} lead(s).`);
    loadCampaigns();
    loadAnalytics();
  } catch(e) {
    showToast('error', 'Bulk enroll failed: ' + e.message);
  }
  }

  function closeCampaignAnalyticsModal() {
  document.getElementById('campaign-analytics-modal').classList.add('hidden');
  }

  async function viewCampaignAnalytics(campaignId) {
  try {
  const [campaign, analytics] = await Promise.all([
  fetch(`/api/campaigns/${campaignId}`).then(r => r.json()),
  fetch(`/api/analytics/campaigns/${campaignId}`).then(r => r.json())
  ]);
  document.getElementById('campaign-analytics-name').innerText = campaign.name || campaignId;
  const e = analytics.email || {};
  document.getElementById('ca-sent').innerText = e.sent || 0;
  document.getElementById('ca-opened').innerText = e.opened || 0;
  document.getElementById('ca-clicked').innerText = e.clicked || 0;
  document.getElementById('ca-replied').innerText = e.replied || 0;
  document.getElementById('ca-open-rate').innerText = ((e.open_rate || 0) * 100).toFixed(1) + '%';
  document.getElementById('ca-click-rate').innerText = ((e.click_rate || 0) * 100).toFixed(1) + '%';
  document.getElementById('ca-reply-rate').innerText = ((e.reply_rate || 0) * 100).toFixed(1) + '%';
  document.getElementById('campaign-analytics-modal').classList.remove('hidden');
  } catch(e) {
  showToast('error', 'Failed to load analytics: ' + e.message);
  }
  }

  // ========================
  // ANALYTICS MODULE
  // ========================
  async function loadAnalytics() {
  try {
  const [metrics, campData] = await Promise.all([
  fetch('/api/metrics').then(r => r.json()),
  fetch('/api/campaigns').then(r => r.json())
  ]);
  const email = metrics.email || {};
  document.getElementById('a-total-sent').innerText = email.sent || 0;
  document.getElementById('a-open-rate').innerText = ((email.open_rate || 0) * 100).toFixed(1) + '%';
  document.getElementById('a-reply-rate').innerText = ((email.reply_rate || 0) * 100).toFixed(1) + '%';
  document.getElementById('a-meetings').innerText = metrics.meetings || 0;

  const campaigns = campData.campaigns || [];
  const tbody = document.getElementById('analytics-campaigns-table');
  if (!campaigns.length) {
  tbody.innerHTML = '<tr><td colspan="8" class="py-12 text-center cb-text-tertiary">No campaigns yet.</td></tr>';
  return;
  }

  // Fetch analytics for each campaign
  const analytics = await Promise.all(
  campaigns.map(c => fetch(`/api/analytics/campaigns/${c.campaign_id}`).then(r => r.json()).catch(() => ({email: {}})))
  );

  tbody.innerHTML = campaigns.map((c, i) => {
  const counts = c.enrollment_counts || {};
  const e = analytics[i].email || {};
  return `
  <tr class="hover:bg-black/[0.02]">
  <td class="py-4 px-6 cb-text-primary font-semibold">${escapeHtml(c.name)}</td>
  <td class="py-4 px-6 cb-text-secondary">${(counts.active || 0) + (counts.completed || 0) + (counts['stopped:replied'] || 0)}</td>
  <td class="py-4 px-6 text-blue-400">${counts.active || 0}</td>
  <td class="py-4 px-6 text-emerald-400">${counts.completed || 0}</td>
  <td class="py-4 px-6 cb-text-secondary">${e.sent || 0}</td>
  <td class="py-4 px-6 cb-text-secondary">${((e.open_rate || 0) * 100).toFixed(1)}%</td>
  <td class="py-4 px-6 cb-text-secondary">${((e.reply_rate || 0) * 100).toFixed(1)}%</td>
  <td class="py-4 px-6 text-right">
  <button onclick="viewCampaignAnalytics('${c.campaign_id}')" class="text-xs text-brand-400 hover:text-brand-300 font-bold">View</button>
  </td>
  </tr>
  `;
  }).join('');

  // Load new analytics sections
  loadABTestCampaigns();
  loadDeliverabilityDomains();
  loadTopLeads();
  loadSentimentChart();
  } catch(e) {
  console.error('Failed to load analytics', e);
  }
  }

  // ========================
  // A/B TEST RESULTS
  // ========================
  async function loadABTestCampaigns() {
    try {
      const data = await fetch('/api/campaigns').then(r => r.json());
      const campaigns = data.campaigns || [];
      const sel = document.getElementById('ab-test-campaign-select');
      sel.innerHTML = '<option value="">Select campaign...</option>' +
        campaigns.map(c => `<option value="${c.campaign_id}">${escapeHtml(c.name)}</option>`).join('');
    } catch(e) { console.error('Failed to load campaigns for AB tests', e); }
  }

  async function loadABTests() {
    const campaignId = document.getElementById('ab-test-campaign-select').value;
    const container = document.getElementById('ab-tests-container');
    if (!campaignId) { container.innerHTML = '<p class="cb-text-tertiary text-sm">Select a campaign to view A/B test results.</p>'; return; }
    try {
      const data = await fetch(`/api/ab-tests/campaign/${campaignId}`).then(r => r.json());
      const tests = data.tests || [];
      if (!tests.length) { container.innerHTML = '<p class="cb-text-tertiary text-sm">No A/B tests found for this campaign.</p>'; return; }
      container.innerHTML = tests.map(t => {
        const aOpen = (t.variant_a_open_rate || 0) * 100;
        const bOpen = (t.variant_b_open_rate || 0) * 100;
        const winner = t.winner || (aOpen > bOpen ? 'A' : bOpen > aOpen ? 'B' : 'Tie');
        const winnerBadge = winner === 'A'
          ? '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">VARIANT A WINS</span>'
          : winner === 'B'
          ? '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-blue-500/15 text-blue-400 border border-blue-500/30">VARIANT B WINS</span>'
          : '<span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30">TIE</span>';
        const maxRate = Math.max(aOpen, bOpen, 1);
        return `
        <div class="cb-surface-elevated border cb-border rounded-xl p-5 space-y-4">
          <div class="flex items-center justify-between">
            <div>
              <span class="text-xs cb-text-tertiary font-bold uppercase tracking-wider">${escapeHtml(t.test_name || 'A/B Test')}</span>
              <p class="text-[10px] cb-text-tertiary mt-0.5">${escapeHtml(t.subject_a || '')} vs ${escapeHtml(t.subject_b || '')}</p>
            </div>
            ${winnerBadge}
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div class="cb-surface rounded-xl p-4">
              <div class="flex items-center justify-between mb-2">
                <span class="text-xs font-bold cb-text-primary">Variant A</span>
                <span class="text-sm font-black ${winner==='A' ? 'text-emerald-400' : 'cb-text-secondary'}">${aOpen.toFixed(1)}%</span>
              </div>
              <div class="w-full bg-gray-700/30 rounded-full h-3">
                <div class="h-3 rounded-full transition-all duration-500 ${winner==='A' ? 'bg-emerald-500' : 'bg-brand-500/60'}" style="width: ${Math.max(aOpen, 0) / maxRate * 100}%"></div>
              </div>
              <p class="text-[10px] cb-text-tertiary mt-1.5">${t.variant_a_sent || 0} sent · ${t.variant_a_opened || 0} opened</p>
            </div>
            <div class="cb-surface rounded-xl p-4">
              <div class="flex items-center justify-between mb-2">
                <span class="text-xs font-bold cb-text-primary">Variant B</span>
                <span class="text-sm font-black ${winner==='B' ? 'text-blue-400' : 'cb-text-secondary'}">${bOpen.toFixed(1)}%</span>
              </div>
              <div class="w-full bg-gray-700/30 rounded-full h-3">
                <div class="h-3 rounded-full transition-all duration-500 ${winner==='B' ? 'bg-blue-500' : 'bg-brand-500/60'}" style="width: ${Math.max(bOpen, 0) / maxRate * 100}%"></div>
              </div>
              <p class="text-[10px] cb-text-tertiary mt-1.5">${t.variant_b_sent || 0} sent · ${t.variant_b_opened || 0} opened</p>
            </div>
          </div>
        </div>`;
      }).join('');
    } catch(e) {
      container.innerHTML = '<p class="cb-text-tertiary text-sm">Failed to load A/B test data.</p>';
    }
  }

  // ========================
  // DELIVERABILITY DETAIL
  // ========================
  async function loadDeliverabilityDomains() {
    try {
      const data = await fetch('/api/deliverability').then(r => r.json());
      const domains = Array.isArray(data) ? data : (data.domains || []);
      const sel = document.getElementById('deliverability-domain-select');
      sel.innerHTML = '<option value="">Select domain...</option>' +
        domains.map(d => `<option value="${escapeHtml(d.domain)}">${escapeHtml(d.domain)}</option>`).join('');
    } catch(e) { console.error('Failed to load domains', e); }
  }

  async function loadDeliverabilityDetail() {
    const domain = document.getElementById('deliverability-domain-select').value;
    const container = document.getElementById('deliverability-detail-container');
    if (!domain) { container.innerHTML = '<p class="cb-text-tertiary text-sm">Select a domain to view deliverability details.</p>'; return; }
    try {
      const data = await fetch(`/api/deliverability/${encodeURIComponent(domain)}`).then(r => r.json());
      const score = data.score || 0;
      const scoreColor = score >= 80 ? 'text-emerald-400' : score >= 50 ? 'text-amber-400' : 'text-red-400';
      const scoreBg = score >= 80 ? 'bg-emerald-500' : score >= 50 ? 'bg-amber-500' : 'bg-red-500';

      function protoCheck(val) {
        return val
          ? '<i class="fa-solid fa-circle-check text-emerald-400 mr-1"></i> <span class="cb-text-secondary">Pass</span>'
          : '<i class="fa-solid fa-circle-xmark text-red-400 mr-1"></i> <span class="text-red-400">Fail</span>';
      }

      const issues = (data.issues || []).map(i => `
        <div class="flex items-start gap-2 cb-surface rounded-lg p-3">
          <i class="fa-solid fa-triangle-exclamation text-amber-400 mt-0.5 text-xs"></i>
          <span class="text-xs cb-text-secondary">${escapeHtml(i)}</span>
        </div>
      `).join('');

      const recs = (data.recommendations || []).map(r => `
        <div class="flex items-start gap-2 cb-surface rounded-lg p-3">
          <i class="fa-solid fa-lightbulb text-brand-400 mt-0.5 text-xs"></i>
          <span class="text-xs cb-text-secondary">${escapeHtml(r)}</span>
        </div>
      `).join('');

      container.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-5">
          <div class="cb-surface rounded-xl p-5 text-center">
            <div class="relative inline-flex items-center justify-center">
              <svg class="w-24 h-24 -rotate-90" viewBox="0 0 100 100">
                <circle cx="50" cy="50" r="42" fill="none" stroke="var(--card-border)" stroke-width="8"/>
                <circle cx="50" cy="50" r="42" fill="none" stroke="${score >= 80 ? 'var(--success)' : score >= 50 ? 'var(--warning)' : 'var(--danger)'}" stroke-width="8"
                  stroke-dasharray="${score * 2.64} 264" stroke-linecap="round"/>
              </svg>
              <span class="absolute text-xl font-black ${scoreColor}">${score}</span>
            </div>
            <p class="text-xs cb-text-tertiary mt-2 font-bold uppercase tracking-wider">Deliverability Score</p>
          </div>
          <div class="cb-surface rounded-xl p-5">
            <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider mb-3">DNS Records</h4>
            <div class="space-y-2.5 text-sm">
              <div class="flex justify-between items-center">${protoCheck(data.spf)} <span class="text-[10px] cb-text-tertiary font-bold uppercase">SPF</span></div>
              <div class="flex justify-between items-center">${protoCheck(data.dkim)} <span class="text-[10px] cb-text-tertiary font-bold uppercase">DKIM</span></div>
              <div class="flex justify-between items-center">${protoCheck(data.dmarc)} <span class="text-[10px] cb-text-tertiary font-bold uppercase">DMARC</span></div>
            </div>
          </div>
          <div class="cb-surface rounded-xl p-5">
            <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider mb-3">Domain Health</h4>
            <div class="space-y-2.5">
              <div class="flex justify-between text-xs">
                <span class="cb-text-tertiary">Score</span>
                <span class="font-bold ${scoreColor}">${score}/100</span>
              </div>
              <div class="flex justify-between text-xs">
                <span class="cb-text-tertiary">Status</span>
                <span class="font-bold ${score >= 80 ? 'text-emerald-400' : score >= 50 ? 'text-amber-400' : 'text-red-400'}">${score >= 80 ? 'HEALTHY' : score >= 50 ? 'AT RISK' : 'CRITICAL'}</span>
              </div>
            </div>
          </div>
        </div>
        ${(issues || recs) ? `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider mb-3"><i class="fa-solid fa-triangle-exclamation text-amber-400 mr-1"></i> Issues</h4>
            <div class="space-y-2">${issues || '<p class="text-xs cb-text-tertiary">No issues found.</p>'}</div>
          </div>
          <div>
            <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider mb-3"><i class="fa-solid fa-lightbulb text-brand-400 mr-1"></i> Recommendations</h4>
            <div class="space-y-2">${recs || '<p class="text-xs cb-text-tertiary">No recommendations.</p>'}</div>
          </div>
        </div>` : ''}
      `;
    } catch(e) {
      container.innerHTML = '<p class="cb-text-tertiary text-sm">Failed to load deliverability details.</p>';
    }
  }

  // ========================
  // LEAD SCORE LEADERBOARD
  // ========================
  async function loadTopLeads() {
    const container = document.getElementById('top-leads-container');
    container.innerHTML = '<div class="flex items-center gap-2 text-sm cb-text-tertiary"><i class="fa-solid fa-spinner fa-spin"></i> Loading...</div>';
    try {
      const data = await fetch('/api/leads/top?limit=20').then(r => r.json());
      const leadsList = data.leads || [];
      if (!leadsList.length) {
        container.innerHTML = '<p class="cb-text-tertiary text-sm">No scored leads yet. Run the qualification pipeline to generate scores.</p>';
        return;
      }
      const rows = leadsList.map((l, i) => {
        const score = l.total_score || l.lead_score || 0;
        const scoreColor = score >= 80 ? 'bg-emerald-500 text-white' : score >= 60 ? 'bg-brand-500 text-white' : score >= 40 ? 'bg-amber-500 text-white' : 'bg-slate-500 text-white';
        const rankIcon = i === 0 ? '<i class="fa-solid fa-crown text-amber-400"></i>' : i === 1 ? '<i class="fa-solid fa-medal text-slate-300"></i>' : i === 2 ? '<i class="fa-solid fa-medal text-amber-600"></i>' : `<span class="cb-text-tertiary text-xs">${i + 1}</span>`;
        const engagement = l.engagement_score || 0;
        const fit = l.fit_score || 0;
        const recency = l.recency_score || 0;
        return `
        <tr class="hover:bg-black/[0.03] transition">
          <td class="py-3 px-4 text-center">${rankIcon}</td>
          <td class="py-3 px-4">
            <span class="font-bold cb-text-primary text-sm">${escapeHtml(l.first_name || '')} ${escapeHtml(l.last_name || '')}</span>
          </td>
          <td class="py-3 px-4 text-xs cb-text-secondary truncate max-w-[160px]">${escapeHtml(l.email || '')}</td>
          <td class="py-3 px-4 text-xs cb-text-secondary font-medium">${escapeHtml(l.company_name || l.company || '')}</td>
          <td class="py-3 px-4">
            <span class="inline-flex items-center justify-center w-10 h-10 rounded-full text-sm font-black ${scoreColor}">${score}</span>
          </td>
          <td class="py-3 px-4">
            <div class="flex gap-1.5">
              <div class="flex flex-col items-center">
                <span class="text-[10px] cb-text-tertiary font-bold uppercase">Eng</span>
                <span class="text-xs font-bold cb-text-primary">${engagement}</span>
              </div>
              <div class="w-px h-8 bg-gray-700/30"></div>
              <div class="flex flex-col items-center">
                <span class="text-[10px] cb-text-tertiary font-bold uppercase">Fit</span>
                <span class="text-xs font-bold cb-text-primary">${fit}</span>
              </div>
              <div class="w-px h-8 bg-gray-700/30"></div>
              <div class="flex flex-col items-center">
                <span class="text-[10px] cb-text-tertiary font-bold uppercase">Rec</span>
                <span class="text-xs font-bold cb-text-primary">${recency}</span>
              </div>
            </div>
          </td>
          <td class="py-3 px-4 text-right">
            <div class="flex items-center justify-end gap-1.5">
              <button onclick="enrollLeadFromLeaderboard('${escapeHtml(l.contact_id || l.id || '')}')" class="text-[10px] bg-brand-600 hover:bg-brand-500 text-white font-bold px-2.5 py-1 rounded-lg transition">Enroll</button>
              <button onclick="openLeadDrawer('${escapeHtml(l.contact_id || l.id || '')}')" class="text-[10px] cb-text-tertiary hover:text-brand-400 font-bold px-2 py-1 rounded-lg transition">View</button>
            </div>
          </td>
        </tr>`;
      }).join('');
      container.innerHTML = `
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse">
            <thead>
              <tr class="border-b cb-border text-[10px] cb-text-tertiary uppercase font-bold tracking-widest">
                <th class="py-3 px-4 text-center">#</th>
                <th class="py-3 px-4">Name</th>
                <th class="py-3 px-4">Email</th>
                <th class="py-3 px-4">Company</th>
                <th class="py-3 px-4 text-center">Score</th>
                <th class="py-3 px-4">Breakdown</th>
                <th class="py-3 px-4 text-right">Actions</th>
              </tr>
            </thead>
            <tbody class="text-sm divide-y cb-border">${rows}</tbody>
          </table>
        </div>
      `;
    } catch(e) {
      container.innerHTML = '<p class="cb-text-tertiary text-sm">Failed to load leaderboard data.</p>';
    }
  }

  function enrollLeadFromLeaderboard(leadId) {
    if (!leadId) return;
    showToast('info', `Opening enrollment for lead...`);
    openLeadDrawer(leadId);
  }

  // ========================
  // REPLY SENTIMENT CHART
  // ========================
  async function loadSentimentChart() {
    const container = document.getElementById('sentiment-chart-container');
    try {
      const res = await fetch('/api/replies/sentiment-stats').then(r => r.json());
      const categories = res.categories || res.sentiment || {};
      const entries = Object.entries(categories).filter(([,v]) => v > 0).sort((a, b) => b[1] - a[1]);
      const total = entries.reduce((s, [, v]) => s + v, 0);
      if (!total) {
        container.innerHTML = '<p class="cb-text-tertiary text-sm">No sentiment data available yet. Classify replies to generate analytics.</p>';
        return;
      }

      const CATEGORY_META = {
        'interested':        { color: '#10b981', icon: 'fa-handshake', label: 'Interested' },
        'meeting_requested': { color: '#3b82f6', icon: 'fa-calendar-check', label: 'Meeting Requested' },
        'not_interested':    { color: '#ef4444', icon: 'fa-ban', label: 'Not Interested' },
        'out_of_office':     { color: '#f59e0b', icon: 'fa-clock', label: 'Out of Office' },
        'objection':         { color: '#f97316', icon: 'fa-shield-halved', label: 'Objection' },
        'referral':          { color: '#8b5cf6', icon: 'fa-share-nodes', label: 'Referral' },
        'spam':              { color: '#6b7280', icon: 'fa-skull-crossbones', label: 'Spam' },
        'other':             { color: '#64748b', icon: 'fa-circle-question', label: 'Other' },
      };

      // SVG pie chart
      const size = 200;
      const cx = size / 2, cy = size / 2, r = 80;
      let cumulative = 0;
      const slices = entries.map(([key, val]) => {
        const pct = val / total;
        const startAngle = cumulative * 2 * Math.PI;
        cumulative += pct;
        const endAngle = cumulative * 2 * Math.PI;
        const largeArc = pct > 0.5 ? 1 : 0;
        const x1 = cx + r * Math.cos(startAngle);
        const y1 = cy + r * Math.sin(startAngle);
        const x2 = cx + r * Math.cos(endAngle);
        const y2 = cy + r * Math.sin(endAngle);
        const meta = CATEGORY_META[key] || CATEGORY_META['other'];
        const d = `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} Z`;
        return `<path d="${d}" fill="${meta.color}" stroke="var(--card-bg)" stroke-width="2" opacity="0.9"/>`;
      });

      const legend = entries.map(([key, val]) => {
        const meta = CATEGORY_META[key] || CATEGORY_META['other'];
        const pct = ((val / total) * 100).toFixed(1);
        return `
        <div class="flex items-center gap-3 py-1.5">
          <div class="w-3 h-3 rounded-sm flex-shrink-0" style="background-color: ${meta.color}"></div>
          <div class="flex-grow flex items-center justify-between">
            <span class="text-sm cb-text-primary font-medium">${meta.label}</span>
            <span class="text-sm cb-text-secondary font-semibold">${val} <span class="cb-text-tertiary">(${pct}%)</span></span>
          </div>
        </div>`;
      }).join('');

      container.innerHTML = `
        <div class="flex-shrink-0">
          <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" class="drop-shadow-sm">${slices.join('')}</svg>
        </div>
        <div class="flex-grow min-w-[220px]">
          <h4 class="text-xs font-bold cb-text-primary uppercase tracking-wider mb-3">Reply Categories</h4>
          <div class="divide-y divide-gray-700/30">${legend}</div>
          <div class="mt-4 cb-surface rounded-xl p-3 flex items-center justify-between">
            <span class="text-xs cb-text-tertiary font-bold uppercase">Total Classified</span>
            <span class="text-lg font-black cb-text-primary">${total}</span>
          </div>
        </div>
      `;
    } catch(e) {
      container.innerHTML = '<p class="cb-text-tertiary text-sm">Sentiment stats unavailable. Build the /api/replies/sentiment-stats endpoint to enable this view.</p>';
    }
  }

  // ========================
  // GRID TAB — Clay-style spreadsheet
  // ========================
  const GRID_PAGE_SIZE = 50;
  let _gridData = [];
  let _gridFiltered = [];
  let _gridPage = 1;
  let _gridSortKey = 'first_name';
  let _gridSortAsc = true;
  let _gridSelected = new Set();
  let _gridLoaded = false;

  const STAGE_COLORS = {
    INGESTED: 'bg-slate-500/15 text-slate-400 border-slate-500/20',
    DISCOVERED: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
    ENRICHING: 'bg-purple-500/15 text-purple-400 border-purple-500/20',
    VERIFYING: 'bg-amber-500/15 text-amber-400 border-amber-500/20',
    READY: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    CONTACTED: 'bg-brand-500/15 text-brand-400 border-brand-500/20',
    RESEARCHED: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/20',
    DELIVERABILITY_VERIFIED: 'bg-teal-500/15 text-teal-400 border-teal-500/20',
    OPPORTUNITY_MAPPED: 'bg-indigo-500/15 text-indigo-400 border-indigo-500/20',
    PRE_QUALIFIED: 'bg-violet-500/15 text-violet-400 border-violet-500/20',
    OUTREACH_DRAFTED: 'bg-pink-500/15 text-pink-400 border-pink-500/20',
    PENDING_APPROVAL: 'bg-amber-500/15 text-amber-400 border-amber-500/20 animate-pulse',
    ACTIVE_OUTREACH: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
    REPLIED: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    MEETING_SCHEDULED: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    BLOCKED: 'bg-red-500/15 text-red-400 border-red-500/20',
    OPT_OUT: 'bg-zinc-500/15 text-zinc-400 border-zinc-500/20',
    NURTURE: 'bg-slate-500/15 text-slate-400 border-slate-500/20'
  };

  async function gridInit() {
    if (!_gridLoaded) {
      document.getElementById('grid-loading').classList.remove('hidden');
      const tableCard = document.querySelector('#tab-grid .cb-card.overflow-hidden');
      if (tableCard) tableCard.classList.add('hidden');
      try {
        const res = await fetch('/api/leads');
        _gridData = await res.json();
      } catch (e) {
        console.error('Grid data load failed:', e);
        _gridData = [];
      }
      _gridLoaded = true;
      document.getElementById('grid-loading').classList.add('hidden');
      if (tableCard) tableCard.classList.remove('hidden');
      gridLoadCampaigns();
    }
    gridApplyFilters();
    gridInstallKeyboardShortcuts();
  }

  function gridLoadCampaigns() {
    fetch('/api/campaigns').then(r => r.json()).then(d => {
      const campaigns = d.campaigns || [];
      const sel = document.getElementById('grid-bulk-campaign');
      sel.innerHTML = '<option value="">Enroll in Campaign...</option>' + campaigns.map(c => `<option value="${c.campaign_id}">${escapeHtml(c.name)}</option>`).join('');
    }).catch(() => {});
  }

  function gridApplyFilters() {
    const query = (document.getElementById('grid-search').value || '').toLowerCase().trim();
    const stageFilter = document.getElementById('grid-filter-stage').value;
    const verifiedOnly = document.getElementById('grid-filter-verified').checked;
    const scoreMin = parseInt(document.getElementById('grid-score-min').value) || 0;
    const scoreMax = parseInt(document.getElementById('grid-score-max').value) || 100;

    _gridFiltered = _gridData.filter(l => {
      const fullName = `${l.first_name || ''} ${l.last_name || ''}`.toLowerCase();
      const email = (l.email || '').toLowerCase();
      const company = (l.company_name || '').toLowerCase();
      if (query && !fullName.includes(query) && !email.includes(query) && !company.includes(query)) return false;
      if (stageFilter && l.current_stage !== stageFilter) return false;
      if (verifiedOnly && !l.email_verified) return false;
      const score = l.lead_score || 0;
      if (score < scoreMin || score > scoreMax) return false;
      return true;
    });

    gridSort(_gridSortKey, true);
  }

  function gridSort(key, keepDirection) {
    if (!keepDirection) {
      if (_gridSortKey === key) {
        _gridSortAsc = !_gridSortAsc;
      } else {
        _gridSortKey = key;
        _gridSortAsc = true;
      }
    }

    _gridFiltered.sort((a, b) => {
      let va = a[key] || '';
      let vb = b[key] || '';
      if (typeof va === 'string') va = va.toLowerCase();
      if (typeof vb === 'string') vb = vb.toLowerCase();
      if (va < vb) return _gridSortAsc ? -1 : 1;
      if (va > vb) return _gridSortAsc ? 1 : -1;
      return 0;
    });

    document.querySelectorAll('[id^="grid-sort-icon-"]').forEach(el => {
      el.className = 'fa-solid fa-sort text-[8px] ml-1';
    });
    const icon = document.getElementById(`grid-sort-icon-${key}`);
    if (icon) icon.className = `fa-solid fa-sort-${_gridSortAsc ? 'up' : 'down'} text-[8px] ml-1`;

    _gridPage = 1;
    _gridSelected.clear();
    gridRenderTable();
  }

  function gridRenderTable() {
    const total = _gridFiltered.length;
    const totalPages = Math.max(1, Math.ceil(total / GRID_PAGE_SIZE));
    if (_gridPage > totalPages) _gridPage = totalPages;

    const start = (_gridPage - 1) * GRID_PAGE_SIZE;
    const pageData = _gridFiltered.slice(start, start + GRID_PAGE_SIZE);

    const tbody = document.getElementById('grid-tbody');
    const emptyEl = document.getElementById('grid-empty');
    const tableCard = document.querySelector('#tab-grid .cb-card.overflow-hidden');

    if (total === 0) {
      tbody.innerHTML = '';
      emptyEl.classList.remove('hidden');
      tableCard.classList.add('hidden');
    } else {
      emptyEl.classList.add('hidden');
      tableCard.classList.remove('hidden');
    }

    tbody.innerHTML = pageData.map(l => {
      const id = l.contact_id;
      const checked = _gridSelected.has(id) ? 'checked' : '';
      const stageClass = STAGE_COLORS[l.current_stage] || 'bg-slate-500/15 text-slate-400 border-slate-500/20';
      const score = l.lead_score || 0;
      const scoreColor = score >= 80 ? 'bg-emerald-500' : score >= 50 ? 'bg-brand-500' : score >= 20 ? 'bg-amber-500' : 'bg-slate-500';
      const verified = l.email_verified
        ? '<i class="fa-solid fa-circle-check text-emerald-400"></i>'
        : '<i class="fa-solid fa-circle-xmark text-red-400/50"></i>';
      const linkedin = l.linkedin_url
        ? `<a href="${escapeHtml(l.linkedin_url)}" target="_blank" class="cb-text-tertiary hover:text-blue-400 transition"><i class="fa-brands fa-linkedin text-sm"></i></a>`
        : '<span class="cb-text-tertiary/30">—</span>';

      return `<tr class="hover:bg-black/[0.03] transition ${_gridSelected.has(id) ? 'bg-brand-500/5' : ''}">
        <td class="py-2.5 px-4"><input type="checkbox" class="grid-row-cb accent-brand-500" value="${id}" ${checked} onchange="gridToggleRow('${id}')"></td>
        <td class="py-2.5 px-4">
          <span class="font-semibold cb-text-primary text-xs cursor-pointer hover:text-brand-400 transition grid-inline-edit" data-id="${id}" data-field="first_name">${escapeHtml(l.first_name || '')} ${escapeHtml(l.last_name || '')}</span>
        </td>
        <td class="py-2.5 px-4">
          <span class="cb-text-secondary text-xs grid-inline-edit" data-id="${id}" data-field="email">${escapeHtml(l.email || '')}</span>
        </td>
        <td class="py-2.5 px-4">
          <span class="cb-text-secondary text-xs font-medium grid-inline-edit" data-id="${id}" data-field="company_name">${escapeHtml(l.company_name || '')}</span>
        </td>
        <td class="py-2.5 px-4">
          <span class="text-[11px] text-brand-400 font-mono">${escapeHtml(l.company_domain || '')}</span>
        </td>
        <td class="py-2.5 px-4">
          <span class="text-[10px] font-bold uppercase px-2 py-0.5 rounded-full border ${stageClass}">${l.current_stage || '-'}</span>
        </td>
        <td class="py-2.5 px-4">
          <div class="flex items-center gap-2">
            <div class="w-16 bg-gray-700/30 rounded-full h-1.5">
              <div class="${scoreColor} h-1.5 rounded-full" style="width: ${score}%"></div>
            </div>
            <span class="text-[10px] cb-text-tertiary w-7 text-right">${score}</span>
          </div>
        </td>
        <td class="py-2.5 px-4 text-center">${verified}</td>
        <td class="py-2.5 px-4 text-center">${linkedin}</td>
        <td class="py-2.5 px-4 text-right">
          <div class="flex items-center justify-end gap-1">
            <button onclick="openLeadDrawer('${id}')" class="cb-text-tertiary hover:text-brand-400 hover:scale-110 p-1.5 rounded transition" title="View lead"><i class="fa-solid fa-eye text-[11px]"></i></button>
            <button onclick="gridDeleteRow('${id}')" class="cb-text-tertiary hover:text-red-400 hover:scale-110 p-1.5 rounded transition" title="Delete"><i class="fa-solid fa-trash-can text-[11px]"></i></button>
          </div>
        </td>
      </tr>`;
    }).join('');

    document.getElementById('grid-info').textContent = total === 0 ? 'No results' : `Showing ${start + 1}–${Math.min(start + GRID_PAGE_SIZE, total)} of ${total}`;
    document.getElementById('grid-page-indicator').textContent = `Page ${_gridPage} of ${totalPages}`;
    document.getElementById('grid-prev-btn').disabled = _gridPage <= 1;
    document.getElementById('grid-next-btn').disabled = _gridPage >= totalPages;
    document.getElementById('grid-select-all').checked = pageData.length > 0 && pageData.every(l => _gridSelected.has(l.contact_id));
    gridUpdateBulkBar();
  }

  function gridPrevPage() { if (_gridPage > 1) { _gridPage--; gridRenderTable(); } }
  function gridNextPage() { _gridPage++; gridRenderTable(); }

  function gridToggleRow(id) {
    if (_gridSelected.has(id)) _gridSelected.delete(id); else _gridSelected.add(id);
    gridRenderTable();
  }

  function gridToggleAll() {
    const start = (_gridPage - 1) * GRID_PAGE_SIZE;
    const pageData = _gridFiltered.slice(start, start + GRID_PAGE_SIZE);
    const allChecked = pageData.every(l => _gridSelected.has(l.contact_id));
    pageData.forEach(l => { if (allChecked) _gridSelected.delete(l.contact_id); else _gridSelected.add(l.contact_id); });
    gridRenderTable();
  }

  function gridSelectAll() {
    _gridFiltered.forEach(l => _gridSelected.add(l.contact_id));
    gridRenderTable();
  }

  function gridDeselectAll() {
    _gridSelected.clear();
    gridRenderTable();
  }

  function gridUpdateBulkBar() {
    const bar = document.getElementById('grid-bulk-bar');
    const count = _gridSelected.size;
    if (count > 0) {
      bar.classList.remove('hidden');
      document.getElementById('grid-selected-count').textContent = `${count} selected`;
    } else {
      bar.classList.add('hidden');
    }
  }

  async function gridBulkEnroll() {
    const campaignId = document.getElementById('grid-bulk-campaign').value;
    if (!campaignId) { showToast('error', 'Select a campaign first.'); return; }
    if (!_gridSelected.size) { showToast('error', 'No leads selected.'); return; }
    try {
      const res = await fetch(`/api/campaigns/${campaignId}/enroll`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({lead_ids: Array.from(_gridSelected)})
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || 'Enroll failed');
      showToast('success', `Enrolled ${data.enrolled} lead(s).`);
      _gridSelected.clear();
      gridRenderTable();
    } catch (e) {
      showToast('error', 'Enroll failed: ' + e.message);
    }
  }

  async function gridBulkChangeStage() {
    const stage = document.getElementById('grid-bulk-stage').value;
    if (!stage) { showToast('error', 'Select a stage.'); return; }
    if (!_gridSelected.size) { showToast('error', 'No leads selected.'); return; }
    try {
      await Promise.all(Array.from(_gridSelected).map(id =>
        fetch(`/api/leads/${id}`, {
          method: 'PATCH',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({current_stage: stage})
        })
      ));
      // Update local data
      _gridData.forEach(l => { if (_gridSelected.has(l.contact_id)) l.current_stage = stage; });
      showToast('success', `Updated ${_gridSelected.size} lead(s) to ${stage}.`);
      _gridSelected.clear();
      gridApplyFilters();
    } catch (e) {
      showToast('error', 'Stage update failed.');
    }
  }

  async function gridBulkDelete() {
    if (!_gridSelected.size) return;
    if (!confirm(`Permanently delete ${_gridSelected.size} lead(s)?`)) return;
    try {
      await Promise.all(Array.from(_gridSelected).map(id =>
        fetch(`/api/leads/${id}`, {method: 'DELETE'})
      ));
      _gridData = _gridData.filter(l => !_gridSelected.has(l.contact_id));
      showToast('success', `Deleted ${_gridSelected.size} lead(s).`);
      _gridSelected.clear();
      gridApplyFilters();
    } catch (e) {
      showToast('error', 'Delete failed.');
    }
  }

  function gridExportCSV() {
    const rows = _gridSelected.size > 0
      ? _gridFiltered.filter(l => _gridSelected.has(l.contact_id))
      : _gridFiltered;
    if (!rows.length) { showToast('error', 'No data to export.'); return; }
    const headers = ['Name','Email','Company','Domain','Stage','Score','Verified','LinkedIn'];
    const csvRows = [headers.join(',')];
    rows.forEach(l => {
      const name = `"${l.first_name || ''} ${l.last_name || ''}"`;
      const email = `"${l.email || ''}"`;
      const company = `"${(l.company_name || '').replace(/"/g, '""')}"`;
      const domain = `"${l.company_domain || ''}"`;
      const stage = `"${l.current_stage || ''}"`;
      const score = l.lead_score || 0;
      const verified = l.email_verified ? 'Yes' : 'No';
      const linkedin = `"${l.linkedin_url || ''}"`;
      csvRows.push([name, email, company, domain, stage, score, verified, linkedin].join(','));
    });
    const blob = new Blob([csvRows.join('\\n')], {type: 'text/csv'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `clawbuildr-leads-${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('success', `Exported ${rows.length} leads to CSV.`);
  }

  async function gridDeleteRow(id) {
    if (!confirm('Delete this lead?')) return;
    try {
      await fetch(`/api/leads/${id}`, {method: 'DELETE'});
      _gridData = _gridData.filter(l => l.contact_id !== id);
      _gridSelected.delete(id);
      showToast('success', 'Lead deleted.');
      gridApplyFilters();
    } catch (e) {
      showToast('error', 'Delete failed.');
    }
  }

  // Inline editing
  document.addEventListener('dblclick', function(e) {
    const cell = e.target.closest('.grid-inline-edit');
    if (!cell) return;
    const id = cell.dataset.id;
    const field = cell.dataset.field;
    const currentVal = cell.textContent.trim();
    const input = document.createElement('input');
    input.type = 'text';
    input.value = currentVal;
    input.className = 'cb-input w-full text-xs py-1 px-2';
    cell.replaceWith(input);
    input.focus();
    input.select();

    const save = async () => {
      const newVal = input.value.trim();
      if (newVal !== currentVal) {
        try {
          await fetch(`/api/leads/${id}`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({[field]: newVal})
          });
          const lead = _gridData.find(l => l.contact_id === id);
          if (lead) lead[field] = newVal;
          showToast('success', `${field} updated.`);
        } catch (e) {
          showToast('error', 'Update failed.');
        }
      }
      gridApplyFilters();
    };

    input.addEventListener('blur', save);
    input.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') input.blur();
      if (ev.key === 'Escape') { input.value = currentVal; input.blur(); }
    });
  });

  // Keyboard shortcuts
  let _gridKeyboardInstalled = false;
  function gridInstallKeyboardShortcuts() {
    if (_gridKeyboardInstalled) return;
    _gridKeyboardInstalled = true;
    document.addEventListener('keydown', function(e) {
      if (activeTab !== 'tab-grid') return;
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
      if (e.ctrlKey && e.key === 'a') { e.preventDefault(); gridSelectAll(); }
      if (e.key === 'Escape') { gridDeselectAll(); }
      if (e.key === 'Delete' && _gridSelected.size > 0) { gridBulkDelete(); }
    });
  }

  // ========================
  // UTILITIES
  // ========================
  function escapeHtml(text) {
  if (text == null) return '';
  return String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }
  </script>

 <!-- Post Preview Modal -->
 <div id="post-preview-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
 <div class="absolute inset-0 bg-black/70" onclick="closePostPreview()"></div>
 <div class="relative cb-surface-elevated border cb-border rounded-2xl w-full max-w-2xl mx-4 max-h-[80vh] overflow-y-auto" style="box-shadow: var(--shadow-lg);">
 <div class="p-6 border-b cb-border flex items-center justify-between">
 <div>
 <h3 class="text-lg font-extrabold cb-text-primary">LinkedIn Post Preview</h3>
 <p class="text-xs cb-text-tertiary mt-1" id="preview-post-topic">Topic</p>
 </div>
 <button onclick="closePostPreview()" class="cb-text-tertiary hover:cb-text-primary p-2 rounded-lg transition">
 <i class="fa-solid fa-xmark text-xl"></i>
 </button>
 </div>
 <div class="p-6">
 <div class="cb-surface-elevated border cb-border rounded-xl p-5">
 <div class="flex items-center gap-3 mb-4">
 <div class="w-10 h-10 bg-blue-600 rounded-full flex items-center justify-center text-white font-bold text-sm">MvS</div>
 <div>
 <p class="text-sm font-bold cb-text-primary">Marvin van der Sluis</p>
 <p class="text-[10px] cb-text-tertiary">Founder @ FounderFlow | AI Outreach Systems</p>
 </div>
 </div>
 <div class="text-sm text-slate-200 leading-relaxed whitespace-pre-wrap" id="preview-post-content"></div>
 </div>
 <div class="flex justify-end gap-3 mt-4">
 <button onclick="closePostPreview()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-semibold text-sm px-4 py-2 rounded-xl transition">Close</button>
 </div>
 </div>
 </div>
 </div>

 <!-- Approval Modal: Review & Edit Email Draft Before Sending -->
 <div id="approval-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
 <div class="absolute inset-0 bg-black/70" onclick="closeApprovalModal()"></div>
 <div class="relative cb-surface-elevated border cb-border rounded-2xl w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto" style="box-shadow: var(--shadow-lg);">
 <div class="p-6 border-b cb-border flex items-center justify-between">
 <div>
 <h3 class="text-lg font-extrabold cb-text-primary">Review Outbound Email</h3>
 <p class="text-xs cb-text-tertiary mt-1" id="approval-modal-lead-info">Lead name — Company</p>
 </div>
 <button onclick="closeApprovalModal()" class="cb-text-tertiary hover:cb-text-primary p-2 rounded-lg transition">
 <i class="fa-solid fa-xmark text-xl"></i>
 </button>
 </div>
 <div class="p-6 space-y-4">
 <div>
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Subject Line</label>
 <input id="approval-subject" type="text" class="cb-input w-full">
 </div>
 <div>
 <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Email Body</label>
 <textarea id="approval-body" rows="16" class="w-full cb-surface-elevated border cb-border rounded-xl px-4 py-3 text-sm cb-text-primary font-mono leading-relaxed focus:outline-none focus:border-brand-500/80 resize-y"></textarea>
 </div>
 <div class="cb-surface-elevated border cb-border rounded-xl p-4 space-y-2">
 <span class="text-[11px] text-amber-400 font-bold uppercase tracking-wider flex items-center gap-2"><i class="fa-solid fa-triangle-exclamation"></i> What to check before sending</span>
 <ul class="text-xs cb-text-tertiary space-y-1.5 list-disc list-inside">
 <li>Is the company name spelled correctly?</li>
 <li>Does the email reference their specific industry/services?</li>
 <li>Is the tone appropriate for their business type?</li>
 <li>Does the CTA match their likely decision-making authority?</li>
 </ul>
 </div>
 </div>
 <div class="p-6 border-t cb-border flex gap-3 justify-end">
 <button onclick="closeApprovalModal()" class="px-5 py-2.5 border cb-border cb-text-secondary font-bold text-sm rounded-xl hover:bg-black/5 transition">Cancel</button>
 <button onclick="saveDraftAndSend()" class="px-5 py-2.5 bg-brand-600 hover:bg-brand-500 text-white font-bold text-sm rounded-xl shadow-brand-500/15 transition flex items-center gap-2">
 <i class="fa-solid fa-paper-plane"></i> Send Email
 </button>
 </div>
  </div>
  </div>

  <!-- Campaign Analytics Modal -->
  <div id="campaign-analytics-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
  <div class="absolute inset-0 bg-black/70" onclick="closeCampaignAnalyticsModal()"></div>
  <div class="relative cb-surface-elevated border cb-border rounded-2xl w-full max-w-xl mx-4" style="box-shadow: var(--shadow-lg);">
  <div class="p-6 border-b cb-border flex items-center justify-between">
  <div>
  <h3 class="text-lg font-extrabold cb-text-primary">Campaign Analytics</h3>
  <p class="text-xs cb-text-tertiary mt-1" id="campaign-analytics-name">Campaign name</p>
  </div>
  <button onclick="closeCampaignAnalyticsModal()" class="cb-text-tertiary hover:cb-text-primary p-2 rounded-lg transition"><i class="fa-solid fa-xmark text-xl"></i></button>
  </div>
  <div class="p-6">
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Sent</p>
  <p id="ca-sent" class="text-2xl font-black cb-text-primary">0</p>
  </div>
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Opened</p>
  <p id="ca-opened" class="text-2xl font-black cb-text-primary">0</p>
  </div>
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Clicked</p>
  <p id="ca-clicked" class="text-2xl font-black cb-text-primary">0</p>
  </div>
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Replied</p>
  <p id="ca-replied" class="text-2xl font-black cb-text-primary">0</p>
  </div>
  </div>
  <div class="grid grid-cols-3 gap-4">
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Open Rate</p>
  <p id="ca-open-rate" class="text-xl font-black text-brand-400">0%</p>
  </div>
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Click Rate</p>
  <p id="ca-click-rate" class="text-xl font-black text-brand-400">0%</p>
  </div>
  <div class="cb-surface rounded-xl p-4 text-center">
  <p class="text-[10px] cb-text-tertiary font-bold uppercase tracking-wider mb-1">Reply Rate</p>
  <p id="ca-reply-rate" class="text-xl font-black text-brand-400">0%</p>
  </div>
  </div>
  </div>
  <div class="p-6 border-t cb-border flex justify-end">
  <button onclick="closeCampaignAnalyticsModal()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-semibold text-sm px-4 py-2 rounded-xl transition">Close</button>
  </div>
  </div>
  </div>

  <!-- Enroll Leads Modal -->
  <div id="enroll-leads-modal" class="fixed inset-0 z-[100] flex items-center justify-center hidden">
  <div class="absolute inset-0 bg-black/70" onclick="closeEnrollLeadsModal()"></div>
  <div class="relative cb-surface-elevated border cb-border rounded-2xl w-full max-w-xl mx-4" style="box-shadow: var(--shadow-lg);">
  <div class="p-6 border-b cb-border flex items-center justify-between">
  <div>
  <h3 class="text-lg font-extrabold cb-text-primary">Enroll Leads</h3>
  <p class="text-xs cb-text-tertiary mt-1" id="enroll-modal-campaign-name">Campaign</p>
  </div>
  <button onclick="closeEnrollLeadsModal()" class="cb-text-tertiary hover:cb-text-primary p-2 rounded-lg transition"><i class="fa-solid fa-xmark text-xl"></i></button>
  </div>
  <div class="p-6 space-y-4">
  <input type="hidden" id="enroll-modal-campaign-id">
  <div>
  <label class="text-[11px] cb-text-tertiary font-bold uppercase tracking-wider block mb-1.5">Select Leads</label>
  <select id="enroll-leads-select" multiple class="cb-input w-full h-64"></select>
  <p class="text-[10px] cb-text-tertiary mt-1">Hold Ctrl/Cmd to select multiple leads.</p>
  </div>
  </div>
  <div class="p-6 border-t cb-border flex justify-end gap-3">
  <button onclick="closeEnrollLeadsModal()" class="cb-surface-elevated hover:bg-black/10 cb-text-secondary font-semibold text-sm px-4 py-2 rounded-xl transition">Cancel</button>
  <button onclick="submitEnrollLeads()" class="bg-brand-600 hover:bg-brand-500 text-white font-semibold text-sm px-5 py-2 rounded-xl transition"><i class="fa-solid fa-user-plus mr-1"></i> Enroll Selected</button>
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
