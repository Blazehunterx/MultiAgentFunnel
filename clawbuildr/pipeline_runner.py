#!/usr/bin/env python3
"""
24/7 Pipeline Runner — Lead gen + email outreach + LinkedIn connections + follow-ups.
Designed for continuous operation within rate limits.

Goal: 300+ emails in 7 days (≈43/day), 20 LinkedIn connections/day.

Rate limits:
- Hunter.io: ~25 requests/day (free) or 500/day (paid)
- Gmail SMTP: 500/day (free), 2000/day (Workspace)
- Gemini API: generous free tier
- LinkedIn: 20 connections/day (warming-aware)

Strategy:
- Lead gen: 3 domain searches every 30 min
- Email gen: hyper-personalized via Gemini with company research
- Email send: 10-15 emails/day spread throughout the day
- LinkedIn: 20 connections/day via background auto-scheduler
- Follow-ups: 48h/96h/168h sequence
- Reply detection: Gmail IMAP polling
"""

import os
import sys
import json
import re
import time
import random
import sqlite3
import asyncio
import signal
import logging
import threading
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")
LOG_DIR = os.path.join(DATA_DIR, "logs")

sys.path.insert(0, os.path.join(BASE_DIR, "clawbuildr"))
sys.path.insert(0, BASE_DIR)

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "pipeline_runner.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ClawBuildr.PipelineRunner")

shutdown_requested = False

PID_FILE = os.path.join(DATA_DIR, "pipeline_runner.pid")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return code.value == STILL_ACTIVE
            return False
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _acquire_pid_lock() -> bool:
    """Return True if this process may run; False if another runner is already alive."""
    try:
        if os.path.exists(PID_FILE):
            try:
                with open(PID_FILE) as f:
                    old = int(f.read().strip())
            except (ValueError, OSError):
                old = -1
            if old > 0 and old != os.getpid() and _pid_alive(old):
                return False
    except OSError:
        pass
    try:
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except OSError:
        pass
    return True


def _release_pid_lock():
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(PID_FILE)
    except OSError:
        pass


def _signal_handler(sig, frame):
    global shutdown_requested
    logger.info("Shutdown requested — finishing current cycle then exiting.")
    shutdown_requested = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def _get_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    for attempt in range(5):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=60.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=60000;")
            return conn
        except sqlite3.OperationalError:
            time.sleep(2)
    conn = sqlite3.connect(DB_PATH, timeout=120.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=120000;")
    return conn


def _get_tenant_setting(key: str, default=None):
    """Read a setting from the active tenant config."""
    try:
        db = _get_db()
        row = db.execute(f"SELECT {key} FROM tenant_config WHERE active = 1 LIMIT 1").fetchone()
        db.close()
        if row and row[key] is not None:
            val = row[key]
            if isinstance(val, str) and val.strip().startswith('['):
                try:
                    return json.loads(val)
                except Exception:
                    pass
            return val
    except Exception as e:
        logger.debug(f"[TenantSetting] Could not read {key}: {e}")
    return default


def _db_write(sql, params=(), retries=10):
    """Execute a DB write with retry logic for lock contention."""
    for attempt in range(retries):
        try:
            db = _get_db()
            db.execute(sql, params)
            db.commit()
            db.close()
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise
    return False


def _now():
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: LEAD GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def _run_lead_gen():
    """Generate leads from sources chosen during onboarding."""
    try:
        from clawbuildr_lead_generator import generate_leads, KNOWN_BUSINESS_DOMAINS, _is_valid_business_domain
        lead_sources = set(_get_tenant_setting("lead_sources", ["hunter", "directories", "kvk"]))
        db = _get_db()
        known = {r[0].lower() for r in db.execute("SELECT domain FROM companies").fetchall()}
        db.close()

        available = []
        if "hunter" in lead_sources:
            available = [d for d in KNOWN_BUSINESS_DOMAINS if d.lower() not in known and _is_valid_business_domain(d)]

        if not available and "kvk" in lead_sources:
            logger.info("[LeadGen] Running KVK discovery")
            available = _discover_new_domains(known)

        if not available and "directories" in lead_sources:
            logger.info("[LeadGen] Running business directory discovery")
            available = _discover_from_business_directories(known)

        if not available:
            logger.info("[LeadGen] No new domains found for selected sources — will retry next cycle")
            return 0

        sample = random.sample(available, min(3, len(available)))
        result = asyncio.run(generate_leads(
            domains=sample, max_leads=10, use_hunter=("hunter" in lead_sources), use_website_scrape=True,
        ))
        logger.info(f"[LeadGen] {result['found']} leads from {result['domains_processed']} domains")
        return result["found"]
    except Exception as e:
        logger.error(f"[LeadGen] Error: {e}")
        return 0


def _discover_new_domains(known_domains: set) -> list:
    """Discover new Dutch/Belgian/German business domains via KVK API."""
    new_domains = []
    try:
        from tools import kvk_search_api
        from clawbuildr_lead_generator import _is_valid_business_domain
        # Search for Dutch SMEs in ICP industries
        queries = [
            "accountancy kantoor MKB", "tandartspraktijk", "juridisch adviesbureau",
            "vastgoed bureau", "IT dienstverlening", "marketing bureau",
            "recruitment bureau", "bouwbedrijf", "schoonmaakbedrijf",
            "fysiotherapie praktijk", "architectenbureau", "ingenieursbureau",
        ]
        query = random.choice(queries)
        results = asyncio.run(kvk_search_api(query, limit=10))
        for r in results:
            website = r.get("website", "")
            if website:
                domain = website.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
                if domain and domain not in known_domains and "." in domain:
                    # Only accept Dutch/Belgian/German domains
                    if _is_valid_business_domain(domain):
                        new_domains.append(domain)
        logger.info(f"[LeadGen] Dynamic discovery found {len(new_domains)} new Dutch/B2B domains from KVK")
    except Exception as e:
        logger.debug(f"[LeadGen] Dynamic discovery error: {e}")
    return new_domains[:10]


def _discover_from_business_directories(known_domains: set) -> list:
    """Scrape Dutch business directories for actual MKB companies with websites."""
    import httpx
    new_domains = []
    try:
        from clawbuildr_lead_generator import _is_valid_business_domain

        # Dutch business directories with actual company listings
        directories = [
            {
                "name": "Telefoonboek.nl",
                "url": "https://www.telefoonboek.nl/zoeken/{query}",
                "queries": ["accountant", "tandarts", "advocaat", "makelaar", "IT bedrijf"],
            },
            {
                "name": "Bedrijvenregister.nl",
                "url": "https://www.bedrijvenregister.nl/zoek/{query}",
                "queries": ["accountancy", "juridisch", "marketing", "bouw", "schoonmaak"],
            },
        ]

        for directory in directories:
            query = random.choice(directory["queries"])
            try:
                with httpx.Client(timeout=10, follow_redirects=True) as client:
                    resp = client.get(
                        directory["url"].format(query=query),
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/130.0"}
                    )
                    if resp.status_code == 200:
                        # Extract domains from links
                        text = resp.text
                        # Find links to company websites
                        links = re.findall(r'href="(https?://[^"]*)"', text)
                        for link in links:
                            # Skip directory's own links
                            if directory["name"].lower().replace(".nl", "") in link.lower():
                                continue
                            # Extract clean domain
                            domain = link.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
                            if (domain and domain not in known_domains and "." in domain
                                    and _is_valid_business_domain(domain)):
                                new_domains.append(domain)
                                known_domains.add(domain)
            except Exception as e:
                logger.debug(f"[LeadGen] Directory {directory['name']} error: {e}")

        logger.info(f"[LeadGen] Business directories found {len(new_domains)} new Dutch B2B domains")
    except Exception as e:
        logger.debug(f"[LeadGen] Directory discovery error: {e}")
    return new_domains[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: ENROLL IN FOLLOW-UP SEQUENCE
# ═══════════════════════════════════════════════════════════════════════════════

def _run_enrollment():
    """Enroll INGESTED contacts into follow-up sequence."""
    for attempt in range(3):
        try:
            from clawbuildr_scheduler import enroll_contact
            db = _get_db()
            contacts = db.execute(
                """SELECT c.contact_id, c.first_name, c.last_name, c.email, c.company_id,
                          co.name as company_name, co.domain
                   FROM contacts c
                   LEFT JOIN companies co ON c.company_id = co.company_id
                   WHERE c.current_stage = 'INGESTED'
                     AND c.email IS NOT NULL AND c.email != ''
                   LIMIT 5"""
            ).fetchall()

            contact_ids = [c["contact_id"] for c in contacts]
            db.close()

            if not contact_ids:
                return 0

            count = 0
            for cid in contact_ids:
                try:
                    db2 = _get_db()
                    existing = db2.execute(
                        "SELECT id FROM followup_queue WHERE contact_id = ? AND status IN ('pending', 'ready')",
                        (cid,),
                    ).fetchone()
                    db2.close()

                    if existing:
                        continue

                    enrolled = enroll_contact(cid)
                    if enrolled:
                        db3 = _get_db()
                        db3.execute(
                            "UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH', updated_at = ? WHERE contact_id = ?",
                            (_now(), cid),
                        )
                        db3.execute(
                            """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
                               VALUES (?, 'sequence_enrolled', 'Enrolled in outreach sequence', ?, ?)""",
                            (cid, json.dumps({"source": "pipeline_runner"}), _now()),
                        )
                        db3.commit()
                        db3.close()
                        count += 1
                except Exception as e:
                    logger.debug(f"[Enrollment] Error for {cid}: {e}")

            logger.info(f"[Enrollment] {count} contacts enrolled")
            return count
        except Exception as e:
            if attempt < 2:
                time.sleep(10)
                continue
            logger.error(f"[Enrollment] Error: {e}")
            return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2b: COMPANY RESEARCH (before email generation)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_research():
    """Research companies that don't have research results yet."""
    try:
        from pipeline import research_company
        from models import LeadInput

        db = _get_db()
        contacts = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.email, c.role,
                      co.name as company_name, co.domain, co.industry
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.current_stage = 'ACTIVE_OUTREACH'
                 AND (c.research_result IS NULL OR c.research_result = '')
                 AND co.domain IS NOT NULL AND co.domain != ''
               ORDER BY c.lead_score DESC
               LIMIT 5"""
        ).fetchall()

        if not contacts:
            db.close()
            return 0

        count = 0
        for c in contacts:
            try:
                lead = LeadInput(
                    first_name=c["first_name"] or "",
                    last_name=c["last_name"] or "",
                    email=c["email"] or "",
                    company=c["company_name"] or c["domain"] or "",
                    domain=c["domain"] or "",
                    role=c["role"] or "Eigenaar",
                )
                research = asyncio.run(research_company(lead))
                research_dict = research.model_dump() if hasattr(research, "model_dump") else research.dict()

                db.execute(
                    "UPDATE contacts SET research_result = ? WHERE contact_id = ?",
                    (json.dumps(research_dict), c["contact_id"]),
                )
                db.commit()
                count += 1
                logger.info(f"[Research] Researched {c['company_name']}: {len(research_dict.get('content', ''))} chars")
                time.sleep(2)
            except Exception as e:
                logger.debug(f"[Research] Failed for {c['company_name']}: {e}")

        db.close()
        logger.info(f"[Research] {count}/{len(contacts)} companies researched")
        return count
    except Exception as e:
        logger.error(f"[Research] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: AI EMAIL GENERATION (hyper-personalized)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_email_gen():
    """Generate hyper-personalized AI emails with A/B test assignment."""
    try:
        from clawbuildr_ai_email import generate_ai_email, save_generated_email
        from clawbuildr_learning import record_ab_test_result

        db = _get_db()

        active_tests = db.execute(
            "SELECT id, test_name, variant_a, variant_b FROM ab_tests WHERE status = 'running'"
        ).fetchall()

        contacts = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.email, c.role,
                      c.research_result, c.outreach_draft,
                      co.name as company_name, co.domain, co.industry
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.current_stage = 'ACTIVE_OUTREACH'
                 AND (c.outreach_draft IS NULL OR c.outreach_draft = '')
                 AND c.email IS NOT NULL AND c.email != ''
               ORDER BY c.lead_score DESC
               LIMIT 10"""
        ).fetchall()

        if not contacts:
            db.close()
            return 0

        count = 0
        for c in contacts:
            try:
                research_ctx = c["research_result"] or ""
                if isinstance(research_ctx, str) and research_ctx.startswith("{"):
                    try:
                        research_ctx = json.dumps(json.loads(research_ctx), indent=1)
                    except Exception:
                        pass

                language = "nl"
                domain = (c["domain"] or "").lower()
                if any(tld in domain for tld in [".com", ".io", ".co.uk"]):
                    language = "en"

                ab_modifiers = {}
                for test in active_tests:
                    variant = random.choice(["a", "b"])
                    ab_modifiers[test["test_name"]] = variant

                custom_prompt = ""
                if ab_modifiers.get("subject_question_vs_statement") == "a":
                    custom_prompt += " Subject line MUST be a question ending with '?'. "
                elif ab_modifiers.get("subject_question_vs_statement") == "b":
                    custom_prompt += " Subject line MUST be a statement, not a question. "

                if ab_modifiers.get("body_short_vs_medium") == "a":
                    custom_prompt += " Email body MUST be very short (max 50 words). "
                elif ab_modifiers.get("body_short_vs_medium") == "b":
                    custom_prompt += " Email body should be medium length (80-120 words). "

                if ab_modifiers.get("cta_soft_vs_direct") == "a":
                    custom_prompt += " CTA must be a soft open-ended question. "
                elif ab_modifiers.get("cta_soft_vs_direct") == "b":
                    custom_prompt += " CTA must be a direct ask (e.g. 'Can we schedule a call?'). "

                result = generate_ai_email(
                    contact_id=c["contact_id"],
                    company_name=c["company_name"] or c["domain"] or "Unknown",
                    contact_name=f"{c['first_name'] or ''} {c['last_name'] or ''}".strip(),
                    role=c["role"] or "Decision Maker",
                    research_context=research_ctx,
                    icp_context=f"Industry: {c['industry'] or 'B2B services'}. ICP: NL/BE/DE service businesses, founder-led, manual processes.",
                    language=language,
                    custom_prompt=custom_prompt,
                )

                if result.get("subject") and result.get("body"):
                    email_json = json.dumps({
                        "subject": result["subject"],
                        "body": result["body"],
                        "confidence": result.get("confidence", 0),
                        "model": result.get("model_used", ""),
                        "ab_tests": ab_modifiers,
                    })

                    db.execute(
                        "UPDATE contacts SET outreach_draft = ?, updated_at = ? WHERE contact_id = ?",
                        (email_json, _now(), c["contact_id"]),
                    )

                    save_generated_email(
                        contact_id=c["contact_id"],
                        subject=result["subject"],
                        body=result["body"],
                        direction="outbound",
                        confidence=result.get("confidence", 0),
                        model_used=result.get("model_used", ""),
                    )

                    for test_name, variant in ab_modifiers.items():
                        test_row = db.execute(
                            "SELECT id FROM ab_tests WHERE test_name = ? AND status = 'running'",
                            (test_name,)
                        ).fetchone()
                        if test_row:
                            record_ab_test_result(test_row["id"], variant, is_reply=False)

                    db.execute(
                        """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
                           VALUES (?, 'draft_generated', 'AI draft with A/B assignment', ?, ?)""",
                        (c["contact_id"], json.dumps({
                            "subject": result["subject"],
                            "confidence": result.get("confidence", 0),
                            "ab_tests": ab_modifiers,
                        }), _now()),
                    )
                    count += 1
                    logger.info(
                        f"[EmailGen] Draft for {c['first_name']} {c['last_name']} @ {c['company_name']} "
                        f"(confidence: {result.get('confidence', 0):.0%}, A/B: {ab_modifiers})"
                    )
            except Exception as e:
                logger.debug(f"[EmailGen] Failed for {c['email']}: {e}")

            time.sleep(3)

        db.commit()
        db.close()
        logger.info(f"[EmailGen] {count}/{len(contacts)} drafts generated with A/B tests")
        return count
    except Exception as e:
        logger.error(f"[EmailGen] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3b: QUALITY GATE (review drafts before approval)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_quality_gate():
    """Run quality gate on generated drafts before they enter approval queue."""
    try:
        from clawbuildr_quality_gate import run_quality_gate
        result = run_quality_gate(limit=10)
        if result.get("error"):
            logger.error(f"[QualityGate] Error: {result['error']}")
            return 0
        passed = result.get("passed", 0)
        failed = result.get("failed", 0)
        logger.info(f"[QualityGate] {passed} passed, {failed} failed out of {result.get('reviewed', 0)}")
        return passed
    except Exception as e:
        logger.error(f"[QualityGate] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3c: REGENERATION (retry failed drafts)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_regeneration():
    """Regenerate drafts for contacts that failed quality gate."""
    try:
        from clawbuildr_ai_email import _prefab_template_fallback

        db = _get_db()
        contacts = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.email,
                      c.company_id, c.research_result, co.name as company_name, co.domain, co.industry
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.current_stage = 'NEEDS_REGENERATION'
               ORDER BY RANDOM()
               LIMIT 5"""
        ).fetchall()

        if not contacts:
            db.close()
            return 0

        count = 0
        for c in contacts:
            try:
                research = json.loads(c["research_result"]) if c["research_result"] else {}
                industry = c["industry"] or research.get("industry", "dienstverlening")

                # Extract pain_point and value_prop from research
                pain_point = research.get("pain_point", "")
                value_prop = research.get("value_prop", "")
                if not pain_point:
                    signals = research.get("signals", [])
                    if signals:
                        pain_point = signals[0]
                    else:
                        pain_point = f"het verbeteren van de bedrijfsprocessen bij {c['company_name'] or 'jullie bedrijf'}"
                if not value_prop:
                    services = research.get("services", [])
                    if services:
                        value_prop = services[0]
                    else:
                        value_prop = "een bewezen aanpak die tijd bespaart en fouten vermindert"

                result = _prefab_template_fallback(
                    company_name=c["company_name"] or c["domain"] or "jullie bedrijf",
                    first_name=c["first_name"] or "daar",
                    industry=industry,
                    pain_point=pain_point,
                    value_prop=value_prop,
                )
                if result and result.get("subject") and result.get("body"):
                    db.execute(
                        "UPDATE contacts SET outreach_draft = ?, current_stage = 'PENDING_APPROVAL', updated_at = ? WHERE contact_id = ?",
                        (json.dumps(result), _now(), c["contact_id"]),
                    )
                    db.commit()
                    count += 1
                    logger.info(f"[Regeneration] Regenerated draft for {c['first_name']} {c['last_name']}")
                    time.sleep(1)
            except Exception as e:
                logger.debug(f"[Regeneration] Failed for {c['email']}: {e}")

        db.close()
        logger.info(f"[Regeneration] {count} drafts regenerated")
        return count
    except Exception as e:
        logger.error(f"[Regeneration] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: SEND EMAILS (spread throughout the day)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_email_send():
    """Send emails with watchdog checks, learning, and human-like delays."""
    # Check if email channel is enabled for this tenant
    if "email" not in _get_tenant_setting("campaign_channels", ["email", "linkedin"]):
        logger.info("[EmailSend] Email channel disabled for this tenant, skipping.")
        return 0

    try:
        from tools import gmail_send
        from clawbuildr_ai_email import save_generated_email
        from clawbuildr_watchdog import can_send, check_daily_limit, check_hourly_limit, check_bounce_rate
        from clawbuildr_learning import record_email_performance, record_timing

        if not can_send():
            daily = check_daily_limit()
            hourly = check_hourly_limit()
            bounce = check_bounce_rate()
            logger.warning(
                f"[EmailSend] WATCHDOG BLOCKED — "
                f"Daily: {daily['sent_today']}/{daily['limit']}, "
                f"Hourly: {hourly['sent_last_hour']}/{hourly['limit']}, "
                f"Bounce: {bounce['bounce_rate']}%"
            )
            return 0

        db = _get_db()
        contacts = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.email,
                      c.outreach_draft, co.name as company_name
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.current_stage = 'PENDING_APPROVAL'
                 AND c.outreach_draft IS NOT NULL AND c.outreach_draft != ''
                  AND c.email NOT LIKE 'info@%' AND c.email NOT LIKE 'sales@%'
                  AND c.email NOT LIKE 'contact@%' AND c.email NOT LIKE 'admin@%'
                  AND c.email NOT LIKE 'support@%' AND c.email NOT LIKE 'hello@%'
                  AND c.email NOT LIKE 'office@%' AND c.email NOT LIKE 'team@%'
                  AND c.email NOT LIKE 'noreply@%' AND c.email NOT LIKE 'press@%'
                  AND c.email NOT LIKE 'webmaster@%' AND c.email NOT LIKE 'help@%'
                  AND c.email NOT LIKE 'marketing@%' AND c.email NOT LIKE 'hr@%'
                  AND c.email NOT LIKE 'pr@%' AND c.email NOT LIKE 'media@%'
                  AND c.email NOT LIKE 'service@%' AND c.email NOT LIKE 'feedback@%'
                  AND c.email NOT LIKE 'accommodations@%' AND c.email NOT LIKE 'billing@%'
                  AND c.email NOT LIKE 'accounts@%' AND c.email NOT LIKE 'careers@%'
                  AND c.email NOT LIKE 'jobs@%' AND c.email NOT LIKE 'hello@%'
                  AND c.email NOT LIKE 'notifications@%' AND c.email NOT LIKE 'no-reply@%'
                  AND c.email NOT LIKE 'newsletter@%' AND c.email NOT LIKE 'abuse@%'
                  AND c.email NOT LIKE 'postmaster@%' AND c.email NOT LIKE 'hostmaster@%'
                  AND c.email NOT LIKE 'abus@%' AND c.email NOT LIKE 'spam@%'
                  AND c.email NOT LIKE 'dmca@%' AND c.email NOT LIKE 'segreteria@%'
                  AND c.email NOT LIKE 'abuse@%' AND c.email NOT LIKE 'legal@%'
                  AND c.email NOT LIKE 'privacy@%' AND c.email NOT LIKE 'compliance@%'
                  AND c.email NOT LIKE 'security@%' AND c.email NOT LIKE 'info-@%'
                  AND c.email NOT LIKE 'donotreply@%' AND c.email NOT LIKE 'mailer-daemon@%'
                  AND c.email NOT LIKE 'bounce@%' AND c.email NOT LIKE 'undisclosed@%'
                  AND c.email NOT LIKE 'editor@%' AND c.email NOT LIKE 'website@%'
                  AND c.email NOT LIKE 'contato@%' AND c.email NOT LIKE 'newstipps@%'
                  AND c.email NOT LIKE 'customerservice@%' AND c.email NOT LIKE 'klantenservice@%'
                  AND c.email NOT LIKE 'secretariaat@%' AND c.email NOT LIKE 'directie@%'
                  AND c.email NOT LIKE 'inkoop@%' AND c.email NOT LIKE 'verkoop@%'
                  AND c.email NOT LIKE 'financien@%' AND c.email NOT LIKE 'personeel@%'
                  AND c.email NOT LIKE 'communicatie@%' AND c.email NOT LIKE 'automatisering@%'
                  AND c.email NOT LIKE '%naam@%' AND c.email NOT LIKE '%voorbeeld@%'
                  AND c.email NOT LIKE '%jouw@%' AND c.email NOT LIKE '%test@%'
                  AND c.email NOT LIKE '%example@%' AND c.email NOT LIKE '%demo@%'
                  AND c.email NOT LIKE '% %'
                  AND c.email NOT LIKE '%@gmail.com' AND c.email NOT LIKE '%@hotmail.com'
                  AND c.email NOT LIKE '%@yahoo.com' AND c.email NOT LIKE '%@outlook.com'
                  AND c.email NOT LIKE '%@live.com' AND c.email NOT LIKE '%@aol.com'
                  AND c.email NOT LIKE '%@icloud.com' AND c.email NOT LIKE '%@protonmail.com'
                  AND c.email NOT LIKE '%@zoho.com' AND c.email NOT LIKE '%@yandex.com'
                  AND c.email NOT LIKE '%@mail.com' AND c.email NOT LIKE '%@gmx.com'
                  AND c.email NOT LIKE '%@fastmail.com' AND c.email NOT LIKE '%@tutanota.com'
                  AND c.email NOT LIKE '%@mail.ru' AND c.email NOT LIKE '%@bk.ru'
                  AND (co.domain LIKE '%.nl' OR co.domain LIKE '%.be' OR co.domain LIKE '%.de')
                  AND c.contact_id NOT IN (
                      SELECT contact_id FROM emails WHERE status = 'bounced' AND contact_id IS NOT NULL
                  )
               ORDER BY RANDOM()
               LIMIT 5"""
        ).fetchall()

        if not contacts:
            logger.info("[EmailSend] No PENDING_APPROVAL contacts with drafts found")
            db.close()
            return 0

        logger.info(f"[EmailSend] Found {len(contacts)} contacts to send")

        # Close DB before long operations (DNS, SMTP) to avoid lock contention with dashboard
        db.close()

        count = 0
        for c in contacts:
            if not can_send():
                logger.info("[EmailSend] Watchdog: limit reached mid-batch")
                break

            try:
                draft = json.loads(c["outreach_draft"]) if c["outreach_draft"].startswith("{") else {
                    "subject": f"Quick question about {c['company_name'] or 'your business'}",
                    "body": c["outreach_draft"],
                }

                # CRITICAL: Never send empty or too-short emails
                subject = (draft.get("subject") or "").strip()
                body = (draft.get("body") or "").strip()
                if not subject or not body or len(body) < 30:
                    logger.warning(f"[EmailSend] SKIP {c['email']}: empty or too-short draft (subject={len(subject)}ch, body={len(body)}ch)")
                    _db_write(
                        "UPDATE contacts SET current_stage = 'NEEDS_REGENERATION', updated_at = ? WHERE contact_id = ?",
                        (_now(), c["contact_id"]),
                    )
                    continue

                # CRITICAL: Verify email before sending to prevent bounces
                from tools import verify_email
                email_check = verify_email(c["email"])
                if email_check.get("confidence", 0) < 50:
                    logger.warning(f"[EmailSend] SKIP {c['email']}: low confidence ({email_check.get('confidence', 0)}%) — {email_check.get('reasons', [])}")
                    # Mark as bad email
                    _db_write(
                        "UPDATE contacts SET current_stage = 'BAD_EMAIL', updated_at = ? WHERE contact_id = ?",
                        (_now(), c["contact_id"]),
                    )
                    continue

                # CRITICAL: Check for company ID in subject before sending
                subject = draft.get("subject", "")
                if re.search(r'co_[a-f0-9]+|prsp_[a-f0-9]+|comp[_\w]+', subject):
                    logger.warning(f"[EmailSend] SKIP {c['email']}: company ID in subject '{subject[:50]}'")
                    _db_write(
                        "UPDATE contacts SET outreach_draft = NULL, current_stage = 'RESEARCHED', updated_at = ? WHERE contact_id = ?",
                        (_now(), c["contact_id"]),
                    )
                    continue

                result = asyncio.run(gmail_send(
                    to=c["email"],
                    subject=draft.get("subject", f"Quick question about {c['company_name'] or 'your business'}"),
                    body=draft.get("body", c["outreach_draft"]),
                ))

                if result.get("status") != "sent":
                    logger.warning(f"[EmailSend] Gmail failed for {c['email']}: status={result.get('status')}, error={result.get('error', 'unknown')}")

                if result.get("status") == "sent":
                    _db_write(
                        "UPDATE contacts SET current_stage = 'EMAIL_SENT', updated_at = ? WHERE contact_id = ?",
                        (_now(), c["contact_id"]),
                    )
                    _db_write(
                        """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
                           VALUES (?, 'email_sent', 'Outbound email sent via Gmail SMTP', ?, ?)""",
                        (c["contact_id"], json.dumps({"to": c["email"], "subject": draft.get("subject", "")}), _now()),
                    )
                    # Track in emails table
                    _db_write(
                        """INSERT INTO emails (contact_id, direction, status, subject, body, sent_at, created_at)
                           VALUES (?, 'outbound', 'SENT', ?, ?, ?, ?)""",
                        (c["contact_id"], draft.get("subject", ""), draft.get("body", ""), _now(), _now()),
                    )

                    perf_id = record_email_performance(
                        template_name=f"cold_{c['company_name'] or 'unknown'}",
                        subject=draft.get("subject", ""),
                        body=draft.get("body", ""),
                        confidence=draft.get("confidence", 0),
                    )

                    now = datetime.now(timezone.utc)
                    record_timing(now.hour, now.weekday(), is_reply=False)
                    count += 1
                    logger.info(f"[EmailSend] Sent to {c['first_name']} {c['last_name']} <{c['email']}> (perf_id: {perf_id})")

                    delay = random.uniform(45, 120)
                    logger.info(f"[EmailSend] Waiting {delay:.0f}s before next send...")
                    time.sleep(delay)
            except Exception as e:
                logger.warning(f"[EmailSend] Failed for {c['email']}: {type(e).__name__}: {e}")

        logger.info(f"[EmailSend] {count} emails sent this cycle")
        return count
    except Exception as e:
        logger.error(f"[EmailSend] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: FOLLOW-UPS
# ═══════════════════════════════════════════════════════════════════════════════

def _run_followups():
    """Send follow-up emails for due contacts."""
    # Check if email channel is enabled for this tenant
    if "email" not in _get_tenant_setting("campaign_channels", ["email", "linkedin"]):
        logger.info("[FollowUp] Email channel disabled for this tenant, skipping.")
        return 0

    # Set the follow-up template for this tenant
    from clawbuildr_scheduler import set_followup_template
    template = _get_tenant_setting("sequence_template", "gentle")
    set_followup_template(template)

    try:
        from clawbuildr_scheduler import process_due_followups, mark_sent
        from clawbuildr_ai_email import generate_followup_email, save_generated_email
        from tools import gmail_send

        due = process_due_followups()
        if not due:
            return 0

        count = 0
        for f in due:
            try:
                db = _get_db()
                contact = db.execute(
                    """SELECT c.*, co.name as company_name
                       FROM contacts c LEFT JOIN companies co ON c.company_id = co.company_id
                       WHERE c.contact_id = ?""",
                    (f["contact_id"],)
                ).fetchone()
                db.close()

                if not contact:
                    continue

                previous_emails = []
                email_db = _get_db()
                prev = email_db.execute(
                    """SELECT direction, subject, body FROM emails
                       WHERE contact_id = ? ORDER BY created_at DESC LIMIT 3""",
                    (f["contact_id"],)
                ).fetchall()
                email_db.close()
                for p in prev:
                    previous_emails.append({
                        "direction": p["direction"],
                        "subject": p["subject"],
                        "body": p["body"],
                    })

                logger.info(f"[FollowUp] Generating email for {contact['email']} step={f['step']}")

                result = generate_followup_email(
                    contact_id=f["contact_id"],
                    company_name=contact["company_name"] or "Unknown",
                    previous_emails=previous_emails,
                    followup_step=f["step"],
                )

                # Fallback to prefab templates if Gemini fails
                if not result.get("subject") or not result.get("body"):
                    from clawbuildr_ai_email import _prefab_template_fallback
                    category = "FOLLOWUP" if f["step"] <= 2 else "FOLLOWUP"
                    result = _prefab_template_fallback(
                        company_name=contact["company_name"] or "jullie bedrijf",
                        first_name=contact["first_name"] or "daar",
                        industry=contact["industry"] or "dienstverlening",
                        pain_point="de uitdagingen die jullie momenteel ervaren",
                        value_prop="een bewezen aanpak die tijd bespaart",
                    )

                if result.get("subject") and result.get("body"):
                    # Skip email verification for follow-ups — we already sent the first email successfully
                    # The SMTP RCPT TO check often fails for corporate mailboxes that block unauthenticated probes

                    # Check for company ID in subject
                    if re.search(r'co_[a-f0-9]+|prsp_[a-f0-9]+|comp[_\w]+', result["subject"]):
                        logger.warning(f"[FollowUp] SKIP {contact['email']}: company ID in subject")
                        mark_sent(f["queue_id"], 0)
                        continue

                    send_result = asyncio.run(gmail_send(
                        to=contact["email"],
                        subject=result["subject"],
                        body=result["body"],
                    ))
                    logger.info(f"[FollowUp] Send result for {contact['email']}: {send_result.get('status', 'unknown')}")

                    if send_result.get("status") == "sent":
                        email_id = save_generated_email(
                            contact_id=f["contact_id"],
                            subject=result["subject"],
                            body=result["body"],
                            direction="outbound",
                            confidence=result.get("confidence", 0),
                            model_used=result.get("model_used", ""),
                        )
                        mark_sent(f["queue_id"], email_id)
                        count += 1
                        logger.info(
                            f"[FollowUp] Step {f['step']} sent to {contact['first_name']} {contact['last_name']}"
                        )
                        time.sleep(random.uniform(30, 90))

            except Exception as e:
                logger.warning(f"[FollowUp] Failed for {f.get('email', 'unknown')}: {e}")

        logger.info(f"[FollowUp] {count}/{len(due)} follow-ups sent")
        return count
    except Exception as e:
        logger.error(f"[FollowUp] Error: {e}", exc_info=True)
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: LINKEDIN CONNECTIONS (20/day target)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_linkedin_connections():
    """Send LinkedIn connection requests to contacts with LinkedIn URLs."""
    # Check if LinkedIn channel is enabled for this tenant
    if "linkedin" not in _get_tenant_setting("campaign_channels", ["email", "linkedin"]):
        logger.info("[LinkedIn] LinkedIn channel disabled for this tenant, skipping.")
        return 0

    try:
        from clawbuildr_linkedin_integration import get_linkedin_stats
        from linkedin_engine import search_and_connect, can_send_connection

        stats = get_linkedin_stats()
        sent_today = stats.get("sent_today", 0)
        daily_limit = 20

        if sent_today >= daily_limit:
            logger.info(f"[LinkedIn] Daily limit reached ({sent_today}/{daily_limit})")
            return 0

        can_send, period_info = can_send_connection()
        if not can_send:
            logger.info(f"[LinkedIn] Cannot send now: {period_info}")
            return 0

        db = _get_db()
        remaining = daily_limit - sent_today
        prospects = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.linkedin_url,
                      c.research_result, co.name as company_name, co.domain
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.linkedin_url IS NOT NULL AND c.linkedin_url != ''
                 AND c.current_stage NOT IN ('CLOSED_WON', 'CLOSED_LOST')
                 AND c.contact_id NOT IN (
                     SELECT contact_id FROM linkedin_outreach
                     WHERE outcome IN ('SUCCESS', 'PENDING', 'ALREADY_PENDING', 'ALREADY_CONNECTED')
                       AND contact_id IS NOT NULL AND contact_id != ''
                 )
               ORDER BY c.lead_score DESC
               LIMIT ?""",
            (min(remaining, 6),)
        ).fetchall()
        db.close()

        if not prospects:
            logger.info("[LinkedIn] No eligible prospects with LinkedIn URLs")
            return 0

        count = 0
        for p in prospects:
            if shutdown_requested:
                break

            try:
                research_result = None
                if p["research_result"]:
                    try:
                        research_result = json.loads(p["research_result"])
                    except Exception:
                        pass

                result = search_and_connect(
                    first_name=p["first_name"] or "",
                    last_name=p["last_name"] or "",
                    company=p["company_name"] or "",
                    email_body="",
                    contact_id=p["contact_id"],
                    profile_url=p["linkedin_url"],
                    research_result=research_result,
                )

                outcome = result.get("outcome", "FAILURE")
                logger.info(
                    f"[LinkedIn] {p['first_name']} {p['last_name']} @ {p['company_name']}: {outcome}"
                )

                if outcome in ("SUCCESS", "SUCCESS_NO_NOTE"):
                    count += 1
                    time.sleep(random.uniform(120, 300))

            except Exception as e:
                logger.warning(f"[LinkedIn] Error for {p['first_name']}: {type(e).__name__}: {e}")

        logger.info(f"[LinkedIn] {count} connections sent today ({sent_today + count}/{daily_limit})")
        return count
    except Exception as e:
        logger.error(f"[LinkedIn] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7: REPLY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _run_reply_check():
    """Check for new replies via Gmail IMAP + record learning + A/B results + bounces."""
    try:
        from clawbuildr_reply_detector import check_replies, check_bounces
        from clawbuildr_learning import record_reply, record_timing, add_learning, record_ab_test_result

        # Check for bounces first
        bounces = check_bounces(since_hours=48)
        if bounces:
            logger.info(f"[ReplyCheck] Detected {len(bounces)} bounces")

        replies = check_replies(since_hours=24)

        if replies:
            db = _get_db()
            for r in replies:
                try:
                    contact_id = r.get("contact_id")
                    if contact_id:
                        draft_json = db.execute(
                            "SELECT outreach_draft FROM contacts WHERE contact_id = ?",
                            (contact_id,)
                        ).fetchone()
                        if draft_json and draft_json["outreach_draft"]:
                            try:
                                draft = json.loads(draft_json["outreach_draft"])
                                ab_tests = draft.get("ab_tests", {})
                                for test_name, variant in ab_tests.items():
                                    test_row = db.execute(
                                        "SELECT id FROM ab_tests WHERE test_name = ? AND status = 'running'",
                                        (test_name,)
                                    ).fetchone()
                                    if test_row:
                                        record_ab_test_result(test_row["id"], variant, is_reply=True)
                                        logger.info(f"[ReplyCheck] A/B reply recorded: {test_name}={variant}")
                            except Exception:
                                pass

                        perf_rows = db.execute(
                            "SELECT id FROM email_performance ORDER BY created_at DESC LIMIT 1"
                        ).fetchall()
                        for row in perf_rows:
                            record_reply(row["id"], is_meeting=False)

                    sentiment = r.get("sentiment", "neutral")
                    if sentiment == "interested":
                        add_learning(
                            "positive_reply",
                            f"Positive reply from {r.get('from', 'unknown')}: {r.get('subject', '')[:100]}",
                            json.dumps(r),
                            confidence=0.8,
                        )
                    elif sentiment == "objection":
                        add_learning(
                            "objection",
                            f"Objection from {r.get('from', 'unknown')}: {r.get('body', '')[:200]}",
                            json.dumps(r),
                            confidence=0.7,
                        )

                    now = datetime.now(timezone.utc)
                    record_timing(now.hour, now.weekday(), is_reply=True)
                except Exception as e:
                    logger.debug(f"[ReplyCheck] Learning error: {e}")

            db.commit()
            db.close()

        logger.info(f"[ReplyCheck] {len(replies)} new replies detected")
        return len(replies)
    except Exception as e:
        logger.error(f"[ReplyCheck] Error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def _print_status():
    try:
        db = _get_db()
        counts = {}
        for row in db.execute("SELECT current_stage, COUNT(*) as cnt FROM contacts GROUP BY current_stage"):
            counts[row["current_stage"]] = row["cnt"]

        total = sum(counts.values())
        queued = db.execute("SELECT COUNT(*) as c FROM followup_queue WHERE status IN ('pending','ready')").fetchone()["c"]
        sent_today = db.execute(
            """SELECT COUNT(*) as c FROM activity_log
               WHERE activity_type = 'email_sent' AND created_at > date('now')"""
        ).fetchone()["c"]
        linkedin_today = db.execute(
            """SELECT COUNT(*) as c FROM linkedin_outreach
               WHERE date(timestamp) = date('now')"""
        ).fetchone()["c"]

        drafts = db.execute(
            """SELECT COUNT(*) as c FROM contacts
               WHERE current_stage = 'ACTIVE_OUTREACH'
                 AND outreach_draft IS NOT NULL AND outreach_draft != ''"""
        ).fetchone()["c"]

        db.close()
        logger.info(
            f"[Status] Total: {total} | "
            f"INGESTED: {counts.get('INGESTED', 0)} | "
            f"ACTIVE: {counts.get('ACTIVE_OUTREACH', 0)} | "
            f"SENT: {counts.get('EMAIL_SENT', 0)} | "
            f"Replied: {counts.get('REPLIED', 0)} | "
            f"Drafts ready: {drafts} | "
            f"Emails today: {sent_today}/15 | "
            f"LinkedIn today: {linkedin_today}/20 | "
            f"Queued FU: {queued}"
        )
    except Exception as e:
        logger.error(f"[Status] Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def run_forever():
    global shutdown_requested
    if not _acquire_pid_lock():
        logger.error("Another pipeline_runner is already running — exiting.")
        return
    logger.info("=" * 60)
    logger.info("ClawBuildr Pipeline Runner — Starting 24/7 loop")
    logger.info("Goal: 300+ emails in 7 days, 20 LinkedIn connections/day")
    logger.info("Agents: LeadGen | Enrollment | AI Email | SMTP Send | LinkedIn | Follow-ups | Reply Check")
    logger.info("Press Ctrl+C to stop gracefully")
    logger.info("=" * 60)

    cycle = 0
    while not shutdown_requested:
        cycle += 1
        logger.info(f"\n{'='*60}\nCycle {cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")

        try:
            _run_lead_gen()
            if shutdown_requested:
                break
            time.sleep(8)

            _run_enrollment()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_research()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_email_gen()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_quality_gate()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_regeneration()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_email_send()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_followups()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_linkedin_connections()
            if shutdown_requested:
                break
            time.sleep(5)

            _run_reply_check()
            if shutdown_requested:
                break

            _print_status()

        except Exception as e:
            logger.error(f"Cycle {cycle} error: {e}", exc_info=True)

        sleep_time = random.uniform(300, 600)
        logger.info(f"Next cycle in {sleep_time/60:.1f} minutes")

        for _ in range(int(sleep_time)):
            if shutdown_requested:
                break
            time.sleep(1)

    logger.info("Pipeline Runner stopped.")
    _release_pid_lock()


if __name__ == "__main__":
    run_forever()
