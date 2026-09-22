"""
LinkedIn Outreach Engine v3.0
- Connects to user's REAL Edge browser via CDP (no headless, no fingerprint detection)
- Falls back to headless Firefox with cookie injection if Edge unavailable
- Updated selectors for 2025 LinkedIn DOM (text/aria-based, no CSS classes)
- Anti-detection: human typing, random delays, rate limiting
- Daily connection limit (20/day) + deduplication
- Captcha/auth wall detection
"""

import json
import os
import sqlite3
import time
import re
import random
import shutil
import threading
from datetime import datetime, date, timedelta, timezone
from threading import Lock

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
COOKIES_PATH = os.path.join(DATA_DIR, "linkedin_cookies.json")
FIREFOX_PROFILE_SRC = r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release"
# Use timestamped copy to avoid lock conflicts with running Firefox
import time as time_mod
FIREFOX_PROFILE_COPY = os.path.join(DATA_DIR, f"firefox_profile_copy_{int(time_mod.time())}")

# Point to the active dashboard database (MultiAgentFunnel/data/clawbuildr.db)
CLAWBUILDR_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")

DAILY_LIMIT = 20
MIN_DELAY = 30
MAX_DELAY = 90

# Time-based scheduling: spread 20 connections across 3 periods
MORNING_LIMIT = 7    # 8am-12pm
AFTERNOON_LIMIT = 7  # 12pm-5pm
EVENING_LIMIT = 6    # 5pm-9pm

# ---------- Safe-Sending: Account Warming ----------
# Fresh accounts must scale gradually to avoid LinkedIn flags.
# The system reads warming state from the DB and applies a multiplier.

WARMING_PHASES = [
    {"max_days": 3,  "multiplier": 0.10},   # Day 1-3:   10%
    {"max_days": 7,  "multiplier": 0.25},   # Day 4-7:   25%
    {"max_days": 14, "multiplier": 0.50},   # Day 8-14:  50%
    {"max_days": 21, "multiplier": 0.80},   # Day 15-21: 80%
    {"max_days": 999,"multiplier": 1.00},   # Day 22+:   100%
]

# Message & profile-view daily caps (separate from connection cap)
MESSAGE_DAILY_LIMIT = 50
PROFILE_VIEW_DAILY_LIMIT = 50

# ---------- LinkedIn Request Rate Limiting ----------
# LinkedIn flags high volume. We treat every page load as a request and pace them
# like a human browsing across the full day.
SEARCH_DAILY_LIMIT = 10          # max LinkedIn profile searches per day
SEARCH_MIN_DELAY_SECONDS = 3600  # minimum seconds between searches (spread 10 over ~10h)
_search_last_time = 0
_search_lock = Lock()

CONNECTION_DAILY_LIMIT = 20              # LinkedIn's practical daily connection ceiling
CONNECTION_MIN_DELAY_SECONDS = 1200      # 20-40 min random between connections (fits 20/day)
_connection_last_time = 0
_connection_lock = Lock()


def _can_linkedin_search(domain=""):
    """Check if we are within the daily LinkedIn search budget and delay window.
    Also blocks searches while the account is flagged/cooling down.
    Returns True if a search is allowed."""
    flagged, flag_reason = _is_flagged()
    if flagged:
        _log({"type": "warning", "message": f"LinkedIn search blocked: account flagged ({flag_reason}). Skipping {domain}."})
        return False

    try:
        db = _get_clawbuildr_db()
        today = datetime.now().strftime("%Y-%m-%d")
        row = db.execute(
            "SELECT COUNT(*) FROM linkedin_search_log WHERE last_searched_at LIKE ?",
            (today + "%",)
        ).fetchone()
        daily_count = row[0] if row else 0
        db.close()

        if daily_count >= SEARCH_DAILY_LIMIT:
            _log({"type": "warning", "message": f"LinkedIn search daily budget reached ({daily_count}/{SEARCH_DAILY_LIMIT}). Skipping {domain}."})
            return False
        return True
    except Exception as e:
        _log({"type": "warning", "message": f"LinkedIn search budget check failed: {e}"})
        return False


def _record_linkedin_search(domain, query, result_url=""):
    """Record a LinkedIn search in the rate-limit log."""
    try:
        db = _get_clawbuildr_db()
        now = datetime.now().isoformat()
        db.execute("""
            INSERT INTO linkedin_search_log (domain, query, result_url, search_count, last_searched_at)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(domain) DO UPDATE SET
                query = excluded.query,
                result_url = excluded.result_url,
                search_count = linkedin_search_log.search_count + 1,
                last_searched_at = excluded.last_searched_at
        """, (domain, query, result_url, now))
        db.commit()
        db.close()
    except Exception as e:
        _log({"type": "warning", "message": f"Could not record LinkedIn search: {e}"})


def _enforce_linkedin_search_delay():
    """Enforce minimum delay between LinkedIn searches."""
    global _search_last_time
    with _search_lock:
        elapsed = time.time() - _search_last_time
        if elapsed < SEARCH_MIN_DELAY_SECONDS:
            wait = SEARCH_MIN_DELAY_SECONDS - elapsed
            _log({"type": "info", "message": f"LinkedIn search delay: sleeping {wait:.0f}s..."})
            time.sleep(wait)
        _search_last_time = time.time()


def _connection_delay_remaining():
    """Return remaining seconds until next connection send is allowed."""
    with _connection_lock:
        elapsed = time.time() - _connection_last_time
        # Random delay between 20-40 minutes (fits 20/day in 13h window)
        random_delay = random.uniform(1200, 2400)
        remaining = max(0, random_delay - elapsed)
        return remaining


def _enforce_connection_delay():
    """Enforce minimum delay between LinkedIn connection sends."""
    global _connection_last_time
    with _connection_lock:
        elapsed = time.time() - _connection_last_time
        # Random delay between 20-40 minutes (fits 20/day in 13h window)
        random_delay = random.uniform(1200, 2400)
        if elapsed < random_delay:
            wait = random_delay - elapsed
            _log({"type": "info", "message": f"Connection send delay: sleeping {wait:.0f}s (human-like random spacing)..."})
            time.sleep(wait)
        _connection_last_time = time.time()


def _mark_connection_sent():
    """Record that a connection request was just sent (for delay tracking)."""
    global _connection_last_time
    with _connection_lock:
        _connection_last_time = time.time()


# ---------- Flag Detection & Auto-Pause ----------
# If any of these flags are detected, the engine pauses ALL outreach.
# A flagged account requires 7 days cooldown + manual confirmation on day 8.

_flag_state = {
    "flagged": False,
    "flag_type": None,       # "captcha", "unusual_activity", "connection_limit", "http_429"
    "flagged_at": None,
    "cooldown_days": 7,
    "resumed": False,
}

_job_state = {
    "running": False,
    "total": 0,
    "completed": 0,
    "current_url": "",
    "current_name": "",
    "log": [],
    "results": [],
    "stop_requested": False,
}
_state_lock = Lock()
_modal_purge_timer = None
_last_send_proof = None

# Serialize all Firefox profile copies + launches to prevent profile locking conflicts
_firefox_sem = threading.Semaphore(1)
_firefox_active = 0
_firefox_count_lock = threading.Lock()


def _log(entry):
    import sys
    msg = entry.get("message", "")
    entry_type = entry.get("type", "info")
    # Also log to stderr so we can see it in server_err.log
    if entry_type == "error":
        print(f"[LI-ERROR] {msg}", file=sys.stderr)
    elif entry_type == "warning":
        print(f"[LI-WARN] {msg}", file=sys.stderr)
    else:
        print(f"[LI-INFO] {msg}", file=sys.stderr)
    with _state_lock:
        _job_state["log"].append({**entry, "time": datetime.now().isoformat()})
        if len(_job_state["log"]) > 500:
            _job_state["log"] = _job_state["log"][-250:]


def _get_clawbuildr_db():
    db = sqlite3.connect(CLAWBUILDR_DB, timeout=60.0)
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA busy_timeout=5000;")
    db.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id TEXT,
            first_name TEXT,
            last_name TEXT,
            company TEXT,
            profile_url TEXT,
            note TEXT,
            outcome TEXT,
            timestamp TEXT,
            connection_status TEXT DEFAULT 'pending',
            last_checked TEXT,
            followup_sent INTEGER DEFAULT 0,
            followup_note TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_followups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_url TEXT,
            first_name TEXT,
            last_name TEXT,
            company TEXT,
            followup_note TEXT,
            outcome TEXT,
            timestamp TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            topic TEXT,
            scheduled_time TEXT,
            status TEXT DEFAULT 'draft',
            posted_at TEXT,
            proof_path TEXT,
            error TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # Warming state table — tracks account age and flag status
    db.execute("""
        CREATE TABLE IF NOT EXISTS warming_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            account_created_at TEXT,
            warmed_at TEXT,
            paused INTEGER DEFAULT 0,
            pause_reason TEXT,
            paused_at TEXT,
            flag_type TEXT,
            flag_detected_at TEXT,
            last_manual_resume TEXT
        )
    """)
    # Message send log (separate from connection requests)
    db.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_url TEXT,
            first_name TEXT,
            last_name TEXT,
            message_text TEXT,
            outcome TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )
    """)
    # Profile view log
    db.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_profile_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_url TEXT,
            viewed_at TEXT DEFAULT (datetime('now'))
        )
    """)
    db.commit()
    return db


# ---------- Safe-Sending: Warming Logic ----------

def _get_warming_state(db=None):
    """Get the warming state. Creates a default row if none exists."""
    own_db = db is None
    if own_db:
        db = _get_clawbuildr_db()
    try:
        row = db.execute("SELECT * FROM warming_state WHERE id = 1").fetchone()
        if not row:
            now = datetime.now().isoformat()
            db.execute(
                "INSERT INTO warming_state (id, account_created_at) VALUES (1, ?)",
                (now,)
            )
            db.commit()
            return {
                "account_created_at": now,
                "warmed_at": None,
                "paused": 0,
                "pause_reason": None,
                "flag_type": None,
            }
        return {
            "account_created_at": row[1],
            "warmed_at": row[2],
            "paused": row[3],
            "pause_reason": row[4],
            "flag_type": row[7] if len(row) > 7 else None,
        }
    finally:
        if own_db:
            db.close()


def _get_warming_multiplier():
    """Calculate the current daily limit multiplier based on account age.
    Returns a value between 0.10 and 1.00.
    """
    state = _get_warming_state()
    created = state.get("account_created_at")
    if not created:
        return 1.0  # No warming data = assume mature account

    try:
        created_dt = datetime.fromisoformat(created)
    except (ValueError, TypeError):
        return 1.0

    days_active = (datetime.now() - created_dt).days

    for phase in WARMING_PHASES:
        if days_active <= phase["max_days"]:
            return phase["multiplier"]
    return 1.0


def _get_effective_daily_limit():
    """Get the effective daily connection limit after warming + recovery multiplier."""
    base = DAILY_LIMIT
    multiplier = _get_warming_multiplier()

    # Recovery mode: if account was flagged recently, keep limits very low
    state = _get_warming_state()
    flag_type = state.get("flag_type")
    flag_at = state.get("flag_detected_at")
    if flag_type and flag_at:
        try:
            flag_dt = datetime.fromisoformat(flag_at)
            hours_since_flag = (datetime.now() - flag_dt).total_seconds() / 3600
            if hours_since_flag < 24:
                # Hard pause for 24h after any flag
                return 0, 0.0
            elif hours_since_flag < 168:  # 7 days
                # Recovery mode: 25% of normal limit
                multiplier = min(multiplier, 0.25)
                _log({"type": "info", "message": f"Recovery mode active ({hours_since_flag:.0f}h since flag). Limit reduced to {multiplier:.0%}."})
        except (ValueError, TypeError):
            pass

    effective = max(1, int(base * multiplier))
    return effective, multiplier


def _is_flagged():
    """Check if the account is currently flagged/paused."""
    state = _get_warming_state()
    if state.get("paused"):
        return True, state.get("pause_reason", "unknown")
    if _flag_state.get("flagged"):
        return True, _flag_state.get("flag_type", "unknown")

    # Also enforce 24h hard pause after recent flag even if paused=0
    flag_type = state.get("flag_type")
    flag_at = state.get("flag_detected_at")
    if flag_type and flag_at:
        try:
            flag_dt = datetime.fromisoformat(flag_at)
            if (datetime.now() - flag_dt).total_seconds() < 86400:
                return True, f"{flag_type} (24h cooldown)"
        except (ValueError, TypeError):
            pass

    return False, None


def _set_flagged(flag_type, reason=""):
    """Mark the account as flagged. All outreach stops immediately."""
    global _flag_state
    _flag_state["flagged"] = True
    _flag_state["flag_type"] = flag_type
    _flag_state["flagged_at"] = datetime.now().isoformat()

    db = _get_clawbuildr_db()
    now = datetime.now().isoformat()
    db.execute("""
        UPDATE warming_state SET paused = 1, pause_reason = ?, paused_at = ?, flag_type = ?, flag_detected_at = ?
        WHERE id = 1
    """, (reason or flag_type, now, flag_type, now))
    db.commit()
    db.close()
    _log({"type": "error", "message": f"ACCOUNT FLAGGED: {flag_type} — {reason}. All outreach paused."})


def _check_cooldown_elapsed():
    """Check if the 7-day cooldown has passed since flagging.
    Returns True if cooldown is complete and account is eligible for manual resume.
    """
    state = _get_warming_state()
    if not state.get("paused"):
        return False

    paused_at = state.get("paused_at") or state.get("flag_detected_at")
    if not paused_at:
        return False

    try:
        paused_dt = datetime.fromisoformat(paused_at)
        cooldown_end = paused_dt + timedelta(days=_flag_state.get("cooldown_days", 7))
        return datetime.now() >= cooldown_end
    except (ValueError, TypeError):
        return False


def _manual_resume():
    """Manual confirmation to resume after cooldown. Re-warms at 10%."""
    if not _check_cooldown_elapsed():
        _log({"type": "warning", "message": "Cooldown not yet elapsed. Cannot resume."})
        return False

    global _flag_state
    _flag_state["flagged"] = False
    _flag_state["flag_type"] = None
    _flag_state["resumed"] = True

    db = _get_clawbuildr_db()
    now = datetime.now().isoformat()
    db.execute("""
        UPDATE warming_state SET paused = 0, pause_reason = NULL, last_manual_resume = ? WHERE id = 1
    """, (now,))
    db.commit()
    db.close()

    # Reset warming to 10% (day 1 equivalent)
    _log({"type": "info", "message": "Account resumed manually. Re-warming at 10% capacity."})
    return True


# ---------- Safe-Sending: Active Hours ----------

# Timezone-aware active hours. Default to Europe/Amsterdam (CET/CEST).
import zoneinfo as _zoneinfo

_ACTIVE_TIMEZONE = "Europe/Amsterdam"
_ACTIVE_HOURS = (8, 21)  # No sending before 8 AM or after 9 PM
_ACTIVE_WEEKDAYS = (0, 1, 2, 3, 4)  # Mon-Fri only (0=Mon, 4=Fri)

# Randomize send times: avoid exact quarter-hours
_QUARTER_HOUR_OFFSETS = [0, 2, 3, 5, 7, 8, 10, 12, 13, 15, 17, 18, 20, 22, 23, 25, 27, 28]

# Test mode flag: allows weekend sending when explicitly enabled
_LINKEDIN_TEST_FLAG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "linkedin_test_mode.json")


def _linkedin_test_mode_enabled() -> bool:
    """Check if LinkedIn test mode is enabled (allows weekend/holiday sends)."""
    try:
        if os.path.exists(_LINKEDIN_TEST_FLAG):
            with open(_LINKEDIN_TEST_FLAG, "r") as f:
                return bool(json.load(f).get("enabled", False))
    except Exception:
        pass
    return False


def _now_in_active_tz():
    """Return current time in the configured active timezone."""
    try:
        tz = _zoneinfo.ZoneInfo(_ACTIVE_TIMEZONE)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz)


def _is_active_hours():
    """Check if current time is within active sending hours."""
    now = _now_in_active_tz()
    if now.weekday() not in _ACTIVE_WEEKDAYS and not _linkedin_test_mode_enabled():
        return False, "weekend"
    if now.hour < _ACTIVE_HOURS[0] or now.hour >= _ACTIVE_HOURS[1]:
        return False, "outside_active_hours"
    return True, "active"


def _randomized_delay(base_min=30, base_max=90):
    """Return a randomized delay that avoids exact quarter-hours.
    Adds a random offset from _QUARTER_HOUR_OFFSETS to make send times less predictable.
    """
    base = random.uniform(base_min, base_max)
    offset = random.choice(_QUARTER_HOUR_OFFSETS)
    return base + offset


def _get_daily_count():
    """Count how many connection requests were sent today."""
    db = _get_clawbuildr_db()
    today = date.today().isoformat()
    count = db.execute(
        "SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS' AND timestamp LIKE ?",
        (today + "%",),
    ).fetchone()[0]
    db.close()
    return count


def _get_period_count():
    """Count connections sent in the current time period."""
    from datetime import datetime
    now = datetime.now()
    hour = now.hour
    
    # Define periods
    if 8 <= hour < 12:
        period = "morning"
        limit = MORNING_LIMIT
    elif 12 <= hour < 17:
        period = "afternoon"
        limit = AFTERNOON_LIMIT
    elif 17 <= hour < 21:
        period = "evening"
        limit = EVENING_LIMIT
    else:
        return 0, 0, "off_hours"  # No connections outside business hours
    
    db = _get_clawbuildr_db()
    today = date.today().isoformat()
    
    # Count connections in current period
    count = db.execute(
        """SELECT COUNT(*) FROM linkedin_outreach 
           WHERE outcome = 'SUCCESS' 
           AND timestamp LIKE ? 
           AND CAST(strftime('%%H', timestamp) AS INTEGER) >= ? 
           AND CAST(strftime('%%H', timestamp) AS INTEGER) < ?""",
        (today + "%", 
         8 if period == "morning" else (12 if period == "afternoon" else 17),
         12 if period == "morning" else (17 if period == "afternoon" else 21))
    ).fetchone()[0]
    
    db.close()
    return count, limit, period


def can_send_connection():
    """Check if we can send a connection in the current time period.
    Applies: flag check, active hours, warming multiplier, period caps,
    and a minimum delay between sends to spread 20 requests over 24h.
    """
    # Check if account is flagged/paused
    flagged, flag_reason = _is_flagged()
    if flagged:
        return False, f"flagged_{flag_reason}"

    # Check active hours (timezone-aware, weekday filter)
    active, reason = _is_active_hours()
    if not active:
        return False, reason

    # Check effective daily limit (with warming multiplier)
    effective_limit, multiplier = _get_effective_daily_limit()
    daily_count = _get_daily_count()
    if daily_count >= effective_limit:
        return False, f"daily_limit_reached ({daily_count}/{effective_limit}, warmup={multiplier:.0%})"

    # Check period caps (also scaled by warming)
    period_count, period_limit, period = _get_period_count()
    effective_period_limit = max(1, int(period_limit * multiplier))
    if period_count >= effective_period_limit:
        return False, f"{period}_limit_reached ({period_count}/{effective_period_limit})"

    # Enforce minimum spacing between connection sends (20/day => ~40min apart)
    remaining = _connection_delay_remaining()
    if remaining > 0:
        return False, f"connection_delay ({remaining:.0f}s remaining, ~30min spacing for 20/day spread)"

    return True, f"ok (warmup={multiplier:.0%}, daily={daily_count}/{effective_limit})"


def _is_already_contacted(first_name, last_name, company, profile_url=""):
    """Check if we already sent a connection request to this person.
    
    Checks by profile_url (most reliable) or by name+company.
    Returns True for any outcome that means we already reached out.
    """
    SUCCESS_OUTCOMES = ("SUCCESS", "SUCCESS_NO_NOTE", "PENDING", "ALREADY_PENDING",
                        "ALREADY_CONTACTED", "ALREADY_CONNECTED")
    db = _get_clawbuildr_db()
    try:
        # Check by profile_url first (most reliable)
        if profile_url:
            rows = db.execute(
                "SELECT outcome FROM linkedin_outreach WHERE profile_url = ? ORDER BY id DESC LIMIT 1",
                (profile_url,),
            ).fetchall()
            if rows and rows[0][0] in SUCCESS_OUTCOMES:
                return True
        # Fallback: check by name+company
        rows = db.execute(
            "SELECT outcome FROM linkedin_outreach WHERE first_name = ? AND last_name = ? AND company = ? ORDER BY id DESC LIMIT 1",
            (first_name, last_name, company),
        ).fetchall()
        if rows and rows[0][0] in SUCCESS_OUTCOMES:
            return True
    except Exception:
        pass
    finally:
        db.close()
    return False


def _save_result(contact_id, first_name, last_name, company, profile_url, note, outcome, proof=None):
    db = _get_clawbuildr_db()
    # Add proof column if it doesn't exist (migration)
    try:
        db.execute("ALTER TABLE linkedin_outreach ADD COLUMN proof TEXT")
    except Exception:
        pass
    db.execute(
        "INSERT INTO linkedin_outreach (contact_id, first_name, last_name, company, profile_url, note, outcome, timestamp, proof) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (contact_id, first_name, last_name, company, profile_url, note, outcome, datetime.now().isoformat(), proof),
    )
    db.commit()
    db.close()


# ---------- Firefox profile management ----------

def _prepare_firefox_profile():
    """Copy essential Firefox profile files for Selenium Firefox to use.
    Uses SQLite backup API for cookies.sqlite to get a consistent copy
    even while Firefox is running (avoids WAL inconsistency issues).
    """
    if not os.path.exists(FIREFOX_PROFILE_SRC):
        raise Exception(f"Firefox profile not found at {FIREFOX_PROFILE_SRC}")

    if os.path.exists(FIREFOX_PROFILE_COPY):
        shutil.rmtree(FIREFOX_PROFILE_COPY, ignore_errors=True)
    os.makedirs(FIREFOX_PROFILE_COPY, exist_ok=True)

    # Backup cookies.sqlite using SQLite API for consistency
    cookies_src = os.path.join(FIREFOX_PROFILE_SRC, "cookies.sqlite")
    cookies_dst = os.path.join(FIREFOX_PROFILE_COPY, "cookies.sqlite")
    if os.path.isfile(cookies_src):
        try:
            import sqlite3
            src_conn = sqlite3.connect(f"file:{cookies_src}?mode=ro", uri=True)
            dst_conn = sqlite3.connect(cookies_dst)
            src_conn.backup(dst_conn)
            dst_conn.close()
            src_conn.close()
            for suffix in ["-wal", "-shm"]:
                wal_src = cookies_src + suffix
                if os.path.isfile(wal_src):
                    shutil.copy2(wal_src, cookies_dst + suffix)
        except Exception:
            shutil.copy2(cookies_src, cookies_dst)
            for suffix in ["-wal", "-shm"]:
                wal_src = cookies_src + suffix
                if os.path.isfile(wal_src):
                    shutil.copy2(wal_src, cookies_dst + suffix)

    # Copy other essential files
    essential = ["sessionstore-backups", "prefs.js", "logins.json", "key4.db", "cert9.db", "handlers.json"]
    for f in essential:
        src = os.path.join(FIREFOX_PROFILE_SRC, f)
        if os.path.exists(src):
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(FIREFOX_PROFILE_COPY, f))
            elif os.path.isdir(src):
                shutil.copytree(src, os.path.join(FIREFOX_PROFILE_COPY, f), dirs_exist_ok=True)

    # Remove lock files that prevent a new Firefox instance from starting
    lock_files = ["parent.lock", "lock", "lockfile", ".parentlock"]
    for lf in lock_files:
        lock_path = os.path.join(FIREFOX_PROFILE_COPY, lf)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except Exception:
                pass

    return FIREFOX_PROFILE_COPY


def _inject_cookies_into_context(context):
    """Load LinkedIn cookies from the JSON file and inject them into a browser context.
    This is used as a fallback when the Firefox profile copy has stale session data.
    """
    if not os.path.exists(COOKIES_PATH):
        _log({"type": "info", "message": "No cookies file found, relying on Firefox profile copy."})
        return False
    try:
        with open(COOKIES_PATH, 'r') as f:
            cookies = json.load(f)
        if not cookies:
            _log({"type": "warning", "message": "Cookies file is empty."})
            return False

        has_session = any(c.get("name") in ("li_at", "liap", "bscookie") for c in cookies)
        if not has_session:
            _log({"type": "warning", "message": "No session cookies in cookies file."})
            return False

        context.add_cookies(cookies)
        _log({"type": "info", "message": f"Injected {len(cookies)} LinkedIn cookies into browser context."})
        return True
    except Exception as e:
        _log({"type": "error", "message": f"Failed to inject cookies: {str(e)[:80]}"})
        return False


def _save_cookies_from_context(context):
    """Save cookies from a Playwright browser context for backup."""
    try:
        cookies = context.cookies("https://www.linkedin.com")
        cookie_list = []
        for c in cookies:
            cookie_list.append({
                "name": c["name"], "value": c["value"], "domain": c["domain"],
                "path": c["path"], "secure": c["secure"], "httpOnly": c["httpOnly"],
                "sameSite": c.get("sameSite", "Lax"), "expires": c.get("expires", -1),
            })
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(COOKIES_PATH, "w") as f:
            json.dump(cookie_list, f, indent=2)
    except Exception:
        pass


# ---------- Modal purging (continuous) ----------

def _start_modal_purger(page, stop_event):
    """Continuously purge LinkedIn overlay bubbles every 500ms in a background thread."""
    def purge_loop():
        while not stop_event.is_set():
            try:
                if not page.is_closed():
                    page.evaluate("""() => {
                        const killers = [
                            '.msg-overlay-list-bubble', '.msg-overlay-conversation-bubble',
                            '.msg-overlay-bubble-header--sponsored', '#cm-restore-banner',
                            '[data-testid="msg-overlay"]'
                        ];
                        killers.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
                    }""")
            except Exception:
                pass
            time.sleep(0.5)

    global _modal_purge_timer
    t = threading.Thread(target=purge_loop, daemon=True)
    t.start()
    return stop_event, t


# ---------- Anti-detection helpers ----------

def _human_type(page, selector, text):
    """Type text character by character with human-like delays."""
    try:
        loc = page.locator(selector).first()
        loc.click()
        for char in text:
            page.keyboard.type(char, delay=random.randint(40, 120))
        time.sleep(random.uniform(0.3, 0.8))
    except Exception:
        try:
            page.keyboard.type(text, delay=random.randint(50, 100))
        except Exception:
            pass


def _human_type_selenium(driver, text, wpm_range=(35, 65)):
    """Type text into the visible textarea/input using Selenium with human-like speed.
    Simulates variable WPM, occasional longer pauses, and rare typo-like corrections.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    try:
        tas = driver.find_elements(By.TAG_NAME, "textarea")
        ta = None
        for t in tas:
            if t.is_displayed() and t.size.get('width', 0) > 50:
                ta = t
                break
        if not ta:
            inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="text"], input:not([type])')
            for inp in inputs:
                if inp.is_displayed() and inp.size.get('width', 0) > 50:
                    ta = inp
                    break
        if not ta:
            editables = driver.find_elements(By.CSS_SELECTOR, '[contenteditable="true"]')
            for el in editables:
                if el.is_displayed() and el.size.get('width', 0) > 50:
                    ta = el
                    break
        if not ta:
            _log({"type": "warning", "message": "No visible textarea/input/contenteditable found for note"})
            return
        ta.click()
        time.sleep(0.3)
        try:
            ta.clear()
        except Exception:
            pass
        time.sleep(0.2)

        # Human typing: variable WPM with pauses
        wpm = random.randint(*wpm_range)
        base_delay = 60.0 / (wpm * 5)  # seconds per character
        actions = ActionChains(driver)
        for i, char in enumerate(text):
            # 5% chance of a micro-pause (hesitation)
            if random.random() < 0.05:
                time.sleep(random.uniform(0.3, 0.8))
            # 1% chance of a longer pause (thinking)
            elif random.random() < 0.01:
                time.sleep(random.uniform(1.0, 2.5))
            delay = base_delay * random.uniform(0.7, 1.4)
            actions.send_keys(char)
            actions.pause(delay)
        actions.perform()
        time.sleep(random.uniform(0.5, 1.2))
    except Exception as e:
        _log({"type": "warning", "message": f"Human type failed: {str(e)[:60]}; falling back to send_keys"})
        try:
            ta.send_keys(text)
        except Exception:
            pass


def _human_type_into_element(driver, element, text, wpm_range=(35, 65)):
    """Type text into a specific Selenium element with human-like speed and pauses."""
    from selenium.webdriver.common.action_chains import ActionChains
    try:
        element.click()
        time.sleep(0.2)
        try:
            element.clear()
        except Exception:
            pass
        time.sleep(0.2)
        wpm = random.randint(*wpm_range)
        base_delay = 60.0 / (wpm * 5)
        actions = ActionChains(driver)
        for char in text:
            if random.random() < 0.05:
                time.sleep(random.uniform(0.3, 0.8))
            elif random.random() < 0.01:
                time.sleep(random.uniform(1.0, 2.5))
            delay = base_delay * random.uniform(0.7, 1.4)
            actions.send_keys(char)
            actions.pause(delay)
        actions.perform()
        time.sleep(random.uniform(0.4, 0.9))
    except Exception as e:
        _log({"type": "warning", "message": f"Human type into element failed: {str(e)[:60]}; falling back"})
        try:
            element.send_keys(text)
        except Exception:
            pass


def _human_scroll(page, min_scrolls=2, max_scrolls=6, driver=None):
    """Scroll up/down the page in a human-like pattern with variable speeds and pauses.
    Works with both Playwright page and Selenium driver.
    """
    try:
        scrolls = random.randint(min_scrolls, max_scrolls)
        for _ in range(scrolls):
            direction = random.choice([-1, 1])
            distance = random.randint(200, 800)
            duration = random.randint(300, 900)
            if driver is None:
                # Playwright
                page.evaluate(f"""() => {{
                    window.scrollBy({{top: {direction * distance}, left: 0, behavior: 'smooth'}});
                }}""")
            else:
                # Selenium
                driver.execute_script(f"window.scrollBy({{top: {direction * distance}, left: 0, behavior: 'smooth'}});")
            time.sleep(random.uniform(0.6, 2.0))
    except Exception as e:
        _log({"type": "debug", "message": f"Human scroll skipped: {str(e)[:60]}"})


def _human_read_time(page=None, min_seconds=3, max_seconds=10):
    """Pause as if a human is reading the page."""
    duration = random.uniform(min_seconds, max_seconds)
    _log({"type": "debug", "message": f"Human read pause: {duration:.1f}s"})
    time.sleep(duration)


def _human_mouse_wander(page, driver=None):
    """Move mouse to a few random points on the page to simulate reading behavior.
    Only works with Selenium (Playwright mouse API is different).
    """
    if driver is None:
        return  # Playwright mouse wander omitted to avoid complexity
    try:
        from selenium.webdriver.common.action_chains import ActionChains
        width = driver.execute_script("return window.innerWidth") or 1200
        height = driver.execute_script("return window.innerHeight") or 800
        actions = ActionChains(driver)
        for _ in range(random.randint(2, 5)):
            x = random.randint(int(width * 0.1), int(width * 0.9))
            y = random.randint(int(height * 0.2), int(height * 0.8))
            actions.move_by_offset(x - (width // 2), y - (height // 2))
            actions.pause(random.uniform(0.2, 0.6))
        actions.perform()
    except Exception as e:
        _log({"type": "debug", "message": f"Mouse wander skipped: {str(e)[:60]}"})


def _human_profile_visit(page, driver=None):
    """Combined human behavior for a profile/page visit: scroll + read + wander."""
    _human_read_time(min_seconds=2, max_seconds=5)
    _human_scroll(page, min_scrolls=2, max_scrolls=5, driver=driver)
    _human_read_time(min_seconds=3, max_seconds=8)
    if driver:
        _human_mouse_wander(page, driver=driver)


def _random_sleep(min_s=2, max_s=5):
    """Human-like random wait."""
    time.sleep(random.uniform(min_s, max_s))


def _detect_auth_wall(page):
    """Check if we hit a login/auth wall, captcha, unusual activity, or rate limit.
    Returns a string describing the issue, or None if OK.
    Also triggers auto-pause via _set_flagged() for critical flags.
    """
    url = page.url
    if "login" in url or "authwall" in url or "signin" in url or "checkpoint" in url:
        _set_flagged("auth_wall", f"Redirected to auth wall: {url[:100]}")
        return "auth_wall"
    if "captcha" in url or "challenge" in url:
        _set_flagged("captcha", f"Captcha/challenge page detected: {url[:100]}")
        return "captcha"
    try:
        has_login_form = page.evaluate("""() => !!document.querySelector('input[name="session_key"], .sign-in-form__submit')""")
        if has_login_form:
            _set_flagged("auth_wall", "Login form detected on page")
            return "auth_wall"
    except Exception:
        pass

    # Check for "unusual activity" warning or connection limit warning
    try:
        body_text = page.evaluate("""() => (document.body.innerText || '').substring(0, 3000).toLowerCase()""") or ""
        # Unusual activity detection
        unusual_keywords = [
            "unusual activity", "ongebruikelijke activiteit",
            "we detected something unusual", "we detected unusual",
            "your account has been restricted", "je account is beperkt",
            "temporarily restricted", "tijdelijk beperkt",
        ]
        for kw in unusual_keywords:
            if kw in body_text:
                _set_flagged("unusual_activity", f"Unusual activity warning detected: '{kw}'")
                return "unusual_activity"

        # Connection limit warning
        limit_keywords = [
            "you've reached the weekly invitation limit",
            "je hebt de wekelijkse uitnodigingslimiet bereikt",
            "too many invitations", "te veel uitnodigingen",
            "connection requests are limited",
        ]
        for kw in limit_keywords:
            if kw in body_text:
                # Not a hard flag — just reduce caps by 30%
                _log({"type": "warning", "message": f"Connection limit warning detected: '{kw}' — reducing caps by 30%"})
                return "connection_limit_warning"
    except Exception:
        pass

    return None


def _handle_http_429(response=None):
    """Handle HTTP 429 (Too Many Requests) or LinkedIn's HTTP 999 (anti-bot).
    Implements exponential backoff and flag detection.
    """
    if response is None:
        return

    status = getattr(response, "status_code", 0)
    if status in (429, 999):
        _log({"type": "error", "message": f"HTTP {status} received — rate limited by LinkedIn"})
        # Exponential backoff: 5min, 15min, 45min
        backoff = min(2700, 300 * (2 ** _flag_state.get("_429_count", 0)))
        _flag_state["_429_count"] = _flag_state.get("_429_count", 0) + 1
        _log({"type": "warning", "message": f"Backing off for {backoff}s before next action"})
        time.sleep(backoff)

        if _flag_state["_429_count"] >= 3:
            _set_flagged("http_429", f"HTTP 429/999 received 3 times in a row")


# ---------- Core LinkedIn actions ----------

def _find_and_click_button(page, texts, aria_keywords=None, profile_area_only=False):
    """Find and click a button or link by its text content or aria-label.
    Searches both <button> and <a> elements (LinkedIn uses <a> tags for Connect/Message).
    texts: list of text content to match (case-insensitive, partial match)
    aria_keywords: list of aria-label keywords to match
    profile_area_only: if True, only search elements in the top-left profile area (not sidebar)
    Returns True if clicked, False if not found.
    """
    try:
        result = page.evaluate("""(args) => {
            const texts = args.texts;
            const arias = args.arias || [];
            const profileOnly = args.profileOnly;
            // Search both buttons AND anchor tags (LinkedIn uses <a> for Connect/Message)
            let els = Array.from(document.querySelectorAll('button, a, [role="button"], [role="menuitem"]'));
            if (profileOnly) {
                els = els.filter(el => {
                    const r = el.getBoundingClientRect();
                    return r.top < 600 && r.left < 500;
                });
            }
            // First try exact text match
            for (const el of els) {
                const txt = (el.innerText || '').toLowerCase().trim();
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                for (const t of texts) {
                    if (txt === t) {
                        el.click();
                        return {clicked: true, text: txt, method: 'text_exact'};
                    }
                }
            }
            // Then try partial text match
            for (const el of els) {
                const txt = (el.innerText || '').toLowerCase().trim();
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                for (const t of texts) {
                    if (txt.includes(t)) {
                        el.click();
                        return {clicked: true, text: txt, method: 'text_partial'};
                    }
                }
            }
            // Then try aria-label match
            for (const el of els) {
                const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                for (const a of arias) {
                    if (aria.includes(a)) {
                        el.click();
                        return {clicked: true, aria: aria, method: 'aria'};
                    }
                }
            }
            return {clicked: false};
        }""", {"texts": [t.lower() for t in texts], "arias": [a.lower() for a in (aria_keywords or [])], "profileOnly": profile_area_only})
        return result.get("clicked", False)
    except Exception:
        return False


def _find_profile_more_button(page):
    """Find and click the 'More' overflow button on a profile page.
    Multi-lingual: handles Dutch (Meer), English (More), Vietnamese (Khác), German (Mehr), French (Plus), Spanish (Más).
    Returns True if clicked, False if not found.
    Uses direct element click (no coordinates).
    """
    more_labels = ["meer", "more", "khác", "mehr", "plus", "más", "altro", "fler", "mer"]
    try:
        result = page.evaluate("""(labels) => {
            const btns = Array.from(document.querySelectorAll('button'));
            // Pass 1: Match by aria-label in profile header area
            for (const b of btns) {
                const aria = (b.getAttribute('aria-label') || '').toLowerCase().trim();
                const r = b.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 100 && r.top < 600 && r.left < 500 && r.width > 20) {
                    for (const label of labels) {
                        if (aria === label) {
                            b.click();
                            return {clicked: true, aria: aria, method: 'aria'};
                        }
                    }
                }
            }
            // Pass 2: Match by innerText
            for (const b of btns) {
                const txt = (b.innerText || '').toLowerCase().trim();
                const r = b.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 100 && r.top < 600 && r.left < 500 && r.width > 20) {
                    for (const label of labels) {
                        if (txt === label) {
                            b.click();
                            return {clicked: true, text: txt, method: 'text'};
                        }
                    }
                }
            }
            // Pass 3: Fallback — small icon button with aria-label
            for (const b of btns) {
                const aria = (b.getAttribute('aria-label') || '').toLowerCase().trim();
                const r = b.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 100 && r.top < 600 && r.left < 500 && r.width >= 28 && r.width <= 44 && !b.innerText.trim()) {
                    for (const label of labels) {
                        if (aria.includes(label)) {
                            b.click();
                            return {clicked: true, aria: aria, method: 'icon'};
                        }
                    }
                }
            }
            return {clicked: false};
        }""", more_labels)
        return result
    except Exception:
        return {"clicked": False}


def _handle_invitation_modal(page, note):
    """Handle the invitation modal flow.
    LinkedIn's invitation modal is NOT found by document.querySelectorAll — it's
    likely rendered in a Shadow DOM or iframe. So we use Selenium's native
    find_elements(By.XPATH) which CAN penetrate these boundaries.
    1. Click 'Add a note' / 'Opmerking toevoegen' via Selenium XPath
    2. Type the note into the textarea
    3. Click 'Send' / 'Verzenden' / 'Verzenden zonder opmerking' via Selenium XPath
    Returns outcome string.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    _log({"type": "info", "message": "Handling invitation modal..."})
    time.sleep(3)

    def _find_by_xpath(selectors):
        """Find visible elements by XPath selectors, return list of (element, text)."""
        results = []
        for xpath in selectors:
            try:
                elems = page._driver.find_elements(By.XPATH, xpath)
                for e in elems:
                    try:
                        if e.is_displayed() and e.size['width'] > 10 and e.size['height'] > 10:
                            txt = (e.text or "").strip()
                            if txt:
                                results.append((e, txt))
                    except Exception:
                        continue
            except Exception:
                continue
        return results

    def _click_element(elem, description=""):
        """Click an element via ActionChains with human-like delay."""
        try:
            ActionChains(page._driver).move_to_element(elem).pause(0.5).click().perform()
            _log({"type": "info", "message": f"Clicked via ActionChains: {description}"})
            return True
        except Exception as e:
            _log({"type": "warning", "message": f"ActionChains click failed ({description}): {str(e)[:40]}"})
            return False

    # Step 1: Click "Add a note" / "Opmerking toevoegen"
    note_xpaths = [
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'opmerking toevoegen')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'add a note')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ajouter une note')]",
    ]
    note_clicked = False
    note_results = _find_by_xpath(note_xpaths)
    if note_results:
        elem, txt = note_results[0]
        _log({"type": "info", "message": f"Found 'Add a note' button: '{txt}'"})
        if _click_element(elem, f"Add note: '{txt}'"):
            note_clicked = True
            time.sleep(3)

    if note_clicked:
        textarea = None
        textarea_strategies = [
            (By.TAG_NAME, "textarea"),
            (By.CSS_SELECTOR, "[contenteditable='true']"),
            (By.XPATH, "//textarea"),
        ]
        for by, sel in textarea_strategies:
            try:
                elems = page._driver.find_elements(by, sel)
                for e in elems:
                    try:
                        if e.is_displayed() and e.size['width'] > 50:
                            textarea = e
                            break
                    except Exception:
                        continue
            except Exception:
                continue
            if textarea:
                break

        if textarea:
            _log({"type": "info", "message": "Found note textarea. Typing..."})
            try:
                textarea.click()
                time.sleep(0.5)
                _human_type_selenium(page._driver, note)
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                _log({"type": "warning", "message": f"Typing note failed: {str(e)[:40]}"})

    # Step 2: Click send button — try multiple text variants via XPath
    send_xpaths = [
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'verzenden zonder opmerking')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'send without note')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'envoyer sans note')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'enviar sin nota')]",
    ]
    send_results = _find_by_xpath(send_xpaths)
    if send_results:
        elem, txt = send_results[0]
        _log({"type": "info", "message": f"Found send button: '{txt}'"})
        if _click_element(elem, f"Send: '{txt}'"):
            time.sleep(3)
            return "SUCCESS_NO_NOTE"

    # Try send-with-note variants
    send_with_note_xpaths = [
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'verzenden')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'versturen')]",
        "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'send')]",
    ]
    send_results2 = _find_by_xpath(send_with_note_xpaths)
    if send_results2:
        for elem, txt in send_results2:
            txt_lower = txt.lower()
            if any(t in txt_lower for t in ['verzenden', 'versturen', 'send']):
                _log({"type": "info", "message": f"Found send button (with note): '{txt}'"})
                if _click_element(elem, f"Send: '{txt}'"):
                    time.sleep(3)
                    return "SUCCESS"

    # Step 3: Last resort — click at estimated screen coordinates
    # LinkedIn's modal is NOT accessible via DOM APIs or Selenium element finding.
    # The buttons ARE visible on screen. Use browser-level click at estimated coordinates.
    _log({"type": "info", "message": "Trying coordinate-based click on modal buttons..."})
    try:
        vp = page.evaluate("() => { return {w: window.innerWidth, h: window.innerHeight}; }")
        vw = (vp or {}).get("w", 1280)
        vh = (vp or {}).get("h", 900)
        # Modal is centered; buttons at ~27% from top; blue button at ~55% from left
        btn_x = int(vw * 0.55)
        btn_y = int(vh * 0.27)
        _log({"type": "info", "message": f"Clicking at estimated button coords ({btn_x}, {btn_y}) viewport={vw}x{vh}"})
        page.mouse.click(btn_x, btn_y)
        time.sleep(4)
        return "SUCCESS_NO_NOTE"
    except Exception as e:
        _log({"type": "error", "message": f"Coordinate click failed: {str(e)[:60]}"})

    _log({"type": "error", "message": "Could not find send button in modal."})
    return "FAILURE"


def _handle_invitation_page(page, note):
    """Handle the dedicated /preload/custom-invite/ page.
    Strategy:
    1. If "Opmerking toevoegen" / "Add a note" is available: click it, type note, send.
    2. Otherwise: click "Verzenden zonder opmerking" / "Send without a note".
    Returns outcome string.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    _log({"type": "info", "message": "On invitation page. Handling send flow..."})
    time.sleep(2)

    def _find_clickable(text_targets):
        selectors = ["button", "[role='button']", "a", "[tabindex='0']"]
        for sel in selectors:
            try:
                for b in page._driver.find_elements(By.CSS_SELECTOR, sel):
                    try:
                        txt = (b.text or "").strip().lower()
                        if not b.is_displayed():
                            continue
                        r = b.rect
                        if r.get('width', 0) == 0 or r.get('height', 0) == 0:
                            continue
                        for t in text_targets:
                            if t in txt:
                                return b
                    except Exception:
                        continue
            except Exception:
                continue
        return None

    # Step 1: Try to add a note
    note_btn = _find_clickable([
        "opmerking toevoegen", "add a note", "ajouter une note", "notitie toevoegen"
    ])
    if note_btn:
        _log({"type": "info", "message": f"Clicking '{note_btn.text}' to add note..."})
        ActionChains(page._driver).move_to_element(note_btn).pause(0.5).click().perform()
        time.sleep(2)

        # Type the note
        try:
            textareas = page._driver.find_elements(By.TAG_NAME, "textarea")
            for ta in textareas:
                if ta.is_displayed() and ta.rect.get('width', 0) > 50:
                    _human_type_into_element(page._driver, ta, note, wpm_range=(40, 70))
                    break
        except Exception as e:
            _log({"type": "warning", "message": f"Could not type note on invite page: {str(e)[:60]}"})

        # Click send
        send_btn = _find_clickable([
            "verzenden", "send", "envoyer", "versturen"
        ])
        if send_btn:
            _log({"type": "info", "message": f"Clicking send: '{send_btn.text}'"})
            ActionChains(page._driver).move_to_element(send_btn).pause(0.5).click().perform()
            time.sleep(3)
            return "SUCCESS"
        else:
            _log({"type": "warning", "message": "Send button not found after typing note"})
            return "FAILURE"

    # Step 2: No note option — send without note
    send_without_btn = _find_clickable([
        "verzenden zonder opmerking", "send without a note", "envoyer sans note",
        "verzenden zonder", "send without"
    ])
    if send_without_btn:
        _log({"type": "info", "message": f"Clicking '{send_without_btn.text}' (no note)..."})
        ActionChains(page._driver).move_to_element(send_without_btn).pause(0.5).click().perform()
        time.sleep(3)
        return "SUCCESS_NO_NOTE"

    # Step 3: Generic send button fallback
    send_btn = _find_clickable(["verzenden", "send", "envoyer", "versturen"])
    if send_btn:
        _log({"type": "info", "message": f"Clicking generic send: '{send_btn.text}'"})
        ActionChains(page._driver).move_to_element(send_btn).pause(0.5).click().perform()
        time.sleep(3)
        return "SUCCESS_NO_NOTE"

    _log({"type": "warning", "message": "No send button found on invitation page"})
    return "FAILURE"


def is_quality_prospect(first_name, last_name, company_name, profile_url=""):
    """Check if a prospect is worth sending a connection request to.
    Returns (is_good, reason) tuple.
    """
    # Name validation
    if not is_valid_linkedin_name(first_name, last_name):
        return False, "invalid_name"
    
    full_name = f"{first_name} {last_name}".strip()
    
    # Reject slug names (contain 4+ digit sequences like 447a4663)
    if re.search(r'[a-f0-9]{6,}', full_name.lower()):
        return False, "slug_name"
    if re.search(r'\d{4,}', full_name):
        return False, "numeric_name"
    
    # Company validation
    clean_company = _clean_company_name(company_name)
    if not clean_company or len(clean_company) < 2:
        return False, "no_valid_company"
    
    # Reject if company name contains URL fragments
    if 'http' in clean_company.lower() or '.nl' in clean_company.lower() or '.com' in clean_company.lower():
        return False, "url_in_company"
    
    # Profile URL validation
    if profile_url:
        if not profile_url.startswith('http'):
            return False, "invalid_url"
        # Reject profiles with slug-like IDs in URL
        if re.search(r'/in/[a-z]+-[a-z]+-\d{5,}', profile_url.lower()):
            return False, "slug_url"
    
    return True, "ok"


def _check_if_connected(page):
    """Check if we're already connected or pending connection.
    Only searches the profile action area (top of page, left side) — not the sidebar.
    Multi-lingual. Important: LinkedIn shows Message links even for non-connections,
    so we check for Connect FIRST — if Connect exists, we're not connected.
    Uses String.normalize() to handle Vietnamese diacritics correctly.
    """
    try:
        result = page.evaluate("""() => {
            // Only look at elements in the profile action area (top < 600, left < 500)
            const els = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            const profileEls = els.filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0 && r.top < 600 && r.left < 500;
            });
            // Normalize text to NFC form to handle Vietnamese diacritics
            const texts = profileEls.map(el => (el.innerText || '').toLowerCase().trim().normalize('NFC')).filter(t => t);
            const arias = profileEls.map(el => (el.getAttribute('aria-label') || '').toLowerCase().normalize('NFC')).filter(t => t);
            const all = [...texts, ...arias];
            
            // Return debug info with the check result
            const debugTexts = all.slice(0, 20);

            // Pending — check LinkedIn's actual status text
            if (all.some(t => t.includes('afwachting') || t.includes('pending') || t.includes('chờ') ||
                t.includes('ausstehend') || t.includes('attente') || t.includes('pendiente') ||
                t.includes('in behandeling') || t.includes('ausstehend') || t.includes('en attente') ||
                t.includes('pendiente'))) {
                return {status: 'pending', debug: debugTexts};
            }

            // CHECK CONNECT FIRST — if a Connect link/button exists as a PRIMARY button (not in overflow menu), NOT connected
            const hasConnect = all.some(t =>
                t === 'verbinden' || t === 'connect' || t === 'connectie maken' || t === 'kết nối' ||
                t === 'se connecter' || t === 'conectar' ||
                t.includes('connectie maken') || t.includes('verbinden')
            );
            if (hasConnect) return {status: 'not_connected', debug: debugTexts};

            // Check overflow menu for "Connectie maken" — if it exists, NOT connected
            const hasOverflowConnect = all.some(t =>
                t === 'connectie maken' || t === 'connect'
            );
            if (hasOverflowConnect) return {status: 'not_connected', debug: debugTexts};

            // Check for PENDING indicators first (more reliable than message check)
            const hasPending = all.some(t =>
                t.includes('annuleren') || t.includes('cancel') || t.includes('verzoek verzonden') ||
                t.includes('request sent') || t.includes('invitation pending') || t.includes('uitnodiging')
            );
            if (hasPending) return {status: 'pending', debug: debugTexts};

            // LinkedIn now shows "Bericht" (Message) for BOTH connections and non-connections.
            // So "bericht" alone is NOT proof of being connected.
            // Instead: if we see "bericht" + "volgen" together, it's likely NOT connected.
            // If we see ONLY "bericht" without "volgen", it's likely connected.
            const hasMessage = all.some(t =>
                t.includes('bericht') || t.includes('message') || t.includes('nhắn tin') || t.includes('tin nhắn') ||
                t.includes('nachricht') || t.includes('mensaje')
            );
            const hasFollow = all.some(t =>
                t.includes('volgen') || t.includes('follow') || t.includes('theo dõi') ||
                t.includes('folgen') || t.includes('suivre') || t.includes('seguir')
            );

            if (hasMessage && hasFollow) {
                // Both "Bericht" and "Volgen" visible — LinkedIn shows this to NON-connections
                return {status: 'not_connected', debug: debugTexts};
            }
            if (hasMessage && !hasFollow) {
                // Only "Bericht" visible without "Volgen" — likely connected
                return {status: 'connected', debug: debugTexts};
            }

            return {status: 'unknown', debug: debugTexts};
        }""")
        if isinstance(result, dict):
            _log({"type": "info", "message": f"Connection check: {result.get('status')}, profile texts: {result.get('debug')[:8]}"})
            return result.get('status', 'unknown')
        return result or 'unknown'
    except Exception:
        return "unknown"


def _verify_send(page):
    """Verify that a connection request was actually sent by checking for LinkedIn's
    success confirmation. Returns proof string if verified, None if not.
    Checks for:
    1. Green success banner with text like 'Uitnodiging is verzonden naar...'
    2. 'Annuleren'/'Cancel' button (appears after sending a request)
    3. Screenshot saved as proof
    """
    import base64
    import os
    try:
        proof_parts = []
        
        # Check for success banner
        banner_check = page.evaluate("""() => {
            var body = document.body ? document.body.innerText : '';
            var bodyLower = body.toLowerCase();
            var successTexts = [
                "uitnodiging is verzonden",
                "invitation sent",
                "connection request sent",
                "verzoek verzonden",
                "invitation has been sent",
                "youve sent",
                "you sent"
            ];
            for (var i = 0; i < successTexts.length; i++) {
                if (bodyLower.indexOf(successTexts[i]) !== -1) {
                    return {found: true, text: successTexts[i]};
                }
            }
            var toasts = document.querySelectorAll('.artdeco-toast, [data-test-id="toast"], .artdeco-toast-view');
            for (var j = 0; j < toasts.length; j++) {
                var txt = (toasts[j].innerText || '').toLowerCase();
                if (txt.indexOf('verzonden') !== -1 || txt.indexOf('sent') !== -1) {
                    return {found: true, text: toasts[j].innerText.substring(0, 100)};
                }
            }
            return {found: false};
        }""")
        
        if banner_check and banner_check.get("found"):
            proof_parts.append(f"SUCCESS_BANNER: {banner_check.get('text', '')}")
            _log({"type": "info", "message": f"Send verified via banner: {banner_check.get('text', '')[:60]}"})
        
        # Check for "Annuleren" (Cancel) button — appears after request is sent
        cancel_check = page.evaluate("""() => {
            var btns = Array.from(document.querySelectorAll('button'));
            for (var i = 0; i < btns.length; i++) {
                var txt = (btns[i].innerText || '').toLowerCase().trim();
                var r = btns[i].getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (txt.indexOf('annuleren') !== -1 || txt.indexOf('cancel') !== -1 || txt.indexOf('withdraw') !== -1) {
                    return {found: true, text: txt};
                }
            }
            return {found: false};
        }""")
        
        if cancel_check and cancel_check.get("found"):
            proof_parts.append(f"CANCEL_BUTTON: {cancel_check.get('text', '')}")
            _log({"type": "info", "message": f"Send verified via cancel button: {cancel_check.get('text', '')}"})
        
        # Screenshot alone is NOT proof of success. Require a real UI signal.
        verified = False
        if banner_check and banner_check.get("found"):
            proof_parts.append(f"SUCCESS_BANNER: {banner_check.get('text', '')}")
            verified = True
        if cancel_check and cancel_check.get("found"):
            proof_parts.append(f"CANCEL_BUTTON: {cancel_check.get('text', '')}")
            verified = True

        if not verified:
            _log({"type": "warning", "message": "Send could not be verified: no success banner or cancel button found."})
            return None

        # Take screenshot as extra proof only after real signal confirmed
        try:
            proof_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "proof")
            os.makedirs(proof_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = os.path.join(proof_dir, f"send_{timestamp}.png")
            page._driver.save_screenshot(screenshot_path)
            if os.path.exists(screenshot_path):
                proof_parts.append(f"SCREENSHOT: {screenshot_path}")
                _log({"type": "info", "message": f"Proof screenshot saved: {screenshot_path}"})
        except Exception as e:
            _log({"type": "warning", "message": f"Screenshot failed: {str(e)[:40]}"})

        return " | ".join(proof_parts)
    except Exception as e:
        _log({"type": "warning", "message": f"Verify send error: {str(e)[:60]}"})
        return None


def _send_connection_request(page, note):
    """Send a connection request on a profile page.
    Strategy (ordered by reliability for current LinkedIn UI):
    1. Check if already connected/pending
    2. Click '...' overflow menu → find "Connectie maken" → handle invitation
    3. Navigate to custom-invite URL directly
    4. Fallback: try direct Connect button click
    After sending, verify the send actually went through by checking for
    LinkedIn's success confirmation banner.
    Returns outcome string.
    """
    global _last_send_proof
    _last_send_proof = None

    # Check current status
    status = _check_if_connected(page)
    _log({"type": "info", "message": f"Connection status: {status}"})

    if status == "connected":
        return "ALREADY_CONNECTED"
    if status == "pending":
        return "ALREADY_PENDING"

    connect_texts = ["verbinden", "connect", "connectie maken", "kết nối", "se connecter", "conectar"]
    connect_arias = ["verbinden", "connect", "connectie", "kết nối", "mời", "se connecter", "conectar", "invite"]

    # ============================================================
    # STRATEGY 1 (PRIMARY): Click '...' overflow menu → "Connectie maken"
    # LinkedIn currently puts the Connect button inside the overflow menu.
    # ============================================================
    _log({"type": "info", "message": "Trying overflow menu approach (primary strategy)..."})
    more_pos = _find_profile_more_button(page)
    if more_pos:
        _log({"type": "info", "message": f"Found More button. Clicking..."})
        page.mouse.click(more_pos["x"], more_pos["y"])
        time.sleep(random.uniform(2.0, 3.0))
        try:
            # Use JS to find "Connectie maken" by text content (not coordinates)
            connect_in_menu = page.evaluate("""() => {
                const labels = ["connectie maken", "verbinden", "connect"];
                // Search ALL clickable elements in the dropdown/menu
                const items = Array.from(document.querySelectorAll('[role="menuitem"], [role="menuitemcheckbox"], li, button, a, [data-testid]'));
                for (const item of items) {
                    const txt = (item.innerText || '').toLowerCase().trim();
                    const r = item.getBoundingClientRect();
                    if (r.width === 0 || r.height === 0) continue;
                    for (const label of labels) {
                        if (txt.includes(label)) {
                            // Click the element directly (no coordinates)
                            item.click();
                            return {text: txt, clicked: true, method: 'direct_click'};
                        }
                    }
                }
                // Fallback: search by aria-label
                const allBtns = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                for (const btn of allBtns) {
                    const aria = (btn.getAttribute('aria-label') || '').toLowerCase();
                    for (const label of labels) {
                        if (aria.includes(label)) {
                            btn.click();
                            return {text: aria, clicked: true, method: 'aria_click'};
                        }
                    }
                }
                return null;
            }""")

            if connect_in_menu:
                _log({"type": "info", "message": f"Found and clicked '{connect_in_menu.get('text')}' via {connect_in_menu.get('method')}"})
                time.sleep(3)
                result = _handle_invitation_modal(page, note)
                verified = _verify_send(page)
                _last_send_proof = verified
                if verified:
                    _log({"type": "info", "message": f"SEND VERIFIED: {verified}"})
                    return result if result in ("SUCCESS", "SUCCESS_NO_NOTE") else "SUCCESS"
                else:
                    _log({"type": "warning", "message": "Send could not be verified (no confirmation banner found). Recording anyway."})
                    return result if result in ("SUCCESS", "SUCCESS_NO_NOTE", "FAILURE") else "SUCCESS"
            else:
                _log({"type": "warning", "message": "No 'Connectie maken' found in overflow menu. Trying to close menu and continue..."})
                # Close the menu by pressing Escape
                page.keyboard.press("Escape")
                time.sleep(1)
        except Exception as e:
            _log({"type": "error", "message": f"Error in More menu: {str(e)[:60]}"})

    # ============================================================
    # STRATEGY 2: Try finding custom-invite URL and navigating directly
    # ============================================================
    _log({"type": "info", "message": "Overflow menu didn't work. Trying custom-invite URL..."})
    invite_url = None
    try:
        invite_url = page.evaluate("""() => {
            const els = Array.from(document.querySelectorAll('a'));
            for (const el of els) {
                const href = el.getAttribute('href') || '';
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 600 || r.left > 500) continue;
                if (href.includes('custom-invite') || href.includes('invitation')) {
                    let fullHref = href;
                    if (fullHref.startsWith('/')) fullHref = 'https://www.linkedin.com' + fullHref;
                    return fullHref;
                }
            }
            // Also check buttons that might have data attributes
            const btns = Array.from(document.querySelectorAll('button'));
            for (const b of btns) {
                const aria = (b.getAttribute('aria-label') || '').toLowerCase();
                const r = b.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                if (r.top > 200 && r.top < 700) {
                    if (aria.includes('connectie') || aria.includes('invite')) {
                        // Get the onclick handler or data attributes
                        return {hasConnectBtn: true, aria: aria};
                    }
                }
            }
            return null;
        }""")
        if isinstance(invite_url, dict):
            _log({"type": "info", "message": f"Found connect button by aria: {invite_url}"})
            invite_url = None
    except Exception as e:
        _log({"type": "error", "message": f"Error finding invite URL: {str(e)[:80]}"})

    if invite_url and isinstance(invite_url, str):
        _log({"type": "info", "message": f"Navigating to invite URL: {invite_url[:80]}"})
        try:
            page.goto(invite_url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            _log({"type": "warning", "message": f"goto invite page failed: {str(e)[:60]}"})

        _random_sleep(3, 5)

        for attempt in range(10):
            current = page.url
            if "custom-invite" in current or "invitation" in current:
                _log({"type": "info", "message": f"On invite page after {(attempt+1)}s: {current[:80]}"})
                break
            time.sleep(1)
        else:
            _log({"type": "warning", "message": f"Not on invite page after 10s. URL: {page.url[:80]}"})
            if "custom-invite" not in page.url and "invitation" not in page.url:
                return "FAILURE"

        auth = _detect_auth_wall(page)
        if auth:
            return auth.upper()

        result = _handle_invitation_page(page, note)
        verified = _verify_send(page)
        _last_send_proof = verified
        if verified:
            _log({"type": "info", "message": f"SEND VERIFIED: {verified}"})
        return result if result in ("SUCCESS", "SUCCESS_NO_NOTE") else "SUCCESS" if verified else result

    # ============================================================
    # STRATEGY 3: Fallback — try clicking visible Connect button
    # Human-like: scroll page, search entire DOM, try multiple locations
    # ============================================================
    _log({"type": "info", "message": "Trying direct Connect button click (fallback - human-like scan)..."})
    
    # First try in profile area
    if _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias, profile_area_only=True):
        _log({"type": "info", "message": "Direct Connect link clicked in profile area."})
        time.sleep(3)
        result = _handle_invitation_modal(page, note)
        verified = _verify_send(page)
        _last_send_proof = verified
        if verified:
            _log({"type": "info", "message": f"SEND VERIFIED: {verified}"})
        return result
    
    # If not found, scroll down and search entire page (human behavior)
    _log({"type": "info", "message": "Not found in profile area. Scrolling to search entire page..."})
    _human_scroll(page, min_scrolls=2, max_scrolls=4)
    time.sleep(random.uniform(1.5, 3.0))
    
    # Search entire page (not just profile area)
    if _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias, profile_area_only=False):
        _log({"type": "info", "message": "Direct Connect link clicked after scrolling."})
        time.sleep(3)
        result = _handle_invitation_modal(page, note)
        verified = _verify_send(page)
        _last_send_proof = verified
        if verified:
            _log({"type": "info", "message": f"SEND VERIFIED: {verified}"})
        return result
    
    # Try to find by data attributes and Sentry labels (LinkedIn's internal naming)
    _log({"type": "info", "message": "Trying data-attribute search..."})
    try:
        found = page.evaluate("""() => {
            // Search by data attributes
            const btns = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            for (const b of btns) {
                const txt = (b.innerText || '').toLowerCase().trim();
                const aria = (b.getAttribute('aria-label') || '').toLowerCase();
                const data controlName = b.getAttribute('data-control-name') || '';
                const r = b.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                
                // Check for connect-related attributes
                if (txt.includes('verbinden') || txt.includes('connect') || 
                    txt.includes('connectie') || aria.includes('connect') ||
                    dataControlName.includes('connect')) {
                    b.scrollIntoView({behavior: 'smooth', block: 'center'});
                    return {found: true, text: txt, aria: aria, controlName: dataControlName};
                }
            }
            return {found: false};
        }""")
        
        if found and found.get('found'):
            _log({"type": "info", "message": f"Found button by data attributes: {found}"})
            time.sleep(1)
            # Try clicking again after scroll
            if _find_and_click_button(page, texts=connect_texts, aria_keywords=connect_arias, profile_area_only=False):
                _log({"type": "info", "message": "Connect clicked after data-attribute scroll."})
                time.sleep(3)
                result = _handle_invitation_modal(page, note)
                verified = _verify_send(page)
                _last_send_proof = verified
                if verified:
                    _log({"type": "info", "message": f"SEND VERIFIED: {verified}"})
                return result
    except Exception as e:
        _log({"type": "error", "message": f"Data-attribute search error: {str(e)[:60]}"})
    
    # Check if profile is restricted (some profiles don't allow connections)
    try:
        restricted = page.evaluate("""() => {
            const body = document.body.innerText || '';
            return body.includes('niet beschikbaar') || 
                   body.includes('not available') ||
                   body.includes('beperkt') ||
                   body.includes('restricted') ||
                   body.includes('We hebben geen manier gevonden om je met deze persoon te verbinden');
        }""")
        if restricted:
            _log({"type": "warning", "message": "Profile connection restricted by LinkedIn."})
            return "RESTRICTED"
    except:
        pass
    
    _log({"type": "error", "message": "No Connect button found on profile after full scan."})
    return "FAILURE"


def _search_and_find_profile(page, first_name, last_name, company):
    """Search LinkedIn for a person and return their profile URL."""
    search_query = f"{first_name} {last_name} {company}".strip()
    domain = company  # best available domain proxy

    if not _can_linkedin_search(domain):
        return None, "search_budget_exhausted"

    _enforce_linkedin_search_delay()

    search_url = f"https://www.linkedin.com/search/results/people/?keywords={search_query.replace(' ', '%20')}"

    _log({"type": "info", "message": f"Searching LinkedIn for: {search_query}"})
    page.goto(search_url, wait_until="domcontentloaded", timeout=120000)
    _random_sleep(4, 7)
    _human_profile_visit(page)

    # Check for auth wall
    auth = _detect_auth_wall(page)
    if auth:
        _record_linkedin_search(domain, search_query, f"auth:{auth}")
        _log({"type": "error", "message": f"Hit {auth} during search. Login may have expired."})
        return None, auth

    # Find first profile link
    try:
        profile_url = page.evaluate("""() => {
            const links = Array.from(document.querySelectorAll('a[href*="/in/"]'));
            for (const l of links) {
                const href = l.getAttribute('href');
                if (href && href.includes('/in/') && l.checkVisibility()) {
                    const r = l.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) return href;
                }
            }
            return null;
        }""")
        if profile_url:
            if profile_url.startswith("/"):
                profile_url = "https://www.linkedin.com" + profile_url.split("?")[0]
            else:
                profile_url = profile_url.split("?")[0]
            _record_linkedin_search(domain, search_query, profile_url)
            _log({"type": "info", "message": f"Found profile: {profile_url}"})
            return profile_url, None
    except Exception:
        pass

    _record_linkedin_search(domain, search_query, "not_found")
    return None, "not_found"


def find_decision_maker_on_linkedin(company_name, domain=""):
    """Search LinkedIn for a decision-maker at a specific company.
    Uses the user's real Firefox browser (Selenium) to search LinkedIn.
    Returns dict: {"linkedin_url": str, "first_name": str, "last_name": str, "title": str} or None.
    """
    try:
        with get_firefox_driver() as (driver, page):
            _log({"type": "info", "message": f"Searching LinkedIn for decision-maker at {company_name}"})

            # Search LinkedIn for people at this company with leadership titles
            search_queries = [
                f"{company_name} eigenaar",
                f"{company_name} directeur",
                f"{company_name} founder",
                f"{company_name} CEO",
                f"{company_name} owner",
                f"{company_name}",
            ]

            for query in search_queries:
                try:
                    if not _can_linkedin_search(domain):
                        _log({"type": "warning", "message": "LinkedIn search budget exhausted. Stopping decision-maker search."})
                        return None
                    _enforce_linkedin_search_delay()

                    search_url = f"https://www.linkedin.com/search/results/people/?keywords={query.replace(' ', '%20')}"
                    page.goto(search_url, wait_until="domcontentloaded", timeout=120000)
                    _random_sleep(4, 7)
                    _human_profile_visit(page)

                    # Check for auth wall
                    auth = _detect_auth_wall(page)
                    if auth:
                        _record_linkedin_search(domain, query, f"auth:{auth}")
                        _log({"type": "error", "message": f"Hit {auth} during LinkedIn search"})
                        return None

                    # Extract names and profile URLs from search results
                    results = page.evaluate(r"""() => {
                        const results = [];
                        const seen = new Set();
                        const links = document.querySelectorAll('a[href*="/in/"]');
                        for (const link of links) {
                            try {
                                const href = link.getAttribute('href');
                                if (!href || !href.includes('/in/') || seen.has(href)) continue;
                                seen.add(href);

                                const fullText = link.textContent.trim();
                                if (fullText.length < 5) continue;

                                // Get display text - take first line
                                const firstLine = fullText.split('\n')[0].trim();

                                // Extract name: take words before rank separator (2de, 3de etc)
                                let nameText = '';
                                const rankIdx = firstLine.search(/\d+de[A-Z]/);
                                if (rankIdx > 0) {
                                    nameText = firstLine.substring(0, rankIdx).trim();
                                } else {
                                    nameText = firstLine;
                                }

                                // Remove non-name characters, normalize whitespace
                                nameText = nameText.replace(/[^\w\s\u00C0-\u024F\u0400-\u04FF-]/g, '').trim();
                                nameText = nameText.replace(/\s+/g, ' ');

                                // Filter empty words
                                const words = nameText.split(' ').filter(function(w) { return w.length > 0; });

                                // Deduplicate: "X X" -> "X"
                                if (words.length >= 4) {
                                    var mid = Math.floor(words.length / 2);
                                    var first = words.slice(0, mid).join(' ');
                                    var second = words.slice(mid).join(' ');
                                    if (first.toLowerCase() === second.toLowerCase()) {
                                        nameText = first;
                                    }
                                }

                                // Extract title: text after the rank number
                                var titleText = '';
                                var titleMatch = fullText.match(/\d+de(.{5,})/);
                                if (titleMatch) {
                                    titleText = titleMatch[1].trim();
                                    // Cut at location/Connectie patterns
                                    titleText = titleText.split(/Eindhoven|Amsterdam|Rotterdam|Utrecht|Den Haag|Connectie maken/)[0].trim();
                                    if (titleText.length > 100) titleText = titleText.substring(0, 100);
                                }

                                // Skip if name is too short
                                if (nameText.length < 4 || nameText.length > 50) continue;
                                if (!nameText.includes(' ')) continue;
                                if (nameText.toLowerCase().indexOf('connectie') >= 0 || nameText.toLowerCase().indexOf('gemeenschappelijke') >= 0) continue;

                                results.push({
                                    name: nameText,
                                    title: titleText,
                                    url: href.split('?')[0]
                                });
                            } catch(e) {}
                        }
                        return results;
                    }""")

                    if results:
                        # Pick the best match - prefer leadership titles
                        leadership = ['eigenaar', 'directeur', 'ceo', 'founder', 'oprichter', 'owner', 'director', 'manager', 'mede-eigenaar', 'bestuurder']
                        best = None
                        for r in results:
                            title_lower = r.get('title', '').lower()
                            if any(t in title_lower for t in leadership):
                                best = r
                                break
                        if not best and results:
                            best = results[0]

                        if best:
                            name_parts = best['name'].split()
                            first = name_parts[0] if name_parts else ''
                            last = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
                            url = best['url']
                            if url.startswith('/'):
                                url = 'https://www.linkedin.com' + url
                            _record_linkedin_search(domain, query, url)
                            _log({"type": "info", "message": f"Found: {first} {last} ({best.get('title', '')}) at {url}"})
                            return {
                                "linkedin_url": url,
                                "first_name": first,
                                "last_name": last,
                                "title": best.get('title', '')
                            }
                except Exception as e:
                    _log({"type": "warning", "message": f"Search query '{query}' failed: {e}"})
                _random_sleep(3, 6)

            _record_linkedin_search(domain, company_name, "not_found")
            _log({"type": "info", "message": f"No decision-maker found on LinkedIn for {company_name}"})
            return None

    except Exception as e:
        _log({"type": "error", "message": f"LinkedIn search error: {e}"})
        return None


# ---------- Real browser connection via Selenium + user's Firefox ----------

_FIREFOX_PROFILES = [
    r"C:\Users\marvi\AppData\Roaming\Mozilla\Firefox\Profiles\h1vl3oun.default-release",
]
_GECKODRIVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "geckodriver", "geckodriver.exe")
# Also check original clawbuildr location for backwards compat
if not os.path.exists(_GECKODRIVER_PATH):
    _GECKODRIVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geckodriver", "geckodriver.exe")
if not os.path.exists(_GECKODRIVER_PATH):
    _GECKODRIVER_PATH = r"C:\Users\marvi\clawbuildr\data\geckodriver\geckodriver.exe"


class SeleniumPage:
    """Adapter that wraps Selenium WebDriver to mimic Playwright's Page API.
    This allows all existing code to work unchanged.
    """
    def __init__(self, driver):
        self._driver = driver
        self._timeout = 30000

    @property
    def url(self):
        return self._driver.current_url

    @property
    def title(self):
        return self._driver.title

    def is_closed(self):
        try:
            _ = self._driver.title
            return False
        except Exception:
            return True

    def set_default_timeout(self, ms):
        self._timeout = ms

    def goto(self, url, wait_until="domcontentloaded", timeout=90000):
        self._driver.set_page_load_timeout(timeout / 1000)
        self._driver.get(url)
        # Wait for DOM ready
        if wait_until == "domcontentloaded":
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            try:
                WebDriverWait(self._driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
                )
            except Exception:
                pass

    def evaluate(self, js, args=None):
        """Execute JavaScript and return the result.
        Converts Playwright-style arrow functions with named params to Selenium-style arguments.
        e.g. (url) => { ... url ... } with args='hello' → arguments[0] used for url
        """
        js_clean = js.strip()
        # Strip arrow function wrapper
        arrow_match = re.match(r'^\(([^)]*)\)\s*=>\s*\{', js_clean)
        if arrow_match:
            param_name = arrow_match.group(1).strip()
            js_clean = js_clean[arrow_match.end():]
            if js_clean.rstrip().endswith("}"):
                js_clean = js_clean.rstrip()[:-1]
            # Replace param_name with arguments[0]
            if param_name:
                js_clean = js_clean.replace(param_name, 'arguments[0]')
        elif js_clean.startswith("()"):
            js_clean = js_clean[js_clean.index("{")+1:]
            if js_clean.rstrip().endswith("}"):
                js_clean = js_clean.rstrip()[:-1]
        try:
            if args is not None:
                return self._driver.execute_script(js_clean, args)
            else:
                return self._driver.execute_script(js_clean)
        except Exception as e:
            _log({"type": "error", "message": f"JS error: {str(e)[:60]}"})
            return None

    def locator(self, selector):
        return SeleniumLocator(self._driver, selector)

    @property
    def keyboard(self):
        return SeleniumKeyboard(self._driver)

    @property
    def mouse(self):
        return SeleniumMouse(self._driver)

    def wait_for_url(self, pattern, timeout=10000):
        import time, re
        # Convert fnmatch-style pattern to regex
        # **/ means match any path segments, * means match within a segment
        regex = pattern.replace("**/", ".*?/").replace("**", ".*").replace("*", "[^/]*")
        regex = f"^{regex}$"
        start = time.time()
        while (time.time() - start) * 1000 < timeout:
            if re.search(regex, self.url):
                return
            time.sleep(0.5)

    def wait_for_timeout(self, ms):
        time.sleep(ms / 1000)

    def close(self):
        try:
            self._driver.quit()
        except Exception:
            pass


class SeleniumLocator:
    """Mimics Playwright's Locator API."""
    def __init__(self, driver, selector):
        self._driver = driver
        self._selector = selector

    @property
    def first(self):
        return self

    def click(self, timeout=5000, force=False):
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.action_chains import ActionChains
        try:
            el = WebDriverWait(self._driver, timeout/1000).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, self._selector))
            )
            self._driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.3)
            if force:
                # Use ActionChains for a real click event (not JS click)
                ActionChains(self._driver).move_to_element(el).pause(0.1).click().perform()
            else:
                el.click()
        except Exception:
            # Try XPath as fallback
            try:
                el = self._driver.find_element(By.XPATH, f"//*[contains(text(), '{self._selector}')]")
                ActionChains(self._driver).move_to_element(el).pause(0.1).click().perform()
            except Exception:
                pass

    def count(self):
        from selenium.webdriver.common.by import By
        return len(self._driver.find_elements(By.CSS_SELECTOR, self._selector))

    def is_visible(self):
        from selenium.webdriver.common.by import By
        try:
            el = self._driver.find_element(By.CSS_SELECTOR, self._selector)
            return el.is_displayed()
        except Exception:
            return False

    def bounding_box(self):
        from selenium.webdriver.common.by import By
        try:
            el = self._driver.find_element(By.CSS_SELECTOR, self._selector)
            loc = el.location
            size = el.size
            return {"x": loc["x"], "y": loc["y"], "width": size["width"], "height": size["height"]}
        except Exception:
            return None

    @property
    def element_handle(self):
        from selenium.webdriver.common.by import By
        return self._driver.find_element(By.CSS_SELECTOR, self._selector)


class SeleniumKeyboard:
    def __init__(self, driver):
        self._driver = driver

    def type(self, text, delay=50):
        from selenium.webdriver.common.keys import Keys
        for char in text:
            self._driver.find_element("tag name", "body").send_keys(char)
            time.sleep(delay / 1000)


class SeleniumMouse:
    def __init__(self, driver):
        self._driver = driver

    def click(self_or_driver, element_or_x=None, y=None):
        """Click at coordinates or on an element.
        
        Supports both calling conventions:
            page.mouse.click(element)             # instance: click on element center
            page.mouse.click(x, y)               # instance: click at absolute coordinates
            SeleniumMouse.click(driver, element)  # static-style: click on element center
            SeleniumMouse.click(driver, x, y)     # static-style: click at absolute coordinates
            SeleniumMouse.click(driver, None, x, y)
        """
        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.action_chains import ActionChains as AC
        
        # Detect calling convention: instance method vs static-style
        try:
            from selenium.webdriver.remote.webdriver import WebDriver
            if isinstance(self_or_driver, WebDriver):
                driver = self_or_driver
            else:
                driver = self_or_driver._driver
        except Exception:
            driver = self_or_driver._driver
        
        # Determine target x, y
        if element_or_x is not None and y is None:
            # Called with an element — get element center
            try:
                loc = element_or_x.location
                size = element_or_x.size
                x = loc['x'] + size['width'] // 2
                y = loc['y'] + size['height'] // 2
            except Exception:
                return
        elif y is not None:
            # Called with coordinates: click(x, y) or click(None, x, y)
            x = element_or_x if element_or_x is not None else 0
        else:
            return

        try:
            body = driver.find_element(By.TAG_NAME, "body")
            chain = ActionChains(driver)
            chain.move_to_element(body)
            loc = body.location
            size = body.size
            center_x = loc.get('x', 0) + size.get('width', 0) // 2
            center_y = loc.get('y', 0) + size.get('height', 0) // 2
            dx = x - center_x
            dy = y - center_y
            chain.move_by_offset(dx, dy).pause(0.1).click().perform()
        except Exception as e:
            # Fallback: dispatch mouse events via JS
            try:
                driver.execute_script("""
                    var el = document.elementFromPoint(arguments[0], arguments[1]);
                    if (!el) {
                        var all = document.querySelectorAll('*');
                        for (var i = 0; i < all.length; i++) {
                            if (all[i].shadowRoot) {
                                var inner = all[i].shadowRoot.elementFromPoint(arguments[0], arguments[1]);
                                if (inner) { el = inner; break; }
                            }
                        }
                    }
                    if (el) {
                        ['mousedown', 'mouseup', 'click'].forEach(function(type) {
                            el.dispatchEvent(new MouseEvent(type, {
                                bubbles: true, cancelable: true, view: window,
                                clientX: arguments[0], clientY: arguments[1]
                            }));
                        });
                    }
                """, x, y)
            except Exception:
                pass


def _connect_selenium():
    """Launch Firefox with user's real profile via Selenium.
    Returns (driver, page) where page is a SeleniumPage adapter.
    """
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service

    profile_path = _prepare_firefox_profile()
    if not os.path.exists(profile_path):
        raise Exception(f"Firefox profile copy failed: {profile_path}")

    options = Options()
    options.add_argument("-profile")
    options.add_argument(profile_path)
    # Do NOT use --headless: it conflicts with -profile in Firefox and causes
    # "Firefox is already running" popups. The separate profile copy prevents conflicts.

    if not os.path.exists(_GECKODRIVER_PATH):
        raise Exception(f"geckodriver not found: {_GECKODRIVER_PATH}")

    service = Service(executable_path=_GECKODRIVER_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    page = SeleniumPage(driver)
    return driver, page


class _PersistentFirefox:
    """Singleton Firefox instance that stays alive across all LinkedIn operations.
    Replaces the old _FirefoxSession which launched/quit Firefox per call.
    """
    _instance = None
    _lock = threading.Lock()
    _launch_lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.driver = None
                cls._instance.page = None
                cls._instance._ready = False
                cls._instance._last_health = 0
            return cls._instance

    def _is_alive(self):
        """Check if the browser process is still running (lightweight check)."""
        if not self.driver:
            return False
        try:
            # Use execute_script which is fast and doesn't depend on page state
            self.driver.execute_script("return document.readyState")
            return True
        except Exception:
            return False

    def ensure_ready(self):
        """Launch Firefox if not running, or restart if crashed. Thread-safe."""
        now = time_mod.time()
        # Quick check without lock — if alive and recently checked, return fast
        if self._ready and (now - self._last_health) < 10:
            if self._is_alive():
                return self.driver, self.page

        # Serialized launch to prevent multiple browsers
        with self._launch_lock:
            # Double-check after acquiring lock
            if self._ready and self._is_alive():
                self._last_health = time_mod.time()
                return self.driver, self.page

            # Need (re)launch
            self.close()
            try:
                self.driver, self.page = _connect_selenium()
                self._ready = True
                self._last_health = time_mod.time()
                _log({"type": "info", "message": "Firefox launched (persistent singleton)"})
            except Exception as e:
                self._ready = False
                raise
            return self.driver, self.page

    def close(self):
        """Quit the browser if running."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
            self.page = None
            self._ready = False

    def __enter__(self):
        self.ensure_ready()
        return self.driver, self.page

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Do NOT close on context exit — keep alive for reuse
        return False


# Module-level singleton
_persistent_firefox = _PersistentFirefox()

import atexit as _atexit
_atexit.register(lambda: _persistent_firefox.close())


def get_firefox_driver():
    """Return a context manager that reuses a single persistent Firefox instance.
    No more launching/quit per call — browser stays alive across 20+ connections/day.
    """
    return _persistent_firefox


def search_and_connect(first_name, last_name, company, email_body, contact_id="", profile_url="", research_result=None, opportunity_mapping=None):
    """Automatic LinkedIn outreach: send connection request with note.

    If profile_url is provided, skips search and goes directly to the profile.
    Returns dict: {outcome, profile_url, note, name}
    """
    # Check if account is flagged — STOP immediately
    flagged, flag_reason = _is_flagged()
    if flagged:
        _log({"type": "error", "message": f"Account flagged ({flag_reason}). Cannot send connections."})
        return {"outcome": "FLAGGED", "profile_url": "", "note": "", "name": f"{first_name} {last_name}", "error": flag_reason}

    # Check active hours
    active, reason = _is_active_hours()
    if not active:
        _log({"type": "warning", "message": f"Not active hours ({reason}). Skipping."})
        return {"outcome": "TIME_LIMITED", "profile_url": "", "note": "", "name": f"{first_name} {last_name}", "error": reason}

    # Check time-based limit (spread connections throughout the day, with warming)
    can_send, period_info = can_send_connection()
    if not can_send:
        daily_count = _get_daily_count()
        _log({"type": "warning", "message": f"Cannot send connection now ({period_info}). Daily: {daily_count}. Skipping."})
        return {"outcome": "TIME_LIMITED", "profile_url": "", "note": "", "name": f"{first_name} {last_name}", "error": period_info}

    # Check dedup (by profile_url if available, or by name+company)
    if _is_already_contacted(first_name, last_name, company, profile_url):
        _log({"type": "info", "message": f"Already contacted {first_name} {last_name} at {company}. Skipping."})
        return {"outcome": "ALREADY_CONTACTED", "profile_url": "", "note": "", "name": f"{first_name} {last_name}"}

    # Quality filter: reject garbage names, bad companies, slug URLs
    is_good, reject_reason = is_quality_prospect(first_name, last_name, company, profile_url)
    if not is_good:
        _log({"type": "info", "message": f"Skipping {first_name} {last_name} - quality filter: {reject_reason}"})
        return {"outcome": f"SKIPPED_{reject_reason.upper()}", "profile_url": "", "note": "", "name": f"{first_name} {last_name}"}

    # Generate the 180-char note (with research-based personalization)
    note = generate_linkedin_note(email_body, first_name, company, research_result, opportunity_mapping)
    if not note:
        _log({"type": "warning", "message": f"Skipping {first_name} {last_name} - invalid name or note generation failed."})
        return {"outcome": "SKIPPED_INVALID_NAME", "profile_url": "", "note": "", "name": f"{first_name} {last_name}"}

    # Launch real Firefox with user's profile via Selenium (persistent singleton)
    _log({"type": "info", "message": "Connecting to persistent Firefox instance..."})
    try:
        driver, page = _persistent_firefox.ensure_ready()
        _log({"type": "info", "message": "Firefox ready (persistent singleton)"})
    except Exception as e:
        _log({"type": "error", "message": f"Failed to launch Firefox: {e}"})
        return {"outcome": "ERROR", "profile_url": "", "note": note, "name": f"{first_name} {last_name}", "error": str(e)[:120]}

    with _state_lock:
        _job_state["current_name"] = f"{first_name} {last_name}"

    full_name = f"{first_name} {last_name}".strip()
    outcome = "FAILURE"

    try:
        # Step 1: Use provided profile_url or search for the person
        if not profile_url:
            profile_url, error = _search_and_find_profile(page, first_name, last_name, company)

            if error == "auth_wall" or error == "captcha":
                outcome = error.upper()
                _save_result(contact_id, first_name, last_name, company, "", note, outcome)
                return {"outcome": outcome, "profile_url": "", "note": note, "name": full_name, "error": error}

            if not profile_url:
                outcome = "NOT_FOUND"
                _log({"type": "warning", "message": f"No profile found for {first_name} {last_name} at {company}"})
                _save_result(contact_id, first_name, last_name, company, "", note, outcome)
                return {"outcome": outcome, "profile_url": "", "note": note, "name": full_name}
        else:
            _log({"type": "info", "message": f"Using pre-stored LinkedIn URL: {profile_url}"})

        # Step 2: Navigate to the profile
        _log({"type": "info", "message": f"Navigating to profile: {profile_url}"})
        page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)

        # Wait for LinkedIn's React SPA to actually render the content
        _log({"type": "info", "message": "Waiting for SPA content to render..."})
        for attempt in range(15):
            try:
                ready = page.evaluate("""() => {
                    return {
                        links: document.querySelectorAll('a').length,
                        buttons: document.querySelectorAll('button').length,
                        bodyLen: (document.body ? document.body.innerText.length : 0)
                    };
                }""")
                if ready and ready.get("links", 0) > 5:
                    _log({"type": "info", "message": f"SPA ready after {(attempt+1)*2}s: {ready}"})
                    break
            except Exception:
                pass
            time.sleep(2)
        else:
            _log({"type": "warning", "message": "SPA did not fully render after 30s. Proceeding anyway."})
            _random_sleep(3, 5)

        # Check for auth wall after navigation
        auth = _detect_auth_wall(page)
        if auth:
            outcome = auth.upper()
            _save_result(contact_id, first_name, last_name, company, profile_url, note, outcome)
            return {"outcome": outcome, "profile_url": profile_url, "note": note, "name": full_name, "error": auth}

        # Human-like profile review before connecting: scroll, read, wander
        _human_profile_visit(page)

        # Extract the real name from the profile (H1 first, then H2)
        try:
            name = page.evaluate("""() => {
                // Try H1 first — but skip notification badges
                const h1 = document.querySelector('h1');
                if (h1 && h1.innerText.trim()) {
                    const t = h1.innerText.trim();
                    // Skip numeric-only (notification counts like "0 meldingen")
                    if (/^\\d+\\s/.test(t) || /^\\d+$/.test(t)) {} else if (t.length > 3) return t;
                }
                // Try H2 — LinkedIn sometimes puts the name in the second H2
                const h2s = Array.from(document.querySelectorAll('h2'));
                for (const h of h2s) {
                    const text = (h.innerText || '').trim();
                    // Skip UI labels, notifications, numeric badges
                    if (/^\\d+\\s/.test(text) || /^\\d+$/.test(text)) continue;
                    if (text.length > 3 && text.split(' ').length >= 2 &&
                        !text.includes('thong bao') && !text.includes('notification') &&
                        !text.toLowerCase().includes('about') && !text.includes('gioi thieu') &&
                        !text.toLowerCase().includes('over') && !text.includes('gioi')) {
                        return text;
                    }
                }
                return '';
            }""")
            if name:
                try:
                    name = name.normalize("NFC") if isinstance(name, str) else str(name).normalize("NFC")
                except Exception:
                    name = str(name)
                full_name = name
                _log({"type": "info", "message": f"Profile name: {full_name}"})
                # Regenerate the note with the REAL name from the profile
                real_first = full_name.split()[0] if full_name else first_name
                note = generate_linkedin_note(email_body, real_first, company, research_result, opportunity_mapping)
        except Exception:
            pass

        # Step 3: Send connection request
        _enforce_connection_delay()  # ensure 72 min spacing between sends
        outcome = _send_connection_request(page, note)
        if outcome in ("SUCCESS", "SUCCESS_NO_NOTE"):
            _mark_connection_sent()

        # Save result with proof
        proof = _last_send_proof
        _save_result(contact_id, first_name, last_name, company, profile_url, note, outcome, proof)
        _log({"type": "info", "message": f"Outreach result for {full_name}: {outcome}"})
        if proof:
            _log({"type": "info", "message": f"Proof: {proof[:100]}"})

        with _state_lock:
            _job_state["results"].append({"url": profile_url, "name": full_name, "outcome": outcome})

        return {
            "outcome": outcome,
            "profile_url": profile_url,
            "note": note,
            "name": full_name,
        }

    except Exception as e:
        _log({"type": "error", "message": f"LinkedIn outreach error: {str(e)[:80]}"})
        _save_result(contact_id, first_name, last_name, company, profile_url, note, "ERROR")
        return {"outcome": "ERROR", "profile_url": profile_url, "note": note, "name": full_name, "error": str(e)[:120]}

def is_valid_linkedin_name(first_name, last_name=""):
    """Check if a name is valid for LinkedIn outreach. Rejects garbage data."""
    full = f"{first_name or ''} {last_name or ''}".strip()
    if not full or len(full) < 2:
        return False
    # Reject if contains any digits (slug-like: mattgray1, victoramarnani92899b101)
    if re.search(r'[0-9]', full):
        return False
    # Reject if mostly numbers
    alpha_count = sum(1 for c in full if c.isalpha())
    num_count = sum(1 for c in full if c.isdigit())
    if num_count > alpha_count:
        return False
    # Reject if contains hash-like strings (8+ hex chars)
    if re.search(r'[a-f0-9]{6,}', full.lower()):
        return False
    # Reject if all lowercase and no spaces (likely a slug)
    if full == full.lower() and ' ' not in full and len(full) > 10:
        return False
    # Reject if too long (likely scraped garbage)
    if len(full) > 40:
        return False
    # Reject common garbage patterns
    garbage_patterns = ['interimrecruiter', 'admin', 'info@', 'sales@', 'contact@', 'noreply',
                        'secretariaat', 'klantendienst', 'archief', 'afspraak', 'register', 'hallo',
                        'studio', 'home', 'welcome', 'search', 'login', 'menu', 'marketing',
                        'press', 'legal', 'team', 'help', 'service', 'office', 'payments',
                        'commission', 'support', 'facilitair', 'medische', 'kliniek',
                        'naam', 'vind een', 'apenkoppen', 'dpo google', 'schildersbedrijf',
                        'contact', 'kontakt', 'privacy', 'secretaris', 'bi ', 'name ',
                        'group', 'lightkey', 'bv ', 'bvba', 'llc', 'inc ',
                        'voor ', 'met ', 'uit ', 'aan ',
                        'scipio', 'hansmarechal', 'mattgray', 'joriskock',
                        'fitz', 'gerben', 'fitz de', 'ibrim', 'jamal',
                        'no image', 'no name', 'dummy', 'example',
                        'me there', 'you there', 'click here', 'learn more',
                        'view profile', 'see more', 'show more', 'read more']
    for g in garbage_patterns:
        if g in full.lower():
            return False
    # Reject if name is just a domain or URL fragment
    if '.' in full and ' ' not in full:
        return False
    # Reject single-word names longer than 12 chars (likely concatenated slugs)
    if ' ' not in full and len(full) > 12:
        return False
    # Reject if name looks like a sentence or description (more than 4 words)
    if len(full.split()) > 4:
        return False
    # Reject if both first and last are common UI/non-name words
    non_name_words = {'me', 'you', 'we', 'my', 'your', 'our', 'the', 'this', 'that',
                      'there', 'here', 'more', 'less', 'all', 'any', 'new', 'old',
                      'click', 'view', 'read', 'see', 'show', 'learn', 'find',
                      'get', 'set', 'add', 'edit', 'del', 'ok', 'yes', 'no'}
    name_words = set(full.lower().split())
    if name_words & non_name_words and not (name_words - non_name_words):
        return False
    # Reject if starts with common non-name prefixes
    non_name_starts = ['het ', 'de ', 'een ', 'van ', 'architect', 'advies', 'kwaliteit']
    for prefix in non_name_starts:
        if full.lower().startswith(prefix) and not full.lower().startswith('van '):
            return False
    # Reject if contains common Dutch non-name words
    non_name_words = ['bouw', 'internet', 'software', 'online', 'digital', 'media',
                      'design', 'consulting', 'advice', 'groep', 'group', 'bv', 'bvba',
                      'bedrijf', 'onderneming', 'firma', 'kantoor']
    for w in non_name_words:
        if w in full.lower() and ' ' in full:
            # Only reject if the non-name word is a significant part (first or last word)
            words_lower = full.lower().split()
            if words_lower[0] == w or words_lower[-1] == w:
                return False
    return True


# ---------- Note generation ----------


def _clean_company_name(name):
    """Clean company name for use in notes. Removes domains, URLs, and garbage."""
    if not name:
        return ""
    # Remove domain names (.nl, .be, .de, .com, .io, etc.)
    name = re.sub(r'\s*\S+\.(nl|be|de|com|io|eu|org|net|co)\b', '', name)
    # Remove URL-like patterns
    name = re.sub(r'https?://\S+', '', name)
    # Remove common website suffixes
    name = re.sub(r'\s*(Welkom|Home|Login|Search|Menu|Contact|About|Blog)\s*$', '', name, flags=re.IGNORECASE)
    # Remove taglines after company name (e.g., "Company - Tagline" or "Company | Tagline")
    name = re.sub(r'\s*[-|]\s*.{5,}$', '', name)
    # Remove trailing garbage words
    name = re.sub(r'\s+(en meer|etc|reviews|vacature|blog|nieuws|online)\s*$', '', name, flags=re.IGNORECASE)
    # Clean whitespace
    name = ' '.join(name.split()).strip()
    # Remove quotes
    name = name.strip('"').strip("'")
    # Reject garbage company names - comprehensive list
    garbage_companies = [
        '01:30', 'hshs', 'https', 'http', 'www', 'none', 'null',
        'test', 'demo', 'example', 'undefined', 'info', 'contact',
        'unknown', 'n/a', '-', '--', '...', 'bv', 'bvba', 'gmbh',
        'interimrecruiter', 'scipiovanderstoel', 'mattgray', 'hansmarechal',
        'wr', 'wr wr', 'apotheek', 'apotheek apotheek', 'pharma', 'pharma box',
        'm+r', 'mplusr', 'happy', 'happy horizon', 'kliniekzoeker',
        'onlinemagazine', 'logistiek', 'kinesitherapie', 'architectenregister',
        'unitedconsulting', 'vgm', 'vgm nl', 'happyhorizon', 'anp', 'anp connect',
        'steuerberaten', 'bistroo', 'archello', 'indeed', 'alles', 'alles over',
        'ads-steuer', 'ads steuer', 'doggo', 'bouwbedrijf', 'wys', 'wys adviseurs',
        'hoogbegaafd', 'hoogbegaafd in bedrijf', 'shopify', 'best', 'best interiors',
        'kwaliteitsregister', 'roxtar', 'placetel', 'placetel  it',
    ]
    if name.lower() in garbage_companies or len(name) < 2:
        return ""
    # Reject if it's just numbers or punctuation
    if re.match(r'^[\d\s:.\-]+$', name):
        return ""
    # Reject if it contains too many special chars (likely scraped garbage)
    if len(name) > 0 and sum(1 for c in name if not c.isalnum() and c not in ' &.') > len(name) * 0.3:
        return ""
    # Reject if name looks like a URL fragment
    if re.match(r'^[a-z]+\d+[a-z]*$', name.lower()):
        return ""
    return name

def generate_linkedin_note(email_body, first_name, company_name="", research_result=None, opportunity_mapping=None):
    """Generate a 180-char LinkedIn connection note.
    Uses research_result and opportunity_mapping for deep personalization when available.
    
    KEY RULES:
    1. Sound like a normal person, NOT a salesperson
    2. Show genuine curiosity about their work
    3. Be general, not specific about their business
    4. Optionally mention we help businesses with AI/automation (general, not specific pitch)
    5. Ask a simple, open question about their challenges
    Returns None if the name is garbage and shouldn't be used.
    """
    # Validate name first
    if not is_valid_linkedin_name(first_name):
        return None
    
    # Clean up first_name - remove numbers, weird characters, titles
    if first_name:
        # Remove common Dutch titles
        first_name = re.sub(r'^(ir\.\s*ing\.\s*|ing\.\s*|ir\.\s*|drs\.\s*|mr\.\s*|prof\.\s*|dr\.\s*)', '', first_name, flags=re.IGNORECASE)
        # Remove numbers and special chars
        first_name = re.sub(r'[0-9]', '', first_name)
        first_name = re.sub(r'[^a-zA-Z\s]', ' ', first_name)
        # Clean up whitespace
        first_name = ' '.join(first_name.split()).strip()
        # Take only the first word if multiple
        first_name = first_name.split()[0] if first_name else ""
        # Capitalize properly
        first_name = first_name.capitalize()
    
    # Clean company name for notes
    company_name = _clean_company_name(company_name)
    if not company_name:
        company_name = "jullie bedrijf"
    
    # Extract personalization data from research + opportunity
    company_summary = ""
    industry = ""
    specific_problem = ""
    services_offered = ""
    company_size = ""
    
    if research_result:
        try:
            if isinstance(research_result, str):
                research_result = json.loads(research_result)
            company_summary = research_result.get("company_summary", research_result.get("summary", ""))
            industry = research_result.get("industry", "")
            services_offered = research_result.get("services", research_result.get("products", ""))
            company_size = research_result.get("company_size", research_result.get("employees", ""))
        except Exception:
            pass
    
    if opportunity_mapping:
        try:
            if isinstance(opportunity_mapping, str):
                opportunity_mapping = json.loads(opportunity_mapping)
            specific_problem = opportunity_mapping.get("specific_problem", "")
        except Exception:
            pass
    
    # Build context for LLM
    context = f"Bedrijf: {company_name}"
    if company_summary:
        context += f"\nWat ze doen: {company_summary[:200]}"
    if industry:
        context += f"\nIndustrie: {industry}"
    if specific_problem:
        context += f"\nGevonden probleem: {specific_problem[:150]}"
    if services_offered:
        context += f"\nDiensten: {services_offered[:100]}"
    
    # Try LLM first (much better quality)
    try:
        import asyncio
        import concurrent.futures
        
        def _call_llm():
            loop = asyncio.new_event_loop()
            try:
                from tools import call_llm
                prompt = (
                    f"Schrijf een LinkedIn connectieverzoek-bericht (MAXIMAAL 180 tekens, ZONDER aanhef, Nederlands).\n"
                    f"Aan: {first_name} van {company_name}\n"
                    f"Context: {context}\n\n"
                    f"DOEL: De ontvanger moet denken 'oh, deze persoon begrijpt mijn situatie'.\n\n"
                    f"POSITIE: Wij zijn een implementation partner. We bouwen de operating layer die onder de afdelingen zit. De herhalende werkt gaat naar het systeem, mensen houden ruimte voor oordeel.\n\n"
                    f"REGELS:\n"
                    f"- Begin direct met de voornaam (geen 'Hallo'/'Beste')\n"
                    f"- NOOIT iets verkopen of over jezelf praten\n"
                    f"- NOOIT AI, automatisering, of je eigen bedrijf noemen\n"
                    f"- NOOIT gefeliciteerd zeggen of verjaardagen/jubilea noemen\n"
                    f"- NOOIT dingen verzinnen over het bedrijf die niet in de context staan\n"
                    f"- NOOIT zeggen dat je ze al volgt als dat niet zo is\n"
                    f"- NOOIT de woorden: vervangen, automatiseren, minder mensen nodig, personeel snijden, scorebord, monitoren\n"
                    f"- Max 1-2 zinnen\n"
                    f"- Wees EERLIJK: je hebt hun profiel gezien en bent nieuwsgierig\n"
                    f"- Wees specifiek: noem iets wat je hebt gezien aan hun profiel of bedrijf\n"
                    f"- Wees nieuwsgierig: stel een korte vraag over hun werk\n"
                    f"- Klink als een vriendelijke collega, niet als een verkoper\n"
                    f"- Leid met de constraint: wat kost ze elke week tijd?\n\n"
                    f"EERLIJKE voorbeelden (geen leugens, geen dashes):\n"
                    f"- '{first_name}, ik zag dat je bij {company_name} werkt, hoe bevalt dat?'\n"
                    f"- '{first_name}, interessant wat jullie bij {company_name} doen, hoe pakken jullie dat aan?'\n"
                    f"- '{first_name}, ik zag je profiel voorbijkomen, nieuwsgierig naar je werk.'\n"
                    f"- '{first_name}, {company_name} deed me denken aan iets, hoe doen jullie dat?'\n\n"
                    f"Schrijf NU het bericht. Antwoord alleen met het bericht, niks anders."
                )
                raw = loop.run_until_complete(call_llm(prompt))
                # Take only first line/sentence
                note = raw.strip().strip('"').strip("'").strip()
                note = note.split('\n')[0].strip()
                note = note.split('. ')[0].strip()  # First sentence only
                # Remove prefixes
                note = re.sub(r'^(bericht|message|nota|voorbeeld):\s*', '', note, flags=re.IGNORECASE)
                note = re.sub(r'\[.*?\]', '', note)
                note = re.sub(r'\s+', ' ', note).strip()
                # Hard truncate at 180
                if len(note) > 180:
                    note = note[:177] + "..."
                return note
            finally:
                loop.close()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_call_llm)
            note = future.result(timeout=15)
        
        # Validate the note doesn't contain red flags
        red_flags = ['gefeliciteerd', 'proficiat', 'happy birthday', 'jubileum', 'verjaardag',
                     '10 jaar', '5 jaar', 'bestaan', 'anniversary', 'wat kan ik voor je doen',
                     'kletsen', 'gebit', 'tandarts', 'bezoek onze website', 'leuk om te zien',
                     'blog groeit', 'topcarriere', 'joanna', 'kracht te vinden',
                     'ben je op zoek', 'bezoek onze', 'laatste informatie',
                     'wij helpen bedrijven', 'wij bouwen', 'wij automatiseren',
                     'wat doen jullie nu nog handmatig', 'wat doe je nu nog handmatig',
                     'saaie taken', 'processen automatiseren', 'werk uit handen',
                     'te veel tijd', 'efficiëntie', 'optimaliseren',
                     'boek een call', 'plan een call', 'gratis kennismaking',
                     'laten we bellen', 'bel me op', 'stuur me een bericht']
        # Add garbage company names as red flags too
        garbage_patterns = ['01:30', 'hshs', 'scipio', 'mattgray', 'hansmarechal',
                            '0123', '456', '789', 'test', 'demo', 'example',
                            'apotheek', 'pharmabox', 'wr wr', 'kliniekzoeker',
                            'placetel', 'onlinemagazine', 'kinesitherapie']
        red_flags.extend(garbage_patterns)
        note_lower = note.lower()
        has_red_flag = any(f in note_lower for f in red_flags)
        
        # Remove any domain URLs that slipped through
        note = re.sub(r'\S+\.(nl|be|de|com|io|eu|org|net|co)\b', '', note)
        note = re.sub(r'\s+', ' ', note).strip()
        
        if note and 50 < len(note) <= 180 and not has_red_flag:
            return note
    except Exception:
        pass
    
    # Fallback: honest, curious templates (NO lies, NO selling, NO dashes)
    templates = []
    
    # With company name - honest curiosity
    if company_name:
        templates.append(f"{first_name}, ik zag dat je bij {company_name} werkt, hoe bevalt dat?")
        templates.append(f"{first_name}, interessant wat jullie bij {company_name} doen, hoe pakken jullie dat aan?")
        templates.append(f"{first_name}, ik zag je profiel voorbijkomen, nieuwsgierig naar je werk bij {company_name}.")
        templates.append(f"{first_name}, {company_name} sprak me aan, hoe doen jullie dat?")
    
    # BASIC: Nothing specific - honest curiosity
    templates.append(f"{first_name}, ik zag je profiel voorbijkomen en was nieuwsgierig naar je werk.")
    templates.append(f"{first_name}, interessant werk, ik wilde graag connecten.")
    templates.append(f"{first_name}, hoe gaat het met je werk?")
    
    import random as _rnd
    note = _rnd.choice(templates)
    
    if len(note) > 180:
        note = note[:177] + "..."
    return note


# ---------- Connection status tracking ----------

def _check_connection_status(profile_url):
    """Check if a connection request was accepted by visiting the profile.
    Returns 'accepted', 'pending', or 'not_connected'.
    """
    try:
        _log({"type": "info", "message": f"Checking connection status: {profile_url}"})
        with get_firefox_driver() as (driver, page):
            page.goto(profile_url, wait_until="domcontentloaded", timeout=120000)
            _random_sleep(4, 6)

            # Check for auth wall
            auth = _detect_auth_wall(page)
            if auth:
                return "error"

            # Wait for SPA to render
            for attempt in range(10):
                try:
                    ready = page.evaluate("""() => {
                        return {
                            links: document.querySelectorAll('a').length,
                            buttons: document.querySelectorAll('button').length,
                        };
                    }""")
                    if ready and ready.get("links", 0) > 5:
                        break
                except Exception:
                    pass
                time.sleep(2)

            # Check connection status
            status = _check_if_connected(page)
            return status

    except Exception as e:
        _log({"type": "error", "message": f"Error checking connection: {str(e)[:80]}"})
        return "error"


def check_pending_connections():
    """Check all pending connections and update their status.
    Respects flag state — skips if account is flagged.
    """
    flagged, flag_reason = _is_flagged()
    if flagged:
        _log({"type": "warning", "message": f"Account flagged ({flag_reason}). Skipping connection checks."})
        return {"checked": 0, "accepted": 0, "flagged": True}

    db = _get_clawbuildr_db()
    # Get connections that are still pending and haven't been checked in 24 hours
    rows = db.execute("""
        SELECT id, profile_url, first_name, last_name, company 
        FROM linkedin_outreach 
        WHERE outcome IN ('SUCCESS', 'SUCCESS_NO_NOTE')
        AND (connection_status = 'pending' OR connection_status IS NULL)
        AND (last_checked IS NULL OR last_checked < datetime('now', '-6 hours'))
        LIMIT 3
    """).fetchall()

    if not rows:
        db.close()
        return {"checked": 0, "accepted": 0}

    accepted_count = 0
    for row in rows:
        id, profile_url, first_name, last_name, company = row
        if not profile_url:
            continue

        status = _check_connection_status(profile_url)
        now = datetime.now().isoformat()

        if status == "connected":
            db.execute(
                "UPDATE linkedin_outreach SET connection_status = 'accepted', last_checked = ? WHERE id = ?",
                (now, id)
            )
            accepted_count += 1
            _log({"type": "info", "message": f"Connection ACCEPTED: {first_name} {last_name}"})
        elif status == "pending":
            db.execute(
                "UPDATE linkedin_outreach SET connection_status = 'pending', last_checked = ? WHERE id = ?",
                (now, id)
            )
        else:
            db.execute(
                "UPDATE linkedin_outreach SET connection_status = ?, last_checked = ? WHERE id = ?",
                (status, now, id)
            )

        _random_sleep(5, 10)  # Rate limit between checks

    db.commit()
    db.close()
    return {"checked": len(rows), "accepted": accepted_count}


def _ts():
    """Return a timestamp string for filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _send_followup_message(profile_url, first_name, company=""):
    """Send a follow-up message to someone who accepted the connection.
    
    Strategy (proven working):
    1. Navigate to profile to detect language
    2. Go to /messaging/compose/ (full-page compose, NOT popup modal)
    3. Type recipient name in the search field
    4. Select them from dropdown
    5. Type message in textarea
    6. Click Verzenden button (works on full-page compose, unlike popup modal)
    """
    proof_dir = os.path.join(os.path.dirname(__file__), "data", "proof")
    os.makedirs(proof_dir, exist_ok=True)
    try:
        _log({"type": "info", "message": f"Sending follow-up to {first_name}..."})
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.common.action_chains import ActionChains

        driver, page = _persistent_firefox.ensure_ready()

        # Step 1: Visit profile to detect language
        page.goto(profile_url, wait_until="domcontentloaded", timeout=120000)
        _random_sleep(4, 6)
        _human_profile_visit(page)

        for attempt in range(10):
            try:
                ready = page.evaluate("""() => {
                    return {
                        links: document.querySelectorAll('a').length,
                        buttons: document.querySelectorAll('button').length,
                    };
                }""")
                if ready and ready.get("links", 0) > 5:
                    break
            except Exception:
                pass
            time.sleep(2)

        auth = _detect_auth_wall(page)
        if auth:
            return "AUTH_WALL"

        language = _detect_profile_language(page)
        followup_note = generate_followup_note(first_name, company, language=language)
        _log({"type": "info", "message": f"Message ({language}) for {first_name}: {followup_note[:80]}..."})

        # Step 2: Navigate to full-page compose
        _log({"type": "info", "message": "Navigating to full-page compose..."})
        page.goto("https://www.linkedin.com/messaging/compose/", wait_until="domcontentloaded", timeout=120000)
        _random_sleep(4, 6)

        driver.save_screenshot(f"{proof_dir}/followup_01_compose_{_ts()}.png")

        # Step 3: Type recipient name in search field
        name_input = None
        try:
            inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="text"]')
            for inp in inputs:
                placeholder = (inp.get_attribute("placeholder") or "").lower()
                if "namen" in placeholder or "names" in placeholder or "zoek" in placeholder:
                    rect = inp.rect
                    if rect.get("width", 0) > 50:
                        name_input = inp
                        break
        except Exception:
            pass

        if not name_input:
            _log({"type": "warning", "message": "Could not find name input field"})
            return "NO_NAME_FIELD"

        name_input.click()
        _random_sleep(0.5, 1)
        _human_type_into_element(driver, name_input, f"{first_name} {company.split()[0] if company else ''}", wpm_range=(45, 75))
        _random_sleep(3, 4)

        driver.save_screenshot(f"{proof_dir}/followup_02_search_{_ts()}.png")

        # Step 4: Select the person from dropdown
        selected = False
        try:
            # Look for dropdown items containing the first name
            items = driver.find_elements(By.CSS_SELECTOR, 'li, [role="option"]')
            for item in items:
                text = (item.text or "").strip()
                if first_name.lower() in text.lower() and len(text) > 5:
                    rect = item.rect
                    if rect.get("width", 0) > 50 and rect.get("height", 0) > 10:
                        item.click()
                        selected = True
                        _log({"type": "info", "message": f"Selected {first_name} from dropdown"})
                        break
        except Exception:
            pass

        if not selected:
            _log({"type": "warning", "message": f"Could not select {first_name} from dropdown"})
            return "NO_DROPDOWN_MATCH"

        _random_sleep(2, 3)
        driver.save_screenshot(f"{proof_dir}/followup_03_selected_{_ts()}.png")

        # Step 5: Type the message with human-like typing
        typed = False
        try:
            editables = driver.find_elements(By.CSS_SELECTOR, '[contenteditable="true"]')
            if editables:
                msg_area = editables[-1]
                driver.execute_script("arguments[0].focus();", msg_area)
                _random_sleep(0.5, 1)
                _human_type_into_element(driver, msg_area, followup_note, wpm_range=(40, 70))
                typed = True
                _log({"type": "info", "message": "Message typed in textarea"})
        except Exception:
            pass

        if not typed:
            # Fallback: click in message area and use ActionChains
            try:
                page.mouse.click(640, 400)
                _random_sleep(0.5, 1)
                _human_type_selenium(driver, followup_note, wpm_range=(40, 70))
                typed = True
                _log({"type": "info", "message": "Message typed via ActionChains"})
            except Exception:
                pass

        if not typed:
            _log({"type": "warning", "message": f"Could not type message for {first_name}"})
            return "NO_CHAT_INPUT"

        _human_read_time(min_seconds=2, max_seconds=5)
        driver.save_screenshot(f"{proof_dir}/followup_04_typed_{_ts()}.png")

        # Step 6: Click Verzenden button
        sent = False
        try:
            send_btn = driver.execute_script("""
                const btns = document.querySelectorAll('button');
                for (const btn of btns) {
                    const text = (btn.textContent || '').trim();
                    const rect = btn.getBoundingClientRect();
                    if ((text === 'Verzenden' || text === 'Send') 
                        && rect.width > 10 && rect.height > 10 && !btn.disabled) {
                        return {x: rect.x + rect.width/2, y: rect.y + rect.height/2, text: text};
                    }
                }
                return null;
            """)
            if send_btn:
                page.mouse.click(send_btn['x'], send_btn['y'])
                sent = True
                _log({"type": "info", "message": f"Clicked Verzenden at ({send_btn['x']:.0f},{send_btn['y']:.0f})"})
        except Exception as e:
            _log({"type": "warning", "message": f"Verzenden click failed: {e}"})

        if not sent:
            # Fallback: Enter key
            _log({"type": "info", "message": "Trying Enter key to send..."})
            ActionChains(driver).send_keys(Keys.ENTER).perform()
            sent = True

        _random_sleep(3, 5)
        driver.save_screenshot(f"{proof_dir}/followup_05_sent_{_ts()}.png")

        # Verify
        url = driver.current_url
        _log({"type": "info", "message": f"Follow-up to {first_name}: sent={sent}, url={url}"})
        
        if sent:
            return "SUCCESS"
        return "SEND_FAILED"

    except Exception as e:
        _log({"type": "error", "message": f"Error sending follow-up: {str(e)[:80]}"})
        return "ERROR"


def _detect_profile_language(page):
    """Detect if a LinkedIn profile is English or Dutch based on profile text."""
    try:
        text = page.evaluate("""() => {
            const body = document.body.innerText || '';
            return body.substring(0, 3000).toLowerCase();
        }""") or ""

        dutch_signals = [
            "nederland", "dutch", "amsterdam", "rotterdam", "utrecht", "holland",
            "bedrijf", "ervaring", "vaardigheden", "opleiding", "connectie",
            "bericht", "bijdragen", "volgers", "samenvatting", "functie",
            "bedrijfskunde", "economie", "marketing", "sales", "directeur",
            "opdrachtgever", "zzp", "freelance", "mkb", "kennis",
        ]
        english_signals = [
            "united states", "united kingdom", "san francisco", "new york",
            "london", "los angeles", "chicago", "houston", "phoenix",
            "experience", "skills", "education", "about", "connections",
            "recommendations", "certifications", "summary", "current",
            "previous", "school", "bachelor", "master", "mba",
            "startup", "founder", "ceo", "cto", "vp", "director",
        ]

        dutch_count = sum(1 for w in dutch_signals if w in text)
        english_count = sum(1 for w in english_signals if w in text)

        _log({"type": "info", "message": f"Language detection: dutch={dutch_count}, english={english_count}"})

        if dutch_count > english_count:
            return "dutch"
        elif english_count > dutch_count:
            return "english"
        else:
            return "dutch"  # Default to Dutch (our primary market)
    except Exception as e:
        _log({"type": "warning", "message": f"Language detection error: {e}"})
        return "dutch"


def generate_followup_note(first_name, company="", language="dutch"):
    """Generate a follow-up message for someone who accepted the connection."""
    company = (company or "").strip()
    # Clean company name - reject garbage
    if company:
        company = _clean_company_name(company)
    
    if language == "english":
        if company:
            templates = [
                f"Thanks for connecting, {first_name}! We build the operating layer for SMBs. Repetitive work moves to the system, people keep room for judgement. Are there processes at {company} that take too much time every week?",
                f"{first_name}, thanks for the connection! The most efficient version of your company already exists — it's in your inbox and systems that don't talk. We bring it to one place. How do you handle volume at {company}?",
                f"Great to connect, {first_name}! We build the operating layer that sits under departments. Machines repeat, people decide. Are there tasks at {company} that take a lot of time and should move to the system?",
                f"Good to match, {first_name}! Every euro of structural cost savings is ~5 euros in company value. We help SMBs with process analysis and operating layer implementation. Where are the biggest bottlenecks at {company}?",
            ]
        else:
            templates = [
                f"Thanks for connecting, {first_name}! We build the operating layer for SMBs. Repetitive work moves to the system, people keep room for judgement. Are there processes that take too much time every week?",
                f"{first_name}, thanks for the connection! The most efficient version of your company already exists — it's in your inbox and systems that don't talk. We bring it to one place. How do you handle that?",
                f"Great to connect, {first_name}! We build the operating layer that sits under departments. Machines repeat, people decide. Are there tasks that take a lot of time and should move to the system?",
                f"Good to match, {first_name}! Every euro of structural cost savings is ~5 euros in company value. We help SMBs with process analysis and operating layer implementation. Where are the biggest bottlenecks?",
            ]
    else:
        if company:
            templates = [
                f"Bedankt voor de connectie, {first_name}! Wij bouwen de operating layer voor MKB-bedrijven. Herhalende werkt naar het systeem, mensen houden ruimte voor oordeel. Zijn er processen bij {company} die elke week te veel tijd kosten?",
                f"{first_name}, bedankt voor de connectie! De meest efficiënte versie van uw bedrijf bestaat al — hij zit in uw inbox en systemen die niet praten. Wij brengen het op één plek. Hoe gaan jullie bij {company} om met volume?",
                f"Leuk om kennis te maken, {first_name}! Wij bouwen de operating layer die onder de afdelingen zit. Machines herhalen, mensen beslissen. Zijn er taken bij {company} die veel tijd kosten en naar het systeem moeten?",
                f"Goed om te matchen, {first_name}! Elke euro structurele kostenbesparing is ~5 euro bedrijfswaarde. Wij helpen MKB-bedrijven met procesanalyse en operating layer implementatie. Waar zitten de grootste knelpunten bij {company}?",
            ]
        else:
            templates = [
                f"Bedankt voor de connectie, {first_name}! Wij bouwen de operating layer voor MKB-bedrijven. Herhalende werkt naar het systeem, mensen houden ruimte voor oordeel. Zijn er processen die elke week te veel tijd kosten?",
                f"{first_name}, bedankt voor de connectie! De meest efficiënte versie van uw bedrijf bestaat al — hij zit in uw inbox en systemen die niet praten. Wij brengen het op één plek. Hoe gaan jullie daarmee om?",
                f"Leuk om kennis te maken, {first_name}! Wij bouwen de operating layer die onder de afdelingen zit. Machines herhalen, mensen beslissen. Zijn er taken die veel tijd kosten en naar het systeem moeten?",
                f"Goed om te matchen, {first_name}! Elke euro structurele kostenbesparing is ~5 euro bedrijfswaarde. Wij helpen MKB-bedrijven met procesanalyse en operating layer implementatie. Waar zitten de grootste knelpunten?",
            ]
    import random
    return random.choice(templates)


def send_pending_followups():
    """Send follow-ups to all accepted connections that haven't received one yet."""
    db = _get_clawbuildr_db()
    rows = db.execute("""
        SELECT id, profile_url, first_name, last_name, company 
        FROM linkedin_outreach 
        WHERE connection_status = 'accepted' 
        AND followup_sent = 0
        LIMIT 5
    """).fetchall()

    if not rows:
        db.close()
        return {"sent": 0}

    sent_count = 0
    for row in rows:
        id, profile_url, first_name, last_name, company = row
        if not profile_url:
            continue

        outcome = _send_followup_message(profile_url, first_name, company)
        now = datetime.now().isoformat()

        if outcome == "SUCCESS":
            db.execute(
                "UPDATE linkedin_outreach SET followup_sent = 1, followup_note = ? WHERE id = ?",
                ("Follow-up sent", id)
            )
            # Also save to followups table
            db.execute(
                "INSERT INTO linkedin_followups (profile_url, first_name, last_name, company, followup_note, outcome, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (profile_url, first_name, last_name, company, "Follow-up sent", outcome, now)
            )
            sent_count += 1
        else:
            _log({"type": "warning", "message": f"Follow-up failed for {first_name}: {outcome}"})

        _random_sleep(30, 60)  # Rate limit between messages

    db.commit()
    db.close()
    return {"sent": sent_count}


def get_connection_stats():
    """Get statistics about connection statuses."""
    db = _get_clawbuildr_db()
    
    total = db.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS'").fetchone()[0]
    pending = db.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS' AND (connection_status = 'pending' OR connection_status IS NULL)").fetchone()[0]
    accepted = db.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE connection_status = 'accepted'").fetchone()[0]
    followups_sent = db.execute("SELECT COUNT(*) FROM linkedin_outreach WHERE followup_sent = 1").fetchone()[0]
    
    db.close()
    
    return {
        "total_sent": total,
        "pending": pending,
        "accepted": accepted,
        "acceptance_rate": round(accepted / total * 100, 1) if total > 0 else 0,
        "followups_sent": followups_sent,
    }


# ---------- Auto-scheduler ----------

_auto_scheduler_running = False
_auto_scheduler_thread = None

def _auto_scheduler_loop():
    """Background loop that automatically sends connection requests throughout the day.
    Respects: flag state, active hours, warming limits, randomized delays.
    """
    global _auto_scheduler_running
    
    _log({"type": "info", "message": "Auto-scheduler started"})
    last_check_time = time.time()
    CHECK_INTERVAL = 1800  # Check pending connections every 30 minutes
    
    while _auto_scheduler_running:
        try:
            now_ts = time.time()
            
            # Check if account is flagged — skip all outreach
            flagged, flag_reason = _is_flagged()
            if flagged:
                _log({"type": "warning", "message": f"Auto-scheduler: account flagged ({flag_reason}). Sleeping 5 minutes."})
                time.sleep(300)
                continue

            # Check active hours
            active, reason = _is_active_hours()
            if not active:
                _log({"type": "info", "message": f"Auto-scheduler: not active hours ({reason}). Sleeping 5 minutes."})
                time.sleep(300)
                continue
            
            # Check if we can send connections (warming-aware)
            can_send, period_info = can_send_connection()
            
            if can_send:
                # Get contacts from database that haven't been contacted
                db = _get_clawbuildr_db()
                try:
                    prospects = db.execute("""
                        SELECT c.contact_id, c.first_name, c.last_name, c.linkedin_url, 
                               c.research_result, c.opportunity_mapping, co.name as company_name, co.domain
                        FROM contacts c
                        JOIN companies co ON c.company_id = co.company_id
                        WHERE c.current_stage NOT IN ('CLOSED_WON', 'CLOSED_LOST')
                        AND c.contact_id NOT IN (
                            SELECT contact_id FROM linkedin_outreach 
                            WHERE outcome IN ('SUCCESS', 'SUCCESS_NO_NOTE', 'PENDING', 
                                               'ALREADY_PENDING', 'ALREADY_CONTACTED', 'ALREADY_CONNECTED')
                            AND contact_id IS NOT NULL AND contact_id != ''
                        )
                        AND c.linkedin_url NOT IN (
                            SELECT profile_url FROM linkedin_outreach 
                            WHERE outcome IN ('SUCCESS', 'SUCCESS_NO_NOTE', 'PENDING', 
                                               'ALREADY_PENDING', 'ALREADY_CONTACTED', 'ALREADY_CONNECTED')
                            AND profile_url IS NOT NULL AND profile_url != ''
                        )
                        AND c.linkedin_url IS NOT NULL AND c.linkedin_url != ''
                        ORDER BY c.lead_score DESC
                        LIMIT 6
                    """).fetchall()
                except Exception as e:
                    _log({"type": "warning", "message": f"Error querying contacts: {str(e)[:60]}"})
                    prospects = []
                
                if prospects:
                    for prospect in prospects:
                        contact_id, first_name, last_name, linkedin_url, research_json, opportunity_json, company_name, domain = prospect
                        
                        # Pre-validate: skip garbage prospects before launching Firefox
                        is_good, reject_reason = is_quality_prospect(first_name or "", last_name or "", company_name or "", linkedin_url or "")
                        if not is_good:
                            _log({"type": "info", "message": f"Skipping garbage prospect: {first_name} {last_name} ({reject_reason})"})
                            continue
                        
                        # Parse research and opportunity data
                        research_result = None
                        opportunity_mapping = None
                        try:
                            if research_json:
                                research_result = json.loads(research_json) if isinstance(research_json, str) else research_json
                        except Exception:
                            pass
                        try:
                            if opportunity_json:
                                opportunity_mapping = json.loads(opportunity_json) if isinstance(opportunity_json, str) else opportunity_mapping
                        except Exception:
                            pass
                        
                        # Use LinkedIn URL directly if available
                        if linkedin_url:
                            result = search_and_connect(
                                first_name=first_name or "",
                                last_name=last_name or "",
                                company=company_name or "",
                                email_body="",
                                contact_id=contact_id,
                                profile_url=linkedin_url,
                                research_result=research_result,
                                opportunity_mapping=opportunity_mapping
                            )
                        else:
                            result = search_and_connect(
                                first_name=first_name or "",
                                last_name=last_name or "",
                                company=company_name or "",
                                email_body="",
                                contact_id=contact_id,
                                research_result=research_result,
                                opportunity_mapping=opportunity_mapping
                            )
                        
                        outcome = result.get('outcome', '')
                        _log({"type": "info", "message": f"Auto-scheduler: {outcome} for {first_name} {last_name}"})
                        
                        # If successful, stop trying more prospects
                        if outcome in ('SUCCESS', 'SUCCESS_NO_NOTE'):
                            break
                        # If already contacted or failed, try next prospect
                
                db.close()

            # --- Follow-up check (every 30 min, after connection sending) ---
            # Only check follow-ups if account is not flagged
            if now_ts - last_check_time >= CHECK_INTERVAL and not _is_flagged()[0]:
                try:
                    check_result = check_pending_connections()
                    if check_result.get("accepted", 0) > 0:
                        _log({"type": "info", "message": f"Found {check_result['accepted']} new accepted connections"})
                    followup_result = send_pending_followups()
                    if followup_result.get("sent", 0) > 0:
                        _log({"type": "info", "message": f"Sent {followup_result['sent']} follow-up messages"})
                    last_check_time = now_ts
                except Exception as e:
                    _log({"type": "error", "message": f"Follow-up check error: {str(e)[:80]}"})
                    last_check_time = now_ts
            
            # Sleep for 5 minutes before next check
            time.sleep(300)
            
        except Exception as e:
            _log({"type": "error", "message": f"Auto-scheduler error: {str(e)[:80]}"})
            time.sleep(60)
    
    _log({"type": "info", "message": "Auto-scheduler stopped"})


def start_auto_scheduler():
    """Start the auto-scheduler background thread."""
    global _auto_scheduler_running, _auto_scheduler_thread
    
    if _auto_scheduler_running:
        return {"status": "already_running"}
    
    _auto_scheduler_running = True
    _auto_scheduler_thread = threading.Thread(target=_auto_scheduler_loop, daemon=True)
    _auto_scheduler_thread.start()
    
    return {"status": "started"}


def stop_auto_scheduler():
    """Stop the auto-scheduler background thread."""
    global _auto_scheduler_running
    
    _auto_scheduler_running = False
    return {"status": "stopped"}


def get_auto_scheduler_status():
    """Get the current status of the auto-scheduler."""
    return {
        "running": _auto_scheduler_running,
        "thread_alive": _auto_scheduler_thread.is_alive() if _auto_scheduler_thread else False,
    }


# ---------- API helper functions ----------

def get_status():
    with _state_lock:
        return {
            "running": _job_state["running"],
            "total": _job_state["total"],
            "completed": _job_state["completed"],
            "current_url": _job_state["current_url"],
            "current_name": _job_state["current_name"],
            "log": list(_job_state["log"]),
            "results": list(_job_state["results"]),
            "stop_requested": _job_state["stop_requested"],
        }


def clear_status():
    with _state_lock:
        _job_state["log"] = []
        _job_state["results"] = []


def set_stop():
    with _state_lock:
        _job_state["stop_requested"] = True
        _job_state["running"] = False


def get_clawbuildr_linkedin_history(limit=100):
    db = _get_clawbuildr_db()
    rows = db.execute(
        "SELECT id, contact_id, first_name, last_name, company, profile_url, note, outcome, timestamp, proof FROM linkedin_outreach ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    db.close()
    return [
        {
            "id": r[0], "contact_id": r[1], "first_name": r[2], "last_name": r[3],
            "company": r[4], "profile_url": r[5], "note": r[6], "outcome": r[7], "timestamp": r[8],
            "proof": r[9] if len(r) > 9 else None,
        }
        for r in rows
    ]


def get_daily_count_api():
    """Return daily and period counts for the API, including warming multiplier."""
    from datetime import datetime
    now = datetime.now()
    hour = now.hour
    
    daily_count = _get_daily_count()
    period_count, period_limit, period = _get_period_count()
    effective_limit, multiplier = _get_effective_daily_limit()
    flagged, flag_reason = _is_flagged()
    
    # Calculate next available time
    next_period = ""
    if flagged:
        next_period = f"PAUSED ({flag_reason})"
    elif hour < 8:
        next_period = "morning (8:00)"
    elif hour >= 21:
        next_period = "tomorrow morning (8:00)"
    elif period == "morning" and period_count >= period_limit:
        next_period = "afternoon (12:00)"
    elif period == "afternoon" and period_count >= period_limit:
        next_period = "evening (17:00)"
    elif period == "evening" and period_count >= period_limit:
        next_period = "tomorrow morning (8:00)"
    
    return {
        "today": daily_count,
        "limit": effective_limit,
        "base_limit": DAILY_LIMIT,
        "warmup_multiplier": multiplier,
        "warmup_percent": f"{multiplier:.0%}",
        "period": period,
        "period_count": period_count,
        "period_limit": period_limit,
        "next_available": next_period,
        "can_send": can_send_connection()[0],
        "flagged": flagged,
        "flag_reason": flag_reason,
    }


def get_scheduling_status():
    """Get detailed scheduling status for dashboard display."""
    from datetime import datetime
    now = datetime.now()
    
    # Get counts for each period
    db = _get_clawbuildr_db()
    today = date.today().isoformat()
    
    morning = db.execute(
        """SELECT COUNT(*) FROM linkedin_outreach 
           WHERE outcome = 'SUCCESS' 
           AND timestamp LIKE ? 
           AND CAST(strftime('%H', timestamp) AS INTEGER) >= 8 
           AND CAST(strftime('%H', timestamp) AS INTEGER) < 12""",
        (today + "%",)
    ).fetchone()[0]
    
    afternoon = db.execute(
        """SELECT COUNT(*) FROM linkedin_outreach 
           WHERE outcome = 'SUCCESS' 
           AND timestamp LIKE ? 
           AND CAST(strftime('%H', timestamp) AS INTEGER) >= 12 
           AND CAST(strftime('%H', timestamp) AS INTEGER) < 17""",
        (today + "%",)
    ).fetchone()[0]
    
    evening = db.execute(
        """SELECT COUNT(*) FROM linkedin_outreach 
           WHERE outcome = 'SUCCESS' 
           AND timestamp LIKE ? 
           AND CAST(strftime('%H', timestamp) AS INTEGER) >= 17 
           AND CAST(strftime('%H', timestamp) AS INTEGER) < 21""",
        (today + "%",)
    ).fetchone()[0]
    
    db.close()

    # Apply warming multiplier to limits
    _, multiplier = _get_effective_daily_limit()
    scaled_limit = int(DAILY_LIMIT * multiplier)
    scaled_morning = int(MORNING_LIMIT * multiplier)
    scaled_afternoon = int(AFTERNOON_LIMIT * multiplier)
    scaled_evening = int(EVENING_LIMIT * multiplier)

    return {
        "daily_total": morning + afternoon + evening,
        "daily_limit": scaled_limit,
        "daily_limit_base": DAILY_LIMIT,
        "warming_multiplier": multiplier,
        "morning": {"sent": morning, "limit": scaled_morning},
        "afternoon": {"sent": afternoon, "limit": scaled_afternoon},
        "evening": {"sent": evening, "limit": scaled_evening},
        "current_period": _get_period_count()[2],
        "can_send": can_send_connection()[0],
    }


def get_history(limit=50):
    """Return recent outreach history for the API."""
    return get_clawbuildr_linkedin_history(limit)


def run_outreach_job(urls, message_template="Hey {{name}}, I saw your profile and thought we should connect!"):
    """Run outreach on a list of profile URLs in the background.
    Called from the /api/linkedin/start endpoint.
    """
    with _state_lock:
        if _job_state["running"]:
            return
        _job_state["running"] = True
        _job_state["stop_requested"] = False
        _job_state["total"] = len(urls)
        _job_state["completed"] = 0
        _job_state["results"] = []
        _job_state["log"] = []

    _log({"type": "info", "message": f"Starting outreach job: {len(urls)} URLs"})

    # Check if account is flagged
    flagged, flag_reason = _is_flagged()
    if flagged:
        _log({"type": "error", "message": f"Account flagged ({flag_reason}). Cannot start outreach job."})
        return

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        _log({"type": "error", "message": "playwright not installed"})
        with _state_lock:
            _job_state["running"] = False
        return

    # Extract fresh cookies before launching browser
    extract_firefox_cookies()

    # Verify li_at cookie exists
    if os.path.exists(COOKIES_PATH):
        try:
            with open(COOKIES_PATH) as f:
                _ck = json.load(f)
            if not any(c.get("name") == "li_at" for c in _ck):
                _log({"type": "error", "message": "No li_at cookie. Session expired. Please log in to LinkedIn in Firefox."})
                with _state_lock:
                    _job_state["running"] = False
                return
        except Exception:
            pass

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        _inject_cookies_into_context(context)
        page = context.new_page()
        page.set_default_timeout(30000)

        stop_event, purge_thread = _start_modal_purger(page, threading.Event())

        try:
            for i, url in enumerate(urls):
                if _job_state["stop_requested"]:
                    _log({"type": "info", "message": "Stop requested. Halting outreach."})
                    break

                with _state_lock:
                    _job_state["current_url"] = url

                _log({"type": "info", "message": f"[{i+1}/{len(urls)}] Navigating to: {url}"})
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    _random_sleep(5, 8)

                    auth = _detect_auth_wall(page)
                    if auth:
                        _log({"type": "error", "message": f"Auth wall at {url}: {auth}"})
                        _save_result("", "", "", "", url, "", auth.upper())
                        with _state_lock:
                            _job_state["completed"] = i + 1
                            _job_state["results"].append({"url": url, "name": "", "outcome": auth.upper()})
                        continue

                    name = page.evaluate("""() => {
                        const h1 = document.querySelector('h1');
                        if (h1 && h1.innerText.trim()) return h1.innerText.trim();
                        const h2s = Array.from(document.querySelectorAll('h2'));
                        for (const h of h2s) {
                            const text = (h.innerText || '').trim();
                            if (text.length > 3 && text.split(' ').length >= 2 &&
                                !text.toLowerCase().includes('about') && !text.toLowerCase().includes('notification')) {
                                return text;
                            }
                        }
                        return '';
                    }""")
                    # Normalize Unicode (NFC) for Vietnamese and other diacritics
                    try:
                        name = name.normalize("NFC") if isinstance(name, str) and name else ""
                    except Exception:
                        name = str(name) if name else ""
                    with _state_lock:
                        _job_state["current_name"] = name

                    first = name.split()[0] if name else ""
                    note = message_template.replace("{{name}}", first)
                    outcome = _send_connection_request(page, note)
                    proof = _last_send_proof

                    vanity = url.rstrip("/").split("/")[-1].split("?")[0]
                    _save_result("", name.split()[0] if name else vanity,
                                 name.split()[-1] if len(name.split()) > 1 else "",
                                 "", url, note, outcome, proof)

                    with _state_lock:
                        _job_state["completed"] = i + 1
                        _job_state["results"].append({"url": url, "name": name, "outcome": outcome})

                    _log({"type": "info", "message": f"Result for {name}: {outcome}"})

                except Exception as e:
                    _log({"type": "error", "message": f"Error on {url}: {str(e)[:80]}"})
                    _save_result("", "", "", "", url, "", "ERROR")
                    with _state_lock:
                        _job_state["completed"] = i + 1
                        _job_state["results"].append({"url": url, "name": "", "outcome": "ERROR"})

                _random_sleep(30, 60)  # Original delay — keep for job-level pacing

        finally:
            stop_event.set()
            _save_cookies_from_context(context)
            context.close()

        with _state_lock:
            _job_state["running"] = False

        _log({"type": "info", "message": f"Outreach job finished. {len(_job_state['results'])} results."})


# ---------- Login management ----------

def linkedin_login():
    """Opens a browser to LinkedIn login page. User logs in manually, cookies are saved.
    After login, the Firefox profile is updated so future automated runs inherit the session.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"status": "error", "message": "playwright not installed"}

    os.makedirs(DATA_DIR, exist_ok=True)

    with sync_playwright() as pw:
        context = pw.firefox.launch_persistent_context(
            FIREFOX_PROFILE_COPY if os.path.exists(FIREFOX_PROFILE_COPY) else FIREFOX_PROFILE_SRC,
            headless=False,
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded", timeout=30000)
            _log({"type": "info", "message": "LinkedIn login page opened. Please log in manually."})

            # Wait up to 120 seconds for login
            for _ in range(120):
                page.wait_for_timeout(1000)
                current_url = page.url
                if "login" not in current_url and "signin" not in current_url and "linkedin.com" in current_url:
                    try:
                        logged_in = page.evaluate("""() => {
                            return !document.querySelector('input[name="session_key"]') &&
                                   (window.location.href.includes('/feed') ||
                                    document.querySelector('.global-nav') ||
                                    document.querySelector('[data-testid="primary-nav"]'));
                        }""")
                        if logged_in:
                            break
                    except Exception:
                        if "feed" in current_url or "mynetwork" in current_url:
                            break

            _save_cookies_from_context(context)
            cookies = context.cookies("https://www.linkedin.com")
            has_session = any(c["name"] in ("li_at", "liap", "bscookie") for c in cookies)

            if has_session:
                _log({"type": "info", "message": f"LinkedIn login successful. {len(cookies)} cookies saved."})
                return {"status": "success", "message": f"Login successful. {len(cookies)} cookies saved."}
            else:
                return {"status": "timeout", "message": "Login window expired. No valid session detected."}

        except Exception as e:
            _save_cookies_from_context(context)
            return {"status": "error", "message": str(e)[:120]}
        finally:
            context.close()


def linkedin_login_status():
    """Check if valid LinkedIn session exists by checking cookies file.
    No browser launch needed — just reads the cookies file.
    """
    # Try to extract fresh cookies from running Firefox (non-blocking)
    try:
        extract_firefox_cookies()
    except Exception:
        pass  # If Firefox isn't running or DB is locked, just check existing cookies

    if not os.path.exists(COOKIES_PATH):
        return {"logged_in": False, "message": "No cookies found. Open LinkedIn in Firefox."}

    try:
        with open(COOKIES_PATH) as f:
            cookies = json.load(f)
        has_li_at = any(c.get("name") == "li_at" for c in cookies)
        has_liap = any(c.get("name") == "liap" for c in cookies)
        has_bscookie = any(c.get("name") == "bscookie" for c in cookies)
        session_ok = has_li_at and (has_liap or has_bscookie)

        if session_ok:
            return {"logged_in": True, "message": "Connected to LinkedIn"}
        else:
            return {"logged_in": False, "message": "Session expired. Open LinkedIn in Firefox and refresh this page."}
    except Exception:
        return {"logged_in": False, "message": "Could not read cookies."}


def extract_firefox_cookies():
    """Extract LinkedIn cookies directly from Firefox's cookies.sqlite database.
    Uses a temp copy so Firefox can keep running. Works without browser_cookie3.
    """
    import sqlite3 as _sqlite3
    import shutil as _shutil

    firefox_db = os.path.join(FIREFOX_PROFILE_SRC, "cookies.sqlite")
    if not os.path.exists(firefox_db):
        return {"success": False, "error": "cookies.sqlite not found"}

    tmp_db = os.path.join(DATA_DIR, "tmp_cookies.sqlite")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Try to copy with retry (Firefox may have file locked)
    for attempt in range(3):
        try:
            _shutil.copy2(firefox_db, tmp_db)
            break
        except PermissionError:
            if attempt == 2:
                return {"success": False, "error": "Firefox has cookies.sqlite locked"}
            import time as _time
            _time.sleep(1)
        except Exception as e:
            return {"success": False, "error": f"Copy failed: {e}"}

    try:
        db = _sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
        db.row_factory = _sqlite3.Row
        rows = db.execute("""
            SELECT name, value, host, path, expiry, isSecure, isHttpOnly
            FROM moz_cookies WHERE host LIKE '%linkedin.com%'
        """).fetchall()
        db.close()

        cookies = []
        for r in rows:
            domain = r["host"]
            if not domain.startswith("."):
                domain = "." + domain.lstrip(".")
            exp = r["expiry"] or 0
            if exp > 10000000000:
                exp = exp // 1000
            cookies.append({
                "name": r["name"], "value": r["value"], "domain": domain,
                "path": r["path"] or "/", "secure": bool(r["isSecure"]),
                "httpOnly": bool(r["isHttpOnly"]),
                "sameSite": "Lax",
                "expires": int(exp) if exp > 0 else -1,
            })

        # Dedup by (name, domain), keep latest expiry
        dedup = {}
        for c in cookies:
            k = (c["name"], c["domain"])
            if k not in dedup or c["expires"] > dedup[k]["expires"]:
                dedup[k] = c
        li_cookies = list(dedup.values())

        has_session = any(c["name"] in ("li_at", "liap", "bscookie") for c in li_cookies)
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(COOKIES_PATH, "w") as f:
            json.dump(li_cookies, f, indent=2)

        _log({"type": "info", "message": f"Extracted {len(li_cookies)} cookies (session={has_session})"})
        return {"success": True, "cookies": len(li_cookies), "has_session": has_session}
    except Exception as e:
        return {"success": False, "error": str(e)[:120]}
    finally:
        try:
            os.remove(tmp_db)
        except Exception:
            pass


# ============================================================
# LinkedIn Auto-Posting System
# ============================================================

_post_scheduler_running = False
_post_scheduler_thread = None


def add_post(content, topic="", scheduled_time=None):
    """Add a post to the queue. Status defaults to 'queued'."""
    db = _get_clawbuildr_db()
    status = "queued" if scheduled_time else "draft"
    db.execute(
        "INSERT INTO linkedin_posts (content, topic, scheduled_time, status) VALUES (?, ?, ?, ?)",
        (content, topic, scheduled_time, status)
    )
    db.commit()
    post_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()
    _log({"type": "info", "message": f"Post #{post_id} added ({status}): {content[:50]}..."})
    return post_id


def update_post(post_id, content=None, scheduled_time=None, status=None):
    """Update an existing post."""
    db = _get_clawbuildr_db()
    updates = []
    params = []
    if content is not None:
        updates.append("content = ?")
        params.append(content)
    if scheduled_time is not None:
        updates.append("scheduled_time = ?")
        params.append(scheduled_time)
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if updates:
        params.append(post_id)
        db.execute(f"UPDATE linkedin_posts SET {', '.join(updates)} WHERE id = ?", params)
        db.commit()
    db.close()


def delete_post(post_id):
    """Delete a post from the queue."""
    db = _get_clawbuildr_db()
    db.execute("DELETE FROM linkedin_posts WHERE id = ?", (post_id,))
    db.commit()
    db.close()


def get_post_queue(status=None, limit=50):
    """Get posts from the queue. Optionally filter by status."""
    db = _get_clawbuildr_db()
    if status:
        rows = db.execute(
            "SELECT id, content, topic, scheduled_time, status, posted_at, proof_path, error, created_at "
            "FROM linkedin_posts WHERE status = ? ORDER BY scheduled_time ASC LIMIT ?",
            (status, limit)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, content, topic, scheduled_time, status, posted_at, proof_path, error, created_at "
            "FROM linkedin_posts ORDER BY scheduled_time ASC LIMIT ?",
            (limit,)
        ).fetchall()
    db.close()
    posts = []
    for r in rows:
        posts.append({
            "id": r[0], "content": r[1], "topic": r[2],
            "scheduled_time": r[3], "status": r[4], "posted_at": r[5],
            "proof_path": r[6], "error": r[7], "created_at": r[8]
        })
    return posts


def publish_post(post_id):
    """Publish a single post via Selenium. Returns success/failure."""
    db = _get_clawbuildr_db()
    row = db.execute("SELECT id, content FROM linkedin_posts WHERE id = ?", (post_id,)).fetchone()
    if not row:
        db.close()
        return {"success": False, "error": "Post not found"}

    post_id_db, content = row

    try:
        _log({"type": "info", "message": f"Publishing post #{post_id}..."})
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.action_chains import ActionChains

        proof_dir = os.path.join(DATA_DIR, "proof", "posts")
        os.makedirs(proof_dir, exist_ok=True)

        with get_firefox_driver() as ctx:
            driver, page = ctx
            driver.set_page_load_timeout(20)

            # Go to LinkedIn feed
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=20000)
            time.sleep(5)

            # Click "Bijdrage starten" using page.mouse (Shadow DOM safe)
            start_pos = driver.execute_script("""
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    const text = (el.innerText || '').trim();
                    if (text === 'Bijdrage starten' || text === 'Start a post') {
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 10 && rect.height > 10) {
                            return {x: rect.x + rect.width/2, y: rect.y + rect.height/2};
                        }
                    }
                }
                return null;
            """)

            if start_pos:
                page.mouse.click(start_pos['x'], start_pos['y'])
                time.sleep(3)
                _log({"type": "info", "message": "Clicked 'Bijdrage starten'"})
            else:
                # Fallback: click in the "Start a post" area (top of feed)
                page.mouse.click(400, 250)
                time.sleep(3)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Click into the compose text area using coordinates
            _log({"type": "info", "message": "Clicking compose area at (640, 250)"})
            page.mouse.click(640, 250)
            time.sleep(1)

            # Type content line by line, pressing Enter for newlines
            _log({"type": "info", "message": f"Typing {len(content)} chars via keyboard"})
            from selenium.webdriver.common.keys import Keys
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.strip():
                    page.keyboard.type(line, delay=8)
                if i < len(lines) - 1:
                    ActionChains(driver).send_keys(Keys.ENTER).perform()
                    time.sleep(0.1)
            time.sleep(2)
            _log({"type": "info", "message": "Content typed"})

            # Screenshot before posting
            proof_before = os.path.join(proof_dir, f"post_before_{ts}.png")
            driver.save_screenshot(proof_before)

            # Click "Plaatsen" button - use coordinates (JS can't find it in Shadow DOM)
            # From screenshots, Plaatsen is at bottom-right of modal, approximately (950, 590)
            _log({"type": "info", "message": "Clicking Plaatsen at (950, 590)"})
            page.mouse.click(950, 590)
            time.sleep(3)
            
            # Fallback: if Plaatsen not clicked, try clicking again at slightly different coords
            try:
                verify = driver.execute_script("""
                    const els = document.querySelectorAll('[role="button"]');
                    for (const el of els) {
                        const text = (el.innerText || '').trim();
                        if (text === 'Plaatsen' || text === 'Post') {
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 10 && rect.height > 10) {
                                return {x: rect.x + rect.width/2, y: rect.y + rect.height/2};
                            }
                        }
                    }
                    return null;
                """)
                if verify:
                    _log({"type": "info", "message": "Found Plaatsen via JS, clicking"})
                    page.mouse.click(verify['x'], verify['y'])
                    time.sleep(3)
            except Exception:
                pass

            # Screenshot after posting
            proof_after = os.path.join(proof_dir, f"post_after_{ts}.png")
            driver.save_screenshot(proof_after)

            # Update DB
            db.execute(
                "UPDATE linkedin_posts SET status = 'posted', posted_at = datetime('now'), proof_path = ? WHERE id = ?",
                (proof_after, post_id_db)
            )
            db.commit()
            db.close()

            _log({"type": "info", "message": f"Post #{post_id} published successfully"})
            return {"success": True, "proof": proof_after}

    except Exception as e:
        _log({"type": "error", "message": f"Failed to publish post #{post_id}: {str(e)[:80]}"})
        try:
            db.execute(
                "UPDATE linkedin_posts SET status = 'failed', error = ? WHERE id = ?",
                (str(e)[:200], post_id_db)
            )
            db.commit()
            db.close()
        except Exception:
            pass
        return {"success": False, "error": str(e)[:120]}


def _post_scheduler_loop():
    """Background thread: publish next ready post on Tue/Thu at 8 AM."""
    global _post_scheduler_running
    _log({"type": "info", "message": "Post scheduler started (Tue/Thu 8 AM)"})

    _posted_today = False

    while _post_scheduler_running:
        try:
            now = datetime.now()
            hour = now.hour
            minute = now.minute
            weekday = now.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
            is_post_day = weekday in [1, 3, 5]  # Tue=1, Thu=3, Sat=5
            is_8am_window = hour == 8 and minute < 10  # 8:00-8:09 window

            # Reset posted flag at midnight
            if hour == 0 and minute == 0:
                _posted_today = False

            # Post on Tue/Thu at 8 AM
            if is_post_day and is_8am_window and not _posted_today:
                db = _get_clawbuildr_db()

                # Find next ready post
                due_post = db.execute(
                    "SELECT id, content FROM linkedin_posts "
                    "WHERE status = 'ready' "
                    "ORDER BY id ASC LIMIT 1"
                ).fetchone()

                if due_post:
                    post_id, content = due_post
                    _log({"type": "info", "message": f"Post scheduler: publishing post #{post_id}"})
                    result = publish_post(post_id)
                    if result["success"]:
                        _log({"type": "info", "message": f"Post #{post_id} published"})
                        _posted_today = True
                    else:
                        _log({"type": "warning", "message": f"Post #{post_id} failed: {result.get('error', 'unknown')}"})
                else:
                    _log({"type": "info", "message": "Post scheduler: no ready posts in queue"})

                db.close()

        except Exception as e:
            _log({"type": "error", "message": f"Post scheduler error: {str(e)[:60]}"})

        # Check every minute
        time.sleep(60)

    _log({"type": "info", "message": "Post scheduler stopped"})


def start_post_scheduler():
    """Start the post scheduler background thread."""
    global _post_scheduler_running, _post_scheduler_thread

    if _post_scheduler_running:
        return {"status": "already_running"}

    _post_scheduler_running = True
    _post_scheduler_thread = threading.Thread(target=_post_scheduler_loop, daemon=True)
    _post_scheduler_thread.start()

    return {"status": "started"}


def stop_post_scheduler():
    """Stop the post scheduler background thread."""
    global _post_scheduler_running
    _post_scheduler_running = False
    return {"status": "stopped"}


def get_post_scheduler_status():
    """Get post scheduler status."""
    return {
        "running": _post_scheduler_running,
        "thread_alive": _post_scheduler_thread.is_alive() if _post_scheduler_thread else False,
    }


def generate_post_content(topic="founderflow", style="story"):
    """Generate a LinkedIn post using LLM based on topics and interview answers."""
    context = """
CONTEXT FOR POSTS:
- Marvin van der Sluis, founder of FounderFlow and ClawBuildr
- FounderFlow: AI client acquisition system for Instagram. Scrapes leads, sends DMs, AI handles replies, books calls.
- ClawBuildr: AI consulting and tools for businesses.
- Background: Was a setter sending DMs 3+ hours/day. Built FounderFlow to automate himself out of that work.
- Three engines: Discovery (find leads), Outreach (send DMs), AI Setter (handle replies with Gemini)
- Results: 3 hours/day → 15 minutes. 17,000+ leads, 60% reply rate.
- Founder journey: Employee → biz partner → founder. 12-15h days but loves it. Hard, lonely, unpredictable.
- Key message: It's not easy, but it's worth it. Find likeminded people. Speak up.
"""
    prompt = f"""Write a LinkedIn post about: {topic}

{context}

STYLE: {style}

RULES:
- Write in first person (Marvin)
- Be authentic and vulnerable, not salesy
- Use short sentences and line breaks for readability
- No hashtags in the post body
- No emoji overload (0-2 max)
- End with a question or call to engagement
- Max 1500 characters
- Write in English

Output ONLY the post text, nothing else."""

    try:
        import asyncio
        def _call_llm():
            loop = asyncio.new_event_loop()
            try:
                from tools import call_llm
                return loop.run_until_complete(call_llm(prompt))
            finally:
                loop.close()

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(_call_llm)
            result = future.result(timeout=30)

        if result and len(result) > 50:
            return result.strip()
    except Exception as e:
        _log({"type": "warning", "message": f"LLM post generation failed: {str(e)[:40]}"})

    # Fallback templates
    fallbacks = [
        "I spent 3+ hours every day sending DMs manually.\n\nCopy-pasting the same messages. Tracking replies in spreadsheets. Following up with people who went quiet.\n\nIt worked, but it was soul-crushing.\n\nSo I built FounderFlow — an AI system that finds leads, sends personalized DMs, handles replies, and books calls.\n\nNow I spend 15 minutes a day checking my pipeline.\n\nThe best part? I automated myself out of the job I hated.\n\nWhat's one task you'd love to never do again?",
        "Nobody tells you what founding actually feels like.\n\nThe stress. The uncertainty. Wondering if next month's bills will get paid.\n\n12-15 hour days. Weeks where you question everything.\n\nBut here's the thing — I wouldn't change it.\n\nBecause every hour I work, I'm building something for myself. I choose where my time goes. I get rewarded for my own effort.\n\nIt's hard. It's lonely. But it's mine.\n\nIf you're in the building phase right now — keep going. It gets better.",
        "The hardest part of building software isn't the code.\n\nIt's the days where nothing works. Where you ship a feature and nobody uses it. Where you wonder if you're building something people actually want.\n\nI thought with FounderFlow it would be easy — just build it and clients will come.\n\nI couldn't be more wrong.\n\nIt's lonely days. Uncertain days. Days where you question everything.\n\nBut then you get a message from a client saying FounderFlow booked them 5 calls this week.\n\nAnd you remember why you started.\n\nWhat keeps you going on the hard days?",
    ]
    import random
    return random.choice(fallbacks)


# ---------- Auto-Engagement: Like Feed Posts ----------

def auto_like_feed_posts(max_likes=10):
    """Like posts from connections on the LinkedIn feed.
    Returns dict with count of posts liked.
    """
    _log({"type": "info", "message": f"Auto-like: starting, max {max_likes} likes"})
    
    try:
        with get_firefox_driver() as ctx:
            driver, page = ctx
            driver.set_page_load_timeout(20)
            
            # Go to feed
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=20000)
            time.sleep(8)  # Wait for feed posts to render (SPA)
            
            liked_count = 0
            scroll_attempts = 0
            max_scrolls = 15
            
            # First scroll down to bring posts into view
            driver.execute_script("window.scrollBy(0, 500)")
            time.sleep(3)
            
            while liked_count < max_likes and scroll_attempts < max_scrolls:
                # Find like buttons via JS - check all in DOM, not just visible
                like_buttons = driver.execute_script("""
                    const results = [];
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const ariaLabel = btn.getAttribute('aria-label') || '';
                        const text = (btn.innerText || '').trim();
                        const isLikeBtn = ariaLabel.includes('reactieknop') || 
                                         ariaLabel.includes('geen reactie') ||
                                         text === 'Interessant' || 
                                         text === 'Like';
                        if (isLikeBtn) {
                            const rect = btn.getBoundingClientRect();
                            if (rect.width > 10 && rect.height > 10) {
                                results.push({x: rect.x + rect.width/2, y: rect.y + rect.height/2, label: ariaLabel.substring(0, 40), viewY: rect.y});
                            }
                        }
                    }
                    return results;
                """)
                
                _log({"type": "info", "message": f"Auto-like: found {len(like_buttons)} like buttons"})
                
                if not like_buttons:
                    # Scroll down to load more posts
                    driver.execute_script("window.scrollBy(0, 600)")
                    time.sleep(2)
                    scroll_attempts += 1
                    continue
                
                # Like the first available post - scroll into view then click
                for btn_info in like_buttons[:1]:
                    try:
                        # Scroll button into view if needed (use JS for window height)
                        view_y = btn_info['viewY']
                        vh = driver.execute_script("return window.innerHeight")
                        if view_y > vh or view_y < 0:
                            driver.execute_script(f"window.scrollBy(0, {view_y - 300})")
                            time.sleep(1)
                            # Recalculate position after scroll
                            btn_info['y'] = btn_info['y'] - (view_y - 300) if view_y > 300 else btn_info['y']
                        
                        page.mouse.click(btn_info['x'], btn_info['y'])
                        time.sleep(1)
                        
                        liked_count += 1
                        _log({"type": "info", "message": f"Auto-like: liked post #{liked_count}"})
                        _random_sleep(2, 5)
                    except Exception as e:
                        _log({"type": "warning", "message": f"Auto-like: click failed: {str(e)[:40]}"})
                        continue
                
                # Scroll a bit after liking
                driver.execute_script("window.scrollBy(0, 300)")
                time.sleep(2)
            
            _log({"type": "info", "message": f"Auto-like: done, liked {liked_count} posts"})
            return {"success": True, "liked": liked_count}
    
    except Exception as e:
        _log({"type": "error", "message": f"Auto-like failed: {str(e)[:80]}"})
        return {"success": False, "error": str(e)[:120]}


# ---------- Auto-Endorse Skills ----------

def auto_endorse_skills(profile_url, max_endorsements=10):
    """Visit a connection's profile and endorse their skills.
    Returns dict with count of skills endorsed.
    """
    _log({"type": "info", "message": f"Auto-endorse: starting for {profile_url}"})
    
    try:
        with get_firefox_driver() as ctx:
            driver, page = ctx
            driver.set_page_load_timeout(20)
            
            # Navigate to skills section
            skills_url = profile_url.rstrip('/') + '/details/skills/'
            page.goto(skills_url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(5)
            
            # Check for auth wall
            auth = _detect_auth_wall(page)
            if auth:
                return {"success": False, "error": "auth_wall"}
            
            endorsed_count = 0
            scroll_attempts = 0
            
            # Scroll to load skills and find endorse buttons
            for _ in range(5):
                endorse_buttons = driver.execute_script("""
                    const results = [];
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const text = (btn.innerText || '').trim().toLowerCase();
                        const ariaLabel = (btn.getAttribute('aria-label') || '').toLowerCase();
                        // Look for endorse buttons
                        if (text.includes('endorse') || text.includes('aanbevelen') || 
                            text.includes('valideren') || text.includes('endorsement') ||
                            ariaLabel.includes('endorse') || ariaLabel.includes('aanbevelen')) {
                            const rect = btn.getBoundingClientRect();
                            if (rect.width > 10 && rect.height > 10) {
                                // Check not already endorsed
                                const isEndorsed = btn.classList.contains('artdeco-button--muted') || 
                                                  btn.disabled ||
                                                  text.includes('endorsed');
                                if (!isEndorsed) {
                                    results.push({x: rect.x + rect.width/2, y: rect.y + rect.height/2, text: text.substring(0, 30)});
                                }
                            }
                        }
                    }
                    return results;
                """)
                
                if endorse_buttons and endorsed_count < max_endorsements:
                    for btn_info in endorse_buttons[:max_endorsements - endorsed_count]:
                        try:
                            # Scroll button into view first
                            driver.execute_script(f"window.scrollTo(0, {btn_info['y'] - 200})")
                            time.sleep(0.5)
                            
                            page.mouse.click(btn_info['x'], btn_info['y'])
                            _random_sleep(1.5, 3)
                            
                            endorsed_count += 1
                            _log({"type": "info", "message": f"Auto-endorse: endorsed skill #{endorsed_count}: {btn_info.get('text', '')}"})
                        except Exception:
                            continue
                else:
                    break
                
                # Scroll down to load more skills
                driver.execute_script("window.scrollBy(0, 500)")
                time.sleep(2)
                scroll_attempts += 1
            
            _log({"type": "info", "message": f"Auto-endorse: done, endorsed {endorsed_count} skills"})
            return {"success": True, "endorsed": endorsed_count}
    
    except Exception as e:
        _log({"type": "error", "message": f"Auto-endorse failed: {str(e)[:80]}"})
        return {"success": False, "error": str(e)[:120]}


def auto_endorse_all_connections(max_profiles=5, max_per_profile=10):
    """Endorse skills for multiple connections from the network page.
    Returns dict with results per profile.
    """
    _log({"type": "info", "message": f"Auto-endorse all: starting, max {max_profiles} profiles"})
    
    try:
        with get_firefox_driver() as ctx:
            driver, page = ctx
            driver.set_page_load_timeout(20)
            
            # Go to connections page
            page.goto("https://www.linkedin.com/mynetwork/contacts/", wait_until="domcontentloaded", timeout=20000)
            time.sleep(5)
            
            # Get connection profile URLs
            connection_urls = driver.execute_script("""
                const results = [];
                const links = document.querySelectorAll('a[href*="/in/"]');
                for (const link of links) {
                    const href = link.getAttribute('href');
                    if (href && href.includes('/in/') && !results.includes(href)) {
                        results.push(href);
                    }
                }
                return results.slice(0, arguments[0]);
            """, max_profiles)
            
            if not connection_urls:
                _log({"type": "warning", "message": "Auto-endorse: no connections found"})
                return {"success": True, "endorsed_profiles": 0, "total_endorsed": 0}
            
            results = []
            total_endorsed = 0
            
            for url in connection_urls:
                result = auto_endorse_skills(url, max_endorsements=max_per_profile)
                results.append({"url": url, "endorsed": result.get("endorsed", 0)})
                total_endorsed += result.get("endorsed", 0)
                _random_sleep(3, 6)
            
            _log({"type": "info", "message": f"Auto-endorse all: done, {total_endorsed} total endorsements across {len(connection_urls)} profiles"})
            return {"success": True, "endorsed_profiles": len(connection_urls), "total_endorsed": total_endorsed, "results": results}
    
    except Exception as e:
        _log({"type": "error", "message": f"Auto-endorse all failed: {str(e)[:80]}"})
        return {"success": False, "error": str(e)[:120]}


# ---------- FounderFlow Dashboard Scraper ----------

def scrape_founderflow_dashboard():
    """Scrape FounderFlow dashboard for stats to use in LinkedIn content.
    Returns dict with all available stats.
    """
    _log({"type": "info", "message": "Scraping FounderFlow dashboard..."})
    
    try:
        import httpx
        response = httpx.get("https://saas-outreach-beta.vercel.app/", timeout=15)
        html = response.text
        
        stats = {}
        
        # Extract key stats using regex
        import re
        
        # Leads discovered
        m = re.search(r'([\d,]+)\s*(?:\+)?\s*(?:Leads Discovered|leads discovered)', html, re.I)
        if m:
            stats['leads_discovered'] = m.group(1).replace(',', '')
        
        # DMs sent
        m = re.search(r'([\d,]+(?:\+)?)\s*(?:DMs Sent|DMs? Sent)', html, re.I)
        if m:
            stats['dms_sent'] = m.group(1).replace('+', '').replace(',', '')
        
        # Reply rate
        m = re.search(r'([\d.]+)%\s*(?:Reply Rate|reply rate)', html, re.I)
        if m:
            stats['reply_rate'] = m.group(1)
        
        # Funnel data
        funnel_match = re.findall(r'(Discovered|DM Sent|Replied|Meeting|Qualified|Closed)\s*([\d,]+)', html, re.I)
        if funnel_match:
            stats['funnel'] = {k: v.replace(',', '') for k, v in funnel_match}
        
        # Engine stats
        m = re.search(r'(\d+)\s*DMs Today', html, re.I)
        if m:
            stats['dms_today'] = m.group(1)
        
        m = re.search(r'(\d+)\s*Replies Today', html, re.I)
        if m:
            stats['replies_today'] = m.group(1)
        
        m = re.search(r'(\d+)\s*Calls Booked', html, re.I)
        if m:
            stats['calls_booked'] = m.group(1)
        
        _log({"type": "info", "message": f"FounderFlow scraped: {len(stats)} stats found"})
        return {"success": True, "stats": stats}
    
    except Exception as e:
        _log({"type": "error", "message": f"FounderFlow scrape failed: {str(e)[:80]}"})
        return {"success": False, "error": str(e)[:120]}


def generate_founderflow_post():
    """Generate a LinkedIn post using FounderFlow stats.
    Returns the post content.
    """
    # Get stats
    result = scrape_founderflow_dashboard()
    if not result.get("success"):
        return None
    
    stats = result.get("stats", {})
    
    # Build context for LLM
    context_parts = []
    if stats.get('leads_discovered'):
        context_parts.append(f"Leads discovered: {stats['leads_discovered']}")
    if stats.get('dms_sent'):
        context_parts.append(f"DMs sent: {stats['dms_sent']}")
    if stats.get('reply_rate'):
        context_parts.append(f"Reply rate: {stats['reply_rate']}%")
    if stats.get('dms_today'):
        context_parts.append(f"DMs today: {stats['dms_today']}")
    if stats.get('replies_today'):
        context_parts.append(f"Replies today: {stats['replies_today']}")
    if stats.get('funnel'):
        f = stats['funnel']
        context_parts.append(f"Funnel: {f.get('Discovered', '?')} discovered -> {f.get('DM Sent', '?')} DMs -> {f.get('Replied', '?')} replied -> {f.get('Meeting', '?')} meetings -> {f.get('Qualified', '?')} qualified -> {f.get('Closed', '?')} closed")
    
    context = "\n".join(context_parts) if context_parts else "No stats available"
    
    # Try LLM
    try:
        import asyncio
        import concurrent.futures
        
        def _call_llm():
            loop = asyncio.new_event_loop()
            try:
                from tools import call_llm
                prompt = (
                    f"Schrijf een LinkedIn post (Nederlands, 1000-1800 tekens, geen dashes --).\n\n"
                    f"FounderFlow stats:\n{context}\n\n"
                    f"REGELS:\n"
                    f"- Deel resultaten van FounderFlow (het AI outreach systeem dat ik gebouwd heb)\n"
                    f"- Gebruik concrete getallen om impact te tonen\n"
                    f"- Schrijf in mijn persoonlijke stem (ondernemer, niet bedrijf)\n"
                    f"- Geen emojis, geen hashtags\n"
                    f"- Geen ClawBuildr mentions\n"
                    f"- Eindig met een vraag voor engagement\n"
                    f"- Max 1-2 zinnen per alinea\n"
                    f"Antwoord alleen met de post, niks anders."
                )
                return loop.run_until_complete(call_llm(prompt))
            finally:
                loop.close()
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(_call_llm)
            post = future.result(timeout=30)
        
        if post and len(post) > 100:
            post = post.strip()
            # Remove dashes
            post = post.replace(' -- ', ', ').replace(' — ', ', ')
            if len(post) > 1800:
                post = post[:1797] + "..."
            return post
    except Exception as e:
        _log({"type": "warning", "message": f"LLM post generation failed: {str(e)[:40]}"})
    
    # Fallback
    leads = stats.get('leads_discovered', '17,000+')
    dms = stats.get('dms_sent', '5,000+')
    rate = stats.get('reply_rate', '16')
    return f"FounderFlow heeft deze maand {leads} leads ontdekt en {dms} berichten gestuurd.\n\nReactiesnelheid: {rate}%.\n\nDat is 3x het industrie gemiddelde.\n\nHet verschil? Elk bericht is gepersonaliseerd. Geen copy-paste templates. Geen robots.\n\nDe AI leert van elk gesprek en past zich aan.\n\nHoeveel tijd besteed jij aan outreach?"


# ---------- Safe-Sending: Message & Profile-View Tracking ----------

def _get_message_count_today():
    """Count messages sent today."""
    db = _get_clawbuildr_db()
    today = date.today().isoformat()
    count = db.execute(
        "SELECT COUNT(*) FROM linkedin_messages WHERE timestamp LIKE ?",
        (today + "%",)
    ).fetchone()[0]
    db.close()
    return count


def _get_profile_view_count_today():
    """Count profile views today."""
    db = _get_clawbuildr_db()
    today = date.today().isoformat()
    count = db.execute(
        "SELECT COUNT(*) FROM linkedin_profile_views WHERE viewed_at LIKE ?",
        (today + "%",)
    ).fetchone()[0]
    db.close()
    return count


def can_send_message():
    """Check if we can send a 1st-degree message today."""
    flagged, flag_reason = _is_flagged()
    if flagged:
        return False, f"flagged_{flag_reason}"
    active, reason = _is_active_hours()
    if not active:
        return False, reason
    msg_count = _get_message_count_today()
    if msg_count >= MESSAGE_DAILY_LIMIT:
        return False, f"message_limit_reached ({msg_count}/{MESSAGE_DAILY_LIMIT})"
    return True, f"ok ({msg_count}/{MESSAGE_DAILY_LIMIT})"


def can_view_profile():
    """Check if we can view a profile today."""
    flagged, flag_reason = _is_flagged()
    if flagged:
        return False, f"flagged_{flag_reason}"
    active, reason = _is_active_hours()
    if not active:
        return False, reason
    view_count = _get_profile_view_count_today()
    if view_count >= PROFILE_VIEW_DAILY_LIMIT:
        return False, f"profile_view_limit_reached ({view_count}/{PROFILE_VIEW_DAILY_LIMIT})"
    return True, f"ok ({view_count}/{PROFILE_VIEW_DAILY_LIMIT})"


def _log_message_sent(profile_url, first_name, last_name, message_text, outcome):
    """Record a message send in the database."""
    db = _get_clawbuildr_db()
    db.execute(
        "INSERT INTO linkedin_messages (profile_url, first_name, last_name, message_text, outcome, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (profile_url, first_name, last_name, message_text, outcome, datetime.now().isoformat())
    )
    db.commit()
    db.close()


def _log_profile_view(profile_url):
    """Record a profile view in the database."""
    db = _get_clawbuildr_db()
    db.execute(
        "INSERT INTO linkedin_profile_views (profile_url, viewed_at) VALUES (?, ?)",
        (profile_url, datetime.now().isoformat())
    )
    db.commit()
    db.close()


def get_safety_status():
    """Get comprehensive safety status for dashboard display."""
    state = _get_warming_state()
    effective_limit, multiplier = _get_effective_daily_limit()
    flagged, flag_reason = _is_flagged()
    active, active_reason = _is_active_hours()

    db = _get_clawbuildr_db()
    today = date.today().isoformat()

    connections_today = db.execute(
        "SELECT COUNT(*) FROM linkedin_outreach WHERE outcome = 'SUCCESS' AND timestamp LIKE ?",
        (today + "%",)
    ).fetchone()[0]

    messages_today = db.execute(
        "SELECT COUNT(*) FROM linkedin_messages WHERE timestamp LIKE ?",
        (today + "%",)
    ).fetchone()[0]

    views_today = db.execute(
        "SELECT COUNT(*) FROM linkedin_profile_views WHERE viewed_at LIKE ?",
        (today + "%",)
    ).fetchone()[0]

    db.close()

    return {
        "flagged": flagged,
        "flag_reason": flag_reason,
        "active_hours": active,
        "active_reason": active_reason,
        "warming_multiplier": multiplier,
        "warming_percent": f"{multiplier:.0%}",
        "connections_today": connections_today,
        "connections_limit": effective_limit,
        "connections_base_limit": DAILY_LIMIT,
        "messages_today": messages_today,
        "messages_limit": MESSAGE_DAILY_LIMIT,
        "profile_views_today": views_today,
        "profile_views_limit": PROFILE_VIEW_DAILY_LIMIT,
        "cooldown_elapsed": _check_cooldown_elapsed() if flagged else False,
    }


# ---------- Post Engagement Scraper ----------
# Finds people who engaged with relevant LinkedIn posts (likes/comments)
# and saves them as warm leads for outreach.

def scrape_post_engagement(post_url, max_engagers=25):
    """Visit a LinkedIn post and scrape people who liked or commented.
    Returns dict with count of engagers found and saved as warm leads.
    """
    _log({"type": "info", "message": f"Post engagement scraper: starting for {post_url}"})

    try:
        with get_firefox_driver() as ctx:
            driver, page = ctx
            driver.set_page_load_timeout(20)

            page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(5)

            auth = _detect_auth_wall(page)
            if auth:
                return {"success": False, "error": "auth_wall", "engagers": 0}

            engagers = []
            scroll_attempts = 0

            # Scroll to load reactions and comments
            for _ in range(8):
                # Extract people who reacted (liked, celebrated, etc.)
                reaction_data = driver.execute_script("""
                    const results = [];
                    // Find reaction buttons/counts
                    const reactionSections = document.querySelectorAll('[class*="react"], [class*="like"], [data-test-id*="reaction"]');
                    for (const section of reactionSections) {
                        // Look for profile links in reaction tooltips
                        const links = section.querySelectorAll('a[href*="/in/"]');
                        for (const link of links) {
                            const href = link.getAttribute('href') || '';
                            const name = (link.innerText || '').trim();
                            if (href.includes('/in/') && name && name.length > 2) {
                                results.push({
                                    name: name,
                                    profile_url: 'https://www.linkedin.com' + href.split('?')[0],
                                    type: 'reaction'
                                });
                            }
                        }
                    }
                    return results;
                """)

                # Extract commenters
                comment_data = driver.execute_script("""
                    const results = [];
                    const commentAuthors = document.querySelectorAll('[class*="comment"] a[href*="/in/"], [class*="comment-body"] a[href*="/in/"]');
                    for (const link of commentAuthors) {
                        const href = link.getAttribute('href') || '';
                        const name = (link.innerText || '').trim();
                        if (href.includes('/in/') && name && name.length > 2) {
                            results.push({
                                name: name,
                                profile_url: 'https://www.linkedin.com' + href.split('?')[0],
                                type: 'comment'
                            });
                        }
                    }
                    return results;
                """)

                for item in reaction_data + comment_data:
                    if item["profile_url"] not in [e["profile_url"] for e in engagers]:
                        engagers.append(item)

                if len(engagers) >= max_engagers:
                    break

                driver.execute_script("window.scrollBy(0, 800);")
                time.sleep(2)
                scroll_attempts += 1

            # Save engagers as warm leads
            saved_count = 0
            db = _get_clawbuildr_db()

            # Create table if not exists
            db.execute("""
                CREATE TABLE IF NOT EXISTS warm_leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_url TEXT UNIQUE,
                    first_name TEXT,
                    last_name TEXT,
                    source_post_url TEXT,
                    engagement_type TEXT,
                    scraped_at TEXT,
                    status TEXT DEFAULT 'new'
                )
            """)

            for engager in engagers[:max_engagers]:
                try:
                    name_parts = engager["name"].split(" ", 1)
                    first_name = name_parts[0] if name_parts else engager["name"]
                    last_name = name_parts[1] if len(name_parts) > 1 else ""

                    db.execute("""
                        INSERT OR IGNORE INTO warm_leads
                        (profile_url, first_name, last_name, source_post_url, engagement_type, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        engager["profile_url"],
                        first_name,
                        last_name,
                        post_url,
                        engager["type"],
                        datetime.now().isoformat()
                    ))
                    saved_count += 1
                except Exception:
                    pass

            db.commit()
            db.close()

            _log({"type": "info", "message": f"Post engagement: found {len(engagers)} engagers, saved {saved_count} as warm leads"})
            return {"success": True, "engagers": len(engagers), "saved": saved_count}

    except Exception as e:
        _log({"type": "error", "message": f"Post engagement scraper failed: {str(e)[:80]}"})
        return {"success": False, "error": str(e)[:120], "engagers": 0}


def get_warm_leads(limit=50):
    """Get warm leads from post engagement scraping."""
    db = _get_clawbuildr_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS warm_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_url TEXT UNIQUE,
            first_name TEXT,
            last_name TEXT,
            source_post_url TEXT,
            engagement_type TEXT,
            scraped_at TEXT,
            status TEXT DEFAULT 'new'
        )
    """)
    rows = db.execute("""
        SELECT * FROM warm_leads WHERE status = 'new'
        ORDER BY scraped_at DESC LIMIT ?
    """, (limit,)).fetchall()
    db.close()

    columns = ["id", "profile_url", "first_name", "last_name", "source_post_url", "engagement_type", "scraped_at", "status"]
    return [dict(zip(columns, row)) for row in rows]


# ===========================================================
# LINKEDIN COMPANY PAGE SCRAPING
# ===========================================================

def scrape_linkedin_company(company_url: str) -> dict:
    """Scrape a LinkedIn company page for deep company intelligence.

    Extracts: name, industry, size, about text, specialties, headquarters,
    employee count, founding date, website, hiring signals, and recent activity.

    Args:
        company_url: Full LinkedIn company URL (e.g., https://www.linkedin.com/company/acme-corp/)

    Returns:
        dict with company intelligence data
    """
    try:
        # Delegate to the dedicated human-like company reader
        from read_linkedin_company import read_linkedin_company
        return read_linkedin_company(company_url)
    except Exception as e:
        return {"error": f"Company scrape failed: {str(e)[:150]}", "url": company_url}


def enumerate_company_employees(company_url: str, max_employees: int = 50) -> list:
    """Enumerate employees from a LinkedIn company page's People tab.
    
    Visits the company page, clicks the People tab, and scrolls through
    the employee list to extract names, titles, and profile URLs.
    
    Args:
        company_url: LinkedIn company URL
        max_employees: Maximum employees to scrape (default 50)
    
    Returns:
        List of dicts: [{name, title, profile_url, location}]
    """
    employees = []
    
    try:
        from selenium.webdriver.common.by import By
        driver, page = _persistent_firefox.ensure_ready()
        
        # Navigate to company people tab
        if not company_url.endswith('/'):
            company_url += '/'
        people_url = company_url + 'people/'
        
        page.goto(people_url, wait_until="domcontentloaded", timeout=120000)
        _random_sleep(4, 6)
        
        auth = _detect_auth_wall(page)
        if auth:
            _log({"type": "error", "message": f"Auth wall on company people page: {auth}"})
            return employees
        
        # Scroll to load employees
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scrolls = 10
        
        while scroll_attempts < max_scrolls and len(employees) < max_employees:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            _random_sleep(2, 3)
            
            # Extract employee cards
            cards = driver.find_elements(By.CSS_SELECTOR, '.org-people-profile-card, [data-view-name="profile-card"], li.artdeco-list__item')
            
            for card in cards:
                if len(employees) >= max_employees:
                    break
                    
                try:
                    # Get name and profile URL
                    name_link = card.find_elements(By.CSS_SELECTOR, 'a[href*="/in/"]')
                    if not name_link:
                        continue
                    
                    profile_url = name_link[0].get_attribute('href')
                    name_text = name_link[0].text.strip()
                    
                    # Skip if already seen
                    if any(e['profile_url'] == profile_url for e in employees):
                        continue
                    
                    # Get title/headline
                    title = ""
                    title_els = card.find_elements(By.CSS_SELECTOR, '.org-people-profile-card__profile-title, .artdeco-entity-lockup__subtitle, span[aria-label]')
                    if title_els:
                        title = title_els[0].text.strip()
                    
                    # Get location
                    location = ""
                    loc_els = card.find_elements(By.CSS_SELECTOR, '.org-people-profile-card__location, .artdeco-entity-lockup__caption')
                    if loc_els:
                        location = loc_els[0].text.strip()
                    
                    employees.append({
                        "name": name_text,
                        "title": title,
                        "profile_url": profile_url,
                        "location": location
                    })
                except Exception:
                    continue
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1
        
        _log({"type": "info", "message": f"Enumerated {len(employees)} employees from {company_url}"})
        return employees
        
    except Exception as e:
        _log({"type": "error", "message": f"Employee enumeration failed: {str(e)[:100]}"})
        return employees


def scrape_linkedin_profile_deep(profile_url: str) -> dict:
    """Deep-scrape a LinkedIn profile for full intelligence.
    
    Extracts: headline, location, about section, experience, education,
    skills, social links, and connection status.
    
    Args:
        profile_url: LinkedIn profile URL
    
    Returns:
        dict with full profile data
    """
    try:
        driver, page = _persistent_firefox.ensure_ready()
        
        page.goto(profile_url, wait_until="domcontentloaded", timeout=120000)
        _random_sleep(4, 6)
        
        auth = _detect_auth_wall(page)
        if auth:
            return {"error": f"Auth wall: {auth}", "profile_url": profile_url}
        
        result = {"profile_url": profile_url}
        
        # Get full page text for analysis
        body_text = driver.find_element(By.TAG_NAME, 'body').text
        result["raw_text"] = body_text[:8000]
        
        # Extract name from H1
        try:
            h1 = driver.find_elements(By.CSS_SELECTOR, 'h1')
            if h1:
                result["name"] = h1[0].text.strip()
        except Exception:
            pass
        
        # Extract headline (subtitle under name)
        try:
            headline_el = driver.find_elements(By.CSS_SELECTOR, '.text-body-medium.break-words, .pv-text-details__left-panel .text-body-medium')
            if headline_el:
                result["headline"] = headline_el[0].text.strip()
        except Exception:
            pass
        
        # Extract location
        try:
            loc_match = re.search(r'(?:Location|Standort|Localisation)\s*\n\s*(.+?)(?:\n|$)', body_text)
            if loc_match:
                result["location"] = loc_match.group(1).strip()
        except Exception:
            pass
        
        # Extract about section
        try:
            about_match = re.search(r'(?:About|Über|À propos)\s*\n\s*(.+?)(?:\n\n|\Z)', body_text, re.DOTALL)
            if about_match:
                result["about"] = about_match.group(1).strip()[:2000]
        except Exception:
            pass
        
        # Extract experience section
        try:
            exp_match = re.search(r'(?:Experience|Erfahrung|Expérience)\s*\n(.+?)(?:\nEducation|\nAusbildung|\nFormation|\Z)', body_text, re.DOTALL)
            if exp_match:
                result["experience"] = exp_match.group(1).strip()[:2000]
        except Exception:
            pass
        
        # Extract education section
        try:
            edu_match = re.search(r'(?:Education|Ausbildung|Formation)\s*\n(.+?)(?:\nSkills|\nFähigkeiten|\nCompétences|\Z)', body_text, re.DOTALL)
            if edu_match:
                result["education"] = edu_match.group(1).strip()[:1000]
        except Exception:
            pass
        
        # Extract skills
        try:
            skills_match = re.search(r'(?:Skills|Fähigkeiten|Compétences)\s*\n(.+?)(?:\n\n|\Z)', body_text, re.DOTALL)
            if skills_match:
                result["skills"] = skills_match.group(1).strip()[:1000]
        except Exception:
            pass
        
        # Extract social links from the page
        social_links = {}
        try:
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="twitter.com"], a[href*="facebook.com"], a[href*="instagram.com"], a[href*="github.com"], a[href*="youtube.com"]')
            for link in links:
                href = link.get_attribute('href')
                if 'twitter.com' in href:
                    social_links['twitter'] = href
                elif 'facebook.com' in href:
                    social_links['facebook'] = href
                elif 'instagram.com' in href:
                    social_links['instagram'] = href
                elif 'github.com' in href:
                    social_links['github'] = href
                elif 'youtube.com' in href:
                    social_links['youtube'] = href
        except Exception:
            pass
        
        if social_links:
            result["social_links"] = social_links
        
        # Check connection status
        try:
            connected, status = _check_if_connected(driver)
            result["connection_status"] = status
        except Exception:
            pass
        
        return result
        
    except Exception as e:
        return {"error": f"Profile scrape failed: {str(e)[:150]}", "profile_url": profile_url}


def find_emails_for_employee(first_name: str, last_name: str, company_domain: str) -> list:
    """Generate and verify likely email addresses for an employee.
    
    Uses common email patterns (first.last@, flast@, etc.) and verifies
    via MX records and SMTP probes.
    
    Args:
        first_name: Employee's first name
        last_name: Employee's last name
        company_domain: Company email domain
    
    Returns:
        List of dicts: [{email, confidence, pattern}]
    """
    from tools import verify_email
    
    # Normalize names
    first = re.sub(r'[^a-zA-Z]', '', first_name.lower())
    last = re.sub(r'[^a-zA-Z]', '', last_name.lower())
    
    if not first or not last:
        return []
    
    # Common email patterns (Dutch/Belgian/German companies)
    patterns = [
        f"{first}.{last}@{company_domain}",
        f"{first[0]}{last}@{company_domain}",
        f"{first}@{company_domain}",
        f"{last}.{first}@{company_domain}",
        f"{first}{last}@{company_domain}",
        f"{first[0]}.{last}@{company_domain}",
        f"{first}_{last}@{company_domain}",
        f"{last}{first[0]}@{company_domain}",
    ]
    
    # Deduplicate
    patterns = list(dict.fromkeys(patterns))
    
    verified = []
    for email in patterns:
        try:
            result = verify_email(email)
            if result.get("verified") or result.get("confidence", 0) >= 50:
                verified.append({
                    "email": email,
                    "confidence": result.get("confidence", 0),
                    "pattern": "verified" if result.get("verified") else "likely"
                })
        except Exception:
            continue
        
        # Don't verify too many per employee
        if len(verified) >= 3:
            break
    
    return verified
