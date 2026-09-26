import httpx
import sqlite3
import json
import os
import re
import smtplib
import hashlib
import time
import logging
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("ClawBuildr")


def fix_text_encoding(text: str) -> str:
    """Fix common mojibake from Ollama responses on Windows.
    Tries to recover UTF-8 sequences that were decoded as CP437.
    """
    if not text:
        return text
    try:
        # Common CP437<->UTF-8 mojibake fix: encode as cp437, decode as utf-8
        fixed = text.encode("cp437").decode("utf-8")
        return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If cp437 encoding fails, fall back to specific replacements
        replacements = {
            "\u251c\u00bd": "ë",  # ├½ -> ë
            "\u251c\u00bc": "ü",  # ├╝ -> ü
            "\u251c\u00a9": "é",  # ├й -> é (approximation)
            "\u251c\u00a8": "è",  # ├и -> è (approximation)
        }
        for bad, good in replacements.items():
            text = text.replace(bad, good)
        return text

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
# Active dashboard DB lives in MultiAgentFunnel/data (parent of clawbuildr/)
CLAWBUILDR_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db"
)

# Load Gmail credentials from .env
_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_GMAIL_USER = ""
_GMAIL_PASSWORD = ""
_GMAIL_SMTP = "smtp.gmail.com"
_GEMINI_API_KEYS = []
_HUNTER_API_KEY = ""
_GROQ_API_KEY = ""
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("GMAIL_USER="):
            _GMAIL_USER = line.split("=", 1)[1]
        elif line.startswith("GMAIL_PASSWORD="):
            _GMAIL_PASSWORD = line.split("=", 1)[1]
        elif line.startswith("GEMINI_API_KEYS="):
            _GEMINI_API_KEYS = [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]
        elif line.startswith("GEMINI_API_KEY="):
            _GEMINI_API_KEYS = [line.split("=", 1)[1].strip()]
        elif line.startswith("HUNTER_API_KEY="):
            _HUNTER_API_KEY = line.split("=", 1)[1]
        elif line.startswith("GROQ_API_KEY="):
            _GROQ_API_KEY = line.split("=", 1)[1]
        elif line.startswith("GMAIL_SMTP="):
            _GMAIL_SMTP = line.split("=", 1)[1]
os.makedirs(DATA_DIR, exist_ok=True)

import asyncio
import random

_API_LOCK = None
_LAST_API_CALL_TIME = 0.0

# --- AI Rate Limiting & Caching ---
AI_DAILY_LIMIT = 100000  # Effectively unlimited for local Ollama/Gemini usage
_ai_calls_today = 0
_ai_calls_date = None
_ai_cache = {}  # {hash: {"response": str, "timestamp": float}}
_AI_CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds

def _get_ai_cache_db():
    """Get/create AI usage tracking table in learning.db."""
    db = sqlite3.connect(os.path.join(DATA_DIR, "learning.db"), timeout=10.0)
    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            calls_count INTEGER DEFAULT 0,
            UNIQUE(date)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_cache (
            cache_key TEXT PRIMARY KEY,
            response TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    db.commit()
    return db

def _get_ai_calls_today():
    """Get the number of AI calls made today."""
    global _ai_calls_today, _ai_calls_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _ai_calls_date == today:
        return _ai_calls_today
    # Load from DB
    db = _get_ai_cache_db()
    row = db.execute("SELECT calls_count FROM ai_usage WHERE date = ?", (today,)).fetchone()
    db.close()
    _ai_calls_today = row[0] if row else 0
    _ai_calls_date = today
    return _ai_calls_today

def _increment_ai_calls():
    """Increment the AI call counter for today."""
    global _ai_calls_today, _ai_calls_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _ai_calls_date != today:
        _ai_calls_today = 0
        _ai_calls_date = today
    _ai_calls_today += 1
    # Persist to DB
    db = _get_ai_cache_db()
    db.execute("""
        INSERT INTO ai_usage (date, calls_count) VALUES (?, 1)
        ON CONFLICT(date) DO UPDATE SET calls_count = calls_count + 1
    """, (today,))
    db.commit()
    db.close()

def _check_ai_rate_limit():
    """Check if we're within the daily AI call limit."""
    return _get_ai_calls_today() < AI_DAILY_LIMIT

def _get_cache_key(prompt, system_prompt, model):
    """Generate a cache key from the prompt content."""
    content = f"{prompt}|{system_prompt}|{model}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]

def _get_cached_response(prompt, system_prompt, model):
    """Check cache for a recent response (within 24 hours)."""
    key = _get_cache_key(prompt, system_prompt, model)
    # Check in-memory cache first
    if key in _ai_cache:
        entry = _ai_cache[key]
        if time.time() - entry["timestamp"] < _AI_CACHE_TTL:
            return entry["response"]
    # Check DB cache
    db = _get_ai_cache_db()
    row = db.execute("SELECT response, created_at FROM ai_cache WHERE cache_key = ?", (key,)).fetchone()
    if row:
        created_at = datetime.fromisoformat(row[1])
        if (datetime.now(timezone.utc) - created_at).total_seconds() < _AI_CACHE_TTL:
            _ai_cache[key] = {"response": row[0], "timestamp": created_at.timestamp()}
            db.close()
            return row[0]
    db.close()
    return None

def _cache_response(prompt, system_prompt, model, response):
    """Store a response in cache."""
    key = _get_cache_key(prompt, system_prompt, model)
    now = datetime.now(timezone.utc).isoformat()
    _ai_cache[key] = {"response": response, "timestamp": time.time()}
    db = _get_ai_cache_db()
    db.execute("""
        INSERT INTO ai_cache (cache_key, response, created_at) VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET response = ?, created_at = ?
    """, (key, response, now, response, now))
    db.commit()
    db.close()

async def call_gemini(prompt: str, system_prompt: str = "", model: str = "gemini-3.1-flash-lite", max_tokens: int = 1024) -> str:
    """Call Google Gemini API with key rotation.
    Tries each key once; if a key returns 429/quota, moves to the next.
    """
    if not _GEMINI_API_KEYS:
        raise Exception("GEMINI_API_KEY not configured")
    
    headers = {"Content-Type": "application/json"}
    
    contents = []
    if system_prompt:
        contents.append({"role": "user", "parts": [{"text": system_prompt}]})
        contents.append({"role": "model", "parts": [{"text": "Ik begrijp de instructies. Ik zal ze opvolgen."}]})
    contents.append({"role": "user", "parts": [{"text": prompt}]})
    
    payload = {
        "contents": contents,
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": max_tokens,
            "topP": 0.9,
        }
    }
    
    last_error = None
    for key in _GEMINI_API_KEYS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(url, headers=headers, json=payload)
                if r.status_code == 200:
                    data = r.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        content = candidates[0].get("content", {})
                        parts = content.get("parts", [])
                        if parts:
                            return parts[0].get("text", "")
                    raise Exception("Empty Gemini response")
                elif r.status_code == 429:
                    logger.warning(f"[Gemini] Key ...{key[-6:]} rate limited (429), rotating")
                    last_error = f"HTTP 429 on key ...{key[-6:]}"
                    continue
                else:
                    last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                    continue
        except Exception as e:
            last_error = str(e)[:120]
            continue
    
    raise Exception(f"All Gemini keys failed. Last error: {last_error}")


async def call_groq(prompt: str, system_prompt: str = "") -> str:
    """Call Groq API (free tier, fast inference with Llama 3)."""
    if not _GROQ_API_KEY:
        raise Exception("GROQ_API_KEY not configured")
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {_GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 512,
    }
    
    attempts = 2
    delay = 1.0
    for attempt in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(url, headers=headers, json=payload)
                if r.status_code == 200:
                    data = r.json()
                    choices = data.get("choices", [])
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                else:
                    raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            if attempt == attempts - 1:
                raise e
            await asyncio.sleep(delay)
    raise Exception("Groq API call failed")


def extract_json_from_llm_response(text: str) -> dict | None:
    """Extract a JSON object from LLM response text, handling markdown code blocks and extra text."""
    if not text:
        return None
    text = text.strip()
    
    # 1. Try direct parse
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, ValueError):
        pass
    
    # 2. Extract from markdown code block
    import re
    code_block = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
    if code_block:
        try:
            result = json.loads(code_block.group(1).strip())
            if isinstance(result, dict):
                return result
        except (json.JSONDecodeError, ValueError):
            pass
    
    # 3. Find first { ... } block
    brace_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if brace_match:
        try:
            result = json.loads(brace_match.group(0))
            if isinstance(result, dict):
                return result
        except (json.JSONDecodeError, ValueError):
            pass
    
    return None


async def call_llm(prompt: str, system_prompt: str = "", model: str = "llama3.2:3b", json_mode: bool = False, max_tokens: int = 1024) -> str:
    """Helper to query LLM - tries Groq first (fast, reliable), then Gemini, then local Ollama.
    Includes 50 calls/day rate limit and 24-hour response cache."""
    # --- Check cache first ---
    cached = _get_cached_response(prompt, system_prompt, model)
    if cached:
        return cached

    # --- Check rate limit ---
    if not _check_ai_rate_limit():
        logger.warning(f"[LLM] Daily AI call limit reached ({AI_DAILY_LIMIT}). Using fallback response.")
        raise Exception(f"AI rate limit: {AI_DAILY_LIMIT} calls/day exceeded")

    # Try Groq first (free, fast, reliable)
    try:
        result = await asyncio.wait_for(call_groq(prompt, system_prompt), timeout=15)
        _increment_ai_calls()
        _cache_response(prompt, system_prompt, model, result)
        return result
    except Exception:
        pass
    
    # Try Gemini (fallback)
    try:
        result = await asyncio.wait_for(call_gemini(prompt, system_prompt, model=model, max_tokens=max_tokens), timeout=30)
        _increment_ai_calls()
        _cache_response(prompt, system_prompt, model, result)
        return result
    except Exception:
        pass
    
    # Fallback to local Ollama (short timeout — usually not running)
    url = "http://localhost:11434/api/generate"
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    if system_prompt:
        payload["system"] = system_prompt
        
    if json_mode:
        payload["format"] = "json"
        
    attempts = 2
    delay = 2.0
    for attempt in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(url, headers=headers, json=payload)
                if r.status_code == 200:
                    res_data = r.json()
                    result = res_data.get("response", "")
                    _increment_ai_calls()
                    _cache_response(prompt, system_prompt, model, result)
                    return result
                else:
                    raise Exception(f"HTTP {r.status_code}: {r.text}")
        except Exception as e:
            if attempt == attempts - 1:
                raise e
            await asyncio.sleep(delay)
            delay *= 1.5
    raise Exception("No LLM available (Gemini, Groq, and Ollama all failed)")

# ---------- Database helpers ----------

def _get_crm_db():
    db = sqlite3.connect(os.path.join(DATA_DIR, "crm.db"), timeout=30.0)
    db.execute("""
        CREATE TABLE IF NOT EXISTS crm_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT,
            domain TEXT,
            trust_score INTEGER,
            qualification_score INTEGER,
            decision TEXT,
            summary TEXT,
            email_sent INTEGER,
            reasoning TEXT,
            created_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS prospects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT UNIQUE,
            domain TEXT,
            contact_email TEXT,
            contact_name TEXT,
            status TEXT DEFAULT 'ingested',
            created_at TEXT
        )
    """)
    db.commit()
    return db

def _get_learning_db():
    db = sqlite3.connect(os.path.join(DATA_DIR, "learning.db"), timeout=30.0)
    db.execute("""
        CREATE TABLE IF NOT EXISTS learnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            insight TEXT,
            trigger_pattern TEXT,
            recommendation TEXT,
            created_at TEXT
        )
    """)
    db.commit()
    return db

# ---------- Tool 1: scrape_url ----------

import urllib.parse

async def _fetch_page(url: str, client: httpx.AsyncClient) -> str:
    """Fetch a single page and extract clean text."""
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        text = r.text
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<nav[^>]*>.*?</nav>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<footer[^>]*>.*?</footer>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:8000]
    except Exception:
        return ""

def _extract_links(html: str, base_url: str) -> list:
    """Extract internal links from HTML."""
    links = set()
    base_parsed = urllib.parse.urlparse(base_url)
    domain = base_parsed.netloc
    scheme = base_parsed.scheme
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        href = m.group(1).strip()
        parsed = urllib.parse.urlparse(href)
        if not parsed.netloc:
            href = urllib.parse.urljoin(base_url, href)
            parsed = urllib.parse.urlparse(href)
        if parsed.netloc == domain or parsed.netloc == domain.replace("www.", "") or parsed.netloc == "www." + domain:
            cleaned = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
            links.add(cleaned)
    return sorted(links)

_PRIORITY_PATHS = [
    "/over", "/over-ons", "/about", "/about-us",
    "/diensten", "/services", "/service", "/oplossingen", "/solutions",
    "/contact", "/contact-us", "/team", "/producten", "/product",
    "/cases", "/referenties", "/blog", "/nieuws",
    "/vacatures", "/werken-bij", "/partners",
    "/specialisten", "/expertise", "/werkwijze", "/klanten",
    "/offerte", "/tarieven", "/privacy", "/wat-we-doen",
    "/onze-werkwijze", "/resultaten", "/succesverhalen",
]

_COMMON_TLDS = {
    "com", "nl", "be", "de", "eu", "net", "org", "io", "co", "app", "dev",
    "info", "biz", "online", "site", "tech", "store", "shop", "nl", "be",
    "fr", "es", "it", "at", "ch", "dk", "se", "no", "fi", "pl", "cz",
    "hu", "ro", "bg", "gr", "pt", "ie", "lu", "lt", "lv", "ee", "sk",
    "si", "hr", "ba", "rs", "al", "mk", "me", "uk", "co.uk", "org.uk",
    "ac.uk", "gov.uk", "com.au", "co.nz", "ca", "us", "mx", "br", "ar",
    "jp", "cn", "in", "ru", "za", "travel", "jobs", "name", "pro",
    "xyz", "club", "world", "agency", "today", "live", "pro", "team",
    "media", "digital", "marketing", "solutions", "consulting", "events",
    "cloud", "network", "technology", "software", "design", "studio",
    "email", "global", "group", "partners", "productions", "properties",
    "systems", "training", "academy", "career", "company", "education",
    "expert", "foundation", "gallery", "guide", "plus", "pics", "photo",
    "photography", "tips", "tools", "video", "vip", "vin", "vision",
    "watch", "works", "zone", "gmbh", "guru", "law", "ltd", "management",
}

_DISPOSABLE_DOMAINS = {
    "mailinator.com", "guerrillamail.com", "10minutemail.com", "tempmail.com",
    "throwaway.email", "yopmail.com", "sharklasers.com", "trashmail.com",
    "mailnator.com", "temp-mail.org", "getnada.com", "fakeinbox.com",
    "maildrop.cc", "dispostable.com", "mytemp.email", "mailexpire.com",
    "tempemail.net", "spamgourmet.com", "jetable.org", "discard.email",
}

def _extract_ld_json_emails(html: str) -> list:
    """Extract email addresses from JSON-LD structured data blocks."""
    emails = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE):
        try:
            data = json.loads(m.group(1))
            # Walk the JSON tree looking for email fields
            def _walk(obj, depth=0):
                if depth > 5:
                    return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(k, str) and "email" in k.lower() and isinstance(v, str) and "@" in v:
                            emails.append(v.lower())
                        _walk(v, depth + 1)
                elif isinstance(obj, list):
                    for item in obj:
                        _walk(item, depth + 1)
            _walk(data)
        except json.JSONDecodeError:
            pass
    return emails

def _prioritize_emails(emails: list, scraped_text: str) -> list:
    """Sort emails so personal/contact emails come first, generic last."""
    try:
        from lead_quality import is_generic_email as _is_generic
    except Exception:
        _is_generic = None
    generic_prefixes = {"info", "contact", "hello", "hi", "support", "sales",
                        "admin", "office", "team", "mail", "enquiries", "help",
                        "privacy", "legal", "dpo", "compliance"}
    def score(e):
        local = e.split("@")[0].lower()
        # Emails found in visible text are prioritized
        if local in scraped_text.lower():
            return 0
        # Personal-name emails (not generic)
        if _is_generic is not None:
            if not _is_generic(e):
                return 1
            return 2
        if local not in generic_prefixes and not any(local.startswith(g) for g in generic_prefixes):
            return 1
        # Generic like info@, contact@
        return 2
    return sorted(set(emails), key=score)

def _extract_emails(html: str) -> list:
    """Extract real email addresses from raw HTML, filtering obfuscated/fake ones."""
    emails = set()
    base = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
    for m in re.finditer(base, html):
        email = m.group().strip().lower()
        # Skip image/data URIs and common false positives
        if any(x in email for x in ['.png', '.jpg', '.gif', '.svg', '.css', '.js', 'example', 'domain.com', 'you@', 'your@', 'info@domain', 'email@']):
            continue
        if email.count('@') != 1:
            continue
        local, domain = email.split("@", 1)
        # Skip if local part is too long (likely obfuscated/generated)
        if len(local) > 30:
            continue
        # Skip URL-encoded emails (e.g. %20info = " info")
        if "%" in local:
            continue
        # Skip if local part has too many segments (Cloudflare encoding)
        segments = re.split(r'[+._\-]', local)
        if len(segments) > 5:
            continue
        # Skip if TLD is not a known/common TLD
        tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
        if tld not in _COMMON_TLDS:
            continue
        # Skip if domain looks like an IP or has no alpha chars
        if not re.search(r'[a-zA-Z]', domain):
            continue
        emails.add(email)
    return sorted(emails)


def extract_social_links(html: str) -> dict:
    """Extract social media links from HTML content.
    
    Finds links to LinkedIn, Twitter/X, Facebook, Instagram, YouTube,
    GitHub, and other social platforms.
    
    Args:
        html: Raw HTML content
    
    Returns:
        dict with social platform -> URL mappings
    """
    social_patterns = {
        'linkedin': [
            r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[^\s"\'<>]+',
        ],
        'twitter': [
            r'https?://(?:www\.)?twitter\.com/[^\s"\'<>]+',
            r'https?://(?:www\.)?x\.com/[^\s"\'<>]+',
        ],
        'facebook': [
            r'https?://(?:www\.)?facebook\.com/[^\s"\'<>]+',
            r'https?://(?:www\.)?fb\.com/[^\s"\'<>]+',
        ],
        'instagram': [
            r'https?://(?:www\.)?instagram\.com/[^\s"\'<>]+',
        ],
        'youtube': [
            r'https?://(?:www\.)?youtube\.com/(?:channel|c|user)/[^\s"\'<>]+',
            r'https?://youtu\.be/[^\s"\'<>]+',
        ],
        'github': [
            r'https?://(?:www\.)?github\.com/[^\s"\'<>]+',
        ],
        'tiktok': [
            r'https?://(?:www\.)?tiktok\.com/@[^\s"\'<>]+',
        ],
        'pinterest': [
            r'https?://(?:www\.)?pinterest\.(?:com|nl|de|fr)/[^\s"\'<>]+',
        ],
    }
    
    found = {}
    for platform, patterns in social_patterns.items():
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                # Take the first clean URL
                url = matches[0].rstrip('/').split('"')[0].split("'")[0].split('>')[0]
                if url not in found.values():
                    found[platform] = url
                    break
    
    return found


def extract_phone_numbers(html: str) -> list:
    """Extract phone numbers from HTML content.
    
    Supports Dutch, German, Belgian, and international phone formats.
    
    Args:
        html: Raw HTML content
    
    Returns:
        List of phone number strings
    """
    # Phone patterns (various formats)
    patterns = [
        # International format: +31 6 12345678, +49 30 1234567
        r'\+[\d\s\-\(\)]{8,20}',
        # Dutch mobile: 06 12345678, 06-12345678
        r'06[\s\-]?\d{8}',
        # Dutch landline: 020 1234567, 020-1234567
        r'0[1-9][\d][\s\-]?\d{7,8}',
        # German: 030 12345678, 030-12345678
        r'0\d{2,4}[\s\-]?\d{6,10}',
        # General European: 0031, 0049, 0032
        r'00[\d][\s\-]?\d{6,14}',
        # Belgian: 02 123 45 67
        r'0\d{1,2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
    ]
    
    phones = set()
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            # Clean the number
            cleaned = re.sub(r'[^\d+]', '', match)
            if len(cleaned) >= 8 and len(cleaned) <= 15:
                phones.add(match.strip())
    
    return sorted(phones)


async def deep_research_company(url: str) -> dict:
    """Deep research a company: scrape website + extract social links, phones, emails.
    
    Combines website scraping with social link and phone extraction.
    
    Args:
        url: Company website URL
    
    Returns:
        dict with text, emails, social_links, phones, pages_scraped
    """
    result = await scrape_url(url)
    
    # Extract social links from the main page
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as client:
            r = await client.get(url)
            if r.status_code == 200:
                result["social_links"] = extract_social_links(r.text)
                result["phones"] = extract_phone_numbers(r.text)
    except Exception:
        result["social_links"] = {}
        result["phones"] = []
    
    return result


async def _dork_domain_emails(domain: str) -> list:
    """Uses DuckDuckGo Lite to perform a targeted web dork for personal emails associated with the domain."""
    emails = set()
    query = f'"@{domain}"'
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as client:
            r = await client.post("https://lite.duckduckgo.com/lite/", data={"q": query})
            if r.status_code == 200:
                for e in _extract_emails(r.text):
                    if e.endswith(f"@{domain}"):
                        emails.add(e)
    except Exception:
        pass
    return sorted(emails)

async def scrape_url(url: str) -> dict:
    """Deep scrape: homepage + internal pages. Returns dict with text, emails, pages_scraped."""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False,
                                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as client:
            r = await client.get(url)
            r.raise_for_status()
            html = r.text
            texts = []
            all_emails = set()

            # Extract emails from raw homepage HTML
            for e in _extract_emails(html):
                all_emails.add(e)
            # Extract emails from JSON-LD structured data
            for e in _extract_ld_json_emails(html):
                all_emails.add(e)

            # Clean homepage text
            home = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
            home = re.sub(r'<style[^>]*>.*?</style>', '', home, flags=re.DOTALL | re.IGNORECASE)
            home = re.sub(r'<nav[^>]*>.*?</nav>', '', home, flags=re.DOTALL | re.IGNORECASE)
            home = re.sub(r'<footer[^>]*>.*?</footer>', '', home, flags=re.DOTALL | re.IGNORECASE)
            home = re.sub(r'<[^>]+>', ' ', home)
            home = re.sub(r'\s+', ' ', home).strip()
            texts.append(("[home]", home[:8000]))

            # Find priority internal pages to scrape
            all_links = _extract_links(html, url)
            priority_urls = []
            for link in all_links:
                parsed = urllib.parse.urlparse(link)
                path = parsed.path.rstrip("/") or "/"
                if path in _PRIORITY_PATHS or any(path.startswith(pp) for pp in _PRIORITY_PATHS):
                    priority_urls.append(link)
            priority_urls = [u for u in priority_urls if urllib.parse.urlparse(u).path.rstrip("/") not in ("", "/")]
            priority_urls = priority_urls[:6]

            # Scrape priority pages
            for p_url in priority_urls:
                body = await _fetch_page(p_url, client)
                if body:
                    tag = f"[{urllib.parse.urlparse(p_url).path.strip('/').replace('-',' ').replace('/',' > ')}]"
                    texts.append((tag, body[:5000]))
                    # Try to extract emails from subpages too — fetch raw HTML
                    try:
                        sub = await client.get(p_url, timeout=8)
                        sub_html = sub.text
                        for e in _extract_emails(sub_html):
                            all_emails.add(e)
                        for e in _extract_ld_json_emails(sub_html):
                            all_emails.add(e)
                    except Exception:
                        pass
                    if len(texts) >= 7:
                        break

            # Dork for extra emails on DuckDuckGo to bypass info@ generic limitations
            parsed_domain = urllib.parse.urlparse(url).netloc.replace('www.', '')
            dorked_emails = await _dork_domain_emails(parsed_domain)
            for e in dorked_emails:
                all_emails.add(e)

            combined = "\n".join(f"{tag} {txt}" for tag, txt in texts)
            # Re-prioritize: personal-name emails first
            sorted_emails = _prioritize_emails(all_emails, combined)

            return {
                "text": combined.strip()[:25000],
                "emails": sorted_emails,
                "pages_scraped": len(texts)
            }
    except Exception as e:
        return {"text": f"SCRAPE_FAILED: {e}", "emails": [], "pages_scraped": 0}

# ---------- Email Verification ----------

def verify_email(email: str) -> dict:
    """Verify an email address: placeholder check, MX records, SMTP RCPT TO, disposable domain.
    Returns dict with verified (bool), confidence (0-100), reasons (list)."""
    import dns.resolver
    try:
        import dns_client
    except ImportError:
        from clawbuildr import dns_client
    reasons = []
    confidence = 50

    if not email or "@" not in email:
        return {"verified": False, "confidence": 0, "reasons": ["Invalid email format"]}

    local, domain = email.split("@", 1)
    local_lower = local.lower().strip()
    domain_lower = domain.lower().strip()
    email_lower = f"{local_lower}@{domain_lower}"

    # HARD BLOCK: Placeholder/test emails
    placeholder_locals = {
        "naam", "voorbeeld", "test", "demo", "example", "jouw", "uw", "uwnaam",
        "name", "yourname", "username", "email", "info", "contact", "admin",
        "support", "hello", "hi", "hey", "mail", "post", "user",
    }
    if local_lower in placeholder_locals:
        return {"verified": False, "confidence": 0, "reasons": [f"Placeholder email: {local_lower}@..."]}

    placeholder_domains = {
        "voorbeeld.com", "bedrijf.nl", "example.com", "example.org", "example.net",
        "test.com", "test.de", "test.nl", "demo.com", "mailinator.com",
        "yourcompany.com", "yourdomain.com", "company.com",
    }
    if domain_lower in placeholder_domains:
        return {"verified": False, "confidence": 0, "reasons": [f"Placeholder domain: {domain_lower}"]}

    # HARD BLOCK: Spaces in email (invalid RFC 5321)
    if " " in email or "\t" in email:
        return {"verified": False, "confidence": 0, "reasons": ["Email contains whitespace"]}

    # Format checks
    if len(local) > 30:
        reasons.append("Local part too long")
        confidence -= 20
    if not re.match(r'^[a-zA-Z0-9._%+\-]+$', local):
        reasons.append("Local part has invalid characters")
        confidence -= 15
    if len(domain) > 50:
        reasons.append("Domain too long")
        confidence -= 10

    # Disposable domain check
    if domain_lower in _DISPOSABLE_DOMAINS:
        reasons.append(f"Disposable email domain: {domain}")
        confidence -= 40

    # Check if domain looks real (has a dot, not an IP)
    if "." not in domain:
        reasons.append("Domain has no TLD")
        confidence -= 20
    elif not re.search(r'[a-zA-Z]', domain):
        reasons.append("Domain has no letters")
        confidence -= 15

    # MX record check
    mx_found = False
    mx_records = []
    try:
        answers = dns_client.resolve(domain, "MX", lifetime=10)
        mx_records = [str(r.exchange).rstrip(".") for r in answers]
        if mx_records:
            mx_found = True
            reasons.append(f"MX: {mx_records[0]}")
            confidence += 30
        else:
            reasons.append("No MX records found")
            confidence -= 15
    except dns.resolver.NoAnswer:
        reasons.append("No MX records (domain has no mail server)")
        confidence -= 15
    except dns.resolver.NXDOMAIN:
        reasons.append("Domain does not exist (NXDOMAIN)")
        confidence -= 40
    except Exception as e:
        reasons.append(f"MX lookup failed: {e}")
        confidence -= 20  # DNS timeout is a significant issue, not minor

    # Bonus for common mail providers
    common_mx = {"google.com", "googlemail.com", "outlook.com", "hotmail.com",
                 "microsoft.com", "protonmail.com", "zoho.com", "mail.com",
                 "icloud.com", "yahoo.com", "yandex.com", "gmx.com"}
    if mx_found:
        for mx in mx_records[:3]:
            for provider in common_mx:
                if provider in mx.lower():
                    confidence += 10
                    reasons.append(f"Known mail provider: {provider}")
                    break

    # SMTP RCPT TO verification (confirms mailbox actually exists)
    if mx_found and mx_records:
        try:
            import smtplib
            import socket
            smtp_host = mx_records[0]
            smtp = smtplib.SMTP(timeout=10)
            smtp.connect(smtp_host, 25)
            smtp.helo("clawbuildr.com")
            smtp.mail("verify@clawbuildr.com")
            code, _ = smtp.rcpt(email)
            if code == 250:
                confidence += 20
                reasons.append("SMTP RCPT TO: mailbox exists")
            elif code == 550:
                confidence -= 30
                reasons.append("SMTP RCPT TO: mailbox does not exist (550)")
            else:
                reasons.append(f"SMTP RCPT TO: code {code}")
                confidence -= 5
            smtp.quit()
        except (smtplib.SMTPException, socket.timeout, socket.error, OSError) as e:
            reasons.append(f"SMTP check failed: {type(e).__name__}")
            confidence -= 5  # SMTP check failure is not a hard fail

    # Final score
    confidence = max(0, min(100, confidence))
    verified = confidence >= 70

    return {"verified": verified, "confidence": confidence, "reasons": reasons}

# ---------- Tool 2: kvk_lookup ----------

async def kvk_lookup(query: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.kvk.nl/api/v2/zoeken",
                params={"q": query, "pagina": 1, "aantal": 3},
                headers={"User-Agent": "ClawBuildr/1.0"}
            )
            if r.status_code == 200:
                data = r.json()
                results = data.get("resultaten", [])
                if results:
                    first = results[0]
                    return {
                        "found": True,
                        "company": first.get("naam", query),
                        "kvk_number": first.get("kvkNummer", ""),
                        "city": first.get("vestigingsplaats", ""),
                        "status": first.get("status", ""),
                        "type": first.get("rechtspersoon", ""),
                        "raw": json.dumps(first, indent=2)[:1000]
                    }
            if r.status_code == 401:
                return {"found": False, "raw": "API key required (401)", "note": "KVK API not configured, using web-only research"}
            return {"found": False, "raw": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"found": False, "error": str(e)}

# ---------- Tool 3: gmail_send ----------

async def gmail_send(to: str, subject: str, body: str, from_addr: str = None,
                     smtp_host: str = None, smtp_port: int = None,
                     smtp_user: str = None, smtp_password: str = None,
                     html_body: str = None) -> dict:
    """Send email via Gmail SMTP. Supports multiple accounts via parameters.
    If html_body is provided, sends a multipart email with both plain text and HTML.
    If account credentials are provided, uses those. Otherwise falls back to .env defaults.
    """
    host = smtp_host or _GMAIL_SMTP
    port = smtp_port or 465
    user = smtp_user or _GMAIL_USER
    password = smtp_password or _GMAIL_PASSWORD

    if not user or not password:
        return {
            "status": "not_configured",
            "to": to,
            "subject": subject,
            "note": "No email credentials configured"
        }
    try:
        if html_body:
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = from_addr or user
            msg["To"] = to
            msg.attach(MIMEText(body, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))
        else:
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = from_addr or user
            msg["To"] = to

        if int(port) == 465:
            with smtplib.SMTP_SSL(host, port, timeout=30) as server:
                server.login(user, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(user, password)
                server.send_message(msg)

        return {"status": "sent", "to": to, "subject": subject, "from": from_addr or user}
    except Exception as e:
        return {"status": "failed", "to": to, "subject": subject, "error": str(e)}


def _reset_sends_today_if_stale(conn: sqlite3.Connection, today_utc: str) -> None:
    """Zero sends_today when last_send_at is from a previous UTC day (or never sent)."""
    try:
        conn.execute(
            """UPDATE email_accounts SET sends_today = 0
               WHERE last_send_at IS NULL
                  OR substr(last_send_at, 1, 10) < ?""",
            (today_utc,),
        )
        conn.commit()
    except Exception:
        logger.debug("reset_sends_today_if_stale failed")


def _active_tenant_id(conn: sqlite3.Connection) -> Optional[str]:
    """Active tenant from tenant_config, or None when unavailable."""
    try:
        row = conn.execute("SELECT tenant_id FROM tenant_config WHERE active = 1 LIMIT 1").fetchone()
        return row[0] if row else None
    except Exception:
        return None


def resolve_send_kwargs(campaign_id: str = None, contact_id: str = None, tenant_id: str = None) -> dict:
    """Pick an email_accounts row for sending.

    Priority: campaign.account_id → tenant's round-robin under daily
    limit → tenant's default → {}.
    tenant_id=None resolves the ACTIVE tenant; an explicit tenant_id scopes
    the lookup to that tenant (used by sequence sends so each enrollment
    sends from its own tenant's mailbox regardless of which tenant is active).
    Empty dict means no usable account for that tenant.
    """
    try:
        conn = sqlite3.connect(CLAWBUILDR_DB, timeout=10.0)
        conn.row_factory = sqlite3.Row
    except Exception:
        return {}
    try:
        today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _reset_sends_today_if_stale(conn, today_utc)
        tenant = tenant_id or _active_tenant_id(conn)
        tenant_sql = ""
        tenant_params: tuple = ()
        if tenant:
            tenant_sql = " AND (tenant_id = ? OR tenant_id IS NULL OR tenant_id = '')"
            tenant_params = (tenant,)
        if campaign_id:
            row = conn.execute(
                "SELECT account_id FROM campaigns WHERE campaign_id = ?", (campaign_id,)
            ).fetchone()
            if row and row["account_id"]:
                acc = conn.execute(
                    "SELECT * FROM email_accounts WHERE account_id = ? AND is_active = 1",
                    (row["account_id"],),
                ).fetchone()
                if acc:
                    limit = acc["daily_send_limit"]
                    if limit is None or int(acc["sends_today"] or 0) < int(limit):
                        return dict(acc)
        acc = conn.execute(
            """SELECT * FROM email_accounts WHERE is_active = 1
               AND (daily_send_limit IS NULL OR sends_today < daily_send_limit)
               """ + tenant_sql + """
               ORDER BY sends_today ASC, is_default DESC, created_at ASC LIMIT 1""",
            tenant_params,
        ).fetchone()
        if acc:
            return dict(acc)
        acc = conn.execute(
            """SELECT * FROM email_accounts WHERE is_active = 1 AND is_default = 1
               AND (daily_send_limit IS NULL OR sends_today < daily_send_limit)
               """ + tenant_sql + """
               LIMIT 1""",
            tenant_params,
        ).fetchone()
        return dict(acc) if acc else {}
    except Exception:
        return {}
    finally:
        conn.close()


def record_account_send(account_id: str) -> None:
    """Increment sends_today for an account (no-op if missing)."""
    if not account_id:
        return
    conn = None
    try:
        conn = sqlite3.connect(CLAWBUILDR_DB, timeout=10.0)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE email_accounts SET sends_today = sends_today + 1, last_send_at = ? WHERE account_id = ?",
            (now, account_id),
        )
        conn.commit()
    except Exception:
        logger.debug("record_account_send failed for %s", account_id)
    finally:
        if conn is not None:
            conn.close()


async def gmail_send_with_account(to: str, subject: str, body: str, account_id: str) -> dict:
    """Send email using a specific email account from the database."""
    conn = sqlite3.connect(CLAWBUILDR_DB, timeout=10.0)
    try:
        conn.row_factory = sqlite3.Row
        account = conn.execute(
            "SELECT * FROM email_accounts WHERE account_id = ? AND is_active = 1",
            (account_id,)
        ).fetchone()
    finally:
        conn.close()

    if not account:
        return {"status": "error", "error": f"Account {account_id} not found or inactive"}

    acc = dict(account)
    result = await gmail_send(
        to=to, subject=subject, body=body,
        from_addr=acc["email_address"],
        smtp_host=acc["smtp_host"],
        smtp_port=acc["smtp_port"],
        smtp_user=acc["smtp_user"],
        smtp_password=acc["smtp_password"]
    )

    if result.get("status") == "sent":
        record_account_send(account_id)

    return result

# ---------- Tool 4: db_append ----------

def db_append(data: dict) -> dict:
    db = _get_learning_db()
    cur = db.execute(
        "INSERT INTO learnings (insight, trigger_pattern, recommendation, created_at) VALUES (?, ?, ?, ?)",
        (
            data.get("insight", ""),
            data.get("trigger_pattern", ""),
            data.get("recommendation", ""),
            datetime.utcnow().isoformat()
        )
    )
    db.commit()
    return {"status": "appended", "id": cur.lastrowid}

# ---------- Tool 5: db_query ----------

def db_query(query_type: str = "all", limit: int = 20) -> list:
    db = _get_learning_db()
    db.row_factory = sqlite3.Row
    if query_type == "all":
        rows = db.execute("SELECT * FROM learnings ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    elif query_type == "recent":
        rows = db.execute("SELECT * FROM learnings ORDER BY id DESC LIMIT 5").fetchall()
    else:
        rows = db.execute("SELECT * FROM learnings ORDER BY RANDOM() LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]

# ---------- CRM helpers ----------

def save_crm_record(record: dict) -> dict:
    db = _get_crm_db()
    cur = db.execute(
        "INSERT INTO crm_records (company, domain, trust_score, qualification_score, decision, summary, email_sent, reasoning, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            record.get("company", ""),
            record.get("domain", ""),
            record.get("trust_score", 0),
            record.get("qualification_score", 0),
            record.get("decision", ""),
            record.get("summary", ""),
            1 if record.get("email_sent") else 0,
            record.get("reasoning", ""),
            datetime.utcnow().isoformat()
        )
    )
    db.commit()
    record["id"] = cur.lastrowid
    record["created_at"] = datetime.utcnow().isoformat()
    return record

def get_crm_records(limit: int = 50) -> list:
    db = _get_crm_db()
    db.row_factory = sqlite3.Row
    rows = db.execute("SELECT * FROM crm_records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]

def save_prospect(lead: dict) -> dict:
    db = _get_crm_db()
    try:
        cur = db.execute(
            "INSERT INTO prospects (company, domain, contact_email, contact_name, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                lead.get("company", ""),
                lead.get("domain", ""),
                lead.get("contact_email", ""),
                lead.get("contact_name", ""),
                "ingested",
                datetime.utcnow().isoformat()
            )
        )
        db.commit()
        return {"status": "ingested", "id": cur.lastrowid}
    except sqlite3.IntegrityError:
        return {"status": "duplicate", "note": f"Company '{lead.get('company')}' already exists"}


def save_linkedin_enrichment(company_name: str, domain: str, profile_data: dict, company_data: dict) -> dict:
    """Persist LinkedIn enrichment data to contacts and companies tables."""
    import json as _json
    db = _get_crm_db()
    now = datetime.utcnow().isoformat()
    result = {"profile_updated": False, "company_updated": False}

    # Update company — match domain first (stable), fall back to name
    if company_data and company_name:
        try:
            domain_val = (domain or "").strip().lower()
            cur = db.execute(
                """UPDATE companies SET
                    industry = COALESCE(NULLIF(?, ''), industry),
                    estimated_size = COALESCE(NULLIF(?, ''), estimated_size),
                    headquarters = COALESCE(NULLIF(?, ''), headquarters),
                    founded = COALESCE(NULLIF(?, ''), founded),
                    specialties = COALESCE(NULLIF(?, ''), specialties),
                    website = COALESCE(NULLIF(?, ''), website),
                    followers = COALESCE(NULLIF(?, ''), followers),
                    employee_count_text = COALESCE(NULLIF(?, ''), employee_count_text),
                    hiring_count = COALESCE(NULLIF(?, ''), hiring_count),
                    hiring_jobs_json = COALESCE(NULLIF(?, ''), hiring_jobs_json),
                    company_posts_json = COALESCE(NULLIF(?, ''), company_posts_json),
                    linkedin_last_enriched_at = ?
                WHERE (domain IS NOT NULL AND LOWER(domain) = ?) OR name = ?""",
                (
                    company_data.get("industry", ""),
                    company_data.get("size", ""),
                    company_data.get("headquarters", ""),
                    company_data.get("founded", ""),
                    company_data.get("specialties", ""),
                    company_data.get("website", ""),
                    company_data.get("followers", ""),
                    company_data.get("employee_count_text", ""),
                    str(company_data.get("hiring_count", "")) if company_data.get("hiring_count") is not None else "",
                    _json.dumps(company_data.get("hiring_jobs", [])) if company_data.get("hiring_jobs") else None,
                    _json.dumps(company_data.get("posts", [])) if company_data.get("posts") else None,
                    now,
                    domain_val,
                    company_name,
                )
            )
            if cur.rowcount > 0:
                result["company_updated"] = True
            db.commit()
        except Exception as e:
            result["company_error"] = str(e)[:100]

    # Update contact — also backfill linkedin_url when empty
    if profile_data:
        try:
            linkedin_url = profile_data.get("profile_url", "")
            first_name = profile_data.get("name", "").split()[0] if profile_data.get("name") else ""
            cur = db.execute(
                """UPDATE contacts SET
                    role = COALESCE(NULLIF(?, ''), role),
                    tenure = COALESCE(NULLIF(?, ''), tenure),
                    skills_json = COALESCE(NULLIF(?, ''), skills_json),
                    mutual_connections_count = COALESCE(NULLIF(?, ''), mutual_connections_count),
                    mutual_connections_text = COALESCE(NULLIF(?, ''), mutual_connections_text),
                    linkedin_url = CASE
                        WHEN linkedin_url IS NULL OR linkedin_url = '' THEN COALESCE(NULLIF(?, ''), linkedin_url)
                        ELSE linkedin_url
                    END,
                    linkedin_last_enriched_at = ?
                WHERE (linkedin_url != '' AND linkedin_url = ?)
                   OR (first_name = ? AND company_id IN (SELECT company_id FROM companies WHERE name = ?))""",
                (
                    profile_data.get("current_role", ""),
                    profile_data.get("tenure", ""),
                    _json.dumps(profile_data.get("skills", [])) if profile_data.get("skills") else None,
                    profile_data.get("mutual_connections_count", 0),
                    profile_data.get("mutual_connections_text", ""),
                    linkedin_url,
                    now,
                    linkedin_url,
                    first_name,
                    company_name,
                )
            )
            if cur.rowcount > 0:
                result["profile_updated"] = True
            db.commit()
        except Exception as e:
            result["profile_error"] = str(e)[:100]

    return result

# ---------- Tool 10: Hunter.io Search ----------
async def hunter_domain_search(domain: str) -> dict:
    """Queries Hunter.io for a domain to extract high-value personal emails and names."""
    if not _HUNTER_API_KEY:
        return {"found": False, "emails": []}
        
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={_HUNTER_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
            if r.status_code == 200:
                data = r.json()
                emails = data.get("data", {}).get("emails", [])
                
                # Filter for personal emails (best for B2B)
                personal_emails = [e for e in emails if e.get("type") == "personal"]
                target_list = personal_emails if personal_emails else emails
                
                if target_list:
                    best = target_list[0]
                    first_name = best.get("first_name", "")
                    last_name = best.get("last_name", "")
                    name = f"{first_name} {last_name}".strip() if first_name else None
                    
                    return {
                        "found": True,
                        "email": best.get("value"),
                        "name": name,
                        "position": best.get("position"),
                        "confidence": best.get("confidence")
                    }
        return {"found": False, "emails": []}
    except Exception as e:
        print(f"[Hunter.io] API error for {domain}: {e}")
        return {"found": False, "emails": []}


async def kvk_search(query: str, max_results: int = 5) -> list:
    """Search KVK (Kamer van Koophandel) for Dutch companies.
    Scrapes the public KVK website to find company details.
    Returns list of dicts with: kvk_nr, name, address, city, website, industry, phone, email.
    Note: KVK website uses JavaScript rendering; this is a best-effort HTML parse.
    For production use, integrate with KVK API (requires API key at https://developers.kvk.nl).
    """
    results = []
    urls_to_try = [
        # New URL format
        ("https://www.kvk.nl/zoeken/", {"start": "0", "q": query, "site": "handelsregister"}),
        # Old URL format
        ("https://www.kvk.nl/zoeken/handelsregister/", {"start": "0", "q": query}),
        # Alternative format
        ("https://www.kvk.nl/zoeken/?q=", {"q": query, "site": "handelsregister"}),
    ]
    
    for base_url, params in urls_to_try:
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"}) as client:
                r = await client.get(base_url, params=params)
                if r.status_code != 200:
                    continue
                html = r.text
                
                # Try to find JSON-LD structured data first
                json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
                if json_ld_match:
                    try:
                        import json
                        data = json.loads(json_ld_match.group(1))
                        if isinstance(data, dict) and data.get("@type") == "ItemList":
                            for item in data.get("itemListElement", [])[:max_results]:
                                it = item.get("item", {})
                                results.append({
                                    "kvk_nr": str(it.get("kvkNummer", "")),
                                    "name": it.get("name", ""),
                                    "address": it.get("address", {}).get("streetAddress", ""),
                                    "city": it.get("address", {}).get("addressLocality", ""),
                                    "website": it.get("website", ""),
                                    "phone": it.get("telephone", ""),
                                    "email": it.get("email", ""),
                                })
                            if results:
                                return results[:max_results]
                    except Exception:
                        pass
                
                # Fallback: parse HTML for company links
                for m in re.finditer(
                    r'<a[^>]*href="[^"]*handelsregister[^"]*"[^>]*>(.*?)</a>',
                    html, re.DOTALL | re.IGNORECASE
                ):
                    if len(results) >= max_results:
                        break
                    title_html = m.group(1)
                    name = re.sub(r'<[^>]+>', '', title_html).strip()
                    if not name or len(name) < 3:
                        continue
                    
                    # Extract KVK number from nearby text
                    kvk_match = re.search(r'KvK[:\s]*(\d{8})', title_html + html[max(0,m.start()-50):m.end()+50])
                    kvk_nr = kvk_match.group(1) if kvk_match else ""
                    
                    results.append({
                        "kvk_nr": kvk_nr,
                        "name": name,
                        "website": "",
                        "address": "",
                        "city": "",
                        "phone": "",
                        "email": "",
                    })
                
                if results:
                    return results[:max_results]
                    
        except Exception as e:
            logger.warning(f"[KVK] Attempt failed for '{query}': {str(e)[:60]}")
            continue
    
    logger.info(f"[KVK] No results found for '{query}' after trying all URL formats")
    return results


async def kvk_search_api(query: str, max_results: int = 5) -> list:
    """Search KVK using their public search API (JSON endpoint).
    More reliable than HTML scraping.
    """
    results = []
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            # KVK has a public search API
            r = await client.get(
                "https://zoeken.handelsregister.nl/zoeken/nummers",
                params={"kvkNummer": "", "ondernemingsnaam": query, "vestigingsadres": "", "postCode": "", "straat": "", "datumOprichtingVanaf": "", "datumOprichtingTot": "", "actueleVestigingen": "true", "uitgebreidZoeken": "false", "pageSize": str(max_results)}
            )
            if r.status_code != 200:
                # Fallback: try the regular website
                return await kvk_search(query, max_results)

            data = r.json() if "json" in r.headers.get("content-type", "") else {}
            if not data:
                return await kvk_search(query, max_results)

            for item in data.get("results", [])[:max_results]:
                results.append({
                    "kvk_nr": str(item.get("kvkNummer", "")),
                    "name": item.get("ondernemingsnaam", ""),
                    "address": item.get("adres", {}).get("straatHuisnummer", ""),
                    "city": item.get("adres", {}).get("woonplaats", ""),
                    "postal_code": item.get("adres", {}).get("postcode", ""),
                    "website": item.get("website", ""),
                    "phone": item.get("telefoonnummer", ""),
                    "email": item.get("emailadres", ""),
                })

    except Exception as e:
        logger.warning(f"[KVK API] Error for '{query}': {e}")
        return await kvk_search(query, max_results)

    return results
