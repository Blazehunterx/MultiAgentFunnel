import httpx
import sqlite3
import json
import os
import re
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Load Gmail credentials from .env
_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_GMAIL_USER = ""
_GMAIL_PASSWORD = ""
_GMAIL_SMTP = "smtp.gmail.com"
_GEMINI_API_KEY = ""
_HUNTER_API_KEY = ""
_GROQ_API_KEY = ""
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("GMAIL_USER="):
            _GMAIL_USER = line.split("=", 1)[1]
        elif line.startswith("GMAIL_PASSWORD="):
            _GMAIL_PASSWORD = line.split("=", 1)[1]
        elif line.startswith("GEMINI_API_KEY="):
            _GEMINI_API_KEY = line.split("=", 1)[1]
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

async def call_gemini(prompt: str, system_prompt: str = "") -> str:
    """Call Google Gemini 2.0 Flash API for text generation."""
    if not _GEMINI_API_KEY:
        raise Exception("GEMINI_API_KEY not configured")
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={_GEMINI_API_KEY}"
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
            "maxOutputTokens": 200,
            "topP": 0.9,
        }
    }
    
    attempts = 2
    delay = 1.0
    for attempt in range(attempts):
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
                else:
                    raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            if attempt == attempts - 1:
                raise e
            await asyncio.sleep(delay)
    raise Exception("Gemini API call failed")


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


async def call_llm(prompt: str, system_prompt: str = "", model: str = "llama3.2:3b", json_mode: bool = False) -> str:
    """Helper to query LLM - tries Groq first (fast, reliable), then Gemini, then local Ollama."""
    # Try Groq first (free, fast, reliable)
    try:
        return await call_groq(prompt, system_prompt)
    except Exception:
        pass
    
    # Try Gemini (fallback)
    try:
        return await call_gemini(prompt, system_prompt)
    except Exception:
        pass
    
    # Fallback to local Ollama
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
            async with httpx.AsyncClient(timeout=120) as client:
                r = await client.post(url, headers=headers, json=payload)
                if r.status_code == 200:
                    res_data = r.json()
                    return res_data.get("response", "")
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
    generic_prefixes = {"info", "contact", "hello", "hi", "support", "sales",
                        "admin", "office", "team", "mail", "enquiries", "help"}
    def score(e):
        local = e.split("@")[0].lower()
        # Emails found in visible text are prioritized
        if local in scraped_text.lower():
            return 0
        # Personal-name emails (not generic)
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
    """Verify an email address: MX records, disposable domain, format.
    Returns dict with verified (bool), confidence (0-100), reasons (list)."""
    import dns.resolver
    reasons = []
    confidence = 50

    if not email or "@" not in email:
        return {"verified": False, "confidence": 0, "reasons": ["Invalid email format"]}

    local, domain = email.split("@", 1)

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
    if domain.lower() in _DISPOSABLE_DOMAINS:
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
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=5)
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
        confidence -= 5  # Reduced from -10: DNS timeouts are common, not a hard failure

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

async def gmail_send(to: str, subject: str, body: str, from_addr: str = None) -> dict:
    if not _GMAIL_USER or not _GMAIL_PASSWORD:
        return {
            "status": "not_configured",
            "to": to,
            "subject": subject,
            "note": "No Gmail credentials in .env"
        }
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_addr or _GMAIL_USER
        msg["To"] = to

        with smtplib.SMTP_SSL(_GMAIL_SMTP, 465, timeout=30) as server:
            server.login(_GMAIL_USER, _GMAIL_PASSWORD)
            server.send_message(msg)

        return {"status": "sent", "to": to, "subject": subject}
    except Exception as e:
        return {"status": "failed", "to": to, "subject": subject, "error": str(e)}

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
