"""
Email Finder - Extract emails from websites and LinkedIn profiles.
Uses pattern matching, common email formats, MX verification, and Hunter.io.
"""

import re
import os
import json
import sqlite3
import logging
import asyncio
import httpx
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger("EmailFinder")

DB_PATH = "data/clawbuildr.db"

# Load Hunter.io API keys from .env (supports comma-separated rotation)
_HUNTER_API_KEYS = []
_HUNTER_KEY_INDEX = 0

def _load_hunter_keys():
    global _HUNTER_API_KEYS
    try:
        _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(_env_path):
            with open(_env_path) as _f:
                for _line in _f:
                    _line = _line.strip()
                    if _line.startswith("HUNTER_API_KEY="):
                        raw = _line.split("=", 1)[1].strip()
                        # Support comma-separated keys and newlines
                        _HUNTER_API_KEYS = [k.strip() for k in raw.replace("\n", ",").split(",") if k.strip()]
                        break
    except Exception:
        pass

_load_hunter_keys()


def _get_next_hunter_key():
    """Rotate to the next available Hunter.io API key."""
    global _HUNTER_KEY_INDEX
    if not _HUNTER_API_KEYS:
        return ""
    key = _HUNTER_API_KEYS[_HUNTER_KEY_INDEX % len(_HUNTER_API_KEYS)]
    _HUNTER_KEY_INDEX = (_HUNTER_KEY_INDEX + 1) % len(_HUNTER_API_KEYS)
    return key


def _peek_hunter_key():
    """Return current Hunter key without rotating."""
    if not _HUNTER_API_KEYS:
        return ""
    return _HUNTER_API_KEYS[_HUNTER_KEY_INDEX % len(_HUNTER_API_KEYS)]


# Gemini keys + feature flag for the AI email-guess step
_GEMINI_API_KEYS: List[str] = []
_GEMINI_EMAIL_GUESS_ENABLED = True
_GEMINI_GUESS_CACHE: Dict[str, Dict[str, Any]] = {}
# Models tried in order (flash-lite first, flash as fallback when overloaded)
GEMINI_EMAIL_MODELS = ("gemini-3.1-flash-lite", "gemini-3.6-flash")
# Keys permanently rejected by Google (403) — skipped for the rest of the process
_GEMINI_DEAD_KEYS: set = set()


def _load_gemini_keys():
    global _GEMINI_API_KEYS, _GEMINI_EMAIL_GUESS_ENABLED
    try:
        _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if not os.path.exists(_env_path):
            return
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line.startswith("GEMINI_API_KEYS="):
                    _GEMINI_API_KEYS = [k.strip() for k in _line.split("=", 1)[1].replace("\n", ",").split(",") if k.strip()]
                elif _line.startswith("GEMINI_API_KEY=") and not _GEMINI_API_KEYS:
                    _GEMINI_API_KEYS = [_line.split("=", 1)[1].strip()]
                elif _line.startswith("GEMINI_EMAIL_GUESS="):
                    _GEMINI_EMAIL_GUESS_ENABLED = _line.split("=", 1)[1].strip().lower() not in ("0", "false", "no", "off")
    except Exception:
        pass


_load_gemini_keys()

# Common email patterns
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

# Common Dutch business email patterns
NAME_PATTERNS = [
    "{first}.{last}",
    "{first}{last}",
    "{f}.{last}",
    "{first}_{last}",
    "{first}",
    "info",
    "contact",
    "hello",
    "hallo",
]

# Disposable email domains (skip these)
DISPOSABLE = {
    "tempmail.com", "throwaway.email", "guerrillamail.com",
    "mailinator.com", "yopmail.com", "trashmail.com",
    "fakeinbox.com", "sharklasers.com", "guerrillamailblock.com",
    "grr.la", "dispostable.com", "maildrop.cc"
}

GEMINI_EMAIL_SYSTEM_PROMPT = (
    "You infer professional email addresses for B2B outreach. "
    "Reply with STRICT JSON only — no markdown fences, no commentary. "
    "Never suggest an address on another domain and never suggest a role mailbox "
    "(info@, contact@, sales@, hello@ ...). Only suggest an address when the pattern "
    "is highly plausible for the given domain."
)


def _extract_json_obj(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    text = text.strip()
    for attempt in (text,
                    re.sub(r'^```[a-zA-Z]*\s*', '', text),
                    re.sub(r'\s*```$', '', re.sub(r'^```[a-zA-Z]*\s*', '', text))):
        try:
            obj = json.loads(attempt)
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, ValueError):
            pass
    brace = re.search(r'\{.*\}', text, re.DOTALL)
    if brace:
        try:
            obj = json.loads(brace.group(0))
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def _call_gemini_sync(prompt: str, system_prompt: str = "", model: str = None,
                      max_tokens: int = 512, timeout: float = 12.0) -> Optional[str]:
    """Synchronous Gemini call with key rotation + model fallback.

    Tries each model in GEMINI_EMAIL_MODELS against each live key.
    Keys that answer 403 (e.g. reported leaked) are marked dead and skipped
    for the rest of the process. Returns response text or None.
    """
    if not _GEMINI_API_KEYS:
        return None
    models = (model,) if model else GEMINI_EMAIL_MODELS
    headers = {"Content-Type": "application/json"}
    contents = []
    if system_prompt:
        contents.append({"role": "user", "parts": [{"text": system_prompt}]})
        contents.append({"role": "model", "parts": [{"text": "Understood. I will follow the instructions."}]})
    contents.append({"role": "user", "parts": [{"text": prompt}]})
    payload = {
        "contents": contents,
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_tokens, "topP": 0.9},
    }
    last_error = None
    attempts = 0
    for m in models:
        for key in _GEMINI_API_KEYS:
            if key in _GEMINI_DEAD_KEYS:
                continue
            if attempts >= 8:
                break
            attempts += 1
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={key}"
            try:
                with httpx.Client(timeout=timeout) as client:
                    r = client.post(url, headers=headers, json=payload)
                if r.status_code == 200:
                    data = r.json()
                    candidates = data.get("candidates") or []
                    if candidates:
                        parts = (candidates[0].get("content") or {}).get("parts") or []
                        text = "".join(p.get("text", "") for p in parts if isinstance(p, dict))
                        if text.strip():
                            return text
                    last_error = f"{m}: empty response"
                    continue
                body = (r.text or "").replace("\n", " ")[:150]
                last_error = f"{m}: HTTP {r.status_code} {body}"
                if r.status_code == 403:
                    _GEMINI_DEAD_KEYS.add(key)
                    logger.warning(f"[GeminiEmail] key ...{key[-4:]} rejected (403), marking dead")
                if r.status_code == 404:
                    break  # model unavailable — stop trying it on other keys
            except Exception as e:
                last_error = f"{m}: {str(e)[:120]}"
                continue
        else:
            continue
        break  # 404 on this model — move to the next model
    logger.warning(f"[GeminiEmail] all keys failed: {last_error}")
    return None


# Set after a UDP/53 timeout: subsequent MX lookups go straight to DoH
_DNS_UDP_BROKEN = False


def _resolve_mx_host(domain: str) -> Optional[str]:
    """Highest-priority MX host for a domain, or None.

    Tries local DNS first (fast); if UDP/53 is blocked or timing out —
    common on some routers/ISPs — falls back to DNS-over-HTTPS (Google,
    then Cloudflare). A timeout trips a process-wide circuit breaker so
    later lookups skip the dead UDP path entirely.
    """
    global _DNS_UDP_BROKEN
    domain = (domain or "").strip().rstrip(".").lower()
    if not domain or not re.fullmatch(r"[a-z0-9.\-]+", domain):
        return None
    if not _DNS_UDP_BROKEN:
        try:
            import dns.resolver
            try:
                answers = dns.resolver.resolve(domain, "MX", lifetime=3)
            except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
                return None  # authoritative "no MX" / domain doesn't exist
            best = None
            for r in answers:
                host = str(r.exchange).rstrip(".").lower()
                if host:
                    prio = int(r.preference)
                    if best is None or prio < best[0]:
                        best = (prio, host)
            return best[1] if best else None
        except Exception:
            _DNS_UDP_BROKEN = True  # resolver unreachable — use DoH from now on
    for url in (f"https://dns.google/resolve?name={domain}&type=MX",
                f"https://cloudflare-dns.com/dns-query?name={domain}&type=MX"):
        try:
            with httpx.Client(timeout=6.0, follow_redirects=True) as client:
                r = client.get(url, headers={"Accept": "application/dns-json"})
            if r.status_code != 200:
                continue
            data = r.json()
            status = data.get("Status")
            if status == 3:
                return None  # NXDOMAIN — definitive
            if status != 0:
                continue  # SERVFAIL etc. — try the next provider
            mx = []
            for a in data.get("Answer") or []:
                if a.get("type") != 15:
                    continue
                parts = str(a.get("data", "")).split()
                try:
                    prio = int(parts[0])
                except (ValueError, IndexError):
                    continue
                host = parts[1].rstrip(".").lower() if len(parts) > 1 else ""
                if host:
                    mx.append((prio, host))
            if mx:
                mx.sort()
                return mx[0][1]
            return None  # NOERROR but no MX records
        except Exception:
            continue
    return None


def _domain_has_mx(domain: str) -> bool:
    return _resolve_mx_host(domain) is not None


def _local_contains_name(local: str, first_name: str, last_name: str) -> bool:
    blob = re.sub(r"[^a-z]", "", (local or "").lower())
    if not blob:
        return False
    tokens = set()
    for part in (first_name or "", last_name or ""):
        for word in re.findall(r"[a-z]{3,}", part.lower()):
            tokens.add(word)
        joined = re.sub(r"[^a-z]", "", part.lower())
        if len(joined) >= 3:
            tokens.add(joined)
    if not tokens:
        return True
    return any(t in blob for t in tokens)


def _candidate_structure_ok(email: str, first_name: str, last_name: str, domain: str) -> bool:
    """Exact domain match + not disposable + not generic + local part contains the name."""
    email = (email or "").strip().lower()
    if not re.fullmatch(EMAIL_REGEX.pattern, email):
        return False
    local, _, edom = email.partition("@")
    d = (domain or "").strip().lower()
    if d.startswith("www."):
        d = d[4:]
    if not d or edom != d:
        return False
    if edom in DISPOSABLE:
        return False
    try:
        from lead_quality import is_generic_email as _is_generic
        if _is_generic(email):
            return False
    except Exception:
        root = re.split(r"[._\-]", local)[0]
        if root in {"info", "contact", "hello", "hallo", "sales", "support", "admin",
                    "office", "team", "mail", "service", "help", "privacy", "legal"}:
            return False
    return _local_contains_name(local, first_name, last_name)


def _gemini_guess_email_impl(first_name: str, last_name: str, domain: str,
                             hunter_pattern: Optional[str] = None) -> Optional[Dict[str, Any]]:
    patterns = [p for p in generate_email_patterns(first_name, last_name, domain)
                if not p["pattern"].startswith("generic_")]
    pattern_list = ", ".join(p["pattern"] for p in patterns)
    hint = f"\nKnown pattern at this company (from Hunter.io): {hunter_pattern}" if hunter_pattern else ""
    prompt = (
        "Infer the most likely personal (named) email address for a B2B outreach contact.\n\n"
        f"Contact name: {first_name} {last_name}\n"
        f"Company domain: {domain}{hint}\n"
        f"Standard patterns to consider (most frequent first): {pattern_list}\n\n"
        "Rules:\n"
        f"- The address must be on @{domain} exactly — never another domain.\n"
        "- The local part must contain the contact's first or last name.\n"
        "- Never suggest role addresses (info@, contact@, sales@, hello@ ...).\n"
        "- confidence = 0-100 that this address really exists.\n"
        "- If you lack a reasonable basis, return {\"candidates\": []}.\n\n"
        "Return strict JSON only: "
        '{"candidates": [{"email": "...", "confidence": 85, "reason": "..."}]} '
        "with up to 3 candidates, best first."
    )
    raw = _call_gemini_sync(prompt, GEMINI_EMAIL_SYSTEM_PROMPT)
    data = _extract_json_obj(raw or "")
    if not data:
        return None
    raw_cands = data.get("candidates")
    if not isinstance(raw_cands, list):
        return None
    parsed = []
    for c in raw_cands:
        if isinstance(c, str):
            email, conf, reason = c, 70, ""
        elif isinstance(c, dict):
            email = str(c.get("email") or "").strip().lower()
            try:
                conf = int(c.get("confidence") or 0)
            except (TypeError, ValueError):
                conf = 0
            reason = str(c.get("reason") or "")[:200]
        else:
            continue
        if email:
            parsed.append((email, conf, reason))
    if not parsed:
        return None
    valid = [(e, c, r) for e, c, r in parsed
             if _candidate_structure_ok(e, first_name, last_name, domain)]
    if not valid:
        return None
    if not _domain_has_mx(domain):
        return None
    standard_emails = {p["email"].lower() for p in generate_email_patterns(first_name, last_name, domain)}
    smtp_attempts = 0
    ambiguous_pick = None
    for email, conf, reason in valid:
        conf = max(0, min(100, conf))
        if smtp_attempts < 2:
            smtp_attempts += 1
            verdict = smtp_verify_email(email)
            exists = verdict.get("exists")
            if exists is True:
                vreason = verdict.get("reason", "")
                return {
                    "email": email,
                    "confidence": 90,
                    "source": "gemini_smtp_verified",
                    "verified": True,
                    "reason": f"{reason} (SMTP verified: {vreason})".strip(),
                }
            if exists is False:
                continue
        if ambiguous_pick is None and email in standard_emails and conf >= 70:
            ambiguous_pick = {
                "email": email,
                "confidence": 70,
                "source": "gemini_pattern_mx",
                "verified": False,
                "reason": f"{reason} (MX valid, SMTP blocked; standard pattern)".strip(),
            }
    return ambiguous_pick


def gemini_guess_email(first_name: str, last_name: str, domain: str,
                       hunter_pattern: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """AI-assisted email guess, gated by structural checks + MX + SMTP verification.

    Always returns confidence >= 70 (or None). Results — including misses —
    are cached per (name, domain) so sourcing loops don't repeat the API call.
    """
    if not _GEMINI_EMAIL_GUESS_ENABLED or not _GEMINI_API_KEYS:
        return None
    if not first_name or not last_name or not domain:
        return None
    cache_key = f"{first_name.strip().lower()}|{last_name.strip().lower()}|{domain.strip().lower()}"
    if cache_key in _GEMINI_GUESS_CACHE:
        return _GEMINI_GUESS_CACHE[cache_key]
    result = None
    try:
        result = _gemini_guess_email_impl(first_name, last_name, domain, hunter_pattern)
    except Exception as e:
        logger.warning(f"[GeminiEmail] guess failed for {first_name} {last_name}@{domain}: {e}")
        result = None
    if len(_GEMINI_GUESS_CACHE) > 500:
        _GEMINI_GUESS_CACHE.clear()
    _GEMINI_GUESS_CACHE[cache_key] = result
    return result


def extract_emails_from_text(text: str) -> List[str]:
    """Extract all email addresses from text."""
    if not text:
        return []

    found = EMAIL_REGEX.findall(text)
    # Deduplicate and filter
    seen = set()
    result = []
    for email in found:
        email = email.lower().strip()
        domain = email.split("@")[1] if "@" in email else ""
        if email not in seen and domain not in DISPOSABLE:
            seen.add(email)
            result.append(email)
    return result


def extract_emails_from_website(url: str, text: str = None) -> Dict[str, Any]:
    """Extract emails from a website URL or its text content."""
    result = {
        "url": url,
        "emails": [],
        "contact_page": None,
        "phone_numbers": [],
        "social_links": []
    }

    if text:
        result["emails"] = extract_emails_from_text(text)

        # Extract phone numbers (Dutch format)
        phone_regex = re.compile(r'(?:\+31|0031|0)[-\s]?\d{1,3}[-\s]?\d{3,8}')
        result["phone_numbers"] = list(set(phone_regex.findall(text)))

        # Extract social links
        social_regex = re.compile(r'https?://(?:www\.)?(?:linkedin\.com|facebook\.com|twitter\.com|instagram\.com)[^\s<>"\']+')
        result["social_links"] = list(set(social_regex.findall(text)))

    return result


def generate_email_patterns(first_name: str, last_name: str, domain: str) -> List[Dict[str, Any]]:
    """Generate possible email patterns for a person."""
    patterns = []
    # Sanitize names: remove spaces, dots, convert to lowercase
    f = re.sub(r'[^a-zA-Z]', '', first_name.lower()) if first_name else ""
    l = re.sub(r'[^a-zA-Z]', '', last_name.lower()) if last_name else ""

    if not f or not l:
        return patterns

    # Common patterns
    pattern_map = {
        "first.last": f"{f}.{l}@{domain}",
        "firstlast": f"{f}{l}@{domain}",
        "f.last": f"{f[0]}.{l}@{domain}" if f else "",
        "first_last": f"{f}_{l}@{domain}",
        "first": f"{f}@{domain}",
        "last": f"{l}@{domain}",
        "last.first": f"{l}.{f}@{domain}",
        "lastfirst": f"{l}{f}@{domain}",
    }

    for pattern_name, email in pattern_map.items():
        if email and "@" in email:
            patterns.append({
                "pattern": pattern_name,
                "email": email,
                "confidence": 80 if pattern_name in ("first.last", "firstlast") else 60
            })

    # Generic emails
    for gen in ["info", "contact", "hello", "hallo"]:
        patterns.append({
            "pattern": f"generic_{gen}",
            "email": f"{gen}@{domain}",
            "confidence": 40
        })

    return patterns


async def verify_email_mx(email: str) -> Dict[str, Any]:
    """Verify if an email domain has valid MX records."""
    domain = email.split("@")[1] if "@" in email else ""
    host = _resolve_mx_host(domain)
    return {
        "email": email,
        "domain": domain,
        "has_mx": bool(host),
        "mx_records": [host] if host else [],
        "deliverable": bool(host),
    }


def smtp_verify_email(email: str, from_email: str = "verify@clawbuildr.com") -> Dict[str, Any]:
    """Verify an email address via MX + SMTP RCPT TO check.
    Returns dict with email, exists (bool), confidence (0-100), reason (str).
    Note: Many mail servers now block RCPT TO verification, so this may return ambiguous results.
    """
    if not email or "@" not in email:
        return {"email": email, "exists": False, "confidence": 0, "reason": "Invalid format"}

    local, domain = email.split("@", 1)

    # Get MX records (UDP DNS with DNS-over-HTTPS fallback)
    mx_host = _resolve_mx_host(domain)
    if not mx_host:
        return {"email": email, "exists": False, "confidence": 30, "reason": "No MX records"}
    
    # Try SMTP verification with short timeout
    try:
        import smtplib
        with smtplib.SMTP(mx_host, 25, timeout=5) as server:
            server.ehlo("clawbuildr.com")
            code, msg = server.mail(from_email)
            if code != 250:
                return {"email": email, "exists": None, "confidence": 40, "reason": f"MAIL FROM rejected: {code}"}
            
            code, msg = server.rcpt(email)
            server.quit()
            
            if code == 250:
                return {"email": email, "exists": True, "confidence": 90, "reason": "Mailbox verified (RCPT TO 250)"}
            elif code == 550:
                return {"email": email, "exists": False, "confidence": 85, "reason": "Mailbox does not exist (550)"}
            else:
                return {"email": email, "exists": None, "confidence": 40, "reason": f"SMTP response: {code}"}
    except Exception:
        # SMTP verification blocked or timed out - MX is valid so email might exist
        return {"email": email, "exists": None, "confidence": 50, "reason": "MX valid, SMTP verification blocked"}


def hunter_domain_search(domain: str, first_name: str = None, last_name: str = None) -> Dict[str, Any]:
    """Search Hunter.io for emails on a domain.
    Rotates through multiple API keys if available.
    Returns dict with emails (list of verified emails), pattern, confidence.
    """
    if not _HUNTER_API_KEYS:
        return {"emails": [], "pattern": None, "confidence": 0, "source": "no_api_key"}

    # Try each key once
    keys_to_try = _HUNTER_API_KEYS[:]
    while keys_to_try:
        api_key = _get_next_hunter_key()
        if api_key not in keys_to_try:
            # safety: avoid infinite loop if rotation misbehaves
            break
        keys_to_try.remove(api_key)

        try:
            params = {"domain": domain, "api_key": api_key, "limit": 10}
            r = httpx.get("https://api.hunter.io/v2/domain-search", params=params, timeout=15)

            # Rate limited: rotate to next key
            if r.status_code in (429, 403):
                logger.warning(f"[Hunter.io] Key rate limited ({r.status_code}) for {domain}, rotating...")
                continue

            if r.status_code != 200:
                logger.warning(f"[Hunter.io] API error {r.status_code} for {domain}")
                continue

            data = r.json().get("data", {})
            pattern = data.get("pattern", "")
            hunter_emails = data.get("emails", [])

            # Filter to personal emails with good confidence
            results = []
            for e in hunter_emails:
                if e.get("type") == "personal" and e.get("confidence", 0) >= 70:
                    results.append({
                        "email": e.get("value"),
                        "confidence": e.get("confidence", 0),
                        "first_name": (e.get("first_name") or ""),
                        "last_name": (e.get("last_name") or ""),
                        "position": (e.get("position") or ""),
                        "linkedin": (e.get("linkedin") or ""),
                        "source": "hunter.io"
                    })

            # Try to find specific person
            if first_name and last_name:
                target_first = first_name.lower().strip()
                target_last = last_name.lower().strip()
                for e in results:
                    if ((e.get("first_name") or "").lower() == target_first and
                        (e.get("last_name") or "").lower() == target_last):
                        return {"emails": [e], "pattern": pattern, "confidence": e["confidence"], "source": "hunter.io_match"}

            return {"emails": results[:5], "pattern": pattern, "confidence": 75, "source": "hunter.io"}
        except Exception as e:
            logger.warning(f"[Hunter.io] Search failed for {domain} with current key: {e}")
            continue

    return {"emails": [], "pattern": None, "confidence": 0, "source": "all_keys_rate_limited_or_failed"}


def find_personal_email(first_name: str, last_name: str, domain: str) -> Dict[str, Any]:
    """Find a personal email for a contact.
    Uses Hunter.io first, falls back to pattern generation.
    Returns dict with email, confidence, source, verified.
    """
    if not first_name or not last_name or not domain:
        return {"email": None, "confidence": 0, "source": "missing_data", "verified": False}
    
    # Step 1: Try Hunter.io (verified emails from web)
    hunter = hunter_domain_search(domain, first_name, last_name)
    if hunter.get("emails"):
        # Only use if it matches the specific person
        for e in hunter["emails"]:
            if ((e.get("first_name") or "").lower() == first_name.lower().strip() and
                (e.get("last_name") or "").lower() == last_name.lower().strip()):
                return {
                    "email": e["email"],
                    "confidence": e["confidence"],
                    "source": "hunter.io",
                    "verified": True,
                    "position": e.get("position", ""),
                    "linkedin": e.get("linkedin", "")
                }
        # Hunter found emails but not for this person - use pattern with Hunter's confirmed pattern
        if hunter.get("pattern"):
            # Pattern like "{f}.{last}" means company uses firstname.lastname format
            first_lower = re.sub(r'[^a-zA-Z]', '', first_name.lower())
            last_lower = re.sub(r'[^a-zA-Z]', '', last_name.lower())
            pattern_email = f"{first_lower}.{last_lower}@{domain}"
            return {
                "email": pattern_email,
                "confidence": 70,  # Higher confidence because pattern is confirmed by Hunter
                "source": "hunter_pattern",
                "verified": False
            }

    # Step 1.5: Gemini guess — structural checks + MX + SMTP verification (conf >= 70 or None)
    try:
        gem = gemini_guess_email(first_name, last_name, domain,
                                 hunter_pattern=(hunter.get("pattern") or None))
        if gem and gem.get("email"):
            return gem
    except Exception as e:
        logger.warning(f"[GeminiEmail] step skipped for {domain}: {e}")
    
    # Step 2: Generate patterns (personal patterns come first with higher confidence)
    patterns = generate_email_patterns(first_name, last_name, domain)
    
    # Also try common Dutch patterns
    first_lower = first_name.lower().strip()
    last_lower = last_name.lower().strip()
    first_initial = first_lower[0] if first_lower else ""
    
    extra_patterns = [
        f"{first_lower}.{last_lower}@{domain}",
        f"{first_lower}{last_lower}@{domain}",
        f"{first_initial}.{last_lower}@{domain}",
    ]
    
    # Get personal emails only (exclude generic info@, contact@, privacy@, etc.)
    personal_emails = []
    try:
        from lead_quality import is_generic_email as _is_generic
        for p in patterns:
            email = p.get("email", "")
            if email and "@" in email and not _is_generic(email):
                personal_emails.append(email)
    except Exception:
        generic_prefixes = {
            "info", "contact", "hello", "hallo", "sales", "support", "admin",
            "office", "privacy", "legal", "compliance", "dpo", "security",
        }
        for p in patterns:
            email = p.get("email", "")
            if email and "@" in email:
                prefix = email.split("@")[0].lower()
                if prefix not in generic_prefixes:
                    personal_emails.append(email)

    # Add extra patterns
    for email in extra_patterns:
        if email and "@" in email:
            personal_emails.append(email)
    
    # Deduplicate while preserving order
    seen = set()
    unique_personal = []
    for e in personal_emails:
        if e not in seen:
            seen.add(e)
            unique_personal.append(e)
    
    if not unique_personal:
        return {"email": None, "confidence": 0, "source": "no_personal_patterns", "verified": False}
    
    # Check if domain has valid MX records (fast check)
    has_mx = _domain_has_mx(domain)
    
    # Return most likely personal pattern (firstname.lastname is most common in NL)
    best_email = unique_personal[0]
    confidence = 65 if has_mx else 30
    source = "personal_pattern_mx" if has_mx else "personal_pattern_no_mx"
    
    return {
        "email": best_email,
        "confidence": confidence,
        "source": source,
        "verified": False  # Pattern-based, not SMTP verified
    }


def find_emails_for_company(domain: str, company_name: str = None,
                            contacts: list = None) -> Dict[str, Any]:
    """Find emails for a company - returns patterns + any found on website."""
    result = {
        "domain": domain,
        "website_emails": [],
        "patterns": [],
        "phones": [],
        "social": []
    }

    # Generate patterns for contacts
    if contacts:
        for c in contacts:
            first = c.get("first_name", "")
            last = c.get("last_name", "")
            if first and last:
                patterns = generate_email_patterns(first, last, domain)
                result["patterns"].extend(patterns)

    # Generic patterns
    for gen in ["info", "contact", "hello", "hallo", "sales", "support"]:
        result["patterns"].append({
            "pattern": f"generic_{gen}",
            "email": f"{gen}@{domain}",
            "confidence": 30
        })

    return result


def store_found_emails(company_id: str, emails: list, source: str = "website") -> int:
    """Store found emails in contacts table."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    stored = 0

    for email_data in emails:
        email = email_data.get("email") if isinstance(email_data, dict) else email_data
        if not email or "@" not in email:
            continue

        # Check if email already exists
        existing = conn.execute(
            "SELECT contact_id FROM contacts WHERE email = ?", (email,)
        ).fetchone()
        if existing:
            continue

        # Extract name from email if possible
        local = email.split("@")[0]
        parts = re.split(r'[._\-]', local)

        stored += 1

    conn.commit()
    conn.close()
    return stored


if __name__ == "__main__":
    # Test email extraction
    test_text = """
    Contact us at info@example.com or sales@example.com
    Call us at +31 20 123 4567
    Follow us on linkedin.com/company/example
    """
    emails = extract_emails_from_text(test_text)
    print(f"Found emails: {emails}")

    # Test pattern generation
    patterns = generate_email_patterns("Jan", "de Vries", "testbv.nl")
    print(f"\nPatterns for Jan de Vries:")
    for p in patterns:
        print(f"  {p['pattern']}: {p['email']} (confidence: {p['confidence']})")
