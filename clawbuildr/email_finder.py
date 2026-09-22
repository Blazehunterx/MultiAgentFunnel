"""
Email Finder - Extract emails from websites and LinkedIn profiles.
Uses pattern matching, common email formats, MX verification, and Hunter.io.
"""

import re
import os
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
    try:
        import dns.resolver
        domain = email.split("@")[1]
        mx_records = dns.resolver.resolve(domain, "MX")
        return {
            "email": email,
            "domain": domain,
            "has_mx": True,
            "mx_records": [str(r.exchange) for r in mx_records],
            "deliverable": True
        }
    except Exception as e:
        return {
            "email": email,
            "domain": email.split("@")[1] if "@" in email else "",
            "has_mx": False,
            "mx_records": [],
            "deliverable": False,
            "error": str(e)
        }


def smtp_verify_email(email: str, from_email: str = "verify@clawbuildr.com") -> Dict[str, Any]:
    """Verify an email address via MX + SMTP RCPT TO check.
    Returns dict with email, exists (bool), confidence (0-100), reason (str).
    Note: Many mail servers now block RCPT TO verification, so this may return ambiguous results.
    """
    import dns.resolver
    
    if not email or "@" not in email:
        return {"email": email, "exists": False, "confidence": 0, "reason": "Invalid format"}
    
    local, domain = email.split("@", 1)
    
    # Get MX records (fast check)
    try:
        mx_records = dns.resolver.resolve(domain, "MX", lifetime=5)
        mx_host = str(mx_records[0].exchange).rstrip(".")
    except dns.resolver.NoAnswer:
        return {"email": email, "exists": False, "confidence": 30, "reason": "No MX records"}
    except dns.resolver.NXDOMAIN:
        return {"email": email, "exists": False, "confidence": 10, "reason": "Domain does not exist"}
    except Exception as e:
        return {"email": email, "exists": None, "confidence": 20, "reason": f"MX lookup failed: {e}"}
    
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
            first_lower = first_name.lower().strip()
            last_lower = last_name.lower().strip()
            pattern_email = f"{first_lower}.{last_lower}@{domain}"
            return {
                "email": pattern_email,
                "confidence": 70,  # Higher confidence because pattern is confirmed by Hunter
                "source": "hunter_pattern",
                "verified": False
            }
    
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
    
    # Get personal emails only (exclude generic info@, contact@, etc.)
    generic_prefixes = {"info", "contact", "hello", "hallo", "sales", "support", "admin", "office"}
    personal_emails = []
    
    # From generate_email_patterns (personal patterns first)
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
    import dns.resolver
    try:
        mx_records = dns.resolver.resolve(domain, "MX", lifetime=5)
        has_mx = True
    except Exception:
        has_mx = False
    
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
