#!/usr/bin/env python3
"""
ClawBuildr Lead Generator — Multi-source lead discovery.
Works without web search engines (which are geo-locked/rate-limited).
Uses: Hunter.io API, direct website scraping, email pattern generation, MX verification.
"""

import os
import sys
import json
import re
import uuid
import time
import random
import sqlite3
import asyncio
import httpx
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

# Dutch + English placeholder/test emails that must never be used as leads
PLACEHOLDER_EMAILS = {
    "naam@voorbeeld.com", "naam@bedrijf.nl", "name@example.com",
    "test@test.com", "email@example.com", "info@example.com",
    "yourname@company.com", "jouw@email.nl", "voorbeeld@email.nl",
    "voorbeeld@bedrijf.nl", "naam@email.nl", "naam@company.nl",
    "test@email.com", "demo@company.com", "example@example.com",
}

# Local parts that indicate placeholder/fake contacts
PLACEHOLDER_LOCAL_PARTS = {"naam", "voorbeeld", "test", "demo", "example", "jouw", "uw", "uwnaam"}

logger = logging.getLogger("ClawBuildr.LeadGenerator")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_GEMINI_API_KEYS = []
_HUNTER_API_KEYS = []
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("GEMINI_API_KEYS="):
            _GEMINI_API_KEYS = [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]
        elif line.startswith("HUNTER_API_KEY="):
            _HUNTER_API_KEYS = [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]

_hunter_idx = 0

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"

GENERIC_PREFIXES = {
    "info", "contact", "hello", "hi", "support", "sales", "admin",
    "webmaster", "noreply", "no-reply", "mail", "office", "team",
    "enquiries", "help", "service", "careers", "jobs", "hr",
    "marketing", "pr", "social", "partners", "media", "press",
    "billing", "accounts", "payroll", "reception", "general",
    "director", "ceo", "founder", "owner", "manager", "staff",
    "customerservice", "klantenservice", "secretariaat", "directie",
    "inkoop", "verkoop", "financien", "personeel", "ict", "it",
    "automatisering", "communicatie", "juridisch", "legal",
}

SKIP_DOMAINS = {
    "google.com", "youtube.com", "facebook.com", "linkedin.com", "wikipedia.org",
    "reddit.com", "twitter.com", "x.com", "tiktok.com", "instagram.com",
    "kvk.nl", "overheid.nl", "rijksoverheid.nl", "mkb.nl",
    "mojeek.com", "brave.com", "duckduckgo.com", "bing.com",
    "yoys.nl", "f6s.com", "clutch.co", "sortlist.com",
    "indeed.nl", "monsterboard.nl", "marktplaats.nl",
    # Large corps — not ICP (small service businesses)
    "siemens.com", "bmw.de", "sap.com", "deutsche-bank.de",
    "allianz.de", "thyssenkrupp.de", "bayer.de", "basf.com",
    "ey.com", "kpmg.nl", "pwc.nl", "deloitte.nl",
    "capgemini.com", "cgi.com", "accenture.com",
    "dentsu.com", "groupm.com", "bam.nl", "heijmans.nl",
    # News, media, blogs, recipes, education — not B2B
    "nytimes.com", "bbc.com", "cnn.com", "reuters.com", "bloomberg.com",
    "forbes.com", "techcrunch.com", "mashable.com", "buzzfeed.com",
    "medium.com", "substack.com", "wordpress.com", "blogspot.com",
    "github.com", "stackoverflow.com", "quora.com", "medium.com",
    # Dutch media
    "nu.nl", "ad.nl", "telegraaf.nl", "volkskrant.nl", "nrc.nl",
    "rtlnieuws.nl", "nos.nl", "hetparool.nl", "trouw.nl",
    # Recipe, lifestyle, travel
    "allrecipes.com", "epicurious.com", "foodnetwork.com",
    "tripadvisor.com", "booking.com", "airbnb.com",
    # Study, education
    "studocu.com", "coursehero.com", "chegg.com",
    "Coursera.org", "udemy.com", "edx.org",
    # Tech platforms
    "apple.com", "microsoft.com", "amazon.com", "netflix.com",
    "spotify.com", "uber.com", "airbnb.com",
}

# Domains that are definitely NOT B2B service companies
NON_B2B_KEYWORDS = {
    "recipe", "cooking", "food", "blog", "news", "media", "press",
    "travel", "hotel", "restaurant", "cafe", "shop", "store", "buy",
    "download", "free", "stream", "watch", "play", "game", "music",
    "movie", "video", "photo", "image", "design", "art", "fashion",
    "health", "fitness", "yoga", "wellness", "beauty", "makeup",
    "dating", "love", "sex", "adult", "porn", "casino", "gambling",
    "crypto", "bitcoin", "forex", "trading", "investment",
    "study", "learn", "course", "tutorial", "university", "school",
    "government", "gov", "municipality", "gemeente",
}


def _is_valid_business_domain(domain: str) -> bool:
    """Check if domain looks like a real B2B service company in NL/BE/DE."""
    domain = domain.lower().strip()

    # CRITICAL: Only accept Dutch/Belgian/German domains
    if not (domain.endswith('.nl') or domain.endswith('.be') or domain.endswith('.de')):
        return False

    # Skip known non-business domains
    if domain in SKIP_DOMAINS:
        return False

    # Skip domains with non-B2B keywords
    for keyword in NON_B2B_KEYWORDS:
        if keyword in domain:
            return False

    # Skip domains that look like personal blogs or platforms
    if any(x in domain for x in ["blog", "news", "media", "press", "magazine", "journal"]):
        return False

    # Skip government domains
    if any(x in domain for x in [".gov", ".edu", ".org"]):
        return False

    # Skip free email providers
    if any(x in domain for x in ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com"]):
        return False

    return True

# ═══════════════════════════════════════════════════════════════════════════════
# KNOWN DUTCH/BE/DE MKB DOMAINS — 5-200 employees, decision makers accessible
# All .nl/.be/.de only. No large corps, no government, no healthcare, no edu.
# ═══════════════════════════════════════════════════════════════════════════════

KNOWN_BUSINESS_DOMAINS = [
    # ── Accountancy & Finance (MKB) ──
    "bakertilly.nl", "acconavm.nl", "crowe.nl", "grantthornton.nl",
    "samaccountants.nl", "faaccountants.nl", "countus.nl",
    "vhaccountants.nl", "smeetsweerts.nl", "wtt.nl", "govers.nl",
    "balknl.nl", "loonkosten.nl", "moore.nl", "pkf-oostendorp.nl",
    "bdo.nl", "jansenaccountants.nl", "schaap-en-partners.nl",
    "akkermansaccountants.nl", "vvaa.nl", "reynwijzer.nl", "laurus.nl",
    "midt.nl", "accountant.nl", "boekhouder.nl", "finci.nl",
    "e-boekhouden.nl", "snelstart.nl", "moneybird.com", "twinfield.com",

    # ── Legal (MKB) ──
    "hvglaw.nl", "debrauw.com", "stibbe.com", "bounce.law",
    "legalcommunity.nl", "aware.nl", "notaris.nl", "notariswinkel.nl",

    # ── IT & Digital (MKB) ──
    "xfour.nl", "finalist.nl", "afas.nl", "exact.com", "unit4.com",
    "topdesk.com", "before9am.nl", "nettles.nl", "byte.nl",
    "combellgroup.com", "transip.nl", "denit.nl",

    # ── Marketing & Creative (MKB) ──
    "inezia.nl", "dictator.nl", "fabrique.nl", "totaldesign.nl",
    "madebydolphi.nl", "codegorilla.nl", "weareyou.nl",

    # ── Construction & Trades (MKB) ──
    "completed.nl", "smitsbouw.nl",

    # ── Recruitment & HR (MKB) ──
    "devotedpeople.nl", "progresshr.nl", "undutchables.nl",
    "stressadvies.nl", "harting.nl", "ictergezocht.nl",
    "runtime.nl", "progmana.nl",

    # ── Architecture & Engineering (MKB) ──
    "witteveenbos.com", "iv-infra.nl", "rhdhv.com", "stevin.com",

    # ── Logistics (MKB) ──
    "chain-logistics.nl", "sluyter-logistics.nl",

    # ── Fintech & Payments (MKB) ──
    "mollie.com", "pay.nl", "buckaroo.nl", "multisafepay.com",
    "icepay.com",

    # ── Consulting (MKB) ──
    "calco.nl", "implan.nl",

    # ── Belgian SMEs ──
    "dobbelsteen.be", "bedrijfsrevisoren.be", "fpsb.be", "era.be",

    # ── German SMEs ──
    "festo.com", "kuka.com", "trumpf.com", "ziegler.de",
]


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _next_hunter_key() -> str:
    global _hunter_idx
    if not _HUNTER_API_KEYS:
        return ""
    key = _HUNTER_API_KEYS[_hunter_idx % len(_HUNTER_API_KEYS)]
    _hunter_idx += 1
    return key


# ═══════════════════════════════════════════════════════════════════════════════
# HUNTER.IO — Find employees at a company domain
# ═══════════════════════════════════════════════════════════════════════════════

def hunter_domain_search(domain: str, limit: int = 10) -> Dict[str, Any]:
    key = _next_hunter_key()
    if not key:
        return {"success": False, "error": "No Hunter API key"}

    try:
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                "https://api.hunter.io/v2/domain-search",
                params={"domain": domain, "api_key": key, "limit": limit, "type": "personal"},
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                emails = []
                for e in data.get("emails", []):
                    emails.append({
                        "email": e.get("value", ""),
                        "first_name": e.get("first_name", ""),
                        "last_name": e.get("last_name", ""),
                        "role": e.get("position", ""),
                        "confidence": e.get("confidence", 0),
                        "type": e.get("type", ""),
                        "linkedin": e.get("linkedin", ""),
                    })
                return {
                    "success": True,
                    "domain": domain,
                    "organization": data.get("organization", domain),
                    "emails": emails,
                    "total": data.get("total", 0),
                }
            elif resp.status_code == 429:
                return {"success": False, "error": "Rate limited"}
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════════════════════
# WEBSITE SCRAPING — Extract emails, names, roles from company site
# ═══════════════════════════════════════════════════════════════════════════════

async def scrape_company_website(domain: str) -> Dict[str, Any]:
    """Scrape company website for emails. Prioritize /team and /about pages (decision makers)."""
    result = {"emails": [], "names": [], "text": "", "pages_scraped": 0}
    # PRIORITY ORDER: team pages first (real people), then about, skip homepage (generics)
    paths = ["/team", "/over-ons", "/about", "/ons-team", "/people", "/medewerkers", "/contact"]

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False, headers={"User-Agent": UA}) as client:
            for path in paths[:4]:
                try:
                    r = await client.get(f"https://{domain}{path}", timeout=8)
                    if r.status_code != 200:
                        continue

                    text = re.sub(r'<[^>]+>', ' ', r.text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    result["text"] += text[:2000] + " "
                    result["pages_scraped"] += 1

                    emails_found = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', r.text)
                    for e in emails_found:
                        e_lower = e.lower()
                        if e_lower not in [x.lower() for x in result["emails"]]:
                            prefix = e.split("@")[0].lower()
                            if prefix not in GENERIC_PREFIXES and not e_lower.endswith((".png", ".jpg", ".gif")):
                                result["emails"].append(e)

                    name_patterns = [
                        r'(?:Eigenaar|Directeur|CEO|Founder|Oprichter|Manager)\s*[:\-]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)',
                        r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[,]\s*(?:Eigenaar|Directeur|CEO|Founder)',
                        r'(?:ik ben|wij zijn|maak kennis met)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    ]
                    for pat in name_patterns:
                        for nm in re.finditer(pat, text, re.IGNORECASE):
                            result["names"].append(nm.group(1).strip())

                    if result["pages_scraped"] >= 2:
                        break
                except Exception:
                    continue
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL PATTERN GENERATION + MX VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def generate_email_patterns(first_name: str, last_name: str, domain: str) -> List[str]:
    f = first_name.lower().strip()
    l = last_name.lower().strip()
    if not f or not l:
        return []
    patterns = [
        f"{f}.{l}@{domain}",
        f"{f}{l}@{domain}",
        f"{f[0]}.{l}@{domain}",
        f"{f}_{l}@{domain}",
        f"{l}.{f}@{domain}",
    ]
    return list(dict.fromkeys(patterns))


def verify_mx(domain: str) -> bool:
    try:
        import dns.resolver
        mx = dns.resolver.resolve(domain, "MX")
        return len(list(mx)) > 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# INDUSTRY CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_industry(domain: str, text: str) -> str:
    combined = (domain + " " + text).lower()
    mapping = {
        "accountant": "Financiele Dienstverlening", "boekhouder": "Financiele Dienstverlening",
        "belasting": "Financiele Dienstverlening", "financieel": "Financiele Dienstverlening",
        "advocaat": "Juridisch", "notaris": "Juridisch", "juridisch": "Juridisch",
        "makelaar": "Vastgoed", "vastgoed": "Vastgoed",
        "architect": "Architectuur", "ingenieur": "Engineering",
        "it": "IT & Software", "software": "IT & Software", "saas": "IT & Software",
        "webdesign": "Webdesign", "marketing": "Marketing",
        "reinig": "Schoonmaak", "schoonmaak": "Schoonmaak",
        "tuin": "Tuin & Landschap", "bouw": "Bouw",
        "verzekering": "Verzekeringen", "transport": "Logistiek",
        "fitness": "Sport", "fysio": "Gezondheid",
    }
    for kw, ind in mapping.items():
        if kw in combined:
            return ind
    return "B2B"


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL CONFIDENCE SCORING — quality gate for scraped emails
# ═══════════════════════════════════════════════════════════════════════════════

def score_scraped_email(email: str, first_name: str = "", last_name: str = "", page_source: str = "") -> int:
    """Score a scraped email 0-100. Higher = more likely to be a real decision maker."""
    score = 0
    prefix = email.split("@")[0].lower()
    domain = email.split("@")[1].lower() if "@" in email else ""

    # Prefix quality (40 points max)
    if prefix in GENERIC_PREFIXES:
        return 0  # Never accept generic emails
    if "." in prefix or "_" in prefix:
        # firstname.lastname pattern — most likely real person
        parts = prefix.replace(".", " ").replace("_", " ").split()
        if len(parts) >= 2 and all(len(p) >= 2 for p in parts):
            score += 40  # Strong signal
        elif len(parts) == 2:
            score += 25  # Okay signal
    elif prefix.isalpha() and len(prefix) >= 3:
        # Single name — possible but less certain
        score += 15

    # Name match bonus (20 points max)
    if first_name and last_name:
        fn = first_name.lower()
        ln = last_name.lower()
        if prefix.startswith(fn) or prefix.endswith(ln):
            score += 20
        elif fn in prefix or ln in prefix:
            score += 10

    # Domain quality (20 points max)
    if domain.endswith(".nl"):
        score += 15  # Dutch — strong
    elif domain.endswith(".be") or domain.endswith(".de"):
        score += 12  # Belgian/German — good
    else:
        score += 5   # Other — weak

    # Page source quality (20 points max)
    if page_source:
        text_lower = page_source.lower()
        # Found on team/about page = high quality
        if any(x in text_lower for x in ["team", "medewerker", "over ons", "about us", "ontmoet"]):
            score += 15
        # Found near job titles = high quality
        if any(x in text_lower for x in ["directeur", "eigenaar", "manager", "ceo", "founder"]):
            score += 5

    return min(score, 100)


def _is_quality_email(email: str, first_name: str = "", last_name: str = "", page_source: str = "") -> bool:
    """Quick quality gate — reject emails below threshold."""
    confidence = score_scraped_email(email, first_name, last_name, page_source)
    return confidence >= 30  # Minimum threshold


# ═══════════════════════════════════════════════════════════════════════════════
# INSERT INTO DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def insert_lead(
    company_name: str, domain: str, first_name: str, last_name: str,
    email: str, role: str = "Decision Maker", linkedin_url: str = "",
    industry: str = "", source: str = "lead_generator",
) -> Optional[str]:
    # FILTER: Skip placeholder/test emails
    email_lower = email.lower().strip()
    local_part = email_lower.split("@")[0] if "@" in email_lower else ""
    if email_lower in PLACEHOLDER_EMAILS or local_part in PLACEHOLDER_LOCAL_PARTS:
        return None
    if not email_lower or "@" not in email_lower:
        return None
    if " " in email or "\t" in email:
        return None

    # FILTER: Skip non-business domains
    if not _is_valid_business_domain(domain):
        return None

    # FILTER: Skip free email providers
    free_providers = {
        "gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com",
        "aol.com", "icloud.com", "protonmail.com", "zoho.com", "yandex.com",
        "mail.com", "gmx.com", "fastmail.com", "tutanota.com", "hushmail.com",
        "mail.ru", "bk.ru", "inbox.ru", "list.ru",
    }
    if domain_lower in free_providers:
        return None

    db = _get_db()
    try:
        existing = db.execute(
            "SELECT contact_id FROM contacts WHERE email = ? OR (first_name = ? AND last_name = ? AND company_id IN (SELECT company_id FROM companies WHERE domain = ?))",
            (email, first_name, last_name, domain),
        ).fetchone()
        if existing:
            return None

        company_id = f"co_{uuid.uuid4().hex[:8]}"
        db.execute(
            """INSERT INTO companies (company_id, name, domain, industry, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (company_id, company_name, domain, industry, datetime.now(timezone.utc).isoformat()),
        )

        contact_id = f"ct_{uuid.uuid4().hex[:8]}"
        db.execute(
            """INSERT INTO contacts
               (contact_id, company_id, first_name, last_name, email, role,
                linkedin_url, current_stage, lead_score, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'INGESTED', 50, ?, ?)""",
            (contact_id, company_id, first_name, last_name, email, role,
             linkedin_url, datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()),
        )

        db.execute(
            """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
               VALUES (?, 'lead_generated', ?, ?, ?)""",
            (contact_id, f"Lead generated from {source}", json.dumps({"source": source, "domain": domain}),
             datetime.now(timezone.utc).isoformat()),
        )

        db.commit()
        logger.info(f"Inserted lead: {first_name} {last_name} ({email}) at {company_name}")
        return contact_id
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        db.rollback()
        return None
    finally:
        db.close()


def _get_known_domains() -> set:
    db = _get_db()
    try:
        rows = db.execute("SELECT domain FROM companies WHERE domain IS NOT NULL").fetchall()
        return {r["domain"].lower() for r in rows if r["domain"]}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

async def generate_leads(
    domains: List[str] = None,
    max_leads: int = 50,
    use_hunter: bool = True,
    use_website_scrape: bool = True,
) -> Dict[str, Any]:
    """Run the lead generation pipeline.

    Uses Hunter.io to find employees at known business domains,
    then scrapes websites for additional contacts.
    """
    if not domains:
        domains = list(KNOWN_BUSINESS_DOMAINS)
        random.shuffle(domains)

    known_domains = _get_known_domains()
    stats = {"found": 0, "duplicates": 0, "errors": 0, "domains_processed": 0, "leads": []}
    inserted_ids = []

    # Also get domains with 3+ active contacts (skip these)
    db = _get_db()
    crowded = {r[0].lower() for r in db.execute(
        """SELECT co.domain FROM companies co
           JOIN contacts c ON c.company_id = co.company_id
           WHERE c.current_stage NOT IN ('CLOSED_LOST','BOUNCED')
           GROUP BY co.domain HAVING COUNT(*) >= 3"""
    ).fetchall()}
    db.close()

    for domain in domains:
        if stats["found"] >= max_leads:
            break

        domain = domain.lower().strip()
        if domain in known_domains or domain in crowded:
            stats["duplicates"] += 1
            continue

        stats["domains_processed"] += 1
        company_name = domain.split(".")[0].replace("-", " ").title()

        # Try Hunter.io first
        if use_hunter:
            try:
                hunter = hunter_domain_search(domain, limit=5)
                if hunter.get("success") and hunter.get("emails"):
                    company_name = hunter.get("organization", company_name)
                    for emp in hunter["emails"]:
                        if stats["found"] >= max_leads:
                            break
                        if not emp.get("first_name") or not emp.get("last_name"):
                            continue
                        if emp.get("confidence", 0) < 70:
                            continue
                        prefix = emp["email"].split("@")[0].lower()
                        if prefix in GENERIC_PREFIXES:
                            continue

                        industry = _classify_industry(domain, emp.get("role", ""))
                        cid = insert_lead(
                            company_name=company_name, domain=domain,
                            first_name=emp["first_name"], last_name=emp["last_name"],
                            email=emp["email"], role=emp.get("role", "Contact"),
                            linkedin_url=emp.get("linkedin", ""),
                            industry=industry, source="hunter_io",
                        )
                        if cid:
                            stats["found"] += 1
                            inserted_ids.append(cid)
                            stats["leads"].append({
                                "name": f"{emp['first_name']} {emp['last_name']}",
                                "email": emp["email"],
                                "company": company_name,
                                "domain": domain,
                            })
                            known_domains.add(domain)
                    await asyncio.sleep(1)
                    continue
                elif hunter.get("error") == "Rate limited":
                    logger.info("Hunter.io rate limited, waiting 5s...")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.debug(f"Hunter failed for {domain}: {e}")

        # Fallback: scrape website (only team/about pages)
        if use_website_scrape and stats["found"] < max_leads:
            try:
                scrape = await scrape_company_website(domain)
                if scrape.get("emails"):
                    industry = _classify_industry(domain, scrape.get("text", ""))
                    for email_addr in scrape["emails"][:3]:
                        if stats["found"] >= max_leads:
                            break
                        prefix = email_addr.split("@")[0].lower()
                        if prefix in GENERIC_PREFIXES:
                            continue

                        name_parts = prefix.replace(".", " ").replace("_", " ").replace("-", " ").split()
                        if len(name_parts) >= 2:
                            fn = name_parts[0].capitalize()
                            ln = " ".join(p.capitalize() for p in name_parts[1:])
                        elif scrape.get("names"):
                            name = scrape["names"][0]
                            parts = name.split()
                            fn = parts[0] if parts else "Contact"
                            ln = " ".join(parts[1:]) if len(parts) > 1 else ""
                        else:
                            continue

                        if fn.lower() in GENERIC_PREFIXES or fn == "Contact":
                            continue

                        # QUALITY GATE: score email before inserting
                        confidence = score_scraped_email(email_addr, fn, ln, scrape.get("text", ""))
                        if confidence < 30:
                            logger.debug(f"[LeadGen] Low confidence ({confidence}) for {email_addr} @ {domain} — skipping")
                            continue

                        cid = insert_lead(
                            company_name=company_name, domain=domain,
                            first_name=fn, last_name=ln,
                            email=email_addr, role="Contactpersoon",
                            industry=industry, source="website_scrape",
                        )
                        if cid:
                            stats["found"] += 1
                            inserted_ids.append(cid)
                            stats["leads"].append({
                                "name": f"{fn} {ln}",
                                "email": email_addr,
                                "company": company_name,
                                "domain": domain,
                                "confidence": confidence,
                            })
                            known_domains.add(domain)
                            break
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.debug(f"Scrape failed for {domain}: {e}")

    logger.info(
        f"[LeadGen] Complete: {stats['found']} leads, "
        f"{stats['duplicates']} dupes, {stats['errors']} errors, "
        f"{stats['domains_processed']} domains processed"
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="ClawBuildr Lead Generator")
    parser.add_argument("--max", type=int, default=20, help="Max leads to generate")
    parser.add_argument("--domains", nargs="*", help="Custom domain list")
    parser.add_argument("--no-hunter", action="store_true")
    parser.add_argument("--no-scrape", action="store_true")
    args = parser.parse_args()

    result = asyncio.run(generate_leads(
        domains=args.domains, max_leads=args.max,
        use_hunter=not args.no_hunter, use_website_scrape=not args.no_scrape,
    ))

    print(f"\n=== Lead Generation Complete ===")
    print(f"Leads found: {result['found']}")
    print(f"Duplicates: {result['duplicates']}")
    print(f"Domains processed: {result['domains_processed']}")
    if result["leads"]:
        print(f"\nTop leads:")
        for lead in result["leads"][:10]:
            print(f"  {lead['name']} <{lead['email']}> @ {lead['company']}")
