import asyncio
import json
import logging
import os
import random
import re
import sqlite3
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import httpx

from clawbuildr.icp_scorer import score_company, get_icp_config
from clawbuildr.query_adaptation import (
    get_adapted_queries,
    record_query_result,
    get_learning_stats,
    generate_new_queries,
)

logger = logging.getLogger("learning_scraper")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")

# Max companies to discover per day
DAILY_CAP = 200

# Delay between search queries (seconds)
QUERY_DELAY = 4

# Max concurrent scrapes
_SCRAPE_SEMAPHORE = asyncio.Semaphore(3)

# Domains to skip (same as dashboard)
_SKIP_DOMAINS = {
    "rijksoverheid.nl", "overheid.nl", "mkb.nl", "kvk.nl", "nederlanddigitaal.nl",
    "google.com", "youtube.com", "facebook.com", "linkedin.com", "wikipedia.org",
    "reddit.com", "tweakers.net", "nu.nl", "nos.nl", "ad.nl", "telegraaf.nl",
    "fd.nl", "instagram.com", "twitter.com", "x.com", "tiktok.com", "pinterest.com",
    "mojeek.com", "brave.com", "duckduckgo.com", "bing.com", "startpage.com",
    "yoys.nl", "f6s.com", "europages.nl", "clutch.co", "sortlist.com",
    "indeed.nl", "monsterboard.nl", "marktplaats.nl",
    "trustpilot.com", "g2.com", "coursera.org", "udemy.com",
}

# Industry detection keywords
_INDUSTRY_KEYWORDS = {
    "Groothandel": ["groothandel", "wholesale", "distributie", "import", "export", "groothandels"],
    "Logistiek & Transport": ["logistiek", "transport", "opslag", "warehousing", "supply chain", "versnijding"],
    "B2B SaaS": ["saas", "software as a service", "cloud platform", "applicatie"],
    "IT Dienstverlening": ["ict", "it-dienst", "consultancy", "automatisering", "digitalisering", "software ontwikkeling"],
    "Advies & Consultancy": ["adviesbureau", "consultancy", "management consulting", "advisory"],
    "Financiele Dienstverlening": ["accountant", "boekhouder", "belastingadviseur", "financieel"],
    "Juridisch": ["advocaat", "legal", "juridisch", "notaris"],
    "Vastgoed": ["vastgoed", "makelaar", "real estate", "onroerend goed"],
    "Marketing": ["marketing", "reclame", "advertising", "communicatie"],
    "HR & Recruitment": ["recruitment", "uitzendbureau", "hr-", "personeelsdiensten"],
}


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _today_count() -> int:
    """Get how many companies were discovered today."""
    conn = _get_db()
    count = conn.execute(
        "SELECT COUNT(*) as cnt FROM scraper_learning WHERE created_at > date('now')"
    ).fetchone()["cnt"]
    conn.close()
    return count


def _domain_exists(domain: str) -> bool:
    """Check if domain already exists in companies table."""
    conn = _get_db()
    row = conn.execute(
        "SELECT 1 FROM companies WHERE domain = ?", (domain,)
    ).fetchone()
    conn.close()
    return row is not None


def _queue_exists(domain: str) -> bool:
    """Check if domain is already in discovery queue."""
    conn = _get_db()
    row = conn.execute(
        "SELECT 1 FROM discovery_queue WHERE domain = ?", (domain,)
    ).fetchone()
    conn.close()
    return row is not None


def _detect_industry(text: str, query: str) -> str:
    """Detect industry from scraped text and query."""
    combined = (text + " " + query).lower()
    scores = {}
    for industry, keywords in _INDUSTRY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[industry] = score

    if scores:
        return max(scores, key=scores.get)
    return "Overig"


def _extract_personal_name(emails: list, text: str) -> tuple:
    """Extract a personal name from emails or text. Returns (first, last, role)."""
    generic = {"info", "contact", "hello", "support", "sales", "admin", "webmaster",
               "noreply", "mail", "office", "team", "enquiries", "help", "service"}

    # Try email prefix
    for email in emails:
        prefix = email.split("@")[0].lower()
        if prefix not in generic and not prefix.startswith("info"):
            parts = re.split(r'[._\-]', prefix)
            if len(parts) >= 2:
                return (parts[0].capitalize(), parts[1].capitalize(), "Unknown")
            elif len(parts) == 1 and len(parts[0]) > 2:
                return (parts[0].capitalize(), "", "Unknown")

    # Try text patterns
    role_kw = r'(?:Eigenaar|Directeur|CEO|Founder|Oprichter|Manager)'
    patterns = [
        r'(?:Naam|Contact|' + role_kw + r')\s*:\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        r'([A-Z][a-z]+(?:\s+(?:van|de|der)\s+)?[A-Z][a-z]+)\s*(?:-|–)\s*(?:' + role_kw + r')',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip()
            parts = name.split()
            if len(parts) >= 2:
                return (parts[0], parts[-1], "Unknown")

    return ("Unknown", "", "Unknown")


async def _search_web(query: str) -> List[Dict]:
    """Search multiple sources and return results."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"}

    # Try Yelp first (most reliable for business discovery)
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False, headers=headers) as client:
            # Extract location from query if present
            location = "Nederland"
            search_term = query

            # Remove generic B2B/IT terms that don't help Yelp search
            for generic in ["B2B", "IT", "ICT", "MKB", "Klein", "Groot"]:
                search_term = search_term.replace(generic, "").strip()

            # Try to find a city name
            for city in ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Eindhoven", "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen"]:
                if city.lower() in query.lower():
                    location = city
                    search_term = search_term.replace(city, "").strip()
                    break

            # Clean up search term
            search_term = " ".join(search_term.split())  # Remove extra spaces
            if not search_term:
                search_term = "bedrijf"

            r = await client.get("https://www.yelp.nl/search", params={"find_desc": search_term, "find_loc": location})
            if r.status_code == 200:
                results = _parse_yelp_results(r.text, query)
                if results:
                    logger.info(f"Yelp found {len(results)} results for: {query[:50]}")
                    return results
    except Exception as e:
        logger.warning(f"Yelp error: {e}")

    # Try Brave search
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False, headers=headers) as client:
                r = await client.get("https://search.brave.com/search", params={"q": query})
                if r.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.info(f"Brave 429, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status_code == 200:
                    return _parse_search_results(r.text, query)
        except Exception as e:
            logger.warning(f"Brave error: {e}")
            await asyncio.sleep(3)

    # Fallback to Startpage
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False, headers=headers) as client:
            r = await client.get("https://www.startpage.com/do/search", params={"q": query})
            if r.status_code == 200 and "captcha" not in r.text.lower():
                logger.info(f"Startpage fallback for: {query[:50]}")
                return _parse_search_results(r.text, query)
    except Exception as e:
        logger.warning(f"Startpage error: {e}")

    # Fallback to Mojeek
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False, headers=headers) as client:
            r = await client.get("https://www.mojeek.com/search", params={"q": query})
            if r.status_code == 200:
                logger.info(f"Mojeek fallback for: {query[:50]}")
                return _parse_search_results(r.text, query)
    except Exception as e:
        logger.warning(f"Mojeek error: {e}")

    return []


def _parse_yelp_results(html: str, query: str) -> List[Dict]:
    """Parse Yelp search results into company data."""
    results = []
    seen_slugs = set()

    # Extract business slugs from /biz/ links
    for m in re.finditer(r'/biz/([a-z0-9-]+?)(?:\?|$|#)', html):
        slug = m.group(1)
        if slug in seen_slugs or len(slug) < 3:
            continue
        seen_slugs.add(slug)

        # Convert slug to readable name
        # e.g., "sligro-amsterdam" -> "Sligro Amsterdam"
        name_parts = slug.split("-")
        # Remove trailing city names
        cities = ["amsterdam", "rotterdam", "utrecht", "den-haag", "eindhoven", "tilburg", "groningen", "almere", "breda", "nijmegen"]
        name_parts = [p for p in name_parts if p not in cities]
        name = " ".join(name_parts).title()

        # Skip generic results
        if name.lower() in ("search", "login", "signup", "support", "about"):
            continue

        results.append({
            "title": name,
            "url": f"https://www.yelp.nl/biz/{slug}",
            "domain": f"{slug.replace('-', '')}.nl",
            "query": query,
            "source": "yelp",
            "yelp_slug": slug,
        })

        if len(results) >= 5:
            break

    return results


def _parse_search_results(html: str, query: str) -> List[Dict]:
    """Parse search engine HTML into results."""
    results = []
    # Match both single and double quoted hrefs
    for m in re.finditer(r'<a[^>]*href=["\']?(https?://[^"\'<\s]+)["\']?[^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE):
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


async def _scrape_website(url: str) -> Dict[str, Any]:
    """Scrape a website for text and emails."""
    async with _SCRAPE_SEMAPHORE:
        try:
            async with httpx.AsyncClient(
                timeout=15,
                follow_redirects=True,
                verify=False,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"}
            ) as client:
                r = await client.get(url)
                if r.status_code != 200:
                    return {"text": "", "emails": []}

                html = r.text
                # Extract emails
                emails = list(set(re.findall(
                    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
                    html
                )))

                # Extract text (strip HTML)
                text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
                text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
                text = re.sub(r'<[^>]+>', ' ', text)
                text = re.sub(r'\s+', ' ', text).strip()

                return {"text": text[:3000], "emails": emails}
        except Exception as e:
            logger.warning(f"Scrape error for {url}: {e}")
            return {"text": "", "emails": []}


def _extract_name_from_domain(domain: str) -> str:
    """Guess company name from domain."""
    name = domain.split('.')[0]
    # Remove common prefixes
    for prefix in ["www", "mail", "web"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Capitalize
    return name.replace('-', ' ').replace('_', ' ').title()


def _add_to_queue(
    domain: str,
    name: str,
    source_query: str,
    source: str,
    icp_score: float,
):
    """Add a discovered company to the queue."""
    try:
        conn = _get_db()
        conn.execute("""
            INSERT OR IGNORE INTO discovery_queue (domain, name, source_query, source, icp_score)
            VALUES (?, ?, ?, ?, ?)
        """, (domain, name, source_query, source, icp_score))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not add to queue: {e}")


def _add_to_pipeline(
    domain: str,
    name: str,
    emails: list,
    industry: str,
    icp_score: float,
    source_query: str,
):
    """Add a qualified company to the pipeline (companies + contacts tables)."""
    try:
        conn = _get_db()

        # Check if company already exists
        existing = conn.execute(
            "SELECT company_id FROM companies WHERE domain = ?", (domain,)
        ).fetchone()
        if existing:
            conn.close()
            return existing["company_id"]

        # Insert company
        company_id = f"comp_{domain.replace('.', '_')}"
        conn.execute("""
            INSERT INTO companies (company_id, name, domain, industry, estimated_size)
            VALUES (?, ?, ?, ?, NULL)
        """, (company_id, name, domain, industry))

        # Insert contact (first personal email, or generic)
        contact_id = f"lead_{domain.replace('.', '_')}"
        personal_email = None
        generic_email = None
        for e in emails:
            # Skip invalid emails (images, yelp domains, etc.)
            if not e or "@" not in e:
                continue
            local_part = e.split("@")[0].lower()
            domain_part = e.split("@")[1].lower() if "@" in e else ""
            # Skip junk emails
            if any(x in e.lower() for x in ["yelp:", "40x40", "claim_your_page", ".png", ".jpg", ".gif"]):
                continue
            if local_part not in ("info", "contact", "hello", "support", "sales", "admin"):
                personal_email = e
                break
            elif not generic_email:
                generic_email = e

        final_email = personal_email or generic_email

        # Skip if no valid email found
        if not final_email or "@" not in final_email or "." not in final_email.split("@")[1]:
            logger.info(f"  Skipping {name} ({domain}) - no valid email found")
            conn.close()
            return None

        # Additional validation: email must have valid TLD
        tld = final_email.split("@")[1].split(".")[-1]
        if len(tld) < 2 or len(tld) > 10:
            logger.info(f"  Skipping {name} ({domain}) - invalid email TLD: {final_email}")
            conn.close()
            return None

        conn.execute("""
            INSERT INTO contacts (contact_id, company_id, first_name, last_name, email, role, current_stage, lead_score)
            VALUES (?, ?, ?, ?, ?, ?, 'INGESTED', ?)
        """, (
            contact_id, company_id,
            "Unknown", "", final_email, "Unknown",
            int(icp_score)
        ))

        conn.commit()
        conn.close()
        logger.info(f"Added to pipeline: {name} ({domain}) - {industry} - ICP: {icp_score}")
        return company_id
    except Exception as e:
        logger.warning(f"Could not add to pipeline: {e}")
        return None


async def run_single_query(query: str, category: str, tenant_id: str = "injexion") -> int:
    """Run a single search query and process results. Returns number of companies added."""
    added = 0
    icp_config = get_icp_config(tenant_id)

    logger.info(f"Searching: [{category}] {query}")
    results = await _search_web(query)

    if not results:
        logger.info(f"  No results for: {query[:50]}")
        return 0

    for result in results:
        domain = result["domain"]
        source = result.get("source", "brave")

        # Use Yelp slug as unique identifier if from Yelp
        if source == "yelp" and "yelp_slug" in result:
            domain = f"yelp:{result['yelp_slug']}"

        # Skip if already in pipeline or queue
        if _domain_exists(domain) or _queue_exists(domain):
            continue

        # Check daily cap
        if _today_count() >= DAILY_CAP:
            logger.info(f"Daily cap reached ({DAILY_CAP})")
            return added

        # Scrape website (use Yelp page if from Yelp)
        if source == "yelp" and "yelp_slug" in result:
            scrape_url = f"https://www.yelp.nl/biz/{result['yelp_slug']}"
        else:
            scrape_url = result["url"]

        scrape_data = await _scrape_website(scrape_url)
        text = scrape_data["text"]
        emails = scrape_data["emails"]

        # Detect industry
        industry = _detect_industry(text, query)

        # Score against ICP
        name = result.get("title", _extract_name_from_domain(domain))
        score_result = await score_company(
            name=name,
            domain=domain,
            scraped_text=text,
            detected_industry=industry,
            tenant_id=tenant_id,
        )

        icp_score = score_result.get("total", 0)
        passed = score_result.get("passed", False)
        business_phase = score_result.get("business_phase", "unknown")
        pain_points = score_result.get("pain_points", [])

        # Record learning
        record_query_result(
            query_text=query,
            query_category=category,
            source=source,
            company_domain=domain,
            company_industry=score_result.get("detected_industry", industry),
            company_size=score_result.get("estimated_size"),
            icp_score=icp_score,
            pipeline_stage="INGESTED" if passed else None,
            outcome=None,
        )

        if passed:
            # Add to pipeline
            company_id = _add_to_pipeline(
                domain=domain,
                name=name,
                emails=emails,
                industry=score_result.get("detected_industry", industry),
                icp_score=icp_score,
                source_query=query,
            )
            if company_id:
                added += 1
                phase_display = f" [{business_phase}]" if business_phase != "unknown" else ""
                pain_display = f" - Pain: {', '.join(pain_points[:2])}" if pain_points else ""
                logger.info(f"  + {name} ({domain}) - {industry}{phase_display} - ICP: {icp_score}{pain_display}")
        else:
            # Add to queue as rejected (for learning)
            _add_to_queue(domain, name, query, source, icp_score)
            logger.info(f"  - {name} ({domain}) - ICP: {icp_score} (rejected)")

        # Small delay between scrapes
        await asyncio.sleep(1)

    return added


async def run_scraping_cycle(tenant_id: str = "injexion") -> int:
    """Run one full scraping cycle. Returns total companies added."""
    logger.info("=" * 60)
    logger.info(f"Starting scraping cycle (today: {_today_count()}/{DAILY_CAP})")

    # Check daily cap
    if _today_count() >= DAILY_CAP:
        logger.info(f"Daily cap reached ({DAILY_CAP}). Skipping cycle.")
        return 0

    # Get adapted queries
    queries = get_adapted_queries(tenant_id, limit=15)
    logger.info(f"Running {len(queries)} queries")

    total_added = 0
    for query, category in queries:
        if _today_count() >= DAILY_CAP:
            logger.info(f"Daily cap reached during cycle")
            break

        added = await run_single_query(query, category, tenant_id)
        total_added += added

        # Delay between queries
        await asyncio.sleep(QUERY_DELAY)

    # Periodically generate new query variations
    if random.random() < 0.3:  # 30% chance each cycle
        new_queries = generate_new_queries()
        if new_queries:
            logger.info(f"Generated {len(new_queries)} new query variations")
            for q, c in new_queries[:3]:
                added = await run_single_query(q, c, tenant_id)
                total_added += added
                await asyncio.sleep(QUERY_DELAY)

    logger.info(f"Cycle complete: {total_added} companies added")
    logger.info("=" * 60)
    return total_added


async def learning_scraper_loop(tenant_id: str = "injexion"):
    """Background loop for continuous company discovery."""
    logger.info("[Learning Scraper] Starting continuous discovery loop")

    # Wait a bit for server to start
    await asyncio.sleep(10)

    cycle_count = 0
    while True:
        try:
            # Check if we're within business hours (flexible: 07:00-22:00 CET)
            from datetime import timezone as tz
            now = datetime.now(tz.utc)
            cet_hour = (now.hour + 2) % 24  # Approximate CET

            if 7 <= cet_hour <= 22:
                added = await run_scraping_cycle(tenant_id)
                cycle_count += 1

                # Log stats every 10 cycles
                if cycle_count % 10 == 0:
                    stats = get_learning_stats()
                    logger.info(f"[Learning Scraper] Stats after {cycle_count} cycles: "
                              f"discovered={stats['total_discovered']}, "
                              f"icp_pass={stats['icp_passed']}, "
                              f"qualified={stats['qualified']}")
            else:
                logger.info(f"[Learning Scraper] Outside business hours ({cet_hour}:00), sleeping 30min")

            # Sleep between cycles (5-10 minutes, randomized)
            sleep_time = random.randint(300, 600)
            await asyncio.sleep(sleep_time)

        except Exception as e:
            logger.error(f"[Learning Scraper] Error in loop: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying
