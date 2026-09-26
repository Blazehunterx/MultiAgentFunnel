"""
Email Enrichment Agent.
Finds contacts with generic emails (info@, contact@, etc.) that have real names,
then uses Hunter.io + pattern matching to discover personal decision-maker emails.
"""

import sqlite3
import os
import sys
import re
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EmailEnricher")

# Paths
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")
CLAWBUILDR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(CLAWBUILDR_DIR, "clawbuildr"))

from email_finder import find_personal_email

GENERIC_PREFIXES = {
    "info", "contact", "hello", "hallo", "sales", "support", "admin", "office",
    "team", "mail", "service", "help", "webmaster", "postmaster", "noreply",
    "no-reply", "marketing", "hr", "recruitment", "jobs", "careers", "info2",
    "klantenservice", "klantendienst", "aanmelden", "afspraak", "secretariaat",
    "privacy", "legal", "compliance", "dpo", "security", "billing", "press",
}


def is_generic_email(email: str) -> bool:
    """Check if email uses a generic prefix."""
    if not email or "@" not in email:
        return True
    try:
        from lead_quality import is_generic_email as _is_generic
        return _is_generic(email)
    except Exception:
        pass
    prefix = email.split("@")[0].lower().split("+")[0]
    # Remove trailing digits
    prefix_no_digits = re.sub(r'\d+$', '', prefix)
    return prefix in GENERIC_PREFIXES or prefix_no_digits in GENERIC_PREFIXES or prefix.startswith("info") or prefix.startswith("contact")


def has_real_name(first_name: str, last_name: str) -> bool:
    """Check if contact has a real person name (not garbage)."""
    if not first_name or len(first_name) < 2:
        return False
    full = f"{first_name} {last_name or ''}".strip()
    if len(full) < 3:
        return False
    garbage = {"contact", "info", "name", "events", "admin", "sales", "support", "team", "office", "webmaster"}
    if first_name.lower() in garbage or (last_name or "").lower() in garbage:
        return False
    return True


def enrich_contact(db, contact_id: str, first_name: str, last_name: str, domain: str) -> dict:
    """Try to find a better email for a contact. Returns result dict."""
    logger.info(f"Enriching {first_name} {last_name} @ {domain}")
    result = find_personal_email(first_name, last_name, domain)
    new_email = result.get("email")
    confidence = result.get("confidence", 0)
    source = result.get("source", "unknown")

    if not new_email or is_generic_email(new_email):
        return {"success": False, "reason": "no_better_email", "tried": new_email, "confidence": confidence}

    if confidence < 70:
        return {"success": False, "reason": "low_confidence", "tried": new_email, "confidence": confidence, "source": source}

    # Update contact
    cur = db.cursor()
    cur.execute("""
        UPDATE contacts
        SET email = ?, updated_at = ?, deliverability_audit = COALESCE(deliverability_audit, '') || ?
        WHERE contact_id = ?
    """, (
        new_email,
        datetime.now().isoformat(),
        f" | Enriched from generic email to {new_email} (confidence {confidence}, source {source})",
        contact_id
    ))
    db.commit()

    return {
        "success": True,
        "email": new_email,
        "confidence": confidence,
        "source": source
    }


def run_enrichment(min_confidence: int = 70, dry_run: bool = False) -> dict:
    """Run enrichment on all contacts with generic emails and real names."""
    db = sqlite3.connect(DB_PATH, timeout=30.0)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    cur.execute("""
        SELECT c.contact_id, c.first_name, c.last_name, c.email, co.domain
        FROM contacts c
        JOIN companies co ON c.company_id = co.company_id
        WHERE c.current_stage IN ('INGESTED', 'RESEARCHED', 'DELIVERABILITY_VERIFIED', 'OPPORTUNITY_MAPPED', 'PRE_QUALIFIED', 'NURTURE', 'PENDING_APPROVAL')
        ORDER BY c.created_at DESC
    """)

    total_generic = 0
    enriched = 0
    failed = 0
    skipped_no_name = 0
    results = []

    for row in cur.fetchall():
        contact_id = row["contact_id"]
        first_name = row["first_name"]
        last_name = row["last_name"]
        email = row["email"]
        domain = row["domain"]

        if not is_generic_email(email):
            continue

        total_generic += 1

        if not has_real_name(first_name, last_name):
            skipped_no_name += 1
            continue

        if dry_run:
            logger.info(f"[DRY RUN] Would enrich {contact_id}: {first_name} {last_name} ({email}) @ {domain}")
            continue

        try:
            res = enrich_contact(db, contact_id, first_name, last_name, domain)
            results.append({
                "contact_id": contact_id,
                "name": f"{first_name} {last_name}",
                "old_email": email,
                "domain": domain,
                **res
            })
            if res["success"]:
                enriched += 1
                logger.info(f"✅ Enriched {first_name} {last_name}: {email} -> {res['email']} ({res['confidence']}%)")
            else:
                failed += 1
                logger.info(f"❌ Failed {first_name} {last_name}: {res.get('reason')} (tried: {res.get('tried')}, confidence: {res.get('confidence')})")
        except Exception as e:
            failed += 1
            logger.error(f"Error enriching {contact_id}: {e}")

    db.close()

    return {
        "total_generic": total_generic,
        "enriched": enriched,
        "failed": failed,
        "skipped_no_name": skipped_no_name,
        "results": results
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without updating DB")
    parser.add_argument("--min-confidence", type=int, default=70, help="Minimum confidence to update")
    args = parser.parse_args()

    logger.info(f"Starting email enrichment (DB: {DB_PATH})")
    summary = run_enrichment(min_confidence=args.min_confidence, dry_run=args.dry_run)
    logger.info(f"Done. Generic: {summary['total_generic']}, Enriched: {summary['enriched']}, Failed: {summary['failed']}, Skipped (no name): {summary['skipped_no_name']}")
