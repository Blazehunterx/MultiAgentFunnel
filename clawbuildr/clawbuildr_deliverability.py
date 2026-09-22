#!/usr/bin/env python3
"""
ClawBuildr Deliverability — SPF, DKIM, DMARC record verification.
"""

import os
import json
import sqlite3
import logging
import dns.resolver
import dns.reversename
from datetime import datetime, timezone
from typing import Dict, Any, List

logger = logging.getLogger("ClawBuildr.Deliverability")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def check_spf(domain: str) -> Dict[str, Any]:
    try:
        txt_records = dns.resolver.resolve(domain, "TXT")
        for r in txt_records:
            txt = str(r)
            if "v=spf1" in txt:
                return {"valid": True, "record": txt, "domain": domain}
        return {"valid": False, "error": "No SPF record found", "domain": domain}
    except dns.resolver.NXDOMAIN:
        return {"valid": False, "error": "Domain not found", "domain": domain}
    except Exception as e:
        return {"valid": False, "error": str(e), "domain": domain}


def check_dkim(domain: str, selector: str = "google") -> Dict[str, Any]:
    try:
        query = f"{selector}._domainkey.{domain}"
        txt_records = dns.resolver.resolve(query, "TXT")
        records = [str(r) for r in txt_records]
        return {"valid": len(records) > 0, "records": records, "domain": domain, "selector": selector}
    except dns.resolver.NXDOMAIN:
        return {"valid": False, "error": "DKIM record not found", "domain": domain}
    except Exception as e:
        return {"valid": False, "error": str(e), "domain": domain}


def check_dmarc(domain: str) -> Dict[str, Any]:
    try:
        txt_records = dns.resolver.resolve(f"_dmarc.{domain}", "TXT")
        records = [str(r) for r in txt_records]
        dmarc_found = any("v=DMARC1" in r for r in records)
        return {"valid": dmarc_found, "records": records, "domain": domain}
    except dns.resolver.NXDOMAIN:
        return {"valid": False, "error": "DMARC record not found", "domain": domain}
    except Exception as e:
        return {"valid": False, "error": str(e), "domain": domain}


def check_mx(domain: str) -> Dict[str, Any]:
    try:
        mx_records = dns.resolver.resolve(domain, "MX")
        records = [{"priority": r.preference, "exchange": str(r.exchange)} for r in mx_records]
        return {"valid": len(records) > 0, "records": records, "domain": domain}
    except dns.resolver.NXDOMAIN:
        return {"valid": False, "error": "Domain not found", "domain": domain}
    except Exception as e:
        return {"valid": False, "error": str(e), "domain": domain}


def full_deliverability_check(domain: str) -> Dict[str, Any]:
    spf = check_spf(domain)
    dkim = check_dkim(domain)
    dmarc = check_dmarc(domain)
    mx = check_mx(domain)

    all_valid = spf["valid"] and dkim["valid"] and dmarc["valid"] and mx["valid"]
    score = sum([spf["valid"], dkim["valid"], dmarc["valid"], mx["valid"]]) * 25

    result = {
        "domain": domain,
        "score": score,
        "all_valid": all_valid,
        "spf": spf,
        "dkim": dkim,
        "dmarc": dmarc,
        "mx": mx,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    db = _get_db()
    try:
        db.execute("""
            INSERT OR REPLACE INTO deliverability_stats
            (domain, sends_count, replies_count, bounces_count, bounce_rate, domain_health, updated_at)
            VALUES (?, 0, 0, 0, 0.0, ?, ?)
        """, (domain, "HEALTHY" if all_valid else "AT_RISK", datetime.now(timezone.utc).isoformat()))
        db.commit()
    except Exception:
        pass
    finally:
        db.close()

    return result


if __name__ == "__main__":
    result = full_deliverability_check("clawbuildr.com")
    print(json.dumps(result, indent=2, default=str))
