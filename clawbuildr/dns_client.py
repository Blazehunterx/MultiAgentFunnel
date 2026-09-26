"""
DNS client that survives blocked UDP/53 (common on some routers/ISPs).

Tries the system resolver first (fast); when a lookup times out it trips a
process-wide circuit breaker and uses DNS-over-HTTPS (Google, then Cloudflare)
for every subsequent query.

API mirrors dns.resolver.resolve(): returns a list of rdata objects (real
dnspython objects even on the DoH path) and raises dns.resolver.NXDOMAIN /
dns.resolver.NoAnswer when appropriate, so existing call sites keep working.
"""

import logging
from typing import List, Tuple

import dns.rdata
import dns.rdataclass
import dns.rdatatype
import dns.resolver
import httpx

logger = logging.getLogger("DnsClient")

# Set after a UDP/53 failure: subsequent lookups go straight to DoH
_UDP_BROKEN = False

_TYPE_IDS = {
    "A": 1, "NS": 2, "CNAME": 5, "SOA": 6, "PTR": 12, "MX": 15,
    "TXT": 16, "AAAA": 28, "SRV": 33,
}


def reset_circuit_breaker() -> None:
    """Test helper: allow UDP/53 attempts again."""
    global _UDP_BROKEN
    _UDP_BROKEN = False


def _doh_lookup(name: str, rdtype: str) -> Tuple[str, List]:
    """Query DNS-over-HTTPS JSON APIs.

    Returns (status, records) where status is one of
    'ok' | 'nxdomain' | 'noanswer' | 'error'.
    """
    type_id = _TYPE_IDS.get(rdtype.upper())
    if type_id is None:
        logger.warning(f"[DnsClient] unsupported record type for DoH: {rdtype}")
        return "error", []
    for url in (f"https://dns.google/resolve?name={name}&type={type_id}",
                f"https://cloudflare-dns.com/dns-query?name={name}&type={type_id}"):
        try:
            with httpx.Client(timeout=6.0, follow_redirects=True) as client:
                r = client.get(url, headers={"Accept": "application/dns-json"})
            if r.status_code != 200:
                continue
            data = r.json()
            status = data.get("Status")
            if status == 3:
                return "nxdomain", []
            if status != 0:
                continue  # SERVFAIL etc. — try the next provider
            answers = [a for a in (data.get("Answer") or [])
                       if a.get("type") == type_id]
            if not answers:
                return "noanswer", []
            rdclass = dns.rdataclass.from_text("IN")
            rdtype_obj = dns.rdatatype.from_text(rdtype)
            records = []
            for a in answers:
                try:
                    records.append(
                        dns.rdata.from_text(rdclass, rdtype_obj, str(a.get("data", ""))))
                except Exception:
                    continue
            if not records:
                return "noanswer", []
            return "ok", records
        except Exception:
            continue
    return "error", []


def resolve(name: str, rdtype: str, lifetime: float = 5.0) -> List:
    """Like dns.resolver.resolve(), but works when UDP/53 is blocked.

    Raises dns.resolver.NXDOMAIN / dns.resolver.NoAnswer for definitive
    answers and a plain exception (TimeoutError) when lookup truly fails.
    """
    global _UDP_BROKEN
    name = (name or "").strip().rstrip(".")
    if not name:
        raise dns.resolver.NoAnswer()

    if not _UDP_BROKEN:
        try:
            return list(dns.resolver.resolve(name, rdtype, lifetime=lifetime))
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            raise  # the server answered definitively — do not fall back
        except Exception:
            _UDP_BROKEN = True
            logger.warning(
                "[DnsClient] UDP/53 failed (%s %s) — switching to DNS-over-HTTPS",
                name, rdtype)

    status, records = _doh_lookup(name, rdtype)
    if status == "ok":
        return records
    if status == "nxdomain":
        raise dns.resolver.NXDOMAIN()
    if status == "noanswer":
        raise dns.resolver.NoAnswer()
    raise TimeoutError(f"DNS lookup failed: {name} {rdtype} (UDP blocked, DoH unavailable)")
