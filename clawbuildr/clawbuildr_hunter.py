#!/usr/bin/env python3
"""
ClawBuildr Hunter — Hunter.io API client for email discovery and verification.
"""

import os
import json
import httpx
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.Hunter")

_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_HUNTER_API_KEYS = []
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("HUNTER_API_KEY="):
            _HUNTER_API_KEYS = [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]

_key_idx = 0


def _next_key() -> str:
    global _key_idx
    if not _HUNTER_API_KEYS:
        raise RuntimeError("No HUNTER_API_KEY configured in .env")
    key = _HUNTER_API_KEYS[_key_idx % len(_HUNTER_API_KEYS)]
    _key_idx += 1
    return key


def domain_search(domain: str, limit: int = 10, type: str = "personal") -> Dict[str, Any]:
    key = _next_key()
    url = f"https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": key, "limit": limit, "type": type}

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params=params)
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
                    })
                return {
                    "success": True,
                    "domain": domain,
                    "organization": data.get("organization", ""),
                    "emails": emails,
                    "total": data.get("total", 0),
                }
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


def email_verify(email: str) -> Dict[str, Any]:
    key = _next_key()
    url = f"https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": key}

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                return {
                    "success": True,
                    "email": email,
                    "status": data.get("status", "unknown"),
                    "score": data.get("score", 0),
                    "deliverable": data.get("deliverable", False),
                    "reason": data.get("reason", ""),
                    "mx_valid": data.get("mx_found", False),
                    "smtp_valid": data.get("smtp_check", False),
                }
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


def account_info() -> Dict[str, Any]:
    key = _next_key()
    url = f"https://api.hunter.io/v2/account"
    params = {"api_key": key}

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                plan = data.get("plan", {})
                return {
                    "success": True,
                    "email": data.get("email", ""),
                    "plan_name": plan.get("name", ""),
                    "requests": {
                        "used": plan.get("requests", {}).get("used", 0),
                        "available": plan.get("requests", {}).get("available", 0),
                        "reset": plan.get("requests", {}).get("reset", ""),
                    },
                }
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


if __name__ == "__main__":
    result = domain_search("clawbuildr.com", limit=5)
    print(json.dumps(result, indent=2))
