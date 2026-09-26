#!/usr/bin/env python3
"""Shared lead quality gate — reject junk at INSERT time.

One place for generic emails, placeholders, free mail, page-title names,
and non-business domains. Used by dashboard sourcing, lead generator,
learning scraper, and Agent Zero ingest.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

# Local-parts that are role/role-like mailboxes, never decision makers
GENERIC_EMAIL_LOCALS = frozenset({
    "info", "info2", "contact", "hello", "hi", "hallo", "hola",
    "sales", "support", "admin", "administrator", "office", "team",
    "mail", "service", "help", "hr", "recruitment", "jobs", "careers",
    "enquiries", "inquiries", "webmaster", "postmaster", "noreply",
    "no-reply", "donotreply", "mailer-daemon", "bounce", "abuse",
    "spam", "dmca", "security", "legal", "privacy", "dpo", "compliance",
    "marketing", "pr", "social", "partners", "media", "press",
    "billing", "accounts", "payroll", "reception", "general",
    "feedback", "newsletter", "notifications", "editor", "website",
    "klantenservice", "klantendienst", "secretariaat", "directie",
    "inkoop", "verkoop", "financien", "personeel", "communicatie",
    "juridisch", "automatisering", "supportdesk", "customerservice",
    "ukinfo", "group", "corporate", "groupprivacyofficer",
    "privacyofficer", "data Protection", "dataprotection",
    "accommodations", "segreteria", "contato", "newstipps",
    "websiteadmin", "webadmin", "master", "root", "sysadmin",
    "info.", "contact.", "sales.", "support.", "hello.",
    "naam", "name", "voorbeeld", "example", "demo", "test",
    "user", "username", "placeholder", "johndoe", "john", "doe",
})

# Leading tokens that make a local-part generic (privacy.foo, info-uk, …)
_GENERIC_LOCAL_PREFIXES = (
    "info", "contact", "hello", "hallo", "sales", "support", "admin",
    "office", "team", "mail", "service", "help", "hr", "privacy",
    "legal", "compliance", "dpo", "security", "billing", "press",
    "marketing", "webmaster", "noreply", "no-reply", "donotreply",
    "newsletter", "feedback", "secretariaat", "klantenservice",
)

PLACEHOLDER_EMAILS = frozenset({
    "naam@voorbeeld.com", "naam@bedrijf.nl", "name@example.com",
    "test@test.com", "email@example.com", "info@example.com",
    "yourname@company.com", "jouw@email.nl", "voorbeeld@email.nl",
    "voorbeeld@bedrijf.nl", "naam@email.nl", "naam@company.nl",
    "test@email.com", "demo@company.com", "example@example.com",
})

PLACEHOLDER_LOCALS = frozenset({
    "naam", "voorbeeld", "test", "demo", "example", "jouw", "uw",
    "uwnaam", "user", "username", "placeholder", "johndoe",
})

FREE_MAIL_DOMAINS = frozenset({
    "gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com",
    "aol.com", "icloud.com", "protonmail.com", "zoho.com", "yandex.com",
    "mail.com", "gmx.com", "fastmail.com", "tutanota.com", "hushmail.com",
    "mail.ru", "bk.ru", "inbox.ru", "list.ru", "gmx.net", "web.de",
})

PLACEHOLDER_EMAIL_DOMAINS = frozenset({
    "organisatie.nl", "domein.nl", "voorbeeld.nl", "example.com",
    "example.org", "example.nl", "domain.com", "yourdomain.nl",
    "bedrijf.nl", "firma.nl", "test.nl", "localhost.local",
    "example.org", "test.local",
})

# first_name values that are page titles, roles, or garbage
GARBAGE_FIRST_NAMES = frozenset({
    "contact", "info", "privacy", "name", "events", "verzuim",
    "compliance", "ukinfo", "home", "about", "blog", "news",
    "search", "login", "menu", "office", "team", "support",
    "admin", "webmaster", "noreply", "dpo", "payments",
    "secretaris", "group", "corporate", "unknown", "interim",
    "recruiter", "secretariaat", "klantenservice", "archief",
    "afspraak", "register", "hallo", "studio", "welcome",
    "commission", "service", "naam", "voorbeeld", "bij",
    "de", "het", "een", "die", "dit", "dat",
    "eigenaar", "directeur", "manager", "directie",
    "bedrijfspanden", "veenendaal", "sweco", "architecten",
    "diensten", "vacatures", "vacature", "nieuws", "menu",
    "start", "welkom", "hoofdpagina", "pagina", "website",
    "vastgoedtaxaties", "bedrijfsmakelaar", "makelaardij",
    "contactpersoon", "afdeling", "divisie", "hoofdkantoor",
})

# company names that are obviously page titles / not companies
GARBAGE_COMPANY_NAMES = frozenset({
    "home", "contact", "about", "blog", "news", "images",
    "community", "summary", "search", "login", "menu", "start",
    "welkom", "hoofdpagina", "nieuws", "architecten", "diensten",
})

_ALLOWED_TLDS = (".nl", ".be", ".de")

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
# Real person name: letters (incl. accents), initial periods, hyphen/apostrophe/space
_NAME_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ'’\.\-\s]{1,40}$")
_HAS_DIGIT_RE = re.compile(r"\d")


def _local_part(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[0].lower().split("+")[0]


def _domain_part(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].lower().strip()


def _normalized_local(local: str) -> str:
    """Strip trailing digits and split on separators for prefix checks."""
    local = re.sub(r"\d+$", "", local or "")
    root = re.split(r"[._\-]", local)[0] if local else ""
    return local, root


def is_generic_email(email: str) -> bool:
    """True for role mailboxes (info@, privacy@, groupprivacyofficer@…)."""
    if not email or "@" not in email:
        return True
    local = _local_part(email)
    if not local:
        return True
    if local in GENERIC_EMAIL_LOCALS:
        return True
    local_no_digits, root = _normalized_local(local)
    if local_no_digits in GENERIC_EMAIL_LOCALS or root in GENERIC_EMAIL_LOCALS:
        return True
    # Explicit broad prefixes: anything starting with these is a role mailbox
    for broad in ("info", "contact", "privacy", "noreply", "no-reply", "donotreply"):
        if local.startswith(broad):
            return True
    for pref in _GENERIC_LOCAL_PREFIXES:
        if local == pref:
            return True
        if local.startswith(pref):
            rest = local[len(pref):]
            if rest == "" or not rest[0].isalpha():
                return True
    return False


def is_placeholder_email(email: str) -> bool:
    if not email:
        return True
    e = email.lower().strip()
    if e in PLACEHOLDER_EMAILS:
        return True
    local = _local_part(e)
    domain = _domain_part(e)
    if local in PLACEHOLDER_LOCALS:
        return True
    if domain in PLACEHOLDER_EMAIL_DOMAINS:
        return True
    return False


def is_free_mail(email: str) -> bool:
    return _domain_part(email or "") in FREE_MAIL_DOMAINS


def is_valid_email_format(email: str) -> bool:
    if not email or " " in email or "\t" in email:
        return False
    if email.count("@") != 1:
        return False
    return bool(_EMAIL_RE.match(email.strip()))


def is_business_tld(domain: str) -> bool:
    d = (domain or "").lower().strip()
    d = d.split("/")[0]
    if not d:
        return False
    return any(d.endswith(tld) for tld in _ALLOWED_TLDS)


def is_garbage_person_name(first_name: str, last_name: str = "") -> bool:
    """Reject page titles, role words, empty, or non-name shapes as first_name."""
    fn = (first_name or "").strip()
    ln = (last_name or "").strip()
    if not fn or len(fn) < 1:
        return True
    if len(fn) > 40:
        return True
    fl = fn.lower().strip(".")
    if not fl:
        return True
    if fl in GARBAGE_FIRST_NAMES:
        return True
    if ln and ln.lower() in GARBAGE_FIRST_NAMES and fl in GARBAGE_FIRST_NAMES:
        return True
    if _HAS_DIGIT_RE.search(fn):
        return True
    # Page titles / multi-word slogans as "first name"
    if " " in fn.strip() and not ln:
        tokens = fn.split()
        if any(t.lower() in GARBAGE_FIRST_NAMES for t in tokens):
            return True
        if len(tokens) >= 3:
            return True
    if not _NAME_RE.match(fn):
        return True
    # Long compound with no last name → almost always a page/title fragment
    # (Dutch real first names are rarely >16 chars without a surname)
    if not ln and len(fl) > 16 and " " not in fl:
        return True
    # Role words used as first_name even with a last name
    if fl in ("eigenaar", "directeur", "manager", "directie", "secretariaat"):
        return True
    # all-lowercase single token is often a slug/page fragment
    if fn == fn.lower() and " " not in fn and len(fn) <= 4 and not ln:
        if fl in ("home", "menu", "blog", "news", "info"):
            return True
    return False


def is_garbage_company_name(company_name: str) -> bool:
    c = (company_name or "").strip()
    if not c:
        return True
    if c.lower() in GARBAGE_COMPANY_NAMES:
        return True
    return False


def is_personal_email(email: str) -> bool:
    """Convenience: format ok, not generic, not placeholder, not free mail."""
    if not is_valid_email_format(email):
        return False
    if is_generic_email(email) or is_placeholder_email(email) or is_free_mail(email):
        return False
    return True


def validate_lead(
    email: str,
    first_name: str = "",
    last_name: str = "",
    company_name: str = "",
    domain: str = "",
    require_business_tld: bool = True,
    require_good_company: bool = False,
) -> Tuple[bool, str]:
    """Return (ok, reason). ok=False → do not insert.

    company_name garbage (page titles like "Home") is only fatal when
    require_good_company=True; callers can fall back to the domain instead.
    """
    if not email or not str(email).strip():
        return False, "no_email"
    email = str(email).strip()
    if not is_valid_email_format(email):
        return False, "invalid_email_format"
    if is_placeholder_email(email):
        return False, "placeholder_email"
    if is_generic_email(email):
        return False, "generic_email"
    if is_free_mail(email):
        return False, "free_mail"
    if is_garbage_person_name(first_name, last_name):
        return False, "garbage_name"
    if company_name and require_good_company and is_garbage_company_name(company_name):
        return False, "garbage_company"
    dom = (domain or _domain_part(email)).lower().strip()
    if require_business_tld and not is_business_tld(dom):
        return False, "non_business_tld"
    return True, "ok"


def lead_quality_reason(
    email: str,
    first_name: str = "",
    last_name: str = "",
    company_name: str = "",
    domain: str = "",
    require_business_tld: bool = True,
) -> Optional[str]:
    """None if lead is acceptable; otherwise the reject reason."""
    ok, reason = validate_lead(
        email, first_name, last_name, company_name, domain, require_business_tld
    )
    return None if ok else reason
