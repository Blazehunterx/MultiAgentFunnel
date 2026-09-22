"""
Template Scoring & Personalization Engine.
Scores templates against company data and personalizes messages
using the ClawBuilder operating layer positioning.
"""

import re
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("TemplateScorer")

# ─── ClawBuilder Operating Layer Positioning ──────────────────────

# Headline: "De meest efficiënte versie van uw bedrijf bestaat al."
# Tagline: "Machines herhalen. Mensen beslissen."

OPERATOR_PAIN_POINTS = {
    "default": [
        "geen tijd voor strategie",
        "systemen praten niet met elkaar",
        "eigenaar is het operating system",
        "handmatig werk dat elke dag terugkomt",
        "medewerkers doen hetzelfde werk elke dag",
        "geen zicht op wat er echt gebeurt",
        "alles loopt via de eigenaar",
    ],
    "groothandel": [
        "voorraadbeheer handmatig, foutgevoelig",
        "bestellingen overtellen tussen systemen",
        "inkoopproces niet gestandaardiseerd",
    ],
    "logistiek": [
        "zendingen tracken in losse systemen",
        "planning handmatig, niet geautomatiseerd",
        "communicatie met chauffeurs via WhatsApp",
    ],
    "b2b_saas": [
        "klantdata verspreid over meerdere tools",
        "onboarding niet gestandaardiseerd",
        "te veel handmatig werk in customer success",
    ],
    "it_dienstverlening": [
        "projectadministratie in losse bestanden",
        "urenregistratie handmatig",
        "facturatie niet gekoppeld aan projecten",
    ],
}

PHASE_CTA = {
    "default": "Als je volume volgend kwartaal verdubbelt, moet je dan mensen aannemen?",
    "groothandel": "Als je volume volgend kwartaal verdubbelt, moet je dan mensen aannemen?",
    "logistiek": "Als je volume volgend kwartaal verdubbelt, moet je dan mensen aannemen?",
    "b2b_saas": "Als je volume volgend kwartaal verdubbelt, moet je dan mensen aannemen?",
    "it_dienstverlening": "Als je volume volgend kwartaal verdubbelt, moet je dan mensen aannemen?",
}

INDUSTRY_GREETINGS = {
    "groothandel": "als groothandel",
    "logistiek": "in de logistiek",
    "it": "in de IT",
    "saas": "in de SaaS",
    "dienstverlening": "in de dienstverlening",
}

# BANNED WORDS — from the 4-sheet positioning document
BANNED_WORDS = [
    "vervangen", "automatiseren", "minder mensen nodig", "personeel snijden",
    "hoofdtelling", "FTE-reductie", "redundant", "scorebord", "monitoren",
    "volgen wat mensen doen",
]

# FIXED HEADLINE/TAGLINE
FIXED_HEADLINE = "De meest efficiënte versie van uw bedrijf bestaat al."
FIXED_TAGLINE = "Machines herhalen. Mensen beslissen."

# VALUE MATH
VALUE_MATH = "Elke euro structurele kostenbesparing is ongeveer vijf euro bedrijfswaarde."


def check_banned_words(text: str) -> list:
    """Check for banned words in text. Returns list of found words."""
    text_lower = text.lower()
    return [w for w in BANNED_WORDS if w in text_lower]


def score_template(template: str, company: dict = None, contact: dict = None) -> Dict[str, Any]:
    """Score a template's personalization potential (0-100)."""
    score = 0
    details = []

    # Has personalization tokens
    tokens = re.findall(r'\{\{(\w+)\}\}', template)
    if tokens:
        score += min(30, len(tokens) * 10)
        details.append(f"Tokens: {tokens}")

    # Has company-specific content
    if company:
        if company.get("industry") and company["industry"].lower() in template.lower():
            score += 15
            details.append("Industry mentioned")
        if company.get("name") and company["name"].lower() in template.lower():
            score += 10
            details.append("Company name used")

    # Has contact-specific content
    if contact:
        if contact.get("first_name") and "{{first_name}}" in template:
            score += 10
            details.append("First name token")
        if contact.get("role") and any(r in template.lower() for r in ["directeur", "eigenaar", "ceo"]):
            score += 10
            details.append("Role-aware")

    # Has CTA (operating layer style — question about scaling)
    cta_keywords = ["verdubbelt", "aannemen", "volume", "volgende kwartaal", "schalen"]
    if any(kw in template.lower() for kw in cta_keywords):
        score += 15
        details.append("Has operating layer CTA")
    elif any(kw in template.lower() for kw in ["plan", "gesprek", "bellen", "call"]):
        score += 10
        details.append("Has basic CTA")

    # Has value proposition (operating layer style)
    value_keywords = ["operating system", "efficiëntste versie", "bron van waarde", "systeem", "processen"]
    if any(kw in template.lower() for kw in value_keywords):
        score += 15
        details.append("Has operating layer value prop")
    elif any(kw in template.lower() for kw in ["helpen", "resultaat", "bewezen", "aanpak"]):
        score += 10
        details.append("Has basic value prop")

    # Owner constraint framing (directeur/eigenaar as bottleneck)
    owner_keywords = ["uw bedrijf", "uw team", "uw processen", "uw bedrijfsvoering"]
    if any(kw in template.lower() for kw in owner_keywords):
        score += 10
        details.append("Owner constraint framing")

    # Banned words check (deduction)
    banned = check_banned_words(template)
    if banned:
        score -= 20 * len(banned)
        details.append(f"BANNED WORDS FOUND: {banned}")

    # Personalization depth
    if company and contact:
        if company.get("industry") and contact.get("first_name"):
            score += 10
            details.append("Full personalization available")

    return {
        "score": max(0, min(100, score)),
        "details": details,
        "rating": "excellent" if score >= 80 else "good" if score >= 60 else "fair" if score >= 40 else "poor"
    }


def personalize_template(template: str, contact: dict = None, company: dict = None) -> dict:
    """Personalize a template with contact and company data."""
    result = {"body": template, "subject": "", "personalized_fields": []}

    if not contact and not company:
        return result

    # Basic replacements
    if contact:
        if contact.get("first_name"):
            result["body"] = result["body"].replace("{{first_name}}", contact["first_name"])
            result["personalized_fields"].append("first_name")
        if contact.get("last_name"):
            result["body"] = result["body"].replace("{{last_name}}", contact["last_name"])
        if contact.get("role"):
            result["body"] = result["body"].replace("{{role}}", contact["role"])

    if company:
        if company.get("name"):
            result["body"] = result["body"].replace("{{company_name}}", company["name"])
            result["personalized_fields"].append("company_name")
        if company.get("industry"):
            result["body"] = result["body"].replace("{{industry}}", company["industry"])
            result["personalized_fields"].append("industry")
        if company.get("domain"):
            result["body"] = result["body"].replace("{{website}}", company["domain"])

    # Detect industry for CTA and pain points
    industry = _detect_industry(company) if company else "default"
    result["body"] = result["body"].replace("{{phase_cta}}", PHASE_CTA.get(industry, PHASE_CTA["default"]))
    result["body"] = result["body"].replace("{{pain_point}}", _get_primary_pain(company) if company else "")
    result["business_phase"] = industry

    # Generate subject line if not provided
    if "{{subject}}" in result["body"] or not result.get("subject"):
        result["subject"] = _generate_subject(contact, company, industry)

    # Banned words check
    banned = check_banned_words(result["body"])
    if banned:
        logger.warning(f"BANNED WORDS in template: {banned}")

    return result


def _detect_industry(company: dict) -> str:
    """Detect industry from company data for template selection."""
    if not company:
        return "default"

    industry = (company.get("industry") or "").lower()

    if "groothandel" in industry or "wholesale" in industry:
        return "groothandel"
    elif "logistiek" in industry or "transport" in industry:
        return "logistiek"
    elif "saas" in industry or "software" in industry:
        return "b2b_saas"
    elif "it" in industry or "ict" in industry or "tech" in industry:
        return "it_dienstverlening"

    return "default"


def _get_primary_pain(company: dict) -> str:
    """Get the primary pain point based on company data."""
    industry = _detect_industry(company)
    pains = OPERATOR_PAIN_POINTS.get(industry, OPERATOR_PAIN_POINTS["default"])
    return pains[0] if pains else ""


def _generate_subject(contact: dict, company: dict, industry: str) -> str:
    """Generate a personalized subject line (operating layer style)."""
    name = contact.get("first_name", "") if contact else ""
    comp = company.get("name", "") if company else ""

    subjects = {
        "default": [
            f"Hoi {name}, een vraag over {comp}",
            f"{name}, iets voor {comp}",
            f"Beperking in het systeem bij {comp}",
        ],
        "groothandel": [
            f"{name}, voorraadbeheer bij {comp}",
            f"Hoi {name}, something for {comp}",
        ],
        "logistiek": [
            f"{name}, planning bij {comp}",
            f"Hoi {name}, iets voor {comp}",
        ],
        "b2b_saas": [
            f"{name}, onboarding bij {comp}",
            f"Hoi {name}, iets voor {comp}",
        ],
        "it_dienstverlening": [
            f"{name}, projectadministratie bij {comp}",
            f"Hoi {name}, iets voor {comp}",
        ],
    }

    import random
    options = subjects.get(industry, subjects["default"])
    return random.choice(options)


# ─── Template Library (Operating Layer Positioning) ────────────────

DEFAULT_TEMPLATES = {
    "cold_outreach_v1": {
        "name": "Cold Outreach - Operating Layer",
        "category": "initial",
        "text": (
            "Hoi {{first_name}},\n\n"
            "Bij {{company_name}} kost het waarschijnlijk elke week tijd "
            "om dingen over te tellen tussen systemen, statussen bij te werken "
            "en te checken of iets goed is gegaan.\n\n"
            "De meest efficiënte versie van uw bedrijf bestaat al. "
            "Hij zit in uw inbox, uw administratie en een half dozijn systemen "
            "die niet met elkaar praten.\n\n"
            "Wij brengen het op één plek en maken het de versie die elke dag draait, "
            "over het hele bedrijf.\n\n"
            "{{phase_cta}}\n\n"
            "Groet,\nMarvin"
        ),
        "score_threshold": 50
    },
    "followup_v1": {
        "name": "Follow-up - Operating Layer",
        "category": "followup",
        "text": (
            "Hoi {{first_name}},\n\n"
            "Even een kort berichtje ter opvolging.\n\n"
            "Als je volume volgend kwartaal verdubbelt, moet je dan "
            "nieuwe mensen aannemen? Of moet je eerst kijken of je "
            "huidige processen efficiënter kunnen?\n\n"
            "Wij helpen bedrijven zoals {{company_name}} om hun operating system "
            "te optimaliseren — zodat uw team meer kan doen zonder meer mensen.\n\n"
            "Plan een kort gesprek om te zien wat we voor {{company_name}} kunnen betekenen.\n\n"
            "Groet,\nMarvin"
        ),
        "score_threshold": 60
    },
    "objection_price_v1": {
        "name": "Objection: Price",
        "category": "objection",
        "text": (
            "Hoi {{first_name}},\n\n"
            "Begrijpelijk dat je er goed over nadenkt.\n\n"
            "De meeste bedrijven die met ons werken zien een ROI van 3-5x "
            "binnen de eerste 3 maanden. Elke euro structurele kostenbesparing "
            "is ongeveer vijf euro bedrijfswaarde.\n\n"
            "Wat als we beginnen met één proces — het proces dat elke dag "
            "de meeste tijd kost — en kijken of het werkt?\n\n"
            "{{phase_cta}}\n\n"
            "Groet,\nMarvin"
        ),
        "score_threshold": 55
    },
    "meeting_booked_v1": {
        "name": "Meeting Confirmation",
        "category": "confirmation",
        "text": (
            "Hoi {{first_name}},\n\n"
            "Bedankt voor het inplannen van het gesprek! "
            "Ik kijk ernaar uit om meer te horen over {{company_name}} "
            "en hoe we je kunnen helpen.\n\n"
            "Tot {{meeting_date}}!\n\n"
            "Groet,\nMarvin"
        ),
        "score_threshold": 40
    }
}


def get_template(template_id: str) -> dict:
    """Get a template from the library."""
    return DEFAULT_TEMPLATES.get(template_id, {})


def list_templates(category: str = None) -> list:
    """List all templates, optionally filtered by category."""
    templates = []
    for tid, t in DEFAULT_TEMPLATES.items():
        if category and t.get("category") != category:
            continue
        templates.append({"id": tid, **t})
    return templates


def score_all_templates(company: dict = None, contact: dict = None) -> list:
    """Score all templates for a given company/contact."""
    results = []
    for tid, t in DEFAULT_TEMPLATES.items():
        scored = score_template(t["text"], company, contact)
        results.append({
            "id": tid,
            "name": t["name"],
            "category": t["category"],
            "score": scored["score"],
            "rating": scored["rating"],
            "details": scored["details"]
        })
    return sorted(results, key=lambda x: x["score"], reverse=True)


if __name__ == "__main__":
    # Test with sample company
    company = {
        "name": "Test BV",
        "industry": "IT Dienstverlening",
        "domain": "testbv.nl",
        "estimated_size": 25
    }
    contact = {
        "first_name": "Jan",
        "last_name": "de Vries",
        "role": "Directeur"
    }

    results = score_all_templates(company, contact)
    print("=== TEMPLATE SCORES ===")
    for r in results:
        print("  %s: %d/100 (%s)" % (r["name"], r["score"], r["rating"]))
        print("    Details: %s" % r["details"])

    # Test personalization
    print("\n=== PERSONALIZATION ===")
    best = results[0]
    template = get_template(best["id"])
    personalized = personalize_template(template["text"], contact, company)
    print("Subject: %s" % personalized["subject"])
    print("Body:\n%s" % personalized["body"])
    print("Phase: %s" % personalized.get("business_phase"))

    # Test banned words
    print("\n=== BANNED WORDS CHECK ===")
    test = "Wij helpen bedrijven om personeel te vervangen door AI."
    banned = check_banned_words(test)
    print("Text: %s" % test)
    print("Banned: %s" % banned)
