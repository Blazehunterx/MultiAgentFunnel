import json
import os
import sqlite3
import logging
import httpx
import asyncio
from typing import Dict, Any, Optional

logger = logging.getLogger("icp_scorer")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")

# Load Gemini API key
_GEMINI_API_KEY = ""
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("GEMINI_API_KEY="):
                _GEMINI_API_KEY = line.split("=", 1)[1]


def get_icp_config(tenant_id: str = "injexion") -> Dict[str, Any]:
    """Load ICP configuration from tenant_config table."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT icp_industries, icp_roles, icp_company_size FROM tenant_config WHERE tenant_id = ?",
        (tenant_id,)
    ).fetchone()
    conn.close()

    if not row:
        return {
            "industries": ["Groothandel", "Logistiek & Transport", "B2B SaaS", "IT Dienstverlening"],
            "roles": ["Directeur", "CEO", "Eigenaar", "Oprichter"],
            "size_min": 10,
            "size_max": 200,
        }

    industries = json.loads(row["icp_industries"]) if row["icp_industries"] else []
    roles = json.loads(row["icp_roles"]) if row["icp_roles"] else []
    size_str = row["icp_company_size"] or "10-200"
    parts = size_str.split("-")
    size_min = int(parts[0]) if len(parts) > 0 else 10
    size_max = int(parts[1]) if len(parts) > 1 else 200

    return {
        "industries": industries,
        "roles": roles,
        "size_min": size_min,
        "size_max": size_max,
    }


ICP_SYSTEM_PROMPT = """Je bent een B2B bedrijfsanalist. Jouw taak is om te beoordelen of een bedrijf past bij het Ideal Customer Profile (ICP).

Je geeft ALTIJD een JSON response zonder extra tekst."""


ICP_SCORING_PROMPT = """Beoordeel dit bedrijf tegen het ICP en geef een JSON score.

BEDRIJF:
- Naam: {name}
- Domein: {domain}
- Indicatie industrie: {detected_industry}
- Geschrapt tekst (eerste 800 tekens):
{scraped_text}

ICP CRITERIA:
- Doel industrieën: {industries}
- Bedrijfsgrootte: {size_min}-{size_max} medewerkers
- B2B focus vereist (geen B2C consumentenbedrijven)
- Geografie: Nederland (Voorkeur voor .nl domeinen)

VLOD STRATEGIE (Verkopen, Leveren, Oplossen, Delen):
De doelgroep is ondernemers in 3 fases:
1. STARTENDE: Zoeken naar eerste klanten, leren verkopen, laag budget
2. OVERKOMENDE: Willen prijzen verhogen, consistente klanten, schalen
3. BLOEIENEN: Systemen bouwen, winst verhogen, meer doorverwijzingen

Signs per fase:
- STARTENDE: "eerste klant", "beginnend", "net gestart", "leercurve"
- OVERKOMENDE: "groeien", "schalen", "prijzen verhogen", "meer klanten"
- BLOEIENEN: "systemen", "automatisering", "winst", "team", "doorverwijzingen"

Geef een JSON response met EXACTLY deze structuur:
{{
  "industry_match": <0-30, hoe goed past de industrie>,
  "size_match": <0-25, geschatte grootte vs doel range>,
  "b2b_signals": <0-20, B2B signalen in de tekst>,
  "decision_maker": <0-15, aanwezigheid van beslisser info>,
  "geography": <0-10, Nederlandse signalen>,
  "total": <0-100, som van alle punten>,
  "detected_industry": "<geclassificeerde industrie>",
  "estimated_size": <geschat aantal medewerkers of null>,
  "is_b2b": <true/false>,
  "business_phase": "<startende/overkomende/bloeiende/unknown>",
  "pain_points": ["<lijst van gedetecteerde pijnpunten>"],
  "reasoning": "<korte uitleg in 1-2 zinnen>"
}}

Let op:
- industry_match: 30 als perfect match, 15 als gerelateerd, 0 als geen match
- size_match: 25 als in range, 15 als dichtbij, 0 als te groot/klein
- b2b_signals: 20 als duidelijk B2B (diensten, consultancy, wholesale), 10 als mixed, 0 als B2C
- decision_maker: 15 als directeur/eigenaar gevonden, 5 als contact info, 0 als geen
- geography: 10 als .nl domein of NL adres, 5 als EU, 0 als onbekend
- business_phase: Detecteer de fase van het bedrijf voor gepersonaliseerde aanpak
- pain_points: Identificeer specifieke problemen die het bedrijf heeft (bijv. "geen consistente klanten", "te lage prijzen", "geen tijd voor marketing")"""


async def call_gemini(prompt: str, system_prompt: str = "") -> str:
    """Call Google Gemini API."""
    if not _GEMINI_API_KEY:
        raise Exception("GEMINI_API_KEY not configured")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key={_GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}

    contents = []
    if system_prompt:
        contents.append({"role": "user", "parts": [{"text": system_prompt}]})
        contents.append({"role": "model", "parts": [{"text": "Ik begrijp de instructies."}]})
    contents.append({"role": "user", "parts": [{"text": prompt}]})

    payload = {
        "contents": contents,
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 500,
            "topP": 0.8,
        }
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            data = r.json()
            candidates = data.get("candidates", [])
            if candidates:
                content = candidates[0].get("content", {})
                parts = content.get("parts", [])
                if parts:
                    return parts[0].get("text", "")
        raise Exception(f"Gemini HTTP {r.status_code}: {r.text[:200]}")


def parse_icp_response(response_text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON from Gemini response, handling markdown code blocks."""
    text = response_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        json_lines = []
        in_block = False
        for line in lines:
            if line.startswith("```") and not in_block:
                in_block = True
                continue
            elif line.startswith("```") and in_block:
                break
            elif in_block:
                json_lines.append(line)
        text = "\n".join(json_lines)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        import re
        match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


async def score_company(
    name: str,
    domain: str,
    scraped_text: str,
    detected_industry: str = "unknown",
    tenant_id: str = "injexion",
) -> Dict[str, Any]:
    """Score a company against ICP using Gemini. Returns scoring breakdown."""
    icp = get_icp_config(tenant_id)

    prompt = ICP_SCORING_PROMPT.format(
        name=name,
        domain=domain,
        detected_industry=detected_industry,
        scraped_text=scraped_text[:800] if scraped_text else "(geen tekst beschikbaar)",
        industries=", ".join(icp["industries"]),
        size_min=icp["size_min"],
        size_max=icp["size_max"],
    )

    try:
        response = await call_gemini(prompt, ICP_SYSTEM_PROMPT)
        result = parse_icp_response(response)
        if result and "total" in result:
            result["passed"] = result["total"] >= 50
            return result
    except Exception as e:
        logger.warning(f"Gemini ICP scoring failed for {domain}: {e}")

    # Fallback: rule-based scoring
    return _rule_based_score(name, domain, scraped_text, detected_industry, icp)


def _rule_based_score(
    name: str,
    domain: str,
    scraped_text: str,
    detected_industry: str,
    icp: Dict[str, Any],
) -> Dict[str, Any]:
    """Fallback rule-based scoring when Gemini is unavailable."""
    text_lower = (scraped_text or "").lower()
    domain_lower = domain.lower()

    # Industry match (0-30)
    industry_score = 0
    detected = detected_industry.lower() if detected_industry else ""
    for ind in icp["industries"]:
        if ind.lower() in text_lower or ind.lower() in detected:
            industry_score = 30
            break
    if industry_score == 0:
        # Check related keywords
        related = {
            "groothandel": ["wholesale", "groothandel", "distributie", "import", "export"],
            "logistiek": ["logistiek", "transport", "opslag", "warehousing", "versnijding"],
            "saas": ["software", "saas", "platform", "applicatie", "cloud"],
            "it": ["ict", "it-dienst", "consultancy", "automatisering", "digitalisering"],
        }
        for ind_key, keywords in related.items():
            for kw in keywords:
                if kw in text_lower:
                    industry_score = 15
                    break
            if industry_score > 0:
                break

    # B2B signals (0-20)
    b2b_score = 0
    b2b_keywords = ["b2b", "bedrijven", "ondernemers", "zakelijk", "dienstverlening",
                     "consultancy", "advies", "oplossingen", "klanten", "partners"]
    b2b_count = sum(1 for kw in b2b_keywords if kw in text_lower)
    b2b_score = min(20, b2b_count * 5)

    # Geography (0-10)
    geo_score = 0
    if domain_lower.endswith(".nl"):
        geo_score = 10
    elif any(tld in domain_lower for tld in [".be", ".de", ".eu"]):
        geo_score = 5

    # Decision maker (0-15)
    dm_score = 0
    dm_keywords = ["directeur", "eigenaar", "oprichter", "ceo", "founder", "managing"]
    for kw in dm_keywords:
        if kw in text_lower:
            dm_score = 15
            break

    # Size match (0-25) - hard to determine from text, give neutral score
    size_score = 12  # neutral

    total = industry_score + b2b_score + geo_score + dm_score + size_score

    # Detect business phase (VLOD strategy)
    phase = "unknown"
    pain_points = []

    # Startende signals
    startende_signals = ["eerste klant", "beginnend", "net gestart", "startend", "opstarten"]
    if any(sig in text_lower for sig in startende_signals):
        phase = "startende"
        pain_points.append("geen consistente klanten")

    # Overkomende signals
    overkomende_signals = ["groeien", "schalen", "prijzen verhogen", "meer klanten", "consistente", "groei"]
    if any(sig in text_lower for sig in overkomende_signals):
        phase = "overkomende"
        pain_points.append("te lage prijzen")
        pain_points.append("geen schaalbaarheid")

    # Bloeiende signals
    bloeiende_signals = ["systemen", "automatisering", "winst", "team", "doorverwijzing", "systeem"]
    if any(sig in text_lower for sig in bloeiende_signals):
        phase = "bloeiende"
        pain_points.append("geen tijd voor strategie")

    # Generic pain points
    if "marketing" in text_lower or "bekendheid" in text_lower:
        pain_points.append("weinig zichtbaarheid")
    if "verkoop" in text_lower or "sales" in text_lower:
        pain_points.append("moeite met verkopen")
    if "klant" in text_lower:
        pain_points.append("klantbehoud")

    return {
        "industry_match": industry_score,
        "size_match": size_score,
        "b2b_signals": b2b_score,
        "decision_maker": dm_score,
        "geography": geo_score,
        "total": total,
        "detected_industry": detected_industry or "unknown",
        "estimated_size": None,
        "is_b2b": b2b_score >= 10,
        "business_phase": phase,
        "pain_points": pain_points[:3],  # max 3 pain points
        "reasoning": f"Rule-based scoring. Total: {total}/100. Phase: {phase}",
        "passed": total >= 50,
    }
