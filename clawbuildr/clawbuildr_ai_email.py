#!/usr/bin/env python3
"""
ClawBuildr AI Email Generation via Gemini 2.5 Flash.
Generates personalized cold outreach emails using company research, ICP context, and conversation history.
"""

import os
import json
import sqlite3
import logging
import httpx
import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.AiEmail")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_GEMINI_API_KEYS = []
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("GEMINI_API_KEYS="):
            _GEMINI_API_KEYS = [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]
        elif line.startswith("GEMINI_API_KEY="):
            _GEMINI_API_KEYS = [line.split("=", 1)[1].strip()]

_key_idx = 0


def _next_key() -> str:
    global _key_idx
    if not _GEMINI_API_KEYS:
        raise RuntimeError("No GEMINI_API_KEYS configured in .env")
    key = _GEMINI_API_KEYS[_key_idx % len(_GEMINI_API_KEYS)]
    _key_idx += 1
    return key


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


CONSERVATIVE_TONE = """
CRITICAL RULES — NEVER BREAK:
- No sexual, suggestive, or flirtatious language. Ever.
- No body part mentions. No innuendo. No double entendres.
- No "exclusive page" — say "my website" or "check out my site".
- No "80% off" or aggressive discount language.
- Keep it professional, friendly, and platonic.
- Instagram auto-flags suggestive content + links = account restriction.
"""

DEFAULT_SYSTEM_PROMPT = f"""You are a B2B cold email specialist for ClawBuildr, a platform that helps service businesses automate their outreach.

Write short, personalized cold outreach emails in Dutch (or English if the company is international).
The email should feel like it was written by a human who did their research — not a template.

{CONSERVATIVE_TONE}

Email structure:
- Subject line: short, specific, no spam triggers
- Opening: reference something specific about their company (from research)
- Pain point: identify a clear problem they likely have
- Value prop: how ClawBuildr solves it (1 sentence)
- CTA: soft question, not a hard sell
- Sign-off: Marvin van der Sluis, ClawBuildr

Rules:
- Max 150 words
- No exclamation marks in subject
- No "Dear Sir/Madam" — use first name if available
- No generic phrases like "I hope this email finds you well"
- One clear CTA only
- Dutch B2B tone: direct but not aggressive
"""


# Template fallbacks when Gemini is rate-limited
_TEMPLATES_NL = [
    {
        "subject": "Snel vraagje over {company}",
        "body": """Hoi {first_name},

Ik zag dat jullie bij {company} actief zijn in de {industry} — knap werk.

Wij helpen soortgelijke bedrijven met het automatiseren van hun klantcommunicatie, zodat er meer tijd overblijft voor het echte werk.

Zou het zinvol zijn om kort te bekijken of dit ook voor {company} een verschil kan maken?

Groet,
Marvin van der Sluis
ClawBuildr""",
    },
    {
        "subject": "{company} — iets wat ik opviel",
        "body": """Hoi {first_name},

Ik was aan het kijken naar {company} en het viel me op dat jullie groeien — dat is altijd een goed teken.

In mijn ervaring groeien bedrijven vaak vast in hun processen op een gegeven moment. Wij helpen daarbij met slimme automatisering.

Geen sell — gewoon een kort gesprek of het relevant is. Zo niet, geen probleem.

Groet,
Marvin van der Sluis
ClawBuildr""",
    },
    {
        "subject": "Vraag over {company}",
        "body": """Hoi {first_name},

Ik houd me bezig met het helpen van MKB-bedrijven zoals {company} bij het stroomlijnen van hun dagelijkse processen.

Geen standaard pitch — ik wil gewoon weten: waar gaat jullie tijd het meest heen? En zou een deel daarvan automatiserbaar zijn?

Even kort bellen of mailen?

Groet,
Marvin van der Sluis
ClawBuildr""",
    },
]

_TEMPLATES_EN = [
    {
        "subject": "Quick question about {company}",
        "body": """Hi {first_name},

I noticed {company} is growing in the {industry} space — impressive work.

We help similar businesses automate their client communication so they can focus on what matters most.

Would it make sense to spend 10 minutes exploring if this could work for {company}?

Best,
Marvin van der Sluis
ClawBuildr""",
    },
    {
        "subject": "{company} — something I noticed",
        "body": """Hi {first_name},

I was looking into {company} and noticed you're scaling — that's always exciting.

In my experience, growing companies often hit process bottlenecks. We help with smart automation to keep things smooth.

No hard pitch — just a quick chat to see if it's relevant. If not, no worries.

Best,
Marvin van der Sluis
ClawBuildr""",
    },
]


def _template_fallback(company_name: str, first_name: str, role: str, research: str) -> Dict[str, Any]:
    """Generate email from templates when Gemini is unavailable."""
    import random

    # Detect language from research context or company name
    is_nl = True
    if research:
        research_lower = research.lower()
        if any(w in research_lower for w in ["london", "berlin", "paris", "international", "uk", "us", "global"]):
            is_nl = False

    templates = _TEMPLATES_NL if is_nl else _TEMPLATES_EN
    tmpl = random.choice(templates)

    # Extract industry from research if possible
    industry = "dienstverlening"
    if research:
        for word in ["IT", "tech", "marketing", "finance", "legal", "HR", "logistics", "bouw", "zorg"]:
            if word.lower() in research.lower():
                industry = word
                break

    subject = tmpl["subject"].format(company=company_name, first_name=first_name)
    body = tmpl["body"].format(
        company=company_name,
        first_name=first_name,
        industry=industry,
    )

    return {
        "subject": subject,
        "body": body,
        "confidence": 0.6,
        "model_used": "template_fallback",
        "tokens_used": 0,
    }


def _prefab_template_fallback(company_name: str, first_name: str, industry: str = "", pain_point: str = "", value_prop: str = "") -> Dict[str, Any]:
    """Use prefab_messages templates from the database as primary email source."""
    import random

    db = _get_db()
    try:
        templates = db.execute(
            "SELECT subject_line, body, variables FROM prefab_messages WHERE category = 'COLD' AND language = 'NL'"
        ).fetchall()
    finally:
        db.close()

    if not templates:
        return _template_fallback(company_name, first_name, "", "")

    tmpl = random.choice(templates)
    subject = tmpl["subject_line"] or f"Vraag over {company_name}"
    body = tmpl["body"] or ""

    # Fill in variables
    variables = {
        "first_name": first_name,
        "company_name": company_name,
        "industry": industry or "dienstverlening",
        "pain_point": pain_point or "handmatige processen en tijdrovende administratie",
        "value_prop": value_prop or "het automatiseren van klantcommunicatie",
        "calendar_link": "https://calendly.com/marvin-clawbuildr",
        "sender_name": "Marvin van der Sluis",
    }

    for var, value in variables.items():
        subject = subject.replace("{{" + var + "}}", value)
        body = body.replace("{{" + var + "}}", value)

    # Clean up any unfilled variables
    subject = re.sub(r'\{\{[a-z_]+\}\}', '', subject).strip()
    body = re.sub(r'\{\{[a-z_]+\}\}', '', body).strip()

    return {
        "subject": subject,
        "body": body,
        "confidence": 0.7,
        "model_used": "prefab_template",
        "tokens_used": 0,
    }


def generate_ai_email(
    contact_id: int,
    company_name: str,
    contact_name: str = "",
    role: str = "",
    research_context: str = "",
    icp_context: str = "",
    previous_emails: Optional[List[Dict]] = None,
    tone: str = "professional",
    language: str = "nl",
    custom_prompt: str = "",
) -> Dict[str, Any]:
    """Generate a personalized email using Gemini 2.5 Flash.

    Returns dict with keys: subject, body, confidence, model_used, tokens_used
    """
    db = _get_db()
    try:
        contact = db.execute(
            "SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)
        ).fetchone()
    finally:
        db.close()

    research = research_context
    if not research and contact:
        research = contact["research_result"] or ""

    first_name = contact_name.split()[0] if contact_name else ""
    if not first_name and contact:
        first_name = contact["first_name"] or ""

    system_prompt = DEFAULT_SYSTEM_PROMPT
    if language == "en":
        system_prompt = system_prompt.replace("Dutch", "English")
    if custom_prompt:
        system_prompt += f"\n\nAdditional instructions: {custom_prompt}"

    context_parts = []
    context_parts.append(f"Company: {company_name}")
    if first_name:
        context_parts.append(f"Contact: {first_name}")
    if role:
        context_parts.append(f"Role: {role}")
    if research:
        context_parts.append(f"Research findings:\n{research[:1500]}")
    if icp_context:
        context_parts.append(f"ICP context: {icp_context}")
    if previous_emails:
        context_parts.append("Previous emails in this thread:")
        for pe in previous_emails[-3:]:
            direction = "We sent" if pe.get("direction") == "outbound" else "They replied"
            context_parts.append(f"  [{direction}]: {pe.get('body', '')[:300]}")

    prompt = "\n".join(context_parts)
    prompt += "\n\nGenerate a subject line and email body. Return JSON: {\"subject\": \"...\", \"body\": \"...\"}"

    result = _call_gemini(prompt, system_prompt)

    if not result.get("success"):
        logger.warning(f"AI email generation failed for contact {contact_id}, using prefab template: {result.get('error')}")
        return _prefab_template_fallback(company_name, first_name, role, research)

    parsed = _parse_email_json(result["text"])

    # Validate that Gemini actually returned content
    subject = (parsed.get("subject") or "").strip()
    body = (parsed.get("body") or "").strip()
    if not subject or not body or len(body) < 20:
        logger.warning(f"AI email too short for {contact_id}, using prefab template")
        return _prefab_template_fallback(company_name, first_name, role, research)

    confidence = _estimate_confidence(parsed, company_name, first_name)

    return {
        "subject": subject,
        "body": body,
        "confidence": confidence,
        "model_used": result.get("model", "unknown"),
        "tokens_used": result.get("tokens", 0),
    }


def generate_followup_email(
    contact_id: int,
    company_name: str,
    previous_emails: List[Dict],
    followup_step: int = 1,
    tone: str = "professional",
) -> Dict[str, Any]:
    """Generate a follow-up email based on previous correspondence."""
    db = _get_db()
    try:
        contact = db.execute(
            "SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)
        ).fetchone()
    finally:
        db.close()

    first_name = ""
    if contact:
        first_name = contact["first_name"] or ""

    system_prompt = f"""You are a B2B follow-up email specialist.
Write a short follow-up email that continues the conversation naturally.
This is follow-up #{followup_step} after no reply.

{CONSERVATIVE_TONE}

Rules:
- Max 80 words
- Reference the previous email briefly
- Add ONE new value point or insight
- Soft CTA: asking a question, not demanding
- Never guilt-trip or pressure
- Dutch B2B tone
"""

    context_parts = [f"Company: {company_name}"]
    if first_name:
        context_parts.append(f"Contact: {first_name}")
    context_parts.append(f"Follow-up step: {followup_step}")
    context_parts.append("Previous emails:")
    for pe in previous_emails[-5:]:
        direction = "We sent" if pe.get("direction") == "outbound" else "They replied"
        context_parts.append(f"  [{direction}]: {pe.get('body', '')[:400]}")

    prompt = "\n".join(context_parts)
    prompt += "\n\nGenerate a follow-up subject and body. Return JSON: {\"subject\": \"...\", \"body\": \"...\"}"

    result = _call_gemini(prompt, system_prompt)

    if not result.get("success"):
        return {"subject": "", "body": "", "confidence": 0, "error": result.get("error")}

    parsed = _parse_email_json(result["text"])
    return {
        "subject": parsed.get("subject", ""),
        "body": parsed.get("body", ""),
        "confidence": _estimate_confidence(parsed, company_name, first_name),
        "model_used": result.get("model", "gemini-3.6-flash"),
        "tokens_used": result.get("tokens", 0),
    }


def _call_gemini(prompt: str, system_prompt: str = "", model: str = "gemini-3.1-flash-lite") -> Dict[str, Any]:
    """Call Gemini API with retry, key rotation, and exponential backoff."""
    import time as _time
    last_error = None
    tried_keys = set()

    for attempt in range(min(len(_GEMINI_API_KEYS), 6)):
        key = _next_key()
        if key in tried_keys:
            continue
        tried_keys.add(key)

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        if system_prompt:
            payload["systemInstruction"] = {"parts": [{"text": system_prompt}]}

        try:
            with httpx.Client(timeout=60.0) as client:
                resp = client.post(url, json=payload)
                if resp.status_code == 429:
                    last_error = "Rate limited"
                    delay = min(5 * (attempt + 1), 30)
                    _time.sleep(delay)
                    continue
                if resp.status_code != 200:
                    last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    continue

                data = resp.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    last_error = "No candidates in response"
                    continue

                text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                usage = data.get("usageMetadata", {})
                tokens = usage.get("totalTokenCount", 0)

                return {"success": True, "text": text, "model": model, "tokens": tokens}
        except Exception as e:
            last_error = str(e)[:100]

    return {"success": False, "error": last_error or "All API keys exhausted", "model": model}


def _parse_email_json(text: str) -> Dict[str, str]:
    """Parse email JSON from LLM response, handling markdown code blocks."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[^{}]*"subject"\s*:\s*"[^"]*"[^{}]*"body"\s*:\s*"[^"]*"[^{}]*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        subject_match = re.search(r'"subject"\s*:\s*"([^"]*)"', text)
        body_match = re.search(r'"body"\s*:\s*"((?:[^"\\]|\\.)*)"', text, re.DOTALL)
        return {
            "subject": subject_match.group(1) if subject_match else "",
            "body": body_match.group(1).replace("\\n", "\n") if body_match else text[:500],
        }


def _estimate_confidence(email: Dict, company_name: str, first_name: str) -> float:
    """Estimate email quality confidence (0-1)."""
    score = 0.5
    body = email.get("body", "")
    subject = email.get("subject", "")

    if company_name.lower() in body.lower():
        score += 0.1
    if first_name and first_name.lower() in body.lower():
        score += 0.1
    if len(body) > 50:
        score += 0.05
    if len(body) < 500:
        score += 0.05
    if "?" in body:
        score += 0.05
    if any(w in body.lower() for w in ["probleem", "uitdaging", "helpen", "oplossing"]):
        score += 0.1
    if "!" in subject:
        score -= 0.1

    return min(max(score, 0.0), 1.0)


def save_generated_email(
    contact_id: int,
    subject: str,
    body: str,
    direction: str = "outbound",
    confidence: float = 0.0,
    model_used: str = "",
) -> int:
    """Save generated email to DB. Returns email_id."""
    db = _get_db()
    try:
        cursor = db.execute(
            """INSERT INTO emails (contact_id, direction, status, subject, body, created_at, sent_at)
               VALUES (?, ?, 'draft', ?, ?, ?, NULL)""",
            (contact_id, direction, subject, body, datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
        email_id = cursor.lastrowid

        db.execute(
            """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
               VALUES (?, 'email_generated', 'AI email generated', ?, ?)""",
            (contact_id, json.dumps({"confidence": confidence, "model": model_used}), datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
        return email_id
    finally:
        db.close()


if __name__ == "__main__":
    result = generate_ai_email(
        contact_id=1,
        company_name="Test BV",
        contact_name="Jan de Vries",
        role="Founder",
        research_context="They do IT consulting for SMBs in Amsterdam.",
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
