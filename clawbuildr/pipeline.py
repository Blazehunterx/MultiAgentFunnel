import json
import re
import random
import asyncio
import concurrent.futures
from models import LeadInput, ResearchData, TrustResult, OpportunityResult, QualificationResult, EmailDraft, LearningUpdate, CrmRecord, PipelineResult
from tools import scrape_url, kvk_lookup, gmail_send, db_append, db_query, save_prospect, save_crm_record, call_llm, hunter_domain_search

def run_sync(coro):
    """Safely runs async coroutine in a synchronous context using a background thread."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(coro))
        return future.result()

# ---------- MODE 1: Research ----------

async def research_company(lead: LeadInput) -> ResearchData:
    """Uses KVK lookup + website scraping + Gemini 2.5 Flash to generate a structured profile."""
    # KVK lookup
    kvk = await kvk_lookup(lead.company)
    kvk_str = json.dumps(kvk, indent=2) if kvk.get("found") else "No KVK record found"

    content = ""
    found_emails = []
    # Hunter disabled (free credits exhausted, not critical)
    hunter_name = None
    pages_scraped = 0
    
    if lead.domain:
        # 1. Scrape Website
        result = await scrape_url(f"https://{lead.domain}")
        content = result.get("text", "") if isinstance(result, dict) else result
        scraped_emails = result.get("emails", []) if isinstance(result, dict) else []
        pages_scraped = result.get("pages_scraped", 0) if isinstance(result, dict) else 0
        
        # 2. Hunter API for high-value emails & names
        # Hunter disabled — credits exhausted
        found_emails = scraped_emails

    # Snippet size optimization
    scraped_snippet = content[:8000] if content else "No scraped text available"

    system_prompt = (
        "You are the Research Agent. Your task is to analyze scraped website text and KVK registry data "
        "of a Dutch MKB company to extract strategic business insights. Return valid JSON only."
    )
    
    user_prompt = f"""
Analyze the following business information:
- Company Name: {lead.company}
- Domain: {lead.domain}
- KVK Details:
{kvk_str}

- Scraped Homepage / Subpage Snippet:
{scraped_snippet}

Return a JSON object matching this schema exactly:
{{
  "summary": "Concise Dutch summary of what this company does, its target audience, and business model.",
  "services": ["service 1", "service 2"],
  "signals": ["signal 1", "signal 2"],
  "data_quality_score": 75
}}
Where data_quality_score is an integer from 0 to 100 based on completeness of KVK and scraped text.
"""
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="llama3.2:3b", json_mode=True)
        data = json.loads(res_text.strip())
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None
    
    if not data:
        data = {
            "summary": f"Informatie over {lead.company} op domein {lead.domain}.",
            "services": ["Zakelijke diensten"],
            "signals": ["Aanwezigheid online"],
            "data_quality_score": 40
        }

    # Ensure found_emails are appended if found during scraping
    emails = found_emails
    if lead.contact_email and lead.contact_email not in emails:
        emails.insert(0, lead.contact_email)

    return ResearchData(
        company=lead.company,
        domain=lead.domain or "",
        summary=data.get("summary", "")[:2000],
        services=data.get("services", [])[:12],
        signals=data.get("signals", [])[:20],
        sources=["website"] if lead.domain else ["KVK"],
        data_quality_score=data.get("data_quality_score", 50),
        contact_emails=emails,
        contact_name=hunter_name
    )

# ---------- MODE 2: Trust / Deliverability ----------

def assess_trust(research: ResearchData) -> TrustResult:
    """Verifies the deliverability safety of the lead (Strict Quality Gate)."""
    risk_flags = []
    trust = 50

    if research.data_quality_score >= 75:
        trust += 20
    elif research.data_quality_score >= 50:
        trust += 15
    elif research.data_quality_score >= 30:
        trust += 10

    if research.signals:
        trust += 10

    if research.domain and any(tld in research.domain for tld in [".nl", ".eu", ".com"]):
        trust += 5

    # Bonus for having actual scraped content (not just KVK)
    if research.summary and len(research.summary) > 200:
        trust += 5

    if not research.sources:
        risk_flags.append("NO_DATA_SOURCES")
        trust -= 20

    if research.data_quality_score < 40:
        risk_flags.append("LOW_DATA_RELIABILITY")

    # Email verification step with quality gate
    if research.contact_emails:
        from tools import verify_email
        best_email = research.contact_emails[0]
        v = verify_email(best_email)
        if v["confidence"] >= 80:
            trust += 15
        elif v["confidence"] >= 70:
            trust += 5
        elif v["confidence"] >= 50:
            # Moderate confidence: flag but don't block
            trust -= 10
            risk_flags.append(f"LOW_CONFIDENCE_EMAIL: {best_email} (confidence: {v['confidence']})")
        else:
            # Very low confidence: significant penalty but not zero
            trust -= 25
            risk_flags.append(f"POOR_EMAIL: {best_email} ({v['reasons'][0] if v['reasons'] else 'low confidence'})")
    else:
        risk_flags.append("NO_EMAIL_FOUND")
        trust -= 15  # Penalty but not block

    trust = max(0, min(100, trust))

    decision = "PASS"
    if trust < 40:
        decision = "BLOCK"
    elif trust < 55:
        decision = "REVIEW"

    email_note = ""
    if research.contact_emails:
        email_note = f" Email: {research.contact_emails[0]}"

    return TrustResult(
        trust_score=trust,
        decision=decision,
        risk_flags=risk_flags,
        reasoning=f"Trust score {trust}/100 based on data quality, signals, sources.{email_note} Decision: {decision}."
    )

# ---------- MODE 3: Opportunity Mapping ----------

async def map_opportunity(research: ResearchData) -> OpportunityResult:
    """Uses Gemini 2.5 Flash to match target business pains with ClawBuildr offerings."""
    # Load active tenant value doctrine for dynamic prompt injection
    _value_doctrine = "AI-gestuurde automatisering en workflow optimalisatie voor MKB-bedrijven."
    try:
        import sqlite3 as _sq
        import os
        _db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")
        _tc = _sq.connect(_db, timeout=5.0).execute("SELECT value_doctrine, display_name FROM tenant_config WHERE active=1 LIMIT 1").fetchone()
        if _tc and _tc[0]:
            _value_doctrine = _tc[0]
    except Exception:
        pass

    system_prompt = (
        "You are the Opportunity Mapping Agent. You match business pain points to the following services: "
        + _value_doctrine +
        " Generate a qualified impact case. Return valid JSON only."
    )
    user_prompt = f"""
Analyze these research details of a Dutch MKB company:
- Company Summary: {research.summary}
- Services Offered: {research.services}
- Signals: {research.signals}

Your goal is to identify ACTUAL or highly probable internal operational inefficiencies/issues (e.g. lacking direct online booking, manual invoicing/quoting processes, slow customer response times, phone busy hours/interruptions, manual lead entry) based on their industry and website features.

CRITICAL RULES:
1. DO NOT copy the company's value propositions, marketing benefits, or customer promises (such as "Duidelijke prijzen", "heldere afspraken", "altijd betrokken", "kwaliteit") and label them as pains. These are benefits they offer to their clients, NOT internal inefficiencies.
2. Inferred Pains: Look at their features. If they have a phone number, they likely suffer from constant phone interruptions. If they have contact forms, they likely suffer from manual lead entry or slow email follow-ups.
3. Dutch Language & Natural Tone: Express the pain points in natural, professional Dutch.
4. NO ROBOT PREFIXES: Do NOT include prefixing like "Gedetecteerd...", "Op de pagina over ons valt op:", or "Scrape data". Just list the operational issue directly (e.g., "Veel tijd kwijt aan het handmatig inplannen van afspraken via de telefoon" or "Trage opvolging van offerte-aanvragen door handmatige administratie").

Match them to ClawBuildr's specific solutions:
- AI Email Assistants: Handles customer emails, quotes, support tickets.
- AI Phone Assistants: 24/7 conversational voice agent to book appointments/answer calls.
- Workflow Automation: Connects apps (Zapier/Make) to eliminate manual administration.
- CRM Integrations: Syncs databases (HubSpot, Salesforce, AFAS, Teamleader).

Return a JSON object matching this schema exactly:
{{
  "pain_points": ["Specific operational issue 1 in Dutch", "Specific operational issue 2 in Dutch"],
  "matched_services": ["AI Phone Assistants", "Workflow Automation"],
  "value_angles": ["Personalized value angle 1 in Dutch", "Personalized value angle 2 in Dutch"],
  "strongest_hook": "One strong value hook sentence in Dutch describing how we solve their specific pain"
}}
"""
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="llama3.2:3b", json_mode=True)
        data = json.loads(res_text.strip())
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None
    if not data:
        data = {
            "pain_points": ["Handmatige administratie en klantopvolging"],
            "matched_services": ["Workflow Automation"],
            "value_angles": ["Bespaar tijd door handmatig werk te automatiseren"],
            "strongest_hook": "Optimaliseer uw bedrijfsvoering met slimme workflows"
        }
    return OpportunityResult(
        pain_points=data.get("pain_points", [])[:5],
        matched_services=data.get("matched_services", [])[:4],
        value_angles=data.get("value_angles", [])[:4],
        strongest_hook=data.get("strongest_hook", "")
    )

# ---------- MODE 4: Qualification (BANT) ----------

async def qualify(research: ResearchData, opportunity: OpportunityResult) -> QualificationResult:
    """Uses llama3.2:3b to qualify the prospect against Budget, Authority, Need, and Timing (BANT)."""
    system_prompt = (
        "Je bent de Qualificatie Agent. Je analyseert het doelprofiel en de kansen om de leadprioriteit te scoren "
        "met SPIN- en BANT-kaders. Retourneer alleen geldige JSON."
    )
    pain_summary = "; ".join(opportunity.pain_points[:3]) if opportunity.pain_points else "geen specifieke pijnpunten geïdentificeerd"
    solution_summary = "; ".join(opportunity.matched_services[:3]) if opportunity.matched_services else "geen specifieke oplossingen"
    user_prompt = f"""
Analyseer deze gegevens en geef een EERLIJKE, GEDIFFERENTIEERDE BANT-score:

BEDRIJF:
- Samenvatting: {research.summary[:300]}
- Geïdentificeerde pijnpunten: {pain_summary}
- Matchende oplossingen: {solution_summary}
- Aanwezigheid op website: {len(research.signals)} signalen gedetecteerd
- Data kwaliteit: {research.data_quality_score}/100

BANT-SCORING — Wees STRENG en EERLIJK:
- budget (max 30): 
  - 25-30: Groot MKB (>50 medewerkers), duidelijke investerings信号en
  - 15-24: Gemiddeld MKB, enige tekenen van groei
  - 5-14: Klein bedrijf, geen investeringssignalen
  - 0-4: Geen enkel signaal

- authority (max 25):
  - 20-25: Directeur/Eigenaar gevonden met naam
  - 12-19: Rol onbekend maar waarschijnlijk beslisser
  - 5-11: Mogelijk niet-decisair (info@ email, geen naam)
  - 0-4: Geen contactgegevens

- need (max 30):
  - 25-30: 3+ pijnpunten die perfect matchen met onze diensten
  - 15-24: 1-2 pijnpunten die goed matchen
  - 5-14: Zwakke match, algemene behoeften
  - 0-4: Geen match

- timing (max 25):
  - 20-25: Actieve groei, recente vacatures, expansie
  - 12-19: Stabiel bedrijf, enige activiteit
  - 5-11: Weinig activiteit, geen urgentie
  - 0-4: Geen enkel timingssignaal

BELANGRIJK: Elk bedrijf moet een ANDERE score krijgen. Geen twee leads mogen dezelfde score hebben.
Bereken total_score = budget + authority + need + timing.
Geef een label: HOT (>=75), WARM (>=50), of COLD (<50).

Retourneer JSON:
{{
  "budget": <0-30>,
  "authority": <0-25>,
  "need": <0-30>,
  "timing": <0-25>,
  "total_score": <som>,
  "label": "HOT" of "WARM" of "COLD"
}}
"""
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="llama3.2:3b", json_mode=True)
        data = json.loads(res_text.strip())
        if not isinstance(data, dict):
            data = None
    except Exception as e:
        print(f"QUALIFY EXCEPTION: {e}")
        data = None
    if not data:
        data = {
            "budget": 12, "authority": 12, "need": 12, "timing": 12,
            "total_score": 48, "label": "COLD"
        }
    return QualificationResult(
        budget=data.get("budget", 12),
        authority=data.get("authority", 12),
        need=data.get("need", 12),
        timing=data.get("timing", 12),
        total_score=data.get("total_score", 48),
        label=data.get("label", "COLD")
    )

# ---------- MODE 5: Outreach (Dutch Email) ----------

async def generate_email(lead: LeadInput, research: ResearchData, opportunity: OpportunityResult) -> EmailDraft:
    """Uses Gemini 2.5 Flash to generate a highly personalized cold email outreach draft."""
    # Bug Fix: Many Dutch companies use .com or .io. Force Dutch unless specified otherwise.
    target_language = "Dutch"
    
    # Load active tenant signature block dynamically
    _sig_block = "Marvin van der Sluis"
    try:
        import sqlite3 as _sq
        import os
        _db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")
        _tc = _sq.connect(_db, timeout=5.0).execute("SELECT signature_block, calendar_link FROM tenant_config WHERE active=1 LIMIT 1").fetchone()
        if _tc:
            if _tc[0]: _sig_block = _tc[0]
    except Exception:
        pass

    system_prompt = (
        f"You are the Outreach Agent. Your job is to draft highly personalized outbound email sequences in {target_language} "
        f"targeting decision-makers of MKB companies. Return valid JSON only."
    )
    user_prompt = f"""
Draft a cold outreach email to:
- Name: {research.contact_name or lead.contact_name or "decision-maker"}
- Company: {lead.company}
- Summary: {research.summary}
- Key Pains: {opportunity.pain_points}
- Solutions: {opportunity.matched_services}
- Hook: {opportunity.strongest_hook}

CRITICAL RULES FOR COPYWRITING:
1. Word Count: STRICTLY under 120 words. Keep it short, direct, and pragmatic.
2. Tone: Friendly, direct, professional {target_language} (use 'je' or 'jullie' if Dutch). Avoid pushy sales pitches, hyperbole, or words like "revolutionair".
3. NO INTERNAL NOTES OR QUALIFICATIONS: Never mention internal database tags, qualifications, or internal assessments. NEVER say "the budget fits", "we speak to the right person", "budget past", "BANT", "SPIN", or anything about budget/authority. You have not spoken with them yet, so claiming the budget fits makes no sense.
4. NO ROBOT SPEAKER: Do not use phrases like "Gedetecteerd via deep scrape:", "Op de pagina over ons valt op:", or "Scrape data". Start the email naturally like a human who looked at their website.
5. Personalization Hook: Open directly with a natural, human observation about their services or website.
6. Call to Action: Low friction (e.g. 'Zullen we volgende week kort 10 minuten bellen?' or 'Could we have a brief 10 min call next week?').
7. NO PLACEHOLDERS: Do NOT use brackets like [Naam] or [Bedrijf]. You must write the actual name and company directly in the text.
8. Signature Block: End the email exactly with this signature block:
{"Met vriendelijke groet," if target_language == "Dutch" else "Best regards,"}

{_sig_block}

Return a JSON object matching this schema:
{{
  "subject": "Catchy subject line",
  "body": "Complete email body",
  "personalization_points_used": ["point 1", "point 2"]
}}
"""
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="llama3.2:3b", json_mode=True)
        data = json.loads(res_text.strip())
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None
    if not data:
        name_to_use = research.contact_name or lead.contact_name or 'relatie'
        name_to_use_en = research.contact_name or lead.contact_name or 'there'
        if target_language == "Dutch":
            subject = f"Idee voor {lead.company}"
            body = f"Beste {name_to_use},\n\nIk zag dat {lead.company} actief is met digitalisering. Wij bouwen maatwerk AI-assistenten die tijd besparen.\n\nZouden we kort kunnen bellen?\n\nMet vriendelijke groet,\n\nMarvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/\nhttps://goldclaw.ai/"
        else:
            subject = f"Idea for {lead.company}"
            body = f"Hi {name_to_use_en},\n\nI noticed {lead.company} is active in digitalization. We build custom AI assistants to save time.\n\nCould we have a quick call?\n\nBest regards,\n\nMarvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/\nhttps://goldclaw.ai/"
        data = {
            "subject": subject,
            "body": body,
            "personalization_points_used": []
        }
    body = data.get("body", "")
    body = re.sub(r'\[[^\]]+\]', '', body)
    return EmailDraft(
        subject=data.get("subject", ""),
        body=body.strip(),
        personalization_points_used=data.get("personalization_points_used", [])
    )

async def regenerate_email_with_data(
    lead: LeadInput,
    research: ResearchData,
    opportunity: OpportunityResult,
    opp_data: dict = None,
    qual_data: dict = None,
) -> EmailDraft:
    """Alternative helper used by the live dashboard to regenerate tailored copy."""
    target_language = "Dutch"
    system_prompt = (
        f"Je bent de Outreach Agent. Je schrijft persoonlijke cold outreach e-mails in het Nederlands "
        f"gericht op beslissers van MKB-bedrijven. Geef alleen geldige JSON terug."
    )
    user_prompt = f"""
Schrijf een koude outreach e-mail met deze details:
- Naam: {lead.contact_name}
- Bedrijf: {lead.company}
- Samenvatting: {research.summary}
- Kansen: {opp_data}
- Hook: {opportunity.strongest_hook}

STRIKTE REGELS:
1. Woordenaantal: STRENG onder 120 woorden.
2. Toon: Vriendelijk, direct, professioneel Nederlands (gebruik 'je' of 'jullie'). Geen pushy sales pitches.
3. GEEN INTERNE NOTITIES: Nooit interne kwalificaties, budget, of autoriteit vermelden. Je hebt nog niet met ze gesproken!
4. GEEN ROBOT TAAL: Geen "Gedetecteerd via deep scrape:" of tech jargon. Open natuurlijk als een mens.
5. Call to Action: Lage drempel (bijv. 'Zullen we volgende week kort 10 minuten bellen?').
6. Geen template houders als [Bedrijfsnaam] of [Naam] in de e-mail.
7. Gebruik de voornaam van de persoon in de aanhef (bijv. 'Beste {lead.contact_name}').
8. Nooit Engelse zinnen of woorden in een Nederlandse e-mail.
9. Maak de e-mail specifiek voor dit bedrijf — geen generieke tekst.

Retourneer een JSON object:
{{
  "subject": "Onderwerpregel",
  "body": "Volledige e-mail tekst",
  "personalization_points_used": ["punt 1", "punt 2"]
}}
"""
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="llama3.2:3b", json_mode=True)
        data = json.loads(res_text.strip())
        if not isinstance(data, dict):
            data = None
    except Exception:
        data = None
    if not data:
        data = {
            "subject": f"Idee voor {lead.company}",
            "body": f"Beste {lead.contact_name or 'relatie'},\n\nIk zag dat {lead.company} actief is. Wij bouwen AI-assistenten die tijd besparen.\n\nZouden we kort kunnen bellen?\n\nMet vriendelijke groet,\n\nMarvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/\nhttps://goldclaw.ai/",
            "personalization_points_used": []
        }
    body = data.get("body", "")
    body = re.sub(r'\[[^\]]+\]', '', body)
    return EmailDraft(
        subject=data.get("subject", ""),
        body=body.strip(),
        personalization_points_used=data.get("personalization_points_used", [])
    )

# ---------- MODE 6: Memory / Learning ----------

def update_learning(decision: str, trust_score: int, qual_score: int) -> LearningUpdate:
    learnings = db_query("recent", 5)
    
    insight = f"Pipeline run: decision={decision}, trust={trust_score}, qualification={qual_score}"
    trigger = f"trust < 70" if trust_score < 70 else f"qualification={qual_score}"

    adjustment = ""
    if decision == "BLOCK":
        adjustment = "Improve data gathering before running pipeline"
    elif decision == "DRAFT":
        adjustment = "Review qualification criteria - may need more personalized scoring"
    else:
        adjustment = "Pipeline executing within expected parameters"

    update = LearningUpdate(
        insight=insight,
        trigger_pattern=trigger,
        recommended_adjustment=adjustment
    )

    db_append({
        "insight": insight,
        "trigger_pattern": trigger,
        "recommendation": adjustment
    })

    return update

# ---------- MODE 7: CRM Formatting ----------

def format_crm(lead: LeadInput, trust: TrustResult, qual: QualificationResult, email_sent: bool, decision_label: str = "") -> CrmRecord:
    return CrmRecord(
        company=lead.company,
        domain=lead.domain or "",
        trust_score=trust.trust_score,
        qualification_score=qual.total_score,
        decision=decision_label or trust.decision,
        summary=f"Trust {trust.trust_score}/100, Qualification {qual.total_score}/100 ({qual.label})",
        email_sent=email_sent,
        reasoning_short=trust.reasoning
    )

# ---------- MAIN PIPELINE ORCHESTRATOR ----------

async def run_pipeline(lead: LeadInput) -> PipelineResult:
    # Step 1: Ingest prospect
    save_prospect(lead.dict())

    # Step 2: Research Mode
    research = await research_company(lead)

    # Step 3: Trust / Deliverability Mode (Safety Gate check)
    trust = assess_trust(research)
    if trust.decision == "BLOCK":
        update_learning("BLOCK", trust.trust_score, 0)
        crm = format_crm(lead, trust, QualificationResult(budget=0, authority=0, need=0, timing=0, total_score=0, label="COLD"), False, "BLOCKED")
        save_crm_record(crm.dict())
        return PipelineResult(
            decision="BLOCKED",
            trust_score=trust.trust_score,
            qualification_score=0,
            company=lead.company,
            crm_record=crm,
            reasoning_summary=f"Blocked at trust gate: {trust.reasoning}",
            learning_update=update_learning("BLOCKED", trust.trust_score, 0)
        )

    # Step 4: Opportunity Mapping
    opportunity = await map_opportunity(research)

    # Step 5: Qualification
    qual = await qualify(research, opportunity)

    if qual.total_score < 75:
        learning = update_learning("NURTURE", trust.trust_score, qual.total_score)
        crm = format_crm(lead, trust, qual, False, "NURTURE")
        save_crm_record(crm.dict())
        return PipelineResult(
            decision="NURTURE",
            trust_score=trust.trust_score,
            qualification_score=qual.total_score,
            company=lead.company,
            email=None,
            crm_record=crm,
            reasoning_summary=f"Score {qual.total_score}/100 is below 75, lead moved to NURTURE.",
            learning_update=learning
        )

    # Step 6: Outreach
    email = await generate_email(lead, research, opportunity)

    # Step 7: Final decision
    email_sent = False
    decision_label = "DRAFT"
    if lead.contact_email:
        # OUTREACH DISABLED PER INSTRUCTIONS
        # await gmail_send(lead.contact_email, email.subject, email.body)
        pass
    email_sent = False

    # Step 8: Learning
    learning = update_learning(decision_label, trust.trust_score, qual.total_score)

    # Step 9: CRM
    crm = format_crm(lead, trust, qual, email_sent, decision_label)
    save_crm_record(crm.dict())

    reasoning = (
        f"Trust={trust.trust_score}, Qual={qual.total_score}({qual.label}), "
        f"Decision={decision_label}, Email={'sent' if email_sent else 'draft'}"
    )

    return PipelineResult(
        decision=decision_label,
        trust_score=trust.trust_score,
        qualification_score=qual.total_score,
        company=lead.company,
        email=email,
        crm_record=crm,
        reasoning_summary=reasoning,
        learning_update=learning
    )
