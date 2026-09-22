import json
import re
import random
import asyncio
import concurrent.futures
import logging
from models import LeadInput, ResearchData, TrustResult, OpportunityResult, QualificationResult, EmailDraft, LearningUpdate, CrmRecord, PipelineResult
from tools import scrape_url, kvk_lookup, gmail_send, db_append, db_query, save_prospect, save_crm_record, call_llm, hunter_domain_search, deep_research_company, extract_json_from_llm_response

logger = logging.getLogger("Pipeline")

def run_sync(coro):
    """Safely runs async coroutine in a synchronous context using a background thread."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(coro))
        return future.result()

# ---------- MODE 1: Research (DEEP MULTI-SOURCE) ----------

async def research_company(lead: LeadInput) -> ResearchData:
    """Deep multi-source company research: website, KVK, LinkedIn, social, Google signals."""
    
    # Parallel data collection
    kvk_task = asyncio.ensure_future(asyncio.wait_for(kvk_lookup(lead.company), timeout=10))
    
    content = ""
    found_emails = []
    pages_scraped = 0
    social_links = {}
    phones = []
    linkedin_data = {}
    employees = []
    google_signals = []
    
    # Website deep scrape
    if lead.domain:
        try:
            result = await asyncio.wait_for(deep_research_company(f"https://{lead.domain}"), timeout=20)
        except asyncio.TimeoutError:
            result = {"text": "", "emails": [], "pages_scraped": 0, "social_links": {}, "phones": []}
        content = result.get("text", "") if isinstance(result, dict) else result
        scraped_emails = result.get("emails", []) if isinstance(result, dict) else []
        pages_scraped = result.get("pages_scraped", 0) if isinstance(result, dict) else 0
        social_links = result.get("social_links", {}) if isinstance(result, dict) else {}
        phones = result.get("phones", []) if isinstance(result, dict) else []
        found_emails = scraped_emails
        
        # Google search signals (news, hiring, press)
        try:
            from tools import _search_google
            company_name_enc = lead.company.replace(" ", "+")
            queries = [
                f'"{lead.company}" vacatures OR hiring OR werken',
                f'"{lead.company}" nieuws OR news OR persbericht',
                f'"{lead.company}" klanten OR cases OR projecten',
            ]
            for q in queries[:2]:
                try:
                    results = await asyncio.wait_for(_search_google(q), timeout=10)
                    for r in results[:3]:
                        google_signals.append(r.get("snippet", "")[:200])
                except Exception:
                    pass
        except Exception:
            pass
        
        # LinkedIn company + employees + optional profile enrichment
        linkedin_url = social_links.get("linkedin", "")
        profile_url = getattr(lead, "linkedin_url", "") or ""

        # Try to find LinkedIn profile URL from DB if not provided
        if not profile_url and lead.contact_name:
            try:
                import sqlite3 as _sq
                import os as _os
                _db = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "clawbuildr.db")
                _con = _sq.connect(_db, timeout=5.0)
                _parts = lead.contact_name.strip().split()
                if len(_parts) >= 2:
                    _row = _con.execute(
                        "SELECT linkedin_url FROM contacts WHERE first_name = ? AND last_name = ? AND linkedin_url IS NOT NULL AND linkedin_url != '' LIMIT 1",
                        (_parts[0], _parts[-1])
                    ).fetchone()
                    if _row and _row[0]:
                        profile_url = _row[0]
                _con.close()
            except Exception:
                pass

        if not linkedin_url and lead.company:
            slug = re.sub(r'[^a-zA-Z0-9]', '-', lead.company.lower()).strip('-')
            linkedin_url = f"https://www.linkedin.com/company/{slug}/"

        profile_data = {}
        if profile_url:
            try:
                from read_linkedin_profile import read_linkedin_profile
                loop = asyncio.get_event_loop()
                profile_data = await asyncio.wait_for(
                    loop.run_in_executor(None, read_linkedin_profile, profile_url),
                    timeout=90
                )
                if profile_data.get("current_company") and not linkedin_url:
                    slug = re.sub(r'[^a-zA-Z0-9]', '-', profile_data["current_company"].lower()).strip('-')
                    linkedin_url = f"https://www.linkedin.com/company/{slug}/"
            except Exception as e:
                logger.warning(f"[Pipeline] LinkedIn profile enrichment failed: {e}")

        if linkedin_url:
            try:
                from linkedin_engine import scrape_linkedin_company, enumerate_company_employees
                loop = asyncio.get_event_loop()
                linkedin_data = await asyncio.wait_for(
                    loop.run_in_executor(None, scrape_linkedin_company, linkedin_url),
                    timeout=75
                )
                if linkedin_data and "error" not in linkedin_data:
                    try:
                        employees = await asyncio.wait_for(
                            loop.run_in_executor(None, enumerate_company_employees, linkedin_url, 10),
                            timeout=15
                        )
                        linkedin_data["employees"] = employees or []
                    except asyncio.TimeoutError:
                        linkedin_data["employees"] = []
            except asyncio.TimeoutError:
                linkedin_data = {"error": "linkedin_scrape_timeout", "url": linkedin_url}
            except Exception as e:
                linkedin_data = {"error": str(e)[:100]}

        # Persist enrichment to DB
        try:
            from tools import save_linkedin_enrichment
            save_linkedin_enrichment(lead.company, lead.domain or "", profile_data, linkedin_data)
        except Exception as e:
            logger.warning(f"[Pipeline] Failed to save enrichment: {e}")
    
    # Wait for KVK
    try:
        kvk = await kvk_task
    except Exception:
        kvk = {"found": False, "error": "timeout"}
    kvk_str = json.dumps(kvk, indent=2) if kvk.get("found") else "No KVK record found"
    
    # Build rich context for LLM
    scraped_snippet = content[:8000] if content else "No scraped text available"
    
    linkedin_context = ""
    if linkedin_data and "error" not in linkedin_data:
        linkedin_context = f"""
LINKEDIN COMPANY PROFILE:
- Industry: {linkedin_data.get('industry', 'Unknown')}
- Company Size: {linkedin_data.get('size', 'Unknown')}
- Headquarters: {linkedin_data.get('headquarters', 'Unknown')}
- Founded: {linkedin_data.get('founded', 'Unknown')}
- Specialties: {linkedin_data.get('specialties', 'Unknown')}
- Employee Count: {linkedin_data.get('employee_count_text', 'Unknown')}
- About: {linkedin_data.get('about_text', '')[:1500]}
"""
        if employees:
            linkedin_context += f"\nKEY EMPLOYEES ({len(employees)} found):\n"
            for emp in employees[:8]:
                linkedin_context += f"  - {emp.get('name', '?')} | {emp.get('title', '?')}\n"
    
    social_context = ""
    if social_links:
        social_context = "\nSOCIAL MEDIA PRESENCE:\n"
        for platform, url in social_links.items():
            social_context += f"  - {platform}: {url}\n"
    
    google_context = ""
    if google_signals:
        google_context = "\nGOOGLE SEARCH SIGNALS (recent news/hiring):\n"
        for s in google_signals[:5]:
            google_context += f"  - {s}\n"
    
    # LLM Research Analysis
    system_prompt = """You are a Senior Business Intelligence Analyst. Your job is to deeply analyze a Dutch B2B company using ALL available data sources. Extract strategic insights that would help a sales person understand this company's operations, challenges, and opportunities.

Focus on:
1. What EXACTLY does this company do? (specific services, products, target market)
2. How big are they? (employees, revenue estimate, growth stage)
3. What's their technology stack? (CRM, website platform, tools they use)
4. What are their VISIBLE pain points? (hiring signals, customer complaints, manual processes)
5. Who are the DECISION MAKERS? (names, titles, roles from LinkedIn)
6. What's their GROWTH TRAJECTORY? (expanding, stable, declining)
7. What SPECIFIC operational challenges do they face based on their industry?
"""
    
    user_prompt = f"""
DEEP COMPANY ANALYSIS — {lead.company}

BASIC INFO:
- Company: {lead.company}
- Domain: {lead.domain}
- KVK Data: {kvk_str}

WEBSITE CONTENT (first 8000 chars):
{scraped_snippet}

{linkedin_context}{social_context}{google_context}

TASK: Analyze this company and return a structured JSON with:

{{
  "company_name": "Official company name",
  "one_liner": "One sentence: what they do, for whom, where",
  "industry": "Specific industry (not generic)",
  "company_size": "micro/small/medium/large with employee estimate",
  "growth_stage": "startup/growing/stable/mature/declining",
  "business_model": "B2B/B2C/B2B2C + how they make money",
  "target_customers": "Who are their clients?",
  "services_summary": ["Specific service 1", "Specific service 2", ...],
  "tech_stack": ["CRM they use", "Website platform", "Other tools detected"],
  "pain_points": [
    "Specific operational challenge 1 (with evidence)",
    "Specific operational challenge 2 (with evidence)"
  ],
  "decision_makers": [
    {{"name": "Full Name", "title": "Job Title", "role": "decision_maker/influencer/user"}}
  ],
  "growth_signals": ["Evidence of growth 1", "Evidence of growth 2"],
  "social_presence": {{"platform": "url", ...}},
  "personalization_hooks": [
    "Specific fact about company 1 (for email opening)",
    "Specific fact about company 2 (for LinkedIn message)"
  ],
  "data_quality_score": 75
}}

RULES:
- Pain points must be SPECIFIC and EVIDENCE-BASED (not generic "manual processes")
- Decision makers from LinkedIn data only — don't guess names
- Personalization hooks must be UNIQUE to this company (not "jullie bedrijf groeit")
- Data quality score: 0-100 based on how many sources confirmed the data
"""
    
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="gemini-3.1-flash-lite", json_mode=True, max_tokens=2048)
        data = extract_json_from_llm_response(res_text)
    except Exception as e:
        logger.warning(f"Research LLM failed: {e}")
        data = None
    
    # Fallback / enrichment merge: ensure key fields are populated even if LLM returns sparse data
    industry = linkedin_data.get("industry", "Unknown") if linkedin_data else "Unknown"
    size = linkedin_data.get("size", "unknown") if linkedin_data else "unknown"
    about = linkedin_data.get("about_text", "")[:300] if linkedin_data else ""
    services = []
    if linkedin_data and linkedin_data.get("specialties"):
        services = [s.strip() for s in linkedin_data["specialties"].split(",") if s.strip()][:8]
    fallback_signals = []
    if linkedin_data and linkedin_data.get("hiring_count", 0) > 0:
        fallback_signals.append(f"{linkedin_data['hiring_count']} openstaande vacatures bij {lead.company}")
    if profile_data and profile_data.get("current_role"):
        fallback_signals.append(f"Contact: {profile_data.get('name', '')} — {profile_data['current_role']}")
    if profile_data and profile_data.get("mutual_connections_count", 0) > 0:
        fallback_signals.append(f"{profile_data['mutual_connections_count']} gemeenschappelijke LinkedIn-connectie(s)")
    if linkedin_data and linkedin_data.get("posts"):
        fallback_signals.append(f"Recente bedrijfspost: {linkedin_data['posts'][0][:120]}")
    if profile_data and profile_data.get("posts"):
        fallback_signals.append(f"Recente persoonlijke post: {profile_data['posts'][0][:120]}")

    if not data:
        data = {}

    # Fill missing/empty fields from enrichment
    if not data.get("company_name"):
        data["company_name"] = lead.company
    if not data.get("one_liner"):
        data["one_liner"] = about or f"{lead.company} is actief in {industry}"
    if not data.get("industry") or data.get("industry") == "Unknown":
        data["industry"] = industry
    if not data.get("company_size") or data.get("company_size") == "unknown":
        data["company_size"] = size
    if not data.get("services_summary"):
        data["services_summary"] = services if services else ["Zakelijke diensten"]
    if not data.get("growth_signals"):
        data["growth_signals"] = fallback_signals
    if not data.get("personalization_hooks"):
        data["personalization_hooks"] = fallback_signals[:3]
    if not data.get("pain_points"):
        data["pain_points"] = ["Handmatige processen en administratie"]
    if not data.get("data_quality_score"):
        data["data_quality_score"] = 55 if linkedin_data and "error" not in linkedin_data else 40
    
    # Ensure found_emails are appended
    emails = found_emails
    if lead.contact_email and lead.contact_email not in emails:
        emails.insert(0, lead.contact_email)
    
    # Try to find personal email via Hunter.io + pattern
    if lead.domain and lead.contact_name and not any("@" in e and not e.startswith("info") and not e.startswith("contact") for e in emails):
        try:
            from email_finder import find_personal_email
            parts = lead.contact_name.strip().split()
            if len(parts) >= 2:
                first_name = parts[0]
                last_name = parts[-1]
                personal = find_personal_email(first_name, last_name, lead.domain)
                if personal.get("email") and personal.get("confidence", 0) >= 50:
                    emails.insert(0, personal["email"])
                    logger.info(f"[Pipeline] Found personal email: {personal['email']} (confidence: {personal['confidence']})")
        except Exception as e:
            logger.warning(f"[Pipeline] Personal email lookup failed: {e}")
    
    # Build research result
    sources = ["website"] if lead.domain else ["KVK"]
    if linkedin_data and "error" not in linkedin_data:
        sources.append("linkedin")
    if social_links:
        sources.append("social_media")
    if google_signals:
        sources.append("google")
    
    # Merge LLM insights into ResearchData
    services = data.get("services_summary", []) or data.get("services", [])
    signals = []
    signals.extend(data.get("growth_signals", []))
    signals.extend(data.get("pain_points", []))
    if data.get("personalization_hooks"):
        signals.extend(data["personalization_hooks"])
    
    return ResearchData(
        company=data.get("company_name", lead.company),
        domain=lead.domain or "",
        summary=data.get("one_liner", data.get("summary", ""))[:2000],
        services=services[:12],
        signals=signals[:20],
        sources=sources,
        data_quality_score=data.get("data_quality_score", 50),
        contact_emails=emails,
        contact_name=lead.contact_name,
        # LinkedIn data
        linkedin_company_url=linkedin_data.get("url", "") if linkedin_data else "",
        linkedin_industry=data.get("industry", linkedin_data.get("industry", "") if linkedin_data else ""),
        linkedin_size=data.get("company_size", linkedin_data.get("size", "") if linkedin_data else ""),
        linkedin_about=linkedin_data.get("about_text", "")[:2000] if linkedin_data else "",
        linkedin_specialties=linkedin_data.get("specialties", "") if linkedin_data else "",
        linkedin_headquarters=linkedin_data.get("headquarters", "") if linkedin_data else "",
        linkedin_founded=linkedin_data.get("founded", "") if linkedin_data else "",
        linkedin_employee_count=linkedin_data.get("employee_count_text", "") if linkedin_data else "",
        linkedin_website=linkedin_data.get("website", "") if linkedin_data else "",
        linkedin_followers=linkedin_data.get("followers", "") if linkedin_data else "",
        linkedin_hiring_count=linkedin_data.get("hiring_count", 0) if linkedin_data else 0,
        linkedin_hiring_jobs=linkedin_data.get("hiring_jobs", []) if linkedin_data else [],
        linkedin_company_posts=linkedin_data.get("posts", []) if linkedin_data else [],
        # Social & phones
        social_links=social_links,
        phones=phones,
        # Employees
        employees=employees,
        # Deep profile data
        headline=profile_data.get("headline", "") if profile_data else None,
        location=profile_data.get("location", "") if profile_data else None,
        experience=json.dumps(profile_data.get("experience", [])) if profile_data and profile_data.get("experience") else None,
        education=json.dumps(profile_data.get("education", [])) if profile_data and profile_data.get("education") else None,
        skills=", ".join(profile_data.get("skills", [])) if profile_data and profile_data.get("skills") else None,
        tenure=profile_data.get("tenure", "") if profile_data else None,
        profile_posts=profile_data.get("posts", []) if profile_data else [],
        mutual_connections_count=profile_data.get("mutual_connections_count", 0) if profile_data else 0,
        mutual_connections_text=profile_data.get("mutual_connections_text", "") if profile_data else None,
        # KVK data
        kvk_data=kvk if kvk.get("found") else {}
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
            trust += 10
        elif v["confidence"] >= 50:
            trust += 5
        else:
            risk_flags.append("LOW_EMAIL_CONFIDENCE")
            trust -= 10

    # Decision maker access bonus
    if research.employees:
        trust += 5
        for emp in research.employees[:3]:
            title = (emp.get("title") or "").lower()
            if any(w in title for w in ["directeur", "ceo", "owner", "eigenaar", "oprichter", "manager", "head"]):
                trust += 5
                break

    trust = max(0, min(100, trust))
    decision = "PASS" if trust >= 60 else ("REVIEW" if trust >= 40 else "BLOCK")

    return TrustResult(
        trust_score=trust,
        decision=decision,
        risk_flags=risk_flags,
        reasoning=f"Trust {trust}/100 based on data quality {research.data_quality_score}/100, {len(research.sources)} sources, {len(research.signals)} signals."
    )

# ---------- MODE 3: Opportunity Mapping (DEEP PAIN ANALYSIS) ----------

async def map_opportunity(research: ResearchData) -> OpportunityResult:
    """Deep opportunity mapping using all research data to find SPECIFIC pain points."""
    
    # Load tenant config
    _value_doctrine = "Implementation partner: de operating layer onder de afdelingen. Herhalende werkt naar het systeem, mensen houden ruimte voor oordeel."
    _clawbuildr_services = [
        "Operating layer — bron van waarde, agent laag, controle laag",
        "Proces-analyse — gouden batch, knelpunten, herhalende taken",
        "Waardecreatie — elke euro structurele kosten = ~5 euro bedrijfswaarde"
    ]
    try:
        import sqlite3 as _sq
        import os
        _db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")
        _tc = _sq.connect(_db, timeout=5.0).execute("SELECT value_doctrine, display_name FROM tenant_config WHERE active=1 LIMIT 1").fetchone()
        if _tc and _tc[0]:
            _value_doctrine = _tc[0]
    except Exception:
        pass
    
    # Build comprehensive context
    pain_evidence = research.signals[:10] if research.signals else []
    pain_str = "\n".join([f"  - {p}" for p in pain_evidence]) if pain_evidence else "  - Geen specifieke pijnpunten gedetecteerd"
    
    employee_str = ""
    if research.employees:
        employee_str = "\nBESLISSERS (via LinkedIn):\n"
        for emp in research.employees[:5]:
            employee_str += f"  - {emp.get('name', '?')} | {emp.get('title', '?')}\n"
    
    industry = research.linkedin_industry or "Unknown"
    size = research.linkedin_size or "Unknown"
    about = research.linkedin_about[:1000] if research.linkedin_about else "Niet beschikbaar"
    
    system_prompt = """You are a Senior Sales Intelligence Analyst specializing in Dutch B2B SaaS sales. Your job is to identify EXACT operational pain points that our AI automation solutions can solve.

CRITICAL RULES:
1. NEVER use the company's own marketing claims as pain points (e.g., "duidelijke prijzen", "kwaliteit", "persoonlijke service")
2. NEVER use generic pain points like "handmatige processen" — be SPECIFIC to THIS company
3. Pain points must be INTERNAL OPERATIONAL problems, not customer-facing issues
4. Base pain points on EVIDENCE from the research data
5. Match pain points to SPECIFIC ClawBuildr solutions with concrete ROI

AVAILABLE SOLUTIONS:
"""
    for svc in _clawbuildr_services:
        system_prompt += f"- {svc}\n"
    
    user_prompt = f"""
DEEP OPPORTUNITY ANALYSIS for: {research.company}

COMPANY PROFILE:
- One-liner: {research.summary}
- Industry: {industry}
- Size: {size}
- About: {about}

DETECTED SIGNALS (evidence of pain points):
{pain_str}

KEY CONTACTS:
{employee_str if employee_str else "  - No specific contacts identified"}

SOCIAL PRESENCE: {', '.join(research.social_links.keys()) if research.social_links else 'Limited'}
PHONE NUMBERS: {', '.join(research.phones[:3]) if research.phones else 'Not detected'}
DATA QUALITY: {research.data_quality_score}/100

YOUR TASK:
1. Identify the TOP 2-3 SPECIFIC operational pain points based on the evidence
2. For each pain point, explain WHY it's a problem for THIS company
3. Match each pain point to the MOST relevant ClawBuildr solution
4. Calculate estimated hours saved per month for each solution
5. Create a compelling value hook that addresses their SPECIFIC situation

Return JSON:
{{
  "pain_points": [
    {{
      "problem": "SPECIFIC operational problem (not generic)",
      "evidence": "What in the data suggests this problem exists",
      "impact": "Business impact of this problem (time lost, revenue lost, etc.)"
    }}
  ],
  "matched_solutions": [
    {{
      "solution": "Solution name",
      "pain_addressed": "Which pain point this solves",
      "roi": "Concrete ROI (hours saved, cost reduced, etc.)",
      "implementation": "How it would work for this specific company"
    }}
  ],
  "strongest_hook": "One compelling sentence that addresses their biggest pain point with a specific solution",
  "personalization_angle": "What makes THIS company different from others (for email personalization)"
}}
"""
    
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="gemini-3.1-flash-lite", json_mode=True, max_tokens=2048)
        data = extract_json_from_llm_response(res_text)
    except Exception as e:
        logger.warning(f"map_opportunity LLM failed: {e}")
        data = None
    
    if not data or not data.get("pain_points"):
        # Fallback with context-aware logic, enriched by LinkedIn signals
        all_text = (research.summary + " " + " ".join(research.signals) + " " + about).lower()
        pain_points = []
        matched_services = []
        evidence_base = "LinkedIn-enrichment" if research.linkedin_about else "Website analyse"

        # Hiring = growth pain
        if research.linkedin_hiring_count and research.linkedin_hiring_count > 0:
            pain_points.append(f"{research.linkedin_hiring_count} openstaande vacatures: groei vraagt meer handmatig werk zonder systematische opvolging")
            matched_services.append("Operating layer — schaalbare opvolging zonder extra FTE")
        if research.mutual_connections_count and research.mutual_connections_count > 0:
            pain_points.append(f"Warme ingang via {research.mutual_connections_count} gemeenschappelijke connectie(s), maar geen gestructureerd account-ontwikkelingstraject")
            matched_services.append("Operating layer — relatiebeheer en touchpoint-automatisering")
        if any(w in all_text for w in ["telefoon", "bellen", "klantenservice", "support", "helpdesk"]):
            pain_points.append("Veel inkomende telefoongesprekken onderbreken het kernteam")
            matched_services.append("Operating layer — telefonische intake")
        if any(w in all_text for w in ["email", "contactformulier", "offerte", "aanvraag", "reactie"]):
            pain_points.append("Handmatige emailafhandeling vertraagt de opvolging")
            matched_services.append("Operating layer — emailverwerking")
        if any(w in all_text for w in ["crm", "hubspot", "salesforce", "teamleader", "administratie", "klantgegevens"]):
            pain_points.append("Verspreide klantgegevens remmen de groei")
            matched_services.append("Operating layer — één bron van waarde")
        if any(w in all_text for w in ["planning", "schema", "afspraak", "inplannen", "rooster"]):
            pain_points.append("Handmatig plannen kost te veel tijd")
            matched_services.append("Operating layer — procesautomatisering")

        if not pain_points:
            pain_points = ["Herhalende administratie en klantopvolging slokken tijd op die beter in groei gestopt kan worden"]
            matched_services = ["Operating layer — procesanalyse en -automatisering"]

        data = {
            "pain_points": [{"problem": p, "evidence": evidence_base, "impact": "Tijdverlies en groeifrictie"} for p in pain_points[:3]],
            "matched_solutions": [{"solution": s, "pain_addressed": pain_points[0] if pain_points else "", "roi": "35-40 uur/maand", "implementation": "Procesoptimalisatie"} for s in matched_services[:3]],
            "strongest_hook": f"De meest efficiënte versie van {research.company} bestaat al — wij bouwen de operating layer die de herhalende werkzaamheden naar het systeem verplaatst",
            "personalization_angle": research.summary[:100]
        }
    
    # Extract simple lists for OpportunityResult
    pain_list = []
    for p in data.get("pain_points", []):
        if isinstance(p, dict):
            pain_list.append(p.get("problem", str(p)))
        else:
            pain_list.append(str(p))
    
    service_list = []
    for s in data.get("matched_solutions", []):
        if isinstance(s, dict):
            service_list.append(s.get("solution", str(s)))
        else:
            service_list.append(str(s))
    
    return OpportunityResult(
        pain_points=pain_list[:5],
        matched_services=service_list[:4],
        value_angles=[s.get("roi", "") for s in data.get("matched_solutions", []) if isinstance(s, dict)][:4],
        strongest_hook=data.get("strongest_hook", "")
    )

# ---------- MODE 4: Qualification (DATA-DRIVEN BANT) ----------

async def qualify(research: ResearchData, opportunity: OpportunityResult) -> QualificationResult:
    """Data-driven BANT qualification using all research signals."""
    
    pain_summary = "; ".join(opportunity.pain_points[:3]) if opportunity.pain_points else "geen specifieke pijnpunten"
    solution_summary = "; ".join(opportunity.matched_services[:3]) if opportunity.matched_services else "geen specifieke oplossingen"
    
    system_prompt = """You are a Senior Sales Qualification Analyst. Score this lead honestly using BANT framework.
    
SCORING RULES:
- Budget (0-30): Company size + growth signals + industry = budget potential
- Authority (0-25): Decision maker identified? Title seniority? Direct access?
- Need (0-30): How many pain points match our solutions? How specific is the evidence?
- Timing (0-25): Growth signals? Hiring? Expanding? Recent activity?

CRITICAL: Every lead must get a DIFFERENT score. No two leads should have identical scores.
"""
    
    user_prompt = f"""
QUALIFY THIS LEAD:

COMPANY: {research.company}
- Summary: {research.summary[:300]}
- Industry: {research.linkedin_industry or 'Unknown'}
- Size: {research.linkedin_size or 'Unknown'}
- Decision Makers: {', '.join([f"{e.get('name', '?')} ({e.get('title', '?')})" for e in research.employees[:3]]) if research.employees else 'Not identified'}
- Data Quality: {research.data_quality_score}/100
- Growth Signals: {', '.join(research.signals[:3]) if research.signals else 'None detected'}

IDENTIFIED PAIN POINTS:
{pain_summary}

MATCHED SOLUTIONS:
{solution_summary}

SCORING:
- budget (0-30): Large MKB (>50) with growth = 25-30; Medium MKB = 15-24; Small = 5-14; Unknown = 0-4
- authority (0-25): Director/Owner with name = 20-25; Manager = 12-19; Unknown role = 5-11; No contact = 0-4
- need (0-30): 3+ specific pain points matching our solutions = 25-30; 1-2 matches = 15-24; Weak = 5-14
- timing (0-25): Actively hiring/expanding = 20-25; Stable with activity = 12-19; Low activity = 5-11

Return JSON:
{{
  "budget": <0-30>,
  "authority": <0-25>,
  "need": <0-30>,
  "timing": <0-25>,
  "total_score": <sum>,
  "label": "HOT" (>=75) or "WARM" (>=50) or "COLD" (<50)
}}
"""
    
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="gemini-3.1-flash-lite", json_mode=True, max_tokens=512)
        data = extract_json_from_llm_response(res_text)
    except Exception as e:
        logger.warning(f"qualify LLM failed: {e}")
        data = None
    
    if not data:
        # Data-driven scoring based on available signals
        budget_score = 10
        if research.linkedin_size:
            size_str = str(research.linkedin_size).lower()
            if any(s in size_str for s in ["51-200", "201-500", "10000+", "1001-5000"]):
                budget_score = 22
            elif any(s in size_str for s in ["11-50", "201-500"]):
                budget_score = 16
            else:
                budget_score = 10
        
        authority_score = 8
        if research.employees:
            for emp in research.employees[:2]:
                title = (emp.get("title") or "").lower()
                if any(w in title for w in ["directeur", "ceo", "owner", "eigenaar", "oprichter"]):
                    authority_score = 22
                    break
                elif any(w in title for w in ["manager", "head", "lead"]):
                    authority_score = 16
                    break
            if authority_score == 8:
                authority_score = 12
        elif research.contact_emails and not any(e.startswith("info") for e in research.contact_emails):
            authority_score = 14
        elif research.linkedin_company_url:
            authority_score = 12  # LinkedIn company page found = established business
        
        need_score = 10
        if opportunity.pain_points:
            need_score = 15
            for p in opportunity.pain_points:
                if any(w in p.lower() for w in ["telefoon", "email", "crm", "planning", "administratie", "opvolg"]):
                    need_score += 3
        
        timing_score = 10
        if research.employees and len(research.employees) > 5:
            timing_score += 5
        if any("groei" in s.lower() or "hiring" in s.lower() or "vacature" in s.lower() for s in research.signals):
            timing_score += 5
        if research.data_quality_score >= 70:
            timing_score += 3
        
        budget_score = min(30, budget_score)
        authority_score = min(25, authority_score)
        need_score = min(30, need_score)
        timing_score = min(25, timing_score)
        
        total = budget_score + authority_score + need_score + timing_score
        label = "HOT" if total >= 75 else ("WARM" if total >= 50 else "COLD")
        
        data = {
            "budget": budget_score,
            "authority": authority_score,
            "need": need_score,
            "timing": timing_score,
            "total_score": total,
            "label": label
        }
    
    return QualificationResult(
        budget=data.get("budget", 12),
        authority=data.get("authority", 12),
        need=data.get("need", 12),
        timing=data.get("timing", 12),
        total_score=data.get("total_score", 48),
        label=data.get("label", "COLD")
    )

# ---------- MODE 5: Outreach (HYPER-PERSONALIZED) ----------

async def generate_email(lead: LeadInput, research: ResearchData, opportunity: OpportunityResult) -> EmailDraft:
    """Generate cold email aligned with ClawBuildr 4-sheet positioning."""
    
    target_language = "Dutch"
    
    # Load tenant config
    _sig_block = "Marvin van der Sluis"
    _calendar_link = ""
    try:
        import sqlite3 as _sq
        import os
        _db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clawbuildr.db")
        _tc = _sq.connect(_db, timeout=5.0).execute("SELECT signature_block, calendar_link FROM tenant_config WHERE active=1 LIMIT 1").fetchone()
        if _tc:
            if _tc[0]: _sig_block = _tc[0]
            if _tc[1]: _calendar_link = _tc[1]
    except Exception:
        pass
    
    # Build rich personalization context
    pain_evidence = ""
    for p in opportunity.pain_points[:2]:
        if isinstance(p, dict):
            pain_evidence += f"- {p.get('problem', '')}: {p.get('evidence', '')}\n"
        else:
            pain_evidence += f"- {p}\n"

    employee_context = ""
    if research.employees:
        for emp in research.employees[:3]:
            employee_context += f"  - {emp.get('name', '?')} ({emp.get('title', '?')})\n"

    # LinkedIn-derived personalization
    linkedin_hooks = []
    if research.profile_posts:
        linkedin_hooks.append(f"Recente persoonlijke post: {research.profile_posts[0][:200]}")
    if research.linkedin_company_posts:
        linkedin_hooks.append(f"Recente bedrijfspost: {research.linkedin_company_posts[0][:200]}")
    if research.linkedin_hiring_count and research.linkedin_hiring_count > 0:
        jobs_summary = ", ".join([j.get("title", "") for j in research.linkedin_hiring_jobs[:3]])
        linkedin_hooks.append(f"{research.linkedin_hiring_count} openstaande vacatures: {jobs_summary}")
    if research.mutual_connections_count and research.mutual_connections_count > 0:
        linkedin_hooks.append(f"{research.mutual_connections_count} gemeenschappelijke LinkedIn-connectie(s)")
    if research.tenure:
        linkedin_hooks.append(f"Huidige functie sinds: {research.tenure}")
    if research.headline:
        linkedin_hooks.append(f"LinkedIn headline: {research.headline}")
    if research.linkedin_specialties:
        linkedin_hooks.append(f"Bedrijfsspecialismen: {research.linkedin_specialties[:200]}")

    personalization_hooks = []
    for s in research.signals[:5]:
        if any(w in s.lower() for w in ["vacature", "hiring", "nieuw", "expansie", "groei", "klant", "project"]):
            personalization_hooks.append(s)
    hooks_str = "\n".join(personalization_hooks[:3]) if personalization_hooks else "Geen specifieke hooks gevonden"
    linkedin_hooks_str = "\n".join(linkedin_hooks[:4]) if linkedin_hooks else "Geen LinkedIn-hooks beschikbaar"
    
    system_prompt = f"""Je bent Marvin van der Sluis van ClawBuildr. Je schrijft cold outreach e-mails in het Nederlands aan eigenaren van Nederlandse MKB-bedrijven.

WAT WE ZIJN:
ClawBuildr is een implementation partner. We bouwen de operating layer die onder de afdelingen van een bedrijf zit. De herhalende werkzaamheden gaan naar het systeem: retypen, opzoeken, doorsturen, controleren, status najagen. Wat bij mensen blijft is het werk dat oordeel vereist: klanten, uitzonderingen, beslissingen, relaties.

WIJ ZIJN GEEN:
- AI-bureau (bouwen losse tools op een operatie die niet werkt)
- Software (geen product per zitplaats te licentiëren)
- Consultancy (een rapport dat één keer gelezen wordt verandert niets)

DE VASTE HOOFDREGEL:
"De meest efficiënte versie van uw bedrijf bestaat al. Hij zit in uw inbox, uw bestellingen en een half dozijn systemen die niet met elkaar praten. Wij brengen het op één plek en maken het de versie die elke dag draait, over het hele bedrijf."

HET TAGLINE: Machines herhalen. Mensen beslissen.

REGELS:
1. Max 120 woorden
2. Gebruik 'je' of 'jullie' (informeel-professioneel)
3. NOOIT placeholders zoals [Naam] of [Bedrijf]
4. NOOIT interne jargon zoals "BANT", "budget past", "kwalificatie"
5. NOOIT robot-zinnen zoals "Gedetecteerd via" of "Scrape data toont"
6. NOOIT de woorden: vervangen, automatiseren, minder mensen nodig, personeel snijden, hoofdtn, FTE-reductie, overbodig, scorebord, monitoren, bijhouden wat mensen doen
7. NOOIT AI, automatisering of het woord "systeem" als openingszin
8. Open met een SPECIFIEK feit uit de LinkedIn-persoonlijking — geen generieke observatie
9. Koppel dat feit aan de constraint van de eigenaar: meer volume = meer handwerk = iemand aannemen
10. Koppel de constraint aan onze oplossing: de herhalende werkt naar het systeem, mensen krijgen ruimte voor oordeel
11. Laagdrempelige CTA: "Zullen we 10 minuten bellen?"
12. Teken af als: Met vriendelijke groet, Marvin van der Sluis / ClawBuildr
13. VERPLICHT: noem minstens 1 concreet feit uit de LinkedIn-persoonlijking in de eerste 2 zinnen
"""
    
    user_prompt = f"""
SCHRIJF EEN EMAIL AAN:
- Naam: {research.contact_name or lead.contact_name or "decision-maker"}
- Bedrijf: {research.company}
- Functie: {research.employees[0].get('title', 'Decision Maker') if research.employees else (research.headline or 'Eigenaar')}
- LinkedIn headline: {research.headline or 'Onbekend'}
- Locatie: {research.location or 'Onbekend'}

BEDRIJFSINZICHTEN:
- Wat doen ze: {research.summary}
- Branche: {research.linkedin_industry or 'Onbekend'}
- Grootte: {research.linkedin_size or 'Onbekend'}
- Hoofdkantoor: {research.linkedin_headquarters or 'Onbekend'}
- Specialismen: {research.linkedin_specialties or 'Onbekend'}
- Website: {research.linkedin_website or 'Onbekend'}

LINKEDIN-PERSOONLIJKING (gebruik minstens 1 hiervan in de opening):
{linkedin_hooks_str}

PIJNPUNTEN (met bewijs):
{pain_evidence if pain_evidence else "  - Geen specifieke pijnpunten gedetecteerd"}

PERSOONLIJKE HOOKS (voor email opening):
{hooks_str}

DE VIJF VRAGEN DIE HET VERSCHIL MAKEN (gebruik er max 1-2 in de email):
1. Hoeveel bestellingen, offertes, facturen of klantberichten komen er per week binnen?
2. Hoeveel daarvan komt als e-mail, PDF of portaal inlog in plaats van direct in een systeem?
3. Hoeveel mensen raken een enkel item voordat het af is?
4. Welke klinkt het meest als jouw week: status najagen, tussen systemen overtellen, of cijfers die niet kloppen?
5. Als je volume volgend kwartaal verdubbelt, moet je dan iemand aannemen?

VRAAG 5 is de belangrijkste. Het verplaatst het gesprek van kosten besparen (wat mensen moet afsnijden) naar meer aan kunnen nemen zonder te huren (wat de eigenaar echt wil).

DE WISKUNDE: Elke euro structurele kostenbesparing is ~5 euro bedrijfswaarde (Nederlandse MKB-veelvoud ~5.0x). Een bedrijf dat 120.000 euro aan handmatig werk naar het systeem verplaatst, creëert ~600.000 euro aan waarde.

TAKE: Schrijf een email die:
1. OPENT met de CONSTRAINT van de eigenaar (wat kost hem elke week de meeste tijd)
2. BESCHRIJFT wat er gebeurt als volume groeit zonder systeem (hij moet iemand aannemen)
3. KOPPELT onze oplossing: de herhalende werkt naar het systeem, mensen krijgen ruimte voor oordeel
4. NOEMT de waardecreatie (elke euro = vijf euro bedrijfswaarde)
5. EINDIGT met een laagdrempelige CTA

BELANGRIJK: Gebruik de FEITEN uit de pijnpunten hierboven. Schrijf NIET generiek.

Return JSON:
{{
  "subject": "Specifieke onderwerpregel (niet generiek)",
  "body": "Volledige email body",
  "personalization_points_used": ["Feit 1 dat is gebruikt", "Feit 2"]
}}
"""
    
    try:
        res_text = await call_llm(user_prompt, system_prompt, model="gemini-3.1-flash-lite", json_mode=True, max_tokens=1500)
        data = extract_json_from_llm_response(res_text)
    except Exception:
        data = None
    
    if not data:
        name_to_use = research.contact_name or lead.contact_name or 'relatie'
        # Build fallback email using LinkedIn enrichment when LLM fails
        hook = ""
        personalization_points = []
        if research.profile_posts:
            hook = f"Ik zag je recente post over {research.profile_posts[0].split('|')[0].strip()[:80]}. "
            personalization_points.append("recente LinkedIn-post")
        elif research.linkedin_company_posts:
            hook = f"Ik volg {research.company} op LinkedIn en zag jullie recente post over {research.linkedin_company_posts[0].split('|')[0].strip()[:80]}. "
            personalization_points.append("recente bedrijfspost")
        elif research.linkedin_hiring_count and research.linkedin_hiring_count > 0:
            hook = f"{research.company} groeit zichtbaar — ik zag {research.linkedin_hiring_count} openstaande vacatures op LinkedIn. "
            personalization_points.append("hiring signal")
        elif research.mutual_connections_count and research.mutual_connections_count > 0:
            hook = f"We hebben {research.mutual_connections_count} gemeenschappelijke connectie op LinkedIn. "
            personalization_points.append("mutual connection")
        elif research.headline:
            hook = f"Je profiel beschrijft je als {research.headline}. "
            personalization_points.append("LinkedIn headline")

        body = f"Beste {name_to_use},\n\n{hook}Bij een bedrijf als {research.company} merk ik vaak dat de herhalende werkzaamheden — mailtjes, status najagen, tussen systemen overtellen — de meeste tijd opslokken.\n\nDat is geen capaciteitsprobleem. Het is een probleem van een operating layer die niet met elkaar praat.\n\nWij bouwen die laag: het repetitieve werk gaat naar het systeem, jullie team houdt ruimte voor oordeel en klantcontact.\n\nElke euro die je daar structureel uit haalt, is ongeveer vijf euro bedrijfswaarde.\n\nZullen we 10 minuten bellen om te kijken of dit past?\n\nMet vriendelijke groet,\n\nMarvin van der Sluis\nClawBuildr\nhttps://clawbuildr.com/"
        data = {
            "subject": f"Tijd die elke week terugkomt bij {research.company}",
            "body": body,
            "personalization_points_used": personalization_points
        }
    
    body = data.get("body", "")
    body = re.sub(r'\[[^\]]+\]', '', body)
    
    # Banned word filter — Sheet 4
    _banned = ['vervangen', 'automatiseren', 'minder mensen nodig', 'personeel snijden',
               'hoofdtn', 'fte-reductie', 'overbodig', 'scorebord', 'monitoren',
               'bijhouden wat mensen doen', 'kosten besparen', 'efficiëntie']
    _body_lower = body.lower()
    for bw in _banned:
        if bw in _body_lower:
            # Log warning but don't block — the prompt should prevent this
            print(f"[BANNED WORD] '{bw}' found in email body — review needed")
    
    # Apply encoding fix
    try:
        from tools import fix_text_encoding
        body = fix_text_encoding(body)
        data["subject"] = fix_text_encoding(data.get("subject", ""))
    except Exception:
        pass
    
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
    # Reuse generate_email with the same research data
    return await generate_email(lead, research, opportunity)

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

    # Step 2: Research Mode (DEEP MULTI-SOURCE)
    research = await research_company(lead)

    # Step 3: Trust / Deliverability Mode (Safety Gate check)
    trust = assess_trust(research)
    if trust.decision == "BLOCK":
        return PipelineResult(
            lead=lead,
            research=research,
            trust=trust,
            opportunity=OpportunityResult(pain_points=[], matched_services=[], value_angles=[], strongest_hook=""),
            qualification=QualificationResult(budget=0, authority=0, need=0, timing=0, total_score=0, label="BLOCKED"),
            email=EmailDraft(subject="", body="", personalization_points_used=[]),
            decision="BLOCK"
        )

    # Step 4: Opportunity Mapping (DEEP PAIN ANALYSIS)
    opportunity = await map_opportunity(research)

    # Step 5: Qualification (DATA-DRIVEN BANT)
    qualification = await qualify(research, opportunity)

    # Step 6: Outreach (HYPER-PERSONALIZED EMAIL)
    email = await generate_email(lead, research, opportunity)

    # Step 7: Memory / Learning
    update_learning("DRAFT", trust.trust_score, qualification.total_score)

    # Format CRM record
    crm = format_crm(lead, trust, qualification, False, "DRAFT")

    return PipelineResult(
        lead=lead,
        research=research,
        trust=trust,
        opportunity=opportunity,
        qualification=qualification,
        email=email,
        decision="DRAFT"
    )
