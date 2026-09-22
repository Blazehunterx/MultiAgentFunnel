#!/usr/bin/env python3
"""
ClawBuildr API — Central API layer with 42+ endpoints.
Integrates all modules: auth, workspace, onboarding, email, LinkedIn, learning, sequences, etc.
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Request, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger("ClawBuildr.API")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")
CLAWBUILDR_DIR = os.path.dirname(__file__)

if CLAWBUILDR_DIR not in sys.path:
    sys.path.insert(0, CLAWBUILDR_DIR)
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

app = FastAPI(title="ClawBuildr API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup_event():
    """Ensure all tables exist when API starts."""
    from clawbuildr_workspace import _ensure_workspace_tables
    from clawbuildr_auth import _ensure_auth_tables
    from clawbuildr_onboarding import _ensure_onboarding_tables
    from clawbuildr_scheduler import _ensure_followup_table
    from clawbuildr_learning import _ensure_learning_tables
    _ensure_workspace_tables()
    _ensure_auth_tables()
    _ensure_onboarding_tables()
    _ensure_followup_table()
    _ensure_learning_tables()

# ─── Auth helpers ────────────────────────────────────────────────────────────

def _get_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Auth models ─────────────────────────────────────────────────────────────

class SignupRequest(BaseModel):
    email: str
    password: str
    name: str = ""

class LoginRequest(BaseModel):
    email: str
    password: str

class WorkspaceRequest(BaseModel):
    name: str
    slug: str
    plan: str = "free"

class ICPRequest(BaseModel):
    name: str
    description: str = ""
    industries: List[str] = []
    company_sizes: List[str] = []
    roles: List[str] = []
    regions: List[str] = []
    keywords: List[str] = []

class EmailGenerateRequest(BaseModel):
    contact_id: int
    company_name: str
    contact_name: str = ""
    role: str = ""
    language: str = "nl"
    custom_prompt: str = ""

class SequenceEnrollRequest(BaseModel):
    sequence_id: int
    contact_id: int

# ═══════════════════════════════════════════════════════════════════════════════
# 1. AUTH ENDPOINTS (4)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/auth/signup")
def auth_signup(req: SignupRequest):
    from clawbuildr_auth import create_user
    result = create_user(req.email, req.password, req.name)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result

@app.post("/auth/login")
def auth_login(req: LoginRequest):
    from clawbuildr_auth import authenticate_user
    result = authenticate_user(req.email, req.password)
    if not result:
        raise HTTPException(401, "Invalid credentials")
    return result

@app.get("/auth/me")
def auth_me(request: Request):
    from clawbuildr_auth import get_user_from_request
    user = get_user_from_request(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    return user

@app.post("/auth/api-key")
def create_api_key(request: Request, name: str = ""):
    from clawbuildr_auth import get_user_from_request, create_api_key as _create
    user = get_user_from_request(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    key = _create(user["user_id"], user.get("workspace_id", 1), name)
    return {"api_key": key}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. WORKSPACE ENDPOINTS (5)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/workspaces")
def list_workspaces():
    from clawbuildr_workspace import list_workspaces
    return list_workspaces()

@app.post("/workspaces")
def create_workspace(req: WorkspaceRequest):
    from clawbuildr_workspace import create_workspace
    return create_workspace(req.name, req.slug, req.plan)

@app.get("/workspaces/{workspace_id}")
def get_workspace(workspace_id: int):
    from clawbuildr_workspace import get_workspace
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(404, "Workspace not found")
    return ws

@app.put("/workspaces/{workspace_id}/settings")
def update_workspace_settings(workspace_id: int, settings: Dict):
    from clawbuildr_workspace import update_workspace_settings
    update_workspace_settings(workspace_id, settings)
    return {"ok": True}

@app.get("/workspaces/{workspace_id}/stats")
def get_workspace_stats(workspace_id: int):
    from clawbuildr_workspace import get_workspace_stats
    return get_workspace_stats(workspace_id)

# ═══════════════════════════════════════════════════════════════════════════════
# 3. ONBOARDING ENDPOINTS (5)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/onboarding/start")
def start_onboarding(workspace_id: int):
    from clawbuildr_onboarding import start_onboarding
    return start_onboarding(workspace_id)

@app.get("/onboarding/status")
def get_onboarding_status(workspace_id: int):
    from clawbuildr_onboarding import get_onboarding_status
    return get_onboarding_status(workspace_id)

@app.post("/onboarding/complete-step")
def complete_onboarding_step(workspace_id: int, step: str, data: Dict = {}):
    from clawbuildr_onboarding import complete_step
    complete_step(workspace_id, step, data)
    return {"ok": True}

@app.post("/onboarding/connect-email")
def connect_email(workspace_id: int, email: str, provider: str = "gmail"):
    from clawbuildr_onboarding import connect_email_account
    return connect_email_account(workspace_id, email, provider)

@app.post("/onboarding/verify-domain")
def verify_domain(workspace_id: int, domain: str):
    from clawbuildr_onboarding import verify_domain
    return verify_domain(workspace_id, domain)

# ═══════════════════════════════════════════════════════════════════════════════
# 4. CONTACT/LEAD ENDPOINTS (6)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/contacts")
def list_contacts(stage: str = None, limit: int = 50, offset: int = 0):
    from clawbuildr_client_dashboard import get_client_contacts
    return get_client_contacts(stage=stage, limit=limit, offset=offset)

@app.get("/contacts/{contact_id}")
def get_contact(contact_id: int):
    db = _get_db()
    try:
        row = db.execute("SELECT * FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Contact not found")
        return dict(row)
    finally:
        db.close()

@app.get("/contacts/top")
def get_top_leads(limit: int = 10):
    from clawbuildr_lead_scoring import get_top_leads
    return get_top_leads(limit)

@app.post("/contacts/{contact_id}/score")
def score_contact(contact_id: int):
    from clawbuildr_lead_scoring import score_contact
    return score_contact(contact_id)

@app.get("/contacts/{contact_id}/activity")
def get_contact_activity(contact_id: int):
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT * FROM activity_log WHERE contact_id = ? ORDER BY created_at DESC LIMIT 20",
            (contact_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/contacts/{contact_id}/emails")
def get_contact_emails(contact_id: int):
    db = _get_db()
    try:
        rows = db.execute(
            "SELECT * FROM emails WHERE contact_id = ? ORDER BY created_at DESC",
            (contact_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class LeadGenRequest(BaseModel):
    queries: List[str] = []
    max_leads: int = 20
    use_hunter: bool = True
    use_web_search: bool = True
    use_website_scrape: bool = True

@app.post("/leads/generate")
def generate_leads_endpoint(req: LeadGenRequest):
    from clawbuildr_lead_generator import generate_leads
    import asyncio
    result = asyncio.run(generate_leads(
        queries=req.queries or None,
        max_leads=req.max_leads,
        use_hunter=req.use_hunter,
        use_web_search=req.use_web_search,
        use_website_scrape=req.use_website_scrape,
    ))
    return result

@app.post("/leads/generate/sync")
def generate_leads_sync(queries: str = "", max_leads: int = 20):
    from clawbuildr_lead_generator import generate_leads
    import asyncio
    query_list = [q.strip() for q in queries.split(",") if q.strip()] if queries else None
    result = asyncio.run(generate_leads(queries=query_list, max_leads=max_leads))
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# 5. EMAIL ENDPOINTS (6)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/email/generate")
def generate_email(req: EmailGenerateRequest):
    from clawbuildr_ai_email import generate_ai_email
    result = generate_ai_email(
        contact_id=req.contact_id,
        company_name=req.company_name,
        contact_name=req.contact_name,
        role=req.role,
        language=req.language,
        custom_prompt=req.custom_prompt,
    )
    return result

@app.post("/email/generate-followup")
def generate_followup(contact_id: int, company_name: str, step: int = 1):
    from clawbuildr_ai_email import generate_followup_email
    from clawbuildr_scheduler import get_pending_followups
    pending = get_pending_followups()
    previous = [{"direction": "outbound", "body": ""}]
    return generate_followup_email(contact_id, company_name, previous, step)

@app.post("/email/send")
def send_email(contact_id: int, subject: str, body: str):
    from clawbuildr_ai_email import save_generated_email
    from clawbuildr_watchdog import can_send
    if not can_send():
        raise HTTPException(429, "Send limit reached or bounce rate too high")

    db = _get_db()
    try:
        contact = db.execute("SELECT email FROM contacts WHERE contact_id = ?", (contact_id,)).fetchone()
        if not contact or not contact["email"]:
            raise HTTPException(404, "Contact not found or no email")

        from tools import gmail_send
        success = gmail_send(contact["email"], subject, body)

        email_id = save_generated_email(contact_id, subject, body, "outbound")

        if success:
            db.execute(
                "UPDATE contacts SET current_stage = 'ACTIVE_OUTREACH', updated_at = ? WHERE contact_id = ?",
                (datetime.now(timezone.utc).isoformat(), contact_id),
            )
            db.commit()

        return {"success": success, "email_id": email_id}
    finally:
        db.close()

@app.get("/email/check-replies")
def check_replies(background_tasks: BackgroundTasks):
    from clawbuildr_reply_detector import check_replies
    replies = check_replies(since_hours=24)
    return {"processed": len(replies), "replies": replies}

@app.get("/email/stats")
def email_stats():
    from clawbuildr_reply_detector import get_reply_stats
    return get_reply_stats()

@app.get("/email/deliverability/{domain}")
def check_deliverability(domain: str):
    from clawbuildr_deliverability import full_deliverability_check
    return full_deliverability_check(domain)

# ═══════════════════════════════════════════════════════════════════════════════
# 6. LINKEDIN ENDPOINTS (5)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/linkedin/stats")
def linkedin_stats():
    from clawbuildr_linkedin_integration import get_linkedin_stats
    return get_linkedin_stats()

@app.get("/linkedin/campaigns")
def linkedin_campaigns():
    from clawbuildr_linkedin_integration import get_linkedin_campaigns
    return get_linkedin_campaigns()

@app.get("/linkedin/warming")
def linkedin_warming():
    from clawbuildr_linkedin_integration import get_warming_status
    return get_warming_status()

@app.get("/linkedin/learning")
def linkedin_learning():
    from clawbuildr_linkedin_integration import get_linkedin_learning
    return get_linkedin_learning()

@app.get("/linkedin/history")
def linkedin_history():
    from linkedin_engine import get_clawbuildr_linkedin_history
    return get_clawbuildr_linkedin_history()

# ═══════════════════════════════════════════════════════════════════════════════
# 7. FOLLOW-UP ENDPOINTS (4)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/followup/enroll")
def enroll_followup(contact_id: int):
    from clawbuildr_scheduler import enroll_contact
    return {"enrolled": enroll_contact(contact_id)}

@app.get("/followup/pending")
def get_pending_followups():
    from clawbuildr_scheduler import get_pending_followups
    return get_pending_followups()

@app.get("/followup/stats")
def followup_stats():
    from clawbuildr_scheduler import get_followup_stats
    return get_followup_stats()

@app.post("/followup/process")
def process_followups():
    from clawbuildr_scheduler import process_due_followups
    due = process_due_followups()
    return {"processed": len(due), "followups": due}

# ═══════════════════════════════════════════════════════════════════════════════
# 8. SEQUENCE ENDPOINTS (5)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/sequences")
def list_sequences():
    from clawbuildr_sequences import get_sequences
    return get_sequences()

@app.post("/sequences")
def create_sequence(name: str, description: str = "", steps: List[Dict] = []):
    from clawbuildr_sequences import create_sequence as _create
    return {"sequence_id": _create(name, description, steps)}

@app.post("/sequences/enroll")
def enroll_in_sequence(req: SequenceEnrollRequest):
    from clawbuildr_sequences import enroll_in_sequence
    return {"enrolled": enroll_in_sequence(req.sequence_id, req.contact_id)}

@app.get("/sequences/{sequence_id}/stats")
def sequence_stats(sequence_id: int):
    from clawbuildr_sequences import get_sequence_stats
    return get_sequence_stats(sequence_id)

@app.get("/sequences/enrollments")
def get_active_enrollments(sequence_id: int = None):
    from clawbuildr_sequences import get_active_enrollments
    return get_active_enrollments(sequence_id)

# ═══════════════════════════════════════════════════════════════════════════════
# 9. LEARNING ENDPOINTS (4)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/learning/summary")
def learning_summary():
    from clawbuildr_learning import get_performance_summary
    return get_performance_summary()

@app.get("/learning/timing")
def best_timing():
    from clawbuildr_learning import get_best_send_times
    return get_best_send_times()

@app.get("/learning/insights")
def get_insights(category: str = None, limit: int = 20):
    from clawbuildr_learning import get_learnings
    return get_learnings(category, limit)

@app.post("/learning/insight")
def add_insight(category: str, insight: str, evidence: str = "", confidence: float = 0.5):
    from clawbuildr_learning import add_learning
    add_learning(category, insight, evidence, confidence)
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════════════════════
# 10. ICP ENDPOINTS (2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/icp")
def list_icps(workspace_id: int = 1):
    from clawbuildr_onboarding import get_icps
    return get_icps(workspace_id)

@app.post("/icp")
def create_icp(req: ICPRequest, workspace_id: int = 1):
    from clawbuildr_onboarding import create_icp
    return create_icp(workspace_id, req.name, req.description,
                      req.industries, req.company_sizes, req.roles,
                      req.regions, req.keywords)

# ═══════════════════════════════════════════════════════════════════════════════
# 11. PIPELINE & INTEGRATION ENDPOINTS (5)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/pipeline/overview")
def pipeline_overview():
    from clawbuildr_integration import get_pipeline_overview
    return get_pipeline_overview()

@app.get("/pipeline/activity")
def pipeline_activity(limit: int = 50):
    from clawbuildr_integration import get_activity_feed
    return get_activity_feed(limit)

@app.post("/pipeline/enrich/{contact_id}")
def enrich_contact(contact_id: int):
    from clawbuildr_integration import run_enrichment_pipeline
    return run_enrichment_pipeline(contact_id)

@app.post("/pipeline/process-replies")
def process_replies():
    from clawbuildr_integration import run_reply_processing
    return run_reply_processing()

@app.get("/pipeline/health")
def pipeline_health():
    from clawbuildr_watchdog import get_health_report
    return get_health_report()

# ═══════════════════════════════════════════════════════════════════════════════
# 12. HUNTER.IO ENDPOINTS (2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/hunter/domain-search")
def hunter_domain_search(domain: str, limit: int = 10):
    from clawbuildr_hunter import domain_search
    return domain_search(domain, limit)

@app.get("/hunter/verify-email")
def hunter_verify_email(email: str):
    from clawbuildr_hunter import email_verify
    return email_verify(email)

# ═══════════════════════════════════════════════════════════════════════════════
# 13. SENTIMENT ENDPOINTS (2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/sentiment/analyze")
def analyze_sentiment(text: str):
    from clawbuildr_sentiment import analyze_sentiment
    return analyze_sentiment(text)

@app.post("/sentiment/meeting-info")
def extract_meeting_info(text: str):
    from clawbuildr_sentiment import extract_meeting_info
    return extract_meeting_info(text)


# ─── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
