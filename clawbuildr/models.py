from pydantic import BaseModel
from typing import Optional, List

class LeadInput(BaseModel):
    company: str
    domain: Optional[str] = None
    contact_email: Optional[str] = None
    contact_name: Optional[str] = None

class ResearchData(BaseModel):
    company: str
    domain: str
    summary: str
    services: List[str]
    signals: List[str]
    sources: List[str]
    data_quality_score: int
    contact_emails: List[str] = []
    contact_name: Optional[str] = None

class TrustResult(BaseModel):
    trust_score: int
    decision: str
    risk_flags: List[str]
    reasoning: str

class OpportunityResult(BaseModel):
    pain_points: List[str]
    matched_services: List[str]
    value_angles: List[str]
    strongest_hook: str

class QualificationResult(BaseModel):
    budget: int
    authority: int
    need: int
    timing: int
    total_score: int
    label: str

class EmailDraft(BaseModel):
    subject: str
    body: str
    personalization_points_used: List[str]

class LearningUpdate(BaseModel):
    insight: str
    trigger_pattern: str
    recommended_adjustment: str

class CrmRecord(BaseModel):
    company: str
    domain: str
    trust_score: int
    qualification_score: int
    decision: str
    summary: str
    email_sent: bool
    reasoning_short: str

class PipelineResult(BaseModel):
    decision: str
    trust_score: int
    qualification_score: int
    company: str
    email: Optional[EmailDraft] = None
    crm_record: Optional[CrmRecord] = None
    reasoning_summary: str
    learning_update: Optional[LearningUpdate] = None
