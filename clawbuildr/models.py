from pydantic import BaseModel
from typing import Optional, List

class LeadInput(BaseModel):
    company: str
    domain: Optional[str] = None
    contact_email: Optional[str] = None
    contact_name: Optional[str] = None
    linkedin_url: Optional[str] = None

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
    # LinkedIn company data
    linkedin_company_url: Optional[str] = None
    linkedin_industry: Optional[str] = None
    linkedin_size: Optional[str] = None
    linkedin_about: Optional[str] = None
    linkedin_specialties: Optional[str] = None
    linkedin_headquarters: Optional[str] = None
    linkedin_founded: Optional[str] = None
    linkedin_employee_count: Optional[str] = None
    linkedin_website: Optional[str] = None
    linkedin_followers: Optional[str] = None
    linkedin_hiring_count: int = 0
    linkedin_hiring_jobs: List[dict] = []
    linkedin_company_posts: List[str] = []
    # Social links & phones
    social_links: dict = {}
    phones: List[str] = []
    # Employee data
    employees: List[dict] = []
    # Deep profile data (if single lead)
    headline: Optional[str] = None
    location: Optional[str] = None
    experience: Optional[str] = None
    education: Optional[str] = None
    skills: Optional[str] = None
    tenure: Optional[str] = None
    profile_posts: List[str] = []
    mutual_connections_count: int = 0
    mutual_connections_text: Optional[str] = None
    # KVK data
    kvk_data: dict = {}

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
