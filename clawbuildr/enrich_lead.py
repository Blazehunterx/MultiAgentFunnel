"""
Combined LinkedIn lead enrichment: profile + company in one call.
Useful for the outbound pipeline to enrich a lead before drafting outreach.
"""
import os
import sys
import json
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLAWBUILDR_DIR = os.path.join(BASE_DIR, "clawbuildr")
if CLAWBUILDR_DIR not in sys.path:
    sys.path.insert(0, CLAWBUILDR_DIR)

from read_linkedin_profile import read_linkedin_profile
from read_linkedin_company import read_linkedin_company


def enrich_lead_from_linkedin(profile_url: str, company_name: str = "", company_url: str = ""):
    """Enrich a lead by reading LinkedIn profile and company page.

    Args:
        profile_url: LinkedIn profile URL
        company_name: Company name (used to guess company URL if not provided)
        company_url: LinkedIn company URL if known

    Returns:
        dict with profile, company, and combined signals
    """
    result = {
        "profile_url": profile_url,
        "company_url": company_url,
        "profile": {},
        "company": {},
        "icp_signals": {},
        "personalization": {},
        "errors": []
    }

    # Read profile
    profile = read_linkedin_profile(profile_url)
    result["profile"] = profile
    if profile.get("error"):
        result["errors"].append(f"Profile error: {profile['error']}")

    # Determine company URL
    if not company_url and (company_name or profile.get("current_company")):
        name = (company_name or profile.get("current_company", "")).strip()
        if name:
            slug = re.sub(r'[^a-zA-Z0-9]', '-', name.lower()).strip('-')
            company_url = f"https://www.linkedin.com/company/{slug}/"
            result["company_url"] = company_url

    # Read company
    if company_url:
        company = read_linkedin_company(company_url)
        result["company"] = company
        if company.get("error"):
            result["errors"].append(f"Company error: {company['error']}")
    else:
        result["errors"].append("No company URL available")

    # Build ICP signals
    icp = {}
    company = result.get("company", {})
    profile = result.get("profile", {})

    # Size category
    size_text = company.get("size", "")
    icp["company_size"] = size_text
    icp["company_size_category"] = _categorize_size(size_text)

    # Industry
    icp["industry"] = company.get("industry", "")

    # Hiring intensity
    hiring_count = company.get("hiring_count", 0)
    icp["hiring_count"] = hiring_count
    icp["hiring_signal"] = "high" if hiring_count >= 10 else "medium" if hiring_count >= 3 else "low" if hiring_count > 0 else "none"

    # Role seniority
    role = profile.get("current_role", "")
    icp["role_seniority"] = _categorize_seniority(role)

    # Tenure
    icp["tenure"] = profile.get("tenure", "")

    # Mutual connections
    icp["mutual_connections_count"] = profile.get("mutual_connections_count", 0)

    result["icp_signals"] = icp

    # Build personalization snippets
    personalization = {}
    posts = profile.get("posts", [])
    if posts:
        personalization["recent_post"] = posts[0][:300]
    company_posts = company.get("posts", [])
    if company_posts:
        personalization["company_recent_post"] = company_posts[0][:300]
    if profile.get("current_company") and company.get("name"):
        personalization["company_context"] = f"{profile['current_company']} — {company.get('industry', '')}, {company.get('size', '')}"
    if profile.get("mutual_connections_count", 0) > 0:
        personalization["warmth"] = f"{profile['mutual_connections_count']} mutual connection(s) on LinkedIn"

    result["personalization"] = personalization

    return result


def _categorize_size(size_text: str) -> str:
    if not size_text:
        return "unknown"
    size_lower = size_text.lower()
    if any(x in size_lower for x in ["10.001+", "10000+", "meer dan 10.000", "+ medewerkers"]):
        return "enterprise"
    if any(x in size_lower for x in ["1001", "5001", "1000-", "5000"]):
        return "large"
    if any(x in size_lower for x in ["201", "501", "1000", "200-", "500-"]):
        return "midmarket"
    if any(x in size_lower for x in ["11", "50", "51", "10-", "11-"]):
        return "sme"
    if any(x in size_lower for x in ["1-10", "1-9", "2-10"]):
        return "startup"
    return "unknown"


def _categorize_seniority(role: str) -> str:
    if not role:
        return "unknown"
    role_lower = role.lower()
    if any(x in role_lower for x in ["ceo", "founder", "oprichter", "eigenaar", "owner", "directeur", "director", "cfo", "cto", "coo"]):
        return "c_suite_founder"
    if any(x in role_lower for x in ["manager", "teamlead", "team lead", "lead", "hoofd", "head"]):
        return "manager"
    if any(x in role_lower for x in ["consultant", "adviseur", "specialist", "engineer", "developer", "analist", "coordinator"]):
        return "specialist"
    return "other"


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    profile_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.linkedin.com/in/nando-odems-698202177/"
    company_url = sys.argv[2] if len(sys.argv) > 2 else "https://www.linkedin.com/company/randstad/"
    data = enrich_lead_from_linkedin(profile_url, company_url=company_url)
    print(json.dumps(data, indent=2, ensure_ascii=False))
