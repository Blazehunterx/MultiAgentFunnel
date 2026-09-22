#!/usr/bin/env python3
"""
ClawBuildr Email Quality Gate — Reviews drafts before they enter the approval queue.
Checks: personalization, grammar, tone, spam triggers, Dutch language quality.
"""

import os
import re
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("ClawBuildr.QualityGate")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY RULES
# ═══════════════════════════════════════════════════════════════════════════════

# Words that indicate generic/template emails (Dutch + English)
GENERIC_MARKERS = [
    # Dutch
    "geachte heer", "geachte mevrouw", "geachte meneer", "beste relatie",
    "hoogachtend",
    # English
    "dear sir", "dear madam", "to whom it may concern", "dear hiring manager",
    "sincerely", "best regards",
    # Generic business
    "wij helpen bedrijven", "we help companies", "onzee diensten", "our services",
    "klik hier", "click here", "lees meer", "read more",
]

# Spam trigger words (Dutch + English)
SPAM_TRIGGERS = [
    # Urgency
    "gratis", "free", "beperkte tijd", "limited time",
    "vandaag", "today", "onnenedelbaar", "urgent", "onmiddellijk", "immediately",
    # Money
    "bespaar", "save", "korting", "discount", "aanbieding", "offer",
    "goedkoop", "cheap", "laagste prijs", "lowest price",
    #承诺
    "garanderen", "guarantee", "100%", "altijd", "always",
    # Pressure
    "laatste kans", "last chance", "niet missen", "don't miss",
    "exclusief", "exclusive", "slechts", "only",
]

# Required personalization elements
PERSONALIZATION_REQUIRED = [
    "company_name",  # Must mention the company
    "first_name",    # Must use the contact's name
]

# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY CHECKS
# ═══════════════════════════════════════════════════════════════════════════════

def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def _check_personalization(subject: str, body: str, contact: Dict) -> Tuple[float, List[str]]:
    """Check if email is personalized to the specific contact/company."""
    score = 100.0
    issues = []
    combined = (subject + " " + body).lower()

    # CRITICAL: Check for company IDs leaked into email (e.g., co_29d033e6)
    id_patterns = re.findall(r'co_[a-f0-9]+|prsp_[a-f0-9]+|ct_[a-f0-9]+', combined)
    if id_patterns:
        score -= 60
        issues.append(f"Company ID leaked into email: {id_patterns[0]}")

    # CRITICAL: Check for domain names in subject line
    domain = (contact.get("domain") or "").lower()
    if domain and len(domain) > 4:
        # Domain appearing in subject is almost always wrong
        if domain in subject.lower():
            score -= 40
            issues.append(f"Domain '{domain}' found in subject line — use company name only")
        # Domain in body is OK only if it's a natural reference, but repeated is bad
        body_domain_count = combined.count(domain)
        if body_domain_count > 1:
            score -= 20
            issues.append(f"Domain '{domain}' appears {body_domain_count}x in email — reduce")

    # Check company name mentioned
    company = (contact.get("company_name") or "").lower()
    if company and len(company) > 2:
        if company not in combined:
            score -= 30
            issues.append(f"Company name '{contact.get('company_name')}' not mentioned")

    # Check contact name used
    first_name = (contact.get("first_name") or "").lower()
    if first_name and len(first_name) > 1:
        if first_name not in combined:
            score -= 25
            issues.append(f"Contact name '{contact.get('first_name')}' not used in greeting")

    # Check for generic markers
    for marker in GENERIC_MARKERS:
        if marker in combined:
            score -= 15
            issues.append(f"Generic marker detected: '{marker}'")

    # Check if email is too short (likely not personalized enough)
    word_count = len(body.split())
    if word_count < 30:
        score -= 20
        issues.append(f"Email too short ({word_count} words) — likely not personalized")
    elif word_count > 200:
        score -= 10
        issues.append(f"Email too long ({word_count} words) — reduce length")

    return max(0, score), issues


def _check_spam_triggers(subject: str, body: str) -> Tuple[float, List[str]]:
    """Check for spam trigger words."""
    score = 100.0
    issues = []
    combined = (subject + " " + body).lower()

    for trigger in SPAM_TRIGGERS:
        if trigger in combined:
            score -= 10
            issues.append(f"Spam trigger: '{trigger}'")

    # Check for ALL CAPS words (excluding abbreviations)
    caps_words = re.findall(r'\b[A-Z]{3,}\b', subject + " " + body)
    caps_words = [w for w in caps_words if w not in ("CEO", "CTO", "CFO", "B2B", "SaaS", "IT", "NL", "BE", "DE")]
    if caps_words:
        score -= 5 * len(caps_words)
        issues.append(f"Excessive caps: {', '.join(caps_words[:3])}")

    # Check for multiple exclamation marks
    if "!!" in combined or "!!!" in combined:
        score -= 10
        issues.append("Multiple exclamation marks")

    # Check for emoji (except in subject for attention)
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251]+",
        flags=re.UNICODE
    )
    if emoji_pattern.search(body):
        score -= 5
        issues.append("Emoji in email body (unprofessional)")

    return max(0, score), issues


def _check_tone(subject: str, body: str) -> Tuple[float, List[str]]:
    """Check email tone and professionalism."""
    score = 100.0
    issues = []

    # Check for overly casual language
    casual_markers = [
        "hey", "hallo", "hoi",  # "Hallo" is OK in NL, but "hey"/"hoi" is too casual for B2B
        "sup", "yo", "whats up",
    ]
    body_lower = body.lower()
    for marker in casual_markers:
        if marker in body_lower.split():
            score -= 5
            issues.append(f"Too casual: '{marker}'")

    # Check for pushy language
    pushy_markers = [
        "u moet", "you must", "u moet zeker", "you definitely should",
        "ik bel u", "i'll call you", "ik zal u bellen",
        "wacht niet", "don't wait",
    ]
    for marker in pushy_markers:
        if marker in body_lower:
            score -= 10
            issues.append(f"Pushy language: '{marker}'")

    # Check CTA quality — should have exactly one clear CTA
    cta_patterns = [
        r"plan\s+(?:een|een\s+momental)",
        r"maak\s+(?:een|een\s+afspraak)",
        r"laten\s+(?:we|wij)\s+(?:bellen|praten|spreken)",
        r"cal(?:endly|\.com)",
        r"schedule\s+a\s+(?:call|meeting)",
        r"book\s+a\s+(?:call|meeting)",
        r"can\s+we\s+(?:chat|talk|connect|meet)",
    ]
    cta_count = sum(1 for p in cta_patterns if re.search(p, body_lower))
    if cta_count == 0:
        score -= 15
        issues.append("No clear CTA found")
    elif cta_count > 2:
        score -= 10
        issues.append(f"Too many CTAs ({cta_count}) — confusing")

    # Check for proper greeting
    greeting_patterns = [
        r"^hoi\s+\w+", r"^hey\s+\w+", r"^beste\s+\w+",
        r"^hallo\s+\w+", r"^hello\s+\w+", r"^hi\s+\w+",
    ]
    if not any(re.search(p, body_lower.strip()) for p in greeting_patterns):
        score -= 10
        issues.append("Missing proper greeting with name")

    return max(0, score), issues


def _check_dutch_quality(subject: str, body: str) -> Tuple[float, List[str]]:
    """Basic Dutch language quality checks."""
    score = 100.0
    issues = []
    combined = subject + " " + body

    # Check for common Dutch spelling mistakes
    spelling_mistakes = {
        "bedrijfs": "bedrijf",  # Wrong plural
        "klanten": "klant",     # Should be singular in context
        "wij zijn": "we zijn",  # Too formal for je/jullie tone
        "u bent": "je bent",    # Too formal
        "u heeft": "je hebt",   # Too formal
    }
    for mistake, correct in spelling_mistakes.items():
        if mistake in combined.lower():
            score -= 5
            issues.append(f"Tone mismatch: '{mistake}' (should be '{correct}' for je/jullie tone)")

    # Check for proper sentence structure (basic)
    sentences = re.split(r'[.!?]+', body)
    sentences = [s.strip() for s in sentences if s.strip()]
    if sentences:
        avg_length = sum(len(s.split()) for s in sentences) / len(sentences)
        if avg_length > 30:
            score -= 10
            issues.append("Sentences too long — hard to read")
        elif avg_length < 5:
            score -= 5
            issues.append("Sentences too short — fragmented")

    return max(0, score), issues


def _check_structure(subject: str, body: str) -> Tuple[float, List[str]]:
    """Check email structure and formatting."""
    score = 100.0
    issues = []

    # Subject line checks
    if len(subject) < 10:
        score -= 15
        issues.append("Subject too short")
    elif len(subject) > 80:
        score -= 10
        issues.append("Subject too long")

    if subject.endswith("?") or subject.endswith("!"):
        pass  # Questions and exclamations are OK in NL
    elif subject[0].islower():
        score -= 5
        issues.append("Subject should start with capital letter")

    # Body structure
    paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]
    if len(paragraphs) < 2:
        score -= 10
        issues.append("Email should have at least 2 paragraphs")

    # Check for links (should be minimal)
    link_count = len(re.findall(r'https?://\S+', body))
    if link_count > 2:
        score -= 10
        issues.append(f"Too many links ({link_count}) — looks spammy")

    return max(0, score), issues


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN QUALITY GATE
# ═══════════════════════════════════════════════════════════════════════════════

def review_email_quality(contact_id: str) -> Dict[str, Any]:
    """Review a single email draft and return quality assessment."""
    db = _get_db()
    try:
        contact = db.execute(
            """SELECT c.contact_id, c.first_name, c.last_name, c.email, c.role,
                      c.outreach_draft, c.research_result,
                      co.name as company_name, co.domain, co.industry
               FROM contacts c
               LEFT JOIN companies co ON c.company_id = co.company_id
               WHERE c.contact_id = ?""",
            (contact_id,),
        ).fetchone()

        if not contact:
            return {"passed": False, "error": "Contact not found"}

        draft_raw = contact["outreach_draft"]
        if not draft_raw:
            return {"passed": False, "error": "No draft found"}

        # Parse draft JSON
        try:
            draft = json.loads(draft_raw) if isinstance(draft_raw, str) else draft_raw
        except json.JSONDecodeError:
            return {"passed": False, "error": "Invalid draft JSON"}

        # Handle multiple draft formats
        subject = (
            draft.get("subject")
            or draft.get("subject_line_A")
            or draft.get("subject_line")
            or ""
        )
        body = (
            draft.get("body")
            or draft.get("email_body_step_1")
            or draft.get("email_body")
            or ""
        )

        if not subject or not body:
            return {"passed": False, "error": "Missing subject or body"}

        contact_dict = {
            "first_name": contact["first_name"],
            "last_name": contact["last_name"],
            "company_name": contact["company_name"],
            "domain": contact["domain"],
            "industry": contact["industry"],
            "role": contact["role"],
        }

        # Run all checks
        p_score, p_issues = _check_personalization(subject, body, contact_dict)
        s_score, s_issues = _check_spam_triggers(subject, body)
        t_score, t_issues = _check_tone(subject, body)
        d_score, d_issues = _check_dutch_quality(subject, body)
        st_score, st_issues = _check_structure(subject, body)

        # Weighted average
        total_score = (
            p_score * 0.30 +  # Personalization is most important
            s_score * 0.20 +  # Spam avoidance
            t_score * 0.20 +  # Tone
            d_score * 0.15 +  # Language quality
            st_score * 0.15   # Structure
        )

        all_issues = p_issues + s_issues + t_issues + d_issues + st_issues
        passed = total_score >= 70 and p_score >= 50  # Must have basic personalization

        result = {
            "passed": passed,
            "score": round(total_score, 1),
            "breakdown": {
                "personalization": {"score": round(p_score, 1), "issues": p_issues},
                "spam_triggers": {"score": round(s_score, 1), "issues": s_issues},
                "tone": {"score": round(t_score, 1), "issues": t_issues},
                "language": {"score": round(d_score, 1), "issues": d_issues},
                "structure": {"score": round(st_score, 1), "issues": st_issues},
            },
            "all_issues": all_issues,
            "subject": subject,
            "body_preview": body[:100] + "..." if len(body) > 100 else body,
        }

        # Store quality assessment in DB
        for attempt in range(3):
            try:
                db.execute(
                    """UPDATE contacts SET 
                   quality_score = ?, quality_issues = ?, quality_reviewed_at = ?
                   WHERE contact_id = ?""",
                    (total_score, json.dumps(all_issues), datetime.now(timezone.utc).isoformat(), contact_id),
                )
                db.commit()
                break
            except Exception as e:
                if "locked" in str(e).lower() and attempt < 2:
                    import time
                    time.sleep(2)
                else:
                    logger.warning(f"DB write failed for {contact_id}: {e}")

        return result

    except Exception as e:
        logger.error(f"Quality review failed for {contact_id}: {e}")
        return {"passed": False, "error": str(e)}
    finally:
        db.close()


def run_quality_gate(limit: int = 20) -> Dict[str, Any]:
    """Run quality gate on all ACTIVE_OUTREACH contacts with drafts."""
    db = _get_db()
    try:
        # Add quality columns if they don't exist
        try:
            db.execute("ALTER TABLE contacts ADD COLUMN quality_score REAL")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE contacts ADD COLUMN quality_issues TEXT")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE contacts ADD COLUMN quality_reviewed_at TEXT")
        except Exception:
            pass
        db.commit()

        # Find contacts with drafts that haven't been quality-checked
        contacts = db.execute(
            """SELECT contact_id, first_name, last_name, company_id
               FROM contacts
               WHERE current_stage = 'ACTIVE_OUTREACH'
                 AND outreach_draft IS NOT NULL AND outreach_draft != ''
                 AND (quality_score IS NULL OR quality_reviewed_at IS NULL)
               ORDER BY lead_score DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

        if not contacts:
            db.close()
            return {"reviewed": 0, "passed": 0, "failed": 0}

        passed = 0
        failed = 0
        results = []

        for c in contacts:
            review = review_email_quality(c["contact_id"])
            if review.get("passed"):
                passed += 1
                # Move to PENDING_APPROVAL
                for attempt in range(3):
                    try:
                        db.execute(
                            "UPDATE contacts SET current_stage = 'PENDING_APPROVAL' WHERE contact_id = ?",
                            (c["contact_id"],),
                        )
                        db.commit()
                        break
                    except Exception as e:
                        if "locked" in str(e).lower() and attempt < 2:
                            import time
                            time.sleep(2)
                        else:
                            logger.warning(f"DB write failed for {c['contact_id']}: {e}")
                logger.info(
                    f"[QualityGate] PASS: {c['first_name']} {c['last_name']} "
                    f"(score: {review.get('score', 0)})"
                )
            else:
                failed += 1
                # Keep in ACTIVE_OUTREACH, mark for regeneration
                for attempt in range(3):
                    try:
                        db.execute(
                            "UPDATE contacts SET current_stage = 'NEEDS_REGENERATION' WHERE contact_id = ?",
                            (c["contact_id"],),
                        )
                        db.commit()
                        break
                    except Exception as e:
                        if "locked" in str(e).lower() and attempt < 2:
                            import time
                            time.sleep(2)
                        else:
                            logger.warning(f"DB write failed for {c['contact_id']}: {e}")
                logger.warning(
                    f"[QualityGate] FAIL: {c['first_name']} {c['last_name']} "
                    f"(score: {review.get('score', 0)}, issues: {len(review.get('all_issues', []))})"
                )
            results.append({
                "contact_id": c["contact_id"],
                "name": f"{c['first_name']} {c['last_name']}",
                **review,
            })

        db.commit()
        db.close()

        summary = {
            "reviewed": passed + failed,
            "passed": passed,
            "failed": failed,
            "results": results,
        }
        logger.info(f"[QualityGate] Complete: {passed} passed, {failed} failed out of {passed + failed}")
        return summary

    except Exception as e:
        logger.error(f"Quality gate error: {e}")
        return {"error": str(e)}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="ClawBuildr Email Quality Gate")
    parser.add_argument("--contact", help="Review specific contact_id")
    parser.add_argument("--limit", type=int, default=20, help="Max contacts to review")
    args = parser.parse_args()

    if args.contact:
        result = review_email_quality(args.contact)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        result = run_quality_gate(limit=args.limit)
        print(f"\n=== Quality Gate Results ===")
        print(f"Reviewed: {result.get('reviewed', 0)}")
        print(f"Passed: {result.get('passed', 0)}")
        print(f"Failed: {result.get('failed', 0)}")
        if result.get("results"):
            print(f"\nDetails:")
            for r in result["results"]:
                status = "PASS" if r.get("passed") else "FAIL"
                print(f"  [{status}] {r.get('name', '?')} — score: {r.get('score', 0)}")
                for issue in r.get("all_issues", [])[:3]:
                    print(f"         - {issue}")
