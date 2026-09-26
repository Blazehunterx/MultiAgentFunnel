#!/usr/bin/env python3
"""
ClawBuildr Reply Detector — IMAP polling for inbound replies.
Classifies sentiment and updates contact stages automatically.
"""

import os
import imaplib
import email
import email.message
from email.header import decode_header
import sqlite3
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger("ClawBuildr.ReplyDetector")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

_DOT_ENV = os.path.join(os.path.dirname(__file__), ".env")
_GMAIL_USER = ""
_GMAIL_PASSWORD = ""
_GMAIL_IMAP = "imap.gmail.com"
if os.path.exists(_DOT_ENV):
    for line in open(_DOT_ENV):
        line = line.strip()
        if line.startswith("GMAIL_USER="):
            _GMAIL_USER = line.split("=", 1)[1]
        elif line.startswith("GMAIL_PASSWORD="):
            _GMAIL_PASSWORD = line.split("=", 1)[1]

_POSITIVE_KEYWORDS = [
    "interessant", "geïnteresseerd", "laten we", "afspraak", "gesprek", "bellen",
    "call", "meeting", "demo", "geïnteresseerd", "ja graag", "top", "goed",
    "perfect", "geweldig", "thanks", "thank you", "appreciate", "love to",
    "sure", "sounds good", "let's talk", "schedule", "available",
]
_NEGATIVE_KEYWORDS = [
    "niet geïnteresseerd", "afgemeld", "stop", "unsubscribe", "uit",
    "geen interesse", "niet relevant", "stoppen", "afmelden", "no thanks",
    "not interested", "remove", "stop", "opt out", "do not contact",
]
_MEETING_KEYWORDS = [
    "afspraak", "inplannen", "kalender", "calendly", "meet", "call",
    "bellen", "telefoon", "nummers", "datum", "tijd", "when", "schedule",
    "book", "calendar", "available", "free", "slot",
]
_OBJECTION_KEYWORDS = [
    "maar", "echter", "probleem", "kosten", "duur", "twijfel",
    "however", "but", "concern", "worry", "expensive", "price",
]

# Auto-replies must never be classified as real replies (they used to hit
# the positive keywords and wrongly flip contacts to REPLIED).
_OOO_SUBJECT_PREFIXES = (
    "automatic reply", "auto-reply", "autoreply", "out of office",
    "out-of-office", "vacation", "automatisch antwoord", "afwezig",
    "autoreply:", "auto:", "oof:",
)
_OOO_BODY_KEYWORDS = (
    "out of office", "out-of-office", "automatisch antwoord",
    "ik ben afwezig", "on vacation", "away from my desk",
    "will be back", "terug op", "do not reply to this email",
    "niet op deze e-mail reageren", "vacation reply",
)


def _is_auto_reply(subject: str, body: str) -> bool:
    subj_l = (subject or "").lower().strip()
    if any(subj_l.startswith(p) for p in _OOO_SUBJECT_PREFIXES):
        return True
    check_l = (subj_l + " " + (body or "")[:2000].lower())
    return any(kw in check_l for kw in _OOO_BODY_KEYWORDS)


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def classify_sentiment(text: str) -> str:
    """Classify reply sentiment. Returns: positive, negative, meeting_request, objection, neutral."""
    lower = text.lower()

    meeting_score = sum(1 for kw in _MEETING_KEYWORDS if kw in lower)
    positive_score = sum(1 for kw in _POSITIVE_KEYWORDS if kw in lower)
    negative_score = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in lower)
    objection_score = sum(1 for kw in _OBJECTION_KEYWORDS if kw in lower)

    if meeting_score >= 2:
        return "meeting_request"
    if negative_score >= 1:
        return "negative"
    if objection_score >= 2:
        return "objection"
    if positive_score >= 1:
        return "positive"
    return "neutral"


def _decode_mime_header(header_value: str) -> str:
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    result = []
    for part, charset in decoded_parts:
        if isinstance(part, bytes):
            result.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(part)
    return " ".join(result)


def _extract_body(msg: email.message.Message) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    body += payload.decode(charset, errors="replace")
            elif ct == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    body = re.sub(r'<[^>]+>', ' ', html)
                    body = re.sub(r'\s+', ' ', body).strip()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            body = payload.decode(charset, errors="replace")
    return body.strip()


def get_imap_accounts(tenant_id: Optional[str] = None) -> List[Dict[str, str]]:
    """IMAP credentials for connected mailboxes.

    tenant_id=None → ALL connected mailboxes (reply detection must keep
    working for every tenant, regardless of which tenant is active).
    An explicit tenant_id scopes the result to that tenant.
    Uses email_accounts rows (smtp_user/smtp_password double as Gmail IMAP
    credentials). Falls back to .env credentials when nothing is connected.
    """
    accounts: List[Dict[str, str]] = []
    try:
        db = _get_db()
        try:
            sql = """SELECT email_address, smtp_user, smtp_password, provider
                     FROM email_accounts
                     WHERE is_active = 1
                       AND smtp_user IS NOT NULL AND smtp_user != ''
                       AND smtp_password IS NOT NULL AND smtp_password != ''"""
            params: tuple = ()
            if tenant_id:
                sql += " AND (tenant_id = ? OR tenant_id IS NULL OR tenant_id = '')"
                params = (tenant_id,)
            for r in db.execute(sql, params).fetchall():
                if (r["provider"] or "gmail").lower() != "gmail":
                    continue  # IMAP host is only known for Gmail accounts
                accounts.append({
                    "user": r["smtp_user"] or r["email_address"],
                    "password": r["smtp_password"],
                    "host": _GMAIL_IMAP,
                })
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"get_imap_accounts failed: {e}")

    if not accounts and _GMAIL_USER and _GMAIL_PASSWORD:
        accounts.append({"user": _GMAIL_USER, "password": _GMAIL_PASSWORD, "host": _GMAIL_IMAP})
    return accounts


def check_replies(since_hours: int = 24) -> List[Dict[str, Any]]:
    """Poll IMAP for new replies on every connected mailbox. Returns processed replies."""
    accounts = get_imap_accounts()
    if not accounts:
        logger.warning("No IMAP credentials configured. Cannot check replies.")
        return []
    processed: List[Dict[str, Any]] = []
    for account in accounts:
        processed.extend(_poll_account(account, since_hours))
    return processed


def _poll_account(account: Dict[str, str], since_hours: int = 24) -> List[Dict[str, Any]]:
    """Poll one mailbox. `account` = {user, password, host}."""
    imap_user = account["user"]
    processed: List[Dict[str, Any]] = []
    try:
        mail = imaplib.IMAP4_SSL(account["host"])
        mail.login(imap_user, account["password"])
        mail.select("INBOX")

        since_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)
        search_date = since_date.strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(SINCE "{search_date}")')

        if status != "OK":
            return []

        msg_ids = messages[0].split()
        db = _get_db()

        known_message_ids = set()
        try:
            rows = db.execute("SELECT message_id FROM emails WHERE message_id IS NOT NULL").fetchall()
            known_message_ids = {r["message_id"] for r in rows}
        except Exception:
            pass

        # Also collect recently sent message IDs from emails table to avoid self-matching
        sent_subjects = set()
        try:
            rows = db.execute("SELECT subject FROM emails WHERE UPPER(direction) = 'OUTBOUND' AND sent_at IS NOT NULL").fetchall()
            sent_subjects = {r["subject"] for r in rows if r["subject"]}
        except Exception:
            pass

        for msg_id in msg_ids[-100:]:
            try:
                status, data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue

                raw = data[0][1]
                msg = email.message_from_bytes(raw)

                from_addr = msg.get("From", "")
                to_addr = msg.get("To", "")
                subject = _decode_mime_header(msg.get("Subject", ""))
                in_reply_to = msg.get("In-Reply-To", "")
                message_id = msg.get("Message-ID", "")
                references = msg.get("References", "")

                if message_id in known_message_ids:
                    continue

                if imap_user.split("@")[0] not in to_addr:
                    continue

                body = _extract_body(msg)
                if not body or len(body) < 10:
                    continue

                sender_email = re.search(r'<([^>]+)>', from_addr)
                sender_email = sender_email.group(1) if sender_email else from_addr.strip()

                contact = db.execute(
                    "SELECT * FROM contacts WHERE email = ?", (sender_email,)
                ).fetchone()

                if not contact:
                    continue

                sentiment = "ooo" if _is_auto_reply(subject, body) else classify_sentiment(body)
                now = datetime.now(timezone.utc).isoformat()

                db.execute(
                    """INSERT INTO emails (contact_id, direction, status, subject, body, message_id, created_at, sent_at)
                       VALUES (?, 'inbound', ?, ?, ?, ?, ?, ?)""",
                    (contact["contact_id"], sentiment, subject, body[:5000], message_id, now, now),
                )

                stage_update = None
                if sentiment == "positive" or sentiment == "meeting_request":
                    stage_update = "REPLIED"
                elif sentiment == "negative":
                    stage_update = "CLOSED_LOST"
                elif sentiment == "objection":
                    stage_update = "OBJECTION"

                if stage_update:
                    db.execute(
                        "UPDATE contacts SET current_stage = ?, updated_at = ? WHERE contact_id = ?",
                        (stage_update, now, contact["contact_id"]),
                    )

                db.execute(
                    """INSERT INTO activity_log (contact_id, activity_type, description, metadata, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (contact["contact_id"], "ooo_detected" if sentiment == "ooo" else "reply_detected",
                     subject, json.dumps({
                        "sentiment": sentiment,
                        "subject": subject,
                        "preview": body[:200],
                    }), now),
                )
                db.commit()
                known_message_ids.add(message_id)

                processed.append({
                    "contact_id": contact["contact_id"],
                    "email": sender_email,
                    "sentiment": sentiment,
                    "subject": subject,
                    "preview": body[:200],
                    "stage_update": stage_update,
                })

                logger.info(f"Reply from {sender_email}: sentiment={sentiment}, stage={stage_update}")

            except Exception as e:
                logger.error(f"Error processing IMAP message: {e}")
                continue

        db.close()
        mail.logout()

    except Exception as e:
        logger.error(f"IMAP connection error for {imap_user}: {e}")

    return processed


def get_reply_stats() -> Dict[str, Any]:
    """Get reply statistics from the database."""
    db = _get_db()
    try:
        total = db.execute("SELECT COUNT(*) FROM emails WHERE UPPER(direction) = 'INBOUND'").fetchone()[0]
        by_sentiment = {}
        for row in db.execute(
            "SELECT status, COUNT(*) as cnt FROM emails WHERE UPPER(direction) = 'INBOUND' GROUP BY status"
        ).fetchall():
            by_sentiment[row["status"]] = row["cnt"]

        return {"total_replies": total, "by_sentiment": by_sentiment}
    finally:
        db.close()


def check_bounces(since_hours: int = 48) -> List[Dict[str, Any]]:
    """Check IMAP for bounce notifications and mark contacts as BOUNCED."""
    if not _GMAIL_USER or not _GMAIL_PASSWORD:
        return []

    bounced = []
    try:
        mail = imaplib.IMAP4_SSL(_GMAIL_IMAP)
        mail.login(_GMAIL_USER, _GMAIL_PASSWORD)
        mail.select("INBOX")

        since_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)
        search_date = since_date.strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(FROM "mailer-daemon" SINCE "{search_date}")')

        if status != "OK":
            mail.logout()
            return []

        msg_ids = messages[0].split()
        db = _get_db()

        for msg_id in msg_ids[-50:]:
            try:
                status, data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue

                raw = data[0][1]
                msg = email.message_from_bytes(raw)
                body = _extract_body(msg)

                # Prefer structured NDR fields; never grab an arbitrary address from free text
                # (generic regex matched marketing addresses inside quoted outbound copy).
                recipient_match = re.search(r'Final-Recipient:\s*rfc822;\s*([^\s,;]+)', body, re.IGNORECASE)
                if not recipient_match:
                    recipient_match = re.search(r'X-Failed-Recipients:\s*([^\s,;]+)', body, re.IGNORECASE)
                if not recipient_match:
                    recipient_match = re.search(
                        r'(?:original\s+recipient|Action:\s*failed).*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                        body,
                        re.IGNORECASE | re.DOTALL,
                    )

                if not recipient_match:
                    continue

                bounced_email = recipient_match.group(1).lower().strip().strip('<>')

                # Find the contact
                contact = db.execute(
                    "SELECT contact_id, first_name, last_name FROM contacts WHERE email = ?",
                    (bounced_email,)
                ).fetchone()

                if not contact:
                    continue

                # Check if already marked as bounced
                already = db.execute(
                    "SELECT 1 FROM emails WHERE contact_id = ? AND UPPER(status) = 'BOUNCED'",
                    (contact["contact_id"],)
                ).fetchone()
                if already:
                    continue

                # Extract bounce reason
                reason = "unknown"
                if "550" in body or "5.1.1" in body:
                    reason = "mailbox not found"
                elif "552" in body or "5.3.4" in body:
                    reason = "message too large"
                elif "553" in body or "5.7" in body:
                    reason = "policy rejection"
                elif "451" in body or "4.4" in body:
                    reason = "temporary failure"
                elif "delayed" in body.lower() or "Delay" in msg.get("Subject", ""):
                    reason = "delivery delayed"
                else:
                    # Extract first diagnostic line
                    diag = re.search(r'The response was:(.+?)(?:\n\n|\r\n\r\n)', body, re.DOTALL)
                    if diag:
                        reason = diag.group(1).strip()[:200]

                now = datetime.now(timezone.utc).isoformat()

                # Record bounce as a dedicated notification row — do not flip historical sends
                already_note = db.execute(
                    "SELECT 1 FROM emails WHERE contact_id = ? AND subject = 'Bounce notification' AND UPPER(status) = 'BOUNCED'",
                    (contact["contact_id"],),
                ).fetchone()
                if not already_note:
                    db.execute(
                        """INSERT INTO emails (contact_id, direction, status, subject, body, bounce_reason, sent_at, created_at)
                           VALUES (?, 'outbound', 'BOUNCED', 'Bounce notification', ?, ?, ?, ?)""",
                        (contact["contact_id"], body[:2000], reason, now, now),
                    )

                # Update contact stage
                db.execute(
                    "UPDATE contacts SET current_stage = 'BOUNCED', updated_at = ? WHERE contact_id = ?",
                    (now, contact["contact_id"]),
                )

                # Cancel any pending follow-ups
                db.execute(
                    "UPDATE followup_queue SET status = 'cancelled' WHERE contact_id = ? AND status IN ('pending', 'ready')",
                    (contact["contact_id"],),
                )

                db.commit()
                bounced.append({
                    "email": bounced_email,
                    "contact_id": contact["contact_id"],
                    "reason": reason,
                })
                logger.warning(f"Bounce detected: {bounced_email} — {reason}")

            except Exception as e:
                logger.error(f"Error processing bounce: {e}")
                continue

        db.close()
        mail.logout()

    except Exception as e:
        logger.error(f"Bounce check IMAP error: {e}")

    return bounced


if __name__ == "__main__":
    replies = check_replies(since_hours=24)
    print(f"Found {len(replies)} new replies")
    for r in replies:
        print(f"  {r['email']}: {r['sentiment']} — {r['preview'][:80]}")
