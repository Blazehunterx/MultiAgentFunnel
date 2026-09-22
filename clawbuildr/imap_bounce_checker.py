"""
IMAP Bounce Checker for ClawBuildr.
Connects to Gmail via IMAP, finds bounce/NDR emails, and marks
matching contacts/emails as BOUNCED in clawbuildr.db.
"""
import imaplib
import email
import re
import sqlite3
import os
import uuid
import json
from datetime import datetime, timezone, timedelta
from email.header import decode_header

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "clawbuildr.db")

BOUNCE_SENDERS = [
    "postmaster@", "mailer-daemon@", "mail-daemon@", "postmaster",
    "mailer-daemon", "mail delivery subsystem", "delivery status notification",
]

BOUNCE_SUBJECTS = [
    "delivery status notification", "delivery failed", "undeliverable",
    "returned mail", "failure notice", "mail delivery failed",
    "message not delivered", "bounce", "rejected",
]


def _decode_header_value(value):
    if not value:
        return ""
    parts = decode_header(value)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            try:
                decoded.append(part.decode(charset or "utf-8", errors="replace"))
            except Exception:
                decoded.append(part.decode("utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)


def _extract_recipient_from_bounce(msg):
    """Try to find the original recipient email address in a bounce message."""
    # Try X-Failed-Recipients header first
    failed = msg.get("X-Failed-Recipients", "")
    if failed:
        match = re.search(r"[\w\.-]+@[\w\.-]+\.\w+", failed)
        if match:
            return match.group(0).lower()

    # Try Return-Path / From of original message in body
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype in ("text/plain", "text/html"):
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode("utf-8", errors="replace") + "\n"
                except Exception:
                    pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode("utf-8", errors="replace")
        except Exception:
            pass

    # Look for "To: <email>" or "Final-Recipient: rfc822; email" or "Diagnostic-Code" blocks
    patterns = [
        r"X-Failed-Recipients:\s*([\w\.-]+@[\w\.-]+\.\w+)",
        r"Final-Recipient:\s*rfc822;\s*([\w\.-]+@[\w\.-]+\.\w+)",
        r"Original-Recipient:\s*rfc822;\s*([\w\.-]+@[\w\.-]+\.\w+)",
        r"To:\s*<([\w\.-]+@[\w\.-]+\.\w+)>",
        r"\b([\w\.-]+@[\w\.-]+\.\w+)\b",
    ]
    for pat in patterns:
        match = re.search(pat, body, re.IGNORECASE)
        if match:
            return match.group(1).lower()

    return ""


def _is_bounce(msg):
    """Heuristic to identify a bounce/NDR email."""
    from_addr = _decode_header_value(msg.get("From", "")).lower()
    subject = _decode_header_value(msg.get("Subject", "")).lower()

    for sender in BOUNCE_SENDERS:
        if sender in from_addr:
            return True

    for subj in BOUNCE_SUBJECTS:
        if subj in subject:
            return True

    # Content-Type multipart/report is a strong signal
    if msg.get_content_type() == "multipart/report":
        return True

    return False


def check_bounces(gmail_user, gmail_password, mark_read=True, since_days=7):
    """Check Gmail inbox for bounce emails and update the database."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(gmail_user, gmail_password)
    imap.select("inbox")

    # Search messages: unread first, fallback to recent N days if requested
    if since_days and since_days > 0:
        since_str = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime("%d-%b-%Y")
        _, data = imap.search(None, "SENTSINCE", since_str)
    else:
        _, data = imap.search(None, "UNSEEN")
    msg_ids = data[0].split()

    found_bounces = []
    skipped = 0

    for msg_id in msg_ids:
        _, msg_data = imap.fetch(msg_id, "(RFC822)")
        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)

        if not _is_bounce(msg):
            skipped += 1
            continue

        recipient = _extract_recipient_from_bounce(msg)
        if not recipient:
            skipped += 1
            continue

        # Find matching contact
        cur.execute("SELECT contact_id, email, current_stage FROM contacts WHERE email = ?", (recipient,))
        contact = cur.fetchone()

        now = datetime.now(timezone.utc).isoformat()

        if contact:
            contact_id = contact["contact_id"]
            # Mark email as bounced (if exists)
            cur.execute("""
                UPDATE emails
                SET bounced_at = ?, bounce_reason = ?, status = 'BOUNCED'
                WHERE contact_id = ? AND direction = 'outbound' AND bounced_at IS NULL
            """, (now, "Bounce detected via IMAP", contact_id))

            # Mark contact as BOUNCED unless already closed/won/meeting
            if contact["current_stage"] not in ("CLOSED_WON", "MEETING_BOOKED", "BOUNCED"):
                cur.execute("""
                    UPDATE contacts
                    SET current_stage = 'BOUNCED', updated_at = ?
                    WHERE contact_id = ?
                """, (now, contact_id))

            # Log event
            event_id = str(uuid.uuid4())
            cur.execute("""
                INSERT OR IGNORE INTO email_events (event_id, email_id, contact_id, event_type, metadata, created_at)
                VALUES (?, ?, ?, 'BOUNCED', ?, ?)
            """, (event_id, None, contact_id, json.dumps({"bounce_email": recipient}), now))

            found_bounces.append({
                "contact_id": contact_id,
                "email": recipient,
                "stage": contact["current_stage"],
            })
        else:
            found_bounces.append({
                "contact_id": None,
                "email": recipient,
                "stage": "NOT_FOUND_IN_DB",
            })

        if mark_read:
            imap.store(msg_id, "+FLAGS", "\\Seen")

    conn.commit()
    conn.close()
    imap.close()
    imap.logout()

    return found_bounces, skipped, len(msg_ids)


if __name__ == "__main__":
    user = os.environ.get("GMAIL_USER", "marvin@clawbuildr.com")
    password = os.environ.get("GMAIL_PASSWORD", os.environ.get("GMAIL_APP_PASSWORD", ""))
    if not password:
        # Fallback: read from clawbuildr/.env
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("GMAIL_PASSWORD="):
                        password = line.strip().split("=", 1)[1].strip()
                        break

    if not password:
        print("ERROR: No Gmail password found. Set GMAIL_PASSWORD env var.")
        raise SystemExit(1)

    # Default: check last 7 days of all messages, not just unread,
    # so we catch bounces even if the user already glanced at them.
    bounces, skipped, total = check_bounces(user, password, mark_read=True, since_days=7)
    print(f"Checked {total} messages (last 7 days), found {len(bounces)} bounces, skipped {skipped}.")
    for b in bounces:
        print(" ", b)
