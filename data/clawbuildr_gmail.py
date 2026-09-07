#!/usr/bin/env python3
"""
ClawBuildr Gmail API Live Connector
Handles Google OAuth 2.0 flow using client credentials, sends MIME emails,
and reads incoming thread messages for autonomous inbox management.
"""

import os
import base64
import logging
from email.mime.text import MIMEText
from typing import List, Dict, Any, Optional

# Configure logging
logger = logging.getLogger("ClawBuildrGmail")

# Check libraries
HAS_GOOGLE_API = False
try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    HAS_GOOGLE_API = True
except ImportError:
    logger.warning("Google API Client libraries not fully installed. Running in Dry-Run/Mock Capable mode.")

# Scopes required for Gmail sending and reading
SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly"
]

class ClawBuildrGmailConnector:
    def __init__(self, credentials_path: str, token_path: str = "token_gmail.json"):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        
    def authenticate(self, run_local_server: bool = True) -> bool:
        """Runs the Google OAuth 2.0 authentication flow and caches token."""
        if not HAS_GOOGLE_API:
            logger.warning("[Gmail Connector] Cannot authenticate: googleapiclient libraries are missing.")
            return False
            
        creds = None
        # Load cached token if exists
        if os.path.exists(self.token_path):
            try:
                creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
                logger.info("[Gmail Connector] Loaded cached OAuth credentials successfully.")
            except Exception as e:
                logger.error(f"[Gmail Connector] Error loading token cache: {e}")

        # If no valid credentials, run the OAuth Consent flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    logger.info("[Gmail Connector] Token expired. Attempting refresh...")
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"[Gmail Connector] Failed to refresh token: {e}")
                    creds = None
            
            if not creds:
                if not os.path.exists(self.credentials_path):
                    logger.error(f"[Gmail Connector] Client secret JSON missing at: {self.credentials_path}")
                    return False
                
                try:
                    logger.info("[Gmail Connector] Initiating local browser authentication flow...")
                    flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                    # Cache credentials
                    with open(self.token_path, 'w') as token_file:
                        token_file.write(creds.to_json())
                    logger.info(f"[Gmail Connector] Successfully authenticated and saved token to {self.token_path}")
                except Exception as e:
                    logger.error(f"[Gmail Connector] Error during interactive authentication: {e}")
                    return False

        try:
            self.service = build('gmail', 'v1', credentials=creds)
            logger.info("[Gmail Connector] Live Gmail Service build completed successfully.")
            return True
        except Exception as e:
            logger.error(f"[Gmail Connector] Failed to build Gmail Service: {e}")
            return False

    def send_email(self, to_email: str, subject: str, body_text: str, body_html: str = None) -> Dict[str, Any]:
        """Formulates and sends a raw RFC 2822 email via Gmail API. Supports HTML with plain-text fallback."""
        if not self.service:
            logger.warning(f"[Gmail Connector] DRY RUN: Would have sent email to <{to_email}> with subject: '{subject}'")
            return {"status": "MOCK_SENT", "message_id": f"mock_msg_{to_email.split('@')[0]}", "thread_id": "mock_thread_123"}

        try:
            from email.mime.multipart import MIMEMultipart

            if body_html:
                # Send as multipart with HTML part (enables tracking pixel)
                message = MIMEMultipart('alternative')
                message['to'] = to_email
                message['subject'] = subject
                message.attach(MIMEText(body_text, 'plain', 'utf-8'))
                message.attach(MIMEText(body_html, 'html', 'utf-8'))
            else:
                # Fallback: plain text only
                message = MIMEText(body_text, 'plain', 'utf-8')
                message['to'] = to_email
                message['subject'] = subject

            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
            payload = {'raw': raw_message}
            sent_msg = self.service.users().messages().send(userId='me', body=payload).execute()
            logger.info(f"[Gmail Connector] Email sent successfully to {to_email}. Message ID: {sent_msg.get('id')}")
            return {
                "status": "SENT",
                "message_id": sent_msg.get('id'),
                "thread_id": sent_msg.get('threadId')
            }
        except Exception as e:
            logger.error(f"[Gmail Connector] Failed to send email to {to_email}: {e}")
            return {"status": "FAILED", "error": str(e)}


    def check_inbound_replies(self, query: str = "is:unread") -> List[Dict[str, Any]]:
        """Checks for unread incoming emails that can be processed as prospect replies."""
        if not self.service:
            logger.info("[Gmail Connector] DRY RUN: Fetching inbox replies (empty).")
            return []

        try:
            results = self.service.users().messages().list(userId='me', q=query).execute()
            messages = results.get('messages', [])
            replies = []
            
            for msg in messages:
                msg_id = msg['id']
                details = self.service.users().messages().get(userId='me', id=msg_id, format='full').execute()
                
                # Parse headers
                headers = details.get('payload', {}).get('headers', [])
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), '')
                sender = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
                
                # Parse Snippet
                snippet = details.get('snippet', '')
                
                replies.append({
                    "message_id": msg_id,
                    "thread_id": details.get('threadId'),
                    "from": sender,
                    "subject": subject,
                    "body": snippet,
                    "timestamp": details.get('internalDate')
                })
                
                # Optionally mark message as read to prevent reprocessing
                self.service.users().messages().batchModify(
                    userId='me',
                    body={
                        'ids': [msg_id],
                        'removeLabelIds': ['UNREAD']
                    }
                ).execute()
                
            return replies
        except Exception as e:
            logger.error(f"[Gmail Connector] Failed to fetch replies: {e}")
            return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing ClawBuildr Gmail Connector...")
    # Instantiate with user upload client credentials
    client_creds = r"C:\Users\marvi\odysseus\data\uploads\2026\06\08\22d61550477d45b0a27a2522b37115ae.json"
    connector = ClawBuildrGmailConnector(client_creds)
    # Perform standard authentication check
    authenticated = connector.authenticate()
    print(f"Authentication Successful? {authenticated}")
