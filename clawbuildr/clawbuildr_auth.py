#!/usr/bin/env python3
"""
ClawBuildr Auth — JWT authentication, API keys, workspace isolation.
"""

import os
import json
import sqlite3
import hashlib
import secrets
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger("ClawBuildr.Auth")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "clawbuildr.db")

JWT_SECRET = os.environ.get("CLAWBUILDR_JWT_SECRET", "clawbuildr-dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24


def _get_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_auth_tables():
    db = _get_db()
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT,
                role TEXT NOT NULL DEFAULT 'client',
                workspace_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login_at TEXT,
                is_active INTEGER NOT NULL DEFAULT 1
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS workspaces (
                workspace_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                plan TEXT NOT NULL DEFAULT 'free',
                owner_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                settings TEXT DEFAULT '{}',
                is_active INTEGER NOT NULL DEFAULT 1
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                workspace_id INTEGER NOT NULL,
                key_hash TEXT UNIQUE NOT NULL,
                name TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_used_at TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id)
            )
        """)
        db.commit()
    finally:
        db.close()


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    return f"{salt}:{h}"


def verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(":", 1)
    return hashlib.sha256(f"{salt}:{password}".encode()).hexdigest() == h


def create_user(email: str, password: str, name: str = "", role: str = "client") -> Dict[str, Any]:
    _ensure_auth_tables()
    db = _get_db()
    try:
        existing = db.execute("SELECT user_id FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            return {"error": "User already exists"}

        cursor = db.execute(
            "INSERT INTO users (email, password_hash, name, role) VALUES (?, ?, ?, ?)",
            (email, hash_password(password), name, role),
        )
        db.commit()
        return {"user_id": cursor.lastrowid, "email": email, "role": role}
    finally:
        db.close()


def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    _ensure_auth_tables()
    db = _get_db()
    try:
        user = db.execute("SELECT * FROM users WHERE email = ? AND is_active = 1", (email,)).fetchone()
        if not user:
            return None
        if not verify_password(password, user["password_hash"]):
            return None

        now = datetime.now(timezone.utc).isoformat()
        db.execute("UPDATE users SET last_login_at = ? WHERE user_id = ?", (now, user["user_id"]))
        db.commit()

        token = generate_token(user)
        return {
            "token": token,
            "user_id": user["user_id"],
            "email": user["email"],
            "name": user["name"],
            "role": user["role"],
        }
    finally:
        db.close()


def generate_token(user: sqlite3.Row) -> str:
    try:
        import jwt
    except ImportError:
        payload = {
            "user_id": user["user_id"],
            "email": user["email"],
            "role": user["role"],
            "exp": (datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)).isoformat(),
        }
        import hmac, base64
        header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().rstrip("=")
        body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        sig = hmac.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")
        return f"{header}.{body}.{sig_b64}"

    payload = {
        "user_id": user["user_id"],
        "email": user["email"],
        "role": user["role"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        import jwt
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except ImportError:
        pass
    except Exception:
        pass

    try:
        import hmac, base64
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig_b64 = parts
        expected_sig = hmac.new(JWT_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip("=")
        if sig_b64 != expected_b64:
            return None
        padding = 4 - len(body) % 4
        body += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(body))
        if "exp" in payload:
            exp = datetime.fromisoformat(payload["exp"].replace("Z", "+00:00"))
            if exp < datetime.now(timezone.utc):
                return None
        return payload
    except Exception:
        return None


def create_workspace(name: str, slug: str, owner_id: int, plan: str = "free") -> Dict[str, Any]:
    _ensure_auth_tables()
    db = _get_db()
    try:
        cursor = db.execute(
            "INSERT INTO workspaces (name, slug, owner_id, plan) VALUES (?, ?, ?, ?)",
            (name, slug, owner_id, plan),
        )
        db.commit()
        return {"workspace_id": cursor.lastrowid, "name": name, "slug": slug}
    finally:
        db.close()


def create_api_key(user_id: int, workspace_id: int, name: str = "") -> str:
    _ensure_auth_tables()
    raw_key = f"clb_{secrets.token_hex(32)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    db = _get_db()
    try:
        db.execute(
            "INSERT INTO api_keys (user_id, workspace_id, key_hash, name) VALUES (?, ?, ?, ?)",
            (user_id, workspace_id, key_hash, name),
        )
        db.commit()
        return raw_key
    finally:
        db.close()


def validate_api_key(api_key: str) -> Optional[Dict[str, Any]]:
    _ensure_auth_tables()
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    db = _get_db()
    try:
        row = db.execute(
            """SELECT ak.*, u.email, u.name, u.role
               FROM api_keys ak
               JOIN users u ON ak.user_id = u.user_id
               WHERE ak.key_hash = ? AND ak.is_active = 1 AND u.is_active = 1""",
            (key_hash,),
        ).fetchone()
        if not row:
            return None
        now = datetime.now(timezone.utc).isoformat()
        db.execute("UPDATE api_keys SET last_used_at = ? WHERE key_id = ?", (now, row["key_id"]))
        db.commit()
        return dict(row)
    finally:
        db.close()


def get_user_from_request(request) -> Optional[Dict[str, Any]]:
    """Extract user from request (JWT token or API key)."""
    auth_header = request.headers.get("Authorization", "")
    api_key = request.headers.get("X-API-Key", "")

    if api_key:
        return validate_api_key(api_key)

    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        payload = decode_token(token)
        if payload:
            return {
                "user_id": payload.get("user_id"),
                "email": payload.get("email"),
                "role": payload.get("role"),
            }

    return None


if __name__ == "__main__":
    _ensure_auth_tables()
    result = create_user("test@clawbuildr.com", "test123", "Test User", "admin")
    print("Create user:", result)
    if "user_id" in result:
        auth = authenticate_user("test@clawbuildr.com", "test123")
        print("Auth:", auth)
