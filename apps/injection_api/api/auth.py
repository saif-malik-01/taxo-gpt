"""
ingestion_api/api/auth.py

JWT creation and verification.
Single admin user for now — extend to DB-backed users later.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from jose import JWTError, jwt

from api.config import SVC_CONFIG


# ── Password helpers ─────────────────────────────────────────────────────────

def verify_password(plain: str, hashed: str) -> bool:
    """Compare a plain-text password against a stored bcrypt hash."""
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def hash_password(plain: str) -> str:
    """Generate a bcrypt hash — use this in CLI to create ADMIN_PASSWORD_HASH."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


# ── Token creation ────────────────────────────────────────────────────────────

def create_access_token(subject: str) -> tuple[str, int]:
    """
    Create a JWT access token.
    Returns (token_string, expires_in_seconds).
    """
    expire_minutes = SVC_CONFIG.jwt.access_token_expire_minutes
    expire = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)

    payload = {
        "sub": subject,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access",
    }
    token = jwt.encode(
        payload,
        SVC_CONFIG.jwt.secret_key,
        algorithm=SVC_CONFIG.jwt.algorithm,
    )
    return token, expire_minutes * 60


# ── Token verification ────────────────────────────────────────────────────────

def decode_access_token(token: str) -> Optional[str]:
    """
    Decode and validate a JWT.
    Returns the subject (username) on success, None on any failure.
    """
    try:
        payload = jwt.decode(
            token,
            SVC_CONFIG.jwt.secret_key,
            algorithms=[SVC_CONFIG.jwt.algorithm],
        )
        subject: str = payload.get("sub")
        if not subject:
            return None
        if payload.get("type") != "access":
            return None
        return subject
    except JWTError:
        return None


# ── Credential validation ─────────────────────────────────────────────────────

def authenticate_user(username: str, password: str) -> Optional[str]:
    """
    Validate credentials against the configured admin user.
    Returns username on success, None on failure.
    Constant-time comparison to prevent timing attacks.
    """
    cfg = SVC_CONFIG.admin

    # Always run bcrypt even on username mismatch to prevent timing oracle
    expected_username = cfg.username
    stored_hash = cfg.password_hash

    username_ok = username == expected_username
    password_ok = verify_password(password, stored_hash)

    if username_ok and password_ok:
        return username
    return None