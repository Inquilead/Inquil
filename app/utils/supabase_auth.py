"""Verify caller identity for /api/pipeline/* — MiroFish session JWT or Supabase Auth JWT."""

from __future__ import annotations

from typing import Optional, Tuple

import jwt

from flask import request

from ..config import Config
from .mirofish_jwt import verify_mirofish_session_jwt


def verify_supabase_user_jwt(token: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (user_id, error_message). user_id is None on failure.
    """
    if not token or not Config.SUPABASE_JWT_SECRET:
        return None, "JWT verification not configured"
    try:
        payload = jwt.decode(
            token,
            Config.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
            options={"verify_exp": True},
        )
        sub = payload.get("sub")
        if not sub:
            return None, "Invalid token: missing sub"
        return str(sub), None
    except jwt.ExpiredSignatureError:
        return None, "Token expired"
    except jwt.InvalidTokenError as e:
        return None, f"Invalid token: {e}"


def extract_bearer_token() -> Optional[str]:
    h = request.headers.get("Authorization", "")
    if h.lower().startswith("bearer "):
        return h[7:].strip()
    return None


def resolve_pipeline_user_id() -> Tuple[Optional[str], Optional[str]]:
    """
    Identify the caller when Supabase sync is on.

    1) Authorization: Bearer <MiroFish session JWT> from /api/auth/login (public.User).
    2) Else Authorization: Bearer <Supabase Auth JWT> if SUPABASE_JWT_SECRET is set.

    Returns (user_id_str, error_message).
    """
    token = extract_bearer_token()
    if not token:
        return None, "Missing Authorization: Bearer token"

    m_uid, m_hint = verify_mirofish_session_jwt(token)
    if m_uid:
        return m_uid, None
    if m_hint == "expired":
        return None, "Token expired"
    if m_hint in ("invalid", "missing"):
        return None, "Invalid token"

    if Config.SUPABASE_JWT_SECRET:
        s_uid, s_err = verify_supabase_user_jwt(token)
        if s_uid:
            return s_uid, None
        return None, s_err or "Invalid token"

    return None, "Invalid or expired token"
