"""
Read/write public."User" via Supabase service role (server only).
Column names must match the Supabase table editor (quoted identifiers).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger("mirofish.user_auth_db")

USER_TABLE = "User"


def _client():
    from supabase import create_client

    if not Config.SUPABASE_URL or not Config.SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required for auth")
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_ROLE_KEY)


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    email = (email or "").strip()
    if not email:
        return None
    try:
        sb = _client()
        r = sb.table(USER_TABLE).select("id, Username, Email, Password").eq("Email", email).limit(1).execute()
        rows = r.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.error("get_user_by_email: %s", e)
        raise


def email_exists(email: str) -> bool:
    return get_user_by_email(email) is not None


def insert_user(*, username: str, email: str, password: str) -> Dict[str, Any]:
    try:
        sb = _client()
        r = (
            sb.table(USER_TABLE)
            .insert({"Username": username.strip(), "Email": email.strip(), "Password": password})
            .select("id, Username, Email")
            .execute()
        )
        rows = r.data or []
        if not rows:
            raise RuntimeError("Insert returned no row")
        return rows[0]
    except Exception as e:
        logger.error("insert_user: %s", e)
        raise
