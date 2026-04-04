"""Short-lived session JWTs for public.User logins (signed by the backend only)."""

from __future__ import annotations

import time
from typing import Optional, Tuple

import jwt

from ..config import Config

MIROFISH_ISS = "mirofish"
MIROFISH_AUD = "mirofish-web"
# Default 14 days
DEFAULT_TTL_SEC = 14 * 24 * 60 * 60


def _signing_secret() -> str:
    if (Config.MIROFISH_SESSION_JWT_SECRET or "").strip():
        return Config.MIROFISH_SESSION_JWT_SECRET.strip()
    return (Config.SECRET_KEY or "").strip() or "change-me"


def issue_session_token(user_id: str, ttl_sec: int = DEFAULT_TTL_SEC) -> str:
    now = int(time.time())
    payload = {
        "sub": str(user_id),
        "iat": now,
        "exp": now + ttl_sec,
        "iss": MIROFISH_ISS,
        "aud": MIROFISH_AUD,
    }
    return jwt.encode(payload, _signing_secret(), algorithm="HS256")


def verify_mirofish_session_jwt(token: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (user_id, None) on success, (None, error_hint) on failure.
    error_hint is None if the token is simply not a MiroFish session JWT (try Supabase next).
    """
    if not token:
        return None, "missing"
    try:
        payload = jwt.decode(
            token,
            _signing_secret(),
            algorithms=["HS256"],
            audience=MIROFISH_AUD,
            issuer=MIROFISH_ISS,
            options={"verify_exp": True},
        )
        sub = payload.get("sub")
        if not sub:
            return None, "invalid"
        return str(sub), None
    except jwt.ExpiredSignatureError:
        return None, "expired"
    except jwt.InvalidTokenError:
        return None, None
