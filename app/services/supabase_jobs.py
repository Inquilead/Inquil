"""
Persist pipeline runs and finished reports to Supabase (Postgres + Storage).
Uses the service role on the server only — never expose the key to the browser.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger("mirofish.supabase_jobs")


def sync_enabled() -> bool:
    """True when Postgres + Storage sync is available (service role). JWT secret is only required for Bearer auth."""
    return bool(Config.SUPABASE_URL and Config.SUPABASE_SERVICE_ROLE_KEY)


def _client():
    from supabase import create_client

    return create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_ROLE_KEY)


def insert_pending_run(
    *,
    simulation_run_id: str,
    user_id: str,
    backend_task_id: str,
    requirement_preview: str,
) -> None:
    if not sync_enabled():
        return
    try:
        sb = _client()
        sb.table("simulation_runs").insert(
            {
                "id": simulation_run_id,
                "user_id": user_id,
                "backend_task_id": backend_task_id,
                "status": "pending",
                "requirement_preview": (requirement_preview or "")[:2000],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ).execute()
    except Exception as e:
        logger.error("insert_pending_run failed: %s", e)
        raise


def mark_run_failed(*, simulation_run_id: str, error_message: str) -> None:
    if not sync_enabled() or not simulation_run_id:
        return
    try:
        sb = _client()
        sb.table("simulation_runs").update(
            {
                "status": "failed",
                "error_message": (error_message or "")[:4000],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", simulation_run_id).execute()
    except Exception as e:
        logger.error("mark_run_failed failed: %s", e)


def mark_run_completed(
    *,
    simulation_run_id: str,
    user_id: str,
    report_id: str,
    markdown_content: str,
    simulation_id: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Upload markdown to Storage and mark row completed. Returns { report_storage_path, report_public_url? }.
    """
    if not sync_enabled() or not simulation_run_id:
        return None
    path = f"{user_id}/{simulation_run_id}.md"
    body = markdown_content.encode("utf-8")
    try:
        sb = _client()
        bucket = Config.SUPABASE_REPORTS_BUCKET
        # upsert so retries overwrite
        storage_api = sb.storage.from_(bucket)
        try:
            storage_api.upload(
                path,
                body,
                file_options={
                    "content-type": "text/markdown; charset=utf-8",
                    "upsert": "true",
                },
            )
        except TypeError:
            storage_api.upload(path, body)

        pub: Optional[str] = None
        try:
            u = storage_api.get_public_url(path)
            if isinstance(u, dict):
                pub = u.get("publicUrl") or u.get("public_url")
            elif isinstance(u, str):
                pub = u
        except Exception:
            pub = None

        sb.table("simulation_runs").update(
            {
                "status": "completed",
                "report_id": report_id,
                "report_storage_path": path,
                "report_public_url": pub,
                "simulation_id": simulation_id,
                "project_id": project_id,
                "error_message": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", simulation_run_id).execute()

        return {"report_storage_path": path, "report_public_url": pub}
    except Exception as e:
        logger.error("mark_run_completed failed: %s", e)
        mark_run_failed(simulation_run_id=simulation_run_id, error_message=f"Report upload failed: {e}")
        raise


def list_simulation_runs_for_user(*, user_id: str) -> list:
    if not sync_enabled():
        return []
    sb = _client()
    r = sb.table("simulation_runs").select("*").eq("user_id", user_id).order("created_at", desc=True).execute()
    return r.data or []


def get_run_row_for_user(*, run_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    if not sync_enabled():
        return None
    sb = _client()
    r = sb.table("simulation_runs").select("*").eq("id", run_id).eq("user_id", user_id).limit(1).execute()
    rows = r.data or []
    return rows[0] if rows else None


def signed_url_for_storage_path(*, storage_path: str, expires_sec: int = 3600) -> Optional[str]:
    if not sync_enabled() or not storage_path:
        return None
    sb = _client()
    bucket = Config.SUPABASE_REPORTS_BUCKET
    try:
        res = sb.storage.from_(bucket).create_signed_url(storage_path, expires_sec)
        if isinstance(res, dict):
            return (
                res.get("signedURL")
                or res.get("signed_url")
                or (res.get("data") or {}).get("signedUrl")
            )
        signed = getattr(res, "signed_url", None) or getattr(res, "signedURL", None)
        if signed:
            return signed
    except TypeError:
        try:
            res = sb.storage.from_(bucket).create_signed_url(storage_path, {"expires_in": expires_sec})
            if isinstance(res, dict):
                return res.get("signedURL") or res.get("signed_url")
        except Exception as e2:
            logger.error("signed_url_for_storage_path fallback: %s", e2)
    except Exception as e:
        logger.error("signed_url_for_storage_path: %s", e)
    return None
