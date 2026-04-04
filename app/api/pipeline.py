"""
One-shot pipeline API for the website: upload text + requirement → full automation → report.

When Supabase sync is configured (URL + service role), records runs + reports.
Identify users with Bearer JWT (auth.users) or X-Mirofish-User-Id + X-Mirofish-App-Key (public.User).
"""

import threading
import traceback
import uuid

from flask import request, jsonify

from . import pipeline_bp
from ..models.task import TaskManager, TaskStatus
from ..services.pipeline_orchestrator import run_full_pipeline
from ..services.supabase_jobs import (
    get_run_row_for_user,
    insert_pending_run,
    list_simulation_runs_for_user,
    signed_url_for_storage_path,
    sync_enabled,
)
from ..utils.logger import get_logger
from ..utils.supabase_auth import resolve_pipeline_user_id

logger = get_logger("mirofish.api.pipeline")

ALLOWED = {"txt", "md", "text"}


def _allowed(name: str) -> bool:
    if not name or "." not in name:
        return False
    ext = name.rsplit(".", 1)[-1].lower()
    return ext in ALLOWED


@pipeline_bp.route("/start", methods=["POST"])
def pipeline_start():
    """
    multipart/form-data:
      - file: .txt (or .md) reality seed
      - simulation_requirement: string

    When Supabase sync is enabled, send Authorization: Bearer <session JWT>
    from POST /api/auth/login (or a Supabase Auth JWT if SUPABASE_JWT_SECRET is set).

    Returns { task_id, simulation_run_id? }.
    """
    try:
        simulation_requirement = ""
        document_text = ""
        filename = "seed.txt"

        if request.content_type and "multipart/form-data" in request.content_type:
            simulation_requirement = (request.form.get("simulation_requirement") or "").strip()
            f = request.files.get("file")
            if not f or not f.filename:
                return jsonify({"success": False, "error": "Missing file"}), 400
            if not _allowed(f.filename):
                return jsonify(
                    {"success": False, "error": "Only .txt or .md files are allowed for this endpoint"}
                ), 400
            filename = f.filename
            raw = f.read()
            try:
                document_text = raw.decode("utf-8")
            except UnicodeDecodeError:
                document_text = raw.decode("utf-8", errors="replace")
        else:
            data = request.get_json(silent=True) or {}
            simulation_requirement = (data.get("simulation_requirement") or "").strip()
            document_text = (data.get("document_text") or "").strip()
            filename = (data.get("filename") or "seed.txt").strip() or "seed.txt"
            if not document_text:
                return jsonify({"success": False, "error": "document_text is required"}), 400

        if not simulation_requirement:
            return jsonify({"success": False, "error": "simulation_requirement is required"}), 400
        if len(document_text.strip()) < 10:
            return jsonify({"success": False, "error": "Document text is too short"}), 400

        user_id = None
        simulation_run_id = None
        if sync_enabled():
            uid, err = resolve_pipeline_user_id()
            if err or not uid:
                return jsonify({"success": False, "error": err or "Unauthorized"}), 401
            user_id = uid
            simulation_run_id = str(uuid.uuid4())

        tm = TaskManager()
        task_id = tm.create_task(
            task_type="full_pipeline",
            metadata={
                "filename": filename,
                "simulation_run_id": simulation_run_id,
                "user_id": user_id,
            },
        )
        tm.update_task(
            task_id,
            status=TaskStatus.PROCESSING,
            progress=1,
            message="Pipeline queued…",
        )

        if sync_enabled() and simulation_run_id and user_id:
            try:
                insert_pending_run(
                    simulation_run_id=simulation_run_id,
                    user_id=user_id,
                    backend_task_id=task_id,
                    requirement_preview=simulation_requirement,
                )
            except Exception as e:
                logger.error("Supabase insert_pending_run: %s", e)
                tm.fail_task(task_id, f"Could not record run: {e}")
                return jsonify({"success": False, "error": str(e)}), 500

        def worker():
            run_full_pipeline(
                simulation_requirement=simulation_requirement,
                document_text=document_text,
                source_filename=filename,
                task_id=task_id,
                task_manager=tm,
                simulation_run_id=simulation_run_id,
                user_id=user_id,
            )

        threading.Thread(target=worker, daemon=True).start()

        payload = {
            "task_id": task_id,
            "message": "Simulation started. Progress is tracked in your Simulations list when cloud sync is enabled.",
        }
        if simulation_run_id:
            payload["simulation_run_id"] = simulation_run_id

        return jsonify({"success": True, "data": payload})
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@pipeline_bp.route("/status/<task_id>", methods=["GET"])
def pipeline_status(task_id: str):
    tm = TaskManager()
    task = tm.get_task(task_id)
    if not task:
        return jsonify({"success": False, "error": "Unknown task_id"}), 404
    out = task.to_dict()
    return jsonify({"success": True, "data": out})


@pipeline_bp.route("/runs", methods=["GET"])
def pipeline_list_runs():
    """List simulation_runs for the authenticated user (service role query on server)."""
    try:
        if not sync_enabled():
            return jsonify({"success": True, "data": {"runs": []}})
        uid, err = resolve_pipeline_user_id()
        if err or not uid:
            return jsonify({"success": False, "error": err or "Unauthorized"}), 401
        runs = list_simulation_runs_for_user(user_id=uid)
        return jsonify({"success": True, "data": {"runs": runs}})
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@pipeline_bp.route("/runs/<run_id>/signed-download", methods=["GET"])
def pipeline_signed_report_download(run_id: str):
    """Return a time-limited signed URL for the report object in Storage."""
    try:
        if not sync_enabled():
            return jsonify({"success": False, "error": "Sync not configured"}), 503
        uid, err = resolve_pipeline_user_id()
        if err or not uid:
            return jsonify({"success": False, "error": err or "Unauthorized"}), 401
        row = get_run_row_for_user(run_id=run_id, user_id=uid)
        if not row:
            return jsonify({"success": False, "error": "Run not found"}), 404
        path = row.get("report_storage_path") or ""
        if not path:
            return jsonify({"success": False, "error": "Report not ready"}), 400
        url = signed_url_for_storage_path(storage_path=path, expires_sec=3600)
        if not url:
            return jsonify({"success": False, "error": "Could not create download link"}), 500
        return jsonify({"success": True, "data": {"signed_url": url}})
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500
