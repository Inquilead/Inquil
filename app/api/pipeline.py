"""
One-shot pipeline API for the website: upload text + requirement → full automation → report.
"""

import threading
import traceback

from flask import request, jsonify

from . import pipeline_bp
from ..models.task import TaskManager, TaskStatus
from ..services.pipeline_orchestrator import run_full_pipeline
from ..utils.logger import get_logger

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

    Or JSON:
      { "document_text": "...", "simulation_requirement": "...", "filename": "seed.txt" }

    Returns { task_id } for polling GET /api/pipeline/status/<task_id>.
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

        tm = TaskManager()
        task_id = tm.create_task(
            task_type="full_pipeline",
            metadata={"filename": filename},
        )
        tm.update_task(
            task_id,
            status=TaskStatus.PROCESSING,
            progress=1,
            message="Pipeline queued…",
        )

        def worker():
            run_full_pipeline(
                simulation_requirement=simulation_requirement,
                document_text=document_text,
                source_filename=filename,
                task_id=task_id,
                task_manager=tm,
            )

        threading.Thread(target=worker, daemon=True).start()

        return jsonify(
            {
                "success": True,
                "data": {
                    "task_id": task_id,
                    "message": "Pipeline started. Poll GET /api/pipeline/status/<task_id>. "
                    "Full runs often take 20–40+ minutes; you can leave this page and check the Simulations section later.",
                },
            }
        )
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
