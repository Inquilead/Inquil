"""
End-to-end pipeline: ontology → graph → simulation create → prepare → run → report.
Executes inside a worker thread (see api/pipeline.py).
"""

from __future__ import annotations

import time
import traceback
import uuid
from typing import Any, Dict, Optional

from ..config import Config
from ..models.project import ProjectManager, ProjectStatus
from ..models.task import TaskManager, TaskStatus
from ..services.graph_builder import GraphBuilderService
from ..services.ontology_generator import OntologyGenerator
from ..services.report_agent import ReportAgent, ReportManager, ReportStatus
from ..services.simulation_manager import SimulationManager, SimulationStatus
from ..services.simulation_runner import SimulationRunner, RunnerStatus
from ..services.text_processor import TextProcessor
from ..utils.logger import get_logger

logger = get_logger("mirofish.pipeline")


def _update(
    tm: TaskManager,
    task_id: str,
    progress: int,
    message: str,
    *,
    stage: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    detail: Dict[str, Any] = {"stage": stage}
    if extra:
        detail.update(extra)
    tm.update_task(
        task_id,
        status=TaskStatus.PROCESSING,
        progress=max(0, min(100, progress)),
        message=message,
        progress_detail=detail,
    )


def _run_graph_build_sync(
    project_id: str,
    tm: TaskManager,
    pipeline_task_id: str,
) -> str:
    project = ProjectManager.get_project(project_id)
    if not project or not project.ontology:
        raise RuntimeError("Project missing ontology")

    text = ProjectManager.get_extracted_text(project_id)
    if not text:
        raise RuntimeError("No extracted text for project")

    if not Config.ZEP_API_KEY:
        raise RuntimeError("ZEP_API_KEY is not configured")

    graph_name = project.name or "MiroFish Graph"
    chunk_size = project.chunk_size or Config.DEFAULT_CHUNK_SIZE
    chunk_overlap = project.chunk_overlap or Config.DEFAULT_CHUNK_OVERLAP

    project.status = ProjectStatus.GRAPH_BUILDING
    ProjectManager.save_project(project)

    builder = GraphBuilderService(api_key=Config.ZEP_API_KEY)

    _update(tm, pipeline_task_id, 16, "Chunking document…", stage="graph")
    chunks = TextProcessor.split_text(text, chunk_size=chunk_size, overlap=chunk_overlap)
    total_chunks = len(chunks)

    _update(
        tm,
        pipeline_task_id,
        18,
        f"Creating Zep graph ({total_chunks} chunks)…",
        stage="graph",
    )
    graph_id = builder.create_graph(name=graph_name)
    project.graph_id = graph_id
    ProjectManager.save_project(project)

    _update(tm, pipeline_task_id, 20, "Applying ontology to graph…", stage="graph", extra={"graph_id": graph_id})
    builder.set_ontology(graph_id, project.ontology)

    def add_progress(msg: str, ratio: float) -> None:
        p = 20 + int(ratio * 22)  # 20–42
        _update(tm, pipeline_task_id, p, msg, stage="graph", extra={"graph_id": graph_id})

    episode_uuids = builder.add_text_batches(
        graph_id, chunks, batch_size=3, progress_callback=add_progress
    )

    _update(tm, pipeline_task_id, 43, "Waiting for Zep to process episodes…", stage="graph")

    def wait_progress(msg: str, ratio: float) -> None:
        p = 43 + int(ratio * 12)  # 43–55
        _update(tm, pipeline_task_id, p, msg, stage="graph", extra={"graph_id": graph_id})

    builder._wait_for_episodes(episode_uuids, wait_progress, timeout=900)

    project.status = ProjectStatus.GRAPH_COMPLETED
    ProjectManager.save_project(project)

    _update(tm, pipeline_task_id, 55, "Graph build complete.", stage="graph", extra={"graph_id": graph_id})
    return graph_id


def run_full_pipeline(
    *,
    simulation_requirement: str,
    document_text: str,
    source_filename: str,
    task_id: str,
    task_manager: TaskManager,
) -> None:
    """
    Full automated run. Updates task_manager task_id; on success completes with result dict.
    """
    tm = task_manager
    project_id: Optional[str] = None
    simulation_id: Optional[str] = None
    report_id = f"report_{uuid.uuid4().hex[:12]}"

    max_rounds: Optional[int] = None
    if Config.PIPELINE_MAX_ROUNDS:
        try:
            max_rounds = int(Config.PIPELINE_MAX_ROUNDS)
        except ValueError:
            max_rounds = None

    parallel_profiles = Config.PIPELINE_PARALLEL_PROFILES
    sim_timeout = Config.PIPELINE_SIMULATION_TIMEOUT_SEC

    try:
        _update(tm, task_id, 2, "Creating project…", stage="ontology")

        project = ProjectManager.create_project(name=f"Pipeline {source_filename[:40]}")
        project_id = project.project_id
        project.simulation_requirement = simulation_requirement
        project.files.append({"filename": source_filename, "size": len(document_text.encode("utf-8"))})
        ProjectManager.save_extracted_text(project_id, document_text)
        project.total_text_length = len(document_text)
        ProjectManager.save_project(project)

        _update(
            tm,
            task_id,
            5,
            "Generating ontology (LLM)…",
            stage="ontology",
            extra={"project_id": project_id},
        )
        generator = OntologyGenerator()
        doc_label = f"=== {source_filename} ===\n{document_text}"
        ontology = generator.generate(
            document_texts=[document_text],
            simulation_requirement=simulation_requirement,
            additional_context=None,
        )
        project.ontology = {
            "entity_types": ontology.get("entity_types", []),
            "edge_types": ontology.get("edge_types", []),
        }
        project.analysis_summary = ontology.get("analysis_summary", "")
        project.status = ProjectStatus.ONTOLOGY_GENERATED
        ProjectManager.save_project(project)

        graph_id = _run_graph_build_sync(project_id, tm, task_id)

        _update(
            tm,
            task_id,
            56,
            "Creating simulation instance…",
            stage="simulation_create",
            extra={"project_id": project_id, "graph_id": graph_id},
        )
        manager = SimulationManager()
        state = manager.create_simulation(
            project_id=project_id,
            graph_id=graph_id,
            enable_twitter=True,
            enable_reddit=True,
        )
        simulation_id = state.simulation_id

        document_text_full = ProjectManager.get_extracted_text(project_id) or document_text

        def prepare_progress(stage: str, prog: int, message: str, **kwargs: Any) -> None:
            weights = {
                "reading": (56, 60),
                "generating_profiles": (60, 68),
                "generating_config": (68, 71),
                "copying_scripts": (71, 72),
            }
            lo, hi = weights.get(stage, (56, 72))
            pct = int(lo + (hi - lo) * (prog / 100.0))
            _update(
                tm,
                task_id,
                pct,
                message,
                stage="prepare",
                extra={"project_id": project_id, "simulation_id": simulation_id, "graph_id": graph_id},
            )

        _update(
            tm,
            task_id,
            57,
            "Preparing agents and simulation config (may take a long time)…",
            stage="prepare",
            extra={"project_id": project_id, "simulation_id": simulation_id},
        )
        prepared = manager.prepare_simulation(
            simulation_id=simulation_id,
            simulation_requirement=simulation_requirement,
            document_text=document_text_full,
            defined_entity_types=None,
            use_llm_for_profiles=True,
            progress_callback=prepare_progress,
            parallel_profile_count=parallel_profiles,
        )
        if prepared.status == SimulationStatus.FAILED:
            raise RuntimeError(prepared.error or "Simulation prepare failed (no entities or setup error)")

        _update(
            tm,
            task_id,
            73,
            "Starting OASIS simulation (parallel platforms)…",
            stage="run",
            extra={"project_id": project_id, "simulation_id": simulation_id},
        )
        SimulationRunner.start_simulation(
            simulation_id=simulation_id,
            platform="parallel",
            max_rounds=max_rounds,
            enable_graph_memory_update=False,
            graph_id=None,
        )

        deadline = time.time() + sim_timeout
        last_prog = 73
        while time.time() < deadline:
            rs = SimulationRunner.get_run_state(simulation_id)
            if not rs:
                time.sleep(3)
                continue
            status = rs.runner_status
            if status == RunnerStatus.COMPLETED:
                break
            if status in (RunnerStatus.FAILED, RunnerStatus.STOPPED):
                raise RuntimeError(rs.error or f"Simulation ended with status {status.value}")
            last_prog = min(88, last_prog + 1)
            _update(
                tm,
                task_id,
                last_prog,
                f"Simulation running: round ~{rs.twitter_current_round or rs.reddit_current_round or rs.current_round}…",
                stage="run",
                extra={"project_id": project_id, "simulation_id": simulation_id},
            )
            time.sleep(5)
        else:
            raise RuntimeError("Simulation timed out waiting for completion")

        _update(
            tm,
            task_id,
            90,
            "Generating English validation report (LLM)…",
            stage="report",
            extra={
                "project_id": project_id,
                "simulation_id": simulation_id,
                "report_id": report_id,
            },
        )

        agent = ReportAgent(
            graph_id=graph_id,
            simulation_id=simulation_id,
            simulation_requirement=simulation_requirement,
        )

        def report_progress(stage: str, progress: int, message: str) -> None:
            p = 90 + int(progress * 0.09)  # 90–99
            _update(
                tm,
                task_id,
                min(99, p),
                f"[{stage}] {message}",
                stage="report",
                extra={
                    "project_id": project_id,
                    "simulation_id": simulation_id,
                    "report_id": report_id,
                },
            )

        report = agent.generate_report(
            progress_callback=report_progress,
            report_id=report_id,
        )
        ReportManager.save_report(report)

        if report.status != ReportStatus.COMPLETED:
            raise RuntimeError(report.error or "Report generation failed")

        tm.update_task(
            task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Pipeline complete — report ready.",
            result={
                "project_id": project_id,
                "graph_id": graph_id,
                "simulation_id": simulation_id,
                "report_id": report.report_id,
            },
            progress_detail={
                "stage": "completed",
                "project_id": project_id,
                "graph_id": graph_id,
                "simulation_id": simulation_id,
                "report_id": report.report_id,
            },
        )
        logger.info(
            "Pipeline finished: project=%s sim=%s report=%s",
            project_id,
            simulation_id,
            report.report_id,
        )

    except Exception as e:
        logger.error("Pipeline failed: %s", traceback.format_exc())
        tm.fail_task(task_id, str(e))
