"""
gRPC service implementation for Lattice.

Implements: Submit, Cancel, Status, List, StreamEvents, ClusterStats.
Requires compiled proto stubs in lattice/proto_gen/ (run scripts/generate_proto.sh).

Error handling:
  - All RPC methods catch exceptions and map them to gRPC status codes.
  - INVALID_ARGUMENT for bad input.
  - NOT_FOUND for missing resources.
  - INTERNAL for unexpected errors.
  - UNAVAILABLE when the scheduler is not initialised.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Optional
import structlog

from lattice.models import Job, JobState, Priority, ResourceSpec

logger = structlog.get_logger(__name__)

_scheduler = None  # injected by main.py

# ── Proto import ──────────────────────────────────────────────────────────────

try:
    from lattice.proto_gen import lattice_pb2, lattice_pb2_grpc
    import grpc
    _PROTO_AVAILABLE = True
    logger.debug("grpc.stubs_loaded")
except ImportError as _e:
    _PROTO_AVAILABLE = False
    logger.warning(
        "grpc.stubs_not_found",
        hint="Run: ./scripts/generate_proto.sh",
        error=str(_e),
    )


def set_scheduler(s) -> None:
    global _scheduler
    _scheduler = s


# ── gRPC servicer ─────────────────────────────────────────────────────────────

if _PROTO_AVAILABLE:

    class LatticeSchedulerServicer(lattice_pb2_grpc.LatticeSchedulerServicer):
        """Full gRPC service implementation."""

        async def Submit(self, request, context):
            if _scheduler is None:
                await context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "Scheduler not initialised",
                )
                return

            spec = request.spec
            if not spec.team or not spec.name:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "team and name are required",
                )
                return

            try:
                job = Job(
                    job_id=spec.job_id or str(uuid.uuid4()),
                    team=spec.team,
                    name=spec.name,
                    priority=Priority(spec.priority),
                    resources=ResourceSpec(
                        cpu_cores=spec.resources.cpu_cores or 2.0,
                        memory_gb=spec.resources.memory_gb or 4.0,
                        gpu_count=spec.resources.gpu_count,
                    ),
                    num_workers=max(spec.num_workers or 1, 1),
                    max_retries=min(spec.max_retries, 10),
                    estimated_duration_seconds=spec.estimated_duration_seconds or 300,
                    checkpoint_path=spec.checkpoint_path or None,
                    labels=dict(spec.labels),
                )
                job_id = await _scheduler.submit_job(job)
                logger.info(
                    "grpc.submit",
                    job_id=job_id,
                    team=spec.team,
                )
                return lattice_pb2.SubmitResponse(
                    job_id=job_id,
                    accepted=True,
                    message="Accepted",
                )
            except Exception as exc:
                logger.error("grpc.submit_error", error=str(exc))
                await context.abort(
                    grpc.StatusCode.INTERNAL,
                    f"Submit failed: {exc}",
                )

        async def Cancel(self, request, context):
            if _scheduler is None:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Not ready")
                return
            if not request.job_id:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "job_id is required"
                )
                return

            try:
                success = await _scheduler.cancel_job(request.job_id)
                if not success:
                    await context.abort(
                        grpc.StatusCode.NOT_FOUND,
                        f"Job '{request.job_id}' not found or already terminal",
                    )
                    return
                return lattice_pb2.CancelResponse(
                    success=True, message="Cancelled"
                )
            except Exception as exc:
                logger.error("grpc.cancel_error", error=str(exc))
                await context.abort(grpc.StatusCode.INTERNAL, str(exc))

        async def Status(self, request, context):
            if _scheduler is None:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Not ready")
                return
            if not request.job_id:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "job_id is required"
                )
                return

            try:
                job = _scheduler._store.get(request.job_id)
                if not job:
                    await context.abort(
                        grpc.StatusCode.NOT_FOUND,
                        f"Job '{request.job_id}' not found",
                    )
                    return
                return lattice_pb2.StatusResponse(status=_job_to_proto(job))
            except Exception as exc:
                logger.error("grpc.status_error", error=str(exc))
                await context.abort(grpc.StatusCode.INTERNAL, str(exc))

        async def List(self, request, context):
            if _scheduler is None:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Not ready")
                return

            try:
                team  = request.team  or None
                state = request.state or None
                limit = min(request.limit or 100, 1000)
                jobs  = _scheduler._store.list_jobs(
                    team=team, state=state, limit=limit
                )
                return lattice_pb2.ListResponse(
                    jobs=[_job_to_proto(j) for j in jobs]
                )
            except ValueError as exc:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            except Exception as exc:
                logger.error("grpc.list_error", error=str(exc))
                await context.abort(grpc.StatusCode.INTERNAL, str(exc))

        async def StreamEvents(self, request, context):
            """Stream job state changes until terminal or timeout (5 min)."""
            if not request.job_id:
                return

            job_id     = request.job_id
            last_state = None
            timeout    = 300
            start      = time.time()
            state_map  = {
                JobState.PENDING:   0,
                JobState.RESERVED:  1,
                JobState.RUNNING:   2,
                JobState.COMPLETED: 3,
                JobState.FAILED:    4,
                JobState.CANCELLED: 5,
                JobState.PREEMPTED: 6,
            }

            while time.time() - start < timeout:
                try:
                    job = (_scheduler._store.get(job_id) if _scheduler else None)
                except Exception:
                    job = None

                if job and job.state != last_state:
                    last_state = job.state
                    yield lattice_pb2.JobEvent(
                        job_id=job_id,
                        state=state_map.get(job.state, 0),
                        message=job.message,
                        timestamp=int(time.time()),
                    )
                    if job.state in (
                        JobState.COMPLETED,
                        JobState.FAILED,
                        JobState.CANCELLED,
                    ):
                        break

                await asyncio.sleep(1.0)

        async def ClusterStats(self, request, context):
            if _scheduler is None:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Not ready")
                return
            try:
                snap    = _scheduler.get_cluster_snapshot()
                pending = await _scheduler._queue.depth()
                return lattice_pb2.ClusterStatus(
                    total_workers=snap.total_workers,
                    idle_workers=snap.idle_workers,
                    busy_workers=snap.busy_workers,
                    utilisation_pct=snap.utilisation_pct,
                    pending_jobs=pending,
                    running_jobs=snap.running_jobs,
                    team_dominant_shares=snap.team_dominant_shares,
                )
            except Exception as exc:
                logger.error("grpc.cluster_error", error=str(exc))
                await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    # ── Proto helpers ──────────────────────────────────────────────────────────

    _STATE_MAP = {
        JobState.PENDING:   0,
        JobState.RESERVED:  1,
        JobState.RUNNING:   2,
        JobState.COMPLETED: 3,
        JobState.FAILED:    4,
        JobState.CANCELLED: 5,
        JobState.PREEMPTED: 6,
    }

    def _job_to_proto(job: Job) -> "lattice_pb2.JobStatus":
        return lattice_pb2.JobStatus(
            job_id=job.job_id,
            state=_STATE_MAP.get(job.state, 0),
            team=job.team,
            name=job.name,
            priority=job.priority.value,
            worker_ids=job.worker_ids,
            submitted_at=(
                int(job.submitted_at.timestamp()) if job.submitted_at else 0
            ),
            started_at=(
                int(job.started_at.timestamp()) if job.started_at else 0
            ),
            finished_at=(
                int(job.finished_at.timestamp()) if job.finished_at else 0
            ),
            message=job.message,
            retry_count=job.retry_count,
            checkpoint_path=job.checkpoint_path or "",
        )


# ── Server bootstrap ──────────────────────────────────────────────────────────


async def serve_grpc(port: int = 50051) -> None:
    """Start the gRPC server. No-op if proto stubs are not compiled."""
    if not _PROTO_AVAILABLE:
        logger.warning(
            "grpc.server_disabled",
            reason="Proto stubs not compiled",
            hint="Run: ./scripts/generate_proto.sh",
        )
        # Keep the task alive so it doesn't crash the done-callback
        await asyncio.Event().wait()
        return

    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", 10 * 1024 * 1024),
            ("grpc.max_receive_message_length", 10 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
        ]
    )
    lattice_pb2_grpc.add_LatticeSchedulerServicer_to_server(
        LatticeSchedulerServicer(), server
    )
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    logger.info("grpc.server_started", port=port)

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        await server.stop(grace=5)
        logger.info("grpc.server_stopped")
