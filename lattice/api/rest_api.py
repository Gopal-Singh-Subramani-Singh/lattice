"""
Lattice FastAPI REST admin API.

Security:
  - Optional API key auth via X-API-Key header (set LATTICE_API_KEYS env var).
  - If LATTICE_API_KEYS is empty, auth is disabled (development only).

Observability:
  - Every request is logged with method, path, status, and duration.
  - Request IDs are injected and returned in X-Request-ID.
  - /health checks Redis + SQLite + scheduler loop health.
  - /ready is a separate readiness probe (for Kubernetes).

Error handling:
  - LatticeStoreError and RedisError are caught and mapped to 503.
  - Validation errors produce 422 with structured detail.
  - Unexpected errors produce 500 without leaking tracebacks.
"""
from __future__ import annotations

import time
import uuid
from typing import List, Optional

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from redis.exceptions import RedisError

from lattice.models import (
    Job, JobState, Priority, ResourceSpec,
    JobSubmitRequest, JobSubmitResponse,
    JobStatusResponse, ClusterStatusResponse,
)
from lattice.store.job_store import LatticeStoreError
from lattice.metrics import update_uptime

logger = structlog.get_logger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Lattice Scheduler",
    description="Distributed ML Job Scheduler REST API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

_scheduler = None      # injected by main.py
_api_keys: List[str] = []  # injected by main.py


def set_scheduler(s) -> None:
    global _scheduler
    _scheduler = s


def set_api_keys(keys: List[str]) -> None:
    global _api_keys
    _api_keys = keys


# ── Middleware ────────────────────────────────────────────────────────────────


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log every request with duration and inject a request ID."""
    req_id = str(uuid.uuid4())
    request.state.request_id = req_id
    start = time.monotonic()

    response = await call_next(request)

    duration_ms = round((time.monotonic() - start) * 1000, 1)
    logger.info(
        "api.request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=duration_ms,
        request_id=req_id,
    )
    response.headers["X-Request-ID"] = req_id
    return response


# ── Error handlers ────────────────────────────────────────────────────────────


@app.exception_handler(LatticeStoreError)
async def store_error_handler(request: Request, exc: LatticeStoreError):
    logger.error("api.store_error", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily unavailable. Please retry."},
    )


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError):
    logger.error("api.redis_error", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=503,
        content={"detail": "Queue temporarily unavailable. Please retry."},
    )


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception):
    logger.error(
        "api.unhandled_error",
        path=request.url.path,
        error=str(exc),
        exc_type=type(exc).__name__,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error."},
    )


# ── Auth dependency ───────────────────────────────────────────────────────────


def _require_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """
    Validate API key from X-API-Key header.
    Skipped if no API keys are configured (development mode).
    """
    if not _api_keys:
        return  # auth disabled
    if x_api_key not in _api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


def _check_scheduler():
    """Raise 503 if the scheduler is not initialised."""
    if _scheduler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scheduler not initialised.",
        )


# ── Job endpoints ─────────────────────────────────────────────────────────────


@app.post(
    "/jobs",
    response_model=JobSubmitResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(_require_api_key)],
    summary="Submit a new ML job",
)
async def submit_job(req: JobSubmitRequest):
    """Submit a job to the scheduler. Fields are validated before enqueueing."""
    _check_scheduler()

    job = Job(
        job_id=str(uuid.uuid4()),
        team=req.team,
        name=req.name,
        priority=req.priority,
        resources=ResourceSpec(
            cpu_cores=req.cpu_cores,
            memory_gb=req.memory_gb,
        ),
        num_workers=req.num_workers,
        max_retries=req.max_retries,
        estimated_duration_seconds=req.estimated_duration_seconds,
        labels=req.labels,
    )

    job_id = await _scheduler.submit_job(job)
    logger.info("api.job_submitted", job_id=job_id, team=req.team)
    return JobSubmitResponse(job_id=job_id, accepted=True, message="Job accepted")


@app.delete(
    "/jobs/{job_id}",
    response_model=dict,
    dependencies=[Depends(_require_api_key)],
    summary="Cancel a job",
)
async def cancel_job(job_id: str):
    """Cancel a pending or running job by ID."""
    _check_scheduler()

    success = await _scheduler.cancel_job(job_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found or already in terminal state.",
        )
    return {"job_id": job_id, "cancelled": True}


@app.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    dependencies=[Depends(_require_api_key)],
    summary="Get job status",
)
async def get_job(job_id: str):
    """Return the current status of a specific job."""
    _check_scheduler()

    job = _scheduler._store.get(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )
    return _job_to_response(job)


@app.get(
    "/jobs",
    response_model=List[JobStatusResponse],
    dependencies=[Depends(_require_api_key)],
    summary="List jobs",
)
async def list_jobs(
    team: Optional[str]  = Query(None, description="Filter by team name"),
    state: Optional[str] = Query(None, description="Filter by state"),
    limit: int           = Query(100, ge=1, le=1000),
):
    """List jobs with optional team/state filters."""
    _check_scheduler()

    try:
        jobs = _scheduler._store.list_jobs(team=team, state=state, limit=limit)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )
    return [_job_to_response(j) for j in jobs]


# ── Cluster endpoints ─────────────────────────────────────────────────────────


@app.get(
    "/cluster",
    response_model=ClusterStatusResponse,
    dependencies=[Depends(_require_api_key)],
    summary="Cluster status",
)
async def cluster_status():
    """Return current worker utilisation and DRF team shares."""
    _check_scheduler()

    snap    = _scheduler.get_cluster_snapshot()
    pending = await _scheduler._queue.depth()
    return ClusterStatusResponse(
        total_workers=snap.total_workers,
        idle_workers=snap.idle_workers,
        busy_workers=snap.busy_workers,
        utilisation_pct=snap.utilisation_pct,
        pending_jobs=pending,
        running_jobs=snap.running_jobs,
        team_shares=snap.team_dominant_shares,
    )


# ── Observability endpoints ───────────────────────────────────────────────────


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Prometheus metrics endpoint."""
    update_uptime()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", summary="Liveness probe")
async def health():
    """
    Liveness probe. Returns 200 if the process is alive.
    Checks: scheduler loop running, Redis reachable, SQLite reachable.
    """
    checks: dict = {"status": "ok"}
    http_status = 200

    # Scheduler loop
    if _scheduler is not None:
        checks["scheduler"] = "ok" if _scheduler.is_healthy else "degraded"
        if not _scheduler.is_healthy:
            checks["status"] = "degraded"
            http_status = 503
    else:
        checks["scheduler"] = "not_initialised"
        checks["status"] = "starting"
        http_status = 503

    # Redis
    if _scheduler is not None:
        redis_ok = await _scheduler._queue.ping()
        checks["redis"] = "ok" if redis_ok else "unreachable"
        if not redis_ok:
            checks["status"] = "degraded"
            http_status = 503

    # SQLite
    if _scheduler is not None:
        db_ok = _scheduler._store.health_check()
        checks["sqlite"] = "ok" if db_ok else "unreachable"
        if not db_ok:
            checks["status"] = "degraded"
            http_status = 503

    checks["uptime"] = round(update_uptime(), 1)
    return JSONResponse(content=checks, status_code=http_status)


@app.get("/ready", summary="Readiness probe")
async def ready():
    """
    Readiness probe. Returns 200 only when fully ready to serve traffic.
    Distinct from /health (liveness) — used by Kubernetes readiness checks.
    """
    if _scheduler is None or not _scheduler.is_healthy:
        return JSONResponse(
            content={"ready": False, "reason": "Scheduler not ready"},
            status_code=503,
        )
    return JSONResponse(content={"ready": True})


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "Lattice", "version": "0.1.0"}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _job_to_response(job: Job) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=job.job_id,
        state=job.state.value,
        team=job.team,
        name=job.name,
        priority=job.priority.value,
        worker_ids=job.worker_ids,
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        retry_count=job.retry_count,
        message=job.message,
    )
