"""
Lattice entry point.

Startup sequence:
  1. Validate configuration (fail fast with a clear error).
  2. Configure structured logging (JSON in production, pretty in dev).
  3. Connect to Redis — verify with PING.
  4. Open SQLite store — reconcile orphaned jobs.
  5. Start worker pool.
  6. Start scheduler loop.
  7. Start gRPC server (best-effort; requires compiled proto stubs).
  8. Start FastAPI/uvicorn REST server (blocks until shutdown signal).
  9. Graceful drain on shutdown.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import structlog
import uvicorn
import redis.asyncio as aioredis
from redis.exceptions import RedisError

# ── Fix sys.path so `config` resolves from the project root ──────────────────
# The project root is two levels up from this file:
#   lattice/lattice/main.py  →  project root = lattice/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import settings  # noqa: E402  (must come after sys.path fix)
from lattice.store.job_store import JobStore, LatticeStoreError
from lattice.store.redis_queue import RedisJobQueue
from lattice.worker.docker_worker import DockerWorkerManager
from lattice.worker.pool import WorkerPool
from lattice.scheduler import Scheduler
from lattice.api.rest_api import (
    app as rest_app,
    set_scheduler as set_rest_scheduler,
    set_api_keys,
)
from lattice.api.grpc_server import (
    serve_grpc,
    set_scheduler as set_grpc_scheduler,
)


# ── Logging setup ─────────────────────────────────────────────────────────────


def _configure_logging() -> None:
    """
    Configure structlog for the chosen environment.

    production  → JSON lines, machine-readable for Datadog/Loki/Splunk.
    development → Coloured console output.
    """
    log_level = getattr(logging, settings.log_level().upper(), logging.INFO)
    log_format = settings.log_format()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if log_format == "production":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging so uvicorn/grpcio logs are captured
    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        stream=sys.stdout,
    )


# ── Main ──────────────────────────────────────────────────────────────────────


async def main() -> None:
    _configure_logging()
    log = structlog.get_logger("lattice.main")

    # ── 1. Validate configuration ─────────────────────────────────────────
    try:
        settings.validate_all()
    except ValueError as exc:
        log.error("lattice.config_invalid", error=str(exc))
        sys.exit(1)

    log.info(
        "lattice.starting",
        algorithm=settings.scheduler_algorithm(),
        max_workers=settings.max_workers(),
        rest_port=settings.rest_port(),
        grpc_port=settings.grpc_port(),
    )

    # ── 2. Redis connection ───────────────────────────────────────────────
    redis_client = aioredis.from_url(
        settings.redis_url(),
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )
    try:
        await redis_client.ping()
        log.info("lattice.redis_ok")
    except RedisError as exc:
        log.error("lattice.redis_unavailable", error=str(exc))
        log.warning(
            "lattice.redis_warn",
            msg="Continuing in degraded mode — queue operations will retry",
        )

    # ── 3. Job store ──────────────────────────────────────────────────────
    try:
        store = JobStore(db_path=settings.sqlite_path())
        log.info("lattice.sqlite_ok", path=settings.sqlite_path())
    except LatticeStoreError as exc:
        log.error("lattice.sqlite_failed", error=str(exc))
        sys.exit(1)

    queue = RedisJobQueue(redis_client)

    # ── 4. Worker pool ────────────────────────────────────────────────────
    docker_mgr = DockerWorkerManager(
        max_workers=settings.max_workers(),
        cpu_limit=settings.worker_cpu_limit(),
        memory_limit=settings.worker_memory_limit(),
        image=settings.worker_image(),
    )
    pool = WorkerPool(
        docker_manager=docker_mgr,
        heartbeat_timeout_seconds=settings.heartbeat_timeout(),
    )

    try:
        workers = await asyncio.wait_for(
            pool.initialise(settings.max_workers()),
            timeout=30.0,
        )
        log.info("lattice.pool_ready", workers=len(workers))
    except asyncio.TimeoutError:
        log.error(
            "lattice.pool_timeout",
            msg="Worker pool initialisation timed out. Check Docker availability.",
        )
        sys.exit(1)

    # ── 5. Scheduler ──────────────────────────────────────────────────────
    scheduler = Scheduler(
        job_store=store,
        job_queue=queue,
        worker_pool=pool,
        algorithm=settings.scheduler_algorithm(),
        tick_interval_ms=settings.tick_interval_ms(),
        cluster_cpu=settings.cluster_cpu(),
        cluster_mem=settings.cluster_mem(),
        preemption_enabled=settings.preemption_enabled(),
        backfill_enabled=settings.backfill_enabled(),
        gang_scheduling_enabled=settings.gang_scheduling_enabled(),
    )

    # Inject scheduler into API layers
    set_rest_scheduler(scheduler)
    set_grpc_scheduler(scheduler)
    set_api_keys(settings.api_keys())

    await scheduler.start()
    log.info("scheduler.running", algorithm=settings.scheduler_algorithm())

    # ── 6. gRPC server ────────────────────────────────────────────────────
    grpc_task = asyncio.create_task(
        serve_grpc(port=settings.grpc_port()),
        name="grpc-server",
    )

    def _grpc_done(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            log.error("grpc.server_crashed", error=str(exc))

    grpc_task.add_done_callback(_grpc_done)

    # ── 7. REST server ────────────────────────────────────────────────────
    config = uvicorn.Config(
        rest_app,
        host=settings.api_host(),
        port=settings.rest_port(),
        log_level=settings.log_level().lower(),
        loop="asyncio",
        access_log=False,  # handled by our middleware
    )
    server = uvicorn.Server(config)

    log.info(
        "lattice.ready",
        rest_port=settings.rest_port(),
        grpc_port=settings.grpc_port(),
        auth_enabled=bool(settings.api_keys()),
    )

    try:
        await server.serve()
    finally:
        log.info("lattice.shutting_down")
        await scheduler.stop()

        grpc_task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(grpc_task), timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        await redis_client.aclose()
        log.info("lattice.shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
