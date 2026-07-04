"""
Lattice core scheduler engine.

The scheduler runs a tight asyncio loop that:
1. Peeks at the priority queue (Redis Sorted Set).
2. Selects the next job using the configured algorithm.
3. Dispatches the job to idle workers.
4. Handles retries, preemption, and backfill.

Reliability features:
- Exponential back-off when Redis or SQLite is unavailable.
- Orphaned job reconciliation on startup.
- Graceful drain: running jobs are logged on shutdown.
- Watchdog: the loop task is monitored; crashes are reported via metrics.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional
import structlog

from lattice.models import Job, JobState, Priority, ResourceSpec
from lattice.store.job_store import JobStore, LatticeStoreError
from lattice.store.redis_queue import RedisJobQueue
from lattice.worker.pool import WorkerPool
from lattice.algorithms.fifo import assign_workers_for_job
from lattice.algorithms.drf import DRFScheduler
from lattice.algorithms.gang import GangScheduler
from lattice.algorithms.preemption import PreemptionController
from lattice.algorithms.backfill import BackfillScheduler
from lattice.metrics import (
    SCHEDULER_TICKS, JOBS_SUBMITTED, JOBS_COMPLETED,
    QUEUE_DEPTH, JOB_WAIT_TIME, JOB_DURATION, DRF_DOMINANT_SHARE,
    update_uptime,
)

logger = structlog.get_logger(__name__)

_BACKOFF_BASE  = 0.5   # seconds
_BACKOFF_MAX   = 30.0  # seconds
_BACKOFF_RESET = 3     # consecutive clean ticks before resetting backoff


class Scheduler:
    """
    Asynchronous ML job scheduler.

    Usage::

        sched = Scheduler(store, queue, pool, algorithm="drf")
        await sched.start()
        ...
        await sched.stop()
    """

    def __init__(
        self,
        job_store: JobStore,
        job_queue: RedisJobQueue,
        worker_pool: WorkerPool,
        algorithm: str = "drf",
        tick_interval_ms: int = 500,
        cluster_cpu: float = 16.0,
        cluster_mem: float = 32.0,
        preemption_enabled: bool = True,
        backfill_enabled: bool = True,
        gang_scheduling_enabled: bool = True,
    ) -> None:
        self._store = job_store
        self._queue = job_queue
        self._pool  = worker_pool
        self._algorithm = algorithm
        self._tick_interval = tick_interval_ms / 1000.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._drf        = DRFScheduler(cluster_cpu, cluster_mem)
        self._gang       = GangScheduler(worker_pool)
        self._preemption = PreemptionController(job_store, worker_pool)
        self._backfill   = BackfillScheduler()

        self._preemption_enabled = preemption_enabled
        self._backfill_enabled   = backfill_enabled
        self._gang_enabled       = gang_scheduling_enabled

        # In-memory index of currently-running jobs (rebuilt on startup)
        self._running_jobs: Dict[str, Job] = {}

        # Back-off state for error handling
        self._consecutive_errors = 0
        self._clean_ticks = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the scheduler loop.
        Reconciles orphaned jobs before beginning normal operation.
        """
        orphans = self._store.reconcile_orphaned_jobs()
        if orphans:
            logger.warning("scheduler.orphans_reconciled", count=orphans)

        self._running = True
        self._task = asyncio.create_task(self._loop(), name="scheduler-loop")
        self._task.add_done_callback(self._on_loop_done)
        logger.info(
            "scheduler.started",
            algorithm=self._algorithm,
            tick_ms=int(self._tick_interval * 1000),
        )

    async def stop(self) -> None:
        """
        Gracefully stop the scheduler.
        Logs any jobs that were still running so operators can investigate.
        """
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._running_jobs:
            logger.warning(
                "scheduler.stopping_with_running_jobs",
                jobs=list(self._running_jobs.keys()),
            )
        logger.info("scheduler.stopped")

    def _on_loop_done(self, task: asyncio.Task) -> None:
        """Called when the loop task exits (cleanly or via exception)."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(
                "scheduler.loop_crashed",
                error=str(exc),
                exc_info=exc,
            )

    # ── Job submission / cancellation ─────────────────────────────────────────

    async def submit_job(self, job: Job) -> str:
        """
        Persist and enqueue a new job.

        Returns:
            job_id of the accepted job.
        Raises:
            LatticeStoreError: on SQLite failure.
            RedisError:        on Redis failure.
        """
        self._store.save(job)
        await self._queue.enqueue(job)
        JOBS_SUBMITTED.labels(
            team=job.team, priority=job.priority.name
        ).inc()
        logger.info(
            "scheduler.job_submitted",
            job_id=job.job_id,
            team=job.team,
            priority=job.priority.name,
        )
        return job.job_id

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a pending or running job.

        Returns:
            True if cancelled, False if job not found or already terminal.
        """
        job = self._store.get(job_id)
        if not job:
            return False
        if job.state in (
            JobState.COMPLETED, JobState.CANCELLED, JobState.FAILED
        ):
            return False

        if job.state == JobState.RUNNING:
            await self._pool.release_workers(job.worker_ids)
            self._drf.record_job_end(
                job.team,
                job.resources.cpu_cores * job.num_workers,
                job.resources.memory_gb * job.num_workers,
            )
            self._running_jobs.pop(job_id, None)

        await self._queue.remove(job_id)
        self._store.update_state(
            job_id, JobState.CANCELLED, message="Cancelled by user"
        )
        self._store.log_event(job_id, "cancelled")
        logger.info("scheduler.job_cancelled", job_id=job_id)
        return True

    async def complete_job(self, job_id: str, success: bool = True) -> None:
        """
        Mark a job as completed (success) or failed (with optional retry).
        Called by the scheduler when a simulated job finishes.
        """
        job = self._running_jobs.get(job_id) or self._store.get(job_id)
        if not job:
            logger.warning("scheduler.complete_unknown_job", job_id=job_id)
            return

        now = datetime.utcnow()
        job.finished_at = now

        if job.started_at:
            duration = (now - job.started_at).total_seconds()
            JOB_DURATION.labels(team=job.team).observe(duration)

        if not success and job.retry_count < job.max_retries:
            await self._retry_job(job)
            return

        final_state = JobState.COMPLETED if success else JobState.FAILED
        status_label = "completed" if success else "failed"

        await self._pool.release_workers(job.worker_ids)
        self._drf.record_job_end(
            job.team,
            job.resources.cpu_cores * job.num_workers,
            job.resources.memory_gb * job.num_workers,
        )
        self._running_jobs.pop(job_id, None)
        self._store.update_state(job_id, final_state)
        self._store.log_event(job_id, status_label)
        JOBS_COMPLETED.labels(team=job.team, status=status_label).inc()
        logger.info(
            "scheduler.job_complete",
            job_id=job_id,
            status=status_label,
            team=job.team,
        )

    async def _retry_job(self, job: Job) -> None:
        """Re-enqueue a failed job after incrementing its retry counter."""
        self._store.increment_retry(job_id := job.job_id)
        job.retry_count += 1
        job.state = JobState.PENDING
        job.worker_ids = []
        job.started_at = None
        job.finished_at = None

        await self._pool.release_workers(
            self._running_jobs.get(job_id, job).worker_ids
        )
        self._drf.record_job_end(
            job.team,
            job.resources.cpu_cores * job.num_workers,
            job.resources.memory_gb * job.num_workers,
        )
        self._running_jobs.pop(job_id, None)
        await self._queue.enqueue(job)
        self._store.update_state(
            job_id, JobState.PENDING,
            message=f"Retry {job.retry_count}/{job.max_retries}",
        )
        self._store.log_event(
            job_id, "retry",
            f"Retry {job.retry_count}/{job.max_retries}",
        )
        logger.info(
            "scheduler.job_retry",
            job_id=job_id,
            retry=job.retry_count,
            max=job.max_retries,
        )

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        backoff = _BACKOFF_BASE
        while self._running:
            tick_start = time.monotonic()
            try:
                await self._tick()
                update_uptime()
                SCHEDULER_TICKS.inc()
                self._clean_ticks += 1
                if self._clean_ticks >= _BACKOFF_RESET:
                    backoff = _BACKOFF_BASE
                    self._consecutive_errors = 0
                    self._clean_ticks = 0
                await asyncio.sleep(self._tick_interval)

            except asyncio.CancelledError:
                break
            except (LatticeStoreError, Exception) as exc:
                self._consecutive_errors += 1
                self._clean_ticks = 0
                logger.error(
                    "scheduler.tick_error",
                    error=str(exc),
                    consecutive=self._consecutive_errors,
                    backoff=round(backoff, 1),
                )
                await asyncio.sleep(min(backoff, _BACKOFF_MAX))
                backoff = min(backoff * 2, _BACKOFF_MAX)

    async def _tick(self) -> None:
        """Single scheduler iteration."""
        self._pool.evict_stale_workers()
        pending = await self._queue.peek(limit=20)
        idle    = self._pool.idle_workers()
        running = list(self._running_jobs.values())

        # Update Prometheus queue-depth gauges
        depths = await self._queue.depth_by_priority()
        for pname, count in depths.items():
            QUEUE_DEPTH.labels(priority=pname).set(count)

        # Update DRF share gauges
        for team, share in self._drf.all_dominant_shares().items():
            DRF_DOMINANT_SHARE.labels(team=team).set(share)

        if not pending:
            return

        selected = await self._select_job(pending, idle, running)
        if selected is None:
            if self._preemption_enabled and running and pending:
                await self._try_preemption(pending, running)
            return

        job = self._build_job(selected)
        await self._dispatch(job, idle)

    # ── Algorithm selection ───────────────────────────────────────────────────

    async def _select_job(
        self,
        pending: List[dict],
        idle: list,
        running: List[Job],
    ) -> Optional[dict]:
        if self._algorithm == "fifo":
            from lattice.algorithms.fifo import select_next_job_fifo
            return select_next_job_fifo(pending, idle)
        elif self._algorithm == "drf":
            return self._drf.select_next_job(pending, idle)
        else:
            logger.warning(
                "scheduler.unknown_algorithm",
                algorithm=self._algorithm,
                fallback="drf",
            )
            return self._drf.select_next_job(pending, idle)

    # ── Dispatch ──────────────────────────────────────────────────────────────

    async def _dispatch(self, job: Job, idle: list) -> bool:
        """
        Assign workers and start a job.
        Returns True if the job was dispatched, False if it must wait.
        """
        if self._gang_enabled and job.num_workers > 1:
            success, assigned = await self._gang.try_schedule(job, idle)
            if not success:
                if self._backfill_enabled:
                    await self._try_backfill(job, idle)
                return False
        else:
            assigned = assign_workers_for_job(job, idle)
            if not assigned:
                return False
            await self._pool.assign_workers(job, assigned)

        await self._queue.remove(job.job_id)

        job.worker_ids = [w.worker_id for w in assigned]
        job.state      = JobState.RUNNING
        job.started_at = datetime.utcnow()

        self._store.update_state(
            job.job_id,
            JobState.RUNNING,
            worker_ids=job.worker_ids,
        )
        self._store.log_event(
            job.job_id, "started",
            f"Assigned to workers: {job.worker_ids}",
        )
        self._running_jobs[job.job_id] = job

        wait = (job.started_at - job.submitted_at).total_seconds()
        JOB_WAIT_TIME.labels(
            team=job.team, priority=job.priority.name
        ).observe(wait)

        self._drf.record_job_start(
            job.team,
            job.resources.cpu_cores * job.num_workers,
            job.resources.memory_gb * job.num_workers,
        )

        logger.info(
            "scheduler.job_dispatched",
            job_id=job.job_id,
            team=job.team,
            workers=job.worker_ids,
            wait_s=round(wait, 2),
        )
        return True

    # ── Preemption ────────────────────────────────────────────────────────────

    async def _try_preemption(
        self, pending: List[dict], running: List[Job]
    ) -> None:
        if not pending or not running:
            return
        waiting_job = self._build_job(pending[0])
        candidate   = self._preemption.find_preemption_candidate(
            waiting_job, running
        )
        if not candidate:
            return

        success, ckpt = await self._preemption.preempt(candidate, waiting_job)
        if not success:
            return

        await self._pool.release_workers(candidate.worker_ids)
        self._drf.record_job_end(
            candidate.team,
            candidate.resources.cpu_cores * candidate.num_workers,
            candidate.resources.memory_gb * candidate.num_workers,
        )
        self._running_jobs.pop(candidate.job_id, None)

        # Re-enqueue with checkpoint path so it can resume
        candidate.state      = JobState.PENDING
        candidate.worker_ids = []
        candidate.started_at = None
        if ckpt:
            candidate.checkpoint_path = ckpt
        await self._queue.enqueue(candidate)

    # ── Backfill ──────────────────────────────────────────────────────────────

    async def _try_backfill(self, blocked_job: Job, idle: list) -> None:
        """Schedule small jobs that fit while blocked_job waits."""
        pending = await self._queue.peek(limit=20)
        running = list(self._running_jobs.values())
        candidates = self._backfill.find_backfill_candidates(
            blocked_job, pending, idle, running
        )
        for candidate_data in candidates:
            job = self._build_job(candidate_data)
            await self._dispatch(job, self._pool.idle_workers())

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_job(self, data: dict) -> Job:
        """Reconstruct a Job from queue-dict data, preferring the DB record."""
        existing = self._store.get(data["job_id"])
        if existing:
            return existing

        priority_val = data.get("priority", 1)
        priority = (
            Priority(priority_val)
            if isinstance(priority_val, int)
            else Priority[priority_val]
        )
        return Job(
            job_id=data["job_id"],
            team=data["team"],
            name=data.get("name", "unknown"),
            priority=priority,
            resources=ResourceSpec(
                cpu_cores=data.get("cpu_cores", 2.0),
                memory_gb=data.get("memory_gb", 4.0),
            ),
            num_workers=data.get("num_workers", 1),
            estimated_duration_seconds=data.get(
                "estimated_duration_seconds", 300
            ),
            checkpoint_path=data.get("checkpoint_path"),
            state=JobState.PENDING,
        )

    def get_cluster_snapshot(self):
        """Return a current ClusterSnapshot for API responses."""
        return self._pool.cluster_snapshot(
            team_shares=self._drf.all_dominant_shares()
        )

    @property
    def is_healthy(self) -> bool:
        """True if the scheduler loop is running without persistent errors."""
        return (
            self._running
            and self._task is not None
            and not self._task.done()
            and self._consecutive_errors < 10
        )
