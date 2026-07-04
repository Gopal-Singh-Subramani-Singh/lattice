from __future__ import annotations
import asyncio
from typing import List, Optional, Tuple
import structlog

from lattice.models import Job, Worker
from lattice.metrics import GANG_ATTEMPTS

logger = structlog.get_logger(__name__)


class GangScheduler:
    """
    Gang scheduling: all N workers for a multi-worker job must start
    simultaneously, or none start (all-or-nothing).

    Prevents the classic partial-allocation deadlock:
      Job A needs 4 workers. Gets 3. Waits for 1 more.
      Job B needs 2 workers. Both are held by job A's 3.
      Deadlock: A waits for resources held by the waiting state of B,
                B waits for workers allocated-but-idle by A.

    Implementation:
    - Check if ALL N idle workers are available atomically.
    - If yes: reserve ALL of them before any other scheduler tick runs.
    - If no: don't reserve any — job stays pending.
    - Uses Redis lock (in redis_queue.py) to make reservation atomic.
    """

    def __init__(self, worker_pool):
        self._pool = worker_pool
        self._lock = asyncio.Lock()

    async def try_schedule(
        self, job: Job, idle_workers: List[Worker]
    ) -> Tuple[bool, List[Worker]]:
        """
        Try to atomically reserve exactly num_workers idle workers for job.
        Returns (success, assigned_workers).
        """
        async with self._lock:
            needed = job.num_workers
            eligible = [
                w for w in idle_workers
                if (w.cpu_limit >= job.resources.cpu_cores and
                    w.memory_limit_gb >= job.resources.memory_gb)
            ]

            if len(eligible) < needed:
                GANG_ATTEMPTS.labels(result="blocked").inc()
                logger.debug(
                    "gang.blocked",
                    job_id=job.job_id,
                    needed=needed,
                    eligible=len(eligible),
                )
                return False, []

            # Atomically reserve exactly `needed` workers
            assigned = eligible[:needed]
            for w in assigned:
                w.job_id = job.job_id
                w.cpu_used = job.resources.cpu_cores
                w.memory_used_gb = job.resources.memory_gb

            GANG_ATTEMPTS.labels(result="success").inc()
            logger.info(
                "gang.scheduled",
                job_id=job.job_id,
                workers=[w.worker_id for w in assigned],
                num_workers=needed,
            )
            return True, assigned

    async def release(self, workers: List[Worker]):
        async with self._lock:
            for w in workers:
                w.job_id = None
                w.cpu_used = 0.0
                w.memory_used_gb = 0.0
