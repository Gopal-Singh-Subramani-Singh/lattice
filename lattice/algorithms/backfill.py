from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import structlog

from lattice.models import Job, Worker, Priority
from lattice.metrics import BACKFILL_SCHEDULED

logger = structlog.get_logger(__name__)


class BackfillScheduler:
    """
    Backfill scheduling: while a large job waits for enough workers,
    fill idle capacity with smaller jobs that will finish before
    the large job's estimated start time.

    Prevents idle capacity while waiting for gang-scheduled jobs.

    Algorithm:
    1. Identify the head-of-queue job (blocked, waiting for N workers).
    2. Estimate when N workers will be available (based on running job durations).
    3. Find pending jobs that:
       a. Fit in CURRENTLY IDLE worker capacity.
       b. Will finish before the head job's estimated start minus safety_margin.
    4. Schedule those jobs now.
    """

    def __init__(self, safety_margin_seconds: int = 60):
        self.safety_margin = safety_margin_seconds

    def find_backfill_candidates(
        self,
        blocked_job: Job,
        pending_jobs: List[dict],
        idle_workers: List[Worker],
        running_jobs: List[Job],
    ) -> List[dict]:
        """
        Find jobs that can safely backfill while blocked_job waits.
        Returns list of job_data dicts to schedule.
        """
        if not idle_workers or not pending_jobs:
            return []

        # Estimate when blocked job will be able to start
        estimated_start = self._estimate_start(blocked_job, running_jobs)
        deadline = estimated_start - timedelta(seconds=self.safety_margin)
        now = datetime.utcnow()

        available_cpu = sum(w.cpu_limit for w in idle_workers)
        available_mem = sum(w.memory_limit_gb for w in idle_workers)

        candidates = []
        remaining_cpu = available_cpu
        remaining_mem = available_mem

        for job_data in pending_jobs:
            if job_data["job_id"] == blocked_job.job_id:
                continue

            req_cpu = job_data["cpu_cores"] * job_data["num_workers"]
            req_mem = job_data["memory_gb"] * job_data["num_workers"]
            est_duration = job_data.get("estimated_duration_seconds", 300)
            est_finish = now + timedelta(seconds=est_duration)

            if (req_cpu <= remaining_cpu and
                    req_mem <= remaining_mem and
                    est_finish <= deadline):
                candidates.append(job_data)
                remaining_cpu -= req_cpu
                remaining_mem -= req_mem
                BACKFILL_SCHEDULED.inc()
                logger.info(
                    "backfill.candidate",
                    job_id=job_data["job_id"],
                    est_finish=est_finish.isoformat(),
                    deadline=deadline.isoformat(),
                )

                if remaining_cpu < 0.5 or remaining_mem < 0.5:
                    break

        return candidates

    def _estimate_start(
        self, blocked_job: Job, running_jobs: List[Job]
    ) -> datetime:
        """
        Estimate when blocked_job's required workers will be available.
        Conservative: use the maximum remaining time of any running job.
        """
        now = datetime.utcnow()
        if not running_jobs:
            return now + timedelta(seconds=60)

        max_remaining = 0
        for job in running_jobs:
            if job.started_at:
                elapsed = (now - job.started_at).total_seconds()
                remaining = max(
                    0,
                    job.estimated_duration_seconds - elapsed
                )
                max_remaining = max(max_remaining, remaining)

        return now + timedelta(seconds=max_remaining)
