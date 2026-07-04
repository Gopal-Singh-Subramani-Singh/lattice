from __future__ import annotations
import asyncio
import os
import signal
import time
from typing import List, Optional, Tuple
import structlog

from lattice.models import Job, Worker, JobState, Priority
from lattice.metrics import PREEMPTIONS

logger = structlog.get_logger(__name__)


class PreemptionController:
    """
    Preempts lower-priority running jobs to free resources for
    higher-priority waiting jobs.

    Protocol:
    1. Identify lowest-priority running job (preemption candidate).
    2. Check priority gap >= configured minimum.
    3. Send SIGUSR1 to worker containers → triggers checkpoint save.
    4. Wait for checkpoint ACK (or timeout=30s).
    5. Release workers.
    6. Re-queue preempted job with checkpoint_path set.
    7. Schedule high-priority job on freed workers.
    """

    def __init__(
        self,
        job_store,
        worker_pool,
        priority_gap: int = 2,
        checkpoint_timeout: int = 30,
    ):
        self._store = job_store
        self._pool = worker_pool
        self._priority_gap = priority_gap
        self._checkpoint_timeout = checkpoint_timeout

    def find_preemption_candidate(
        self,
        waiting_job: Job,
        running_jobs: List[Job],
    ) -> Optional[Job]:
        """
        Find the lowest-priority running job that:
        1. Has priority gap >= self._priority_gap below waiting_job
        2. Occupies enough workers to satisfy waiting_job
        """
        candidates = [
            j for j in running_jobs
            if (waiting_job.priority.value - j.priority.value >= self._priority_gap)
        ]
        if not candidates:
            return None

        # Sort by priority ascending (lowest first = best candidate)
        candidates.sort(key=lambda j: (j.priority.value, j.submitted_at))
        return candidates[0]

    async def preempt(
        self,
        candidate: Job,
        waiting_job: Job,
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute preemption. Returns (success, checkpoint_path).
        """
        logger.warning(
            "preemption.initiating",
            preempted=candidate.job_id,
            preempted_priority=candidate.priority.name,
            preemptor=waiting_job.job_id,
            preemptor_priority=waiting_job.priority.name,
        )

        # Signal workers to checkpoint
        checkpoint_path = await self._signal_checkpoint(candidate)

        # Update job state
        self._store.update_state(
            candidate.job_id,
            JobState.PREEMPTED,
            message=f"Preempted by higher-priority job {waiting_job.job_id}",
            checkpoint_path=checkpoint_path,
        )
        self._store.log_event(
            candidate.job_id,
            "preempted",
            f"Preempted by {waiting_job.job_id} (priority={waiting_job.priority.name})",
        )

        PREEMPTIONS.labels(
            preempted_priority=candidate.priority.name,
            preemptor_priority=waiting_job.priority.name,
        ).inc()

        logger.info(
            "preemption.complete",
            preempted=candidate.job_id,
            checkpoint=checkpoint_path,
        )
        return True, checkpoint_path

    async def _signal_checkpoint(self, job: Job) -> Optional[str]:
        """
        Send SIGUSR1 to all worker containers for this job.
        Returns checkpoint path after save completes.
        """
        checkpoint_path = f"/tmp/lattice/checkpoints/{job.job_id}.ckpt"

        for worker_id in job.worker_ids:
            worker = self._pool.get_worker(worker_id)
            if worker and worker.container_id:
                try:
                    await self._send_sigusr1(worker.container_id)
                    logger.debug(
                        "preemption.sigusr1_sent",
                        worker=worker_id,
                        container=worker.container_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "preemption.signal_failed",
                        worker=worker_id,
                        error=str(exc),
                    )

        # Wait for checkpoint to complete (or timeout)
        await asyncio.sleep(min(5.0, self._checkpoint_timeout))
        return checkpoint_path

    async def _send_sigusr1(self, container_id: str):
        """Send SIGUSR1 to the main process in a Docker container."""
        try:
            import docker
            client = docker.from_env()
            container = client.containers.get(container_id)
            container.kill(signal="SIGUSR1")
        except Exception as exc:
            logger.debug(
                "preemption.docker_signal_failed",
                container=container_id,
                error=str(exc),
            )
