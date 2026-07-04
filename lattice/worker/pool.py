from __future__ import annotations
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import structlog

from lattice.models import Worker, Job, ClusterSnapshot
from lattice.worker.docker_worker import DockerWorkerManager
from lattice.metrics import (
    CLUSTER_UTILISATION, WORKER_STATE, DRF_DOMINANT_SHARE
)

logger = structlog.get_logger(__name__)


class WorkerPool:
    """
    Manages the pool of simulated worker nodes.
    Tracks allocation, heartbeats, and cluster utilisation.
    """

    def __init__(
        self,
        docker_manager: DockerWorkerManager,
        heartbeat_timeout_seconds: int = 30,
    ):
        self._docker = docker_manager
        self._heartbeat_timeout = heartbeat_timeout_seconds
        self._lock = asyncio.Lock()

    async def initialise(self, num_workers: int) -> List[Worker]:
        workers = await self._docker.start_workers(num_workers)
        logger.info("pool.initialised", workers=len(workers))
        return workers

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        return self._docker.get_worker(worker_id)

    def all_workers(self) -> List[Worker]:
        return self._docker.all_workers()

    def idle_workers(self) -> List[Worker]:
        return self._docker.idle_workers()

    def busy_workers(self) -> List[Worker]:
        return self._docker.busy_workers()

    async def assign_workers(
        self, job: Job, workers: List[Worker]
    ) -> bool:
        async with self._lock:
            for w in workers:
                w.job_id = job.job_id
                w.cpu_used = job.resources.cpu_cores
                w.memory_used_gb = job.resources.memory_gb
            self._update_prometheus()
            return True

    async def release_workers(self, worker_ids: List[str]):
        async with self._lock:
            for wid in worker_ids:
                w = self._docker.get_worker(wid)
                if w:
                    w.job_id = None
                    w.cpu_used = 0.0
                    w.memory_used_gb = 0.0
            self._update_prometheus()

    def update_heartbeat(
        self,
        worker_id: str,
        cpu_percent: float,
        memory_gb: float,
        healthy: bool,
    ):
        self._docker.update_heartbeat(
            worker_id, cpu_percent, memory_gb, healthy
        )

    def evict_stale_workers(self):
        """Mark workers as unhealthy if heartbeat timed out."""
        now = datetime.utcnow()
        timeout = timedelta(seconds=self._heartbeat_timeout)
        for w in self.all_workers():
            if w.last_heartbeat and (now - w.last_heartbeat) > timeout:
                w.healthy = False
                logger.warning(
                    "pool.worker_stale",
                    worker_id=w.worker_id,
                    last_heartbeat=w.last_heartbeat.isoformat(),
                )

    def cluster_snapshot(
        self, team_shares: Optional[Dict[str, float]] = None
    ) -> ClusterSnapshot:
        all_w = self.all_workers()
        idle = self.idle_workers()
        busy = self.busy_workers()
        total = len(all_w)
        utilisation = len(busy) / total if total > 0 else 0.0
        CLUSTER_UTILISATION.set(utilisation)
        return ClusterSnapshot(
            total_workers=total,
            idle_workers=len(idle),
            busy_workers=len(busy),
            utilisation_pct=round(utilisation * 100, 1),
            pending_jobs=0,  # filled in by scheduler
            running_jobs=len(busy),
            team_dominant_shares=team_shares or {},
        )

    def _update_prometheus(self):
        idle = len(self.idle_workers())
        busy = len(self.busy_workers())
        all_w = len(self.all_workers())
        WORKER_STATE.labels(state="idle").set(idle)
        WORKER_STATE.labels(state="busy").set(busy)
        WORKER_STATE.labels(state="unhealthy").set(
            sum(1 for w in self.all_workers() if not w.healthy)
        )
        util = busy / max(all_w, 1)
        CLUSTER_UTILISATION.set(util)
