from __future__ import annotations
import asyncio
import time
import uuid
from typing import Optional, Dict
import structlog

from lattice.models import Worker

logger = structlog.get_logger(__name__)

WORKER_IMAGE = "lattice-worker:latest"


class DockerWorkerManager:
    """
    Manages Docker containers as simulated worker nodes.
    Each container has enforced cgroup CPU and memory limits.
    The worker agent gRPC server runs inside each container.
    """

    def __init__(
        self,
        max_workers: int = 8,
        cpu_limit: str = "2",
        memory_limit: str = "4g",
        image: str = WORKER_IMAGE,
    ):
        self.max_workers = max_workers
        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit
        self.image = image
        self._workers: Dict[str, Worker] = {}
        self._docker_available = False
        self._try_docker()

    def _try_docker(self):
        try:
            import docker
            self._docker = docker.from_env()
            self._docker.ping()
            self._docker_available = True
            logger.info("docker.available")
        except Exception as exc:
            logger.warning("docker.unavailable", error=str(exc))
            self._docker_available = False

    async def start_workers(self, count: int) -> list[Worker]:
        """Start `count` worker containers. Returns list of Worker objects."""
        started = []
        for i in range(min(count, self.max_workers)):
            worker = await self._start_one_worker()
            if worker:
                self._workers[worker.worker_id] = worker
                started.append(worker)
        logger.info("workers.started", count=len(started))
        return started

    async def _start_one_worker(self) -> Optional[Worker]:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        cpu_limit_float = float(self.cpu_limit.replace("m", "")) / (
            1000 if "m" in str(self.cpu_limit) else 1
        )
        mem_gb = (
            float(self.memory_limit.replace("g", ""))
            if "g" in str(self.memory_limit)
            else float(self.memory_limit) / (1024 ** 3)
        )

        if self._docker_available:
            container_id = await self._launch_container(worker_id)
        else:
            # Simulation mode: no real Docker
            container_id = f"sim-{worker_id}"
            logger.info(
                "worker.simulation_mode",
                worker_id=worker_id,
            )

        worker = Worker(
            worker_id=worker_id,
            container_id=container_id,
            cpu_limit=cpu_limit_float,
            memory_limit_gb=mem_gb,
        )
        return worker

    async def _launch_container(self, worker_id: str) -> Optional[str]:
        try:
            loop = asyncio.get_running_loop()
            container = await loop.run_in_executor(
                None,
                lambda: self._docker.containers.run(
                    self.image,
                    name=f"lattice-{worker_id}",
                    detach=True,
                    remove=True,
                    cpu_count=int(self.cpu_limit),
                    mem_limit=self.memory_limit,
                    environment={"WORKER_ID": worker_id},
                    labels={"managed-by": "lattice", "worker-id": worker_id},
                ),
            )
            logger.info(
                "container.started",
                worker_id=worker_id,
                container_id=container.id[:12],
            )
            return container.id
        except Exception as exc:
            logger.warning(
                "container.start_failed",
                worker_id=worker_id,
                error=str(exc),
            )
            return f"sim-{worker_id}"

    async def stop_worker(self, worker_id: str):
        worker = self._workers.get(worker_id)
        if not worker:
            return
        if (self._docker_available and worker.container_id and
                not worker.container_id.startswith("sim-")):
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._docker.containers.get(
                        worker.container_id
                    ).stop(timeout=5),
                )
            except Exception as exc:
                logger.warning(
                    "container.stop_failed",
                    worker_id=worker_id,
                    error=str(exc),
                )
        del self._workers[worker_id]

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        return self._workers.get(worker_id)

    def all_workers(self) -> list[Worker]:
        return list(self._workers.values())

    def idle_workers(self) -> list[Worker]:
        return [w for w in self._workers.values() if w.job_id is None and w.healthy]

    def busy_workers(self) -> list[Worker]:
        return [w for w in self._workers.values() if w.job_id is not None]

    def update_heartbeat(
        self,
        worker_id: str,
        cpu_percent: float,
        memory_gb: float,
        healthy: bool,
    ):
        if worker_id in self._workers:
            from datetime import datetime
            w = self._workers[worker_id]
            w.cpu_used = cpu_percent * w.cpu_limit / 100.0
            w.memory_used_gb = memory_gb
            w.healthy = healthy
            w.last_heartbeat = datetime.utcnow()
