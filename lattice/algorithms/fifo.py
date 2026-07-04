from __future__ import annotations
from typing import List, Optional
from lattice.models import Job, Worker, Priority
import structlog

logger = structlog.get_logger(__name__)


def select_next_job_fifo(
    pending_jobs: List[dict],
    available_workers: List[Worker],
) -> Optional[dict]:
    """
    Select the highest-priority pending job that fits available resources.
    Within same priority, earliest submitted wins (FIFO).

    Jobs are pre-sorted by Redis score (priority × 10¹² − timestamp),
    so we just find the first one that fits.
    """
    total_cpu = sum(w.cpu_limit for w in available_workers)
    total_mem = sum(w.memory_limit_gb for w in available_workers)

    for job_data in pending_jobs:
        required_cpu = job_data["cpu_cores"] * job_data["num_workers"]
        required_mem = job_data["memory_gb"] * job_data["num_workers"]
        if required_cpu <= total_cpu and required_mem <= total_mem:
            return job_data

    return None


def assign_workers_for_job(
    job: Job,
    idle_workers: List[Worker],
) -> List[Worker]:
    """
    Assign the minimum number of workers needed for the job.
    Returns the list of assigned workers.
    """
    needed = job.num_workers
    assigned = []
    for worker in idle_workers:
        if len(assigned) >= needed:
            break
        if (worker.cpu_limit >= job.resources.cpu_cores and
                worker.memory_limit_gb >= job.resources.memory_gb):
            assigned.append(worker)
    if len(assigned) < needed:
        logger.warning(
            "fifo.insufficient_workers",
            job_id=job.job_id,
            needed=needed,
            available=len(assigned),
        )
        return []
    return assigned
