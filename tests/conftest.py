from __future__ import annotations
import pytest
import pytest_asyncio
from unittest.mock import MagicMock, AsyncMock
from fakeredis import aioredis as fake_redis
from datetime import datetime

from lattice.models import (
    Job, JobState, Priority, ResourceSpec, Worker
)


def make_job(
    job_id: str = "job-001",
    team: str = "team_a",
    name: str = "train",
    priority: Priority = Priority.NORMAL,
    cpu: float = 2.0,
    mem: float = 4.0,
    num_workers: int = 1,
    max_retries: int = 0,
    estimated_secs: int = 300,
    state: JobState = JobState.PENDING,
) -> Job:
    job = Job(
        job_id=job_id,
        team=team,
        name=name,
        priority=priority,
        resources=ResourceSpec(cpu_cores=cpu, memory_gb=mem),
        num_workers=num_workers,
        max_retries=max_retries,
        estimated_duration_seconds=estimated_secs,
        state=state,
    )
    return job


def make_worker(
    worker_id: str = "w-001",
    cpu_limit: float = 2.0,
    mem_limit: float = 4.0,
    job_id: str = None,
    healthy: bool = True,
) -> Worker:
    return Worker(
        worker_id=worker_id,
        container_id=f"sim-{worker_id}",
        cpu_limit=cpu_limit,
        memory_limit_gb=mem_limit,
        job_id=job_id,
        healthy=healthy,
        last_heartbeat=datetime.utcnow(),
    )


@pytest_asyncio.fixture
async def fake_redis_client():
    r = fake_redis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest.fixture
def tmp_store(tmp_path):
    from lattice.store.job_store import JobStore
    return JobStore(db_path=str(tmp_path / "test.db"))


@pytest_asyncio.fixture
async def job_queue(fake_redis_client):
    from lattice.store.redis_queue import RedisJobQueue
    return RedisJobQueue(fake_redis_client)


@pytest.fixture
def idle_workers():
    return [
        make_worker(f"w-{i:03d}", cpu_limit=2.0, mem_limit=4.0)
        for i in range(8)
    ]


@pytest.fixture
def sample_job():
    return make_job()


@pytest.fixture
def high_priority_job():
    return make_job(
        "job-hp", priority=Priority.CRITICAL, cpu=2.0, mem=4.0
    )


@pytest.fixture
def batch_job():
    return make_job(
        "job-batch", priority=Priority.BATCH, cpu=1.0, mem=2.0,
        estimated_secs=60,
    )


@pytest.fixture
def gang_job():
    return make_job(
        "job-gang", num_workers=4, cpu=2.0, mem=4.0
    )
