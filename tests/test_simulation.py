from __future__ import annotations
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from lattice.models import Priority, JobState
from tests.conftest import make_job, make_worker


@pytest.mark.asyncio
async def test_scheduler_submits_and_tracks_job(fake_redis_client, tmp_store):
    from lattice.store.redis_queue import RedisJobQueue
    from lattice.scheduler import Scheduler

    queue = RedisJobQueue(fake_redis_client)
    pool = MagicMock()
    pool.idle_workers.return_value = [make_worker("w1"), make_worker("w2")]
    pool.busy_workers.return_value = []
    pool.all_workers.return_value = [make_worker("w1"), make_worker("w2")]
    pool.evict_stale_workers = MagicMock()
    pool.assign_workers = AsyncMock(return_value=True)
    pool.release_workers = AsyncMock()
    pool.cluster_snapshot = MagicMock()

    sched = Scheduler(
        job_store=tmp_store,
        job_queue=queue,
        worker_pool=pool,
        algorithm="fifo",
        tick_interval_ms=50,
        preemption_enabled=False,
        backfill_enabled=False,
        gang_scheduling_enabled=False,
    )

    job = make_job("test-001", cpu=1.0, mem=2.0)
    job_id = await sched.submit_job(job)
    assert job_id == "test-001"
    stored = tmp_store.get("test-001")
    assert stored is not None
    depth = await queue.depth()
    assert depth == 1


@pytest.mark.asyncio
async def test_drf_fairness_with_two_teams(fake_redis_client, tmp_store):
    from lattice.algorithms.drf import DRFScheduler
    drf = DRFScheduler(cluster_cpu=16.0, cluster_mem=32.0)

    # Team A gets 8 CPU
    drf.record_job_start("team_a", 8.0, 8.0)
    # Team B gets 2 CPU
    drf.record_job_start("team_b", 2.0, 4.0)

    share_a = drf.dominant_share("team_a")
    share_b = drf.dominant_share("team_b")
    # Team B has lower share so should be scheduled next
    assert share_b < share_a


@pytest.mark.asyncio
async def test_cancel_running_job(fake_redis_client, tmp_store):
    from lattice.store.redis_queue import RedisJobQueue
    from lattice.scheduler import Scheduler

    queue = RedisJobQueue(fake_redis_client)
    pool = MagicMock()
    pool.idle_workers.return_value = []
    pool.busy_workers.return_value = []
    pool.all_workers.return_value = []
    pool.evict_stale_workers = MagicMock()
    pool.release_workers = AsyncMock()
    pool.cluster_snapshot = MagicMock()

    sched = Scheduler(
        job_store=tmp_store,
        job_queue=queue,
        worker_pool=pool,
    )

    job = make_job("cancel-test")
    await sched.submit_job(job)
    result = await sched.cancel_job("cancel-test")
    assert result is True
    stored = tmp_store.get("cancel-test")
    assert stored.state == JobState.CANCELLED


@pytest.mark.asyncio
async def test_job_retry_on_failure(fake_redis_client, tmp_store):
    from lattice.store.redis_queue import RedisJobQueue
    from lattice.scheduler import Scheduler

    queue = RedisJobQueue(fake_redis_client)
    pool = MagicMock()
    pool.release_workers = AsyncMock()
    pool.cluster_snapshot = MagicMock()

    sched = Scheduler(
        job_store=tmp_store,
        job_queue=queue,
        worker_pool=pool,
    )

    job = make_job("retry-test", max_retries=2, state=JobState.RUNNING)
    job.worker_ids = ["w1"]
    tmp_store.save(job)
    sched._running_jobs[job.job_id] = job

    await sched.complete_job("retry-test", success=False)

    stored = tmp_store.get("retry-test")
    assert stored.retry_count == 1
    depth = await queue.depth()
    assert depth == 1
