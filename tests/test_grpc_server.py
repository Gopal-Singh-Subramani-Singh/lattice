"""
Tests for gRPC server logic via direct method calls (no real gRPC transport needed).
"""
from __future__ import annotations
import pytest
from unittest.mock import MagicMock, AsyncMock
from lattice.models import JobState, Priority
from tests.conftest import make_job, make_worker


@pytest.mark.asyncio
async def test_rest_submit_returns_job_id(fake_redis_client, tmp_store):
    """Test the scheduler submit path (same logic as gRPC Submit)."""
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

    job = make_job("grpc-test-01", team="alpha", priority=Priority.HIGH)
    job_id = await sched.submit_job(job)
    assert job_id == "grpc-test-01"

    stored = tmp_store.get("grpc-test-01")
    assert stored.team == "alpha"
    assert stored.priority == Priority.HIGH


@pytest.mark.asyncio
async def test_rest_cancel_nonexistent_job(fake_redis_client, tmp_store):
    """Cancel a job that doesn't exist should return False."""
    from lattice.store.redis_queue import RedisJobQueue
    from lattice.scheduler import Scheduler

    queue = RedisJobQueue(fake_redis_client)
    pool = MagicMock()
    pool.release_workers = AsyncMock()

    sched = Scheduler(
        job_store=tmp_store,
        job_queue=queue,
        worker_pool=pool,
    )

    result = await sched.cancel_job("does-not-exist")
    assert result is False


@pytest.mark.asyncio
async def test_cluster_snapshot_reflects_worker_state(fake_redis_client, tmp_store):
    """Cluster snapshot idle/busy counts match the pool state."""
    from lattice.store.redis_queue import RedisJobQueue
    from lattice.scheduler import Scheduler

    queue = RedisJobQueue(fake_redis_client)
    pool = MagicMock()
    idle = [make_worker("w1"), make_worker("w2")]
    busy = [make_worker("w3", job_id="j1")]
    pool.idle_workers.return_value = idle
    pool.busy_workers.return_value = busy
    pool.all_workers.return_value = idle + busy
    pool.evict_stale_workers = MagicMock()
    pool.release_workers = AsyncMock()

    from lattice.models import ClusterSnapshot
    from datetime import datetime
    pool.cluster_snapshot.return_value = ClusterSnapshot(
        total_workers=3,
        idle_workers=2,
        busy_workers=1,
        utilisation_pct=33.3,
        pending_jobs=0,
        running_jobs=1,
        team_dominant_shares={},
    )

    sched = Scheduler(
        job_store=tmp_store,
        job_queue=queue,
        worker_pool=pool,
    )

    snap = sched.get_cluster_snapshot()
    assert snap.total_workers == 3
    assert snap.idle_workers == 2
    assert snap.busy_workers == 1
