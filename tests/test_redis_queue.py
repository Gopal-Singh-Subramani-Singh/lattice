from __future__ import annotations
import asyncio
import pytest
from lattice.models import Priority
from tests.conftest import make_job


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(job_queue, sample_job):
    await job_queue.enqueue(sample_job)
    items = await job_queue.dequeue_batch(limit=1)
    assert len(items) == 1
    assert items[0]["job_id"] == sample_job.job_id


@pytest.mark.asyncio
async def test_priority_ordering(job_queue):
    batch_job = make_job("jb", priority=Priority.BATCH)
    critical_job = make_job("jc", priority=Priority.CRITICAL)
    await job_queue.enqueue(batch_job)
    await asyncio.sleep(0.01)
    await job_queue.enqueue(critical_job)
    items = await job_queue.dequeue_batch(limit=2)
    # Critical should come first (higher score)
    assert items[0]["job_id"] == "jc"
    assert items[1]["job_id"] == "jb"


@pytest.mark.asyncio
async def test_fifo_within_same_priority(job_queue):
    jobs = [make_job(f"j{i}", priority=Priority.NORMAL) for i in range(3)]
    for j in jobs:
        await job_queue.enqueue(j)
        await asyncio.sleep(0.01)
    items = await job_queue.dequeue_batch(limit=3)
    assert [it["job_id"] for it in items] == ["j0", "j1", "j2"]


@pytest.mark.asyncio
async def test_remove_job(job_queue, sample_job):
    await job_queue.enqueue(sample_job)
    removed = await job_queue.remove(sample_job.job_id)
    assert removed is True
    depth = await job_queue.depth()
    assert depth == 0


@pytest.mark.asyncio
async def test_depth_by_priority(job_queue):
    await job_queue.enqueue(make_job("jb", priority=Priority.BATCH))
    await job_queue.enqueue(make_job("jn", priority=Priority.NORMAL))
    await job_queue.enqueue(make_job("jh", priority=Priority.HIGH))
    depths = await job_queue.depth_by_priority()
    assert depths["BATCH"] >= 1
    assert depths["NORMAL"] >= 1
    assert depths["HIGH"] >= 1


@pytest.mark.asyncio
async def test_acquire_and_release_gang_lock(job_queue):
    acquired = await job_queue.acquire_gang_lock("job-001", ttl_seconds=5)
    assert acquired is True
    # Second acquire should fail
    again = await job_queue.acquire_gang_lock("job-001", ttl_seconds=5)
    assert again is False
    await job_queue.release_gang_lock("job-001")
    # After release, can acquire again
    third = await job_queue.acquire_gang_lock("job-001", ttl_seconds=5)
    assert third is True
