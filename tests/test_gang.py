from __future__ import annotations
import asyncio
import pytest
from unittest.mock import MagicMock
from lattice.algorithms.gang import GangScheduler
from tests.conftest import make_job, make_worker


@pytest.mark.asyncio
async def test_gang_schedules_when_enough_workers():
    pool = MagicMock()
    gang = GangScheduler(pool)
    job = make_job("j1", num_workers=3)
    workers = [make_worker(f"w{i}") for i in range(5)]
    success, assigned = await gang.try_schedule(job, workers)
    assert success is True
    assert len(assigned) == 3


@pytest.mark.asyncio
async def test_gang_blocks_when_insufficient_workers():
    pool = MagicMock()
    gang = GangScheduler(pool)
    job = make_job("j1", num_workers=5)
    workers = [make_worker(f"w{i}") for i in range(3)]
    success, assigned = await gang.try_schedule(job, workers)
    assert success is False
    assert len(assigned) == 0


@pytest.mark.asyncio
async def test_gang_marks_workers_as_assigned():
    pool = MagicMock()
    gang = GangScheduler(pool)
    job = make_job("j1", num_workers=2, cpu=1.0, mem=2.0)
    workers = [make_worker(f"w{i}", cpu_limit=2.0, mem_limit=4.0) for i in range(4)]
    success, assigned = await gang.try_schedule(job, workers)
    assert success is True
    for w in assigned:
        assert w.job_id == "j1"


@pytest.mark.asyncio
async def test_gang_release_clears_job_id():
    pool = MagicMock()
    gang = GangScheduler(pool)
    job = make_job("j1", num_workers=2)
    workers = [make_worker(f"w{i}") for i in range(4)]
    _, assigned = await gang.try_schedule(job, workers)
    await gang.release(assigned)
    for w in assigned:
        assert w.job_id is None


@pytest.mark.asyncio
async def test_gang_respects_resource_limits():
    pool = MagicMock()
    gang = GangScheduler(pool)
    # Job needs 8 CPU per worker but workers only have 2
    job = make_job("j1", num_workers=2, cpu=8.0, mem=4.0)
    workers = [make_worker(f"w{i}", cpu_limit=2.0) for i in range(4)]
    success, _ = await gang.try_schedule(job, workers)
    assert success is False
