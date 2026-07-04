from __future__ import annotations
from datetime import datetime, timedelta
import pytest
from lattice.algorithms.backfill import BackfillScheduler
from lattice.models import JobState
from tests.conftest import make_job, make_worker


def make_pending(job_id, cpu=1.0, mem=2.0, est_secs=60, num_workers=1):
    return {
        "job_id": job_id,
        "team": "t",
        "priority": 1,
        "cpu_cores": cpu,
        "memory_gb": mem,
        "num_workers": num_workers,
        "estimated_duration_seconds": est_secs,
    }


def test_fills_idle_capacity_with_short_jobs():
    sched = BackfillScheduler(safety_margin_seconds=30)
    blocked = make_job("big", num_workers=4, estimated_secs=600)
    blocked.state = JobState.PENDING
    pending = [make_pending(f"small-{i}", est_secs=60) for i in range(5)]
    idle = [make_worker(f"w{i}") for i in range(2)]
    # Simulate running jobs that will free up in 120s
    running = [
        make_job(f"r{i}", state=JobState.RUNNING, estimated_secs=120)
        for i in range(4)
    ]
    for r in running:
        r.started_at = datetime.utcnow() - timedelta(seconds=10)
    candidates = sched.find_backfill_candidates(blocked, pending, idle, running)
    assert len(candidates) > 0


def test_does_not_exceed_available_capacity():
    sched = BackfillScheduler(safety_margin_seconds=0)
    blocked = make_job("big", num_workers=4, estimated_secs=600)
    pending = [make_pending(f"j{i}", cpu=3.0, mem=6.0) for i in range(10)]
    idle = [make_worker("w1", cpu_limit=2.0, mem_limit=4.0)]  # only 2 CPU available
    candidates = sched.find_backfill_candidates(blocked, pending, idle, [])
    # All require 3 CPU > 2 available, so none should be scheduled
    assert len(candidates) == 0


def test_skips_blocked_job_itself():
    sched = BackfillScheduler()
    blocked = make_job("big", num_workers=4, estimated_secs=300)
    pending = [{"job_id": "big", "team": "t", "priority": 2,
                "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 4,
                "estimated_duration_seconds": 300}]
    idle = [make_worker("w1")]
    candidates = sched.find_backfill_candidates(blocked, pending, idle, [])
    assert all(c["job_id"] != "big" for c in candidates)


def test_estimate_start_with_no_running():
    sched = BackfillScheduler()
    blocked = make_job("big")
    est = sched._estimate_start(blocked, [])
    assert est > datetime.utcnow()
