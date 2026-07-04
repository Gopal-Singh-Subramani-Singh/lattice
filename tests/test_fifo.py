from __future__ import annotations
import pytest
from lattice.algorithms.fifo import select_next_job_fifo, assign_workers_for_job
from lattice.models import Priority
from tests.conftest import make_job, make_worker


def test_select_first_fitting_job():
    pending = [
        {"job_id": "j1", "team": "a", "priority": 1,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
        {"job_id": "j2", "team": "a", "priority": 1,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
    ]
    workers = [make_worker("w1")]
    result = select_next_job_fifo(pending, workers)
    assert result["job_id"] == "j1"


def test_skips_job_exceeding_resources():
    pending = [
        {"job_id": "big", "team": "a", "priority": 2,
         "cpu_cores": 100.0, "memory_gb": 200.0, "num_workers": 1},
        {"job_id": "small", "team": "a", "priority": 1,
         "cpu_cores": 1.0, "memory_gb": 2.0, "num_workers": 1},
    ]
    workers = [make_worker("w1", cpu_limit=2.0, mem_limit=4.0)]
    result = select_next_job_fifo(pending, workers)
    assert result["job_id"] == "small"


def test_returns_none_when_nothing_fits():
    pending = [
        {"job_id": "j1", "team": "a", "priority": 1,
         "cpu_cores": 100.0, "memory_gb": 100.0, "num_workers": 1}
    ]
    workers = [make_worker("w1")]
    result = select_next_job_fifo(pending, workers)
    assert result is None


def test_assign_workers_exact_count():
    job = make_job("j1", num_workers=2, cpu=1.0, mem=2.0)
    workers = [make_worker(f"w{i}", cpu_limit=2.0, mem_limit=4.0) for i in range(5)]
    assigned = assign_workers_for_job(job, workers)
    assert len(assigned) == 2


def test_assign_workers_insufficient():
    job = make_job("j1", num_workers=5, cpu=1.0, mem=2.0)
    workers = [make_worker(f"w{i}") for i in range(3)]
    assigned = assign_workers_for_job(job, workers)
    assert assigned == []
