from __future__ import annotations
import pytest
from lattice.algorithms.drf import DRFScheduler


def test_equal_teams_get_equal_shares():
    drf = DRFScheduler(cluster_cpu=16.0, cluster_mem=32.0)
    drf.record_job_start("team_a", cpu_cores=4.0, memory_gb=8.0)
    drf.record_job_start("team_b", cpu_cores=4.0, memory_gb=8.0)
    assert abs(drf.dominant_share("team_a") - drf.dominant_share("team_b")) < 0.001


def test_selects_team_with_lowest_share():
    drf = DRFScheduler(cluster_cpu=16.0, cluster_mem=32.0)
    drf.record_job_start("team_a", cpu_cores=8.0, memory_gb=4.0)
    drf.record_job_start("team_b", cpu_cores=2.0, memory_gb=4.0)

    from tests.conftest import make_worker
    workers = [make_worker(f"w{i}") for i in range(4)]
    pending = [
        {"job_id": "ja", "team": "team_a", "priority": 1,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
        {"job_id": "jb", "team": "team_b", "priority": 1,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
    ]
    selected = drf.select_next_job(pending, workers)
    assert selected["team"] == "team_b"


def test_dominant_resource_is_max_fraction():
    drf = DRFScheduler(cluster_cpu=10.0, cluster_mem=10.0)
    drf.record_job_start("team_a", cpu_cores=5.0, memory_gb=2.0)
    assert abs(drf.dominant_share("team_a") - 0.5) < 0.001


def test_record_end_decrements_allocation():
    drf = DRFScheduler(cluster_cpu=16.0, cluster_mem=32.0)
    drf.record_job_start("team_a", cpu_cores=4.0, memory_gb=8.0)
    drf.record_job_end("team_a", cpu_cores=4.0, memory_gb=8.0)
    assert drf.dominant_share("team_a") == 0.0


def test_zero_allocation_team_selected_first():
    drf = DRFScheduler(cluster_cpu=16.0, cluster_mem=32.0)
    drf.record_job_start("team_hog", cpu_cores=12.0, memory_gb=24.0)

    from tests.conftest import make_worker
    workers = [make_worker(f"w{i}") for i in range(8)]
    pending = [
        {"job_id": "hog", "team": "team_hog", "priority": 2,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
        {"job_id": "new", "team": "team_new", "priority": 1,
         "cpu_cores": 2.0, "memory_gb": 4.0, "num_workers": 1},
    ]
    selected = drf.select_next_job(pending, workers)
    assert selected["team"] == "team_new"
