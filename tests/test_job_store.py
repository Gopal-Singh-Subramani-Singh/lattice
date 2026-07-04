from __future__ import annotations
import pytest
from lattice.models import JobState, Priority
from tests.conftest import make_job


def test_save_and_retrieve(tmp_store, sample_job):
    tmp_store.save(sample_job)
    retrieved = tmp_store.get(sample_job.job_id)
    assert retrieved is not None
    assert retrieved.job_id == sample_job.job_id
    assert retrieved.team == sample_job.team


def test_update_state_to_running(tmp_store, sample_job):
    tmp_store.save(sample_job)
    tmp_store.update_state(
        sample_job.job_id, JobState.RUNNING,
        worker_ids=["w-001", "w-002"]
    )
    job = tmp_store.get(sample_job.job_id)
    assert job.state == JobState.RUNNING
    assert "w-001" in job.worker_ids
    assert job.started_at is not None


def test_update_state_to_completed(tmp_store, sample_job):
    tmp_store.save(sample_job)
    tmp_store.update_state(sample_job.job_id, JobState.COMPLETED)
    job = tmp_store.get(sample_job.job_id)
    assert job.state == JobState.COMPLETED
    assert job.finished_at is not None


def test_missing_job_returns_none(tmp_store):
    assert tmp_store.get("nonexistent") is None


def test_list_jobs_by_team(tmp_store):
    for i in range(3):
        j = make_job(f"j{i}", team="team_a")
        tmp_store.save(j)
    j_other = make_job("j_other", team="team_b")
    tmp_store.save(j_other)
    jobs = tmp_store.list_jobs(team="team_a")
    assert len(jobs) == 3
    assert all(j.team == "team_a" for j in jobs)


def test_list_jobs_by_state(tmp_store, sample_job):
    tmp_store.save(sample_job)
    tmp_store.update_state(sample_job.job_id, JobState.RUNNING)
    j2 = make_job("j2")
    tmp_store.save(j2)
    running = tmp_store.list_jobs(state="running")
    assert len(running) == 1
    assert running[0].job_id == sample_job.job_id


def test_log_and_retrieve_events(tmp_store, sample_job):
    tmp_store.save(sample_job)
    tmp_store.log_event(sample_job.job_id, "started", "Workers assigned")
    tmp_store.log_event(sample_job.job_id, "preempted", "By critical job")
    events = tmp_store.get_events(sample_job.job_id)
    assert len(events) == 2


def test_increment_retry(tmp_store, sample_job):
    tmp_store.save(sample_job)
    tmp_store.increment_retry(sample_job.job_id)
    tmp_store.increment_retry(sample_job.job_id)
    job = tmp_store.get(sample_job.job_id)
    assert job.retry_count == 2
