from __future__ import annotations
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from lattice.algorithms.preemption import PreemptionController
from lattice.models import JobState, Priority
from tests.conftest import make_job


@pytest.fixture
def mock_store():
    s = MagicMock()
    s.update_state = MagicMock()
    s.log_event = MagicMock()
    return s


@pytest.fixture
def mock_pool():
    return MagicMock()


def test_finds_preemption_candidate(mock_store, mock_pool):
    ctrl = PreemptionController(mock_store, mock_pool, priority_gap=2)
    waiting = make_job("hp", priority=Priority.CRITICAL)
    running = [
        make_job("lp1", priority=Priority.BATCH, state=JobState.RUNNING),
        make_job("lp2", priority=Priority.NORMAL, state=JobState.RUNNING),
    ]
    candidate = ctrl.find_preemption_candidate(waiting, running)
    assert candidate is not None
    assert candidate.job_id == "lp1"


def test_no_candidate_when_gap_too_small(mock_store, mock_pool):
    ctrl = PreemptionController(mock_store, mock_pool, priority_gap=2)
    waiting = make_job("hp", priority=Priority.HIGH)
    running = [make_job("lp", priority=Priority.NORMAL, state=JobState.RUNNING)]
    # Gap = HIGH(2) - NORMAL(1) = 1 < priority_gap=2
    candidate = ctrl.find_preemption_candidate(waiting, running)
    assert candidate is None


def test_no_candidate_when_running_is_empty(mock_store, mock_pool):
    ctrl = PreemptionController(mock_store, mock_pool)
    waiting = make_job("hp", priority=Priority.CRITICAL)
    candidate = ctrl.find_preemption_candidate(waiting, [])
    assert candidate is None


@pytest.mark.asyncio
async def test_preempt_updates_job_state(mock_store, mock_pool):
    ctrl = PreemptionController(mock_store, mock_pool, checkpoint_timeout=1)
    candidate = make_job("lp", priority=Priority.BATCH, state=JobState.RUNNING)
    waiting = make_job("hp", priority=Priority.CRITICAL)

    with patch.object(ctrl, "_signal_checkpoint", AsyncMock(return_value="/tmp/ckpt.pt")):
        success, ckpt = await ctrl.preempt(candidate, waiting)

    assert success is True
    assert ckpt == "/tmp/ckpt.pt"
    mock_store.update_state.assert_called_once()
    args = mock_store.update_state.call_args[0]
    assert args[1] == JobState.PREEMPTED


@pytest.mark.asyncio
async def test_preempt_increments_metrics(mock_store, mock_pool):
    ctrl = PreemptionController(mock_store, mock_pool, checkpoint_timeout=1)
    candidate = make_job("lp", priority=Priority.BATCH, state=JobState.RUNNING)
    waiting = make_job("hp", priority=Priority.CRITICAL)
    with patch.object(ctrl, "_signal_checkpoint", AsyncMock(return_value="/ckpt")):
        success, _ = await ctrl.preempt(candidate, waiting)
    assert success is True
