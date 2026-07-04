"""
Domain models for Lattice.

All external-facing Pydantic models include field-level validation so
invalid requests are rejected at the API boundary with clear 422 errors
rather than propagating into the scheduler core.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
import uuid


# ── Enums ─────────────────────────────────────────────────────────────────────


class Priority(int, Enum):
    BATCH    = 0
    NORMAL   = 1
    HIGH     = 2
    CRITICAL = 3


class JobState(str, Enum):
    PENDING   = "pending"
    RESERVED  = "reserved"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"


# ── Core dataclasses (internal) ───────────────────────────────────────────────


@dataclass
class ResourceSpec:
    """Resources required by a single worker slot."""

    cpu_cores: float = 2.0
    memory_gb: float = 4.0
    gpu_count: int   = 0

    def dominant_fraction(self, cluster_cpu: float, cluster_mem: float) -> float:
        """
        Fraction of the cluster this spec consumes on the dominant resource axis.
        Used by DRF to determine fair-share scheduling order.
        """
        return max(
            self.cpu_cores / max(cluster_cpu, 1e-9),
            self.memory_gb / max(cluster_mem, 1e-9),
        )


@dataclass
class Job:
    """
    Internal representation of a submitted job.
    Created from an API request and persisted to SQLite.
    """

    job_id:      str
    team:        str
    name:        str
    priority:    Priority
    resources:   ResourceSpec
    num_workers: int   = 1
    max_retries: int   = 0
    estimated_duration_seconds: int   = 300
    checkpoint_path: Optional[str]    = None
    labels:      Dict[str, str]       = field(default_factory=dict)

    # Runtime state
    state:       JobState             = JobState.PENDING
    worker_ids:  List[str]            = field(default_factory=list)
    submitted_at: datetime            = field(default_factory=datetime.utcnow)
    started_at:  Optional[datetime]   = None
    finished_at: Optional[datetime]   = None
    retry_count: int = 0
    message:     str = ""
    events:      List[str]            = field(default_factory=list)


@dataclass
class Worker:
    """Represents one simulated worker node (Docker container)."""

    worker_id:    str
    container_id: Optional[str]  = None
    cpu_limit:    float          = 2.0
    memory_limit_gb: float       = 4.0
    cpu_used:     float          = 0.0
    memory_used_gb: float        = 0.0
    healthy:      bool           = True
    job_id:       Optional[str]  = None
    last_heartbeat: Optional[datetime] = None
    started_at:   datetime       = field(default_factory=datetime.utcnow)


@dataclass
class ClusterSnapshot:
    """Point-in-time snapshot of cluster state for API responses."""

    total_workers:        int
    idle_workers:         int
    busy_workers:         int
    utilisation_pct:      float
    pending_jobs:         int
    running_jobs:         int
    team_dominant_shares: Dict[str, float]
    timestamp:            datetime = field(default_factory=datetime.utcnow)


# ── REST API request / response models ───────────────────────────────────────


class JobSubmitRequest(BaseModel):
    """
    Request body for POST /jobs.
    All fields are validated before the job enters the scheduler.
    """

    team: str = Field(..., min_length=1, max_length=128)
    name: str = Field(..., min_length=1, max_length=256)
    priority: Priority = Priority.NORMAL
    cpu_cores: float = Field(default=2.0, gt=0, le=128)
    memory_gb: float = Field(default=4.0, gt=0, le=1024)
    num_workers: int = Field(default=1, ge=1, le=64)
    max_retries: int = Field(default=0, ge=0, le=10)
    estimated_duration_seconds: int = Field(default=300, ge=1, le=86400)
    labels: Dict[str, str] = Field(default_factory=dict)

    @field_validator("team", "name")
    @classmethod
    def no_whitespace_only(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be blank")
        return v.strip()

    @field_validator("labels")
    @classmethod
    def validate_labels(cls, v: Dict[str, str]) -> Dict[str, str]:
        if len(v) > 32:
            raise ValueError("labels dict may not exceed 32 entries")
        for k, val in v.items():
            if len(k) > 63 or len(val) > 256:
                raise ValueError("label key ≤ 63 chars, value ≤ 256 chars")
        return v


class JobSubmitResponse(BaseModel):
    job_id:   str
    accepted: bool
    message:  str


class JobStatusResponse(BaseModel):
    job_id:      str
    state:       str
    team:        str
    name:        str
    priority:    int
    worker_ids:  List[str]
    submitted_at: Optional[datetime]
    started_at:  Optional[datetime]
    finished_at: Optional[datetime]
    retry_count: int
    message:     str


class ClusterStatusResponse(BaseModel):
    total_workers:   int
    idle_workers:    int
    busy_workers:    int
    utilisation_pct: float
    pending_jobs:    int
    running_jobs:    int
    team_shares:     Dict[str, float]
