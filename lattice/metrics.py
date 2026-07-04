from __future__ import annotations
import time
from prometheus_client import Counter, Histogram, Gauge

JOBS_SUBMITTED = Counter(
    "lattice_jobs_submitted_total",
    "Total jobs submitted",
    ["team", "priority"],
)

JOBS_COMPLETED = Counter(
    "lattice_jobs_completed_total",
    "Total jobs completed",
    ["team", "status"],
)

PREEMPTIONS = Counter(
    "lattice_preemptions_total",
    "Total preemption events",
    ["preempted_priority", "preemptor_priority"],
)

GANG_ATTEMPTS = Counter(
    "lattice_gang_schedule_attempts_total",
    "Gang scheduling attempt outcomes",
    ["result"],  # success | blocked
)

BACKFILL_SCHEDULED = Counter(
    "lattice_backfill_jobs_total",
    "Jobs scheduled via backfill",
)

SCHEDULER_TICKS = Counter(
    "lattice_scheduler_ticks_total",
    "Total scheduler loop ticks",
)

JOB_WAIT_TIME = Histogram(
    "lattice_job_wait_seconds",
    "Time from submission to start",
    ["team", "priority"],
    buckets=[1, 5, 15, 30, 60, 120, 300, 600, 1800],
)

JOB_DURATION = Histogram(
    "lattice_job_duration_seconds",
    "Job wall-clock duration",
    ["team"],
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600],
)

CLUSTER_UTILISATION = Gauge(
    "lattice_cluster_utilisation_ratio",
    "Fraction of workers currently busy (0–1)",
)

QUEUE_DEPTH = Gauge(
    "lattice_queue_depth",
    "Number of pending jobs per priority",
    ["priority"],
)

WORKER_STATE = Gauge(
    "lattice_workers",
    "Worker count by state",
    ["state"],  # idle | busy | unhealthy
)

DRF_DOMINANT_SHARE = Gauge(
    "lattice_drf_dominant_share",
    "Dominant resource share per team (0–1)",
    ["team"],
)

UPTIME = Gauge("lattice_uptime_seconds", "Scheduler uptime in seconds")
_START = time.time()


def update_uptime() -> float:
    elapsed = time.time() - _START
    UPTIME.set(elapsed)
    return elapsed
