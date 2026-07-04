"""
SQLite-backed job state persistence.

Design choices:
- WAL journal mode for concurrent read/write without blocking.
- Connection pooling via a module-level singleton per db_path.
- Retry on SQLITE_BUSY with exponential back-off (up to 3 attempts).
- All public methods raise LatticeStoreError on unrecoverable DB errors
  so callers can handle gracefully rather than receiving raw sqlite3 exceptions.
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import structlog

from lattice.models import Job, JobState, Priority, ResourceSpec

logger = structlog.get_logger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    team         TEXT NOT NULL,
    name         TEXT NOT NULL,
    priority     INTEGER NOT NULL,
    cpu_cores    REAL,
    memory_gb    REAL,
    num_workers  INTEGER,
    max_retries  INTEGER DEFAULT 0,
    estimated_duration_seconds INTEGER DEFAULT 300,
    checkpoint_path TEXT,
    state        TEXT NOT NULL DEFAULT 'pending',
    worker_ids   TEXT DEFAULT '[]',
    submitted_at TEXT,
    started_at   TEXT,
    finished_at  TEXT,
    retry_count  INTEGER DEFAULT 0,
    message      TEXT DEFAULT ''
)
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS job_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message    TEXT,
    timestamp  TEXT NOT NULL
)
"""

CREATE_SCHEMA_VERSION = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL
)
"""

SCHEMA_VERSION = 1
_MAX_RETRIES = 3
_RETRY_BASE_SLEEP = 0.05  # seconds


# ── Custom exception ──────────────────────────────────────────────────────────


class LatticeStoreError(RuntimeError):
    """Raised when a database operation fails after all retries."""


# ── JobStore ──────────────────────────────────────────────────────────────────


class JobStore:
    """
    Persistent store for job state, backed by SQLite.

    Thread-safe for concurrent reads via WAL mode.
    Retries on SQLITE_BUSY up to _MAX_RETRIES times with back-off.
    """

    def __init__(self, db_path: str = "lattice.db") -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    # ── Connection ────────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _execute(self, sql: str, params=(), *, fetch: str = "none"):
        """
        Execute SQL with retry on SQLITE_BUSY.

        Args:
            sql:    The SQL statement.
            params: Positional parameters.
            fetch:  "none" | "one" | "all"
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with self._conn() as conn:
                    cursor = conn.execute(sql, params)
                    if fetch == "one":
                        return cursor.fetchone()
                    if fetch == "all":
                        return cursor.fetchall()
                    return None
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < _MAX_RETRIES:
                    sleep = _RETRY_BASE_SLEEP * (2 ** attempt)
                    logger.warning(
                        "store.db_locked",
                        attempt=attempt,
                        sleep=sleep,
                        sql=sql[:80],
                    )
                    time.sleep(sleep)
                    last_exc = exc
                else:
                    raise LatticeStoreError(
                        f"DB operation failed after {attempt} attempt(s): {exc}"
                    ) from exc
            except sqlite3.Error as exc:
                raise LatticeStoreError(f"DB error: {exc}") from exc
        raise LatticeStoreError(
            f"DB locked after {_MAX_RETRIES} retries"
        ) from last_exc

    # ── Schema init ───────────────────────────────────────────────────────────

    def _init(self) -> None:
        try:
            with self._conn() as conn:
                conn.execute(CREATE_JOBS)
                conn.execute(CREATE_EVENTS)
                conn.execute(CREATE_SCHEMA_VERSION)
                row = conn.execute(
                    "SELECT MAX(version) as v FROM schema_version"
                ).fetchone()
                current = row["v"] if row and row["v"] is not None else 0
                if current < SCHEMA_VERSION:
                    conn.execute(
                        "INSERT INTO schema_version (version, applied_at) VALUES (?,?)",
                        (SCHEMA_VERSION, datetime.utcnow().isoformat()),
                    )
                    logger.info(
                        "store.schema_upgraded",
                        from_version=current,
                        to_version=SCHEMA_VERSION,
                    )
        except sqlite3.Error as exc:
            raise LatticeStoreError(f"Failed to initialise database: {exc}") from exc

    def health_check(self) -> bool:
        """Return True if the database is reachable and schema is valid."""
        try:
            row = self._execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
                fetch="one",
            )
            return row is not None and row["version"] == SCHEMA_VERSION
        except LatticeStoreError:
            return False

    # ── Write operations ──────────────────────────────────────────────────────

    def save(self, job: Job) -> None:
        """
        Upsert a job record. Safe to call on both new and existing jobs.
        """
        self._execute(
            """
            INSERT OR REPLACE INTO jobs
            (job_id, team, name, priority, cpu_cores, memory_gb,
             num_workers, max_retries, estimated_duration_seconds,
             checkpoint_path, state, worker_ids, submitted_at,
             started_at, finished_at, retry_count, message)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job.job_id, job.team, job.name, job.priority.value,
                job.resources.cpu_cores, job.resources.memory_gb,
                job.num_workers, job.max_retries,
                job.estimated_duration_seconds, job.checkpoint_path,
                job.state.value, json.dumps(job.worker_ids),
                self._ts(job.submitted_at), self._ts(job.started_at),
                self._ts(job.finished_at), job.retry_count, job.message,
            ),
        )

    def update_state(
        self,
        job_id: str,
        state: JobState,
        message: str = "",
        worker_ids: Optional[List[str]] = None,
        checkpoint_path: Optional[str] = None,
    ) -> None:
        """
        Update job state (and optionally worker IDs / checkpoint path).
        Sets started_at / finished_at timestamps automatically.
        """
        now = datetime.utcnow().isoformat()
        fields: Dict[str, object] = {"state": state.value, "message": message}
        if worker_ids is not None:
            fields["worker_ids"] = json.dumps(worker_ids)
        if checkpoint_path is not None:
            fields["checkpoint_path"] = checkpoint_path
        if state == JobState.RUNNING:
            fields["started_at"] = now
        elif state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED,
                       JobState.PREEMPTED):
            fields["finished_at"] = now

        set_clause = ", ".join(f"{k}=?" for k in fields)
        values = list(fields.values()) + [job_id]
        self._execute(
            f"UPDATE jobs SET {set_clause} WHERE job_id=?", values
        )

    def increment_retry(self, job_id: str) -> None:
        """Atomically increment the retry counter for a job."""
        self._execute(
            "UPDATE jobs SET retry_count=retry_count+1 WHERE job_id=?",
            (job_id,),
        )

    def log_event(
        self, job_id: str, event_type: str, message: str = ""
    ) -> None:
        """Append an audit event to the job event log."""
        self._execute(
            """
            INSERT INTO job_events (job_id, event_type, message, timestamp)
            VALUES (?,?,?,?)
            """,
            (job_id, event_type, message, datetime.utcnow().isoformat()),
        )

    # ── Read operations ───────────────────────────────────────────────────────

    def get(self, job_id: str) -> Optional[Job]:
        """Return a Job by ID, or None if not found."""
        row = self._execute(
            "SELECT * FROM jobs WHERE job_id=?", (job_id,), fetch="one"
        )
        if row is None:
            return None
        return self._row_to_job(dict(row))

    def list_jobs(
        self,
        team: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Job]:
        """
        List jobs with optional filters.

        Args:
            team:  Filter to a specific team name.
            state: Filter to a specific JobState value string.
            limit: Max records to return (server-side cap: 1000).
        """
        # Validate state value to avoid silent empty results
        if state is not None:
            valid_states = {s.value for s in JobState}
            if state not in valid_states:
                raise ValueError(
                    f"Invalid state '{state}'. Valid values: {sorted(valid_states)}"
                )

        limit = min(limit, 1000)  # server-side cap
        query = "SELECT * FROM jobs WHERE 1=1"
        params: List[object] = []
        if team:
            query += " AND team=?"
            params.append(team)
        if state:
            query += " AND state=?"
            params.append(state)
        query += " ORDER BY submitted_at DESC LIMIT ?"
        params.append(limit)

        rows = self._execute(query, params, fetch="all") or []
        return [self._row_to_job(dict(r)) for r in rows]

    def get_events(self, job_id: str, limit: int = 50) -> List[dict]:
        """Return audit events for a job, newest first."""
        rows = self._execute(
            """
            SELECT * FROM job_events WHERE job_id=?
            ORDER BY timestamp DESC LIMIT ?
            """,
            (job_id, limit),
            fetch="all",
        )
        return [dict(r) for r in (rows or [])]

    def running_jobs_by_team(self) -> Dict[str, Dict[str, float]]:
        """Return CPU/memory totals for running jobs grouped by team."""
        rows = self._execute(
            """
            SELECT team,
                   SUM(cpu_cores)  AS cpu,
                   SUM(memory_gb)  AS mem
            FROM jobs WHERE state='running'
            GROUP BY team
            """,
            fetch="all",
        )
        return {
            r["team"]: {"cpu": r["cpu"] or 0.0, "mem": r["mem"] or 0.0}
            for r in (rows or [])
        }

    def reconcile_orphaned_jobs(self) -> int:
        """
        On startup, mark any jobs that were left in RUNNING state as FAILED.
        This handles the case where the scheduler process crashed while jobs
        were running — prevents them staying 'running' forever.

        Returns the number of jobs reconciled.
        """
        rows = self._execute(
            "SELECT job_id FROM jobs WHERE state=?",
            ("running",),
            fetch="all",
        ) or []
        for row in rows:
            self.update_state(
                row["job_id"],
                JobState.FAILED,
                message="Scheduler restarted — job state unknown",
            )
            self.log_event(
                row["job_id"],
                "reconciled",
                "Marked failed on scheduler restart",
            )
        if rows:
            logger.warning(
                "store.reconciled_orphans",
                count=len(rows),
                job_ids=[r["job_id"] for r in rows],
            )
        return len(rows)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _row_to_job(self, row: dict) -> Job:
        job = Job(
            job_id=row["job_id"],
            team=row["team"],
            name=row["name"],
            priority=Priority(row["priority"]),
            resources=ResourceSpec(
                cpu_cores=row["cpu_cores"] or 2.0,
                memory_gb=row["memory_gb"] or 4.0,
            ),
            num_workers=row.get("num_workers") or 1,
            max_retries=row.get("max_retries") or 0,
            estimated_duration_seconds=row.get("estimated_duration_seconds") or 300,
            checkpoint_path=row.get("checkpoint_path"),
            state=JobState(row["state"]),
            worker_ids=json.loads(row.get("worker_ids") or "[]"),
            retry_count=row.get("retry_count") or 0,
            message=row.get("message") or "",
        )
        if row.get("submitted_at"):
            job.submitted_at = datetime.fromisoformat(row["submitted_at"])
        if row.get("started_at"):
            job.started_at = datetime.fromisoformat(row["started_at"])
        if row.get("finished_at"):
            job.finished_at = datetime.fromisoformat(row["finished_at"])
        return job

    @staticmethod
    def _ts(dt: Optional[datetime]) -> Optional[str]:
        return dt.isoformat() if dt else None
