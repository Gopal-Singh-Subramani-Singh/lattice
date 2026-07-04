"""
Lattice configuration.

Priority (highest → lowest):
  1. Environment variables  (LATTICE_*)
  2. config/config.yaml
  3. Hardcoded defaults

Run `cp .env.example .env` and set secrets there; they are loaded
automatically when you start the process (or via Docker Compose env_file).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import yaml

_CONFIG_PATH = Path(__file__).parent / "config.yaml"
_cfg: Optional[dict] = None

# ── Internal helpers ──────────────────────────────────────────────────────────


def _load_yaml() -> dict:
    global _cfg
    if _cfg is None:
        if _CONFIG_PATH.exists():
            with open(_CONFIG_PATH) as f:
                _cfg = yaml.safe_load(f) or {}
        else:
            _cfg = {}
    return _cfg


def _env(key: str) -> Optional[str]:
    """Return stripped env var or None."""
    v = os.environ.get(key)
    return v.strip() if v else None


def _yaml(section: str, key: str, default=None):
    return _load_yaml().get(section, {}).get(key, default)


# ── Public accessors ──────────────────────────────────────────────────────────

_VALID_ALGORITHMS = {"fifo", "drf", "gang", "backfill"}


def scheduler_algorithm() -> str:
    v = _env("LATTICE_ALGORITHM") or _yaml("scheduler", "algorithm", "drf")
    if v not in _VALID_ALGORITHMS:
        raise ValueError(
            f"Invalid LATTICE_ALGORITHM '{v}'. "
            f"Must be one of: {sorted(_VALID_ALGORITHMS)}"
        )
    return v


def tick_interval_ms() -> int:
    v = _env("LATTICE_TICK_INTERVAL_MS")
    return int(v) if v else _yaml("scheduler", "tick_interval_ms", 500)


def cluster_cpu() -> float:
    v = _env("LATTICE_CLUSTER_CPU")
    return float(v) if v else _yaml("cluster", "total_cpu_cores", 16.0)


def cluster_mem() -> float:
    v = _env("LATTICE_CLUSTER_MEM")
    return float(v) if v else _yaml("cluster", "total_memory_gb", 32.0)


def max_workers() -> int:
    v = _env("LATTICE_MAX_WORKERS")
    return int(v) if v else _yaml("cluster", "max_workers", 8)


def redis_url() -> str:
    return _env("LATTICE_REDIS_URL") or _yaml("redis", "url", "redis://localhost:6379")


def sqlite_path() -> str:
    return _env("LATTICE_DB_PATH") or _yaml("sqlite", "db_path", "lattice.db")


def rest_port() -> int:
    v = _env("LATTICE_REST_PORT")
    return int(v) if v else _yaml("api", "rest_port", 8002)


def grpc_port() -> int:
    v = _env("LATTICE_GRPC_PORT")
    return int(v) if v else _yaml("api", "grpc_port", 50051)


def api_host() -> str:
    return _env("LATTICE_API_HOST") or _yaml("api", "host", "0.0.0.0")


def preemption_enabled() -> bool:
    v = _env("LATTICE_PREEMPTION_ENABLED")
    if v is not None:
        return v.lower() in ("1", "true", "yes")
    return bool(_yaml("scheduler", "preemption_enabled", True))


def backfill_enabled() -> bool:
    v = _env("LATTICE_BACKFILL_ENABLED")
    if v is not None:
        return v.lower() in ("1", "true", "yes")
    return bool(_yaml("scheduler", "backfill_enabled", True))


def gang_scheduling_enabled() -> bool:
    v = _env("LATTICE_GANG_ENABLED")
    if v is not None:
        return v.lower() in ("1", "true", "yes")
    return bool(_yaml("scheduler", "gang_scheduling_enabled", True))


def priority_gap() -> int:
    v = _env("LATTICE_PRIORITY_GAP")
    return int(v) if v else _yaml("preemption", "priority_gap", 2)


def checkpoint_timeout() -> int:
    v = _env("LATTICE_CHECKPOINT_TIMEOUT")
    return int(v) if v else _yaml("preemption", "checkpoint_timeout_seconds", 30)


def worker_cpu_limit() -> str:
    return _env("LATTICE_WORKER_CPU") or _yaml("workers", "cpu_limit", "2")


def worker_memory_limit() -> str:
    return _env("LATTICE_WORKER_MEMORY") or _yaml("workers", "memory_limit", "4g")


def worker_image() -> str:
    return _env("LATTICE_WORKER_IMAGE") or _yaml("workers", "image", "lattice-worker:latest")


def heartbeat_timeout() -> int:
    v = _env("LATTICE_HEARTBEAT_TIMEOUT")
    return int(v) if v else _yaml("workers", "heartbeat_timeout_seconds", 30)


def api_keys() -> List[str]:
    """Return list of valid API keys. Empty list = auth disabled."""
    raw = _env("LATTICE_API_KEYS") or ""
    return [k.strip() for k in raw.split(",") if k.strip()]


def log_format() -> str:
    return _env("LATTICE_LOG_FORMAT") or "production"


def log_level() -> str:
    return _env("LATTICE_LOG_LEVEL") or "INFO"


def validate_all() -> None:
    """
    Eagerly validate all configuration at startup.
    Raises ValueError with a clear message on the first bad value.
    """
    scheduler_algorithm()  # validates allowed values

    w = max_workers()
    if w < 1 or w > 256:
        raise ValueError(f"LATTICE_MAX_WORKERS={w} must be between 1 and 256")

    t = tick_interval_ms()
    if t < 50 or t > 60_000:
        raise ValueError(f"tick_interval_ms={t} must be between 50 and 60000")

    p = rest_port()
    if p < 1024 or p > 65535:
        raise ValueError(f"rest_port={p} out of range")

    g = grpc_port()
    if g < 1024 or g > 65535:
        raise ValueError(f"grpc_port={g} out of range")

    if p == g:
        raise ValueError("rest_port and grpc_port must be different")
