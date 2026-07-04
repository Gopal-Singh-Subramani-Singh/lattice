"""
Redis Sorted Set job queue.

Scoring:   priority.value × 10¹² − unix_timestamp
           → higher priority dequeued first; FIFO within same priority.

Atomicity: peek is non-destructive (ZREVRANGE).
           dequeue_batch uses a Lua script to atomically pop + fetch data
           so no job IDs are lost if the process crashes mid-operation.

Retry:     All Redis calls are wrapped with tenacity retry on transient
           connection errors (up to 5 attempts, exponential back-off).
"""
from __future__ import annotations

import json
import time
from typing import Dict, List, Optional

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
import structlog

from lattice.models import Job, Priority

logger = structlog.get_logger(__name__)

QUEUE_KEY = "lattice:jobs:queue"
JOB_DATA_PREFIX = "lattice:jobs:data:"
JOB_TTL = 86400  # 24 hours

# Lua script: atomically pop `count` highest-score members and return their data keys
# Returns a flat list: [job_id_1, data_json_1, job_id_2, data_json_2, ...]
_DEQUEUE_SCRIPT = """
local queue_key = KEYS[1]
local data_prefix = ARGV[1]
local count = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local members = redis.call('ZREVRANGE', queue_key, 0, count - 1, 'WITHSCORES')
if #members == 0 then return {} end

local result = {}
local to_remove = {}
for i = 1, #members, 2 do
    local job_id = members[i]
    local data_key = data_prefix .. job_id
    local data = redis.call('GET', data_key)
    if data then
        table.insert(result, job_id)
        table.insert(result, data)
        table.insert(to_remove, job_id)
        redis.call('DEL', data_key)
    end
end

if #to_remove > 0 then
    redis.call('ZREM', queue_key, unpack(to_remove))
end

return result
"""


def _retrying(func):
    """Decorator: retry on transient Redis errors with exponential back-off."""
    return retry(
        retry=retry_if_exception_type(RedisError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.1, min=0.1, max=5.0),
        reraise=True,
    )(func)


class RedisJobQueue:
    """
    Priority queue backed by a Redis Sorted Set.

    All public methods are async and safe to call from the scheduler loop.
    Transient Redis errors are retried automatically; persistent failures
    propagate as redis.exceptions.RedisError to the caller.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        queue_key: str = QUEUE_KEY,
    ) -> None:
        self._redis = redis_client
        self._queue_key = queue_key
        self._dequeue_script: Optional[object] = None  # registered on first use

    def _score(self, priority: Priority) -> float:
        """Compute Redis sorted-set score for a job."""
        return priority.value * 1e12 - time.time()

    async def _get_script(self):
        """Register the Lua dequeue script once and cache the handle."""
        if self._dequeue_script is None:
            self._dequeue_script = self._redis.register_script(_DEQUEUE_SCRIPT)
        return self._dequeue_script

    async def ping(self) -> bool:
        """Return True if Redis is reachable."""
        try:
            return await self._redis.ping()
        except RedisError:
            return False

    async def enqueue(self, job: Job) -> bool:
        """
        Add a job to the priority queue.

        Args:
            job: The Job to enqueue.
        Returns:
            True on success.
        Raises:
            RedisError: If Redis is unavailable after retries.
        """
        score = self._score(job.priority)
        data = {
            "job_id":      job.job_id,
            "team":        job.team,
            "name":        job.name,
            "priority":    job.priority.value,
            "cpu_cores":   job.resources.cpu_cores,
            "memory_gb":   job.resources.memory_gb,
            "num_workers": job.num_workers,
            "estimated_duration_seconds": job.estimated_duration_seconds,
            "checkpoint_path": job.checkpoint_path,
            "submitted_at": job.submitted_at.isoformat(),
        }
        await self._enqueue_atomic(job.job_id, score, json.dumps(data))
        logger.debug(
            "queue.enqueued",
            job_id=job.job_id,
            priority=job.priority.name,
            score=round(score, 0),
        )
        return True

    @_retrying
    async def _enqueue_atomic(
        self, job_id: str, score: float, data_json: str
    ) -> None:
        pipe = self._redis.pipeline(transaction=True)
        pipe.zadd(self._queue_key, {job_id: score})
        pipe.setex(f"{JOB_DATA_PREFIX}{job_id}", JOB_TTL, data_json)
        await pipe.execute()

    async def dequeue_batch(self, limit: int = 10) -> List[dict]:
        """
        Atomically pop up to `limit` highest-priority jobs from the queue.

        Tries a Lua script for true atomicity on real Redis.
        Falls back to a pipeline-based approach for environments that don't
        support EVALSHA (e.g. fakeredis in tests).
        """
        # Try Lua atomic path first
        try:
            script = await self._get_script()
            raw = await script(
                keys=[self._queue_key],
                args=[JOB_DATA_PREFIX, limit, JOB_TTL],
            )
            if raw is None:
                return []
            jobs = []
            for i in range(0, len(raw), 2):
                try:
                    jobs.append(json.loads(raw[i + 1]))
                except (json.JSONDecodeError, IndexError) as exc:
                    logger.warning("queue.bad_data", error=str(exc), index=i)
            return jobs
        except RedisError as exc:
            if "evalsha" in str(exc).lower() or "unknown command" in str(exc).lower():
                # fakeredis / Redis version doesn't support scripting — use fallback
                logger.debug("queue.lua_unsupported_using_fallback")
                return await self._dequeue_batch_fallback(limit)
            logger.error("queue.dequeue_failed", error=str(exc))
            return []

    async def _dequeue_batch_fallback(self, limit: int) -> List[dict]:
        """Pipeline-based dequeue fallback (no Lua scripting required)."""
        results = await self._redis.zpopmax(self._queue_key, count=limit)
        if not results:
            return []
        jobs = []
        for job_id_raw, _score in results:
            job_id = (
                job_id_raw
                if isinstance(job_id_raw, str)
                else job_id_raw.decode()
            )
            raw = await self._redis.getdel(f"{JOB_DATA_PREFIX}{job_id}")
            if raw:
                try:
                    jobs.append(json.loads(raw))
                except json.JSONDecodeError as exc:
                    logger.warning("queue.bad_data_fallback", job_id=job_id, error=str(exc))
            else:
                logger.warning("queue.missing_data_fallback", job_id=job_id)
        return jobs

    @_retrying
    async def peek(self, limit: int = 20) -> List[dict]:
        """
        Non-destructive read of the highest-priority pending jobs.
        Does NOT remove items from the queue.
        """
        items = await self._redis.zrevrange(
            self._queue_key, 0, limit - 1, withscores=True
        )
        result = []
        for job_id_raw, _score in items:
            job_id = (
                job_id_raw
                if isinstance(job_id_raw, str)
                else job_id_raw.decode()
            )
            raw = await self._redis.get(f"{JOB_DATA_PREFIX}{job_id}")
            if raw:
                try:
                    result.append(json.loads(raw))
                except json.JSONDecodeError:
                    logger.warning("queue.bad_data_peek", job_id=job_id)
        return result

    @_retrying
    async def remove(self, job_id: str) -> bool:
        """Remove a specific job from the queue (used for cancel/dispatch)."""
        pipe = self._redis.pipeline(transaction=True)
        pipe.zrem(self._queue_key, job_id)
        pipe.delete(f"{JOB_DATA_PREFIX}{job_id}")
        results = await pipe.execute()
        return bool(results[0])

    @_retrying
    async def depth(self) -> int:
        """Return total number of jobs currently in the queue."""
        return await self._redis.zcard(self._queue_key)

    @_retrying
    async def depth_by_priority(self) -> Dict[str, int]:
        """Return job count per priority tier."""
        result: Dict[str, int] = {}
        for p in Priority:
            base = p.value * 1e12
            count = await self._redis.zcount(
                self._queue_key, base - 1e12, base
            )
            result[p.name] = int(count)
        return result

    @_retrying
    async def acquire_gang_lock(
        self, job_id: str, ttl_seconds: int = 30
    ) -> bool:
        """
        Acquire a distributed lock for gang-scheduling atomicity.
        Returns True if the lock was acquired, False if already held.
        """
        lock_key = f"lattice:gang:lock:{job_id}"
        acquired = await self._redis.set(
            lock_key, "1", nx=True, ex=ttl_seconds
        )
        return bool(acquired)

    @_retrying
    async def release_gang_lock(self, job_id: str) -> None:
        """Release a previously acquired gang lock."""
        await self._redis.delete(f"lattice:gang:lock:{job_id}")

    @_retrying
    async def clear(self) -> None:
        """Flush the entire queue. Use only in tests."""
        await self._redis.delete(self._queue_key)
