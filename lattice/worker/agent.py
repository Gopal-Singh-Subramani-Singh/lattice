"""
Worker Agent — runs inside each Docker container.
Reports resource usage to the scheduler via gRPC heartbeats.
Handles SIGUSR1 → checkpoint → notify scheduler.
"""
from __future__ import annotations
import asyncio
import os
import signal
import sys
import time
import uuid
from pathlib import Path
import psutil
import structlog

logger = structlog.get_logger(__name__)

WORKER_ID = os.environ.get("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
SCHEDULER_HOST = os.environ.get("SCHEDULER_HOST", "host.docker.internal")
SCHEDULER_PORT = int(os.environ.get("SCHEDULER_PORT", "50051"))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))

_checkpoint_requested = False


def _handle_sigusr1(signum, frame):
    """Signal handler: request checkpoint on next iteration."""
    global _checkpoint_requested
    _checkpoint_requested = True
    logger.info("agent.sigusr1_received", worker_id=WORKER_ID)


def _do_checkpoint(checkpoint_path: str) -> bool:
    """
    Simulate checkpoint save.
    In production this would call torch.save(model.state_dict(), path).
    """
    global _checkpoint_requested
    try:
        Path(checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
        # Simulate torch checkpoint
        checkpoint_data = {
            "worker_id": WORKER_ID,
            "timestamp": time.time(),
            "epoch": 0,
            "step": 0,
        }
        with open(checkpoint_path, "w") as f:
            import json
            json.dump(checkpoint_data, f)
        logger.info(
            "agent.checkpoint_saved",
            path=checkpoint_path,
            worker=WORKER_ID,
        )
        _checkpoint_requested = False
        return True
    except Exception as exc:
        logger.error("agent.checkpoint_failed", error=str(exc))
        _checkpoint_requested = False
        return False


async def _send_heartbeat_grpc(stub, cpu_pct: float, mem_gb: float):
    """Send heartbeat to scheduler gRPC service."""
    try:
        # Import generated proto stubs
        sys.path.insert(0, "/app/proto_gen")
        import lattice_pb2
        import lattice_pb2_grpc

        req = lattice_pb2.HeartbeatRequest(
            worker_id=WORKER_ID,
            cpu_percent=cpu_pct,
            memory_gb=mem_gb,
            healthy=True,
        )
        resp = await stub.Heartbeat(req)
        return resp.ack
    except Exception as exc:
        logger.warning("agent.heartbeat_failed", error=str(exc))
        return False


async def run():
    """Main agent loop."""
    global _checkpoint_requested

    # Register SIGUSR1 handler for checkpoint
    signal.signal(signal.SIGUSR1, _handle_sigusr1)
    logger.info(
        "agent.started",
        worker_id=WORKER_ID,
        scheduler=f"{SCHEDULER_HOST}:{SCHEDULER_PORT}",
    )

    # Try to connect to scheduler gRPC
    stub = None
    try:
        import grpc
        sys.path.insert(0, "/app/proto_gen")
        import lattice_pb2_grpc
        channel = grpc.aio.insecure_channel(
            f"{SCHEDULER_HOST}:{SCHEDULER_PORT}"
        )
        stub = lattice_pb2_grpc.LatticeSchedulerStub(channel)
        logger.info("agent.grpc_connected")
    except Exception as exc:
        logger.warning("agent.grpc_unavailable", error=str(exc))

    checkpoint_path = f"/tmp/lattice/checkpoints/{WORKER_ID}.ckpt"

    while True:
        try:
            # Collect resource metrics
            cpu_pct = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            mem_gb = mem.used / (1024 ** 3)

            # Handle checkpoint request
            if _checkpoint_requested:
                _do_checkpoint(checkpoint_path)

            # Send heartbeat
            if stub:
                await _send_heartbeat_grpc(stub, cpu_pct, mem_gb)
            else:
                logger.debug(
                    "agent.heartbeat_local",
                    worker=WORKER_ID,
                    cpu=round(cpu_pct, 1),
                    mem_gb=round(mem_gb, 2),
                )

        except Exception as exc:
            logger.error("agent.loop_error", error=str(exc))

        await asyncio.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run())
