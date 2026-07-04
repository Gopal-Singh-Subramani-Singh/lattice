"""
200-job stress simulation.
Demonstrates DRF fairness and utilisation improvement.

Usage: python simulation/stress_test.py
Requires: Lattice running at http://localhost:8002
"""
from __future__ import annotations
import asyncio
import random
import time
from datetime import datetime
import httpx
import structlog

logger = structlog.get_logger(__name__)

BASE_URL = "http://localhost:8002"
TEAMS = [f"team_{chr(65+i)}" for i in range(8)]  # team_A .. team_H
PRIORITIES = ["BATCH", "NORMAL", "NORMAL", "HIGH", "CRITICAL"]
N_JOBS = 200


async def submit_job(client: httpx.AsyncClient, i: int) -> dict:
    team = random.choice(TEAMS)
    priority_name = random.choice(PRIORITIES)
    priority_val = {"BATCH": 0, "NORMAL": 1, "HIGH": 2, "CRITICAL": 3}[priority_name]
    payload = {
        "team": team,
        "name": f"job-{i:04d}",
        "priority": priority_val,
        "cpu_cores": random.choice([1.0, 2.0, 2.0, 4.0]),
        "memory_gb": random.choice([2.0, 4.0, 4.0, 8.0]),
        "num_workers": random.choices([1, 1, 1, 2, 4], weights=[50, 30, 10, 7, 3])[0],
        "estimated_duration_seconds": random.randint(30, 600),
        "max_retries": random.choice([0, 0, 1]),
    }
    try:
        resp = await client.post(f"{BASE_URL}/jobs", json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


async def get_cluster_status(client: httpx.AsyncClient) -> dict:
    try:
        resp = await client.get(f"{BASE_URL}/cluster", timeout=5)
        return resp.json()
    except Exception:
        return {}


async def run_simulation():
    print(f"\n{'='*60}")
    print(f"LATTICE STRESS TEST — {N_JOBS} jobs, {len(TEAMS)} teams")
    print(f"{'='*60}\n")

    async with httpx.AsyncClient() as client:
        # Check connectivity
        try:
            await client.get(f"{BASE_URL}/health", timeout=3)
        except Exception:
            print("Lattice not running. Start with:")
            print("  uvicorn lattice.api.rest_api:app --port 8002")
            return

        print(f"Submitting {N_JOBS} jobs in waves...\n")
        start_time = time.monotonic()
        submitted = 0
        errors = 0

        # Submit in waves of 20
        for wave in range(0, N_JOBS, 20):
            wave_jobs = min(20, N_JOBS - wave)
            tasks = [submit_job(client, wave + i) for i in range(wave_jobs)]
            results = await asyncio.gather(*tasks)
            for r in results:
                if "error" in r:
                    errors += 1
                else:
                    submitted += 1

            status = await get_cluster_status(client)
            util = status.get("utilisation_pct", 0)
            pending = status.get("pending_jobs", 0)
            running = status.get("running_jobs", 0)
            print(
                f"Wave {wave//20+1:2d}: submitted={submitted:3d} | "
                f"pending={pending:3d} | running={running:2d} | "
                f"utilisation={util:.1f}%"
            )
            await asyncio.sleep(2.0)

        total_time = time.monotonic() - start_time
        print(f"\n{'='*60}")
        print(f"Simulation complete in {total_time:.1f}s")
        print(f"Submitted: {submitted} | Errors: {errors}")

        # Final cluster state
        status = await get_cluster_status(client)
        print(f"\nFinal cluster state:")
        print(f"  Utilisation:  {status.get('utilisation_pct', 0):.1f}%")
        print(f"  Pending jobs: {status.get('pending_jobs', 0)}")
        print(f"  Running jobs: {status.get('running_jobs', 0)}")
        print("\nDRF team shares:")
        for team, share in (status.get("team_shares") or {}).items():
            print(f"  {team}: {share:.3f} ({share*100:.1f}%)")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(run_simulation())
