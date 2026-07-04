"""
Utilisation analysis and charts.
Generates a comparison chart showing scheduling algorithm performance.

Usage: python simulation/utilisation_report.py
Output: utilisation_report.png
"""
from __future__ import annotations
import asyncio
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import structlog

logger = structlog.get_logger(__name__)

# ── Simulation parameters ────────────────────────────────────────────────────

NUM_WORKERS = 8
NUM_JOBS = 200
TOTAL_CPU = NUM_WORKERS * 2.0  # 2 CPU per worker
TOTAL_MEM = NUM_WORKERS * 4.0  # 4 GB per worker
SIM_DURATION = 600             # seconds to simulate


def generate_jobs(n: int) -> List[dict]:
    """Generate a synthetic workload mix."""
    random.seed(42)
    jobs = []
    priorities = [0, 1, 1, 2, 3]  # BATCH, NORMAL×2, HIGH, CRITICAL

    for i in range(n):
        cpu = random.choice([1.0, 2.0, 2.0, 4.0])
        mem = random.choice([2.0, 4.0, 4.0, 8.0])
        jobs.append({
            "job_id": f"job-{i:04d}",
            "team": f"team_{chr(65 + (i % 8))}",
            "priority": random.choice(priorities),
            "cpu_cores": cpu,
            "memory_gb": mem,
            "num_workers": random.choices([1, 1, 2, 4], weights=[60, 20, 15, 5])[0],
            "estimated_duration": random.randint(30, 600),
            "submitted_at": random.uniform(0, SIM_DURATION * 0.7),
        })

    # Sort by submission time
    jobs.sort(key=lambda j: j["submitted_at"])
    return jobs


def simulate_fifo(jobs: List[dict]) -> List[float]:
    """Simulate FIFO scheduling and return utilisation time series."""
    return _simulate("fifo", jobs)


def simulate_drf(jobs: List[dict]) -> List[float]:
    """Simulate DRF scheduling and return utilisation time series."""
    return _simulate("drf", jobs)


def _simulate(algorithm: str, jobs: List[dict]) -> List[float]:
    """
    Simplified discrete-event simulation.
    Returns utilisation ratio sampled every 10 seconds.
    """
    samples = []
    time_points = list(range(0, SIM_DURATION, 10))

    # Track running jobs as (finish_time, cpu_used)
    running: List[Dict] = []
    pending = list(jobs)
    available_workers = NUM_WORKERS

    team_cpu: Dict[str, float] = {}  # for DRF

    for t in time_points:
        # Complete finished jobs
        newly_finished = [r for r in running if r["finish_time"] <= t]
        for r in newly_finished:
            available_workers += r["num_workers"]
            if algorithm == "drf":
                team = r["team"]
                team_cpu[team] = max(0.0, team_cpu.get(team, 0.0) - r["cpu_cores"])
        running = [r for r in running if r["finish_time"] > t]

        # Collect newly arrived jobs
        arrived = [j for j in pending if j["submitted_at"] <= t]
        pending = [j for j in pending if j["submitted_at"] > t]

        # Sort pending by algorithm
        if algorithm == "fifo":
            arrived.sort(key=lambda j: (-j["priority"], j["submitted_at"]))
        elif algorithm == "drf":
            # Sort by dominant share ascending (fair-share)
            arrived.sort(key=lambda j: (
                team_cpu.get(j["team"], 0.0) / max(TOTAL_CPU, 1e-9),
                -j["priority"],
            ))

        # Schedule jobs that fit
        for job in arrived:
            workers_needed = job["num_workers"]
            if available_workers >= workers_needed:
                available_workers -= workers_needed
                finish_t = t + job["estimated_duration"]
                running.append({
                    "job_id": job["job_id"],
                    "team": job["team"],
                    "num_workers": workers_needed,
                    "cpu_cores": job["cpu_cores"],
                    "finish_time": finish_t,
                })
                if algorithm == "drf":
                    team = job["team"]
                    team_cpu[team] = team_cpu.get(team, 0.0) + job["cpu_cores"]
            else:
                # Put back in pending for next tick
                pending.append(job)

        # Sample utilisation
        busy = NUM_WORKERS - available_workers
        utilisation = busy / NUM_WORKERS
        samples.append(utilisation)

    return samples


def plot_comparison(
    fifo_samples: List[float],
    drf_samples: List[float],
    output_path: str = "utilisation_report.png",
):
    """Generate a comparison chart."""
    time_axis = list(range(0, SIM_DURATION, 10))

    fig, axes = plt.subplots(2, 1, figsize=(14, 9))
    fig.suptitle(
        "Lattice — Scheduling Algorithm Comparison\n"
        f"{NUM_JOBS} jobs, {NUM_WORKERS} workers, {SIM_DURATION}s simulation",
        fontsize=14,
        fontweight="bold",
    )

    # ── Top: utilisation over time ─────────────────────────────────────────
    ax1 = axes[0]
    ax1.plot(time_axis, [u * 100 for u in fifo_samples],
             color="#e74c3c", linewidth=1.5, label="FIFO", alpha=0.8)
    ax1.plot(time_axis, [u * 100 for u in drf_samples],
             color="#2ecc71", linewidth=1.5, label="DRF", alpha=0.8)
    ax1.axhline(y=80, color="#f39c12", linestyle="--", linewidth=1, label="80% target")
    ax1.fill_between(time_axis, [u * 100 for u in drf_samples],
                     [u * 100 for u in fifo_samples],
                     where=[d > f for d, f in zip(drf_samples, fifo_samples)],
                     alpha=0.15, color="#2ecc71", label="DRF advantage")
    ax1.set_ylabel("Cluster Utilisation (%)", fontsize=11)
    ax1.set_ylim(0, 105)
    ax1.legend(loc="lower right")
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Worker Utilisation Over Time")

    # ── Bottom: rolling average comparison ────────────────────────────────
    ax2 = axes[1]
    window = 6  # 60s rolling window

    def rolling_avg(data, w):
        return [
            sum(data[max(0, i-w):i+1]) / len(data[max(0, i-w):i+1])
            for i in range(len(data))
        ]

    fifo_roll = rolling_avg(fifo_samples, window)
    drf_roll = rolling_avg(drf_samples, window)

    fifo_avg = sum(fifo_samples) / len(fifo_samples) * 100
    drf_avg = sum(drf_samples) / len(drf_samples) * 100
    lift = drf_avg - fifo_avg

    ax2.bar(
        [0, 1],
        [fifo_avg, drf_avg],
        color=["#e74c3c", "#2ecc71"],
        width=0.5,
        alpha=0.85,
    )
    ax2.text(0, fifo_avg + 0.5, f"{fifo_avg:.1f}%", ha="center", fontsize=12)
    ax2.text(1, drf_avg + 0.5, f"{drf_avg:.1f}%", ha="center", fontsize=12)
    ax2.text(
        0.5, max(fifo_avg, drf_avg) + 3,
        f"↑ {lift:.1f}% utilisation lift with DRF",
        ha="center", fontsize=12, color="#2ecc71", fontweight="bold",
    )
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels(["FIFO", "DRF"], fontsize=12)
    ax2.set_ylabel("Average Utilisation (%)", fontsize=11)
    ax2.set_ylim(0, 100)
    ax2.set_title("Average Utilisation: FIFO vs DRF")
    ax2.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nChart saved to: {output_path}")
    print(f"  FIFO average utilisation: {fifo_avg:.1f}%")
    print(f"  DRF  average utilisation: {drf_avg:.1f}%")
    print(f"  Utilisation lift:         +{lift:.1f}%")


def main():
    print("Generating utilisation analysis...")
    jobs = generate_jobs(NUM_JOBS)
    print(f"  Generated {len(jobs)} jobs across 8 teams")

    print("  Simulating FIFO...")
    fifo = simulate_fifo(jobs)

    print("  Simulating DRF...")
    drf = simulate_drf(jobs)

    print("  Generating chart...")
    plot_comparison(fifo, drf)


if __name__ == "__main__":
    main()
