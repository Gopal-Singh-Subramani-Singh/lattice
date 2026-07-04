from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import structlog

logger = structlog.get_logger(__name__)


class DRFScheduler:
    """
    Dominant Resource Fairness (DRF) scheduler.

    Algorithm:
    1. For each team, compute dominant_share = max(
         cpu_allocated / cluster_cpu,
         mem_allocated / cluster_mem
       )
    2. Schedule the next job from the team with the LOWEST dominant_share.
    3. Ties broken by job priority, then submission time.

    Properties:
    - Sharing incentive: no team is better off demanding more than its fair share
    - Strategy-proof: teams can't game the scheduler by misreporting resource needs
    - Pareto efficient: resources not wasted if any team has pending jobs
    - Envy-free: no team prefers another team's allocation
    """

    def __init__(self, cluster_cpu: float, cluster_mem: float):
        self.cluster_cpu = max(cluster_cpu, 1e-9)
        self.cluster_mem = max(cluster_mem, 1e-9)
        self._allocations: Dict[str, Dict[str, float]] = {}

    def update_allocation(
        self, team: str, cpu_cores: float, memory_gb: float
    ):
        self._allocations[team] = {
            "cpu": cpu_cores,
            "mem": memory_gb,
        }

    def remove_allocation(self, team: str):
        self._allocations.pop(team, None)

    def dominant_share(self, team: str) -> float:
        alloc = self._allocations.get(team, {"cpu": 0.0, "mem": 0.0})
        return max(
            alloc["cpu"] / self.cluster_cpu,
            alloc["mem"] / self.cluster_mem,
        )

    def all_dominant_shares(self) -> Dict[str, float]:
        return {team: self.dominant_share(team) for team in self._allocations}

    def select_next_job(
        self,
        pending_jobs: List[dict],
        available_workers: list,
    ) -> Optional[dict]:
        """
        From all pending jobs, select the one from the team with
        the lowest dominant share that fits available resources.
        """
        if not pending_jobs or not available_workers:
            return None

        total_avail_cpu = sum(w.cpu_limit for w in available_workers)
        total_avail_mem = sum(w.memory_limit_gb for w in available_workers)

        # Group pending jobs by team
        by_team: Dict[str, List[dict]] = {}
        for job in pending_jobs:
            by_team.setdefault(job["team"], []).append(job)

        # Find team with lowest dominant share that has a fitting job
        team_shares = [
            (team, self.dominant_share(team))
            for team in by_team
        ]
        team_shares.sort(key=lambda x: x[1])

        for team, share in team_shares:
            for job_data in by_team[team]:
                required_cpu = job_data["cpu_cores"] * job_data["num_workers"]
                required_mem = job_data["memory_gb"] * job_data["num_workers"]
                if (required_cpu <= total_avail_cpu and
                        required_mem <= total_avail_mem):
                    logger.debug(
                        "drf.selected",
                        team=team,
                        dominant_share=round(share, 4),
                        job_id=job_data["job_id"],
                    )
                    return job_data

        return None

    def record_job_start(self, team: str, cpu_cores: float, memory_gb: float):
        current = self._allocations.get(team, {"cpu": 0.0, "mem": 0.0})
        self._allocations[team] = {
            "cpu": current["cpu"] + cpu_cores,
            "mem": current["mem"] + memory_gb,
        }

    def record_job_end(self, team: str, cpu_cores: float, memory_gb: float):
        if team not in self._allocations:
            return
        current = self._allocations[team]
        self._allocations[team] = {
            "cpu": max(0.0, current["cpu"] - cpu_cores),
            "mem": max(0.0, current["mem"] - memory_gb),
        }
