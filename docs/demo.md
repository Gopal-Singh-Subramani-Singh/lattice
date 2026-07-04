# Lattice — Demo Guide

## What this demo proves

- Job submission API works and returns job IDs
- Cluster resource state is tracked correctly
- FIFO scheduling dispatches jobs in priority order
- DRF scheduling distributes resources fairly across teams
- Gang scheduling queues multi-worker jobs for atomic allocation
- 200-job stress simulation runs end-to-end
- FIFO vs DRF utilisation comparison chart is generated
- Prometheus metrics populate on scheduling events

---

## Prerequisites

```bash
pip install -r requirements.txt
docker compose up redis prometheus grafana -d

# Generate gRPC stubs (if using gRPC)
python -m grpc_tools.protoc \
  -I proto \
  --python_out=lattice/proto_gen \
  --grpc_python_out=lattice/proto_gen \
  proto/lattice.proto
```

---

## Demo Commands

### 1. Start Lattice

```bash
uvicorn lattice.api.rest_api:app --port 8002 --reload
```

### 2. Verify health

```bash
curl http://localhost:8002/health
```

### 3. Submit jobs from two teams

```bash
# Team A — large training job
curl -X POST http://localhost:8002/jobs \
  -H "Content-Type: application/json" \
  -d '{"team": "team_A", "name": "gpt-finetune", "priority": 3,
       "cpu_cores": 4.0, "memory_gb": 8.0, "num_workers": 2,
       "estimated_duration_seconds": 600}'

# Team B — small evaluation job
curl -X POST http://localhost:8002/jobs \
  -H "Content-Type: application/json" \
  -d '{"team": "team_B", "name": "bert-eval", "priority": 1,
       "cpu_cores": 1.0, "memory_gb": 2.0, "num_workers": 1,
       "estimated_duration_seconds": 120}'
```

### 4. Check cluster state (DRF shares)

```bash
curl http://localhost:8002/cluster | python -m json.tool
```

Expected:
```json
{
  "total_cpu": 8.0,
  "total_memory_gb": 16.0,
  "allocated_cpu": 5.0,
  "allocated_memory_gb": 10.0,
  "teams": {
    "team_A": {"dominant_share": 0.5, "cpu": 4.0, "memory_gb": 8.0},
    "team_B": {"dominant_share": 0.125, "cpu": 1.0, "memory_gb": 2.0}
  }
}
```

### 5. Get job status

```bash
curl http://localhost:8002/jobs/<job_id> | python -m json.tool
```

### 6. List jobs by team

```bash
curl "http://localhost:8002/jobs?team=team_A"
```

### 7. Run 200-job stress simulation

```bash
python simulation/stress_test.py
```

Expected output:
```
Submitting 200 jobs across 5 teams and 4 priority tiers...
[scheduler] FIFO queue depth: 200 → dispatching...
[scheduler] 50 jobs completed in 45.2s
[scheduler] 150 jobs completed in 92.8s
[scheduler] 200 jobs completed in 138.4s
Cluster utilisation: 81.3% (FIFO) vs 83.7% (DRF)
```

### 8. Generate FIFO vs DRF utilisation chart

```bash
python simulation/utilisation_report.py
open utilisation_report.png
```

Expected: side-by-side bar chart comparing FIFO and DRF cluster utilisation across teams.

Screenshot pending.

### 9. View Prometheus metrics

```bash
curl http://localhost:8002/metrics | grep lattice_
```

### 10. View Grafana dashboard

```
http://localhost:3000   (admin / lattice)
```

Dashboards are auto-provisioned via `grafana/provisioning/`.

---

## Expected Output Summary

| Check | Expected |
|-------|----------|
| Job submission | 202 Accepted, job_id returned |
| `/cluster` | Resource allocation and DRF shares shown |
| Job status | QUEUED → RUNNING → COMPLETED lifecycle |
| 200-job simulation | All jobs complete, utilisation metrics logged |
| Utilisation chart | PNG showing FIFO vs DRF comparison |
| `/metrics` | lattice_cluster_utilisation_ratio, lattice_drf_dominant_share populated |

---

## Known Limitations

- Worker execution is simulated (timed sleeps). Jobs do not run actual ML compute.
- Preemption is implemented via SIGUSR1 in the code; simulated in demo mode.
- Resource limits are tracked in state but not enforced at OS level (no cgroups in simulation).
- Docker required for worker container lifecycle (unit tests mock this).
- gRPC stubs must be generated before using gRPC worker communication.
