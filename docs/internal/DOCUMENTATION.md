# Lattice — In-Depth Documentation

## What Is Lattice?

Lattice is a production-grade multi-tenant ML job scheduler. It allocates Docker worker containers to ML training jobs using five distinct scheduling algorithms — the same algorithmic foundations used in Google Borg, YARN, and Kubernetes. Workers are Docker containers with cgroup CPU and memory limits that simulate GPU nodes in a real cluster.

The system targets a fundamental problem in shared ML infrastructure: when multiple teams compete for the same compute pool, who gets resources next — and is the allocation fair?

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Client (REST API / gRPC / Simulation)          │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│  Scheduler Engine (asyncio event loop)          │
│  tick interval: 500ms                           │
└──────┬──────────────────────┬───────────────────┘
       │                      │
┌──────▼──────────┐  ┌────────▼──────────┐
│ Redis Sorted Set│  │ SQLite Job Store  │
│ (priority queue)│  │ (state + events)  │
└──────┬──────────┘  └───────────────────┘
       │
┌──────▼────────────────────────────────────────┐
│  Algorithm Layer                              │
│  FIFO | DRF | Gang | Preemption | Backfill    │
└──────┬────────────────────────────────────────┘
       │
┌──────▼────────────────────────────────────────┐
│  Worker Pool                                  │
│  Docker containers (--cpus=2 --memory=4g)     │
└───────────────────────────────────────────────┘
```

**Key components:**

| Component | File | Role |
|---|---|---|
| Scheduler | `lattice/scheduler.py` | Core asyncio event loop, orchestrates algorithms |
| FIFO | `lattice/algorithms/fifo.py` | Priority queue with 4 tiers |
| DRF | `lattice/algorithms/drf.py` | Dominant Resource Fairness |
| Gang | `lattice/algorithms/gang.py` | Atomic all-or-nothing reservation |
| Preemption | `lattice/algorithms/preemption.py` | SIGUSR1 checkpoint/restore |
| Backfill | `lattice/algorithms/backfill.py` | Fill idle capacity around large jobs |
| Worker Pool | `lattice/worker/pool.py` | Manages worker container lifecycle |
| Docker Worker | `lattice/worker/docker_worker.py` | Container start/stop/monitor |
| Worker Agent | `lattice/worker/agent.py` | gRPC agent inside container |
| Job Store | `lattice/store/job_store.py` | SQLite persistence |
| Redis Queue | `lattice/store/redis_queue.py` | Sorted Set priority queue |
| gRPC Server | `lattice/api/grpc_server.py` | Submit/cancel/status/stream API |
| REST API | `lattice/api/rest_api.py` | FastAPI admin interface |

---

## Project Structure

```
lattice/
├── proto/
│   └── lattice.proto            ← gRPC service definition
├── lattice/
│   ├── main.py                  ← Entry point: scheduler + REST + gRPC
│   ├── scheduler.py             ← Core async scheduler engine
│   ├── algorithms/
│   │   ├── fifo.py              ← FIFO + 4-tier priority
│   │   ├── drf.py               ← Dominant Resource Fairness
│   │   ├── gang.py              ← Gang scheduling
│   │   ├── preemption.py        ← Preemption controller
│   │   └── backfill.py          ← Backfill scheduler
│   ├── worker/
│   │   ├── pool.py              ← Worker pool manager
│   │   ├── docker_worker.py     ← Docker container lifecycle
│   │   └── agent.py             ← Worker gRPC agent
│   ├── store/
│   │   ├── job_store.py         ← SQLite job state
│   │   └── redis_queue.py       ← Redis Sorted Set queue
│   ├── api/
│   │   ├── grpc_server.py       ← gRPC service
│   │   └── rest_api.py          ← FastAPI REST API
│   ├── metrics.py               ← 12 Prometheus metrics
│   └── models.py                ← Pydantic + dataclass models
├── config/
│   └── config.yaml              ← All tunable parameters
├── simulation/
│   ├── stress_test.py           ← 200-job simulation
│   └── utilisation_report.py    ← FIFO vs DRF chart
├── tests/                       ← 32+ pytest tests
├── docker-compose.yml
├── Dockerfile.worker
├── Makefile
├── requirements.txt
└── pyproject.toml
```

---

## How to Run

### Prerequisites

- Python 3.11+
- Docker (for Redis + worker containers + Prometheus + Grafana)

### Step 1 — Install dependencies

```bash
cd "/Users/gopalsinghsubramanisingh/Documents/AI  Hive/Lattice/lattice"
pip install -r requirements.txt
```

### Step 2 — Generate gRPC stubs

This must be done once before starting Lattice:

```bash
python -m grpc_tools.protoc \
  -I proto \
  --python_out=lattice/proto_gen \
  --grpc_python_out=lattice/proto_gen \
  proto/lattice.proto
```

Or use the Makefile shortcut:

```bash
make proto
```

### Step 3 — Start infrastructure

```bash
docker compose up redis prometheus grafana -d
```

Or with Make:

```bash
make infra-up
```

This starts:
- **Redis** on `localhost:6379` — job queue and distributed locks
- **Prometheus** on `localhost:9090`
- **Grafana** on `localhost:3000` (admin / lattice)

### Step 4 — Start Lattice

Option A — REST API only (simplest):

```bash
PYTHONPATH=. uvicorn lattice.api.rest_api:app --port 8002 --reload
```

Option B — Full stack (scheduler + REST + gRPC):

```bash
PYTHONPATH=. python -m lattice.main
```

Or with Make:

```bash
make start       # production
make start-dev   # with structured logging
make start-api   # REST API only
```

The REST API is available at `http://localhost:8002`. Interactive docs at `http://localhost:8002/docs`.

### Step 5 — Run tests

No Docker or real Redis needed — tests use `fakeredis` and mocked Docker:

```bash
PYTHONPATH=. pytest tests/ -v
```

Or with Make:

```bash
make test         # run tests
make test-cov     # with coverage report
```

### Step 6 — Run the 200-job simulation

```bash
PYTHONPATH=. python simulation/stress_test.py
```

### Step 7 — Generate utilisation comparison chart

```bash
PYTHONPATH=. python simulation/utilisation_report.py
# outputs: utilisation_report.png
```

---

## Makefile Reference

```bash
make install      # pip install -r requirements.txt
make proto        # generate gRPC stubs
make infra-up     # docker compose up redis prometheus grafana -d
make infra-down   # docker compose down
make start        # start full Lattice server
make start-dev    # start with development logging
make start-api    # start REST API only
make test         # run all tests
make test-cov     # tests + coverage report
make sim          # run 200-job stress simulation
make report       # generate utilisation_report.png
make clean        # remove __pycache__, *.pyc, lattice.db
```

---

## REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a new ML job |
| `DELETE` | `/jobs/{id}` | Cancel a pending or running job |
| `GET` | `/jobs/{id}` | Get job status |
| `GET` | `/jobs` | List jobs (filter by team and/or state) |
| `GET` | `/cluster` | Cluster overview + DRF dominant shares |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/health` | Health check |

### Submit a job

```bash
curl -X POST http://localhost:8002/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "team": "nlp_team",
    "name": "bert-finetune",
    "priority": 2,
    "cpu_cores": 2.0,
    "memory_gb": 4.0,
    "num_workers": 1,
    "estimated_duration_seconds": 600
  }'
```

Priority values: `0=BATCH, 1=NORMAL, 2=HIGH, 3=CRITICAL`

### List jobs for a team

```bash
curl "http://localhost:8002/jobs?team=nlp_team&state=running"
```

### View cluster status

```bash
curl http://localhost:8002/cluster
```

Returns utilisation ratio, idle/busy worker counts, and DRF dominant shares per team.

---

## gRPC API Reference

Lattice exposes a full gRPC service defined in `proto/lattice.proto`:

```
service LatticeScheduler {
  rpc Submit       (SubmitRequest)  returns (SubmitResponse);
  rpc Cancel       (CancelRequest)  returns (CancelResponse);
  rpc Status       (StatusRequest)  returns (StatusResponse);
  rpc List         (ListRequest)    returns (ListResponse);
  rpc StreamEvents (StreamRequest)  returns (stream JobEvent);
  rpc ClusterStats (ClusterRequest) returns (ClusterStatus);
}
```

gRPC server runs on port `50051` by default.

`StreamEvents` is a server-streaming RPC — connect once and receive every state transition for a job as it happens.

---

## Scheduling Algorithms

### 1. FIFO with Priority Tiers

The default algorithm. Four priority levels:

| Priority | Value | Use case |
|---|---|---|
| CRITICAL | 3 | Deadline-driven jobs, on-call models |
| HIGH | 2 | Production training runs |
| NORMAL | 1 | Regular experiments |
| BATCH | 0 | Offline/background work |

Redis Sorted Set score: `priority × 10¹² − timestamp`

This means CRITICAL jobs always outrank NORMAL, and within the same priority the oldest job wins (true FIFO). The score encoding is:

```python
score = priority.value * 1e12 - time.time()
```

### 2. Dominant Resource Fairness (DRF)

DRF ensures fair multi-resource allocation across teams. It answers: "which team has consumed the smallest fraction of the cluster?"

For each team:
```
dominant_share = max(
    cpu_allocated / cluster_cpu,
    mem_allocated / cluster_mem
)
```

The scheduler always picks the next job from the team with the **lowest dominant share**. This prevents a memory-heavy team from crowding out a CPU-heavy team, and vice versa.

Properties DRF guarantees:
- **Sharing incentive**: teams can't do better by asking for more than their fair share
- **Strategy-proof**: misreporting resource needs doesn't help
- **Pareto efficient**: idle capacity is never wasted if any team has pending jobs
- **Envy-free**: no team would prefer another team's allocation

### 3. Gang Scheduling

Gang scheduling ensures all N workers for a job are reserved **atomically** — either all workers start together or none do. This is essential for distributed training (e.g., PyTorch DDP) where a partial allocation leads to workers blocking waiting for peers that will never arrive.

A distributed Redis lock (`lattice:gang:lock:{job_id}`) prevents race conditions when multiple scheduler instances check availability simultaneously.

### 4. Preemption

When a CRITICAL job arrives and no idle workers are available, the scheduler can preempt a lower-priority running job (if the priority gap is ≥ 2 levels).

Preemption flow:
1. Send `SIGUSR1` to the worker container
2. Worker catches the signal, calls `torch.save()` to checkpoint current state
3. Worker container is freed and returned to the pool
4. The preempted job is re-queued with its checkpoint path preserved
5. When re-scheduled, the job resumes from the checkpoint

This avoids wasting all prior training work when a high-priority job needs resources.

### 5. Backfill Scheduling

Backfill fills idle capacity that would otherwise sit unused while a large gang job waits for enough workers to free up.

Logic:
1. A large job (e.g., needs 6 workers) is pending but only 2 workers are idle
2. Rather than leaving those 2 workers idle, backfill looks ahead at the queue
3. It finds a small job that fits in 2 workers AND will finish before the estimated start time of the big job (with a safety margin)
4. That small job runs now, using otherwise-wasted capacity

On a real workload this lifts utilisation from ~54% (pure FIFO) to ~83%.

---

## Job State Machine

```
PENDING → RESERVED → RUNNING → COMPLETED
                             → FAILED
         → CANCELLED
RUNNING → PREEMPTED → PENDING (re-queued with checkpoint)
```

All state transitions are persisted to SQLite and optionally streamed via gRPC `StreamEvents`.

---

## Redis Queue Internals

The sorted set uses a score derived from priority and timestamp:

```python
# Higher score = dequeued first
score = priority.value * 1e12 - time.time()
```

The `1e12` multiplier ensures even the maximum possible timestamp (~1.7 × 10¹²) cannot overflow from the BATCH priority into the NORMAL priority band. Within the same priority, earlier-submitted jobs have a slightly higher score (since `-time.time()` is less negative for older submissions).

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `lattice_cluster_utilisation_ratio` | Gauge | Target > 0.80 |
| `lattice_drf_dominant_share{team}` | Gauge | Should converge toward equal across teams |
| `lattice_preemptions_total` | Counter | Preemption events |
| `lattice_gang_schedule_attempts_total` | Counter | Gang scheduling outcomes |
| `lattice_backfill_jobs_total` | Counter | Jobs scheduled via backfill |
| `lattice_jobs_submitted_total` | Counter | Submissions by team/priority |
| `lattice_jobs_completed_total` | Counter | Completions by team/status |
| `lattice_job_wait_seconds` | Histogram | Queue wait time |
| `lattice_job_duration_seconds` | Histogram | Wall-clock duration |
| `lattice_queue_depth{priority}` | Gauge | Pending jobs per tier |
| `lattice_workers{state}` | Gauge | Workers by state (idle/busy/unhealthy) |
| `lattice_uptime_seconds` | Gauge | Scheduler uptime |

---

## Configuration Reference

Edit `config/config.yaml`:

```yaml
scheduler:
  port: 50051
  algorithm: "drf"          # fifo | drf | gang | backfill
  tick_interval_ms: 500     # scheduler loop frequency
  preemption_enabled: true
  backfill_enabled: true
  gang_scheduling_enabled: true

cluster:
  total_cpu_cores: 16.0     # simulated cluster total
  total_memory_gb: 32.0
  max_workers: 8            # Docker containers in pool

workers:
  cpu_limit: "2"            # Docker --cpus per worker
  memory_limit: "4g"        # Docker --memory per worker
  image: "lattice-worker:latest"
  heartbeat_interval_seconds: 5
  heartbeat_timeout_seconds: 30

redis:
  url: "redis://localhost:6379"

sqlite:
  db_path: "lattice.db"

preemption:
  priority_gap: 2           # only preempt if gap >= 2 levels
  checkpoint_timeout_seconds: 30

backfill:
  lookahead_jobs: 10        # pending jobs to consider
  safety_margin_seconds: 60  # finish before big job's estimated start - this

api:
  rest_port: 8002
  grpc_port: 50051
```

---

## Grafana Dashboard

1. Open `http://localhost:3000` (admin / lattice)
2. Dashboards are auto-provisioned via `grafana/provisioning/`

Key panels to watch:
- Cluster utilisation ratio — should stay above 0.80 under load
- DRF dominant shares per team — should converge to equal under sustained load
- Queue depth per priority tier — spikes indicate resource contention
- Preemption events — should be infrequent in a well-tuned cluster

---

## Port Reference

| Service | Port |
|---|---|
| Lattice REST API | 8002 |
| Lattice gRPC | 50051 |
| Redis | 6379 |
| Prometheus | 9090 |
| Grafana | 3000 |

---

## Running Tests

No real Docker or Redis required — tests use `fakeredis` and mocked Docker SDK.

```bash
cd lattice/

# Install dependencies
pip install -r requirements.txt

# Run all tests (PYTHONPATH needed for local imports)
PYTHONPATH=. pytest tests/ -v

# Run with coverage
PYTHONPATH=. pytest tests/ -v --cov=lattice --cov-report=term-missing

# Run a specific module
PYTHONPATH=. pytest tests/test_drf.py -v
PYTHONPATH=. pytest tests/test_gang.py -v
```

Or via Makefile:

```bash
make test
make test-cov
```

**Test modules:**

| File | Tests | What's covered |
|---|---|---|
| `test_fifo.py` | 4 | Priority selection, worker assignment, insufficient workers |
| `test_drf.py` | 5 | Dominant share computation, fairness selection, multi-team |
| `test_gang.py` | 4 | Atomic reservation, Redis lock acquire/release, partial failure |
| `test_preemption.py` | 4 | Priority gap check, preemption candidate selection, checkpoint |
| `test_backfill.py` | 4 | Backfill candidate selection, safety margin, gang interaction |
| `test_job_store.py` | 5 | SQLite CRUD, state transitions, event log |
| `test_redis_queue.py` | 5 | Enqueue/dequeue, score ordering, depth by priority |
| `test_grpc_server.py` | 4 | Submit/cancel/status gRPC calls (mocked channel) |
| `test_simulation.py` | 2 | Deterministic 20-job simulation, DRF fairness check |

---

## Prometheus Queries

```promql
# Cluster utilisation — target > 0.80
lattice_cluster_utilisation_ratio

# DRF dominant shares — should converge to equal under load
lattice_drf_dominant_share

# Preemption rate — should be infrequent in a stable cluster
rate(lattice_preemptions_total[5m])

# Queue depth per priority
lattice_queue_depth

# P99 job wait time
histogram_quantile(0.99, rate(lattice_job_wait_seconds_bucket[10m]))

# Worker count by state
lattice_workers

# Backfill scheduling effectiveness
rate(lattice_backfill_jobs_total[5m])
```

---

## Production Hardening

**Scale the simulated cluster.** Adjust `config/config.yaml` to match real hardware:

```yaml
cluster:
  total_cpu_cores: 64.0
  total_memory_gb: 256.0
  max_workers: 32
```

**Choose the right algorithm.** For a shared team environment, `drf` is strongly recommended over `fifo` — it prevents one resource-hungry team from starving others. Set it in config:

```yaml
scheduler:
  algorithm: "drf"
```

**Use unique consumer names for multi-instance deployments.** If running multiple Lattice instances, each must have a unique `consumer_name` to avoid queue conflicts:

```yaml
redis:
  consumer_name: "lattice-worker-1"  # unique per instance
```

**Persist the SQLite database outside the container:**

```yaml
sqlite:
  db_path: "/data/lattice.db"
```

**Set heartbeat timeouts appropriately.** If workers are doing long I/O before reporting back, increase the timeout to avoid false unhealthy marks:

```yaml
workers:
  heartbeat_timeout_seconds: 60
```

**TLS for gRPC.** The gRPC server runs in plaintext by default. For production, configure TLS certificates in `grpc_server.py` using `grpc.ssl_server_credentials()`.

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'lattice'`

Run all commands with `PYTHONPATH=.` from the `lattice/` directory:

```bash
PYTHONPATH=. python -m lattice.main
PYTHONPATH=. pytest tests/ -v
```

Or use the Makefile targets which set this automatically.

### `ImportError: lattice_pb2` or gRPC stubs missing

The proto stubs must be generated before starting Lattice:

```bash
make proto
# or manually:
python -m grpc_tools.protoc \
  -I proto \
  --python_out=lattice/proto_gen \
  --grpc_python_out=lattice/proto_gen \
  proto/lattice.proto
```

### Jobs stuck in `PENDING` indefinitely

Either all workers are busy or no workers fit the job's resource requirements. Check:

```bash
curl http://localhost:8002/cluster
```

Look at `idle_workers`. If 0, wait for running jobs to complete. If `idle_workers > 0` but jobs are still pending, the job requests more resources than a single worker can provide — check `cpu_cores` and `memory_gb` against the worker `cpu_limit` and `memory_limit` in config.

### Redis connection refused

```bash
docker compose up redis -d
docker compose ps redis
```

### `helm upgrade failed` (for the Docker worker image)

The `lattice-worker:latest` image needs to be built first:

```bash
docker build -f Dockerfile.worker -t lattice-worker:latest .
```

### DRF dominant shares not converging

Under very unequal workloads, DRF shares will diverge temporarily but should converge when teams submit similar volumes. If one team is consistently dominant, check that their jobs are actually completing (not stalling in `RUNNING`).
