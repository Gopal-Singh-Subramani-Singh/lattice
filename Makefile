.PHONY: install proto infra start test sim report clean

# ── Setup ──────────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

proto:
	./scripts/generate_proto.sh

# ── Infrastructure ─────────────────────────────────────────────────────────────
infra-up:
	docker compose up redis prometheus grafana -d

infra-down:
	docker compose down

# ── Run ────────────────────────────────────────────────────────────────────────
start:
	PYTHONPATH=. python -m lattice.main

start-dev:
	LATTICE_LOG_FORMAT=development PYTHONPATH=. python -m lattice.main

start-api:
	PYTHONPATH=. uvicorn lattice.api.rest_api:app --port 8002 --reload

# ── Test ───────────────────────────────────────────────────────────────────────
test:
	PYTHONPATH=. pytest tests/ -v --tb=short

test-cov:
	PYTHONPATH=. pytest tests/ -v --tb=short --cov=lattice --cov-report=term-missing

# ── Simulation ─────────────────────────────────────────────────────────────────
sim:
	PYTHONPATH=. python simulation/stress_test.py

report:
	PYTHONPATH=. python simulation/utilisation_report.py

# ── Clean ──────────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -f lattice.db utilisation_report.png
