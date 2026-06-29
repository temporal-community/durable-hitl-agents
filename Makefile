.PHONY: install lint fmt test run worker kill-worker

install:
	uv sync --all-extras

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .

test:
	uv run pytest

run:
	./run.sh

# --- Durability demo: kill the worker mid-run, then bring it back ---
# Temporal + the web server keep running, so the paused workflow survives in Temporal's
# event history. `make worker` replays it straight back to the pause. Run these from a
# SECOND terminal (leave `make run` / ./run.sh running in the first).
kill-worker:
	@pkill -f "agent_fleet.worker" && echo "worker stopped — workflow is parked in Temporal" || echo "no worker running"

worker:
	uv run --env-file .env python -m agent_fleet.worker
