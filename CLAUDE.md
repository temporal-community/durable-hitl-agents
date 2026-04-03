# Meltdown — Ice Cream Delivery Fleet Demo

Conference demo: Temporal durable execution + Google ADK multi-agent reasoning,
visualized as an ice cream delivery fleet on the Las Vegas Strip.

## How to run

```bash
./run.sh          # starts Temporal dev server + FastAPI app on :8080
```

Requires a local Temporal dev server (`temporal server start-dev`).

## Architecture

- **Single process**: FastAPI server + 3 Temporal workers run in the same process (`server.py`).
- **Workflows own state** (`workflows.py`): `MeltdownDemoWorkflow` owns crew positions, order
  assignments, and disconnect status. Builds `CrewSnapshot`s and passes to activities as inputs.
  `CrewRouteWorkflow` is a per-crew child workflow with cancellation scopes for disconnect
  handling. Signals parent on delivery complete.
- **Activities are pure** (`activities.py`): receive all decision data as inputs, never read
  FleetState for logic. Write to FleetState as UI projection only.
- **FleetState** (`simulation.py`): write-only UI projection for the frontend WebSocket.
  Activities write here; nothing reads it for decision-making.
- **3-queue workers** (`worker.py`): workflows-only (no activities), delivery, agents.
- **ADK agents** (`agents.py`): Fleet Agent + Customer Agent (parallel) → Resolver (sequential).
  Runs inline in the workflow via `TemporalModel`. Falls back to mock if `GOOGLE_API_KEY` is unset.
- **Server is signal-only** (`server.py`): disconnect/reconnect endpoints send Temporal signals
  only — no direct FleetState writes. Everything flows through workflows.
- **Frontend** (`frontend/index.html`): single-file SPA with Leaflet map, WebSocket state feed,
  agent reasoning panels.

## Key conventions

- Dataclass models for all Temporal payloads (`models.py`)
- Activities and workflows in separate files
- Mock mode fallback when `GOOGLE_API_KEY` is not set
- Random order generation from a pool of 10 Las Vegas venues (`locations.py`)

## Commands

```bash
ruff check .      # lint
ruff format .     # format
pytest            # run tests
make lint         # ruff check + format check
make fmt          # ruff format (write)
make test         # pytest
make run          # start the demo
```
