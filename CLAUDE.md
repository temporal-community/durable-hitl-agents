# Meltdown — Ice Cream Delivery Fleet Demo

Conference demo: Temporal durable execution + Google ADK multi-agent reasoning,
visualized as an ice cream delivery fleet on the Las Vegas Strip.

## How to run

```bash
./run.sh          # starts Temporal dev server + FastAPI app on :8080
```

Requires a local Temporal dev server (`temporal server start-dev`).

## Architecture

- **Single process**: FastAPI server + Temporal worker run in the same process (`server.py`).
- **FleetState singleton** (`simulation.py`): in-memory state shared between worker and server.
  Couriers, orders, cooler status, agent events — all live here.
- **Workflows** (`workflows.py`): `MeltdownDemoWorkflow` orchestrates order generation and
  multi-agent assignment; `CrewRouteWorkflow` is a per-crew child workflow that continuously
  receives orders via signal, picks up at the shop, delivers, and loops.
  Signal-driven disruption + customer-change handling.
- **Activities** (`activities.py`): discrete retryable units — navigation with heartbeats,
  pickup/deliver, fleet queries, disruption resolution.
- **ADK agents** (`agents.py`): Fleet Agent + Customer Agent (parallel) → Resolver (sequential).
  Runs inline in the workflow via `TemporalModel`. Falls back to `resolve_disruption_mock` if
  `GOOGLE_API_KEY` is unset.
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
