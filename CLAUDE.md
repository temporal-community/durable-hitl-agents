# Meltdown — Ice Cream Delivery Fleet Demo

Conference demo: Temporal durable execution + Google ADK multi-agent reasoning,
visualized as an ice cream delivery fleet on the Las Vegas Strip.

## How to run

```bash
./run.sh          # starts Temporal dev server + worker process + server process
```

`run.sh` starts three processes: Temporal dev server, worker (`python -m agent_fleet.worker`),
and FastAPI server (`python -m agent_fleet.server`). No manual Temporal setup needed.

## Architecture

- **Two separate processes**: FastAPI server (`server.py`) queries Temporal workflows for state
  and sends signals only — no workers, no FleetState reads. Workers run in a separate process
  (`worker.py`) with live/mock mode selection at startup (`GOOGLE_API_KEY` set → live, not set → mock).
- **Workflows own state** (`workflows.py`): `MeltdownDemoWorkflow` owns driver positions, order
  assignments, and disconnect status. Builds `DriverSnapshot`s and passes to activities as inputs.
  Capacity guardrail: if ADK assigns to a full (3 orders) or disconnected driver, auto-reassigns
  to next available. Orders assigned while Fleet Agent is offline get `degraded=True` flag.
  `DriverRouteWorkflow` is a per-driver child workflow — batch-picks up to 3 orders at Ziggy's,
  delivers sequentially (hotel A → hotel B → ...), then returns. Tracks status, is_disconnected,
  is_recovering, path_history, and current_orders. Disconnect uses Temporal-native retry: activities
  check FleetState for disconnect, fail if disconnected, Temporal retries with backoff until
  reconnected. Driver completes delivery, stays at hotel, can't report back until reconnected.
  On reconnect, `sync_driver_position` activity reads actual position from FleetState — no
  teleporting. Completed deliveries are not repeated; batch continues from next pending order.
  Supports mid-delivery reroute: `update_order` signal sets `_reroute_pending` flag — driver
  finishes current navigation leg then re-navigates to new destination. For queued (not yet active)
  orders, coordinates update in-place silently. Cancel via `cancel_order`.
  `OrderGenerationWorkflow` is a child workflow that generates orders on a randomized timer and
  signals the parent. Parent handles assignment.
- **Server reads FleetState** (`server.py`): WebSocket data comes from `fleet.snapshot()` (SQLite).
  Server also writes disconnect/reconnect state directly. Temporal queries used for structural
  state during development — FleetState is the display authority.
- **Activities are pure** (`activities.py`): receive all decision data as inputs, never read
  FleetState for logic. `@activity.defn` with no `name=` override (function names are activity names).
- **FleetState** (`simulation.py`): SQLite WAL-backed UI projection. Backed by `fleet_state.db`
  for cross-process sharing — activities in the worker write positions/statuses, server reads
  for the frontend WebSocket. In production this would be Redis or Postgres.
- **3-queue workers** (`worker.py`): workflows + local activities, delivery, agents.
  `GoogleAdkPlugin` is on both workflow and agents workers (sandbox + determinism on
  workflow side, `invoke_model` activity on agents side). `DemoTemporalModel` (subclass of
  `TemporalModel` in `_demo_model.py`) generates context-aware summaries for the Temporal UI
  and strips null fields from LLM payloads. **Note:** `DemoTemporalModel` is a temporary
  subclass — we've requested these features (dynamic `summary_fn`, null stripping, agent
  name in default summary) be added to the upstream `TemporalModel`. If accepted, revert
  `agents.py` to use `TemporalModel` directly and delete `_demo_model.py`. Same applies to
  `_activity_tool.py` dynamic summaries. `publish_agent_event` and `publish_agent_events_batch` are registered on the
  workflow worker for local activity execution (UI projection with minimal history).
- **ADK agents** (`agents.py`): Fleet Agent + Customer Agent (parallel) → Dispatch Agent (sequential).
  Live path runs ADK inline in the workflow via `_run_adk_assignment()`. No fallback to mock —
  if an activity fails, Temporal retries. Fleet Agent tools fail fast when disconnected (2 attempts),
  error returned to LLM via `_activity_tool.py` catch — Dispatch Agent assigns with available data
  but orders are flagged as `degraded`. Workflow publishes short summary events to FleetState via
  batched local activity after ADK completes (summary from `output_key` fields).
- **Mock mode** (`agent_fleet/mock/`): completely separate folder with its own `activities.py` and
  `worker.py`. Live code has zero mock awareness. Decision at startup: `GOOGLE_API_KEY` set → live
  workers, not set → mock workers. Mock activities use `name=` overrides to match live activity names.
- **Server** (`server.py`): disconnect/reconnect endpoints write to FleetState (SQLite) for
  immediate frontend display AND signal Temporal workflows for durable state.
- **Frontend** (`frontend/index.html`): single-file SPA with Leaflet map, WebSocket state feed,
  agent reasoning panels.
- **PydanticPayloadConverter** on `Client.connect` in both server and worker for `LlmResponse`
  serialization.

## Key conventions

- Dataclass models for all Temporal payloads (`models.py`)
- Activities and workflows in separate files
- Mock mode in `agent_fleet/mock/` when `GOOGLE_API_KEY` is not set
- Two API keys required: `GOOGLE_API_KEY` (Gemini, Generative Language API) and
  `GOOGLE_MAPS_API_KEY` (Directions API) — cannot be combined
- `DEFAULT_MODEL` defaults to `gemini-2.5-flash` (swappable via env)
- Random order generation from 3 Las Vegas venues (`locations.py`)
- Drivers use letter IDs: `driver-a` through `driver-e`, displayed as `Driver-A` etc.
- Ice cream shop is "Ziggy's Ice Cream" (`WAREHOUSE_LABEL` in `locations.py`)
- Max 30 orders per demo run, drivers batch up to 3 orders (`DRIVER_CAPACITY`)

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
