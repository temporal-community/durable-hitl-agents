# Meltdown — Ice Cream Delivery Fleet Demo <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google ADK" height="28">

A conference demo showing **Google ADK** multi-agent reasoning with **Temporal** durable execution, visualized as an ice cream delivery fleet on the Las Vegas Strip.

<p align="center">
  <img src=".github/assets/meltdown-snapshot.png" alt="Meltdown demo dashboard" width="900">
</p>

Orders auto-generate on a timer from Las Vegas Strip venues. AI agents reason about each order — evaluating driver positions, capacity, and priority — then assign it to the best driver. When things go wrong — driver disconnects, agent failures, customer changes — Temporal ensures nothing is lost.

## What It Demonstrates

| Scenario | What Happens | What It Shows |
|----------|-------------|---------------|
| **Agent Disconnect** | Take an agent offline mid-reasoning | ADK degrades gracefully — Resolver compensates with available data. Temporal records every step that completed. Two resilience layers. |
| **Driver Disconnect** | Take a single AI-Driver offline mid-delivery | Workflow cancels the running activity via cancellation scope, waits for reconnect signal, resumes. Everything flows through Temporal — API just sends signals. |
| **Customer Change** | Submit an address change or cancellation | Human-in-the-loop: workflow pauses on `wait_condition`, resumes immediately on signal — no polling, no timeout |

## Architecture

```
┌──────────────────────────────────┐
│         Temporal Server          │
│   (workflow state + replay)      │
└──────────┬───────────────────────┘
           │
     ┌─────┴─────────────────────────────────┐
     │                                       │
┌────▼─────────────────────────────────┐ ┌───▼──────────────────────────┐
│  Worker process (3 workers)          │ │  Server process              │
│                                      │ │  FastAPI + WebSocket         │
│  meltdown-workflows worker           │ │                              │
│  ├─ MeltdownDemoWorkflow (state)     │ │  Queries Temporal for state: │
│  │    ├─ owns driver positions,      │ │  ├─ MeltdownDemoWorkflow     │
│  │    │   order assignments          │ │  │   .get_status             │
│  │    ├─ builds DriverSnapshots      │ │  └─ DriverRouteWorkflow     │
│  │    ├─ _run_adk_assignment()       │ │      .get_status             │
│  │    │   inline (live mode)         │ │                              │
│  │    ├─ OrderGenerationWorkflow     │ │  Sends signals only:         │
│  │    │   child (timer + orders)     │ │  disconnect, reconnect,      │
│  │    └─ DriverRouteWorkflow x3      │ │  customer change, start      │
│  │        child workflows            │ │                              │
│  └─ DriverRouteWorkflow              │ │  No workers, no FleetState   │
│       ├─ owns disconnect state +     │ └──────────────────────────────┘
│       │   cancellation scopes        │
│       ├─ tracks status, path_history,│
│       │   is_disconnected,           │
│       │   is_recovering,             │
│       │   current_orders             │
│       ├─ navigate_to() → DELIVERY    │
│       ├─ pickup_orders() → DELIVERY  │
│       ├─ deliver_order() → DELIVERY  │
│       └─ signals parent on complete  │
│                                      │
│  meltdown-delivery worker (max 20)   │
│  └─ navigation, pickup, deliver,     │
│     order generation, changes        │
│                                      │
│  meltdown-agents worker (max 5)      │
│  └─ ADK tool activities              │
│       (via TemporalModel):           │
│       ParallelAgent:                 │
│       ├─ Fleet Agent                 │
│       │   tool_get_fleet_status      │
│       │   tool_get_route_info (Maps) │
│       └─ Customer Agent              │
│           tool_get_order_priorities  │
│           google_search (grounding)  │
│       Resolver →                     │
│         tool_submit_assignment       │
│       TemporalModel(→AGENTS_QUEUE)   │
└──────────────────────────────────────┘
```

**How ADK and Temporal map to each other:**

| ADK concept | Temporal concept |
|-------------|-----------------|
| **LLM Agent** (`Agent` + `TemporalModel`) | Each Gemini call → `invoke_model` activity, recorded in event log |
| **Orchestrator Agent** (`SequentialAgent`, `ParallelAgent`) | Pure Python coordination — no Temporal activity, no LLM |
| **Tool call** (via `activity_tool`) | Each tool invocation → named Temporal activity, retryable + replayable |
| **Entire agent pipeline** | Runs inline in the workflow via `_run_adk_assignment()` (live); as a single activity `reason_about_assignment` (mock) |

Fleet Agent, Customer Agent, and Resolver are LLM Agents. The outer `order_assignment` pipeline is an Orchestrator Agent — it sequences them with no model of its own. Temporal never sees the orchestration logic; it only sees individual LLM calls and tool calls as discrete activities.

**Two processes**: `run.sh` starts a worker process and a server process (plus Temporal dev server). The server queries Temporal workflows for state (`_build_snapshot_from_queries()`) and sends signals only — no workers, no FleetState reads. Workers run three Temporal workers on three task queues.

**3-queue separation**: LLM calls are slow (3–5s). Without separate queues, assignment requests could starve navigation activities and cause heartbeat timeouts. The agents queue caps at 5 concurrent; delivery at 20. The workflows queue runs only workflows (no activities) — dedicated to replay. `GoogleAdkPlugin` is registered on **both** the workflow worker (sandbox passthroughs + deterministic runtime for replay) and the agents worker (`invoke_model` activity registration). `TemporalModel` uses `ActivityConfig(task_queue=AGENTS_QUEUE)` to route LLM calls to the agents worker.

### What each agent reasons about

| Agent | Reasoning | Tools |
|-------|-----------|-------|
| **Fleet Agent** (operational) | Driver positions, capacity (free slots), ETAs to destination, disconnect status — excludes unavailable drivers | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps) |
| **Customer Agent** (priority) | VIP vs standard tier, deadline pressure, hotel events (conferences, galas), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Resolver** (synthesis) | Weighs Fleet + Customer assessments, compensates if either agent is offline, picks final driver | `tool_submit_assignment`, `tool_publish_agent_event` |

Fleet and Customer run **in parallel** (`ParallelAgent`), then the Resolver runs **sequentially** after both complete (`SequentialAgent`). All tools are wrapped with `activity_tool()` — each call is a Temporal activity, recorded in the event log. If the worker restarts mid-call, results replay from the log.

> **Note:** Gemini's built-in `google_search` grounding normally can't be combined with custom function tools in the same request. ADK's `GoogleSearchTool(bypass_multi_tools_limit=True)` enables this — the Customer Agent uses Google Search alongside `tool_get_order_priorities` and `tool_publish_agent_event` in a single agent, no sub-agent needed.

### Mock mode

Mock mode is completely separate from live code. The `agent_fleet/mock/` folder contains its own `activities.py` and `worker.py`. The decision happens once at startup in `worker.py`: if `GOOGLE_API_KEY` is set, live workers run (with `GoogleAdkPlugin`, ADK inline in workflows); if not, mock workers from `agent_fleet/mock/worker.py` run instead. Live code has zero mock awareness — no `MOCK_MODE` flag, no `_get_api_activities()`, no per-key fallback selection. Mock activities use `@activity.defn(name=...)` overrides to match live activity names so workflows don't know or care which version is running. Real activities let failures propagate to Temporal's retry mechanism.

## Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (`GOOGLE_API_KEY`) — required for live mode; without it the entire demo runs in mock mode. Restricted to **Generative Language API**.
- Google Maps API key (`GOOGLE_MAPS_API_KEY`) — used for route polylines and ETAs. Restricted to **Directions API**. This must be a separate key from `GOOGLE_API_KEY` because the Generative Language API cannot share a key with standard Google Cloud APIs.

The startup decision is binary: `GOOGLE_API_KEY` set → live workers (ADK + all API activities), not set → mock workers (deterministic data, no LLM calls). Default model is `gemini-2.5-flash` (override with `DEFAULT_MODEL` env var).

## Quick Start

### 1. Install and configure

```bash
pip install -e ".[dev]"
echo 'export GOOGLE_API_KEY="your-gemini-key"' > .env
echo 'export GOOGLE_MAPS_API_KEY="your-maps-key"' >> .env  # optional, must be Maps-enabled
```

### 2. Run

```bash
./run.sh    # starts Temporal dev server + worker process + server process
```

### 3. Open the dashboard

| Interface | URL |
|-----------|-----|
| **Demo dashboard** | http://localhost:8080 |
| **Temporal UI** (workflow history, event log) | http://localhost:8233 |

## Demo Flow

1. **Start Deliveries** — Orders auto-generate every 15s. AI agents reason per-order (Fleet Agent checks positions/capacity, Customer Agent evaluates priority) and assign to the best driver. Drivers continuously pick up from Frosty's Ice Cream and deliver.
2. **Driver Disconnect** — Select an AI-Driver → disconnect signal → workflow cancels activity → reconnect signal → seamless resume. Everything flows through Temporal.
3. **Agent Disconnect** — Take an agent offline → Resolver compensates with available data → reconnect → full reasoning resumes
4. **Customer Change** — Submit a change → workflow pauses waiting for approval → approve/reject → order updated or discarded

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads (incl. `DriverSnapshot`) |
| `agent_fleet/simulation.py` | FleetState — SQLite WAL-backed write-only UI projection (`fleet_state.db`, cross-process; used by mock activities only) |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps API, agent tools |
| `agent_fleet/workflows.py` | Temporal workflows — owns driver state, cancellation scopes, signals, queries. Includes `OrderGenerationWorkflow` |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Customer, Assignment Resolver |
| `agent_fleet/config.py` | Centralized env config — `GOOGLE_API_KEY`, `GOOGLE_MAPS_API_KEY`, `DEFAULT_MODEL`, `TEMPORAL_ADDRESS` |
| `agent_fleet/queues.py` | Task queue name constants (workflows / delivery / agents) |
| `agent_fleet/worker.py` | Three Temporal workers — workflow-only, delivery, agents. Live/mock decision at startup |
| `agent_fleet/mock/` | Self-contained mock mode — `activities.py` (deterministic mocks with `name=` overrides) and `worker.py` (3 workers, no GoogleAdkPlugin) |
| `agent_fleet/server.py` | FastAPI server — queries Temporal for state, signal-only API, WebSocket, frontend |
| `agent_fleet/locations.py` | Las Vegas Strip venue pool and random order generation |
| `frontend/index.html` | Single-file SPA — Leaflet map, agent panels, overlays |

## Commands

```bash
make lint    # ruff check + format check
make fmt     # ruff format (write)
make test    # pytest
make run     # start the demo
```
