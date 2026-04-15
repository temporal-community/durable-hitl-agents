# Meltdown — Ice Cream Delivery Fleet Demo <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google ADK" height="28">

A conference demo showing **Google ADK** multi-agent reasoning with **Temporal** durable execution, visualized as an ice cream delivery fleet on the Las Vegas Strip.

<p align="center">
  <img src=".github/assets/meltdown-snapshot.png" alt="Meltdown demo dashboard" width="900">
</p>

Orders auto-generate on a timer from Las Vegas Strip venues. **AI agents** (Fleet, Customer, Dispatch Agent) reason about each order — evaluating positions, capacity, and priority — then assign it to the best **delivery actor**. When things go wrong — delivery actor disconnects, agent failures, customer changes — Temporal ensures nothing is lost.

> **Terminology:** AI agents **reason** (LLM + tools, run inline via ADK). Delivery actors **execute** (child workflows that carry out routes). They are not Temporal workers.

## What It Demonstrates

| Scenario | What Happens | What It Shows |
|----------|-------------|---------------|
| **Tool Degradation** | Take Fleet Agent offline | Fleet Agent's tools fail fast (2 retries), error returned to LLM — Dispatch Agent assigns with Customer Agent data only. Reconnect → tools succeed → full assessment resumes. Temporal shows retry attempts in the UI. |
| **Service Disruption & Recovery** | Take a delivery actor offline mid-delivery | Delivery actor completes current delivery but can't report back. Temporal retries with backoff until reconnected. Stays at hotel on the map — no teleporting. Reconnect → next retry succeeds → navigates home for next order. |
| **Human-in-the-Loop (HITL)** | Submit an address change or cancellation mid-delivery | Workflow pauses on `wait_condition`, approves, signals delivery actor — reroutes mid-delivery to new destination. Cross-workflow coordination via signals. |

## Quick Start

You'll need two keys to get the demo to run: `GOOGLE_API_KEY` and `GOOGLE_MAPS_API_KEY`.

If you don't have them, skip down to [Obtain API Keys](#obtain-api-keys) and come back.

### 0. Install
Run the following to get things installed:

```
# Grab the code.
git clone https://github.com/temporal-community/ice-cream-fleet-demo
cd ice-cream-fleet-demo

# Rename .env file.
mv .env.example .env
```

### 1. Set API keys
Replace the `GOOGLE_*_KEY` placeholder text in `.env` with your actual keys.

```
echo 'export GOOGLE_API_KEY="your-gemini-key"' > .env
echo 'export GOOGLE_MAPS_API_KEY="your-maps-key"' >> .env  # optional, must be Maps-enabled
```

### 2. Run
The `run.sh` script sets up your Python environment, installs dependencies, and gets Temporal running.

```bash
./run.sh    # starts Temporal dev server + worker process + server process
```

### 3. Open the dashboard

| Interface | URL |
|-----------|-----|
| **Demo dashboard** | http://localhost:8080 |
| **Temporal UI** (workflow history, event log) | http://localhost:8233 |

## Demo Flow

1. **Start Deliveries** — Orders auto-generate every 10s. AI agents reason per-order and assign to the best delivery actor. Drivers batch-pickup up to 3 orders at Ziggy's Ice Cream and deliver them sequentially.
2. **Demo 1: Tool Degradation** — Take Fleet Agent offline → tools fail fast (2 retries) → error returned to LLM → Dispatch Agent assigns with Customer Agent data → reconnect → full reasoning resumes
3. **Demo 2: Fleet Disconnect & Recovery** — Select a driver with multiple orders → disconnect → finishes current delivery, stays at hotel → Temporal retries with backoff → reconnect → resumes from next order, no repeated work
4. **Demo 3: Human-in-the-Loop (HITL)** — Pick an active order → submit address change → workflow pauses for approval → approve → delivery actor reroutes mid-delivery to new destination


## Architecture

```
                        ┌─────────────────────────┐
                        │     Temporal Server      │
                        │   event log + replay     │
                        └────────────┬────────────┘
                                     │
                ┌────────────────────┼────────────────────┐
                │                    │                    │
                ▼                    ▼                    ▼
   ┌─────────────────────┐  ┌───────────────┐  ┌──────────────────┐
   │   Workflow Worker    │  │Delivery Worker│  │  Agents Worker   │
   │  meltdown-workflows │  │meltdown-deliv.│  │ meltdown-agents  │
   └─────────┬───────────┘  └───────┬───────┘  └────────┬─────────┘
             │                      │                    │
             │                      │                    │
   ┌─────────▼───────────┐  ┌──────▼────────┐  ┌────────▼─────────┐
   │ MeltdownDemoWorkflow │  │  Activities:  │  │  ADK Agents      │
   │                      │  │  navigate_to  │  │  (via Temporal-  │
   │  OrderGeneration ◄───┤  │  pickup_orders│  │   Model):        │
   │    (child, timer)    │  │  deliver_order│  │                  │
   │                      │  │  get_route_   │  │  ┌────┐ ┌────┐  │
   │  Driver-A ◄──────────┤  │   polyline    │  │  │Fleet│ │Cust│  │
   │  Driver-B ◄──────────┤  │  generate_    │  │  │Agent│ │Agnt│  │
   │  Driver-C ◄──────────┤  │   order       │  │  └──┬─┘ └─┬──┘  │
   │  Driver-D ◄──────────┤  │  sync_driver_ │  │     └──┬──┘     │
   │  Driver-E ◄──────────┤  │   position    │  │  ┌─────▼─────┐  │
   │  (child workflows)   │  │  execute_     │  │  │ Dispatch   │  │
   │                      │  │   customer_   │  │  │  Agent     │  │
   │  ADK runs inline:    │  │   change      │  │  └───────────┘  │
   │  _run_adk_assignment │  └───────────────┘  │                  │
   │  (live mode)         │                     │  invoke_model    │
   └──────────────────────┘                     │  tool_get_fleet  │
                                                │  tool_get_route  │
   ┌──────────────────────┐                     │  tool_get_order  │
   │   Server Process     │                     │  google_search   │
   │   FastAPI + WS       │                     └──────────────────┘
   │                      │
   │  Queries workflows   │   ┌──────────────────────────────────┐
   │  for state (no       │   │         Frontend (SPA)           │
   │  workers, no         │◄──┤  Leaflet map + WebSocket feed    │
   │  FleetState reads)   │   │  Agent reasoning panels          │
   │                      │   │  Fleet/order status cards         │
   │  Sends signals:      │   └──────────────────────────────────┘
   │  start, disconnect,  │
   │  reconnect, change   │
   └──────────────────────┘
```

**Order lifecycle:** Order generates on timer → ADK agents reason (Fleet + Customer in parallel → Dispatch) → capacity check + assignment → driver batch-picks up at Ziggy's → delivers sequentially to hotels → signals parent on each completion → returns to base

**How ADK and Temporal map to each other:**

| ADK concept | Temporal concept |
|-------------|-----------------|
| **LLM Agent** (`Agent` + `TemporalModel`) | Each Gemini call → `invoke_model` activity, recorded in event log |
| **Orchestrator Agent** (`SequentialAgent`, `ParallelAgent`) | Pure Python coordination — no Temporal activity, no LLM |
| **Tool call** (via `activity_tool`) | Each tool invocation → named Temporal activity, retryable + replayable |
| **Entire agent pipeline** | Runs inline in the workflow via `_run_adk_assignment()` (live); as a single activity `reason_about_assignment` (mock) |

Fleet Agent, Customer Agent, and Dispatch Agent are LLM Agents. The outer `order_assignment` pipeline is an Orchestrator Agent — it sequences them with no model of its own. Temporal never sees the orchestration logic; it only sees individual LLM calls and tool calls as discrete activities.

### Core mechanism — how ADK becomes durable

The entire demo hinges on two pieces of code working together:

**1. ADK Runner executes inside a Temporal workflow** (`workflows.py` → `_run_adk_assignment()`):

```python
runner = Runner(agent=agent, app_name="meltdown_demo", session_service=session_service)

async for event in runner.run_async(
    user_id="workflow", session_id=session.id,
    new_message=Content(parts=[Part(text=prompt)]),
):
    events_count += 1
```

A full multi-agent ADK pipeline (Fleet + Customer in parallel → Dispatch Agent) runs **inline inside a Temporal workflow**. Not as an external call — inside the workflow's execution context.

**2. `GoogleAdkPlugin` intercepts every LLM and tool call** (`worker.py` → agents worker):

```python
Worker(
    client, task_queue=AGENTS_QUEUE,
    activities=[register_assignment, tool_get_fleet_status, ...],
    plugins=[GoogleAdkPlugin()],
)
```

The plugin turns each Gemini inference and each tool invocation into a **separate Temporal activity** — recorded in the event log, retryable, and replayable. Without it, ADK agents are ephemeral Python; with it, every reasoning step has Temporal's durability guarantees. If the worker crashes mid-reasoning, the workflow replays from the event log and resumes exactly where it left off.

**Two processes**: `run.sh` starts a worker process and a server process (plus Temporal dev server). The server queries Temporal workflows for state (`_build_snapshot_from_queries()`) and sends signals only — no workers, no FleetState reads. Workers run three Temporal workers on three task queues.

**3-queue separation**: LLM calls are slow (3–5s). Without separate queues, assignment requests could starve navigation activities and cause heartbeat timeouts. The agents queue caps at 5 concurrent; delivery at 20. The workflows queue runs workflows plus `publish_agent_event` as a local activity (UI projection with minimal history). `GoogleAdkPlugin` is registered on **both** the workflow worker (sandbox passthroughs + deterministic runtime for replay) and the agents worker (`invoke_model` activity registration). `DemoTemporalModel` (subclass of `TemporalModel` in `_demo_model.py`) generates context-aware Temporal UI summaries and strips null fields from LLM payloads.

### What each agent reasons about

| Agent | Reasoning | Tools |
|-------|-----------|-------|
| **Fleet Agent** (operational) | Delivery actor positions, capacity (free slots), ETAs to destination, disconnect status — excludes unavailable actors | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps) |
| **Customer Agent** (priority) | VIP vs standard tier, deadline pressure, hotel events (conferences, galas), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Dispatch Agent** (synthesis) | Weighs Fleet + Customer assessments, compensates if either agent is offline, picks final delivery actor | `tool_submit_assignment` |

Fleet and Customer run **in parallel** (`ParallelAgent`), then the Dispatch Agent runs **sequentially** after both complete (`SequentialAgent`). All tools are wrapped with `activity_tool()` — each call is a Temporal activity, recorded in the event log. If the worker restarts mid-call, results replay from the log.

> **Note:** Gemini's built-in `google_search` grounding normally can't be combined with custom function tools in the same request. ADK's `GoogleSearchTool(bypass_multi_tools_limit=True)` enables this — the Customer Agent uses Google Search alongside `tool_get_order_priorities` in a single agent, no sub-agent needed.

> **Agent disconnect resilience:** When Fleet Agent is disconnected, its tool activities (`tool_get_fleet_status`, `tool_get_route_info`) check FleetState and raise `RuntimeError`. Temporal retries (2 attempts, fast backoff via `_FLEET_TOOL_RETRY`). The `_activity_tool.py` wrapper catches the exhausted retry and returns an error string to the LLM — the agent reasons about the failure, and the Dispatch Agent assigns based on Customer Agent data alone. Orders assigned during Fleet Agent outage are flagged as `degraded` in the UI. No pipeline crash.
>
> **Note on Maps API errors:** `tool_get_route_info` calls the Google Maps Directions API for driving ETAs. Occasional failures (rate limiting, quota, transient errors) are normal — the same graceful degradation applies. The error is returned to the LLM as context, the Fleet Agent notes the missing ETA, and the Dispatch Agent assigns with available data. This is the system working as designed, not a bug.

### Mock mode

Mock mode is completely separate from live code. The `agent_fleet/mock/` folder contains its own `activities.py` and `worker.py`. The decision happens once at startup in `worker.py`: if `GOOGLE_API_KEY` is set, live workers run (with `GoogleAdkPlugin`, ADK inline in workflows); if not, mock workers from `agent_fleet/mock/worker.py` run instead. Live code has zero mock awareness — no `MOCK_MODE` flag, no `_get_api_activities()`, no per-key fallback selection. Mock activities use `@activity.defn(name=...)` overrides to match live activity names so workflows don't know or care which version is running. Real activities let failures propagate to Temporal's retry mechanism.

## Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (`GOOGLE_API_KEY`) — required for live mode; without it the entire demo runs in mock mode. Restricted to **Generative Language API**.
- Google Maps API key (`GOOGLE_MAPS_API_KEY`) — used for route polylines and ETAs. Restricted to **Directions API**. This must be a separate key from `GOOGLE_API_KEY` because the Generative Language API cannot share a key with standard Google Cloud APIs.

The startup decision is binary: `GOOGLE_API_KEY` set → live workers (ADK + all API activities), not set → mock workers (deterministic data, no LLM calls). Default model is `gemini-2.5-flash` (override with `DEFAULT_MODEL` env var).

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads (incl. `DriverSnapshot`) |
| `agent_fleet/simulation.py` | FleetState — SQLite WAL-backed write-only UI projection (`fleet_state.db`, cross-process; used by mock activities only) |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps API, agent tools |
| `agent_fleet/workflows.py` | Temporal workflows — owns driver state, signals, queries, Temporal-native retry for disconnect. Includes `OrderGenerationWorkflow` |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Customer, Dispatch Agent |
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
### Obtain API keys

#### Google Gemini API Key
1. Go to [Google AI Studio](https://aistudio.google.com/) > [API Keys](https://aistudio.google.com/api-keys) and sign in with your Google account.
2. Click **Create API key**. Select an existing Google Cloud project or create a new one when prompted.
3. To test that the key is working (replace `PASTE_KEY_HERE`):

```
curl "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent" \
  -H 'Content-Type: application/json' \
  -H 'X-goog-api-key: PASTE_KEY_HERE' \
  -X POST \
  -d '{
    "contents": [
      {
        "parts": [
          {
            "text": "Explain how AI works in a few words"
          }
        ]
      }
    ]
  }'
```
If you get a bunch of JSON back, you're in business!

#### Google Maps API Key

1. Make sure the **Directions API** is enabled: go to[Google Cloud Console](console.cloud.google.com) > [APIs & Services](https://console.cloud.google.com/apis/dashboard), search for it, and click **Enable**.
2. Go to [Google Cloud Console](console.cloud.google.com) > [APIs & Services](https://console.cloud.google.com/apis/dashboard) > [Credentials](https://console.cloud.google.com/apis/credentials) and select your project.
3. Click **+ Create credentials → API key.** A new key is generated immediately.
4. Click **Edit API key** (pencil icon). Under _API restrictions_, select **Restrict key** and choose **Directions API**.

## Troubleshooting

### When I tested my Google Gemini API key, there was an error.

If you something like this instead, double check that you've copied your key correctly:

```
  "error": {
    "code": 400,
    "message": "API key not valid. Please pass a valid API key.",
    "status": "INVALID_ARGUMENT",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "API_KEY_INVALID",
        "domain": "googleapis.com",
        "metadata": {
          "service": "generativelanguage.googleapis.com"
        }
      },
      {
        "@type": "type.googleapis.com/google.rpc.LocalizedMessage",
        "locale": "en-US",
        "message": "API key not valid. Please pass a valid API key."
      }
    ]
  }
  ```
