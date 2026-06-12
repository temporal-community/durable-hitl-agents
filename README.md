# Durable Human-in-the-Loop Agents 🍦 — Ice Cream Fleet Demo <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google ADK" height="28">

> **A durable human-in-the-loop (HITL) example for AI agents** — framework-agnostic, built on Temporal (Google ADK + LangGraph). Adapted from the original **Meltdown** ice cream delivery fleet demo.

Companion demo for the AI Engineer World's Fair talk **"The Human Is an Async API: Designing Durable Human-in-the-Loop Agents."** Ziggy's Ice Cream runs its downtown San Francisco catering fleet on Temporal. Orders flow in from Moscone Center, Fisherman's Wharf, and Chinatown; AI agents reason about which driver to send; and Temporal guarantees every decision and delivery runs to completion. The demo shows **two durable human-in-the-loop patterns** side by side — one where a human interrupts the agents, one where an agent calls a human — both built on Temporal's durable signals and `wait_condition`.

<p align="center">
  <img src=".github/assets/meltdown-screenshot-3.png" alt="Meltdown demo dashboard" width="900">
</p>

## The two patterns, in code

Both human-in-the-loop patterns reduce to the **same durable Temporal primitive** — a `wait_condition` that pauses the workflow and a `@workflow.signal` that resumes it. The only difference is *who initiates*.

### Pattern A — The Human Calls the Agent (Google ADK)

An operator interrupts a delivery mid-flight; the driver **halts gracefully at the venue** and waits for a human decision, then continues. *Human-initiated interrupt with graceful halt and resumption.*

```python
# DriverRouteWorkflow (workflows.py) — driver reaches the venue, then PAUSES until a human resolves it
if order.order_id in self._pending_holds:
    self._status = "awaiting_update"
    await workflow.wait_condition(            # ⏸ durable pause on a signal
        lambda: self._pending_holds[order.order_id].decision is not None or self._stop
    )
    # decision: "cancel" | "address_change" | "release"  → skip / reroute / deliver

@workflow.signal
async def update_pending(self, inp):   # operator submits the change   (human → agent)
    self._pending_holds.setdefault(inp.order_id, PendingHold())

@workflow.signal
async def resolve_update(self, inp):   # the human's decision resumes the driver
    self._pending_holds[inp.order_id].decision = inp.change_type
```

### Pattern B — The Agent Calls the Human (LangGraph)

On the LangGraph tab, a multi-agent team assesses every order inline; for a high-value order the dispatch agent decides — **by calling a tool** — that it needs a human, and the workflow turns that human into a durable signal that **survives a worker crash**. *Agent-initiated approval gate with timeout and escalation.*

```python
# dispatch_gate.py — the agent decides, by calling a tool, that it needs a human
@tool
def request_human_approval(reason: str, recommendation: str) -> str:
    """Escalate before committing scarce fleet capacity."""

model = init_chat_model(DEFAULT_MODEL, model_provider=provider).bind_tools([request_human_approval])
resp = await model.ainvoke(prompt)            # ← the agent's tool-call decision

# The WORKFLOW performs the HITL — the human is a durable Temporal signal
async def _await_human(self, escalation_seconds):
    try:
        await workflow.wait_condition(         # ⏸ pause, survives worker death
            lambda: self._decision is not None,
            timeout=timedelta(seconds=escalation_seconds),
        )
    except TimeoutError:                        # timeout → escalate to a backup approver
        self._approver_tier = "backup"
        await workflow.wait_condition(lambda: self._decision is not None)
    return self._decision

@workflow.signal
def approve(self, decision: str):              # the human responds → the async endpoint resolves
    self._decision = decision
```

> **Two frameworks, one durable contract — the human is a signal.** (LangGraph's native `interrupt()` is first-class too; here it's behind the `HITL_MODE=interrupt` toggle.)

<p align="center">
  <a href="https://youtube.com/shorts/Wq7hiN2KYnk">
    <img src="https://img.youtube.com/vi/Wq7hiN2KYnk/hqdefault.jpg" alt="Watch the Meltdown demo on YouTube" width="280">
  </a>
  <br>
  <em>▶ <a href="https://youtube.com/shorts/Wq7hiN2KYnk">Watch the demo on YouTube</a></em>
</p>

Built with **Google ADK** (multi-agent reasoning for Pattern A), **LangGraph** via `temporalio.contrib.langgraph` (the agent-initiated gate for Pattern B), and **Temporal** for durable execution. Orders auto-generate on a timer. AI agents (Fleet, Customer, Dispatch) evaluate positions, capacity, ETAs, and priority — then assign each order to the best driver, spreading load across the fleet so all five stay active. Drivers batch-pickup at Ziggy's (the Ferry Building) and deliver sequentially. Both human-in-the-loop pauses are durable Temporal signals — survive worker death, resume exactly where they left off.

> **Terminology:** AI agents **reason** (LLM + tools, run inline via ADK). Delivery actors **execute** (child workflows that carry out routes). They are not Temporal workers.

## The Two Patterns

| Pattern | "The Human..." | Built on | What Happens | Durable primitive |
|---------|----------------|----------|--------------|-------------------|
| **A — Human-in-the-loop** | ...calls the agent | **Google ADK** (multi-agent) | An operator submits a customer change (address change / cancel) mid-delivery. The change is **operator-initiated** — the gate lives in the workflow, not in any LLM tool. The driver navigates to the venue but holds before delivering (`awaiting_update`). A human approves or rejects: approve cancel → delivery skipped; approve reroute → driver navigates to Oracle Park; reject → deliver normally. | Signal → `wait_condition` hold → resolve (two `wait_condition`s: parent waits for human, child waits for parent) |
| **B — Agent-in-the-loop** | ...gets called by the agent | **LangGraph** (`temporalio.contrib.langgraph`) | On the LangGraph tab, **every** order runs a **multi-agent** LangGraph team inline in the parent workflow (Fleet + Customer assess in parallel → Dispatch decides) — each node a real Gemini call run as a Temporal **activity** recorded in the parent's history. The Dispatch agent decides whether to escalate by calling `request_human_approval`; if it doesn't, it commits directly. Only on escalation does a durable `gate-<order_id>` child (`DispatchGateWorkflow`) spawn to do the HITL pause via a durable Temporal **signal** + `wait_condition` (timeout escalates to a backup approver). LangGraph `interrupt()` is an optional back-pocket toggle (`HITL_MODE=interrupt`). Survives worker death. | The human is just another tool the agent calls — but a durable, async one. On Temporal, that tool call is a signal. |

The active framework is chosen by the UI tab and applies to all orders. On the LangGraph tab, routine auto-generated orders top out around $1,950 (servings ≤150 × ≤$13), so the Dispatch agent commits them directly; only a genuinely high-value order escalates. The **Drop high-value order** button injects a premium Moscone order the agent escalates — so the agent-in-the-loop demo fires when you choose, not at random.

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
The `run.sh` script syncs dependencies via [uv](https://docs.astral.sh/uv/) (install once with `brew install uv`) and starts everything.

```bash
./run.sh    # uv sync + Temporal dev server + worker process + server process
```

`run.sh` is the easy path. If you start the worker by hand, note it does **not** load `.env` on its own — pass the env file so live mode picks up your keys:

```bash
uv run --env-file .env python -m agent_fleet.worker
```

### 3. Open the dashboard

| Interface | URL |
|-----------|-----|
| **Demo dashboard** | http://localhost:8080 |
| **Temporal UI** (workflow history, event log) | http://localhost:8233 |

## Demo Flow

The dashboard has two tabs — one per pattern. Both start the same way.

1. **Start Deliveries** — Ziggy's (the Ferry Building) opens for business. Orders flow in from Moscone Center, Fisherman's Wharf, and Chinatown. The ADK agents reason per-order and assign to the least-loaded driver. Drivers batch-pickup and deliver sequentially.
2. **Pattern A — Human-in-the-loop tab:** pick an active order, choose **Address Change** or **Cancel Order**, click **Submit Change**. The driver arrives at the venue but holds (`awaiting_update`) while a human decides. Click **Approve** / **Reject** — cancel skips delivery, address change reroutes to Oracle Park, reject delivers normally.
3. **Pattern B — Agent-in-the-loop tab:** click **Drop high-value order** to inject a premium Moscone catering order. The LangGraph team (Fleet + Customer → Dispatch) assesses it inline in the parent workflow; the Dispatch agent decides to call `request_human_approval`, which spawns a durable `gate-<order_id>` child that parks on a Temporal signal; an approval card appears over the map. Approve or reject. To show durability, **kill the worker while the card is up** — the gate survives; restart the worker and the pending approval is still there.


## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Temporal Server                               │
│                     event log, replay, scheduling                      │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
          ┌────────────────────┼──────────────────────┐
          │                    │                      │
          ▼                    ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Worker Process (3 task queues)                       │
│                                                                        │
│  WORKFLOWS QUEUE             DELIVERY QUEUE         AGENTS QUEUE       │
│  ─────────────────           ──────────────         ────────────       │
│  MeltdownDemoWorkflow        navigate_to            invoke_model       │
│  ├─ OrderGeneration          pickup_orders          tool_get_fleet     │
│  │   (child, timer)          deliver_order          tool_get_route     │
│  ├─ ADK inline:              get_route_polyline     tool_get_order     │
│  │   Fleet + Customer        generate_order         google_search      │
│  │   in parallel →           sync_driver_position   tool_submit_       │
│  │   Dispatch Agent          execute_customer_        assignment       │
│  │                             change                                  │
│  ├─ 5 DriverRouteWorkflows                         TemporalModel      │
│  │   (Driver-A … Driver-E)                          routes LLM calls   │
│  │   batch pickup → deliver                         + tool calls here  │
│  │   sequentially → return                          (max 5 concurrent) │
│  │   (max 20 concurrent)                                               │
│  ├─ LangGraph tab inline:                                             │
│  │   multi-agent team (Fleet+Customer→Dispatch) runs in the parent    │
│  │   (each node a Temporal activity in the parent's history)          │
│  └─ DispatchGateWorkflow (Pattern B, per ESCALATED order)             │
│      gate-<order_id> child does the HITL pause via durable signal     │
│      (interrupt() = back-pocket toggle)                               │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────┐      ┌───────────────────────────────────────┐
│    Server Process        │      │           Frontend (SPA)              │
│    FastAPI + WebSocket   │◄────►│  Leaflet map + WebSocket state feed   │
│                          │      │  Agent reasoning panels               │
│  Reads FleetState        │      │  Fleet / order status cards           │
│  (SQLite) for snapshot   │      │  Demo controls (both tabs)            │
│                          │      └───────────────────────────────────────┘
│  Sends signals / queries:│
│  start, reset,           │
│  customer-change +       │
│  approve-change (A),     │
│  inject-order +          │
│  approve-dispatch (B)    │
└──────────┬───────────────┘
           │ queries + signals
           ▼
     Temporal Server
```

**Order lifecycle (routine):** Order generates on timer → ADK agents reason (Fleet + Customer in parallel → Dispatch) → capacity check + least-loaded assignment → driver batch-picks up at Ziggy's → delivers sequentially to venues → signals parent on each completion → returns to base

**Order lifecycle (high-value, LangGraph tab):** High-value order injected → multi-agent team assesses inline in the parent (Fleet + Customer → Dispatch) → Dispatch agent escalates by calling `request_human_approval` → durable `gate-<order_id>` child spawns for the human pause (durable Temporal signal) → on approve, commits to the least-loaded driver and delivers; on reject, the order is cancelled

**How ADK and Temporal map to each other:**

| ADK concept | Temporal concept |
|-------------|-----------------|
| **LLM Agent** (`Agent` + `TemporalModel`) | Each Gemini call → `invoke_model` activity, recorded in event log |
| **Orchestrator Agent** (`SequentialAgent`, `ParallelAgent`) | Pure Python coordination — no Temporal activity, no LLM |
| **Tool call** (via `activity_tool`) | Each tool invocation → named Temporal activity, retryable + replayable |
| **Entire agent pipeline** | Runs inline in the workflow via `_run_adk_assignment()` |

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

**Two processes**: `run.sh` starts a worker process and a server process (plus Temporal dev server). The server builds the frontend snapshot from FleetState (`_build_snapshot()` → `fleet.snapshot()`, SQLite shared across processes) and otherwise sends signals / runs queries only — it runs no workers. Workers run three Temporal workers on three task queues.

**3-queue separation**: LLM calls are slow (3–5s). Without separate queues, assignment requests could starve navigation activities and cause heartbeat timeouts. The agents queue caps at 5 concurrent; delivery at 20. The workflows queue runs workflows plus `publish_agent_event` as a local activity (UI projection with minimal history). `GoogleAdkPlugin` is registered on **both** the workflow worker (sandbox passthroughs + deterministic runtime for replay) and the agents worker (`invoke_model` activity registration). `LangGraphPlugin(graphs={...})` is registered on the **workflow** worker — it now registers **three** LangGraph graphs: the inline multi-agent assessment graph (`GRAPH_NAME`, run inline by the parent workflow), the demo HITL-only interrupt-pause graph (`GRAPH_NAME_HUMAN`), and the full assess+interrupt graph (`GRAPH_NAME_INTERRUPT`, used by the standalone spike path). Their node activities (the Fleet / Customer / Dispatch agent Gemini calls) execute on this worker; the parent workflow runs the assessment graph inline, and `DispatchGateWorkflow` (the `gate-<order_id>` child that does the HITL pause) is registered alongside the other workflows. Agents use the upstream `TemporalModel` with `summary_fn=_build_summary` — `_build_summary` in `agents.py` generates context-aware Temporal UI summaries per LLM call.

### What each agent reasons about

| Agent | Reasoning | Tools |
|-------|-----------|-------|
| **Fleet Agent** (operational) | Delivery actor positions, capacity (free slots), ETAs to destination — excludes unavailable actors | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps) |
| **Customer Agent** (priority) | VIP vs standard tier, deadline pressure, venue events (conference catering, receptions, festivals), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Dispatch Agent** (synthesis) | Weighs Fleet + Customer assessments, picks final delivery actor | `tool_submit_assignment` |

Fleet and Customer run **in parallel** (`ParallelAgent`), then the Dispatch Agent runs **sequentially** after both complete (`SequentialAgent`). All tools are wrapped with `activity_tool()` — each call is a Temporal activity, recorded in the event log. If the worker restarts mid-call, results replay from the log.

> **Note:** Gemini's built-in `google_search` grounding normally can't be combined with custom function tools in the same request. ADK's `GoogleSearchTool(bypass_multi_tools_limit=True)` enables this — the Customer Agent uses Google Search alongside `tool_get_order_priorities` in a single agent, no sub-agent needed.

> **Note on Maps API errors:** `tool_get_route_info` calls the Google Maps Directions API for driving ETAs. Occasional failures (rate limiting, quota, transient errors) are normal — every tool call is a Temporal activity with its own retry policy. The error is returned to the LLM as context, the Fleet Agent notes the missing ETA, and the Dispatch Agent assigns with available data. This is the system working as designed, not a bug.
>
> **Dormant disconnect path:** The codebase retains agent/driver disconnect logic (tool activities raise on disconnect, Temporal retries, orders flag `degraded`). It is **not** part of the talk's two demos and the UI no longer exposes disconnect controls.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (`brew install uv`) — Python package + venv manager used to install dependencies
- [Temporal CLI](https://docs.temporal.io/cli) (`brew install temporal`)
- Google Gemini API key (`GOOGLE_API_KEY`) — required for the demo. Restricted to **Generative Language API**.
- Google Maps API key (`GOOGLE_MAPS_API_KEY`) — used for route polylines and ETAs. Restricted to **Directions API**. This must be a separate key from `GOOGLE_API_KEY` because the Generative Language API cannot share a key with standard Google Cloud APIs.

The worker is live-only and requires `GOOGLE_API_KEY` (ADK + all API activities); there is no mock mode. Default model is `gemini-2.5-flash` (override with `DEFAULT_MODEL` env var).

## Key Files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads (incl. `DriverSnapshot`) |
| `agent_fleet/simulation.py` | FleetState — SQLite WAL-backed write-only UI projection (`fleet_state.db`, cross-process) |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps API, agent tools |
| `agent_fleet/workflows.py` | Temporal workflows — owns driver state, signals, queries, Temporal-native retry for disconnect. Includes `OrderGenerationWorkflow` |
| `agent_fleet/agents.py` | ADK agent composition (Pattern A) — Fleet, Customer, Dispatch Agent |
| `agent_fleet/dispatch_gate.py` | Pattern B — multi-agent LangGraph dispatch-gate graph (Fleet + Customer → Dispatch) + `DispatchGateWorkflow`. Agent decides → workflow does HITL via durable Temporal signal (default); `interrupt()` is a back-pocket toggle (`HITL_MODE=interrupt`) |
| `agent_fleet/config.py` | Centralized env config — `GOOGLE_API_KEY`, `GOOGLE_MAPS_API_KEY`, `DEFAULT_MODEL`, `TEMPORAL_ADDRESS` |
| `agent_fleet/queues.py` | Task queue name constants (workflows / delivery / agents) |
| `agent_fleet/worker.py` | Three Temporal workers — workflow-only, delivery, agents. Live-only; requires `GOOGLE_API_KEY` |
| `agent_fleet/server.py` | FastAPI server — signal/query API (both patterns), WebSocket, frontend |
| `agent_fleet/locations.py` | Downtown SF venue pool (Moscone, Fisherman's Wharf, Chinatown; Ferry Building shop) and random order generation |
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
