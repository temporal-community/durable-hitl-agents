# Durable Human-in-the-Loop Agents 🍦 — Ice Cream Fleet Demo <img src="https://github.com/google/adk-docs/raw/main/docs/assets/agent-development-kit.png" alt="Google ADK" height="28">

> **A durable human-in-the-loop (HITL) example for AI agents** — framework-agnostic, built on Temporal (Google ADK + LangGraph). Adapted from the original **Meltdown** ice cream delivery fleet demo.

Companion demo for the AI Engineer World's Fair talk **"The Human Is an Async API: Designing Durable Human-in-the-Loop Agents."** Ziggy's Ice Cream runs its downtown San Francisco catering fleet on Temporal. Orders flow in from Moscone Center, Fisherman's Wharf, and Chinatown; AI agents reason about which driver to send; and Temporal guarantees every decision and delivery runs to completion. The demo shows **two durable human-in-the-loop patterns** side by side — one where a human interrupts the agents, one where an agent calls a human — both built on Temporal's durable signals and `wait_condition`.

<p align="center">
  <img src=".github/assets/meltdown-screenshot-3.png" alt="Meltdown demo dashboard" width="900">
</p>

## The two patterns, in code

Both human-in-the-loop patterns reduce to the **same durable Temporal primitive** — a `wait_condition` that pauses the workflow and a `@workflow.signal` that resumes it. The only difference is *who initiates*.

### Pattern A — The Human Calls the Agent (Google ADK)

A customer submits an order change mid-delivery (address change → pick a new SF location from a dropdown, or cancel); the driver **halts gracefully at the venue** and waits for a human to approve. ONE human gate feeds **both** loops: for an address change the **ADK assignment team re-reasons** the order for the new location (Fleet recomputes ETAs, Customer re-reads priority, Dispatch reassesses), and the **held driver reroutes** to it. Cancel is a fixed cancel (no re-reason); reject → deliver to the original destination. *Human-initiated interrupt with graceful halt, agent re-reasoning, and resumption.*

```python
# DriverRouteWorkflow (workflows.py) — driver reaches the venue, then PAUSES until a human resolves it
if order.order_id in self._pending_holds:
    self._status = "awaiting_update"
    await workflow.wait_condition(            # ⏸ durable pause on a signal
        lambda: self._pending_holds[order.order_id].decision is not None or self._stop
    )
    # decision: "cancel" | "address_change" | "release"  → skip / reroute / deliver

@workflow.signal
async def update_pending(self, inp):   # customer submits the change → driver holds
    self._pending_holds.setdefault(inp.order_id, PendingHold())

@workflow.signal
async def resolve_update(self, inp):   # the human's decision resumes the driver
    self._pending_holds[inp.order_id].decision = inp.change_type
```

The *same* approval also drives the agent's reasoning loop. On an approved address change, the parent (`_process_customer_change`) updates the order to the human's chosen location and feeds it back to the ADK assignment team via `_rereason_order` — so the human's edit is the new input the agents reason over, and the agents (not a fixed script) decide how to adjust before the held driver reroutes:

```python
# workflows.py — an approved address change re-invokes the ADK assignment team
async def _rereason_order(self, order_id, note):     # human → agent, in the reasoning loop
    # ...the order's coords are already updated to the human's chosen location, then:
    assignment = await self._run_adk_assignment(...)  # Fleet ∥ Customer → Dispatch run again
    # publish the re-assessment to the agent panels; the held driver then reroutes
```

### Pattern B — The Agent Calls the Human (LangGraph)

On the LangGraph tab, a looping multi-agent team assesses every order inline; **mid-reasoning**, an agent decides it needs a human — literally **by calling an `ask_human` tool**. That tool's execution is a durable LangGraph `interrupt()` that suspends the graph; the workflow surfaces the question, parks on a Temporal signal that **survives a worker crash**, and feeds the human's answer back into the agent's reasoning. *Agent-initiated, in the reasoning loop — not a boundary gate.*

```python
# langgraph_agents.py — the agent calls the human as a TOOL, mid-loop
@tool
def ask_human(question: str) -> str:
    """Ask a human for help/sign-off when you can't decide alone."""
    # body never runs: its execution is a durable interrupt() in the human node

async def _human_node(messages, agent_label, state):     # the ask_human "execution"
    answer = interrupt({"question": ..., "order_id": state["order_id"], ...})  # ⏸ suspend the graph
    return [ToolMessage(content=str(answer), ...)]        # answer flows back as the observation

# workflows.py — the durable wait IS a Temporal primitive
while result.get("__interrupt__"):
    self._pending_dispatch[oid] = result["__interrupt__"][0].value          # exposed via @workflow.query
    await workflow.wait_condition(lambda: oid in self._dispatch_answers)    # ⏸ durable pause
    answer = self._dispatch_answers.pop(oid)
    result = await compiled.ainvoke(Command(resume=answer), config=config)  # resume the graph

@workflow.signal
async def answer_dispatch(self, oid, decision):            # human → flips the wait_condition
    self._dispatch_answers[oid] = decision
```

*(The real `_await_dispatch_answer` adds a `_routes_done` shutdown escape; the snippet shows the bare `wait_condition` so the durable primitive is visible.)*

> **Two frameworks, one durable contract — the human is a tool the agent calls, and on Temporal that tool call is a signal.** (The `ask_human` "execution" is a LangGraph `interrupt()`; the durable wait + resume is a Temporal `wait_condition` + `answer_dispatch` signal.)

<p align="center">
  <a href="https://youtube.com/shorts/Wq7hiN2KYnk">
    <img src="https://img.youtube.com/vi/Wq7hiN2KYnk/hqdefault.jpg" alt="Watch the Meltdown demo on YouTube" width="280">
  </a>
  <br>
  <em>▶ <a href="https://youtube.com/shorts/Wq7hiN2KYnk">Watch the demo on YouTube</a></em>
</p>

Built with **Google ADK** (multi-agent reasoning for Pattern A), **LangGraph** via `temporalio.contrib.langgraph` (the looping multi-agent team with the in-loop `ask_human` tool for Pattern B), and **Temporal** for durable execution. Orders auto-generate on a timer. AI agents (Fleet, Customer, Dispatch) evaluate positions, capacity, ETAs, and priority — then assign each order to the best driver, spreading load across the fleet so all five stay active. Drivers batch-pickup at Ziggy's (the Ferry Building) and deliver sequentially. Both human-in-the-loop pauses are durable Temporal signals — survive worker death, resume exactly where they left off.

> **Terminology:** AI agents **reason** (LLM + tools, run inline via ADK). Delivery actors **execute** (child workflows that carry out routes). They are not Temporal workers.

## The Two Patterns

| Pattern | "The Human..." | Built on | What Happens | Durable primitive |
|---------|----------------|----------|--------------|-------------------|
| **A — Human-in-the-loop** | ...calls the agent | **Google ADK** (multi-agent) | A customer submits an order change mid-delivery — an address change (pick a new SF location from a dropdown) or cancel. The change is **customer-initiated** — the gate lives in the workflow, not in any LLM tool. The driver navigates to the venue but holds before delivering (`awaiting_update`). One human approval feeds **both** loops: approve cancel → delivery skipped; approve address change → the ADK assignment team **re-reasons** the order for the new location (Fleet recomputes ETAs, Customer re-reads priority, Dispatch reassesses), then the held driver reroutes to it; reject → deliver to the original destination. | Signal → `wait_condition` hold → resolve, then re-reason via ADK (two `wait_condition`s: parent waits for human, child waits for parent) |
| **B — Agent-in-the-loop** | ...gets called by the agent | **LangGraph** (`temporalio.contrib.langgraph`) | On the LangGraph tab, **every** order runs a looping **multi-agent** LangGraph team inline in the parent workflow (Fleet ∥ Customer are real reason→act→eval ReAct loops → Dispatch decides) — each Gemini reason call and **each tool call** run as its own Temporal **activity** recorded in the parent's history. **Mid-loop**, the Dispatch or Fleet agent can call the `ask_human` tool; that tool's execution is a durable LangGraph `interrupt()` that suspends the graph. The parent workflow (`_run_langgraph_assignment`) surfaces the question, parks on the `answer_dispatch` Temporal **signal** + `wait_condition`, and resumes the agent via `Command(resume=answer)` — the answer flows back as the agent's next observation. No per-order gate child; the HITL is inside the reasoning loop. Survives worker death. | The human is literally a tool the agent calls (`ask_human`) — but a durable, async one. On Temporal, that tool call's pause is a signal. |

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
2. **Pattern A — Human-in-the-loop tab:** pick an active order, choose **Address Change** (pick a new SF location from the dropdown) or **Cancel Order**, click **Submit Change**. The driver arrives at the venue but holds (`awaiting_update`) while a human decides. Click **Approve** / **Reject** — cancel skips delivery; address change has the ADK team **re-reason** the new location (Fleet/Customer/Dispatch reassess) before the held driver reroutes to it; reject delivers normally.
3. **Pattern B — Agent-in-the-loop tab:** click **Drop high-value order** to inject a premium Moscone catering order. The looping LangGraph team (Fleet ∥ Customer → Dispatch) assesses it inline in the parent workflow; **mid-reasoning** the Dispatch agent calls the `ask_human` tool, which suspends the graph on a durable `interrupt()` while the parent parks on the `answer_dispatch` signal; an approval card appears over the map. Approve or reject — the answer flows back into the agent's reasoning. To show durability, **kill the worker while the card is up** — the paused workflow survives; restart the worker and the pending question is still there.


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
│  └─ LangGraph tab inline (Pattern B):                                 │
│      looping multi-agent team (Fleet∥Customer→Dispatch) in the parent │
│      (each reason call + each tool call a Temporal activity)          │
│      agents call ask_human mid-loop → durable interrupt(); the parent │
│      parks on the answer_dispatch signal — no per-order gate child    │
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

**Order lifecycle (high-value, LangGraph tab):** High-value order injected → looping multi-agent team assesses inline in the parent (Fleet ∥ Customer → Dispatch) → mid-reasoning the Dispatch agent calls `ask_human` → durable LangGraph `interrupt()` suspends the graph while the parent parks on the `answer_dispatch` signal → human answers → `Command(resume=answer)` feeds it back into the agent's reasoning → on approve, commits to the least-loaded driver and delivers; on reject, the order is held/cancelled

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

**3-queue separation**: LLM calls are slow (3–5s). Without separate queues, assignment requests could starve navigation activities and cause heartbeat timeouts. The agents queue caps at 5 concurrent; delivery at 20. The workflows queue runs workflows plus `publish_agent_event` as a local activity (UI projection with minimal history). `GoogleAdkPlugin` is registered on **both** the workflow worker (sandbox passthroughs + deterministic runtime for replay) and the agents worker (`invoke_model` activity registration). `LangGraphPlugin(graphs={...})` is registered on the **workflow** worker — it registers exactly **one** LangGraph graph: the looping multi-agent team (`GRAPH_NAME = "dispatch_team"`, Fleet ∥ Customer reason→act→eval loops → Dispatch, run inline by the parent workflow with the in-loop `ask_human` tool). The team's node activities (the Fleet / Customer / Dispatch agent Gemini reason calls and each tool call) execute on this worker; the parent workflow runs the team graph inline. The demo's Pattern B HITL happens in-loop via `ask_human`, not a gate child. Agents use the upstream `TemporalModel` with `summary_fn=_build_summary` — `_build_summary` in `agents.py` generates context-aware Temporal UI summaries per LLM call.

### Core mechanism — how the LangGraph path is invoked

On the **🤖 Agent → Human** tab, the *same* multi-agent idea runs on LangGraph instead of ADK — and it runs **inline inside the parent workflow**, exactly like the ADK path. The team is a looping ReAct team (Fleet ∥ Customer reason→act→eval loops → Dispatch), and the HITL is **inside the loop**: an agent calls the `ask_human` tool mid-reasoning, which suspends the graph on a durable `interrupt()`. There is no per-order gate child — the parent drives the interrupt with a Temporal signal.

**1. The tab selects the framework** (UI → a Temporal signal — it does *not* start a workflow):

```js
// frontend/index.html — switching to the agent tab
api('dispatch-mode', 'POST', { mode: tabName === 'agent' ? 'langgraph' : 'adk' });
```
```python
# server.py — the endpoint just signals the already-running parent workflow
await handle.signal(MeltdownDemoWorkflow.set_dispatch_mode, body.mode)
# workflows.py — set_dispatch_mode signal sets a flag on the parent
self._dispatch_mode = mode
```

**2. Each new order runs the LangGraph assessment inline in the parent** (`workflows.py` → `_assign_order` → `_run_langgraph_assignment`):

```python
if self._dispatch_mode == "langgraph":
    asyncio.create_task(
        self._run_langgraph_assignment(order, self._least_loaded_driver(), onum)
    )
    return
```
```python
# _run_langgraph_assignment — the graph is compiled and invoked HERE, in the parent
compiled = graph(GRAPH_NAME).compile(checkpointer=InMemorySaver())
result = await compiled.ainvoke(state, config=config)   # Fleet ∥ Customer → Dispatch
```

`GRAPH_NAME` is registered on the workflow worker by `LangGraphPlugin` (`worker.py`). Each node carries `metadata={"execute_in": "activity"}`, so the Fleet / Customer / Dispatch Gemini calls run as **Temporal activities recorded in the parent's event history** — not a separate child workflow. It runs as a concurrent task so the fleet keeps moving while the agents deliberate.

**3. Mid-loop, an agent calls `ask_human` — the human is a tool** (`langgraph_agents.py`):

```python
@tool
def ask_human(question: str) -> str:
    """Ask a human for help/sign-off when you can't decide alone."""
    raise NotImplementedError  # its execution is a durable interrupt() in the human node

async def _human_node(messages, agent_label, state):
    answer = interrupt({"question": ..., "order_id": state["order_id"], ...})  # suspend the graph
    return [ToolMessage(content=str(answer), ...)]   # answer flows back as the next observation
```

There is **no code threshold** — whether to ask is the agent's judgment, guided by `ESCALATION_GUIDANCE` and the per-agent system prompts in `langgraph_agents.py` (routine orders dispatch; only exceptional ones warrant calling `ask_human`).

**4. The parent drives the interrupt with a durable signal** (`workflows.py` → `_run_langgraph_assignment`):

```python
result = await compiled.ainvoke(state, config=config)
while result.get("__interrupt__"):
    self._pending_dispatch[order.order_id] = result["__interrupt__"][0].value  # surface the question
    answer = await self._await_dispatch_answer(order.order_id)   # ⏸ wait_condition on answer_dispatch
    if answer is None:                                           # demo shutting down — exit cleanly
        return
    result = await compiled.ainvoke(Command(resume=answer), config=config)     # resume the agent
```

```python
@workflow.signal
async def answer_dispatch(self, order_id: str, decision: str):   # the human responds → resolves it
    self._dispatch_answers[order_id] = decision
```

The pause is the same durable primitive as Pattern A — a Temporal signal (`answer_dispatch`) + `wait_condition` (see *The two patterns, in code* at the top). The difference: it fires **inside** the agent's reasoning loop (via `interrupt()`), not at a boundary gate — so the human's answer becomes the observation the agent reasons on next.

This durability is **verified**: the in-loop `interrupt` survives a worker kill — `kill -9` the worker while parked on the question, restart, then signal the answer, and the agent resumes. Temporal replays from event history; LangGraph's `InMemorySaver` is non-durable on its own — Temporal is what makes the wait survive the crash.

Why `interrupt()` specifically? For an in-loop pattern, the human's answer has to flow **back into the running graph** as the agent's next observation — and `interrupt()` is the only LangGraph primitive that can suspend and resume a graph **mid-node** and inject that answer via `Command(resume=answer)`. So there's **no "signal-only, no interrupt" option** here: the Temporal `answer_dispatch` signal + `wait_condition` is the durable *wait*, but `interrupt()` is the graph plumbing that lets the answer rejoin the loop.

> **In short:** the tab flips a flag → every order runs the looping LangGraph team inline in `MeltdownDemoWorkflow` (each reason call + tool call an activity in the parent's history) → mid-loop an agent calls `ask_human`, which suspends the graph on a durable `interrupt()`, and the parent resolves it with the `answer_dispatch` signal. No per-order child.

#### Why LangGraph and ADK look so different — you own the loop vs. batteries-included

The two framework files diverge on purpose. **In LangGraph, you own the loop**, so `langgraph_agents.py` carries the helpers that hand-build it: the reason↔act loop and its routing, per-tool-call activities (`_run_tools`), message parsing (`_coerce_text` / `_last_text`), the `interrupt()` human node (`_human_node`), and model + tool binding (`_chat_model`). **ADK doesn't need any of that** — its `Runner` bakes the loop in. `TemporalModel` + `activity_tool` run the reason→act→observe cycle and tool-calls-as-activities for you, and structured output comes back through ADK session state. So: **LangGraph = assemble the loop from primitives; ADK = the loop is batteries-included** — same durable contract underneath, different amount of plumbing on top.

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
| `agent_fleet/workflows.py` | Temporal workflows — owns driver state, signals, queries, Temporal-native retry for disconnect. Drives both in-loop HITL flows: `_run_langgraph_assignment` (Pattern B — surfaces `ask_human`, waits on `answer_dispatch`, resumes via `Command`) and `_process_customer_change`/`_rereason_order` (Pattern A — one approval gate that holds the driver and, on an address change, re-reasons via the ADK team). Includes `OrderGenerationWorkflow` |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Customer, Dispatch Agent (an approved address change re-runs this team via `_rereason_order` → `_run_adk_assignment`) |
| `agent_fleet/langgraph_agents.py` | Pattern B — the looping LangGraph multi-agent team (mirror of `agents.py`): Fleet ∥ Customer reason→act→eval ReAct loops → Dispatch loop, each tool call its own Temporal activity. Agents call the in-loop `ask_human` tool, whose execution is a durable `interrupt()` |
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
