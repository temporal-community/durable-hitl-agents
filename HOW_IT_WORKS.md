# How It Works — Under the Hood

Presenter-facing deep dive for **"The Human Is an Async API: Designing Durable
Human-in-the-Loop Agents."** This is the architectural grounding to answer
"how does that actually work?" questions confidently. Not for reading aloud —
see [DEMO_GUIDE.md](DEMO_GUIDE.md) for the talk track.

The demo is Ziggy's Ice Cream catering fleet running on **downtown San
Francisco** (the Ferry Building is the shop; orders come from Moscone Center,
Fisherman's Wharf, and Chinatown). It shows **two durable human-in-the-loop
patterns** on Temporal, each built on a **different agent framework**, to make
the point that the durable-HITL pattern is framework-agnostic.

---

## The two use cases, the two frameworks

| | Pattern A — Human-in-the-loop | Pattern B — Agent-in-the-loop |
|---|---|---|
| "The Human…" | …calls the agent | …gets called by the agent |
| Framework | **Google ADK** (`temporalio[google-adk]`) | **LangGraph** (`temporalio.contrib.langgraph`) |
| Who initiates | An **operator**, externally, mid-delivery | The **agent**, when it hits a decision it shouldn't make alone |
| Triggers on | **Routine** orders (every auto-generated order) | A **high-value** order (`order_value >= GATE_REVIEW_VALUE` = $2,000), injected on demand |
| Agents involved | Fleet + Customer (parallel) → Dispatch (sequential) | Fleet + Customer (parallel) → Dispatch (sequential) — a separate LangGraph team |
| The HITL gate lives in | the **workflow**, not any agent tool | the **workflow** (default), via the agent's tool call |
| Durable primitive | signal → `wait_condition` hold → resolve | signal → `wait_condition` hold → resolve (timeout → backup approver) |

**The key routing fact:** routine and high-value orders take *different* paths.

- **Routine orders → ADK.** The ADK multi-agent pipeline reasons about every
  routine order and assigns it to a driver.
- **High-value orders → LangGraph gate, bypassing ADK entirely.** When
  `order_value >= GATE_REVIEW_VALUE`, `_assign_order` returns early into
  `_spawn_gate` *before* ADK ever runs. The LangGraph dispatch gate — its own
  multi-agent team — handles that order. ADK never sees it.

This split is deliberate: each framework owns one use case, on the same Temporal
runtime, with the same durable-signal HITL primitive underneath both.

The disconnect/recovery scenarios (agent disconnect, driver disconnect, tool
degradation) are **dormant code**, not demo features — the UI no longer surfaces
disconnect controls. The signals, retry logic, and `degraded` flag still exist
and are documented below as mechanism, not as something you'd show on stage.

---

## Terminology

This demo has two distinct actor types:

- **AI Agents** — these **reason**. They call LLMs, use tools, and make
  decisions. Pattern A's agents (Fleet, Customer, Dispatch) run inline in the
  workflow via ADK. Pattern B's agents (Fleet, Customer, Dispatch) are LangGraph
  nodes that run as Temporal activities.
- **Delivery actors** (Driver-A through Driver-E) — these **execute**. They
  receive orders via signals, batch-pickup at Ziggy's (the Ferry Building), then
  deliver sequentially to multiple venues before returning. Each runs in its own
  child workflow (`DriverRouteWorkflow`). They don't reason.

In Temporal terms, the delivery actors are **child workflows**. They are not
Temporal workers (infrastructure) and not AI agents (reasoning).

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as
> code — workflows and activities — and Temporal guarantees it runs to
> completion even if the service crashes, times out, or gets disconnected. Every
> step is recorded in an event log. If the worker dies mid-execution, Temporal
> replays the history, and your code resumes exactly where it left off."

**Key points to land:**
- Workflows are durable — crashes don't lose state.
- Activities are retryable by default — transient failures self-heal.
- Signals let you inject events into a running workflow (new order, delivery
  complete, human approval).
- The Temporal UI shows the full event history for every workflow run — nothing
  is a black box. This is what makes the **worker-kill durability** demo land:
  kill the worker mid-approval, restart it, and the pending state is still there.

---

## How Temporal replay works

Temporal records every activity result in an append-only event log on the
Temporal server. When the worker crashes and restarts, it re-executes your
workflow function from the top — but when it hits an `execute_activity()` call
that already completed, Temporal intercepts it and returns the cached result from
the log instead of running the activity again. The workflow code runs again; the
side effects don't.

This means workflow code has one strict rule: **it must be deterministic**. No
real I/O, no `random`, no `datetime.now()`, no `asyncio.sleep()` directly.
Temporal provides sandboxed equivalents:

| Don't use in a workflow | Use instead |
|------------------------|-------------|
| `logging.info()` | `workflow.logger.info()` — suppressed during replay |
| `asyncio.sleep()` | `workflow.sleep()` — returns immediately if already completed |
| `datetime.now()` | `workflow.now()` — deterministic time from the event log |
| Any real I/O | `workflow.execute_activity()` — cached result during replay |

If you break determinism, Temporal raises a non-determinism error on replay.
This is a feature — it catches bugs that would otherwise silently corrupt state.

> **Why LangGraph nodes are `async`:** any callable that runs *inline in the
> workflow* (the interrupt node, conditional edges) is `async`, because LangGraph
> offloads *sync* callables to a thread executor, which Temporal's deterministic
> event loop forbids.

---

## The workflow classes

**`MeltdownDemoWorkflow`** is the brain. It owns the fleet state — driver
positions, order assignments, disconnect/reconnect status. It routes each new
order: **routine → ADK inline** (`_run_adk_assignment()` in live mode), or
**high-value → the LangGraph dispatch gate** (`_spawn_gate`). It builds
`DriverSnapshot`s from its own state, applies the capacity guardrail and
least-loaded balancing, and handles customer changes. It never does delivery
work directly — it delegates to child workflows.

**`DriverRouteWorkflow`** is the legs. One instance per driver, it batches
pending orders: navigate to Ziggy's → batch-pickup all orders → deliver
sequentially (venue A → venue B → …) → signal parent after each delivery →
return to base → loop. It owns its own state (status, is_disconnected,
is_recovering, path_history, current_orders). The Pattern A HITL hold lives here:
on `update_pending` the driver navigates to the venue but holds before delivering
(`awaiting_update`, `wait_condition`); on `resolve_update` it cancels, reroutes
(to Oracle Park), or releases. Its HITL state is a **per-order dict**
(`_pending_holds`), so two changes on the same driver for different orders each
get their own slot.

**`OrderGenerationWorkflow`** is a child workflow that generates orders on a
timer and signals the parent with each new order. The first 3 orders fire in a
quick burst (2s apart) to get multiple drivers on the road, then it settles into
a normal cadence (±30% jitter around a 10s base). Auto-generated orders stay
*below* `GATE_REVIEW_VALUE`, so they never trip Pattern B — only the deliberately
injected premium order does.

**`DispatchGateWorkflow`** is Pattern B, one instance per high-value order
(`id=gate-<order_id>`). It runs concurrently with the rest of the fleet — the
fleet keeps moving while the agent (and possibly a human) decides. Covered in
detail below.

The workflows connect through signals in both directions:
- **Parent → child:** `add_order`, `update_pending` (HITL hold), `resolve_update`
  (HITL decision), `cancel_order`, plus dormant `driver_disconnected` /
  `driver_reconnected`.
- **Child → parent:** `order_delivered` (driver state), `new_order` (from the
  order generator), `dispatch_gate_awaiting` (from the gate — see Pattern B).

Both child → parent signals are **guarded with try/except** so a terminated
parent (e.g. during demo reset) can't crash the child mid-delivery.

---

## Pattern A — Human-in-the-loop (Google ADK)

**The human calls the agent.** An operator submits a customer change (address
change / cancel) mid-delivery; the driver holds at the venue; a human supervisor
approves or rejects.

This is **operator-in-the-loop**, not agent-in-the-loop. The change is initiated
*externally* (operator submits via REST) and the gate lives in the **workflow**,
not in any agent tool. The ADK agents never see the change. (Contrast an
`ask_user`-style `@function_tool` where the LLM itself pauses for clarification —
that's Pattern B's shape, not this one.)

The flow:
1. `POST /api/customer-change` → signals the parent `customer_change` *and*
   signals the child `update_pending` to hold.
2. The driver navigates to the venue but holds before delivering
   (`awaiting_update`, `wait_condition`). The parent waits for the human; the
   child waits for the parent — **two `wait_condition` pauses, both durable**.
3. `POST /api/approve-change` → signals `change_approved` → `execute_customer_change`
   activity → parent signals `resolve_update` to the child with the decision:
   cancel → skip delivery; address_change → reroute to **Oracle Park**; release →
   deliver normally.

`deliver_order` returns `success=False` when a cancel wins the race, so the
workflow skips the `order_delivered` signal for cancelled orders. The child's
HITL hold also escapes on `self._stop` so demo shutdown can't leave a parked
child hanging the parent's `await handle` join.

### Where the ADK agents fit

The ADK agents run **inline in the workflow** via
`_run_adk_assignment()` in `MeltdownDemoWorkflow`. The workflow builds
`DriverSnapshot`s from its own state and passes them to the ADK pipeline. Each
LLM call becomes an `invoke_model` Temporal activity via `TemporalModel`; each
tool call becomes a Temporal activity via `activity_tool`. If an activity fails,
Temporal retries.

The pipeline is composed in `agent_fleet/agents.py` with ADK's `ParallelAgent`
and `SequentialAgent`:

```python
def create_order_assignment_agent() -> SequentialAgent:
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),    # positions, capacity, ETAs
            create_assignment_customer_agent(), # priority, deadline, venue context
        ],
    )
    dispatch_agent = create_assignment_dispatch_agent()  # synthesizes → tool_submit_assignment
    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, dispatch_agent],
    )
```

**What each agent reasons about:**

| Agent | What it evaluates | Tools |
|-------|-------------------|-------|
| **Fleet Agent** | Delivery actor positions, free capacity slots, driving ETAs to destination | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps Directions) |
| **Customer Agent** | VIP vs standard tier, deadline tightness, venue events (conference catering, receptions, festivals), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Dispatch Agent** | Synthesizes both assessments, picks the final delivery actor, submits a structured assignment | `tool_submit_assignment` |

Fleet Agent and Customer Agent run in parallel; the Dispatch Agent runs
sequentially after both complete. Fleet, Customer, and Dispatch are all **LLM
Agents** (`Agent` + `TemporalModel(...)`). `create_order_assignment_agent()`
returns an **Orchestrator Agent** (`SequentialAgent`) — no model, no LLM call, no
Temporal activity. It purely sequences the sub-agents.

After ADK returns an assignment, the parent applies a **capacity guardrail** (if
ADK picks a full or disconnected driver, reassign to the next available) and then
**spreads load across the fleet**: among eligible drivers it prefers the
least-loaded one, so all five stay active. The agents still reason and publish
their assessment; this only rebalances the final destination.

---

## Pattern B — Agent-in-the-loop (LangGraph dispatch gate)

**The agent calls the human.** A high-value order bypasses ADK and routes to a
per-order `DispatchGateWorkflow`. Inside it runs a **multi-agent LangGraph team**
that mirrors the ADK side, and the Dispatch agent decides for itself whether to
escalate to a human.

### Routing

In `MeltdownDemoWorkflow._assign_order`:

```python
# High-value orders bypass ADK entirely and route to the LangGraph gate.
if order.order_value >= GATE_REVIEW_VALUE:
    self._spawn_gate(order, self._least_loaded_driver(), False, onum)
    return
# ...otherwise, routine: run ADK inline.
```

`GATE_REVIEW_VALUE` is $2,000. Routine auto-generated orders stay under it; the
gate fires only on the deliberately injected premium Moscone order
(`POST /api/inject-order`).

### The multi-agent gate graph

`build_gate_graph` (in `dispatch_gate.py`) compiles a LangGraph graph via
`temporalio.contrib.langgraph`. It mirrors the ADK team — Fleet and Customer
assess in parallel, then Dispatch decides:

```
START → fleet_agent  ─┐
START → customer_agent ┴→ dispatch_agent → (END | request_human → finalize → END)
```

Each of `fleet_agent`, `customer_agent`, and `dispatch_agent` is a **real Gemini
call** (through `init_chat_model`, provider-swappable via `MODEL_PROVIDER`)
executed as a **Temporal activity** — this is how you'd build the ADK team's
equivalent in LangGraph on the integration. The Dispatch agent weighs both
assessments and decides whether to call the `request_human_approval` tool before
committing scarce fleet capacity.

### Two HITL implementations — default is the Temporal signal

The gate has two HITL implementations, chosen per-order via
`DispatchGateInput.use_interrupt` (wired from `config.INTERRUPT_MODE`, set by the
`HITL_MODE` env var, default `"temporal"`):

- **Temporal-signal (default, `HITL_MODE=temporal`):** the Dispatch agent's
  `request_human_approval` tool call surfaces an `escalate` flag + a brief; the
  LangGraph graph **ends there**, and the **workflow** performs the HITL. It
  signals the parent (`dispatch_gate_awaiting`), then parks on `wait_condition`
  for the human decision (arriving via the `approve` signal), with a timeout
  (`GATE_ESCALATION_SECONDS`, 3600s) that escalates to a `backup` approver tier.
  The human decision is a durable Temporal signal — **no LangGraph interrupt
  involved.** This is the version the talk leads with.
- **Interrupt (back-pocket toggle, `HITL_MODE=interrupt`):** a workflow-resident
  node calls LangGraph `interrupt(brief)` to park the workflow; resume via
  `Command(resume=...)`. Same durability, LangGraph's own mechanism.

On resume: approve → `_commit_assignment` (dispatch to the proposed driver);
reject → `_reject_order` (cancel the order, preserve fleet capacity). Gate
failures **fail open** — `_run_gate` commits the assignment with a warning rather
than losing the order.

### How the UI sees the pending approval

When the gate escalates, it signals the parent `dispatch_gate_awaiting`, which
stores the brief in the parent's `pending_dispatch` dict (keyed by order_id).
`GET /api/pending-dispatch` queries the **parent's** `get_status` and reads that
`pending_dispatch` dict — *not* the gate's query directly. (The gate also exposes
its own `pending_brief` query for inspection in the Temporal UI.)
`POST /api/approve-dispatch` signals the per-order `gate-<order_id>`
`DispatchGateWorkflow.approve`.

### The durability moment

Kill the worker while the approval card is up. The fleet freezes — but the
pending-approval state lives in **Temporal's event log, not the worker's memory**.
Restart the worker: the workflow replays from history, the gate is still parked on
its `wait_condition`, and the approval card is still there. Nothing was lost.

---

## How `TemporalModel` and `activity_tool` work (Pattern A's ADK side)

A common question: "where are the Temporal activities defined for each agent's
LLM call and tool call?" They aren't — they're injected automatically by two
wrappers:

- **`TemporalModel(DEFAULT_MODEL, activity_config=...)`** — set as an agent's
  model, every LLM call that agent makes runs as a Temporal `invoke_model`
  activity routed to the agents queue. You don't write the activity. ADK supports
  other providers too — swap `DEFAULT_MODEL`.
- **`activity_tool(tool_get_fleet_status, ...)`** — wrap a tool function this way
  and every call runs as a Temporal activity. Our local `_activity_tool.py` adds
  two fixes over upstream: correct multi-arg handling, and **graceful failure** —
  when an activity exhausts its retry policy, the error is returned as a string to
  the LLM instead of crashing the pipeline. (This is also how the dormant
  disconnect path degrades: Fleet Agent tools fail fast, the LLM sees the error,
  the Dispatch Agent assigns with available data, and the order is flagged
  `degraded`. The same path handles real Maps API failures gracefully.)

```python
fleet_agent = Agent(
    name="assignment_fleet_agent",
    model=TemporalModel(
        DEFAULT_MODEL,
        activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),  # route to agents worker
    ),
    tools=[_fleet_status_tool, _route_info_tool],
    ...
)

_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
```

No `@activity.defn` decorator, no explicit registration. **ADK composes and
sequences agents; Temporal makes every external call durable.** This is the
recommended pattern for `temporalio[google-adk]`.

---

## Communication patterns — what goes where, and why

The demo routes different kinds of data through different mechanisms. This is the
part new Temporal users most often get wrong — they put everything in signals or
workflow state and the event log blows up.

| Data flow | Mechanism | Why |
|-----------|-----------|-----|
| Driver position (updates every ~400ms during navigation) | Shared state (FleetState / SQLite) | High-frequency telemetry. No workflow decision depends on sub-second position. Routing it through signals would bloat the event log ~100× for no benefit. |
| Delivery completed | Child → parent signal (`order_delivered`) | Milestone event. Parent needs it for bookkeeping and as input to the next ADK assignment. |
| New order generated | Child → parent signal (`new_order`) | Milestone, low frequency. The assignment loop waits on it. |
| Order assignment | Parent → child signal (`add_order`) | Parent decides, child executes. The signal is the durable handoff. |
| Customer change (Pattern A) | External → parent → child signal chain | Preserves replay + audit. Every approval/rejection is in the event log. |
| Gate escalation (Pattern B) | Child → parent signal (`dispatch_gate_awaiting`) + human → gate signal (`approve`) | The human decision is a durable async signal; the brief flows up to the parent's `pending_dispatch` for the UI. |
| Driver snapshot for reasoning | Read from parent's in-memory workflow state | Pure workflow-local read — the parent already tracks the bookkeeping it decides on. |

**Temporal event log vs shared state — two different questions:**

| Temporal event log | Shared state (SQLite / FleetState) |
|---|---|
| Durable, append-only, replayable | Mutable, last-writer-wins, disposable |
| *"How did we get here?"* | *"Where are we now?"* |
| Source of truth for workflow replay | Source of truth for the UI's live view |

A production system pairs Temporal with Redis or Postgres for exactly this split;
in the demo, SQLite (`fleet_state.db`, WAL-backed, shared across processes) is the
toy stand-in.

**Driver position updates don't go through the event log at all.**
`navigate_to` heartbeats position to FleetState every ~400ms during a drive —
none of those are Temporal events. At production scale (GPS pings per second
across a 15-minute delivery) that's where the volume lives, against ~100 durable
orchestration events per order. Temporal carries the decisions; shared state
carries the telemetry.

---

## The 3-queue worker architecture

The demo runs three Temporal workers in a **separate worker process**
(`python -m agent_fleet.worker`), each on a dedicated task queue. The FastAPI
server runs in its own process.

| Queue | Worker | What it runs |
|---|---|---|
| `meltdown-workflows` | Workflows + minimal local activities | `MeltdownDemoWorkflow`, `DriverRouteWorkflow`, `OrderGenerationWorkflow`, `DispatchGateWorkflow`; `publish_agent_event` / `publish_agent_events_batch` (local activities); the Pattern B gate's node activities (Fleet/Customer/Dispatch Gemini calls, via `LangGraphPlugin`) |
| `meltdown-delivery` | Delivery | `generate_order`, `navigate_to`, `pickup_orders`, `deliver_order`, `execute_customer_change`, `get_route_polyline`, `get_fleet_status`, `get_order_priorities`, `set_driver_idle`, `set_warmup_hidden`, `sync_driver_position` (max 20 concurrent) |
| `meltdown-agents` | ADK/LLM activities | `register_assignment`, `tool_get_fleet_status`, `tool_get_order_priorities`, `tool_get_route_info`, plus the ADK `invoke_model` activity + `google_search` grounding (max 5 concurrent) |

**Why a workflows-only-ish worker?** Workflows must be deterministic and
replayable. Keeping them off the heavy activity queues makes it physically
impossible for workflow code to touch `FleetState` or do I/O.

**Why separate activity queues?** LLM calls are slow (3–5s each). Without queue
separation, a flood of assignment requests could starve navigation activities and
cause drivers to miss heartbeat timeouts. The agents queue caps at 5 concurrent;
delivery at 20.

**Plugin placement** (in `worker.py`):
- `GoogleAdkPlugin` is on **both** the workflow worker (sandbox passthroughs for
  `google.adk` / `google.genai`, deterministic runtime for replay) and the agents
  worker (hosts the `invoke_model` activity that calls Gemini for Pattern A).
- `LangGraphPlugin(graphs={GRAPH_NAME: build_gate_graph(use_interrupt=INTERRUPT_MODE)})`
  is on the **workflow** worker — it runs the Pattern B dispatch-gate graph, and
  its node activities execute there.

`TemporalModel` uses `ActivityConfig(task_queue=AGENTS_QUEUE)` to route Pattern
A's LLM calls from the workflow to the agents queue.

---

## Two processes

`run.sh` starts a worker process and a server process (plus the Temporal dev
server).

- **The worker process** owns all activities and workflow execution. It does
  **not** load `.env` itself — to start it by hand, pass the env
  file: `uv run --env-file .env python -m agent_fleet.worker`. The worker is
  live-only and requires `GOOGLE_API_KEY`; there is no mock mode.
- **The server process** runs no workers. Its WebSocket snapshot is built from
  **FleetState (SQLite)** — `_build_snapshot()` → `fleet.snapshot()` — not from
  Temporal queries. Activities (in the worker) write positions, statuses, and
  agent events to FleetState; the server reads them for the frontend. The server
  otherwise sends **signals** and runs **queries** only (e.g. `get_status` for
  `/api/pending-dispatch`). It has no GoogleAdkPlugin and no activity
  registration.

---

## What this would look like without Temporal

Without Temporal, the same orchestration would require:
- A state machine in a database (enum column per driver tracking route phase).
- Manual retry loops with custom backoff for every activity.
- A polling loop to implement "wait for human approval"
  (`while not db.get("approved"): sleep(1)`) — for *both* patterns.
- Defensive DB writes before every step so a crash doesn't lose position.
- Manual reconstruction of in-flight state on worker restart.
- A shared state store for cross-service coordination.
- Custom cancellation logic for mid-activity interruption.

Temporal collapses all of that into the workflow execution model. The event log
*is* the state persistence. `execute_activity` *is* the retry logic. Signals
*are* the message passing. `wait_condition` *is* the durable human pause. The
workflow code reads like a straightforward sequential program because Temporal
handles everything else — and the worker-kill demo proves it: kill the process
mid-approval, restart, and the pending state replays from history intact.
