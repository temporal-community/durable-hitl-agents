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
| Triggers on | Every order, while the **ADK tab** is active | Every order, while the **LangGraph tab** is active; the agent escalates only genuinely high-value ones |
| Agents involved | Fleet + Customer (parallel) → Dispatch (sequential) | Fleet ∥ Customer → Dispatch — a separate LangGraph team, each agent a real reason→act→eval ReAct loop |
| Where the human enters | the **workflow** (a boundary hold), not any agent tool | **inside the reasoning loop** — the agent calls an `ask_human` tool |
| Durable primitive | signal → `wait_condition` hold → resolve | `interrupt()` in the loop → `wait_condition` on the `answer_dispatch` signal → `Command(resume=answer)` |

**The key routing fact:** the **active UI tab** picks the dispatch framework for
*all* orders — `set_dispatch_mode("adk" | "langgraph")` sets `_dispatch_mode`, and
`_assign_order` routes on it.

- **ADK tab → ADK.** Every order runs `_run_adk_assignment()` inline in the parent
  workflow; the ADK multi-agent pipeline reasons about it and assigns it to the
  least-loaded driver. No gate.
- **LangGraph tab → inline LangGraph team.** Every order runs
  `_run_langgraph_assignment(order, driver_id, onum)` — the looping multi-agent
  LangGraph team runs *inline in the parent workflow*. There is **no per-order gate
  child**: when an agent decides it needs a human, it calls the `ask_human` tool
  mid-loop, whose execution is a durable LangGraph `interrupt()`; the parent surfaces
  the question and resolves it with the `answer_dispatch` signal.

This split is deliberate: each framework dispatches all orders while its tab is
active, on the same Temporal runtime, with the same durable-signal HITL primitive
underneath both.

The disconnect/recovery scenarios (agent disconnect, driver disconnect, tool
degradation) are **dormant code**, not demo features — the UI no longer surfaces
disconnect controls. The signals, retry logic, and `degraded` flag still exist
and are documented below as mechanism, not as something you'd show on stage.

---

## Terminology

This demo has two distinct actor types:

- **AI Agents** — these **reason**. They call LLMs, use tools, and make
  decisions. Pattern A's agents (Fleet, Customer, Dispatch) run inline in the
  workflow via ADK. Pattern B's agents (Fleet, Customer, Dispatch) are looping
  LangGraph ReAct nodes — each reason call and each tool call runs as its own
  Temporal activity.
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
order by the active tab's `_dispatch_mode`: **ADK tab → ADK inline**
(`_run_adk_assignment()`), or **LangGraph tab → inline LangGraph team**
(`_run_langgraph_assignment()`, which runs the looping team inline and drives any
in-loop `ask_human` interrupt with the `answer_dispatch` signal — no gate child). It builds
`DriverSnapshot`s from its own state, applies the capacity guardrail and
least-loaded balancing, and handles customer changes — including the human→agent
re-reason path (`human_revise_order` → `_reassign_via_adk`). It never does delivery
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
timer and signals the parent with each new order. The first
`WARMUP_BURST_ORDERS` = 5 orders fire in a quick burst (`WARMUP_BURST_SECONDS` =
2s apart) to get multiple drivers on the road, then it settles into a normal
cadence (±30% jitter around a 12s base — `ORDER_INTERVAL_SECONDS`, min 5s).
Auto-generated orders top out around $1,950 (servings ≤150 × ≤$13), so the agent
never escalates them — only the deliberately injected premium order does.

The workflows connect through signals in both directions:
- **Parent → child:** `add_order`, `update_pending` (HITL hold), `resolve_update`
  (HITL decision), `cancel_order`, plus dormant `driver_disconnected` /
  `driver_reconnected`.
- **Child → parent:** `order_delivered` (driver state), `new_order` (from the
  order generator).
- **External → parent (Pattern B):** `answer_dispatch(order_id, decision)` — a human's
  answer to an agent's in-loop `ask_human`, which resumes the suspended LangGraph
  team. (No child workflow involved — the parent runs the team inline.)
- **External → parent (Pattern A in-loop):** `human_revise_order` — a human revision
  that triggers the ADK assignment agent to re-reason (`_reassign_via_adk`).

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

### Variant — human → agent, in the *reasoning* loop (ADK)

The change above gates the *delivery* loop (a boundary hold): the system applies a
fixed decision the human picks. A second ADK flow puts the human **inside the
agent's reasoning loop** instead: an operator revises an order (new location /
details) and the ADK assignment agent **re-reasons** how to adjust — re-checking the
fleet and re-deciding the driver — rather than applying a fixed change. The human's
edit is input the agent reasons over; the agent decides the response.

The flow:
1. `POST /api/revise-order` → signals the parent `human_revise_order`, which appends
   the revision to `self._pending_revisions`.
2. `_process_human_revisions` (a parent task that parks on `wait_condition`) drains
   each revision through `_reassign_via_adk`.
3. `_reassign_via_adk` applies the revision to the order record, then **re-runs the
   full ADK assignment team** (`_run_adk_assignment` — Fleet → Customer → Dispatch)
   over the changed order. It then commits the agent's fresh decision: reassign to a
   better driver (cancel on the old, `add_order` on the new), keep the same driver but
   push the updated destination into its delivery loop (`update_order`), or assign a
   fresh choice if the order isn't on a driver yet.

This is the same durable primitive as the boundary hold (signal + `wait_condition`),
but the human's edit becomes input the agent reasons over — not a fixed change the
system applies. The existing customer-change delivery-hold (above) is **unchanged**
and stays; this is an additional flow.

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

## Pattern B — Agent-in-the-loop (LangGraph, the human is a tool)

**The agent calls the human.** While the LangGraph tab is active, every order runs
a **looping multi-agent LangGraph team** that mirrors the ADK side — *inline in the
parent workflow*, not in a child. The HITL is **inside the reasoning loop**: an agent
that hits a decision it shouldn't make alone calls the `ask_human` tool mid-reasoning,
whose execution is a durable LangGraph `interrupt()`. There is **no per-order gate
child** — the parent drives the interrupt with a Temporal signal.

### Routing

In `MeltdownDemoWorkflow._assign_order`, the LangGraph branch runs the team as a
concurrent asyncio task (still appended to `self._gate_tasks`, the existing task list)
so the order loop and fleet keep moving while the agents — and possibly a human —
deliberate:

```python
if self._dispatch_mode == "langgraph":
    self._gate_tasks.append(
        asyncio.create_task(
            self._run_langgraph_assignment(order, self._least_loaded_driver(), onum)
        )
    )
    return
```

`_run_langgraph_assignment` compiles `graph(GRAPH_NAME)` (`GRAPH_NAME = "dispatch_team"`)
and `ainvoke`s it in-workflow — the Fleet ∥ Customer → Dispatch reason calls **and each
tool call** execute as Temporal activities recorded in the **parent's** history. Whether
to ask a human is the **agent's** judgment (guided by `ESCALATION_GUIDANCE` and the
per-agent system prompts in `langgraph_agents.py`), not a code threshold. Auto-generated
orders top out around $1,950, so the agents dispatch them directly; only the deliberately
injected premium Moscone order (`POST /api/inject-order`) is exceptional enough that an
agent calls `ask_human`.

### The looping multi-agent team graph

`build_dispatch_team_graph` (in `langgraph_agents.py`) compiles a LangGraph graph via
`temporalio.contrib.langgraph`. It mirrors the ADK team — Fleet and Customer fan out from
START in parallel, then converge on Dispatch — but each agent is a **real reason → act →
eval ReAct loop**, not a single `ainvoke`:

```
START → fleet_reason    ──┐  (loops: reason → {ask_human | run tools | done})
START → customer_reason ──┴→ dispatch_reason → (END | ask_human → reason)
```

Each `*_reason` node is a **real Gemini call** (through `init_chat_model`,
provider-swappable via `MODEL_PROVIDER`) executed as a **Temporal activity**. Each tool
call the model asks for runs in the `*_act` node as **its own Temporal activity** (via
`workflow.execute_activity`, on the agents queue with its own retry policy) — mirroring
ADK's `activity_tool` granularity. The tools are the same ones the ADK team uses:
`get_fleet_status`, `get_route_info`, `get_order_priorities`. Fleet and Dispatch also bind
the `ask_human` tool; Customer does not.

### The human is a tool — `ask_human` and the durable interrupt

When an agent decides it needs a human, it calls the `ask_human(question)` tool. The
tool body never runs (`raise NotImplementedError`); the graph's `route` function sees the
`ask_human` tool call and routes to a `*_human` node (`fleet_human` / `dispatch_human`)
whose body is the tool's real "execution":

```python
async def _human_node(messages, agent_label, state):
    answer = interrupt({"question": ..., "order_id": state["order_id"], ...})  # ⏸ suspend
    return [ToolMessage(content=str(answer), ...)]   # answer flows back as the observation
```

`interrupt()` suspends the graph durably; the answer becomes the `ToolMessage` the agent
observes on its **next reason turn**. So the human's answer is an in-loop observation, not
a boundary decision the system applies.

### The parent drives the interrupt with a durable signal

`_run_langgraph_assignment` loops on the graph's `__interrupt__` marker. On each interrupt
it surfaces the question into `self._pending_dispatch[order_id]`, marks the order
`awaiting_dispatch_approval`, publishes an agent event, then parks on
`_await_dispatch_answer` (a `wait_condition` on the `answer_dispatch` signal). The human's
answer resumes the graph via `Command(resume=answer)`:

```python
result = await compiled.ainvoke(state, config=config)
while result.get("__interrupt__"):
    self._pending_dispatch[order.order_id] = result["__interrupt__"][0].value
    answer = await self._await_dispatch_answer(order.order_id)   # ⏸ wait_condition on answer_dispatch
    if answer is None:                                           # demo shutting down — exit cleanly
        return
    result = await compiled.ainvoke(Command(resume=answer), config=config)  # resume the agent
```

```python
@workflow.signal
async def answer_dispatch(self, order_id: str, decision: str):  # the human responds → resolves it
    self._dispatch_answers[order_id] = decision
```

`_await_dispatch_answer` returns `None` if the demo shuts down (`self._routes_done`) while
parked, so the team task exits cleanly instead of hanging the parent's teardown. Once the
team finishes, the workflow uses the human's answer directly (a `rejected` flag) rather
than trusting the graph's free-text `dispatch_decision` — Gemini sometimes returns an empty
final turn. `reject` (or a `HOLD` decision) → `_reject_order` (cancel, preserve fleet
capacity); otherwise → `_commit_assignment` to the proposed driver.

### How the UI sees the pending approval

When an agent calls `ask_human`, the parent stores the interrupt payload (the question +
order context) in its `pending_dispatch` dict (keyed by order_id).
`GET /api/pending-dispatch` queries the parent's `get_status` and reads that
`pending_dispatch` dict. `POST /api/approve-dispatch` signals the parent's
`answer_dispatch(order_id, decision)` directly — no per-order gate child is involved.

### The durability moment

Kill the worker while the approval card is up. The fleet freezes — but the
pending-approval state lives in **Temporal's event log, not the worker's memory**.
Restart the worker: the workflow replays from history, the graph is still suspended on
its `interrupt()` and the parent is still parked on the `answer_dispatch`
`wait_condition`, and the approval card is still there. Nothing was lost.

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
| Agent asks a human (Pattern B) | In-loop LangGraph `interrupt()` + human → parent signal (`answer_dispatch`) | The agent calls `ask_human` mid-loop; the interrupt suspends the graph, the question flows into the parent's `pending_dispatch` for the UI, and the human's decision returns as a durable async signal. |
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
| `meltdown-workflows` | Workflows + minimal local activities | `MeltdownDemoWorkflow`, `DriverRouteWorkflow`, `OrderGenerationWorkflow`; `publish_agent_event` / `publish_agent_events_batch` (local activities); the Pattern B node activities (Fleet/Customer/Dispatch Gemini reason calls **and each tool call**, via `LangGraphPlugin`) — these run for the team graph **inline in `MeltdownDemoWorkflow`**. `LangGraphPlugin` registers exactly **one** graph: `GRAPH_NAME = "dispatch_team"` (the looping multi-agent team with the in-loop `ask_human` tool). |
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
- `LangGraphPlugin(graphs={...})` is on the **workflow** worker, registering exactly one
  graph — `GRAPH_NAME: build_dispatch_team_graph()` (the looping multi-agent team). It
  runs the Pattern B team inline in `MeltdownDemoWorkflow`, and the team's node activities
  (each agent's reason call and each tool call) execute there.

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
