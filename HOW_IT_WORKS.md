## Terminology

This demo has two distinct actor types:
- **AI Agents** (Fleet Agent, Customer Agent, Dispatch Agent) — these **reason**. They call LLMs, use tools, and make decisions about order assignment. Each runs inline in the workflow via ADK.
- **Delivery actors** (Driver-A through Driver-E) — these **execute**. They receive orders via signals and batch-pickup at Ziggy's, then deliver sequentially to multiple hotels before returning. Each runs in its own child workflow (`DriverRouteWorkflow`). They don't reason — they carry out the agents' decisions.

In Temporal terms, the delivery actors are **child workflows**. They are not Temporal workers (infrastructure) and not AI agents (reasoning).

---

**What each agent specifically reasons about:**

| Agent | What it evaluates | Tools it calls |
|-------|-------------------|----------------|
| **Fleet Agent** | Delivery actor positions, free capacity slots, driving ETAs to destination, disconnect status | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps Directions) |
| **Customer Agent** | VIP vs standard priority, deadline tightness, hotel events (conferences, galas, pool parties), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Dispatch Agent** | Synthesizes both assessments, compensates if either agent is offline, picks final delivery actor and submits structured assignment | `tool_submit_assignment` |

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as code — workflows and activities — and Temporal guarantees it runs to completion even if the service crashes, times out, or gets disconnected. Every step is recorded in an event log. If the worker dies mid-execution, Temporal replays the history, and your code resumes exactly where it left off."

**Key points to land:**
- Workflows are durable — crashes don't lose state
- Activities are retryable by default — transient failures self-heal
- Signals let you inject events into a running workflow (driver disconnect, agent disconnect, customer change)
- The Temporal UI shows the full event history for every workflow run — nothing is a black box

---

## The Integration: Why It Matters

> "Here's the key insight: ADK agents run **inline in the Temporal workflow**, not inside an activity. That's a deliberate design choice. Every LLM call that an agent makes becomes its own Temporal activity via `TemporalModel`. Every tool call (Maps, search, fleet status) is also its own Temporal activity via `activity_tool`. If the worker crashes mid-reasoning — say the Fleet Agent already called Gemini but the Customer Agent hasn't yet — Temporal replays the Fleet Agent's result from the event log and only retries what was interrupted. No re-calling Gemini for steps that already completed. No lost context."

This is the "aha" moment. The alternative — running ADK inside a single activity — would make the entire agent pipeline one retry unit. If it fails halfway through, everything restarts from scratch. The inline pattern gives you **per-call durability**: each LLM call and each tool call is independently durable, retryable, and visible in the Temporal UI. This is what `TemporalModel` and `activity_tool` from the `temporalio[google-adk]` package were built for.

Return to this when showing the Temporal UI event history — each `invoke_model` and tool call has a summary showing which agent is acting.

---

## Deeper Background (for technical questions)

This section is for the presenter — not for reading aloud. It gives you the architectural grounding to answer "how does that actually work?" questions confidently.

### How Temporal replay works

Temporal records every activity result in an append-only event log on the Temporal server. When the worker crashes and restarts, it re-executes your workflow function from the top — but when it hits an `execute_activity()` call that already completed, Temporal intercepts it and returns the cached result from the log instead of running the activity again. The workflow code runs again; the side effects don't.

This means workflow code has one strict rule: **it must be deterministic**. No real I/O, no `random`, no `datetime.now()`, no `asyncio.sleep()` directly. Temporal provides sandboxed equivalents for all of these:

| Don't use in a workflow | Use instead |
|------------------------|-------------|
| `logging.info()` | `workflow.logger.info()` — suppressed during replay to avoid duplicate logs |
| `asyncio.sleep()` | `workflow.sleep()` — returns immediately if already completed in history |
| `datetime.now()` | `workflow.now()` — returns the deterministic time from the event log |
| Any real I/O | `workflow.execute_activity()` — returns cached result during replay |

If you break determinism, Temporal raises a non-determinism error on replay. This is a feature — it catches bugs that would otherwise silently corrupt workflow state.

### Why three workflow classes?

**`MeltdownDemoWorkflow`** is the brain. It owns the fleet state — driver positions, order assignments, disconnect/reconnect status. It runs assignment agents (ADK inline via `_run_adk_assignment()` in live mode), builds `DriverSnapshot`s from its own state and passes them to activities as inputs, and handles customer changes. It never does delivery work directly — it delegates to child workflows.

**`DriverRouteWorkflow`** is the legs. One instance per driver, it batches pending orders: navigate to Ziggy's → batch-pickup all orders → deliver sequentially (hotel A → hotel B → ...) → signal parent after each delivery → return to base → loop. It owns its own disconnect state (status, is_disconnected, is_recovering, path_history, current_orders). Disconnect uses **Temporal-native retry**: activities check FleetState for disconnect status and fail if disconnected. Temporal retries with backoff (`NAV_RETRY`, unlimited attempts). The driver finishes its current delivery, stays at the hotel (can't report back), and resumes when reconnected — a `sync_driver_position` activity reads the actual position from FleetState so the workflow never teleports. No workflow-side cancellation needed.

**`OrderGenerationWorkflow`** is a child workflow that generates orders on a timer and signals the parent with each new order. The first 3 orders fire in a quick burst (2s apart) to get multiple drivers on the road immediately, then settles into a normal cadence (±30% jitter around 10s base). The parent handles assignment.

The workflows connect through signals in both directions:
- **Parent → child:** `add_order` (new delivery), `driver_disconnected` / `driver_reconnected`, `update_order` (address change), `cancel_order` (cancellation)
- **Child → parent:** `order_delivered` (updates parent's driver state — position and order count)

```
OrderGenerationWorkflow fires on timer
  → signals parent MeltdownDemoWorkflow with new order
  → MeltdownDemoWorkflow builds DriverSnapshots from workflow state
  → runs ADK inline (_run_adk_assignment) → "give this to Driver-B"
  → updates self._driver_orders, sends add_order signal to DriverRouteWorkflow
  → DriverRouteWorkflow executes the delivery
  → on completion, signals parent with order_delivered
```

The key design principles:
- **Child workflows give you fault isolation.** Each delivery actor runs independently. If Driver-A hits an error, the others keep running.
- **Workflows own state, activities are pure.** Activities receive everything they need as inputs — they never read shared state for decision-making. The server queries workflows directly for the frontend.
- **Disconnect uses Temporal retry.** Activities check FleetState for disconnect (simulates network unreachability), fail, and Temporal retries with backoff. The delivery actor finishes its delivery but can't report back. On reconnect, a `sync_driver_position` activity reads the actual position from FleetState so the workflow resumes from where the driver actually is — no teleporting. Completed deliveries are not repeated; the batch continues from the next pending order.

### Where the ADK agents fit

In **live mode**, the agents run **inline in the workflow** via `_run_adk_assignment()` in `MeltdownDemoWorkflow`. The workflow builds `DriverSnapshot`s from its own state and passes them to the ADK pipeline. Each LLM call and tool call becomes a Temporal activity via `TemporalModel` and `activity_tool` — the workflow code never calls an explicit `reason_about_assignment` activity. If an activity fails, Temporal retries. There is no fallback to mock.

In **mock mode**, `mock_reason_about_assignment` in `agent_fleet/mock/activities.py` is registered as a single activity with `@activity.defn(name="reason_about_assignment")`. The workflow calls this activity instead of running ADK inline. The live workflow code has zero awareness of this — the mock worker registers the activity with the same name.

Fleet Agent, Customer Agent, and Dispatch Agent are all **LLM Agents** — each is an `Agent` with `model=TemporalModel(DEFAULT_MODEL, activity_config=ActivityConfig(task_queue=AGENTS_QUEUE))`, meaning every Gemini call they make becomes an `invoke_model` Temporal activity routed to the agents worker. The `create_order_assignment_agent()` function returns an **Orchestrator Agent** (`SequentialAgent`) — it has no model, makes no LLM calls, and has no corresponding Temporal activity. It purely sequences the sub-agents.

The full agent pipeline is composed in [`agent_fleet/agents.py`](agent_fleet/agents.py) using ADK's `ParallelAgent` and `SequentialAgent`:

```python
def create_order_assignment_agent() -> SequentialAgent:
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),    # checks positions, capacity, ETAs
            create_assignment_customer_agent(), # checks priority, deadline, hotel context
        ],
    )
    dispatch_agent = create_assignment_dispatch_agent()  # synthesizes → calls tool_submit_assignment

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, dispatch_agent],
    )
```

Fleet Agent and Customer Agent run in parallel (ADK handles that). Then the Dispatch Agent runs sequentially after both complete. In live mode, the workflow runs ADK inline via `_run_adk_assignment()`. In mock mode, the workflow calls the `reason_about_assignment` activity (which the mock worker registers).

### How `TemporalModel` and `activity_tool` work — and why you don't define agent activities explicitly

A common question from engineers: "where are the Temporal activities defined for each agent's LLM call and tool call?" The answer is they aren't — they're injected automatically by two wrappers:

- **`TemporalModel(DEFAULT_MODEL, activity_config=...)`** — when you set this as an agent's model, every LLM call that agent makes is automatically executed as a Temporal `invoke_model` activity routed to the agents queue. You don't write the activity. The wrapper does it. ADK supports other models too — swap `DEFAULT_MODEL` for any supported provider.
- **`activity_tool(tool_get_fleet_status, ...)`** — when you wrap a tool function this way, every time an agent calls that tool it executes as a Temporal activity. Again, no explicit activity definition needed. Our local [`_activity_tool.py`](agent_fleet/_activity_tool.py) adds two fixes over the upstream version: correct multi-arg handling, and **graceful failure** — when an activity exhausts its retry policy, the error is returned as a string to the LLM instead of crashing the pipeline. This is how Fleet Agent disconnect works: tools fail fast (2 retries), the LLM sees the error, and the Dispatch Agent assigns without fleet data. The same graceful degradation applies to real API failures — `tool_get_route_info` calls the Google Maps Directions API for driving ETAs, which can fail from rate limiting, quota exhaustion, or transient network errors. These are not bugs — the error flows back to the LLM as context, the Fleet Agent notes the missing ETA, and the Dispatch Agent compensates. Orders assigned during these failures are flagged as `degraded` in the UI.

So when Fleet Agent calls Gemini and then calls `tool_get_fleet_status`, both of those are Temporal activities — durable, retryable, and recorded in the event log — purely by inheritance from the wrappers. This is the `temporalio[google-adk]` integration doing its job.

Here's exactly what this looks like in [`agent_fleet/agents.py`](agent_fleet/agents.py):

```python
# Each agent gets TemporalModel — LLM calls become invoke_model activities automatically
fleet_agent = Agent(
    name="assignment_fleet_agent",
    model=TemporalModel(
        DEFAULT_MODEL,
        activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),  # route to agents worker
    ),
    tools=[_fleet_status_tool, _route_info_tool],
    ...
)

# Each tool is wrapped with activity_tool — tool calls become Temporal activities automatically
_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
```

No activity `@activity.defn` decorator, no explicit registration — the wrappers handle it.

**The division of responsibility:**

ADK owns the agent orchestration — the sequencing of Fleet → Customer → Dispatch Agent via `SequentialAgent` and `ParallelAgent`, the multi-turn reasoning loop, passing context between agents. Temporal owns the durability of every external call those agents make.

An alternative "more Temporal-native" design would be to put the Fleet → Customer → Dispatch Agent sequencing directly in the workflow and only push the raw LLM calls into activities. That gives you more explicit visibility in the Temporal UI — each agent step shows up as a named workflow step. The tradeoff is you'd be rewriting ADK's orchestration in Temporal workflow code, giving up ADK's agent composition primitives.

The current design keeps both frameworks doing what they're best at: **ADK composes and sequences agents, Temporal makes every external call durable.** This is the recommended pattern for the `temporalio[google-adk]` integration — `TemporalModel` and `activity_tool` exist specifically to enable ADK agents running inline in workflows with per-call durability.

### The 3-queue worker architecture

The demo runs three Temporal workers in a **separate worker process** (`python -m agent_fleet.worker`), each on a dedicated task queue. The FastAPI server runs in its own process — it queries Temporal workflows for state and sends signals only.

| Queue | Worker | What it runs |
|---|---|---|
| `meltdown-workflows` | Workflows only | `MeltdownDemoWorkflow`, `DriverRouteWorkflow`, `OrderGenerationWorkflow` — no activities, dedicated to replay |
| `meltdown-delivery` | Delivery | `navigate_to`, `pickup_orders`, `deliver_order`, `generate_order`, `execute_customer_change`, `get_route_polyline`, `get_fleet_status`, `get_order_priorities`, `publish_agent_event`, `sync_driver_position` |
| `meltdown-agents` | Agents | `register_assignment`, `tool_get_fleet_status`, `tool_get_order_priorities`, `tool_get_route_info` + `google_search` (Gemini grounding) |

**Why a workflows-only worker?** Workflows must be deterministic and replayable. Keeping them on a dedicated worker with no activities makes it physically impossible for workflow code to touch `FleetState` (SQLite WAL-backed, `fleet_state.db`) or do I/O. This is the Temporal-idiomatic pattern for production deployments.

**Why separate activity queues?** LLM calls are slow — a single Gemini call can take 3–5 seconds. Without queue separation, a flood of assignment requests could fill all worker slots and starve navigation activities, causing drivers to miss heartbeat timeouts. The agents queue is rate-limited to 5 concurrent activities; the delivery queue runs 20.

**Why separate processes?** The server does not run workers. It queries Temporal workflows directly via `_build_snapshot_from_queries()` — calling `MeltdownDemoWorkflow.get_status` and `DriverRouteWorkflow.get_status` for every WebSocket push. This means the server process has no FleetState dependency, no activity registration, and no GoogleAdkPlugin. All decision data and UI state lives in workflows, accessible via Temporal queries. The worker process owns all activities and workflow execution.

The three workers are set up in [`agent_fleet/worker.py`](agent_fleet/worker.py):

```python
def create_workflow_worker(client: Client) -> Worker:
    """Workflow-only worker — no activities, dedicated to replay."""
    return Worker(client, task_queue=WORKFLOWS_QUEUE,
                  workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow,
                             OrderGenerationWorkflow],
                  plugins=[GoogleAdkPlugin()])  # sandbox + determinism for replay

def create_agents_worker(client: Client) -> Worker:
    """ADK/LLM activities — rate-limited."""
    return Worker(client, task_queue=AGENTS_QUEUE,
                  activities=[register_assignment, tool_get_fleet_status,
                              tool_get_order_priorities, tool_get_route_info],
                  max_concurrent_activities=5,
                  plugins=[GoogleAdkPlugin()])  # invoke_model activity registration
```

`GoogleAdkPlugin` is registered on **both** the workflow worker and the agents worker. The workflow worker needs it for sandbox passthroughs (`google.adk`, `google.genai`) and deterministic runtime (`uuid`, `time`) during replay. The agents worker needs it because it hosts the `invoke_model` activity that actually calls Gemini. `TemporalModel` uses `ActivityConfig(task_queue=AGENTS_QUEUE)` to route LLM calls from the workflow to the agents queue.

### Mock mode — completely separate

Mock mode lives in `agent_fleet/mock/` — its own `activities.py` and `worker.py`. The decision happens once at startup in `agent_fleet/worker.py`: if `GOOGLE_API_KEY` is set, live workers run; if not, mock workers from `agent_fleet/mock/worker.py` run instead. There is no `MOCK_MODE` flag in config, no `_get_api_activities()`, no per-key selection, and no inline try/except fallbacks in live code.

Mock activities use `@activity.defn(name=...)` overrides to match live activity names (e.g., `mock_get_route_polyline` is registered as `"get_route_polyline"`, `mock_reason_about_assignment` as `"reason_about_assignment"`). Workflows don't know or care which version is running. The mock worker also skips `GoogleAdkPlugin` since there are no LLM calls.

This matters for the demo narrative: real activities let failures propagate to Temporal's retry mechanism. If the Google Maps API returns an error, it shows up as a failed activity in the Temporal UI — retried with unlimited attempts and exponential backoff until the issue resolves, exactly as it would in production. Mock mode is an explicit configuration choice, not a hidden fallback that masks failures.

### What this would look like without Temporal

Without Temporal, the same orchestration would require:
- A state machine in a database (enum column per driver tracking route phase)
- Manual retry loops with custom backoff for every activity
- A polling loop to implement "wait for human approval" (`while not db.get("approved"): sleep(1)`)
- Defensive DB writes before every step so a crash doesn't lose position
- Manual reconstruction of in-flight state on worker restart
- A shared state store (Redis, Postgres) for cross-service coordination
- Custom cancellation logic for mid-activity interruption

Temporal collapses all of that into the workflow execution model. The event log *is* the state persistence. `execute_activity` *is* the retry logic. Signals *are* the message passing. Cancellation scopes *are* the interrupt mechanism. The workflow code reads like a straightforward sequential program because Temporal handles everything else.

In this demo, the workflows are the source of truth for all operational state — driver positions, order assignments, disconnect status. Activities receive this state as inputs and return results. The server queries workflows directly for the frontend WebSocket via `_build_snapshot_from_queries()` — no intermediate FleetState needed. If the worker process restarts, Temporal replays the workflows, activities re-execute, and the server's queries return fresh state.

### Live mode execution pipeline (end-to-end)

This traces a single order from button click to delivery — every function and file in sequence.

**1. Browser → Server** — User clicks "Start Deliveries"
- [`server.py`](agent_fleet/server.py) `start_demo()` → starts `MeltdownDemoWorkflow` on `WORKFLOWS_QUEUE`

**2. Main workflow initializes**
- [`workflows.py`](agent_fleet/workflows.py) `MeltdownDemoWorkflow.run()` → starts 5 `DriverRouteWorkflow` children + `OrderGenerationWorkflow` child

**3. Order generates on timer**
- [`workflows.py`](agent_fleet/workflows.py) `OrderGenerationWorkflow.run()` → calls `generate_order` activity every 10s → signals parent with `new_order`
- [`activities.py`](agent_fleet/activities.py) `generate_order()` → picks random venue, registers in FleetState

**4. Parent runs ADK agents inline**
- [`workflows.py`](agent_fleet/workflows.py) `_assign_order()` → builds `DriverSnapshot`s from workflow state → calls `_run_adk_assignment()`
- `_run_adk_assignment()` → creates ADK `Runner`, calls `runner.run_async()` — agents execute **inline in the workflow**

**5. ADK agent pipeline**
- [`agents.py`](agent_fleet/agents.py) `create_order_assignment_agent()` → `SequentialAgent`:
  - `ParallelAgent` runs **Fleet Agent** + **Customer Agent** simultaneously
  - Then **Dispatch Agent** runs sequentially
- Each agent uses `TemporalModel` → every Gemini call becomes an `invoke_model` activity on `AGENTS_QUEUE`

**6. Tool calls → Temporal activities**
- [`agents.py`](agent_fleet/agents.py) — tools wrapped via `activity_tool()` from [`_activity_tool.py`](agent_fleet/_activity_tool.py)
- Fleet Agent calls: `tool_get_fleet_status`, `tool_get_route_info` (Google Maps)
- Customer Agent calls: `tool_get_order_priorities`, `google_search` (Gemini grounding)
- Each tool call → `workflow.execute_activity()` → recorded in Temporal event log

**7. Dispatch Agent decides**
- [`agents.py`](agent_fleet/agents.py) `tool_submit_assignment()` → writes `{driver_id, reasoning_summary}` to ADK session state (in-memory, not a Temporal activity)

**8. Result flows back**
- [`workflows.py`](agent_fleet/workflows.py) `_run_adk_assignment()` → reads `session.state["assignment"]` → returns `ReasonAboutAssignmentOutput`

**9. Assignment registered, delivery actor signaled**
- [`workflows.py`](agent_fleet/workflows.py) `_assign_order()` → checks if chosen driver has capacity (max 3 orders) and isn't disconnected — reassigns to next available driver if not → calls `register_assignment` activity (FleetState write, marks `degraded` if Fleet Agent was offline) → signals chosen `DriverRouteWorkflow` with `add_order`

**10. Delivery actor executes (batch pickup → sequential delivery)**
- [`workflows.py`](agent_fleet/workflows.py) `DriverRouteWorkflow.run()`:
  - Collects all pending orders into a batch
  - If reconnecting: `sync_driver_position` activity reads actual position from FleetState
  - Drives to Ziggy's (skipped if already there)
  - `pickup_orders` activity → batch-picks all orders at once
  - For each order in the batch:
    - `get_route_polyline` activity → Google Maps polyline to hotel
    - `navigate_to` activity → drives to hotel with heartbeats (0.4s/step)
    - (if `_reroute_pending` flag set by `update_order` signal → re-navigates to new destination)
    - `deliver_order` activity → marks delivered (skipped if `_cancel_pending`)
    - Signals parent with `order_delivered` after each delivery
- [`activities.py`](agent_fleet/activities.py) — all activities on `DELIVERY_QUEUE`

**11. Delivery actor returns to base**
- [`workflows.py`](agent_fleet/workflows.py) → after all orders in batch delivered, delivery actor navigates back to Ziggy's (visible on map) → idle, waits for next batch
- On disconnect mid-batch: driver finishes current delivery, stays at hotel, Temporal retries. On reconnect, resumes from next order in batch — completed deliveries are not repeated

**Key difference in live vs mock:** In live mode, ADK runs inline in the workflow — every LLM call and tool call is a separate Temporal activity visible in the event log. In mock mode, the entire reasoning is a single `reason_about_assignment` activity.


