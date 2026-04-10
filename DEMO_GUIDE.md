# Meltdown Demo Delivery Guide

This guide is for anyone presenting the Meltdown demo. It covers setup, the one-minute pitch on each technology, and step-by-step scripts for each demo scenario (~2–5 min each).

---

## Before You Start

**Requirements:**
- `.env` with two API keys (Google requires separate keys for Gemini vs Cloud APIs):
  - `GOOGLE_API_KEY` — Gemini key, restricted to Generative Language API. Without it, the demo runs in mock mode.
  - `GOOGLE_MAPS_API_KEY` — Maps key, restricted to Directions API.
- `./run.sh` (or `make run`) started — this starts the Temporal dev server, worker process, and server process automatically. Browser open at http://localhost:8080
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history)

**Terminology note:** This demo has two distinct actor types:
- **AI Agents** (Fleet Agent, Customer Agent, Resolver) — these **reason**. They call LLMs, use tools, and make decisions about order assignment. Each runs inline in the workflow via ADK.
- **Delivery actors** (Driver 1, 2, 3) — these **execute**. They receive orders via signals and follow a fixed route: pickup → navigate → deliver → return. Each runs in its own child workflow (`DriverRouteWorkflow`). They don't reason — they carry out the agents' decisions.

In Temporal terms, the delivery actors are **child workflows**. They are not Temporal workers (infrastructure) and not AI agents (reasoning).

**Pre-flight check:**
- Map shows 3 hotels (MGM Grand, Caesars, Mandalay Bay) and Frosty's Ice Cream shop
- All 3 delivery actors are at the ice cream shop, status idle
- "Start Deliveries" button is active
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each scenario before presenting to get familiar with the agent reasoning panel timing.

---

## The One-Minute Pitch

Use this framing at the start of the talk before any demo:

> "AI agents are increasingly being used to automate complex decisions — but in production, they break. The worker crashes. A tool call times out. The LLM call returns mid-reasoning and the state is gone. What we're showing today is what happens when you combine Google ADK — a framework for composing multi-agent AI — with Temporal — a durable execution engine — so that every agent action is retryable, replayable, and recoverable."

---

## What is Google ADK?

**The 30-second version:**

> "Google ADK is an open-source framework for building multi-agent AI systems. You compose agents — each with their own tools and model — into pipelines: run them sequentially, in parallel, or nested. In this demo, a Fleet Agent assesses delivery actor positions and capacity, a Customer Agent evaluates order priority and hotel context, and a Resolver Agent synthesizes their output into a delivery assignment."

**Key points to land:**
- ADK has two agent types: **LLM Agents** (`Agent` with a model) call Gemini to reason and use tools; **Orchestrator Agents** (`SequentialAgent`, `ParallelAgent`) coordinate sub-agents without calling an LLM themselves
- In this demo: Fleet Agent, Customer Agent, and Resolver are all LLM Agents — each calls Gemini. The outer pipeline (`create_order_assignment_agent`) is an Orchestrator Agent — it sequences them with no LLM of its own
- Each agent can use tools (Maps, Search, custom functions)
- ADK supports multiple model providers — this demo uses Gemini, but you can swap to other models by changing the config
- ADK manages the multi-turn reasoning loop — the developer just defines the agents and wires them together

**What each agent specifically reasons about:**

| Agent | What it evaluates | Tools it calls |
|-------|-------------------|----------------|
| **Fleet Agent** | Delivery actor positions, free capacity slots, driving ETAs to destination, disconnect status | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps Directions) |
| **Customer Agent** | VIP vs standard priority, deadline tightness, hotel events (conferences, galas, pool parties), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) |
| **Resolver** | Synthesizes both assessments, compensates if either agent is offline, picks final delivery actor and submits structured assignment | `tool_submit_assignment` |

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as code — workflows and activities — and Temporal guarantees it runs to completion even if the service crashes, times out, or gets disconnected. Every step is recorded in an event log. If the worker dies mid-execution, Temporal replays the history deterministically, and your code resumes exactly where it left off."

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

**`DriverRouteWorkflow`** is the legs. One instance per driver, it executes the physical route: navigate to kitchen → pick up → navigate to hotel → deliver → signal parent → return to base → loop. It owns its own disconnect state (status, is_disconnected, is_recovering, path_history, current_orders). Disconnect uses **Temporal-native retry**: activities check FleetState for disconnect status and fail if disconnected. Temporal retries with backoff (`NAV_RETRY`, unlimited attempts). The driver finishes its current delivery, stays at the hotel (can't report back), and resumes when reconnected. No workflow-side cancellation needed.

**`OrderGenerationWorkflow`** is a child workflow that generates orders on a randomized timer (±30% jitter around 15s base) and signals the parent with each new order. The parent handles assignment.

The workflows connect through signals in both directions:
- **Parent → child:** `add_order` (new delivery), `driver_disconnected` / `driver_reconnected`, `update_order` (address change), `cancel_order` (cancellation)
- **Child → parent:** `order_delivered` (updates parent's driver state — position and order count)

```
OrderGenerationWorkflow fires on timer
  → signals parent MeltdownDemoWorkflow with new order
  → MeltdownDemoWorkflow builds DriverSnapshots from workflow state
  → runs ADK inline (_run_adk_assignment) → "give this to Driver 2"
  → updates self._driver_orders, sends add_order signal to DriverRouteWorkflow
  → DriverRouteWorkflow executes the delivery
  → on completion, signals parent with order_delivered
```

The key design principles:
- **Child workflows give you fault isolation.** Each delivery actor runs independently. If Driver 1 hits an error, 2 and 3 keep running.
- **Workflows own state, activities are pure.** Activities receive everything they need as inputs — they never read shared state for decision-making. The server queries workflows directly for the frontend.
- **Disconnect uses Temporal retry.** Activities check FleetState for disconnect (simulates network unreachability), fail, and Temporal retries with backoff. The delivery actor finishes its delivery but can't report back. No workflow-side cancellation or polling — just the standard Temporal retry mechanism.

### Where the ADK agents fit

In **live mode**, the agents run **inline in the workflow** via `_run_adk_assignment()` in `MeltdownDemoWorkflow`. The workflow builds `DriverSnapshot`s from its own state and passes them to the ADK pipeline. Each LLM call and tool call becomes a Temporal activity via `TemporalModel` and `activity_tool` — the workflow code never calls an explicit `reason_about_assignment` activity. If an activity fails, Temporal retries. There is no fallback to mock.

In **mock mode**, `mock_reason_about_assignment` in `agent_fleet/mock/activities.py` is registered as a single activity with `@activity.defn(name="reason_about_assignment")`. The workflow calls this activity instead of running ADK inline. The live workflow code has zero awareness of this — the mock worker registers the activity with the same name.

Fleet Agent, Customer Agent, and Resolver are all **LLM Agents** — each is an `Agent` with `model=TemporalModel(DEFAULT_MODEL, activity_config=ActivityConfig(task_queue=AGENTS_QUEUE))`, meaning every Gemini call they make becomes an `invoke_model` Temporal activity routed to the agents worker. The `create_order_assignment_agent()` function returns an **Orchestrator Agent** (`SequentialAgent`) — it has no model, makes no LLM calls, and has no corresponding Temporal activity. It purely sequences the sub-agents.

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
    resolver = create_assignment_resolver()     # synthesizes → calls tool_submit_assignment

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, resolver],
    )
```

Fleet Agent and Customer Agent run in parallel (ADK handles that). Then the Resolver runs sequentially after both complete. In live mode, the workflow runs ADK inline via `_run_adk_assignment()`. In mock mode, the workflow calls the `reason_about_assignment` activity (which the mock worker registers).

### How `TemporalModel` and `activity_tool` work — and why you don't define agent activities explicitly

A common question from engineers: "where are the Temporal activities defined for each agent's LLM call and tool call?" The answer is they aren't — they're injected automatically by two wrappers:

- **`TemporalModel(DEFAULT_MODEL, activity_config=...)`** — when you set this as an agent's model, every LLM call that agent makes is automatically executed as a Temporal `invoke_model` activity routed to the agents queue. You don't write the activity. The wrapper does it. ADK supports other models too — swap `DEFAULT_MODEL` for any supported provider.
- **`activity_tool(tool_get_fleet_status, ...)`** — when you wrap a tool function this way, every time an agent calls that tool it executes as a Temporal activity. Again, no explicit activity definition needed. Our local [`_activity_tool.py`](agent_fleet/_activity_tool.py) adds two fixes over the upstream version: correct multi-arg handling, and **graceful failure** — when an activity exhausts its retry policy, the error is returned as a string to the LLM instead of crashing the pipeline. This is how Fleet Agent disconnect works: tools fail fast (2 retries), the LLM sees the error, and the Resolver assigns without fleet data.

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

ADK owns the agent orchestration — the sequencing of Fleet → Customer → Resolver via `SequentialAgent` and `ParallelAgent`, the multi-turn reasoning loop, passing context between agents. Temporal owns the durability of every external call those agents make.

An alternative "more Temporal-native" design would be to put the Fleet → Customer → Resolver sequencing directly in the workflow and only push the raw LLM calls into activities. That gives you more explicit visibility in the Temporal UI — each agent step shows up as a named workflow step. The tradeoff is you'd be rewriting ADK's orchestration in Temporal workflow code, giving up ADK's agent composition primitives.

The current design keeps both frameworks doing what they're best at: **ADK composes and sequences agents, Temporal makes every external call durable.** This is the recommended pattern for the `temporalio[google-adk]` integration — `TemporalModel` and `activity_tool` exist specifically to enable ADK agents running inline in workflows with per-call durability.

### The 3-queue worker architecture

The demo runs three Temporal workers in a **separate worker process** (`python -m agent_fleet.worker`), each on a dedicated task queue. The FastAPI server runs in its own process — it queries Temporal workflows for state and sends signals only.

| Queue | Worker | What it runs |
|---|---|---|
| `meltdown-workflows` | Workflows only | `MeltdownDemoWorkflow`, `DriverRouteWorkflow`, `OrderGenerationWorkflow` — no activities, dedicated to replay |
| `meltdown-delivery` | Delivery | `navigate_to`, `pickup_orders`, `deliver_order`, `generate_order`, `execute_customer_change`, `get_route_polyline`, `get_fleet_status`, `get_order_priorities`, `publish_agent_event` |
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
- [`workflows.py`](agent_fleet/workflows.py) `MeltdownDemoWorkflow.run()` → starts 3 `DriverRouteWorkflow` children + `OrderGenerationWorkflow` child

**3. Order generates on timer**
- [`workflows.py`](agent_fleet/workflows.py) `OrderGenerationWorkflow.run()` → calls `generate_order` activity every 15s → signals parent with `new_order`
- [`activities.py`](agent_fleet/activities.py) `generate_order()` → picks random venue, registers in FleetState

**4. Parent runs ADK agents inline**
- [`workflows.py`](agent_fleet/workflows.py) `_assign_order()` → builds `DriverSnapshot`s from workflow state → calls `_run_adk_assignment()`
- `_run_adk_assignment()` → creates ADK `Runner`, calls `runner.run_async()` — agents execute **inline in the workflow**

**5. ADK agent pipeline**
- [`agents.py`](agent_fleet/agents.py) `create_order_assignment_agent()` → `SequentialAgent`:
  - `ParallelAgent` runs **Fleet Agent** + **Customer Agent** simultaneously
  - Then **Resolver** runs sequentially
- Each agent uses `TemporalModel` → every Gemini call becomes an `invoke_model` activity on `AGENTS_QUEUE`

**6. Tool calls → Temporal activities**
- [`agents.py`](agent_fleet/agents.py) — tools wrapped via `activity_tool()` from [`_activity_tool.py`](agent_fleet/_activity_tool.py)
- Fleet Agent calls: `tool_get_fleet_status`, `tool_get_route_info` (Google Maps)
- Customer Agent calls: `tool_get_order_priorities`, `google_search` (Gemini grounding)
- Each tool call → `workflow.execute_activity()` → recorded in Temporal event log

**7. Resolver decides**
- [`agents.py`](agent_fleet/agents.py) `tool_submit_assignment()` → writes `{driver_id, reasoning_summary}` to ADK session state (in-memory, not a Temporal activity)

**8. Result flows back**
- [`workflows.py`](agent_fleet/workflows.py) `_run_adk_assignment()` → reads `session.state["assignment"]` → returns `ReasonAboutAssignmentOutput`

**9. Assignment registered, delivery actor signaled**
- [`workflows.py`](agent_fleet/workflows.py) `_assign_order()` → calls `register_assignment` activity (FleetState write) → signals chosen `DriverRouteWorkflow` with `add_order`

**10. Delivery actor executes**
- [`workflows.py`](agent_fleet/workflows.py) `DriverRouteWorkflow.run()` → for each order:
  - `get_route_polyline` activity → Google Maps polyline to warehouse
  - `navigate_to` activity → interpolates position with heartbeats (0.4s/step)
  - `pickup_orders` activity → marks picked up
  - `get_route_polyline` activity → Google Maps polyline to hotel
  - `navigate_to` activity → drives to hotel
  - (if `_reroute_pending` flag set by `update_order` signal → re-navigates to new destination)
  - `deliver_order` activity → marks delivered (skipped if `_cancel_pending`)
- [`activities.py`](agent_fleet/activities.py) — all activities on `DELIVERY_QUEUE`

**11. Delivery actor signals parent, returns to base**
- [`workflows.py`](agent_fleet/workflows.py) → signals parent with `order_delivered` → parent updates `_driver_orders` and `_driver_last_position`
- After all pending orders delivered, delivery actor navigates back to Frosty's (visible on map) → idle, waits for next order

**Key difference in live vs mock:** In live mode, ADK runs inline in the workflow — every LLM call and tool call is a separate Temporal activity visible in the event log. In mock mode, the entire reasoning is a single `reason_about_assignment` activity.

---

## Demo Scenarios

---

### Opening: Continuous Order Flow — Agents Reasoning in Real Time
**Time: 1–2 min | Best for: opening with the "living system" feel before the 3 demos**

**Setup:** Click **Start Deliveries**. Orders auto-generate every 15 seconds from 3 Las Vegas hotels (MGM Grand, Caesars Palace, Mandalay Bay).

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the Agent Reasoning panel
2. Fleet Agent calls `tool_get_fleet_status` and `tool_get_route_info` — scans delivery actor positions, free capacity slots, and driving ETAs. Recommends the closest available delivery actor.
3. Customer Agent calls `tool_get_order_priorities` and uses `google_search` (Gemini grounding) — evaluates VIP tier, deadline pressure, hotel events (conferences, galas), and guest count. Mandalay Bay orders are always VIP.
4. Resolver synthesizes both assessments and calls `tool_submit_assignment` — picks the best delivery actor and explains why
5. Delivery actors continuously pick up from Frosty's and deliver to hotels, looping back for more

**What to say:**
> "This is a continuous fleet — orders keep coming in, agents keep reasoning. Every assignment is a multi-agent decision. Fleet Agent checks who's closest and has capacity. Customer Agent evaluates priority — that Mandalay Bay order is VIP. The Resolver weighs both and assigns. Each delivery actor runs in its own child workflow, picking up and delivering in a continuous loop."

**Before you demo, set up the Temporal UI:**
- Open http://localhost:8233 in a separate browser tab
- Search for `meltdown-demo` — this is the parent workflow
- Also open `route-driver-1` in another tab — this shows a delivery actor's child workflow
- After starting deliveries, you'll see activities streaming in: `generate_order`, `invoke_model` (LLM calls), `tool_get_fleet_status`, `tool_submit_assignment`, etc.
- Point out how each agent's LLM call and tool call shows up as a separate activity with a summary label — *"Every reasoning step is individually durable and visible"*

**Temporal concept to highlight:** Child workflow isolation, continuous workflows with signals, per-call visibility in the event log

---

### Demo 1: Tool Degradation — Agent Tools Fail, System Adapts
**Time: 2–3 min | Best for: showing Temporal retry at the tool-call level and LLM adaptation**

**Setup:** Start deliveries. Let a few orders get assigned so the audience sees the normal flow first.

**Before disconnecting, set up the Temporal UI:**
- Open the `meltdown-demo` workflow in the Temporal UI → History tab
- Scroll to the latest activities — you should see clusters of `invoke_model` and `tool_get_fleet_status` for recent assignments
- This is where the retry attempts will appear after you disconnect

**Steps:**
1. Click **Disconnect Agent** (Fleet Agent)
2. Wait for the next order to trigger the ADK pipeline
3. **Show the Temporal UI**: `tool_get_fleet_status` shows `ActivityTaskFailed` → retry → `ActivityTaskFailed` (2 attempts exhausted). Point out: *"Temporal tried the tool twice — you can see both attempts here"*
4. The pipeline continues — `invoke_model` for the Resolver runs with the error context
5. `tool_submit_assignment` succeeds — assignment completed despite Fleet Agent failure
6. Orders keep getting assigned — the Resolver adapts each time
7. Click **Reconnect Agent**
8. **Show the Temporal UI**: next order's `tool_get_fleet_status` shows `ActivityTaskCompleted` — tools work again. *"Full assessment restored — the workflow picked up exactly where it left off"*

**What to say:**
> "Fleet Agent's tools are backed by Temporal activities. When the agent is disconnected, those activities fail — Temporal retries twice, then the error is returned to the LLM. The agent doesn't crash, it reasons about the failure. The Resolver sees the error and assigns based on Customer Agent data alone. When we reconnect, the next order's tools work normally. Two layers working together: Temporal retries the tool call, the LLM adapts to the failure."

**Temporal concept to highlight:** Per-tool-call retry (Temporal), LLM reasoning about tool failure (ADK), graceful degradation without pipeline crash

---

### Demo 2: Service Disruption & Recovery — Delivery Actor Loses Connection
**Time: 2–3 min | Best for: showing Temporal activity retry and durable state**

**Setup:** Start deliveries. Wait until at least one delivery actor is en route to a hotel.

**Before disconnecting, set up the Temporal UI:**
- Open the child workflow for the delivery actor you'll disconnect (e.g., `route-driver-1`) in a Temporal UI tab
- Position it side by side with the demo dashboard so the audience can see both
- Also open another delivery actor's workflow (e.g., `route-driver-2`) to show it's unaffected

**Steps:**
1. In the Failure Modes panel, select a delivery actor and click **Service Lost**
2. The delivery actor **finishes its current delivery** (truck keeps moving — it's already on the road)
3. After arriving at the hotel, it can't report back — status shows `DISCONNECTED`
4. The delivery actor stays at the hotel on the map. The other two keep delivering normally.
5. **Show the Temporal UI**: point to the `ActivityTaskFailed` → `ActivityTaskScheduled` retry cycles in the child workflow. Each failed attempt shows the error: "delivered but cannot report — disconnected." The backoff intervals grow between retries.
6. Wait 10–15 seconds, then click **Reconnect**
7. **Show the Temporal UI**: the next retry shows `ActivityTaskCompleted` — the workflow continues
8. On the map, the delivery actor navigates back to Frosty's for the next order

**What to say:**
> "The delivery actor finished the delivery — the truck doesn't stop mid-road. But it can't report back because the connection is lost. Look at the Temporal UI — you can see each retry attempt with growing backoff intervals. Every failure is recorded. When we reconnect, the next retry succeeds, the child workflow gets the delivery result, and the delivery actor heads back for the next order. Temporal held the state the entire time — nothing was lost."
>
> Point to the other delivery actor's workflow: *"Meanwhile, this one has a clean stream of completed activities — completely unaffected. That's child workflow isolation."*

**Temporal concept to highlight:** Activity retry policies with backoff, child workflow isolation, durable state across failures

---

### Demo 3: Human-in-the-Loop (HITL) — Customer Change with Mid-Delivery Reroute
**Time: 2–3 min | Best for: showing signals, workflow waiting, and cross-workflow coordination**

**Setup:** Start deliveries. Wait for a delivery actor to be en route to a hotel (actively delivering).

**Steps:**
1. In the Customer Changes panel, the dropdown shows **active orders with their assigned delivery actor** — pick one that's currently being delivered
2. Select "Address Change" and click **Submit Change Request** — this always reroutes to **The Cosmopolitan**, which appears as a new marker on the map
3. The workflow received the request and is holding it — waiting for approval. Meanwhile, orders keep generating and deliveries continue.
4. Click **Approve** — the parent signals the delivery actor's child workflow with `update_order`
5. Watch the map: the delivery actor **finishes its current navigation leg**, then **reroutes to The Cosmopolitan** — a new marker appears and the order card updates to show the new hotel
6. For cancellation: select "Cancel Order" → Approve → the delivery actor skips delivery and returns to base

**What to say:**
> "The workflow received the change request and is holding it in memory — waiting for the approval signal. Meanwhile, everything else keeps running: orders generate, agents reason, deliveries complete. When the approval arrives, the workflow picks up exactly where it left off and executes the change. No polling loop, no database check — just `wait_condition`. Two things happen: the parent workflow executes the change, then signals the delivery actor's child workflow. The delivery actor finishes the current leg then reroutes to The Cosmopolitan — you can see the new marker appear and the order card update. All of this is cross-workflow coordination via signals — durable and recoverable."

**What you'll see in Temporal UI:**
- `meltdown-demo` workflow: `WorkflowExecutionSignaled` (`customer_change`) → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → signal sent to child
- `route-driver-X` workflow: `WorkflowExecutionSignaled` (`update_order`) → new `get_route_polyline` and `navigate_to` activities as the delivery actor reroutes to The Cosmopolitan
- The parent workflow stays busy between those signals — orders keep generating, agents keep reasoning. The `wait_condition` pauses only the customer-change code path, not the whole workflow.
- Point to the child workflow: *"The delivery actor got the update and rerouted mid-delivery. Two workflows coordinating via signals — all durable."*

**Temporal concept to highlight:** Signals, `wait_condition`, cross-workflow signaling, mid-delivery reroute

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a complete execution model — retries, timeouts, timeouts-per-retry, backoff, heartbeating, child workflows, signals, queries. And it's all in code, not config."

**"What if Gemini returns something unexpected?"**
> "The agents use structured tool calls to submit their output — `tool_submit_assignment` writes a typed object to ADK session state. The workflow reads that object. If the agent produces garbage or skips the tool call, the activity fails and Temporal retries it with backoff. There's a clear contract."

**"Is this production-ready?"**
> "The pattern is production-ready — Temporal runs at Stripe, Netflix, Uber. ADK is Google's framework for building agents at scale. The integration shown here (`TemporalModel`, `activity_tool`, `GoogleAdkPlugin`) is the `temporalio[google-adk]` package, which is the official integration."

---

## Reset Between Demos

1. Click **Reset** on the dashboard
2. Verify all delivery actors return to idle at Frosty's Ice Cream
3. If any workflows are stuck, run: `temporal workflow list` and cancel manually
4. Refresh the browser before the next run
