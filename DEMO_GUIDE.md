# Meltdown Demo Delivery Guide

This guide is for anyone presenting the Meltdown demo. It covers setup, the one-minute pitch on each technology, and step-by-step scripts for each demo scenario (~2–5 min each).

---

## Before You Start

**Requirements:**
- Temporal CLI running: `temporal server start-dev`
- `.env` with `GOOGLE_API_KEY` set (Gemini). Maps and CSE keys are optional — demo works without them.
- `./run.sh` (or `make run`) started, browser open at http://localhost:8080
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history)

**Pre-flight check:**
- Map shows 3 hotels (MGM Grand, Caesars, Mandalay Bay) and Frosty's Ice Cream shop
- All 3 crews are at the ice cream shop, status idle
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

> "Google ADK is an open-source framework for building multi-agent AI systems. You compose agents — each with their own tools and model — into pipelines: run them sequentially, in parallel, or nested. In this demo, a Fleet Agent assesses crew positions and capacity, a Customer Agent evaluates order priority and hotel context, and a Resolver Agent synthesizes their output into a crew assignment."

**Key points to land:**
- ADK has two agent types: **LLM Agents** (`Agent` with a model) call Gemini to reason and use tools; **Orchestrator Agents** (`SequentialAgent`, `ParallelAgent`) coordinate sub-agents without calling an LLM themselves
- In this demo: Fleet Agent, Customer Agent, and Resolver are all LLM Agents — each calls Gemini. The outer pipeline (`create_order_assignment_agent`) is an Orchestrator Agent — it sequences them with no LLM of its own
- Each agent can use tools (Maps, Search, custom functions)
- ADK manages the multi-turn reasoning loop — the developer just defines the agents and wires them together

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as code — workflows and activities — and Temporal guarantees it runs to completion even if the service crashes, times out, or gets disconnected. Every step is recorded in an event log. If the worker dies mid-execution, Temporal replays the history deterministically, and your code resumes exactly where it left off."

**Key points to land:**
- Workflows are durable — crashes don't lose state
- Activities are retryable by default — transient failures self-heal
- Signals let you inject events into a running workflow (crew disconnect, agent disconnect, customer change)
- The Temporal UI shows the full event history for every workflow run — nothing is a black box

---

## The Integration: Why It Matters

> "Here's the key insight: in this demo, every LLM call goes through a `TemporalModel` wrapper — it becomes a Temporal activity. Every tool call (Maps, search, fleet status) is also a Temporal activity. That means if the worker crashes mid-agent-reasoning, Temporal doesn't re-call the LLM. It replays the result from the event log. The agent resumes exactly where it left off, with no extra cost and no lost context."

This is the "aha" moment. Return to it when showing crew disconnect recovery or the customer change approval flow.

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

### Why two workflow classes?

**`MeltdownDemoWorkflow`** is the brain. It owns the fleet state — crew positions, order assignments, disconnect/reconnect status. It runs assignment agents, builds `CrewSnapshot`s from its own state and passes them to activities as inputs, and handles customer changes. It never does delivery work directly — it delegates to child workflows.

**`CrewRouteWorkflow`** is the legs. One instance per crew, it executes the physical route: navigate to kitchen → pick up → navigate to hotel → deliver → signal parent → loop. It owns its own disconnect state and uses **cancellation scopes** for mid-flight disconnect handling. When the crew disconnects, the workflow cancels the running activity, waits for a reconnect signal, then resumes.

The two connect through signals in both directions:
- **Parent → child:** `add_order` (new delivery), `crew_disconnected` / `crew_reconnected`
- **Child → parent:** `order_delivered` (updates parent's crew state — position and order count)

```
New order arrives
  → MeltdownDemoWorkflow builds CrewSnapshots from workflow state
  → runs assignment agents with snapshots as input → "give this to AI-Crew 2"
  → updates self._crew_orders, sends add_order signal to CrewRouteWorkflow
  → CrewRouteWorkflow executes the delivery
  → on completion, signals parent with order_delivered
```

The key design principles:
- **Child workflows give you fault isolation.** Each crew runs independently. If AI-Crew 1 hits an error, 2 and 3 keep running.
- **Workflows own state, activities are pure.** Activities receive everything they need as inputs — they never read shared state for decision-making. `FleetState` is a write-only UI projection.
- **Disconnect flows through Temporal.** API endpoints send signals only. The workflow handles cancellation, waiting, and syncing state to the UI via activities.

### Where the ADK agents fit

The agents are not workflows — they run inside **activities**. `reason_about_assignment` is a regular Temporal activity that spins up an ADK runner internally. The workflow calls the activity and passes crew state as input (`CrewSnapshot`s, `disconnected_agents`); the activity runs the agents using that input. This is the right layering: the workflow handles durability, state, and coordination; the activity handles the work.

Fleet Agent, Customer Agent, and Resolver are all **LLM Agents** — each is an `Agent` with `model=TemporalModel(DEFAULT_MODEL)`, meaning every Gemini call they make becomes a `invoke_model` Temporal activity. The `create_order_assignment_agent()` function returns an **Orchestrator Agent** (`SequentialAgent`) — it has no model, makes no LLM calls, and has no corresponding Temporal activity. It purely sequences the sub-agents.

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

Fleet Agent and Customer Agent run in parallel (ADK handles that). Then the Resolver runs sequentially after both complete. The workflow just calls `execute_activity(reason_about_assignment, ...)` — it doesn't know or care about the agent internals.

### How `TemporalModel` and `activity_tool` work — and why you don't define agent activities explicitly

A common question from engineers: "where are the Temporal activities defined for each agent's LLM call and tool call?" The answer is they aren't — they're injected automatically by two wrappers:

- **`TemporalModel(DEFAULT_MODEL)`** — when you set this as an agent's model, every LLM call that agent makes is automatically executed as a Temporal `invoke_model` activity. You don't write the activity. The wrapper does it.
- **`activity_tool(tool_get_fleet_status, ...)`** — when you wrap a tool function this way, every time an agent calls that tool it executes as a Temporal activity. Again, no explicit activity definition needed.

So when Fleet Agent calls Gemini and then calls `tool_get_fleet_status`, both of those are Temporal activities — durable, retryable, and recorded in the event log — purely by inheritance from the wrappers. This is the `temporalio[google-adk]` integration doing its job.

Here's exactly what this looks like in [`agent_fleet/agents.py`](agent_fleet/agents.py):

```python
# Each agent gets TemporalModel — LLM calls become invoke_model activities automatically
fleet_agent = Agent(
    name="assignment_fleet_agent",
    model=TemporalModel(DEFAULT_MODEL),   # ← this is the only change vs a plain ADK agent
    tools=[_fleet_status_tool, _route_info_tool, _publish_event_tool],
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

The current design keeps both frameworks doing what they're best at: **ADK composes and sequences agents, Temporal makes every external call durable.** The replay safety is there either way — it's a question of where the orchestration logic lives and how much of it is visible in the Temporal event log.

### The 3-queue worker architecture

The demo runs three Temporal workers in the same Python process, each on a dedicated task queue:

| Queue | Worker | What it runs |
|---|---|---|
| `meltdown-workflows` | Workflows only | `MeltdownDemoWorkflow`, `CrewRouteWorkflow` — no activities, dedicated to replay |
| `meltdown-delivery` | Delivery | `navigate_to`, `pickup_orders`, `deliver_order`, `generate_order`, `sync_crew_disconnect`, `execute_customer_change`, `publish_agent_event` |
| `meltdown-agents` | Agents | `reason_about_assignment`, `register_assignment`, all `tool_*` activities |

**Why a workflows-only worker?** Workflows must be deterministic and replayable. Keeping them on a dedicated worker with no activities makes it physically impossible for workflow code to touch `FleetState` or do I/O. This is the Temporal-idiomatic pattern for production deployments.

**Why separate activity queues?** LLM calls are slow — a single Gemini call can take 3–5 seconds. Without queue separation, a flood of assignment requests could fill all worker slots and starve navigation activities, causing crews to miss heartbeat timeouts. The agents queue is rate-limited to 5 concurrent activities; the delivery queue runs 20.

**Why in-process?** Activities write to the `FleetState` singleton for the frontend WebSocket. Splitting them into separate processes would require a shared state layer (Redis, Postgres). For a demo, in-process gives you the right separation without operational overhead. Importantly, activities only **write** to `FleetState` as a UI projection — they never **read** it for decision-making. All decision data flows through workflow state → activity inputs.

The three workers are set up in [`agent_fleet/worker.py`](agent_fleet/worker.py):

```python
def create_workflow_worker(client: Client) -> Worker:
    """Workflow-only worker — no activities, dedicated to replay."""
    return Worker(client, task_queue=WORKFLOWS_QUEUE,
                  workflows=[MeltdownDemoWorkflow, CrewRouteWorkflow])

def create_agents_worker(client: Client) -> Worker:
    """ADK/LLM activities — rate-limited, GoogleAdkPlugin only registered here."""
    return Worker(client, task_queue=AGENTS_QUEUE,
                  activities=[reason_about_assignment, register_assignment, ...],
                  max_concurrent_activities=5,
                  plugins=[GoogleAdkPlugin()])
```

`GoogleAdkPlugin` only needs to be registered on the worker that runs ADK activities — the delivery and workflows workers don't need it.

### What this would look like without Temporal

Without Temporal, the same orchestration would require:
- A state machine in a database (enum column per crew tracking route phase)
- Manual retry loops with custom backoff for every activity
- A polling loop to implement "wait for human approval" (`while not db.get("approved"): sleep(1)`)
- Defensive DB writes before every step so a crash doesn't lose position
- Manual reconstruction of in-flight state on worker restart
- A shared state store (Redis, Postgres) for cross-service coordination
- Custom cancellation logic for mid-activity interruption

Temporal collapses all of that into the workflow execution model. The event log *is* the state persistence. `execute_activity` *is* the retry logic. Signals *are* the message passing. Cancellation scopes *are* the interrupt mechanism. The workflow code reads like a straightforward sequential program because Temporal handles everything else.

In this demo, the workflows are the source of truth for all operational state — crew positions, order assignments, disconnect status. Activities receive this state as inputs and return results. `FleetState` exists only as a read-optimized projection for the frontend WebSocket. If the process restarts, Temporal replays the workflows, activities re-execute, and the UI projection is rebuilt.

---

## Demo Scenarios

---

### Demo 1: Continuous Order Flow — Agents Reasoning in Real Time
**Time: 1–2 min | Best for: opening with the "living system" feel**

**Setup:** Click **Start Deliveries**. Orders auto-generate every 15 seconds from 3 Las Vegas hotels (MGM Grand, Caesars Palace, Mandalay Bay).

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the Agent Reasoning panel
2. Fleet Agent scans crew positions and capacity, recommends the closest available crew
3. Customer Agent evaluates priority — Mandalay Bay orders are always VIP
4. Resolver synthesizes and assigns the order to a crew
5. Crews continuously pick up from Frosty's and deliver to hotels, looping back for more

**What to say:**
> "This is a continuous fleet — orders keep coming in, agents keep reasoning. Every assignment is a multi-agent decision. Fleet Agent checks who's closest and has capacity. Customer Agent evaluates priority — that Mandalay Bay order is VIP. The Resolver weighs both and assigns. Each crew runs in its own child workflow, picking up and delivering in a continuous loop."

**Temporal concept to highlight:** Child workflow isolation, continuous workflows with signals

---

### Demo 2: Crew Disconnect & Auto-Recovery
**Time: 2–3 min | Best for: showing workflow-driven cancellation and signals**

**Setup:** Start deliveries. Wait until at least one crew is en route.

**Steps:**
1. In the Failure Modes panel, select a crew and click **Disconnect Crew**
2. That crew's status changes to `DISCONNECTED`, its truck stops moving
3. The other two crews keep delivering normally
4. Wait 10–15 seconds, then click **Reconnect Crew**
5. The crew's status shows a brief "recovering" state, then resumes

**What to say:**
> "When we disconnect the crew, the API sends a signal — nothing else. The crew's child workflow receives the signal, cancels the running navigation activity via a cancellation scope, and waits. No polling, no shared state flags. When we reconnect, another signal arrives, the workflow resumes, and the activity restarts. Everything flows through Temporal — the API is just a signal relay."

**What you'll see in Temporal UI** (`route-ai-crew-X` workflow → History tab):
- Open the child workflow for the disconnected crew (search `route-ai-crew-1`, `route-ai-crew-2`, or `route-ai-crew-3`)
- A `WorkflowExecutionSignaled` event with signal name `crew_disconnected` — the workflow received the signal
- The `navigate_to` activity shows `ActivityTaskCancelled` — the workflow's cancellation scope cancelled it
- A `sync_crew_disconnect` activity completes — the workflow pushed disconnect state to the UI via an activity
- The event history **pauses** — the workflow is waiting on `wait_condition` for reconnect
- On reconnect: another `WorkflowExecutionSignaled` (`crew_reconnected`), then `sync_crew_disconnect` (reconnect), then `navigate_to` restarts cleanly
- Open the other two crew workflows side by side — clean stream of completed activities, completely unaffected. That's child workflow isolation.

**Temporal concept to highlight:** Cancellation scopes, signals, workflow-driven state management, child workflow isolation

---

### Demo 3: Agent Disconnect — ADK + Temporal Working Together
**Time: 2–3 min | Best for: showing why the integration matters**

**Setup:** Start deliveries. Let a few orders get assigned.

**Steps:**
1. Click **Disconnect Agent** (Fleet Agent)
2. Watch the Agent Reasoning panel — Fleet Agent shows "OFFLINE"
3. New orders still get assigned — the Resolver notes "Fleet Agent OFFLINE — assignment based on last known positions"
4. Customer Agent continues evaluating priority normally
5. Click **Reconnect Agent** — Fleet Agent comes back online, next order shows full fleet assessment again

**What to say:**
> "This is where ADK and Temporal each pull their weight. ADK handles the agent layer — when Fleet Agent goes offline, the Resolver adapts. It doesn't crash, it degrades gracefully, using the last known fleet data. Temporal handles the infrastructure layer — every reasoning step that did complete is recorded. Two different resilience mechanisms, working at two different layers."

**What you'll see in Temporal UI** (`meltdown-demo` workflow → History tab):
- Each order assignment shows a cluster of activities: `invoke_model` (the Gemini call), `tool_get_fleet_status`, `tool_publish_agent_event`, etc. — these are the ADK agents' LLM and tool calls, recorded individually as durable Temporal activities
- When Fleet Agent is offline, `reason_about_assignment` still completes cleanly — no retry, no error. ADK handled the degradation in the application layer; Temporal just recorded the result
- On reconnect, the next assignment shows more `invoke_model` calls — Fleet Agent is reasoning again
- Point to this and say: *"If this worker crashed right now mid-reasoning, Temporal would replay these results from the log. The agent would resume without re-calling Gemini."*

**Concepts to highlight:** ADK graceful degradation (agent layer) vs. Temporal durable execution (infrastructure layer)

---

### Demo 4: Customer Change — Human-in-the-Loop
**Time: 2 min | Best for: showing signals and workflow waiting**

**Setup:** Start deliveries. Wait for a few orders to be assigned.

**Steps:**
1. In the Customer Changes panel, select an order and click **Submit Change Request**
2. The Agent Reasoning panel shows a `customer_request` event — the workflow is now paused, waiting
3. Open Temporal UI — show the workflow is "Running" but blocked on `wait_condition`
4. Click **Approve** (or **Reject**) — the workflow immediately unblocks and executes (or discards) the change

**What to say:**
> "The workflow is literally paused here — waiting for a human signal. There's no polling, no timeout hack, no database flag. Temporal persists the workflow state indefinitely. If I closed the server right now and restarted it, the workflow would still be waiting for this approval. That's what durable execution means."

**What you'll see in Temporal UI** (`meltdown-demo` workflow → History tab):
- Immediately after submitting the change: a `WorkflowExecutionSignaled` event appears with signal name `customer_change` — Temporal received the signal and recorded it
- The event history then **stops growing** — no new activities are scheduled. The workflow is suspended, waiting
- The workflow status shows "Running" even though nothing is executing — it's parked on `wait_condition`, alive and durable
- When you approve: a second `WorkflowExecutionSignaled` event appears (`change_approved`), then immediately `execute_customer_change` and `publish_agent_event` activities complete
- Point to the gap in the event log: *"This silence is the workflow waiting. No polling. No timer. Temporal is just holding state until the signal arrives — which could be seconds or days."*

**Temporal concept to highlight:** Signals, `wait_condition`, indefinite workflow suspension

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a complete execution model — retries, timeouts, timeouts-per-retry, backoff, heartbeating, child workflows, signals, queries. And it's all in code, not config."

**"What if Gemini returns something unexpected?"**
> "The agents use structured tool calls to submit their output — `tool_submit_assignment` writes a typed object to ADK session state. The workflow reads that object. If the agent produces garbage or skips the tool call, the workflow gets `None` and falls back to a deterministic mock resolver. There's a clear contract."

**"Is this production-ready?"**
> "The pattern is production-ready — Temporal runs at Stripe, Netflix, Uber. ADK is Google's framework for building agents at scale. The integration shown here (`TemporalModel`, `activity_tool`, `GoogleAdkPlugin`) is the `temporalio[google-adk]` package, which is the official integration."

---

## Reset Between Demos

1. Click **Reset** on the dashboard
2. Verify all crews return to idle at Frosty's Ice Cream
3. If any workflows are stuck, run: `temporal workflow list` and cancel manually
4. Refresh the browser before the next run
