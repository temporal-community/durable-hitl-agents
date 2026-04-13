# Meltdown Demo Delivery Guide

This guide is for anyone presenting the Meltdown demo. It covers setup, the one-minute pitch on each technology, and step-by-step scripts for each demo scenario (~2–5 min each).

---

## Before You Start
(See [Quickstart](README.md#quick-start) for full setup instructions.)

**Requirements:**
- `.env` with two API keys (Google requires separate keys for Gemini vs Cloud APIs):
  - `GOOGLE_API_KEY` — Gemini key, restricted to Generative Language API. Without it, the demo runs in mock mode.
  - `GOOGLE_MAPS_API_KEY` — Maps key, restricted to Directions API.
- `./run.sh` (or `make run`) started — this starts the Temporal dev server, worker process, and server process automatically.
- Browser open at http://localhost:8080 for the web app
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history)

If completed successfully, the web app should look like the following:

<img width="1502" height="799" alt="Screenshot 2026-04-10 at 11 04 23 PM" src="https://github.com/user-attachments/assets/39a485e5-cbbf-4057-bb93-e15c7285ee3a" />

## How it works
See [How It Works](HOW_IT_WORKS.md) for more detailed "under the hood" information.

## Pre-flight check
- Map shows 3 hotels (MGM Grand, Caesars, Mandalay Bay) and Frosty's Ice Cream shop
- All 5 delivery actors are at the ice cream shop, status idle
- "Start Deliveries" button is active
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each [scenario](#demo-scenarios) before presenting to get familiar with the agent reasoning panel timing.

---

## The One-Minute Pitch

Use this framing at the start of the talk before any demo:

> "AI agents are increasingly being used to automate complex decisions — but in production, they break. The worker crashes. A tool call times out. The LLM call returns mid-reasoning and the state is gone. What we're showing today is what happens when you combine Google ADK — a framework for composing multi-agent AI — with Temporal — a durable execution engine — so that every agent action is retryable, replayable, and recoverable."

---

## What is Google ADK?

**The 30-second version:**

> "Google ADK is an open-source framework for building multi-agent AI systems. You compose agents — each with their own tools and model — into pipelines: run them sequentially, in parallel, or nested. In this demo, a Fleet Agent assesses delivery actor positions and capacity, a Customer Agent evaluates order priority and hotel context, and a Dispatch Agent synthesizes their output into a delivery assignment."

**Key points to land:**
- ADK has two agent types: **LLM Agents** (`Agent` with a model) call Gemini to reason and use tools; **Orchestrator Agents** (`SequentialAgent`, `ParallelAgent`) coordinate sub-agents without calling an LLM themselves
- In this demo: Fleet Agent, Customer Agent, and Dispatch Agent are all LLM Agents — each calls Gemini. The outer pipeline (`create_order_assignment_agent`) is an Orchestrator Agent — it sequences them with no LLM of its own
- Each agent can use tools (Maps, Search, custom functions)
- ADK supports multiple model providers — this demo uses Gemini, but you can swap to other models by changing the config
- ADK manages the multi-turn reasoning loop — the developer just defines the agents and wires them together


## Demo Scenarios

---

### Opening: Continuous Order Flow — Agents Reasoning in Real Time
**Time: 1–2 min | Best for: opening with the "living system" feel before the 3 demos**

**Setup:** Click **Start Deliveries**. Orders auto-generate every 10 seconds from 3 Las Vegas hotels (MGM Grand, Caesars Palace, Mandalay Bay).

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the Agent Reasoning panel
2. Fleet Agent calls `tool_get_fleet_status` and `tool_get_route_info` — scans delivery actor positions, free capacity slots, and driving ETAs. Recommends the closest available delivery actor.
3. Customer Agent calls `tool_get_order_priorities` and uses `google_search` (Gemini grounding) — evaluates VIP tier, deadline pressure, hotel events (conferences, galas), and guest count. Mandalay Bay orders are always VIP.
4. Dispatch Agent synthesizes both assessments and calls `tool_submit_assignment` — picks the best delivery actor and explains why
5. Delivery actors continuously pick up from Frosty's and deliver to hotels, looping back for more

**What to say:**
> "This is a continuous fleet — orders keep coming in, agents keep reasoning. Every assignment is a multi-agent decision. Fleet Agent checks who's closest and has capacity. Customer Agent evaluates priority — that Mandalay Bay order is VIP. The Dispatch Agent weighs both and assigns. Each delivery actor runs in its own child workflow, picking up and delivering in a continuous loop."

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
4. The pipeline continues — `invoke_model` for the Dispatch Agent runs with the error context
5. `tool_submit_assignment` succeeds — assignment completed despite Fleet Agent failure
6. Orders keep getting assigned — the Dispatch Agent adapts each time
7. Click **Reconnect Agent**
8. **Show the Temporal UI**: next order's `tool_get_fleet_status` shows `ActivityTaskCompleted` — tools work again. *"Full assessment restored — the workflow picked up exactly where it left off"*

**What to say:**
> "Fleet Agent's tools are backed by Temporal activities. When the agent is disconnected, those activities fail — Temporal retries twice, then the error is returned to the LLM. The agent doesn't crash, it reasons about the failure. The Dispatch Agent sees the error and assigns based on Customer Agent data alone. When we reconnect, the next order's tools work normally. Two layers working together: Temporal retries the tool call, the LLM adapts to the failure."

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
