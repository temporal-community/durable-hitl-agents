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
- Map shows 3 hotels (MGM Grand, Caesars, Mandalay Bay) and Ziggy's Ice Cream shop
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
5. Delivery actors continuously pick up from Ziggy's and deliver to hotels, looping back for more

**What to say:**
> "This is a continuous fleet — orders keep coming in, agents keep reasoning. Every assignment is a multi-agent decision. Fleet Agent checks who's closest and has capacity. Customer Agent evaluates priority — that Mandalay Bay order is VIP. The Dispatch Agent weighs both and assigns. Each delivery actor runs in its own child workflow, picking up and delivering in a continuous loop."

**Before you demo, set up the Temporal UI:**
- Open http://localhost:8233 in a separate browser tab
- Search for `meltdown-demo` — this is the parent workflow
- Also open `route-driver-a` in another tab — this shows a delivery actor's child workflow
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
1. Click **Agent Disconnect** (Fleet Agent)
2. Wait for the next order to trigger the ADK pipeline
3. **Show the Temporal UI**: `tool_get_fleet_status` shows `ActivityTaskFailed` → retry → `ActivityTaskFailed` (2 attempts exhausted). Point out: *"Temporal tried the tool twice — you can see both attempts here"*
4. The pipeline continues — `invoke_model` for the Dispatch Agent runs with the error context
5. `tool_submit_assignment` succeeds — but notice the order card shows a **DEGRADED** badge (orange). The Dispatch Agent assigned the order without fleet visibility — it doesn't know which driver is closest, available, or has capacity.
6. If Dispatch assigns to a driver that's already full (3 orders), the system automatically reassigns to the next available driver — visible in the workflow log.
7. Orders keep getting assigned — but quality degrades. You may see one driver overloaded while another sits idle.
8. Click **Reconnect Agent**
9. **Show the Temporal UI**: next order's `tool_get_fleet_status` shows `ActivityTaskCompleted` — tools work again. New orders no longer show the DEGRADED badge. *"Full fleet visibility restored — assignments are optimal again."*

**What to say:**
> "Without the Fleet Agent, the Dispatch Agent is flying blind. It can still assign orders — the system doesn't break — but look at the quality. That order went to Driver-A even though Driver-C was right next door and idle. See the orange 'Degraded' badge? That means no fleet data was available for that decision. The system gracefully degrades: Temporal retries the tool, the LLM adapts, but the decisions are measurably worse. When we reconnect, assignments go back to optimal — closest driver, most capacity."

**Temporal concept to highlight:** Per-tool-call retry (Temporal), LLM reasoning about tool failure (ADK), graceful degradation with visible quality impact, automatic capacity-based reassignment

---

### Demo 2: Service Disruption & Recovery — Delivery Actor Loses Connection
**Time: 2–3 min | Best for: showing Temporal activity retry and durable state**

**Setup:** Start deliveries. Wait until a delivery actor has **multiple orders queued** (visible in the fleet panel — drivers batch-pickup up to 3 orders at Ziggy's and deliver them sequentially).

**Before disconnecting, set up the Temporal UI:**
- Open the child workflow for the delivery actor you'll disconnect (e.g., `route-driver-a`) in a Temporal UI tab
- Position it side by side with the demo dashboard so the audience can see both
- Also open another delivery actor's workflow (e.g., `route-driver-b`) to show it's unaffected

**Steps:**
1. In the Fleet Disconnect panel, select a delivery actor with multiple orders and click **Service Lost**
2. The delivery actor **finishes its current delivery** (truck keeps moving — it's already on the road)
3. After arriving at the hotel, it can't report back — status shows `DISCONNECTED`
4. The delivery actor stays at the hotel on the map. Other drivers keep delivering normally.
5. **Show the Temporal UI**: point to the `ActivityTaskFailed` → `ActivityTaskScheduled` retry cycles in the child workflow. Each failed attempt shows the error: "delivered but cannot report — disconnected." The backoff intervals grow between retries.
6. Wait 10–15 seconds, then click **Reconnect**
7. **Show the Temporal UI**: the next retry shows `ActivityTaskCompleted` — the workflow continues
8. On the map, the delivery actor **drives from the hotel to its next delivery** — it doesn't repeat the delivery it already completed, and it doesn't teleport. The workflow knew exactly where it left off.

**What to say:**
> "This driver had 3 orders queued. It completed the first delivery but lost connection before reporting back. Look at the Temporal UI — retry attempts with backoff. When we reconnect, the workflow picks up exactly where it left off: the first delivery is already done, so the driver heads to the second hotel. No repeated work, no lost state. Temporal held the entire execution history — every step is durable."
>
> Point to the other delivery actor's workflow: *"Meanwhile, this one has a clean stream of completed activities — completely unaffected. That's child workflow isolation."*

**Temporal concept to highlight:** Activity retry policies with backoff, child workflow isolation, durable state across failures, no repeated work on recovery

---

### Demo 3: Human-in-the-Loop (HITL) — Customer Change with Mid-Delivery Reroute
**Time: 2–3 min | Best for: showing signals, workflow waiting, and cross-workflow coordination**

**Setup:** Start deliveries. Wait for a delivery actor to be en route to a hotel (actively delivering).

**Steps:**
1. In the Customer Changes panel, the dropdown shows **active orders with their assigned delivery actor** — pick one that's currently being delivered
2. Select "Address Change" and click **Submit Change** — this always reroutes to **The Cosmopolitan**, which appears as a new marker on the map
3. The workflow received the request and is holding it — waiting for approval. Meanwhile, orders keep generating and deliveries continue.
4. Click **Approve** — the parent signals the delivery actor's child workflow with `update_order`
5. Watch the map: the delivery actor **finishes its current navigation leg**, then **reroutes to The Cosmopolitan** — a new marker appears and the order card updates to show the new hotel
6. **Multi-order twist:** If the driver has 3 orders queued, try changing order 3's address while order 1 is being delivered. Orders 1 and 2 deliver normally. When the driver gets to order 3, it goes to the updated address — the workflow held the change in state the entire time.
7. For cancellation: select "Cancel Order" → Approve → the delivery actor skips that delivery and moves to the next order in its batch

**What to say:**
> "The workflow received the change request and is holding it in memory — waiting for the approval signal. Meanwhile, everything else keeps running. When approved, there are two cases: if the order is actively being delivered, the driver finishes the current leg and reroutes. But if the order is still queued — like order 3 while the driver is delivering order 1 — the coordinates update silently in the workflow state. No reroute needed yet. When the driver eventually gets to that order, it goes to the new address. Temporal held the updated state across all three deliveries."

**What you'll see in Temporal UI:**
- `meltdown-demo` workflow: `WorkflowExecutionSignaled` (`customer_change`) → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → signal sent to child
- `route-driver-X` workflow: `WorkflowExecutionSignaled` (`update_order`) — for active orders, new `get_route_polyline` and `navigate_to` activities as the driver reroutes; for queued orders, the signal updates pending state silently
- The parent workflow stays busy between those signals — orders keep generating, agents keep reasoning. The `wait_condition` pauses only the customer-change code path, not the whole workflow.
- Point to the child workflow: *"Two workflows coordinating via signals — durable and recoverable, whether the change hits an active delivery or a queued one."*

**Temporal concept to highlight:** Signals, `wait_condition`, cross-workflow signaling, mid-delivery reroute, durable state for queued changes

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a complete execution model — retries, timeouts, timeouts-per-retry, backoff, heartbeating, child workflows, signals, queries. And it's all in code, not config."

**"What if Gemini returns something unexpected?"**
> "The agents use structured tool calls to submit their output — `tool_submit_assignment` writes a typed object to ADK session state. The workflow reads that object. If the agent produces garbage or skips the tool call, the activity fails and Temporal retries it with backoff. There's a clear contract."

**"Is this production-ready?"**
> "The pattern is production-ready — Temporal runs at Stripe, Netflix, Uber. ADK is Google's framework for building agents at scale. The integration shown here (`TemporalModel`, `activity_tool`, `GoogleAdkPlugin`) is the `temporalio[google-adk]` package, which is the official integration."

**"Why does the Fleet Agent sometimes fail on the ETA assessment?"**
> "That's the Google Maps Directions API — `tool_get_route_info` makes a real API call for driving ETAs. It can fail from rate limiting, quota exhaustion, or transient network errors. But watch what happens: the error is returned to the LLM, not swallowed. The Fleet Agent reasons about the failure, and the Dispatch Agent assigns based on whatever data it has. That's the graceful degradation pattern — every tool call is a Temporal activity with its own retry policy. If it exhausts retries, the error flows back to the agent as context."

---

## Reset Between Demos

1. Click **Reset** on the dashboard
2. Verify all delivery actors return to idle at Ziggy's Ice Cream
3. If any workflows are stuck, run: `temporal workflow list` and cancel manually
4. Refresh the browser before the next run
