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

![Meltdown Demo](.github/assets/meltdown-screenshot-3.png)

## How it works
See [How It Works](HOW_IT_WORKS.md) for more detailed "under the hood" information.

## Pre-flight check
- Map shows the Las Vegas Strip with 3 hotels (MGM Grand, Caesars, Mandalay Bay) and Ziggy's Ice Cream
- All 5 drivers (A–E) are parked at Ziggy's, status idle
- "Start Deliveries" button is active
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each [scenario](#demo-scenarios) before presenting to get familiar with the agent reasoning panel timing.

---

## The One-Minute Pitch

Use this framing at the start of the talk before any demo:

> "Ziggy's Ice Cream delivers to hotels on the Las Vegas Strip. They built their delivery intelligence on Temporal with Google ADK agents handling dispatch decisions. What we're showing today is their system under stress — what happens when an agent goes offline, a driver loses connection, or a customer changes their order mid-delivery. Every failure is recoverable because Temporal holds the state. Ice cream delivery can't afford lost state."

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

### Opening: Ziggy's Opens for Business
**Time: 1–2 min | Best for: opening with the "living system" feel before the 3 demos**

**Setup:** Click **Start Deliveries**. Ziggy's kitchen starts taking orders. Hotels on the Strip place orders every few seconds — MGM Grand, Caesars Palace, Mandalay Bay.

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the ADK Agent Team panel
2. Fleet Agent calls `tool_get_fleet_status` for driver positions and capacity, then `tool_get_route_info` for the 1–3 closest drivers to get driving ETAs from Google Maps. Each ETA call is a separate Temporal activity — individually durable.
3. Customer Agent calls `tool_get_order_priorities` and uses `google_search` (Gemini grounding) — evaluates VIP tier, deadline pressure, hotel events (conferences, galas), and guest count. Mandalay Bay orders are always VIP.
4. Dispatch Agent synthesizes both assessments and calls `tool_submit_assignment` — picks the best driver and explains why
5. Drivers batch-pickup at Ziggy's (up to 3 orders per trip) and deliver sequentially to hotels

**What to say:**
> "This is Ziggy's delivery system running live. Orders keep flooding in from the Strip, and three AI agents reason about every single one. Fleet Agent checks who's closest — those are real Google Maps API calls, each one a separate Temporal activity. Customer Agent evaluates priority. Dispatch Agent weighs both and assigns. The drivers run in their own child workflows — batch-picking up orders and delivering them in sequence. Everything you see in the Temporal UI is individually durable and replayable."

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

### Demo 3: Human-in-the-Loop (HITL) — Customer Change with Delivery Hold
**Time: 2–3 min | Best for: showing signals, wait_condition, and cross-workflow coordination**

**Setup:** Start deliveries. Wait for a driver to be en route to a hotel.

**Steps:**
1. In the Customer Change panel, pick an active order being delivered
2. Select "Cancel Order" or "Address Change" and click **Submit Change**
3. Watch the driver: it **arrives at the hotel but holds before delivering** — status shows `awaiting_update`. The parent workflow is waiting for your approval. The child workflow is waiting for the parent's decision. Two `wait_condition` pauses, both durable.
4. Meanwhile, everything else keeps running — other orders still come in, other drivers still deliver
5. Click **Approve**
6. **For cancel:** the driver skips delivery entirely and moves to its next order (or returns to Ziggy's)
7. **For address change:** the driver reroutes from the hotel to **The Cosmopolitan** — a new marker appears on the map, the order card updates
8. **For reject:** driver delivers normally to the original hotel

**What to say:**
> "The customer submitted a change, and look — the driver arrived at the hotel but it's holding. It won't deliver until we decide. That's two `wait_condition` pauses working together: the parent workflow is waiting for the human to approve, and the child workflow is waiting for the parent to tell it what to do. Meanwhile, the rest of Ziggy's system keeps running — other orders, other drivers, unaffected. Now we approve the cancel — and the driver skips delivery, no race condition, because delivery never started. Temporal held both workflows in that waiting state, fully durable."

**What you'll see in Temporal UI:**
- `meltdown-demo` workflow: `WorkflowExecutionSignaled` (`customer_change`) → `update_pending` signal sent to child → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → `resolve_update` signal sent to child
- `route-driver-X` workflow: `WorkflowExecutionSignaled` (`update_pending`) → driver holds with `awaiting_update` → `WorkflowExecutionSignaled` (`resolve_update`) → cancel skips `deliver_order` / reroute triggers new `navigate_to`
- Point out: *"Two workflows, both paused on wait_condition, both durable. The parent waits for the human. The child waits for the parent. No polling, no database checks, no timeout hacks."*

**Temporal concept to highlight:** Dual `wait_condition` (parent + child), cross-workflow signals, durable pause without polling

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
