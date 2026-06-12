# Meltdown Demo Delivery Guide

Talk track and presenter notes for the AI Engineer World's Fair session
**"The Human Is an Async API: Designing Durable Human-in-the-Loop Agents"**
(Moscone West, San Francisco). The demo shows **two** human-in-the-loop patterns
on Temporal durable execution, visualized as Ziggy's Ice Cream catering fleet in
downtown San Francisco.

---

## Before You Start
(See [Quickstart](README.md#quick-start) for full setup instructions.)

**Requirements:**
- `.env` with two API keys (Google requires separate keys for Gemini vs Cloud APIs):
  - `GOOGLE_API_KEY` — Gemini key, restricted to Generative Language API. Required; the worker is live-only.
  - `GOOGLE_MAPS_API_KEY` — Maps key, restricted to Directions API.
- `./run.sh` (or `make run`) started — this starts the Temporal dev server, worker process, and server process automatically.
  - **Note:** the worker does **not** load `.env` itself. If you ever start it by hand, use `uv run --env-file .env python -m agent_fleet.worker`. `GOOGLE_API_KEY` is required; without it the worker logs a warning and LLM calls fail (no mock fallback).
- Browser open at http://localhost:8080 for the web app
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history and the worker-kill recovery)

## How it works
See [How It Works](HOW_IT_WORKS.md) for more detailed "under the hood" information.

## Pre-flight check
- Map shows **downtown San Francisco** with three delivery venues — **Moscone Center**, **Fisherman's Wharf**, **Chinatown** — and Ziggy's Ice Cream at the **Ferry Building**
- All 5 drivers (A–E) are parked at Ziggy's, status idle
- Two tabs at the top: **Human-in-the-loop** (Operator interrupt) and **Agent-in-the-loop** (Approval gate)
- "Start Deliveries" button is active on both tabs
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each pattern before presenting to get familiar with the agent reasoning panel timing and the approval-card flow.

---

## The Thesis (say this up front)

> "We keep designing human-in-the-loop as a special case — a pause, a webhook, a polling loop someone has to babysit. But there are really only two shapes. Sometimes a **human calls the agent**: they reach in and change something while the work is in flight. And sometimes the **agent calls the human**: it hits a decision it shouldn't make alone, and it asks. The trick is that the human is just another tool your agent calls — but a *durable, async* one. On Temporal, that tool call is a signal. Let me show you both, running on an ice cream fleet here in downtown San Francisco."

---

## Two technologies, one runtime

The demo deliberately uses **two different agent frameworks** on the **same** Temporal runtime, to make the point that the durable-HITL pattern is framework-agnostic.

- **Pattern A (Human-in-the-loop)** is built on **Google ADK** — a multi-agent assignment pipeline (Fleet + Customer Agents in parallel → Dispatch Agent).
- **Pattern B (Agent-in-the-loop)** is built on **LangGraph** via Temporal's `temporalio.contrib.langgraph` integration — a multi-agent team (Fleet + Customer → Dispatch) where the Dispatch agent decides whether to escalate.
- **The active tab picks the framework for *all* orders** — the dashboard signals `set_dispatch_mode` (`adk` or `langgraph`). There's no value threshold steering orders between them; whichever tab you're on dispatches every order.

### What is Google ADK? (30 seconds)

> "Google ADK is an open-source framework for composing multi-agent systems. You wire agents — each with their own tools and model — into pipelines that run sequentially or in parallel. In this demo a Fleet Agent assesses driver positions and capacity, a Customer Agent evaluates order priority and venue context, and a Dispatch Agent synthesizes both into an assignment. Each Gemini call and each tool call becomes its own Temporal activity — individually durable and replayable."

### What is the LangGraph integration? (30 seconds)

> "Pattern B uses LangGraph — a graph of nodes — running *inside* a Temporal workflow via `temporalio.contrib.langgraph`. It's a multi-agent team that mirrors the ADK side: a Fleet node and a Customer node assess in parallel, then a Dispatch node decides. Each node is a real Gemini call, run as its own Temporal activity. When the Dispatch agent decides an order needs sign-off, it calls a `request_human_approval` tool — and on Temporal, that tool call becomes a durable **signal**: the graph hands the escalation back to the workflow, which parks on a `wait_condition` until a human answers (or a timeout escalates to a backup approver). The draft they're approving is exposed through a query. Same durable primitive, completely different framework. LangGraph's own `interrupt()` can drive the pause instead — it's a back-pocket toggle (`HITL_MODE=interrupt`), but the talk leads with the Temporal-signal version."

---

## Architecture Talking Points

Optional drop-ins for mid-demo — when the conversation turns to scale or to what "production Temporal" actually looks like. Open the Temporal UI alongside the dashboard.

- **"Open the event history."** Click into any `route-driver-*` workflow and scroll. You'll see ~50–100 events per order: each Gemini call, each tool call, each navigation leg, each delivery. That's **per-call durability** — every one of those is an independent retry unit. In the LangGraph tab the agent-team activities (Fleet, Customer, Dispatch) run *inline* and are recorded right here in the parent workflow's history, not in a separate per-order child. Crash the worker mid-Dispatch-Agent and the Fleet Agent's earlier assessment is replayed from history, not re-called.
- **"Where are driver positions in the event log?"** They're not. `navigate_to` heartbeats position to shared state (SQLite here, Redis or Postgres in prod) every ~400ms. None of those writes hit Temporal. The pattern: signals for milestones (delivery complete, new order, human approval), shared state for continuous telemetry.
- **"What about the approval gate?"** Open the `gate-<order-id>` workflow while the approval card is up. The agent team already ran inline in the parent — this child does the **human pause only**: it receives a pre-built brief and parks on a `wait_condition` waiting for the `approve` signal (so `gate-*` children equal human approvals, not order count). The brief the human sees is surfaced via `/api/pending-dispatch`, which reads the parent workflow's `pending_dispatch` dict (the gate signals it up via `dispatch_gate_awaiting`) — not from any database the UI polls blindly. The gate also exposes its own `pending_brief` query.

Full breakdown lives in [HOW_IT_WORKS.md](HOW_IT_WORKS.md).

---

## Opening: Ziggy's Opens for Business
**Time: 1–2 min | Run this first on either tab**

**Setup:** Click **Start Deliveries**. Ziggy's kitchen starts taking orders. Venues around downtown place orders every few seconds — Moscone Center, Fisherman's Wharf, Chinatown.

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the ADK Agent Team panel.
2. Fleet Agent calls `tool_get_fleet_status` for driver positions and capacity, then `tool_get_route_info` for the closest drivers to get driving ETAs from Google Maps. Each ETA call is a separate Temporal activity.
3. Customer Agent calls `tool_get_order_priorities` and uses `google_search` (Gemini grounding) — evaluates VIP tier, deadline pressure, venue events, and guest count.
4. Dispatch Agent synthesizes both assessments and calls `tool_submit_assignment` — picks a driver and explains why.
5. The workflow **spreads load across the fleet** (least-loaded driver) so all five drivers stay active.
6. Drivers batch-pickup at Ziggy's (up to 3 orders per trip) and deliver sequentially to the venues.

**What to say:**
> "This is Ziggy's delivery system running live. Orders keep flooding in from downtown, and three AI agents reason about every single one. Fleet Agent checks who's closest — those are real Google Maps calls, each its own Temporal activity. Customer Agent evaluates priority. Dispatch Agent weighs both and assigns. Everything you see in the Temporal UI is individually durable and replayable."

**Temporal concept to highlight:** Child workflow isolation, continuous workflows with signals, per-call visibility in the event log.

---

## Pattern A — Human-in-the-Loop: "The Human Calls the Agent"
**Time: 2–3 min | Tab: Human-in-the-loop | Best for: signals, `wait_condition`, cross-workflow coordination**

This is **operator-initiated**: the change is submitted externally (an operator acting for the customer), and a human supervisor approves it. The ADK agents never see the change — the gate lives in the workflow, not in any agent tool. Contrast that with Pattern B, where the *agent* initiates the escalation.

**Setup:** On the **Human-in-the-loop** tab, click **Start Deliveries** and wait for a driver to be en route to a venue.

**Steps:**
1. In the order dropdown, pick an active order being delivered.
2. Select **Address Change** or **Cancel Order** and click **Submit Change**.
3. Watch the driver: it **arrives at the venue but holds before delivering** — status shows `awaiting_update`. The parent workflow is waiting for your approval; the child workflow is waiting for the parent's decision. Two `wait_condition` pauses, both durable.
4. Meanwhile, everything else keeps running — other orders still come in, other drivers still deliver.
5. Click **Approve** (or **Reject**):
   - **Cancel:** the driver skips delivery entirely and moves to its next order (or returns to Ziggy's).
   - **Address change:** the driver reroutes from the venue to **Oracle Park** — a new marker appears on the map, the order card updates.
   - **Reject:** the driver delivers normally to the original venue.

**What to say:**
> "An operator just changed this order, and look — the driver arrived but it's holding. It won't deliver until we decide. That's two `wait_condition` pauses working together: the parent waits for the human, the child waits for the parent. Meanwhile the rest of the fleet keeps running, unaffected. We approve the cancel — and the driver skips delivery, no race, because delivery never started. Temporal held both workflows in that waiting state, fully durable. No polling, no timeout hacks."

**What you'll see in the Temporal UI:**
- `meltdown-demo`: `WorkflowExecutionSignaled` (`customer_change`) → `update_pending` to child → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → `resolve_update` to child
- `route-driver-X`: `WorkflowExecutionSignaled` (`update_pending`) → driver holds `awaiting_update` → `WorkflowExecutionSignaled` (`resolve_update`) → cancel skips `deliver_order` / reroute triggers a new `navigate_to`

**Temporal concept to highlight:** Dual `wait_condition` (parent + child), cross-workflow signals, durable pause without polling.

---

## Pattern B — Agent-in-the-Loop: "The Agent Calls the Human"
**Time: 3–4 min | Tab: Agent-in-the-loop | Best for: the headline — the human as a durable async tool call, surviving worker death**

On the Agent-in-the-loop tab, **every order** runs a **multi-agent LangGraph team** inline in the parent workflow — Fleet and Customer nodes assess in parallel, then a Dispatch node decides — each node a real Gemini call run as a Temporal activity (recorded in the parent's own history). The Dispatch agent **decides for itself** whether to escalate, the way agents naturally express decisions: by calling a tool, `request_human_approval`. If it doesn't escalate, the order commits directly. If it does, the workflow spawns a per-order `gate-<order_id>` `DispatchGateWorkflow` child to do the **human pause only** — it gets a pre-built brief and parks on a durable Temporal **signal** + `wait_condition`; the draft brief is exposed via a **query**, and a timeout escalates to a backup approver. (LangGraph's own `interrupt()` can drive the pause instead — a back-pocket toggle via `HITL_MODE=interrupt`.) The agent only escalates genuinely high-value orders, and auto-generated orders top out around ~$1,950 (servings ≤150 × ≤$13), so routine orders auto-dispatch — the gate fires only when you drop the premium order.

**Setup:** On the **Agent-in-the-loop** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click **Drop high-value order**. This injects a premium **Moscone Center** catering order (well above the routine ~$1,950 cap) via `POST /api/inject-order`.
2. The LangGraph multi-agent team (Fleet + Customer → Dispatch) — running **inline in the parent workflow** — reasons about the value and fleet impact; the Dispatch agent **decides to call `request_human_approval`**. Only then does the parent spawn the durable `gate-<order_id>` child, which parks on a Temporal signal for the human pause.
3. An **approval card appears over the map** — "Agent is requesting human approval" — with the agent's reasoning, recommendation, order value, and fleet impact. The brief is surfaced via `GET /api/pending-dispatch`, which reads the parent workflow's `pending_dispatch` dict (populated when the gate child signals `dispatch_gate_awaiting`).
4. **The durability moment — kill the worker now.** While the card is up, stop the worker process (Ctrl-C in its terminal, or kill the `python -m agent_fleet.worker` process). The fleet freezes — but the *pending approval state is in Temporal, not in the worker's memory.*
5. **Restart the worker.** The fleet resumes and the approval card is still there, waiting. Nothing was lost. (Optionally show the parked `gate-<order-id>` workflow in the Temporal UI before and after — same `wait_condition`, resumed from history.)
6. Click **Approve dispatch** or **Reject** (`POST /api/approve-dispatch` signals `DispatchGateWorkflow.approve`):
   - **Approve:** the order commits to the proposed driver and the fleet delivers it.
   - **Reject:** the order is not dispatched — fleet capacity is preserved, the order shows as cancelled.

**What to say:**
> "Routine orders, the agents just dispatch. But this one's a big-ticket Moscone catering order — committing the fleet to it bumps other customers and it's costly to get wrong. So the agent does what agents do when they're unsure: it calls a tool. That tool is `request_human_approval`. Here's the thing — that's not a blocking function call. On Temporal it becomes a durable **signal**: the workflow parks on a `wait_condition` and waits for a human, for as long as it takes. Watch: I kill the worker. The agent's 'tool call' is still outstanding — but it's parked in Temporal's event log, not in a process that just died. I restart the worker… and the approval is still right here, waiting for me. The human is just another tool the agent calls — a durable, async one. Now I approve, and the fleet commits."

**Temporal concept to highlight:** Agent-initiated escalation mapped to a durable Temporal signal + `wait_condition` (default; `interrupt()` is the back-pocket toggle), query-backed draft, timeout → backup-approver escalation, **survives worker death**.

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a full execution model — retries, timeouts, backoff, heartbeating, child workflows, signals, queries — all in code, not config. The human pause in both patterns is just a `wait_condition` on a signal; the runtime holds it durably for as long as it takes."

**"Why two frameworks?"**
> "To show the pattern isn't tied to one. Pattern A is Google ADK, Pattern B is LangGraph. Different agent frameworks, same Temporal primitive: a human decision arrives as a durable async signal. The dispatch gate's model provider is even swappable via env — the pattern is model- and framework-agnostic."

**"What if Gemini returns something unexpected?"**
> "The ADK agents submit output via structured tool calls — `tool_submit_assignment` writes a typed object the workflow reads. The LangGraph agent's escalation is a tool call too. If a step produces garbage or fails, it's a Temporal activity, so it retries with backoff. There's a clear contract."

**"What happens if nobody approves the gate?"**
> "There's a timeout. The primary approver window elapses, the gate escalates to a backup approver tier, and it keeps waiting durably. The order isn't lost and the fleet isn't blocked — the gate runs concurrently while the rest of the fleet keeps delivering."

**"Is this production-ready?"**
> "The pattern is. Temporal runs at Stripe, Netflix, Uber. The integrations shown — `temporalio[google-adk]` and `temporalio.contrib.langgraph` — are the official ones."

---

## Reset Between Demos

1. Click **Reset** on the dashboard.
2. Verify all delivery actors return to idle at Ziggy's Ice Cream (Ferry Building).
3. If any workflows are stuck, run `temporal workflow list` and cancel manually (including any `gate-*` workflows).
4. Refresh the browser before the next run.
