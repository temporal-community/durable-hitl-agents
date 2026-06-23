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
- Two tabs at the top: **🧑 Human → Agent** (Human-initiated) and **🤖 Agent → Human** (Agent-initiated)
- "Start Deliveries" button is active on both tabs
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each pattern before presenting to get familiar with the agent reasoning panel timing and the approval-card flow.

---

## The Thesis (say this up front)

> "We keep designing human-in-the-loop as a special case — a pause, a webhook, a polling loop someone has to babysit. But there are really only two shapes. Sometimes a **human calls the agent**: they reach in and change something while the work is in flight. And sometimes the **agent calls the human**: it hits a decision it shouldn't make alone, and it asks. The trick is that the human is just another tool your agent calls — but a *durable, async* one. On Temporal, that tool call is a signal. Let me show you both, running on an ice cream fleet here in downtown San Francisco."

---

## Two technologies, one runtime

The demo deliberately uses **two different agent frameworks** on the **same** Temporal runtime, to make the point that the durable-HITL pattern is framework-agnostic.

- **Pattern A (Human-in-the-loop)** is built on **Google ADK** — a multi-agent assignment pipeline (Fleet + Customer Agents in parallel → Dispatch Agent). It has two flavors: an operator change that gates the *delivery* loop (driver holds at the venue), and a human revision that lands *inside the agent's reasoning loop* — the assignment agent re-reasons the driver.
- **Pattern B (Agent-in-the-loop)** is built on **LangGraph** via Temporal's `temporalio.contrib.langgraph` integration — a looping multi-agent team (Fleet + Customer → Dispatch) where, mid-reasoning, the Dispatch agent calls an `ask_human` tool to escalate.
- **The active tab picks the framework for *all* orders** — the dashboard signals `set_dispatch_mode` (`adk` or `langgraph`). There's no value threshold steering orders between them; whichever tab you're on dispatches every order.

### What is Google ADK? (30 seconds)

> "Google ADK is an open-source framework for composing multi-agent systems. You wire agents — each with their own tools and model — into pipelines that run sequentially or in parallel. In this demo a Fleet Agent assesses driver positions and capacity, a Customer Agent evaluates order priority and venue context, and a Dispatch Agent synthesizes both into an assignment. Each Gemini call and each tool call becomes its own Temporal activity — individually durable and replayable."

### What is the LangGraph integration? (30 seconds)

> "Pattern B uses LangGraph — a graph of nodes — running *inside* the parent Temporal workflow via `temporalio.contrib.langgraph`. It's a looping multi-agent team that mirrors the ADK side: a Fleet node and a Customer node assess in parallel, then a Dispatch node decides — each node a real Gemini call run as its own Temporal activity, recorded in the parent's history. Here's the headline: the HITL is **inside the reasoning loop**. Mid-reasoning, the Dispatch agent calls an `ask_human` tool. That tool's execution is a durable LangGraph `interrupt()` that suspends the graph; the parent workflow surfaces the question, parks on the `answer_dispatch` Temporal **signal** + `wait_condition` until a human answers, then resumes the agent via `Command(resume=answer)` — so the human's answer flows back as the agent's *next observation*. There's no per-order gate child; the pause lives inside the agent's loop. Same durable primitive as Pattern A, completely different framework."

---

## Architecture Talking Points

Optional drop-ins for mid-demo — when the conversation turns to scale or to what "production Temporal" actually looks like. Open the Temporal UI alongside the dashboard.

- **"Open the event history."** Click into any `route-driver-*` workflow and scroll. You'll see ~50–100 events per order: each Gemini call, each tool call, each navigation leg, each delivery. That's **per-call durability** — every one of those is an independent retry unit. In the LangGraph tab the agent-team activities (Fleet, Customer, Dispatch) run *inline* and are recorded right here in the parent workflow's history, not in a separate per-order child. Crash the worker mid-Dispatch-Agent and the Fleet Agent's earlier assessment is replayed from history, not re-called.
- **"Where are driver positions in the event log?"** They're not. `navigate_to` heartbeats position to shared state (SQLite here, Redis or Postgres in prod) every ~400ms. None of those writes hit Temporal. The pattern: signals for milestones (delivery complete, new order, human approval), shared state for continuous telemetry.
- **"Where does the agent's question to the human live?"** There's **no per-order gate child** — open the `meltdown-demo` parent workflow while the approval card is up. The looping LangGraph team ran inline in the parent; mid-reasoning the Dispatch agent called `ask_human`, which suspended the graph on a durable `interrupt()`. The parent surfaces that question into its `pending_dispatch` dict and parks on the `answer_dispatch` signal + `wait_condition` — that single durable pause is the whole HITL. The brief the human sees is surfaced via `/api/pending-dispatch`, which reads the parent's `pending_dispatch` dict (via the `get_status` query) — not from any database the UI polls blindly.

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

### Variant — Human → Agent, in the *reasoning* loop

The flow above gates the *delivery* loop: the change is fixed and a human only says yes/no. This second beat puts the human inside the *agent's* loop — a human revises an order (new location / details) and the ADK assignment agent **re-reasons** how to adjust, instead of the system applying a canned change. Same Pattern A ("the human calls the agent"), but the human's edit is now input the agent reasons over.

- The revision arrives as the `human_revise_order` signal (`POST /api/revise-order`).
- That hands the revised order back to the **ADK assignment team** (`_reassign_via_adk` → `_run_adk_assignment`): Fleet → Customer → Dispatch run **again** over the change.
- The agent **re-decides**: re-check the fleet, then either reassign to a better driver (pull from the old driver, give to the new one), keep the same driver and push the new destination into its delivery loop, or assign fresh if it wasn't on a driver yet.

**What to say:**
> "First time, the operator changed the order and a human just approved a fixed action. Watch this one — I revise the order, and the agents don't apply a script. They re-reason it: Fleet re-checks who's free, Customer re-weighs priority, Dispatch re-decides the driver. The human's edit is just new input the agent reasons over — that's the human *in the agent's loop*, still on the same durable Temporal signal."

> **How to trigger it:** on the Human → Agent tab, pick an order and click **↻ Revise → agent re-reasons** (next to **Submit Change**). It moves the order to a new venue and re-runs the ADK assignment team (`/api/revise-order` → `human_revise_order`). **Submit Change** still drives the separate gate-the-delivery flow (driver holds, human approves).

---

## Pattern B — Agent-in-the-Loop: "The Agent Calls the Human"
**Time: 3–4 min | Tab: Agent-in-the-loop | Best for: the headline — the human as a durable async tool call, surviving worker death**

On the Agent-in-the-loop tab, **every order** runs a **looping multi-agent LangGraph team** inline in the parent workflow — Fleet and Customer nodes assess in parallel, then a Dispatch node decides — each node a real Gemini call run as a Temporal activity (recorded in the parent's own history). The HITL is **inside the reasoning loop**: **mid-reasoning**, the Dispatch agent **decides for itself** to escalate, the way agents naturally express decisions — by calling a tool, `ask_human`. That tool's execution is a durable LangGraph `interrupt()` that **suspends the graph**. The parent workflow (`_run_langgraph_assignment`) surfaces the question into its `pending_dispatch` dict, parks on the `answer_dispatch` Temporal **signal** + `wait_condition`, and resumes the agent with `Command(resume=answer)` — the human's answer flows back as the agent's *next observation*. There is **no per-order gate child**; the pause lives inside the agent's loop. If the agent doesn't escalate, the order commits directly. The agent only escalates genuinely high-value orders, and auto-generated orders top out around ~$1,950 (servings ≤150 × ≤$13), so routine orders auto-dispatch — the approval card fires only when you drop the premium order.

**Setup:** On the **Agent-in-the-loop** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click **Drop high-value order**. This injects a premium **Moscone Center** catering order (well above the routine ~$1,950 cap) via `POST /api/inject-order`.
2. The looping LangGraph multi-agent team (Fleet + Customer → Dispatch) — running **inline in the parent workflow** — assesses the value and fleet impact; **mid-reasoning** the Dispatch agent **calls the `ask_human` tool**. That suspends the graph on a durable `interrupt()`, and the parent parks on the `answer_dispatch` signal — no child workflow spawned.
3. An **approval card appears over the map** — "Agent is requesting human approval" — with the agent's question, reasoning, recommendation, order value, and fleet impact. The brief is surfaced via `GET /api/pending-dispatch`, which reads the parent workflow's `pending_dispatch` dict (populated when the agent's `ask_human` interrupt fires).
4. **The durability moment — kill the worker now.** While the card is up, stop the worker process (Ctrl-C in its terminal, or kill the `python -m agent_fleet.worker` process). The fleet freezes — but the *pending question is in Temporal, not in the worker's memory.*
5. **Restart the worker.** The fleet resumes and the approval card is still there, waiting. Nothing was lost. (Optionally show the parked `meltdown-demo` parent workflow in the Temporal UI before and after — same `wait_condition` on `answer_dispatch`, resumed from history. No `gate-*` child to look for.)
6. Click **Approve dispatch** or **Reject** (`POST /api/approve-dispatch` signals `MeltdownDemoWorkflow.answer_dispatch`):
   - **Approve:** the answer flows back into the agent's reasoning; the order commits to the proposed driver and the fleet delivers it.
   - **Reject:** the answer flows back as a reject; the order is held — fleet capacity is preserved, the order shows as cancelled.

**What to say:**
> "Routine orders, the agents just dispatch. But this one's a big-ticket Moscone catering order — committing the fleet to it bumps other customers and it's costly to get wrong. So the agent does what agents do when they're unsure, right in the middle of reasoning: it calls a tool. That tool is `ask_human`. Here's the thing — that's not a blocking function call. Its execution is a LangGraph `interrupt()` that suspends the graph, and on Temporal the pause becomes a durable **signal**: the parent workflow parks on a `wait_condition` and waits for a human, for as long as it takes. Watch: I kill the worker. The agent's 'tool call' is still outstanding — but it's parked in Temporal's event log, not in a process that just died. I restart the worker… and the question is still right here, waiting for me. The human is just another tool the agent calls — a durable, async one. Now I approve, the answer flows back as the agent's next observation, and it commits the fleet."

**Temporal concept to highlight:** Agent-initiated escalation **inside the reasoning loop** (`ask_human` → LangGraph `interrupt()`) mapped to a durable Temporal signal (`answer_dispatch`) + `wait_condition`, resumed via `Command(resume=answer)`, query-backed brief, **no per-order child**, **survives worker death**.

**Why `interrupt()` and not just the signal?** Because this HITL lives *inside* the loop, the human's answer has to flow **back into the running graph** as the agent's next observation — and `interrupt()` is the only LangGraph primitive that can suspend and resume a graph **mid-node** and inject that answer via `Command(resume=answer)`. There's **no "signal-only, no interrupt" option** for the in-loop pattern: the `answer_dispatch` signal + `wait_condition` is the durable *wait*, but `interrupt()` is the graph plumbing that lets the answer rejoin the loop.

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a full execution model — retries, timeouts, backoff, heartbeating, child workflows, signals, queries — all in code, not config. The human pause in both patterns is just a `wait_condition` on a signal; the runtime holds it durably for as long as it takes."

**"Why two frameworks?"**
> "To show the pattern isn't tied to one. Pattern A is Google ADK, Pattern B is LangGraph. Different agent frameworks, same Temporal primitive: a human decision arrives as a durable async signal. In both, the human is a tool the agent's loop reasons over — the pattern is model- and framework-agnostic."

**"Why does the LangGraph code look so much heavier than the ADK code?"**
> "Because in LangGraph **you own the loop**. `langgraph_agents.py` hand-builds it from primitives — the reason↔act loop and routing, per-tool-call activities (`_run_tools`), message parsing (`_coerce_text` / `_last_text`), the `interrupt()` human node (`_human_node`), and model + tool binding (`_chat_model`). **ADK doesn't need any of that**: its `Runner` bakes the loop in. `TemporalModel` + `activity_tool` run the reason→act→observe cycle and tool-calls-as-activities for you, and structured output comes back through session state. So it's the same durable contract underneath — LangGraph just exposes more of the plumbing. **LangGraph = assemble the loop from primitives; ADK = batteries-included.**"

**"What if Gemini returns something unexpected?"**
> "The ADK agents submit output via structured tool calls — `tool_submit_assignment` writes a typed object the workflow reads. The LangGraph agent's escalation is a tool call too (`ask_human`). If a step produces garbage or fails, it's a Temporal activity, so it retries with backoff. There's a clear contract."

**"What happens if nobody answers the agent?"**
> "Nothing is lost. The agent's `ask_human` is parked on a durable `wait_condition` in the parent workflow — it keeps waiting for as long as it takes, surviving worker restarts. And the rest of the fleet keeps delivering, because the agent's reasoning task runs concurrently — it doesn't block the parent."

**"Is this production-ready?"**
> "The pattern is. Temporal runs at Stripe, Netflix, Uber. The integrations shown — `temporalio[google-adk]` and `temporalio.contrib.langgraph` — are the official ones."

---

## Reset Between Demos

1. Click **Reset** on the dashboard.
2. Verify all delivery actors return to idle at Ziggy's Ice Cream (Ferry Building).
3. If any workflows are stuck, run `temporal workflow list` and cancel manually (`meltdown-demo`, `order-generation`, `route-driver-*`).
4. Refresh the browser before the next run.
