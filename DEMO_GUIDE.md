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
- All 4 drivers (A–D) are parked at Ziggy's, status idle (capacity 2 each — a deliberately tight fleet so capacity pressure shows; driver-d stays hidden during the warm-up burst)
- **Two HITL patterns, three use cases (one per tab):** **🧑 Human → Agent** (use case 1 — Google ADK only, Pattern A), **🤖 Agent → Human** (use case 2 — LangGraph only, Pattern B), and **🔀 Cross-Framework · ADK + LangGraph** (use case 3 — both patterns, both frameworks). The third isn't a third pattern — it combines both.
- "Start Deliveries" button is active on all tabs
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each pattern before presenting to get familiar with the agent reasoning panel timing and the approval-card flow.

---

## The Thesis (say this up front)

> "We keep designing human-in-the-loop as a special case — a pause, a webhook, a polling loop someone has to babysit. But there are really only two shapes. Sometimes a **human calls into the system** and changes work already in flight. Sometimes the **agent calls the human** because it hits a decision it should not make alone. Both use the same durable wait-and-signal primitives. The key agent insight is in the second pattern: human judgment is a model-visible `ask_human` tool. LangGraph interrupts the loop, Temporal preserves the wait, and the human's answer arrives by signal as the agent's next observation. Let me show you both on an ice cream fleet here in downtown San Francisco."

---

## Two frameworks, one durable-execution runtime

The demo deliberately uses **two different agent frameworks** on the **same** durable-execution runtime (Temporal), to make the point that the durable-HITL pattern is framework-agnostic. **Frameworks own the loop — ADK, LangGraph. Temporal is the durable-execution runtime (the substrate) beneath them, and the only thing that coordinates across them.** The agent loop lives in each framework; durable execution is the layer they both run on. Temporal never runs the loop — the framework does, and each framework stops at its own edge.

- **Pattern A (Human-in-the-loop)** is built on **Google ADK** — a multi-agent assignment pipeline (Fleet + Customer Agents in parallel → Dispatch Agent). It starts outside the agent: a customer change enters the workflow by signal and makes the driver hold at the venue. On an approved address change, the new location becomes fresh input to the ADK team, which **re-reasons** before the held driver reroutes.
- **Pattern B (Agent-in-the-loop)** is built on **LangGraph** via Temporal's `temporalio.contrib.langgraph` integration — a looping multi-agent team (Fleet + Customer → Dispatch) where, mid-reasoning, the Dispatch agent calls the model-visible `ask_human` tool.
- **The active tab picks the framework for *all* orders** — the dashboard signals `set_dispatch_mode` (`adk`, `langgraph`, or `crossframework`). There's no value threshold steering orders between them; the active mode handles every order.

### What is Google ADK? (30 seconds)

> "Google ADK is an open-source framework for composing multi-agent systems. You wire agents — each with their own tools and model — into pipelines that run sequentially or in parallel. In this demo a Fleet Agent assesses driver positions and capacity, a Customer Agent evaluates order priority and venue context, and a Dispatch Agent synthesizes both into an assignment. Each Gemini call and each tool call becomes its own Temporal activity — individually durable and replayable."

### What is the LangGraph integration? (30 seconds)

> "Pattern B uses LangGraph — a graph of nodes — running *inside* the parent Temporal workflow via `temporalio.contrib.langgraph`. It's a looping multi-agent team that mirrors the ADK side: Fleet and Customer assess in parallel, then Dispatch decides, with each Gemini reason call recorded as a Temporal activity in the parent's history. Here's the headline: human judgment is **inside the reasoning loop as a tool**. Mid-reasoning, Dispatch calls `ask_human`; LangGraph's `interrupt()` suspends the graph, the parent waits durably in Temporal on `wait_condition`, and the human's answer arrives through the `answer_dispatch` signal. `Command(resume=answer)` returns it as the agent's next observation. There's no per-order gate child. Pattern A and B share the same Temporal wait/signal primitives, but only Pattern B exposes the human interaction as a model-visible tool."

**"How does this demo use LangGraph?"**
> "We use LangGraph as a *framework* here — its graph/loop abstraction for the Dispatch agent's reasoning loop — and let **Temporal** provide durability and persistence, so the LangGraph checkpointer here is just `InMemorySaver`."

---

## Architecture Talking Points

Optional drop-ins for mid-demo — when the conversation turns to scale or to what "production Temporal" actually looks like. Open the Temporal UI alongside the dashboard.

- **"Open the event history."** Open `meltdown-demo` to show the inline LangGraph team's Gemini and ordinary tool-call activities in the parent's history; open a `route-driver-*` child to show navigation and delivery activities. Each activity is an independent retry unit. Crash the worker mid-Dispatch-Agent and the Fleet Agent's completed assessment replays from history instead of being called again.
- **"Where are driver positions in the event log?"** They're not. `navigate_to` heartbeats position to shared state (SQLite here, Redis or Postgres in prod) every ~400ms. None of those writes hit Temporal. The pattern: signals for milestones (delivery complete, new order, human approval), shared state for continuous telemetry.
- **"Where does the agent's question to the human live?"** There's **no per-order gate child** — open the `meltdown-demo` parent workflow while the approval card is up. The looping LangGraph team ran inline in the parent; mid-reasoning the Dispatch agent called `ask_human`, and LangGraph's `interrupt()` suspended the graph. The parent surfaces that question into its `pending_dispatch` dict and waits durably in Temporal on the `answer_dispatch` signal + `wait_condition`. The brief the human sees is surfaced via `/api/pending-dispatch`, which reads the parent's `pending_dispatch` dict (via the `get_status` query) — not from any database the UI polls blindly.

Full breakdown lives in [HOW_IT_WORKS.md](HOW_IT_WORKS.md).

---

## Opening: Ziggy's Opens for Business
**Time: 1–2 min | Run this first on either tab**

**Setup:** Click **Start Deliveries**. Ziggy's kitchen starts taking orders. Venues around downtown place orders every few seconds — Moscone Center, Fisherman's Wharf, Chinatown.

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the ADK Agent Team panel.
2. Fleet Agent calls `tool_get_fleet_status` for driver positions and capacity, then `tool_get_route_info` for the closest drivers to get driving ETAs from Google Maps. Each ETA call is a separate Temporal activity.
3. Customer Agent calls `tool_get_order_priorities` and a venue-events web search (ADK: `google_search` Gemini grounding; LangGraph: `tool_search_venue_events`, the same grounding behind a tool) — evaluates VIP tier, deadline pressure, venue events, and guest count.
4. Dispatch Agent synthesizes both assessments and calls `tool_submit_assignment` — **picks the driver** (from the eligible, under-capacity set) and explains why.
5. The parent applies a **capacity guardrail** over the agent's pick (least-loaded eligible driver as the fallback if the pick is full/disconnected). With only 4 drivers at capacity 2, slots are genuinely scarce.
6. Drivers batch-pickup at Ziggy's (up to capacity, 2 orders per trip) and deliver sequentially to the venues.

**What to say:**
> "This is Ziggy's delivery system running live. Orders keep flooding in from downtown, and three AI agents reason about every single one. Fleet Agent checks who's closest — those are real Google Maps calls, each its own Temporal activity. Customer Agent evaluates priority. Dispatch Agent weighs both and assigns. Everything you see in the Temporal UI is individually durable and replayable."

**Temporal concept to highlight:** Child workflow isolation, continuous workflows with signals, per-call visibility in the event log.

---

## Pattern A — Human-in-the-Loop: "The Human Calls the Agent"
**Time: 2–3 min | Tab: Human-in-the-loop | Best for: signals, `wait_condition`, cross-workflow coordination**

This is **customer-initiated**: the change is submitted externally, and a human supervisor approves it. The gate lives in the workflow, not in any agent tool — but it's **one human gate that feeds both loops**: the driver holds, you approve, and on an address change the ADK agents **re-reason** the new location before the driver reroutes. Contrast that with Pattern B, where the *agent* initiates the escalation.

**Setup:** On the **Human-in-the-loop** tab, click **Start Deliveries** and wait for a driver to be en route to a venue.

**Steps:**
1. In the order dropdown, pick an active order being delivered.
2. Select **Address Change** (then pick a new SF location from the dropdown) or **Cancel Order**, and click **Submit Change**.
3. Watch the driver: it **arrives at the venue but holds before delivering** — status shows `awaiting_update`. The parent workflow is waiting for your approval; the child workflow is waiting for the parent's decision. Two `wait_condition` pauses, both durable.
4. Meanwhile, everything else keeps running — other orders still come in, other drivers still deliver.
5. Click **Approve** (or **Reject**):
   - **Cancel:** the driver skips delivery entirely and moves to its next order (or returns to Ziggy's). (Fixed cancel — no re-reason.)
   - **Address change:** the **ADK assignment team re-reasons** the order for the new location — Fleet recomputes ETAs, Customer re-reads priority, Dispatch reassesses (watch the agent panels update) — then the held driver reroutes from the venue to the chosen location; a new marker appears on the map, the order card updates.
   - **Reject:** the driver delivers normally to the original venue.

**What to say:**
> "A customer just changed this order, and look — the driver arrived but it's holding. It won't deliver until we decide. That's two `wait_condition` pauses working together: the parent waits for the human, the child waits for the parent. Now watch — I approve the new address, and the agents don't apply a script: they *re-reason* it. Fleet re-checks ETAs to the new spot, Customer re-weighs priority, Dispatch re-decides — and *then* the held driver reroutes. One approval feeds both loops: the agents re-reason, and the driver reroutes. Meanwhile the rest of the fleet keeps running, unaffected. Temporal held both workflows in that waiting state, fully durable. No polling, no timeout hacks."

**What you'll see in the Temporal UI:**
- `meltdown-demo`: `WorkflowExecutionSignaled` (`customer_change`) → `update_pending` to child → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → (address change) `_rereason_order` re-runs the ADK team → `resolve_update` to child
- `route-driver-X`: `WorkflowExecutionSignaled` (`update_pending`) → driver holds `awaiting_update` → `WorkflowExecutionSignaled` (`resolve_update`) → cancel skips `deliver_order` / reroute triggers a new `navigate_to`

**Temporal concept to highlight:** One human gate feeding both loops (agent re-reason + driver reroute), dual `wait_condition` (parent + child), cross-workflow signals, durable pause without polling.

The reroute choices come from a curated `REROUTE_OPTIONS` list (Oracle Park + Salesforce Tower, Union Square, Coit Tower, Palace of Fine Arts), served via `/api/locations` and shown in the **Address Change** dropdown.

---

## Pattern B — Agent-in-the-Loop: "The Agent Calls the Human"
**Time: 3–4 min | Tab: Agent-in-the-loop | Best for: the headline — human judgment as an async agent tool whose wait survives worker death**

On the Agent-in-the-loop tab, **every order** runs a **looping multi-agent LangGraph team** inline in the parent workflow — Fleet and Customer nodes assess in parallel, then a Dispatch node decides — each Gemini reason call runs as a Temporal activity recorded in the parent's own history. The HITL is **inside the reasoning loop**: **mid-reasoning**, the Dispatch agent **decides for itself** to escalate by calling the model-visible `ask_human` tool. LangGraph's `interrupt()` suspends the graph. The parent workflow (`_run_langgraph_assignment`) surfaces the question, waits durably in Temporal on the `answer_dispatch` signal + `wait_condition`, and resumes the agent with `Command(resume=answer)` — the human's answer flows back as the agent's *next observation*. There is **no per-order gate child**. If the agent doesn't escalate, the order commits directly. The agent only escalates genuinely exceptional orders, and auto-generated orders top out around ~$1,950 (servings ≤150 × ≤$13), so routine orders auto-dispatch — the approval card fires only when you drop the premium order.

**Setup:** On the **Agent-in-the-loop** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click **Drop high-value order**. This injects a premium **Moscone Center** catering order (well above the routine ~$1,950 cap) via `POST /api/inject-order`.
2. The looping LangGraph multi-agent team (Fleet + Customer → Dispatch) — running **inline in the parent workflow** — assesses the value and fleet impact; **mid-reasoning** the Dispatch agent **calls the `ask_human` tool**. LangGraph interrupts the graph, and the parent waits durably in Temporal for the `answer_dispatch` signal — no child workflow spawned.
3. An **approval card appears over the map** — "Agent called `ask_human`" — with the agent's question, order value, and the reminder that your answer returns as its next observation. The brief is surfaced via `GET /api/pending-dispatch`, which reads the parent workflow's `pending_dispatch` dict (populated when the agent's `ask_human` interrupt fires).
4. **The durability moment — kill the worker now.** While the card is up, from a **second terminal** run **`make kill-worker`** (leave `make run` going in the first — that keeps Temporal + the web server alive). The fleet freezes — but the *pending question is in Temporal, not in the worker's memory.* (Don't Ctrl-C `make run`; that tears down Temporal too and wipes the in-memory dev-server state.)
5. **Restart the worker** with **`make worker`**. It replays from Temporal's history — the fleet resumes and the approval card is still there, waiting. Nothing was lost. (Optionally show the parked `meltdown-demo` parent workflow in the Temporal UI before and after — same `wait_condition` on `answer_dispatch`, resumed from history. No `gate-*` child to look for.) For an even stronger beat, **approve while the worker is down** — the signal is durably recorded by Temporal with no worker present, and applied on restart.
6. Click **Approve dispatch** or **Reject** (`POST /api/approve-dispatch` signals `MeltdownDemoWorkflow.answer_dispatch`):
   - **Approve:** the answer flows back as the agent's next observation; the agent **reasons over that approval plus the Fleet/Customer assessments and picks the driver** (`submit_dispatch`) — it's not a rubber stamp — then the fleet delivers it.
   - **Reject:** the answer flows back as a reject; the agent holds the order — fleet capacity is preserved, the order shows as cancelled.

**What to say:**
> "Routine orders, the agents just dispatch. But this one's a big-ticket Moscone catering order, and committing scarce capacity deserves human judgment. So the agent does what agents do mid-reasoning: it calls a tool, `ask_human`. LangGraph interrupts the loop. Temporal parks the parent on a durable `wait_condition` until the human's answer arrives by signal. Watch: I kill the worker. The tool call is still outstanding, but the wait lives in Temporal's event history, not in the process that just died. I restart the worker, and the question is still waiting. Human judgment is an async tool the agent can call. Now I approve; that answer returns as the agent's next observation, and it reasons before committing the fleet."

**Temporal concept to highlight:** Agent-initiated escalation **inside the reasoning loop**: `ask_human` → LangGraph `interrupt()` → Temporal `wait_condition` → human answer via `answer_dispatch` signal → `Command(resume=answer)` → next agent observation. Query-backed brief, **no per-order child**, **survives worker death**.

**Why `interrupt()` and not just the signal?** Because this HITL lives *inside* the loop, the human's answer has to flow **back into the running graph** as the agent's next observation — and `interrupt()` is the only LangGraph primitive that can suspend and resume a graph **mid-node** and inject that answer via `Command(resume=answer)`. There's **no "signal-only, no interrupt" option** for the in-loop pattern: the `answer_dispatch` signal + `wait_condition` is the durable *wait*, but `interrupt()` is the graph plumbing that lets the answer rejoin the loop.

---

## Cross-Framework — Temporal WITH ADK and LangGraph: "One Runtime Across Two Frameworks"
**Time: 3–4 min | Tab: 🔀 Cross-Framework · ADK + LangGraph | Best for: the cross-framework point — Temporal joining work no single agent framework can coordinate**

This tab runs both frameworks **at once, on the same delivery**. Fleet and Customer assessment runs on **Google ADK**; Dispatch runs on **LangGraph** — each its own Temporal **child workflow**, joined by the Temporal parent. The header shows **both the Google ADK and LangGraph logos** to make the point visible: no agent framework can coordinate across another framework; **only Temporal can**. Both HITL directions appear here — the agent-initiated `ask_human` tool and interrupt (LangGraph), plus the externally initiated address-change flow that re-runs ADK and LangGraph after approval — across the cross-framework boundary.

**Setup:** On the **🔀 Cross-Framework** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click the **🔀 Cross-Framework** tab, then **Start Deliveries**. Orders flow as before, but each one fans out into two child workflows — ADK for assessment, LangGraph for dispatch.
2. **(Agent → human direction.)** Click **Drop high-value order**. The **LangGraph Dispatch agent calls `ask_human` mid-reasoning** and an **approval card appears over the map**. The answer signals the **dispatch agent's OWN child workflow**: **Approve** → dispatched; **Reject** → held (not dispatched).
3. **(Human → agent direction.)** Use the customer-change controls: pick an order, select **Address Change** → a new location, click **Submit Change**, then **Approve**. The driver holds, the **cross-framework team re-reasons** — **ADK reassesses, LangGraph re-decides** — and the driver reroutes.
4. Click **View the cross-framework graph** to show the combined diagram: Temporal parent → ADK child [Fleet ∥ Customer] + LangGraph child [Dispatch + `ask_human`] → driver loop.
5. Open the **Temporal UI** (localhost:8233). Per cross-framework order there are **separate child workflow histories** — `assess-<order>` (ADK) and `dispatch-<order>` (LangGraph) — under `meltdown-demo`. That split is the **visible cross-framework boundary**.

**What to say:**
> "So far each tab used one framework. This one uses both — on the same order. Fleet and Customer assess on Google ADK; Dispatch decides on LangGraph; each is its own Temporal child workflow, and Temporal joins them. Here's the point: no agent framework can reach across another framework and coordinate it — only the durable-execution runtime can. Watch both patterns we just saw, now spanning the boundary. The LangGraph dispatch agent calls `ask_human` and escalates — I approve, and it signals dispatch's own child workflow. And when a customer changes an address, the whole cross-framework team re-reasons — ADK reassesses the new location, LangGraph re-decides — then the driver reroutes. Temporal WITH ADK and LangGraph: one durable-execution runtime, two frameworks, joined durably."

**What you'll see in the Temporal UI:**
- Under `meltdown-demo`, per cross-framework order: a child workflow `assess-<order>` (the ADK Fleet + Customer assessment) and a child workflow `dispatch-<order>` (the LangGraph Dispatch decision, including its in-loop `ask_human` interrupt). The two separate histories are the cross-framework boundary made visible.

**Temporal concept to highlight:** Cross-framework coordination via separate Temporal child workflows (`assess-<order>` ADK + `dispatch-<order>` LangGraph) joined by the parent — coordination across agent frameworks that no single agent framework can do; both HITL directions (agent→human `ask_human`, human→agent re-reason) spanning the cross-framework boundary.

**Operational note:** After any code change, **terminate the `meltdown-demo` workflow (Reset) and restart the worker** — otherwise stale child histories fail to replay.

### Cross-Framework code tour (for "show me the code")

In the order the flow happens (symbol names are stable; line numbers are approximate hints):

1. **Where the graph is defined** — `build_dispatch_only_graph()` in `agent_fleet/langgraph_agents.py` (~L629): `START → dispatch_reason → {dispatch_human → dispatch_reason | END}`. Name: `DISPATCH_ONLY_GRAPH_NAME = "dispatch_only"` (~L63); human node wired at ~L649.
2. **The prompt that makes the agent escalate** — `ESCALATION_GUIDANCE` in `langgraph_agents.py` (~L84); the "call `ask_human` …" instruction is ~L90–94.
3. **Where the agent asks the human** — the tool `_ask_human_tool()` / `ask_human(question)` (~L318, body is `raise NotImplementedError`), and `interrupt(...)` inside `_human_node` (~L296) — that payload is the question. LangGraph suspends here; the Temporal wait in step 5 supplies durability.
4. **The query that loads the human's question box** — `LgDispatchWorkflow.pending_question` query in `agent_fleet/workflows.py` (~L1020); the interrupt payload is captured into `self._pending_question` at ~L1051. (`/api/pending-dispatch` reads this child query to render the card.)
5. **The durable wait** — `await workflow.wait_condition(...)` in `LgDispatchWorkflow.run` (`workflows.py` ~L1052).
6. **The signal that resumes (cross-framework)** — `LgDispatchWorkflow.answer_dispatch` (~L1009); the human signals the dispatch **child** directly (no order_id — the child *is* the order), which unblocks the wait and the graph resumes via `Command(resume=answer)` (~L1066).

**How the pause/resume works.** `interrupt(payload)` checkpoints the graph and returns from `ainvoke()` with the payload in `result["__interrupt__"]` — the graph is suspended at that node. Calling the graph again with `Command(resume=answer)` restores it and the `interrupt()` call *returns* `answer`, so the node finishes and the edge `dispatch_human → dispatch_reason` makes the Dispatch agent **re-reason over the human's answer — on approve *and* reject**. A reject still re-reasons, but the workflow's `rejected` flag forces a final HOLD regardless of what the agent concludes. LangGraph gives the pause/resume; **Temporal makes the gap durable** — the checkpointer is `InMemorySaver` (scratch); the signal + `wait_condition` + re-invoke live in Temporal's event history, so a worker kill mid-pause loses nothing.

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a full execution model — retries, timeouts, backoff, heartbeating, child workflows, signals, queries — all in code, not config. The human pause in both patterns is just a `wait_condition` on a signal; the durable-execution runtime holds it durably for as long as it takes."

**"Why two frameworks?"**
> "To show the durable human-interaction primitives aren't tied to one framework. Pattern A starts outside the agent: human input enters the workflow by signal and, for an approved address change, becomes fresh context for ADK re-reasoning. Pattern B starts inside the LangGraph loop: the agent calls the model-visible `ask_human` tool and receives the answer as its next observation. Same Temporal wait/signal primitives; different initiator and continuation."

**"Aren't these agent loops pretty shallow?"**
> "Yes — deliberately. They're real reason→act→observe loops, and every reason call and ordinary tool call is its own Temporal activity; `ask_human` is the workflow-interrupt exception. The loops are only a few hops deep because picking a driver is a bounded task. Loop depth is orthogonal to the point: a shallow loop still fires `ask_human` mid-reasoning and proves the pause is durable, and a 10×-deeper loop would run the exact same contract with more steps to replay. I kept them shallow so the demo stays legible; depth is a knob, not a missing piece."

**"Why does the LangGraph code look so much heavier than the ADK code?"**
> "Because in LangGraph **you own the loop**. `langgraph_agents.py` hand-builds it from primitives — the reason↔act loop and routing, per-tool-call activities (`_run_tools`), message parsing (`_coerce_text` / `_last_text`), the `interrupt()` human node (`_human_node`), and model + tool binding (`_chat_model`). **ADK doesn't need any of that**: its `Runner` runs the loop. (Temporal never runs the loop — the framework does, in both cases.) `TemporalModel` + `activity_tool` make each model call and each tool call a durable Temporal activity, and structured output comes back through session state. So it's the same durable-execution runtime underneath — LangGraph just exposes more of the plumbing. **LangGraph = assemble the loop from primitives; ADK = batteries-included.**"

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
