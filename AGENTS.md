# Meltdown — Ice Cream Delivery Fleet Demo

> Instructions for AI coding agents working in this repo.

Conference demo for the AI Engineer World's Fair talk **"The Human Is an Async
API: Designing Durable Human-in-the-Loop Agents."** It shows **two** durable
human-in-the-loop patterns on Temporal, visualized as an ice cream delivery
fleet in **downtown San Francisco**:

- **Pattern A — Human-in-the-loop ("The Human Calls the Agent")** — built on
  **Google ADK** (multi-agent assignment). An operator submits a customer change
  (address change / cancel) mid-delivery; the driver holds at the venue; a human
  supervisor approves or rejects; the order reroutes / cancels / releases.
  Durable primitive: signal → `wait_condition` hold → resolve. **Variant (human →
  agent, in the reasoning loop):** a `human_revise_order` signal feeds an order
  revision back into the ADK assignment agent — `_reassign_via_adk` re-runs
  `_run_adk_assignment` so the agent RE-REASONS the assignment (re-checks the fleet,
  re-decides the driver) instead of applying a fixed change.
- **Pattern B — Agent-in-the-loop ("The Agent Calls the Human")** — built on
  **LangGraph** via `temporalio.contrib.langgraph`. The framework is chosen by the
  UI **tab** (`set_dispatch_mode` → `"langgraph"`), applying to all orders. The
  multi-agent team (Fleet ∥ Customer → Dispatch) runs INLINE in the parent
  workflow as a **looping ReAct team** — Fleet and Customer are real
  reason→act→eval loops, each Gemini reason call AND each tool call its own Temporal
  activity in the parent's history. The HITL is **in the loop**: mid-reasoning, the
  Dispatch or Fleet agent calls an `ask_human` tool, whose execution is a durable
  LangGraph `interrupt()` that suspends the graph. The parent
  (`_run_langgraph_assignment`) surfaces the question, parks on the `answer_dispatch`
  Temporal SIGNAL (`wait_condition`), and resumes the agent via `Command(resume=answer)`
  — the answer flows back as the agent's next observation. **No per-order gate child.**
  The thesis: the human is just another tool the agent calls — but a durable, async
  one; on Temporal that tool call is a signal.

The disconnect/recovery scenarios (agent disconnect, driver disconnect, tool
degradation) are **not** part of the talk's two demos. The underlying signals,
retry logic, and `degraded` flag still exist in the code (dormant; the UI no
longer surfaces disconnect controls), so they're documented below as mechanism,
not as demo use cases.

## How to run

```bash
./run.sh          # starts Temporal dev server + worker process + server process
```

`run.sh` starts three processes: Temporal dev server, worker (`python -m agent_fleet.worker`),
and FastAPI server (`python -m agent_fleet.server`). No manual Temporal setup needed. App is
served at http://localhost:8080; Temporal UI at http://localhost:8233.

The worker does **not** load `.env` itself. To run it directly in live mode, pass the env file:

```bash
uv run --env-file .env python -m agent_fleet.worker
```

The server loads `.env` via `load_dotenv()`. Two keys are required for live mode:
`GOOGLE_API_KEY` (Gemini) and `GOOGLE_MAPS_API_KEY` (Directions API).

## Architecture

- **Two separate processes**: FastAPI server (`server.py`) reads FleetState (SQLite) for the
  WebSocket snapshot and sends signals / runs queries only — no workers. Workers run in a
  separate process (`worker.py`). The worker is live-only and requires `GOOGLE_API_KEY`
  (no mock mode).
- **Workflows own state** (`workflows.py`): `MeltdownDemoWorkflow` owns driver positions, order
  assignments, and disconnect status. Builds `DriverSnapshot`s and passes to activities as inputs.
  Capacity guardrail: if ADK assigns to a full (3 orders) or disconnected driver, auto-reassigns
  to next available. Assignment then **spreads load across the fleet**: among eligible drivers
  (connected, under capacity) it prefers the least-loaded one so all 5 drivers stay active —
  the agents still reason and publish their assessment; this only rebalances the final
  destination. Orders assigned while Fleet Agent is offline get `degraded=True` flag.
  `DriverRouteWorkflow` is a per-driver child workflow — batch-picks up to 3 orders at Ziggy's,
  delivers sequentially (venue A → venue B → ...), then returns. Tracks status, is_disconnected,
  is_recovering, path_history, and current_orders. Disconnect uses Temporal-native retry: activities
  check FleetState for disconnect, fail if disconnected, Temporal retries with backoff until
  reconnected. Driver completes delivery, stays at venue, can't report back until reconnected.
  On reconnect, `sync_driver_position` activity reads actual position from FleetState — no
  teleporting. Completed deliveries are not repeated; batch continues from next pending order.
  HITL hold pattern: this is **operator-in-the-loop**, not agent-in-the-loop —
  the change is initiated externally (operator submits a customer change via REST)
  and a human supervisor approves it. The ADK agents never see the change; the
  gate lives in the workflow, not in any agent tool (contrast: an `ask_user`-style
  `@function_tool` where the LLM itself pauses for clarification). When the change
  is submitted, parent signals child with `update_pending` — driver navigates to
  the venue but holds before delivering (`awaiting_update` status, `wait_condition`).
  On approval, parent signals `resolve_update` with the decision: cancel → skip
  delivery, address_change → reroute to new destination, release → deliver
  normally. Two `wait_condition` patterns: parent waits for human, child waits for
  parent. For pending/batched orders, changes apply directly without hold.
  Customer changes process serially in the parent (`_drain_pending_signals`) —
  it's simpler and matches the demo flow (changes submitted one at a time).
  The child's HITL state is a **per-order dict** (`_pending_holds: dict[str,
  PendingHold]`): `update_pending` creates an entry, `resolve_update` fills
  in the decision for that specific order, and the delivery loop waits on
  the hold for the order it's currently processing. No single-slot overwrite
  — two changes for different orders on the same driver each get their own
  slot. `deliver_order` now returns `success=False` when a cancel wins the
  race, so the workflow skips the `order_delivered` parent signal for
  cancelled orders. The child's HITL hold also escapes on `self._stop` so
  demo shutdown can't leave a parked child hanging the parent's
  `await handle` join.
  `OrderGenerationWorkflow` is a child workflow that generates orders on a randomized timer and
  signals the parent. Parent handles assignment. Auto-generated orders top out at ~$1,950
  (servings ≤150 × ≤$13) and the agent only escalates genuinely high-value orders, so routine
  orders auto-dispatch — the agent calls `ask_human` only on the deliberately injected premium order.
- **Pattern B — in-loop `ask_human`** (`langgraph_agents.py`): the agent-in-the-loop path, selected
  by the langgraph tab for **all** orders. `_assign_order` runs `_run_langgraph_assignment` INLINE in
  the parent as a concurrent task — the fleet keeps moving while the agents (and possibly a human)
  decide. That assessment is a **looping multi-agent** LangGraph team (`build_dispatch_team_graph`,
  registered as `GRAPH_NAME = "dispatch_team"`) compiled via `temporalio.contrib.langgraph`, the
  mirror of the ADK team: Fleet and Customer are real reason→act→eval **ReAct loops** that fan out
  from `START`, then converge on a Dispatch loop. Each `*_reason` node is a real Gemini call (through
  `init_chat_model`) executed as a Temporal **activity** recorded in the **parent's** history, and
  **each tool call** (`get_fleet_status`, `get_route_info`, `get_order_priorities`) runs as its own
  Temporal activity inline in the workflow (the `*_act` nodes, `execute_in=workflow`), mirroring ADK's
  `activity_tool` granularity.
  **HITL is in the reasoning loop, not a boundary gate:** Fleet and Dispatch bind an `ask_human`
  tool and can call it mid-loop when they need outside sign-off (whether to ask is the agent's
  judgment, guided by `ESCALATION_GUIDANCE` / per-agent system prompts — there's no code threshold).
  Its execution is NOT an activity; the `*_human` node (`execute_in=workflow`) runs a durable
  LangGraph `interrupt()` that suspends the graph. The parent (`_run_langgraph_assignment`) loops on
  `result.get("__interrupt__")`: it surfaces the question into `_pending_dispatch`, `wait_condition`s
  on the `answer_dispatch` signal (via `_await_dispatch_answer`), then resumes the agent with
  `Command(resume=answer)` — the answer flows back as the agent's next observation. No per-order
  child workflow. On a `DISPATCH` decision the parent calls `_commit_assignment`; on `HOLD`/reject it
  calls `_reject_order` (cancels the order, preserves fleet capacity). `_await_dispatch_answer` also
  unblocks on `_routes_done` (returns `None`) so demo shutdown can't hang a parked workflow.
  LangGraph callables that run inline in the workflow are `async` because LangGraph offloads sync
  callables to a thread executor, which Temporal's deterministic event loop forbids.
- **Legacy dispatch gate** (`dispatch_gate.py`): `DispatchGateWorkflow` + a 1-node interrupt graph
  (`build_human_graph` / `GRAPH_NAME_HUMAN = "human_approval_interrupt"`) for a boundary HITL pause —
  the human via a durable Temporal `approve` signal (default, with timeout → `backup` approver tier)
  or a LangGraph `interrupt()` toggle (`config.INTERRUPT_MODE` / `DispatchGateInput.use_interrupt`),
  pending brief exposed via `pending_brief` query. This is the **old** Pattern B path and is **no
  longer used by the demo** — the in-loop `ask_human` flow above replaced it. The workflow is still
  registered (kept for spikes / the standalone `spikes/langgraph_hitl` path) but the demo never spawns
  a `gate-<order_id>` child. (`request_human_approval`, `escalate`, `_run_gate`, `_spawn_gate`, and
  `build_gate_graph` are gone.)
- **Server reads FleetState** (`server.py`): WebSocket data comes from `fleet.snapshot()` (SQLite).
  Server also writes disconnect/reconnect state directly. Temporal queries used for structural
  state during development — FleetState is the display authority.
- **Activities are pure** (`activities.py`): receive all decision data as inputs, never read
  FleetState for logic. `@activity.defn` with no `name=` override (function names are activity names).
- **FleetState** (`simulation.py`): SQLite WAL-backed UI projection. Backed by `fleet_state.db`
  for cross-process sharing — activities in the worker write positions/statuses, server reads
  for the frontend WebSocket. In production this would be Redis or Postgres.
- **3-queue workers** (`worker.py`): workflows + local activities, delivery, agents.
  `GoogleAdkPlugin` is on both workflow and agents workers (sandbox + determinism on
  workflow side, `invoke_model` activity on agents side). `LangGraphPlugin(graphs={...})` is
  on the **workflow** worker and registers **two** graphs: `GRAPH_NAME = "dispatch_team"` (the
  looping multi-agent team — Fleet ∥ Customer reason→act→eval loops → Dispatch, run inline in the
  parent for every langgraph-tab order, with the in-loop `ask_human` tool), and the legacy
  `GRAPH_NAME_HUMAN` (the 1-node interrupt pause for the unused-by-demo `DispatchGateWorkflow`).
  Their node activities (the fleet/customer/dispatch agent Gemini reason calls and each tool call)
  execute on that worker. `DispatchGateWorkflow` is still registered on the workflow worker
  alongside the others but is legacy/unused-by-demo. Agents use the upstream
  `TemporalModel` with `summary_fn=_build_summary` — `_build_summary`
  in `agents.py` generates context-aware summaries (agent name, order, phase) shown
  in the Temporal UI per invoke_model activity. `_activity_tool.py` builds its own
  dynamic summaries for tool-call activities from the bound arguments.
  `publish_agent_event` and `publish_agent_events_batch` are registered on the
  workflow worker for local activity execution (UI projection with minimal history).
- **ADK agents** (`agents.py`): all three kept — Fleet Agent + Customer Agent (parallel) →
  Dispatch Agent (sequential) — this is the multi-agent reasoning used for order assignment when
  the **adk tab** is selected (Pattern A's substrate). The langgraph tab routes every order to the
  Pattern B LangGraph team instead; the framework is chosen by the tab, not per-order. The ADK path
  runs inline in the workflow via `_run_adk_assignment()`, committing to the least-loaded driver
  with no dispatch gate involved. The same team is re-run for the **human → agent re-reason** path:
  a `human_revise_order` signal queues a revision that `_reassign_via_adk` drains, applying the edit
  and calling `_run_adk_assignment` again so the agent re-reasons the assignment. If an activity
  fails, Temporal retries. (Dormant disconnect path: Fleet
  Agent tools fail fast when disconnected (2 attempts), error returned to LLM via
  `_activity_tool.py` catch — Dispatch Agent assigns with available data but orders are flagged
  `degraded`.) Workflow publishes short summary events to FleetState via batched local activity
  after ADK completes (summary from `output_key` fields). Note: the Pattern B agent team is a
  **separate multi-agent LangGraph team** (`langgraph_agents.py`), not part of the ADK pipeline.
- **Server** (`server.py`): signal-only / query-only REST API plus the WebSocket state feed.
  Pattern A endpoints: `POST /api/customer-change` (signals parent `customer_change` + signals
  the child `update_pending` to hold), `POST /api/approve-change` (signals `change_approved`), and
  `POST /api/revise-order` (the in-loop re-reason variant — signals parent `human_revise_order`).
  Pattern B endpoints: `POST /api/inject-order` (registers a premium Moscone order in FleetState
  and signals `new_order` — the deliberate trigger for the agent's `ask_human`), `GET /api/pending-dispatch`
  (queries the parent's `get_status` and reads its `pending_dispatch` dict — populated when an agent
  calls `ask_human` mid-loop and the parent surfaces the question), `POST /api/approve-dispatch`
  (signals the parent `MeltdownDemoWorkflow.answer_dispatch` — the durable async endpoint the
  agent's in-loop `ask_human` interrupt is parked on; no gate child involved). Dormant disconnect
  endpoints (`/api/disconnect-crew`, `/api/disconnect-agent`, and reconnect variants) still write
  FleetState and signal workflows but are not wired to UI controls.
- **Frontend** (`frontend/index.html`): single-file SPA with Leaflet map, WebSocket state feed,
  agent reasoning panels.
- **PydanticPayloadConverter** on `Client.connect` in both server and worker for `LlmResponse`
  serialization.

## Key conventions

- Dataclass models for all Temporal payloads (`models.py`)
- Activities and workflows in separate files
- Worker is live-only and requires `GOOGLE_API_KEY` (no mock mode)
- Two API keys required: `GOOGLE_API_KEY` (Gemini, Generative Language API) and
  `GOOGLE_MAPS_API_KEY` (Directions API) — cannot be combined
- `DEFAULT_MODEL` defaults to `gemini-2.5-flash` (swappable via env)
- Geography is **downtown San Francisco** (`locations.py`). Random order generation from 3
  venues: **Moscone Center** (platinum tier — the premium target that makes the agent call `ask_human`),
  **Fisherman's Wharf** (silver), **Chinatown** (gold). The reroute-only destination is
  **Oracle Park** (`COSMOPOLITAN` — historical var name). The per-venue `hotel` key is a
  legacy field name; values are SF venue names.
- Drivers use letter IDs: `driver-a` through `driver-e`, displayed as `Driver-A` etc.
- Ice cream shop is "Ziggy's Ice Cream" = the **Ferry Building** (`WAREHOUSE_LABEL` in `locations.py`)
- Auto-generated orders top out at ~$1,950 and the agent escalates only genuinely high-value
  orders, so routine orders auto-dispatch; only the injected premium order makes the agent call `ask_human`
- Max 50 orders per demo run, drivers batch up to 3 orders (`DRIVER_CAPACITY`)

## Commands

Dependencies are managed with [uv](https://docs.astral.sh/uv/) — `uv sync --all-extras`
creates `.venv/` and installs runtime + dev deps. `uv run <cmd>` runs in that env.

```bash
uv sync --all-extras   # install / refresh deps (creates .venv/)
uv run ruff check .    # lint
uv run ruff format .   # format
uv run pytest          # run tests
make lint              # ruff check + format check (via uv)
make fmt               # ruff format (via uv)
make test              # pytest (via uv)
make run               # start the demo
```
