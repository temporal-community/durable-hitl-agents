"""Pattern B — agent-led HITL, the Temporal way (with a LangGraph-interrupt fallback).

A **multi-agent** LangGraph graph (mirroring the ADK Fleet / Customer / Dispatch team) runs
on `temporalio.contrib.langgraph`. Fleet and Customer agents assess the order in parallel;
the Dispatch agent weighs both and DECIDES whether to call the `request_human_approval`
tool before committing scarce fleet capacity. Each agent node is a real Gemini call
executed as a Temporal **activity** — this is how you'd build the ADK team's equivalent in
LangGraph on our integration.

Two HITL implementations, chosen per-order via `DispatchGateInput.use_interrupt`
(default False = the Temporal pattern):

- **Temporal (default):** the agent's tool call surfaces an `escalate` flag + brief; the
  graph ends, and the **workflow** performs the human-in-the-loop with a Temporal SIGNAL +
  `wait_condition` (+ timeout → backup approver). The human is a durable Temporal signal —
  no LangGraph interrupt needed. This is the Temporal-native pattern the talk leads with.
- **Interrupt (back-pocket toggle):** a workflow-node calls LangGraph `interrupt(brief)`;
  the workflow resumes via `Command(resume=...)`. Same durability, LangGraph's mechanism.

Callables that run inline in the workflow (the interrupt node, conditional edges) are
`async` because LangGraph offloads *sync* callables to a thread executor, which Temporal's
deterministic event loop forbids.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, TypedDict

from temporalio import workflow

from agent_fleet.models import DispatchGateInput, DispatchGateResult, PublishAgentEventInput

with workflow.unsafe.imports_passed_through():
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import Command, interrupt
    from temporalio.contrib.langgraph import graph

    from agent_fleet.activities import publish_agent_events_batch

GRAPH_NAME = "dispatch_gate"
GRAPH_NAME_INTERRUPT = "dispatch_gate_interrupt"  # interrupt-mode variant (has the interrupt node)
# Demo path only: a 1-node interrupt graph for the HITL pause AFTER the multi-agent
# assessment has already run inline in the parent (so the agent team is not re-invoked).
GRAPH_NAME_HUMAN = "human_approval_interrupt"

ESCALATION_GUIDANCE = (
    "You are the Dispatch agent for an ice cream catering fleet. You have assessments from "
    "the Fleet agent and the Customer agent below.\n\n"
    "Routine catering orders run up to about $2,000, and the fleet absorbs them automatically. "
    "For those, reply DISPATCH and do NOT call the tool — even if drivers are momentarily busy. "
    "Tight capacity by itself is normal operations, NOT a reason to escalate.\n\n"
    "Only call request_human_approval for an EXCEPTIONAL order — roughly $3,000 or more, or a "
    "major VIP commitment — where committing scarce fleet capacity genuinely warrants a "
    "supervisor's sign-off before you commit. When you do, give a clear reason and your "
    "recommendation. Otherwise, reply DISPATCH."
)


class GateState(TypedDict, total=False):
    order_id: str
    venue: str
    order_value: int
    servings: int
    deadline_minutes: int
    proposed_driver_id: str
    drivers_available: int
    drivers_total: int
    pending_orders: int
    # produced by the agent team
    fleet_assessment: str
    customer_assessment: str
    fleet_impact: str
    escalate: bool
    tool_args: dict[str, Any]
    brief: dict[str, Any]
    decision: str
    approved: bool


def _fleet_impact(state: GateState) -> str:
    return (
        f"{state['drivers_available']}/{state['drivers_total']} drivers free · "
        f"{state['pending_orders']} pending — commits a slot ahead of other customers."
    )


def _build_brief(state: GateState, tool_args: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": state["order_id"],
        "venue": state["venue"],
        "order_value": state["order_value"],
        "servings": state["servings"],
        "deadline_minutes": state["deadline_minutes"],
        "proposed_driver_id": state["proposed_driver_id"],
        "fleet_impact": state.get("fleet_impact", _fleet_impact(state)),
        "recommendation": tool_args.get("recommendation", "approve"),
        "reasoning": tool_args.get("reason", ""),
        "fleet_assessment": state.get("fleet_assessment", ""),
        "customer_assessment": state.get("customer_assessment", ""),
    }


def _chat_model(tools: list | None = None):
    """Build the (provider-swappable) chat model on the activity side."""
    import os

    from langchain.chat_models import init_chat_model

    from agent_fleet.config import DEFAULT_MODEL

    provider = os.environ.get("MODEL_PROVIDER", "google_genai")
    model = init_chat_model(DEFAULT_MODEL, model_provider=provider)
    return model.bind_tools(tools) if tools else model


# --- The multi-agent team (each node runs as a Temporal activity) --- #
async def fleet_agent(state: GateState) -> dict:
    """Fleet agent: assess logistics / capacity for this order (one line)."""
    resp = await _chat_model().ainvoke(
        "You are the Fleet agent for an ice cream delivery fleet. In one sentence, assess the "
        "logistics of taking this order given current capacity.\n"
        f"Order: {state['venue']} — ${state['order_value']:,}, {state['servings']} servings.\n"
        f"Fleet: {_fleet_impact(state)}"
    )
    text = resp.content if isinstance(resp.content, str) else str(resp.content)
    return {"fleet_assessment": text.strip(), "fleet_impact": _fleet_impact(state)}


async def customer_agent(state: GateState) -> dict:
    """Customer agent: assess priority / deadline (one line)."""
    resp = await _chat_model().ainvoke(
        "You are the Customer agent for an ice cream delivery fleet. In one sentence, assess "
        "this order's priority and deadline risk.\n"
        f"Order: {state['venue']} — ${state['order_value']:,}, {state['servings']} servings, "
        f"due in {state['deadline_minutes']} min."
    )
    text = resp.content if isinstance(resp.content, str) else str(resp.content)
    return {"customer_assessment": text.strip()}


async def dispatch_agent(state: GateState) -> dict:
    """Dispatch agent: weigh both assessments, decide whether to call the human tool."""
    from langchain_core.tools import tool

    @tool
    def request_human_approval(reason: str, recommendation: str) -> str:
        """Escalate this order to a human supervisor for sign-off BEFORE committing fleet
        capacity. Call this when the order is high-value or fleet capacity is tight.

        Args:
            reason: why this order needs human judgment.
            recommendation: your recommendation, "approve" or "reject".
        """
        return "escalated"

    model = _chat_model(tools=[request_human_approval])
    prompt = (
        f"{ESCALATION_GUIDANCE}\n\n"
        f"Order: {state['venue']} — ${state['order_value']:,} — {state['servings']} servings, "
        f"due in {state['deadline_minutes']} min.\n"
        f"Fleet agent: {state.get('fleet_assessment', '')}\n"
        f"Customer agent: {state.get('customer_assessment', '')}\n"
        f"Fleet: {_fleet_impact(state)}"
    )
    resp = await model.ainvoke(prompt)
    for call in getattr(resp, "tool_calls", None) or []:
        if call["name"] == "request_human_approval":
            return {"escalate": True, "tool_args": call["args"]}
    return {"escalate": False, "tool_args": {}}


async def request_human(state: GateState) -> dict:
    """Interrupt-mode HITL node (back-pocket toggle): suspend via LangGraph interrupt."""
    brief = _build_brief(state, state.get("tool_args", {}))
    decision = interrupt(brief)
    return {"brief": brief, "decision": decision, "approved": decision != "reject"}


def finalize(state: GateState) -> dict:
    if "approved" in state:
        return {}
    return {"approved": True, "decision": "auto-dispatched"}


async def _route(state: GateState) -> str:
    return "request_human" if state.get("escalate") else "finalize"


def build_gate_graph(use_interrupt: bool = False) -> StateGraph:
    """Fleet + Customer assess in parallel → Dispatch decides.

    In interrupt mode the graph also owns the HITL node; in Temporal mode the graph ends
    after Dispatch and the workflow performs the signal-based HITL.
    """
    g = StateGraph(GateState)
    activity = {"execute_in": "activity", "start_to_close_timeout": timedelta(seconds=60)}
    # Per-node `summary` surfaces a human-readable label on each scheduled activity in
    # the Temporal UI (the integration passes `summary` through to execute_activity).
    # Static — set at graph-build time, so no per-order prefix like the ADK side — but it
    # names the agent on every call instead of the bare `dispatch_gate.fleet_agent` type.
    g.add_node(
        "fleet_agent",
        fleet_agent,
        metadata={**activity, "summary": "Fleet Agent — assess fleet capacity"},
    )
    g.add_node(
        "customer_agent",
        customer_agent,
        metadata={**activity, "summary": "Customer Agent — assess priority & deadline"},
    )
    g.add_node(
        "dispatch_agent",
        dispatch_agent,
        metadata={**activity, "summary": "Dispatch Agent — weigh both, decide whether to escalate"},
    )

    # Fan out to the two assessor agents, then converge on dispatch (multi-agent).
    g.add_edge(START, "fleet_agent")
    g.add_edge(START, "customer_agent")
    g.add_edge("fleet_agent", "dispatch_agent")
    g.add_edge("customer_agent", "dispatch_agent")

    if use_interrupt:
        g.add_node("request_human", request_human, metadata={"execute_in": "workflow"})
        g.add_node(
            "finalize", finalize, metadata={**activity, "summary": "Finalize dispatch decision"}
        )
        g.add_conditional_edges(
            "dispatch_agent", _route, {"request_human": "request_human", "finalize": "finalize"}
        )
        g.add_edge("request_human", "finalize")
        g.add_edge("finalize", END)
    else:
        g.add_edge("dispatch_agent", END)
    return g


class HumanGateState(TypedDict, total=False):
    brief: dict[str, Any]
    decision: str
    approved: bool


async def human_pause(state: HumanGateState) -> dict:
    """HITL-only interrupt node: suspend on the pre-built brief, resume via Command."""
    decision = interrupt(state["brief"])
    return {"decision": decision, "approved": decision != "reject"}


def build_human_graph() -> StateGraph:
    """A single interrupt node for the demo's HITL-only gate child (interrupt toggle).

    The multi-agent assessment has already run inline in the parent workflow, so this
    graph is purely the durable human endpoint — the agent team is NOT re-invoked here.
    """
    g = StateGraph(HumanGateState)
    g.add_node("request_human", human_pause, metadata={"execute_in": "workflow"})
    g.add_edge(START, "request_human")
    g.add_edge("request_human", END)
    return g


# --------------------------------------------------------------------------- #
# Temporal workflow that runs the graph durably
# --------------------------------------------------------------------------- #
@workflow.defn
class DispatchGateWorkflow:
    def __init__(self) -> None:
        self._brief: dict[str, Any] | None = None
        self._decision: str | None = None
        self._approver_tier: str = "primary"
        self._timed_out: bool = False

    @workflow.run
    async def run(self, inp: DispatchGateInput) -> DispatchGateResult:
        # Demo path: the multi-agent assessment already ran inline in the parent and
        # the agent escalated. This child performs the durable HITL pause only — so
        # gate-* children equal human approvals, not order count.
        if inp.brief is not None:
            return await self._run_hitl_only(inp)

        # Standalone path (spikes): run the full multi-agent graph + HITL in one workflow.
        graph_name = GRAPH_NAME_INTERRUPT if inp.use_interrupt else GRAPH_NAME
        compiled = graph(graph_name).compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": workflow.info().workflow_id}}

        state: GateState = {
            "order_id": inp.order_id,
            "venue": inp.venue,
            "order_value": inp.order_value,
            "servings": inp.servings,
            "deadline_minutes": inp.deadline_minutes,
            "proposed_driver_id": inp.proposed_driver_id,
            "drivers_available": inp.drivers_available,
            "drivers_total": inp.drivers_total,
            "pending_orders": inp.pending_orders,
        }
        workflow.set_current_details(
            f"🤖 Agent team assessing **{inp.venue}** (${inp.order_value:,})"
        )

        if inp.use_interrupt:
            return await self._run_interrupt(compiled, config, inp, state)
        return await self._run_temporal(compiled, config, inp, state)

    async def _run_hitl_only(self, inp: DispatchGateInput) -> DispatchGateResult:
        """Demo gate child: the agent already escalated inline; park on the human.

        The pause primitive matches the toggle — a Temporal `approve` signal (default)
        or a LangGraph `interrupt()` (back-pocket). Either way the human is a durable,
        async endpoint the workflow awaits, not a synchronous call.
        """
        brief = inp.brief or {}
        self._brief = brief
        venue = brief.get("venue", "")
        order_value = brief.get("order_value", 0)
        rec = brief.get("recommendation", "—")
        await self._notify_parent_awaiting()
        mode = "interrupt" if inp.use_interrupt else "signal"
        workflow.set_current_details(
            f"⏸ **Awaiting human approval** ({mode}) — {venue} (${order_value:,})\n\n"
            f"Agent recommends **{rec}**. Resolves on the `approve` signal."
        )
        if inp.use_interrupt:
            decision = await self._human_via_interrupt(inp)
        else:
            decision = await self._await_human(inp.escalation_seconds)
        self._brief = None
        approved = decision != "reject"
        outcome = "✓ **Approved & dispatched**" if approved else "✕ **Rejected — order held**"
        workflow.set_current_details(f"{outcome} — {venue} (${order_value:,})")
        return DispatchGateResult(
            order_id=inp.order_id, approved=approved, decision=decision, timed_out=self._timed_out
        )

    async def _human_via_interrupt(self, inp: DispatchGateInput) -> str:
        """Interrupt-toggle HITL for the demo child: drive the 1-node interrupt graph.

        The agent team is NOT re-run — this graph only carries the brief into a
        LangGraph `interrupt()` and resumes from the `approve` signal via Command.
        """
        compiled = graph(GRAPH_NAME_HUMAN).compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": workflow.info().workflow_id}}
        result = await compiled.ainvoke({"brief": inp.brief}, config=config)
        decision = "approve"
        while result.get("__interrupt__"):
            decision = await self._await_human(inp.escalation_seconds)
            result = await compiled.ainvoke(Command(resume=decision), config=config)
        return str(result.get("decision", decision))

    async def _run_temporal(self, compiled, config, inp, state) -> DispatchGateResult:
        """Default: the agent's tool call → the WORKFLOW does Temporal-signal HITL."""
        result = await compiled.ainvoke(state, config=config)
        await self._publish_team_reasoning(result)

        if not result.get("escalate"):
            workflow.set_current_details(f"✓ Auto-dispatched, no human needed — {inp.venue}")
            return DispatchGateResult(
                order_id=inp.order_id, approved=True, decision="auto-dispatched"
            )

        self._brief = _build_brief(result, result.get("tool_args", {}))
        await self._notify_parent_awaiting()
        rec = self._brief.get("recommendation", "—")
        workflow.set_current_details(
            f"⏸ **Awaiting human approval** — {inp.venue} (${inp.order_value:,})\n\n"
            f"Agent recommends **{rec}**. Resolves on the `approve` signal."
        )
        decision = await self._await_human(inp.escalation_seconds)
        self._brief = None
        approved = decision != "reject"
        outcome = "✓ **Approved & dispatched**" if approved else "✕ **Rejected — order held**"
        workflow.set_current_details(f"{outcome} — {inp.venue} (${inp.order_value:,})")
        return DispatchGateResult(
            order_id=inp.order_id, approved=approved, decision=decision, timed_out=self._timed_out
        )

    async def _run_interrupt(self, compiled, config, inp, state) -> DispatchGateResult:
        """Back-pocket: LangGraph interrupt() drives the pause; resume via Command."""
        result = await compiled.ainvoke(state, config=config)
        await self._publish_team_reasoning(result)
        while result.get("__interrupt__"):
            self._brief = result["__interrupt__"][0].value
            await self._notify_parent_awaiting()
            rec = (self._brief or {}).get("recommendation", "—")
            workflow.set_current_details(
                f"⏸ **Awaiting human approval** (interrupt) — {inp.venue} (${inp.order_value:,})"
                f"\n\nAgent recommends **{rec}**."
            )
            decision = await self._await_human(inp.escalation_seconds)
            self._brief = None
            result = await compiled.ainvoke(Command(resume=decision), config=config)

        approved = bool(result.get("approved"))
        decision = str(result.get("decision", "approve"))
        if decision == "auto-dispatched":
            workflow.set_current_details(f"✓ Auto-dispatched, no human needed — {inp.venue}")
        else:
            outcome = "✓ **Approved & dispatched**" if approved else "✕ **Rejected — order held**"
            workflow.set_current_details(f"{outcome} — {inp.venue} (${inp.order_value:,})")
        return DispatchGateResult(
            order_id=inp.order_id, approved=approved, decision=decision, timed_out=self._timed_out
        )

    async def _await_human(self, escalation_seconds: int) -> str:
        """Temporal HITL: wait for the `approve` signal; escalate to backup on timeout."""
        self._approver_tier = "primary"
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None,
                timeout=timedelta(seconds=escalation_seconds),
            )
        except TimeoutError:
            self._timed_out = True
            self._approver_tier = "backup"
            workflow.logger.info("Dispatch approval timed out — escalating to backup approver")
            workflow.set_current_details(
                "⏳ Primary approver timed out — **escalated to backup approver**"
            )
            await workflow.wait_condition(lambda: self._decision is not None)

        decision = self._decision
        self._decision = None
        assert decision is not None
        return decision

    async def _publish_team_reasoning(self, result: dict) -> None:
        """Surface the LangGraph agent team's reasoning in the demo's agent feed.

        Reuses the existing Fleet / Customer / Dispatch panels (agent_name keys). Guarded
        on running under the demo — standalone runs skip it (the batch activity isn't
        registered there, and there's no FleetState to publish to).
        """
        if workflow.info().parent is None:
            return
        fleet = (result.get("fleet_assessment") or "").strip()
        cust = (result.get("customer_assessment") or "").strip()
        reason = (result.get("tool_args") or {}).get("reason", "")
        dispatch_note = (
            f"High-value — calling for human approval. {reason}"
            if result.get("escalate")
            else "Within policy — dispatching."
        )
        await workflow.execute_local_activity(
            publish_agent_events_batch,
            [
                PublishAgentEventInput(
                    agent_name="fleet_agent",
                    event_type="assessment",
                    content=fleet,
                    summary=fleet[:90],
                ),
                PublishAgentEventInput(
                    agent_name="customer_agent",
                    event_type="assessment",
                    content=cust,
                    summary=cust[:90],
                ),
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="plan",
                    content=dispatch_note,
                    summary=dispatch_note[:90],
                ),
            ],
            start_to_close_timeout=timedelta(seconds=10),
        )

    async def _notify_parent_awaiting(self) -> None:
        """Tell the parent demo workflow a human is needed (guarded: no-op standalone)."""
        if not self._brief:
            return
        try:
            parent = workflow.info().parent
            if parent is not None:
                handle = workflow.get_external_workflow_handle(parent.workflow_id)
                await handle.signal("dispatch_gate_awaiting", self._brief)
        except Exception as e:
            workflow.logger.warning(f"could not notify parent of pending approval: {e}")

    @workflow.signal
    def approve(self, decision: str) -> None:
        """The human responds: 'approve' or 'reject'. The async endpoint resolves."""
        self._decision = decision

    @workflow.query
    def pending_brief(self) -> dict[str, Any]:
        """What the agent is waiting on, for the approval popup."""
        return {"brief": self._brief, "approver_tier": self._approver_tier}
