"""Pattern B HITL endpoint — the durable human approval gate.

This module is ONLY the gate. The multi-agent reasoning (Fleet ∥ Customer → Dispatch,
the looping team) lives in ``agent_fleet/langgraph_agents.py`` and runs inline in the
parent workflow. When the Dispatch agent escalates, the parent spawns a
``DispatchGateWorkflow`` child whose entire job is to hold the decision until a human
signs off — so ``gate-*`` children equal human approvals, not order count.

The human is a durable, async endpoint the workflow awaits — never a synchronous call.
Two interchangeable pause primitives, chosen per-order via ``DispatchGateInput.use_interrupt``:

- **Temporal (default):** the workflow parks on a Temporal ``approve`` SIGNAL via
  ``wait_condition`` (+ timeout → backup approver). Pure Temporal; no LangGraph needed.
  This is the Temporal-native pattern the talk leads with.
- **Interrupt (back-pocket toggle):** a one-node LangGraph graph carries the brief into
  ``interrupt()`` and resumes via ``Command(resume=...)``. Same durability, LangGraph's
  mechanism. The agent team is NOT re-run here — only the human pause.

Because the team has already reasoned by the time this gate runs, HITL sits at the
loop's BOUNDARY (a gate on the agents' conclusion), not inside the reason→act loop.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, TypedDict

from temporalio import workflow

from agent_fleet.models import DispatchGateInput, DispatchGateResult

with workflow.unsafe.imports_passed_through():
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import Command, interrupt
    from temporalio.contrib.langgraph import graph

# A single interrupt node for the demo's HITL-only gate child (interrupt toggle).
GRAPH_NAME_HUMAN = "human_approval_interrupt"


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

    The multi-agent assessment already ran inline in the parent workflow, so this
    graph is purely the durable human endpoint — the agent team is NOT re-invoked here.
    """
    g = StateGraph(HumanGateState)
    g.add_node("request_human", human_pause, metadata={"execute_in": "workflow"})
    g.add_edge(START, "request_human")
    g.add_edge("request_human", END)
    return g


# --------------------------------------------------------------------------- #
# The durable human gate
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
        """The agent team already ran inline in the parent and escalated. This workflow
        performs the durable HITL pause only — park on the human, resolve, return.

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
        """Interrupt-toggle HITL: drive the 1-node interrupt graph.

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
