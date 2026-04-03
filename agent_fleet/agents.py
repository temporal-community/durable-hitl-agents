"""
ADK agent definitions for the Meltdown ice cream delivery demo.

Order assignment pipeline:
- Fleet Agent: assesses crew positions, capacity, and ETAs for new orders
- Customer Agent: evaluates order priority, urgency, and hotel context
- Assignment Resolver: synthesizes both and submits a structured crew assignment

Architecture:
- Agent execution happens inline in the workflow via TemporalModel + activity_tool.
  Each LLM call is routed through an invoke_model activity (via TemporalModel),
  and each tool call is its own Temporal activity (via activity_tool wrappers).
- The resolver agent calls tool_submit_assignment to write structured output
  into ADK session state. The workflow reads it back after the runner completes.
"""

from __future__ import annotations

from datetime import timedelta

from google.adk.agents import Agent, ParallelAgent, SequentialAgent
from google.adk.tools import ToolContext
from temporalio.common import RetryPolicy
from temporalio.contrib.google_adk_agents import TemporalModel
from temporalio.contrib.google_adk_agents.workflow import activity_tool

from agent_fleet.activities import (
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_publish_agent_event,
    tool_search_hotel_context,
)
from agent_fleet.config import GEMINI_MODEL
from agent_fleet.queues import AGENTS_QUEUE

_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)


# --- Activity-backed tools (each tool call becomes a Temporal activity) ---

_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
_order_priorities_tool = activity_tool(
    tool_get_order_priorities,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
_publish_event_tool = activity_tool(
    tool_publish_agent_event,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
_route_info_tool = activity_tool(
    tool_get_route_info,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=_TOOL_RETRY,
)
_hotel_search_tool = activity_tool(
    tool_search_hotel_context,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=_TOOL_RETRY,
)


# --- Order assignment agents ---


async def tool_submit_assignment(
    tool_context: ToolContext,
    crew_id: str,
    reasoning_summary: str,
) -> str:
    """Submit the final order assignment decision. You MUST call this tool with your recommendation.

    Args:
        crew_id: The AI-Crew ID to assign the order to (e.g. "ai-crew-1")
        reasoning_summary: Brief explanation of why this crew was chosen
    """
    tool_context.state["assignment"] = {
        "crew_id": crew_id,
        "reasoning_summary": reasoning_summary,
    }
    return "Assignment submitted successfully."


def create_assignment_fleet_agent() -> Agent:
    """
    Fleet Agent for order assignment — assesses crew positions, capacity,
    and ETAs to recommend the best crew for a new order.
    """
    return Agent(
        name="assignment_fleet_agent",
        model=TemporalModel(GEMINI_MODEL),
        description=(
            "Operational fleet specialist for order assignment. Assesses AI-Crew "
            "positions, capacity, cooler status, and ETAs to recommend the best crew."
        ),
        instruction=(
            "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived and you need to assess which AI-Crew is best "
            "positioned to handle it.\n\n"
            "Call tool_get_fleet_status to check current fleet state, then "
            "tool_get_route_info to compare ETAs from available crews to the "
            "delivery destination.\n\n"
            "Rules:\n"
            "- NEVER recommend a DISCONNECTED crew\n"
            "- Skip crews at capacity (no free slots)\n"
            "- Prefer the closest crew with capacity\n\n"
            "Call tool_publish_agent_event with agent_name='fleet_agent' and "
            "event_type='assessment' to share your fleet scan results.\n\n"
            "Be concise and decisive — state which crew you recommend and why."
        ),
        tools=[_fleet_status_tool, _route_info_tool, _publish_event_tool],
        output_key="fleet_assessment",
    )


def create_assignment_customer_agent() -> Agent:
    """
    Customer Agent for order assignment — evaluates priority, urgency,
    hotel context, and deadline pressure for a new order.
    """
    return Agent(
        name="assignment_customer_agent",
        model=TemporalModel(GEMINI_MODEL),
        description=(
            "Customer priority specialist for order assignment. Evaluates order "
            "priority, urgency, deadline pressure, and hotel context."
        ),
        instruction=(
            "You are the Customer Relations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived and you need to assess its priority and urgency.\n\n"
            "Call tool_get_order_priorities to check order details. "
            "Call tool_search_hotel_context to get context about the delivery hotel.\n\n"
            "Assess:\n"
            "- Is this a VIP or standard order?\n"
            "- How tight is the deadline?\n"
            "- Are there events at the hotel that increase urgency?\n"
            "- How many servings/guests are affected?\n\n"
            "Call tool_publish_agent_event with agent_name='customer_agent' and "
            "event_type='assessment' to share your priority assessment.\n\n"
            "Be concise — state the priority level and any urgency factors."
        ),
        tools=[_order_priorities_tool, _hotel_search_tool, _publish_event_tool],
        output_key="customer_assessment",
    )


def create_assignment_resolver() -> Agent:
    """
    Resolver for order assignment — synthesizes fleet and customer assessments,
    picks the best crew, and submits the structured assignment.
    """
    return Agent(
        name="assignment_resolver",
        model=TemporalModel(GEMINI_MODEL),
        description=(
            "Assignment coordinator. Synthesizes fleet and customer assessments "
            "to pick the best crew for a new order."
        ),
        instruction=(
            "You are the Assignment Coordinator for Meltdown Ice Cream Delivery. "
            "You have received assessments from the Fleet Agent (operational) and "
            "Customer Agent (customer priority) about a new order.\n\n"
            "Synthesize both perspectives:\n"
            "- Fleet Agent recommends which crew is best positioned\n"
            "- Customer Agent flags urgency and priority level\n"
            "- If an agent is offline, compensate with available data\n"
            "- NEVER assign to a DISCONNECTED crew\n\n"
            "You MUST call tool_submit_assignment with:\n"
            "- crew_id: the AI-Crew that should get this order\n"
            "- reasoning_summary: brief explanation of the decision\n\n"
            "Also call tool_publish_agent_event with agent_name='resolver' and "
            "event_type='plan' to announce the assignment.\n\n"
            "Be decisive. Pick the crew and explain why in one sentence."
        ),
        tools=[_publish_event_tool, tool_submit_assignment],
    )


def create_order_assignment_agent() -> SequentialAgent:
    """
    Compose the full order assignment pipeline:
    1. ParallelAgent: Fleet Agent + Customer Agent assess simultaneously
    2. Assignment Resolver: synthesizes and submits crew assignment
    """
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),
            create_assignment_customer_agent(),
        ],
    )

    resolver = create_assignment_resolver()

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, resolver],
    )
