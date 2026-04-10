"""
ADK agent definitions for the Meltdown ice cream delivery demo.

Order assignment pipeline:
- Fleet Agent: assesses driver positions, capacity, and ETAs for new orders
- Customer Agent: evaluates order priority, urgency, and hotel context
- Dispatch Agent: synthesizes both and submits a structured driver assignment

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
from google.adk.tools.google_search_tool import GoogleSearchTool
from temporalio.common import RetryPolicy
from temporalio.contrib.google_adk_agents import TemporalModel
from temporalio.workflow import ActivityConfig

from agent_fleet._activity_tool import activity_tool
from agent_fleet.activities import (
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
)
from agent_fleet.config import DEFAULT_MODEL
from agent_fleet.queues import AGENTS_QUEUE

_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)

# Fleet Agent tools fail fast when disconnected — 2 attempts so the Resolver
# gets an error quickly and can assign based on Customer Agent data alone.
# Temporal UI still shows the retry attempt.
_FLEET_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=1.5,
    maximum_interval=timedelta(seconds=5),
    maximum_attempts=2,
)


# --- Activity-backed tools (each tool call becomes a Temporal activity) ---

_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    summary="Fleet Agent — get fleet status",
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_FLEET_TOOL_RETRY,
)
_order_priorities_tool = activity_tool(
    tool_get_order_priorities,
    task_queue=AGENTS_QUEUE,
    summary="Customer Agent — get order priorities",
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
_route_info_tool = activity_tool(
    tool_get_route_info,
    task_queue=AGENTS_QUEUE,
    summary="Fleet Agent — get route info",
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=_FLEET_TOOL_RETRY,
)


# --- Order assignment agents ---


async def tool_submit_assignment(
    tool_context: ToolContext,
    driver_id: str,
    reasoning_summary: str,
) -> str:
    """Submit the final order assignment decision. You MUST call this tool with your recommendation.

    Args:
        driver_id: The Driver ID to assign the order to (e.g. "driver-1")
        reasoning_summary: Brief explanation of why this driver was chosen
    """
    tool_context.state["assignment"] = {
        "driver_id": driver_id,
        "reasoning_summary": reasoning_summary,
    }
    return "Assignment submitted successfully."


def create_assignment_fleet_agent() -> Agent:
    """
    Fleet Agent for order assignment — assesses driver positions, capacity,
    and ETAs to recommend the best driver for a new order.
    """
    return Agent(
        name="assignment_fleet_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(
                task_queue=AGENTS_QUEUE,
                summary="Fleet Agent — LLM reasoning",
            ),
        ),
        description=(
            "Operational fleet specialist for order assignment. Assesses Driver "
            "positions, capacity, cooler status, and ETAs to recommend the best driver."
        ),
        instruction=(
            "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived — assess which Driver should handle it.\n\n"
            "Call tool_get_fleet_status for fleet state, then tool_get_route_info "
            "to compare ETAs to the delivery destination.\n\n"
            "Rules:\n"
            "- NEVER recommend a DISCONNECTED driver\n"
            "- Skip drivers at capacity (no free slots)\n"
            "- Prefer the closest driver with capacity\n\n"
            "Respond with ONLY: the recommended driver ID and ETA. "
            "Example: 'driver-2 — 4min ETA, closest with capacity.' "
            "No preamble, no comparisons of other drivers."
        ),
        tools=[_fleet_status_tool, _route_info_tool],
        output_key="fleet_assessment",
    )


def create_assignment_customer_agent() -> Agent:
    """
    Customer Agent for order assignment — evaluates priority, urgency,
    hotel context, and deadline pressure for a new order.
    """
    return Agent(
        name="assignment_customer_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(
                task_queue=AGENTS_QUEUE,
                summary="Customer Agent — LLM reasoning",
            ),
        ),
        description=(
            "Customer priority specialist for order assignment. Evaluates order "
            "priority, urgency, deadline pressure, and hotel context."
        ),
        instruction=(
            "You are the Customer Relations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived — assess its priority and urgency.\n\n"
            "Call tool_get_order_priorities for order details. "
            "Use Google Search to find current events at the delivery hotel.\n\n"
            "Assess: VIP or standard? Deadline tight? Hotel events increasing urgency? "
            "How many servings?\n\n"
            "Respond with ONLY: priority level and key urgency factor. "
            "Example: 'VIP, tight deadline (25min), Caesars gala tonight.' "
            "No preamble, no full analysis."
        ),
        tools=[
            _order_priorities_tool,
            GoogleSearchTool(bypass_multi_tools_limit=True),
        ],
        output_key="customer_assessment",
    )


def create_assignment_dispatch_agent() -> Agent:
    """
    Dispatch Agent for order assignment — synthesizes fleet and customer assessments,
    picks the best driver, and submits the structured assignment.
    """
    return Agent(
        name="assignment_dispatch_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(
                task_queue=AGENTS_QUEUE,
                summary="Dispatch Agent — LLM reasoning",
            ),
        ),
        description=(
            "Dispatch Agent. Synthesizes fleet and customer assessments "
            "to pick the best driver for a new order."
        ),
        instruction=(
            "You are the Assignment Coordinator for Meltdown Ice Cream Delivery. "
            "Fleet Agent and Customer Agent have assessed a new order.\n\n"
            "Rules:\n"
            "- NEVER assign to a DISCONNECTED driver\n"
            "- If an agent is offline, compensate with available data\n\n"
            "You MUST call tool_submit_assignment with:\n"
            "- driver_id: the Driver that should get this order\n"
            "- reasoning_summary: one sentence explaining the decision\n\n"
            "Keep reasoning_summary under 20 words."
        ),
        tools=[tool_submit_assignment],
    )


def create_order_assignment_agent() -> SequentialAgent:
    """
    Compose the full order assignment pipeline (workflow context).
    Uses TemporalModel + activity_tool — each LLM and tool call is a Temporal activity.
    1. ParallelAgent: Fleet Agent + Customer Agent assess simultaneously
    2. Dispatch Agent: synthesizes and submits driver assignment
    """
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),
            create_assignment_customer_agent(),
        ],
    )

    assignment_dispatch_agent = create_assignment_dispatch_agent()

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, assignment_dispatch_agent],
    )
