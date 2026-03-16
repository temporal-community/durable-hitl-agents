"""
ADK agent definitions for the courier fleet demo.

Three agents:
- Dispatch Agent: reasons about fleet assignments (which courier for which mission)
- Courier Agent: executes mission steps deterministically via tool calls
- Mission Monitor Agent: evaluates conditions mid-flight (battery, weather)

NOTE on the demo approach:
The Dispatch and Monitor agents reason visibly. The Courier Agent follows a
predictable step sequence — the interesting decisions are in assignment and
safety monitoring. Agent prompts are step-specific: the workflow constructs
prompts with coordinates, courier ID, and mission ID baked in.
"""

from __future__ import annotations

import os

try:
    from google.adk.agents import Agent
    from temporalio.contrib.google_adk_agents import TemporalModel, activity_tool
    _ADK_AVAILABLE = True
except ImportError:
    Agent = TemporalModel = activity_tool = None
    _ADK_AVAILABLE = False

DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

from agent_fleet.activities import (
    get_fleet_status,
    assign_courier_to_mission,
    navigate_to,
    pickup_package,
    deliver_package,
    check_courier_battery,
    check_weather,
)


def create_dispatch_agent() -> Agent:
    """
    The fleet dispatch agent. Checks fleet status and assigns couriers
    to missions optimally.
    """
    return Agent(
        name="dispatch_agent",
        model=TemporalModel(DEFAULT_MODEL),
        instruction=(
            "You are a fleet dispatch coordinator. You manage a fleet of "
            "delivery couriers. First, use the get_fleet_status tool to check "
            "the current fleet state. Then assign each pending mission to an "
            "available courier using the assign_courier_to_mission tool. "
            "Be concise — check status, assign, and confirm."
        ),
        tools=[
            activity_tool(get_fleet_status),
            activity_tool(assign_courier_to_mission),
        ],
    )


def create_courier_agent() -> Agent:
    """
    A courier delivery agent. Executes the navigate -> pickup -> navigate ->
    deliver sequence for a single mission.

    Each tool call becomes a Temporal activity — individually retryable
    and crash-safe.
    """
    return Agent(
        name="courier_agent",
        model=TemporalModel(DEFAULT_MODEL),
        instruction=(
            "You are an autonomous delivery courier. Execute delivery missions "
            "step by step. When prompted with a specific step, execute ONLY "
            "that step using the appropriate tool. Report status briefly "
            "after each step."
        ),
        tools=[
            activity_tool(navigate_to),
            activity_tool(pickup_package),
            activity_tool(deliver_package),
        ],
    )


def create_monitor_agent() -> Agent:
    """
    The mission monitor agent. Evaluates safety conditions mid-flight
    by checking battery and weather.

    Returns a decision: CONTINUE, RETURN_TO_BASE, or ESCALATE (if enabled).
    Decision must be on the first line, brief reason after.
    """
    return Agent(
        name="monitor_agent",
        model=TemporalModel(DEFAULT_MODEL),
        instruction=(
            "You are a mission safety monitor. When prompted, check the "
            "courier's battery level and weather conditions using the provided "
            "tools. Based on the results, make a decision:\n"
            "- CONTINUE: if battery is above 20% and weather is safe\n"
            "- RETURN_TO_BASE: if battery is critical (below 20%) or weather "
            "is unsafe (storm)\n"
            "Your response MUST start with the decision word on the first "
            "line (CONTINUE or RETURN_TO_BASE), followed by a brief reason."
        ),
        tools=[
            activity_tool(check_courier_battery),
            activity_tool(check_weather),
        ],
    )
