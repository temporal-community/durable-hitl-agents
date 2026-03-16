"""
Temporal workflows for the courier fleet demo.

DeliveryMissionWorkflow — one per delivery. Interleaves courier agent steps
with monitor agent evaluations. This is the workflow that will be "in flight"
when we kill the worker to demonstrate recovery.

FleetDispatchWorkflow — starts all delivery missions. Uses the Dispatch Agent
to reason about fleet assignments (ADK path) or hardcoded logic (mock path).
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from google.adk.agents import Agent
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part

    from agent_fleet.models import (
        NavigateInput,
        PackageInput,
        AssignCourierInput,
        AssignCourierOutput,
        GetMissionAssignmentInput,
        GetMissionAssignmentOutput,
        MonitorDecision,
        HumanApprovalInput,
        LegType,
    )
    from agent_fleet.locations import WAREHOUSE, DELIVERY_DESTINATIONS
    from agent_fleet.activities import (
        assign_courier,
        navigate_to,
        pickup_package,
        deliver_package,
        request_human_approval,
        get_mission_assignment,
    )
    from agent_fleet.agents import (
        create_dispatch_agent,
        create_courier_agent,
        create_monitor_agent,
    )

    _MOCK_MODE = not os.environ.get("GOOGLE_API_KEY")


def _parse_monitor_decision(event) -> MonitorDecision:
    """Defensively parse the monitor agent's decision from its response text."""
    try:
        if hasattr(event, "content") and event.content and event.content.parts:
            text = event.content.parts[0].text or ""
        elif isinstance(event, str):
            text = event
        else:
            text = str(event)

        first_line = text.strip().split("\n")[0].strip().upper()

        if "RETURN_TO_BASE" in first_line:
            return MonitorDecision.RETURN_TO_BASE
        if "ESCALATE" in first_line:
            return MonitorDecision.ESCALATE
        if "REROUTE" in first_line:
            return MonitorDecision.REROUTE
        return MonitorDecision.CONTINUE
    except Exception:
        return MonitorDecision.CONTINUE


async def _run_agent_turn(runner: Runner, user_id: str, session_id: str, prompt: str) -> str:
    """Run one agent turn and return the final text response.

    NOTE: When temporalio.contrib.google_adk_agents ships, TemporalModel wraps
    each LLM call as a Temporal activity automatically — making this replay-safe.
    The Runner.run_async call itself becomes deterministic because TemporalModel
    intercepts the model invocations and records/replays them via the activity system.
    """
    content = Content(parts=[Part(text=prompt)])
    last_text = ""
    async for event in runner.run_async(
        user_id=user_id, session_id=session_id, new_message=content
    ):
        if hasattr(event, "content") and event.content and event.content.parts:
            text = event.content.parts[0].text
            if text:
                last_text = text
    return last_text


@dataclass
class DeliveryMissionInput:
    """Input for DeliveryMissionWorkflow — keeps workflow params serializable."""
    mission_id: str
    escalation_enabled: bool = False


@workflow.defn
class DeliveryMissionWorkflow:
    """
    Executes a single delivery: courier agent steps interleaved with
    monitor agent evaluations.

    ADK path: agent-driven with monitor checks after navigation steps.
    Mock path: existing hardcoded 5-step sequence.
    """

    def __init__(self) -> None:
        self._human_decision: str | None = None

    @workflow.signal
    async def human_decision(self, decision: str) -> None:
        """Signal for human-in-the-loop escalation (only active when escalation enabled)."""
        self._human_decision = decision

    @workflow.run
    async def run(self, inp: DeliveryMissionInput) -> str:
        if _MOCK_MODE:
            return await self._run_mock(inp.mission_id)
        return await self._run_adk(inp.mission_id, inp.escalation_enabled)

    async def _run_adk(self, mission_id: str, escalation_enabled: bool) -> str:
        fast_retry = RetryPolicy(maximum_attempts=5)

        dest = DELIVERY_DESTINATIONS[mission_id]["coords"]

        # Create courier and monitor agents with separate sessions
        courier_agent = create_courier_agent()
        monitor_agent = create_monitor_agent()

        session_service = InMemorySessionService()

        courier_runner = Runner(
            agent=courier_agent,
            app_name="fleet_demo",
            session_service=session_service,
        )
        monitor_runner = Runner(
            agent=monitor_agent,
            app_name="fleet_demo",
            session_service=session_service,
        )

        await session_service.create_session(
            app_name="fleet_demo", user_id=mission_id, session_id=f"{mission_id}-courier"
        )
        await session_service.create_session(
            app_name="fleet_demo", user_id=mission_id, session_id=f"{mission_id}-monitor"
        )

        # Get the courier assignment via an activity (determinism-safe)
        courier_id: str | None = None
        while courier_id is None:
            await workflow.sleep(timedelta(milliseconds=500))
            assignment: GetMissionAssignmentOutput = await workflow.execute_activity(
                get_mission_assignment,
                GetMissionAssignmentInput(mission_id=mission_id),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=fast_retry,
            )
            courier_id = assignment.courier_id

        # 4 step prompts for the courier agent
        step_prompts = [
            # Step 0: Navigate to warehouse (pickup point)
            (
                f"Navigate courier {courier_id} to the warehouse pickup point at "
                f"lat={WAREHOUSE.lat}, lng={WAREHOUSE.lng} for mission {mission_id}. "
                f"Use navigate_to with courier_id='{courier_id}', mission_id='{mission_id}', "
                f"target_lat={WAREHOUSE.lat}, target_lng={WAREHOUSE.lng}, leg='pickup', steps=2.",
                True,  # monitor after
            ),
            # Step 1: Pick up package
            (
                f"Pick up the package for mission {mission_id}. "
                f"Use pickup_package with courier_id='{courier_id}', mission_id='{mission_id}'.",
                False,
            ),
            # Step 2: Navigate to delivery destination
            (
                f"Navigate courier {courier_id} to the delivery destination at "
                f"lat={dest.lat}, lng={dest.lng} for mission {mission_id}. "
                f"Use navigate_to with courier_id='{courier_id}', mission_id='{mission_id}', "
                f"target_lat={dest.lat}, target_lng={dest.lng}, leg='delivery', steps=10.",
                True,  # monitor after
            ),
            # Step 3: Deliver package
            (
                f"Deliver the package for mission {mission_id}. "
                f"Use deliver_package with courier_id='{courier_id}', mission_id='{mission_id}'.",
                False,
            ),
        ]

        for idx, (prompt, should_monitor) in enumerate(step_prompts):
            # Execute courier agent step
            await _run_agent_turn(
                courier_runner, mission_id, f"{mission_id}-courier", prompt
            )

            # Monitor check after navigation steps
            if should_monitor:
                monitor_prompt = (
                    f"Check safety conditions for courier {courier_id} on mission "
                    f"{mission_id}. Check battery and weather, then decide: "
                    f"CONTINUE or RETURN_TO_BASE."
                )
                if escalation_enabled:
                    monitor_prompt += " You may also decide ESCALATE if conditions are borderline."

                monitor_response = await _run_agent_turn(
                    monitor_runner, mission_id, f"{mission_id}-monitor", monitor_prompt
                )

                decision = _parse_monitor_decision(monitor_response)

                if decision == MonitorDecision.RETURN_TO_BASE:
                    # Navigate back to warehouse
                    abort_prompt = (
                        f"ABORT: Return courier {courier_id} to base at "
                        f"lat={WAREHOUSE.lat}, lng={WAREHOUSE.lng} for mission {mission_id}. "
                        f"Use navigate_to with courier_id='{courier_id}', mission_id='{mission_id}', "
                        f"target_lat={WAREHOUSE.lat}, target_lng={WAREHOUSE.lng}, leg='pickup', steps=8."
                    )
                    await _run_agent_turn(
                        courier_runner, mission_id, f"{mission_id}-courier", abort_prompt
                    )
                    return f"Mission {mission_id} aborted — {courier_id} returned to base"

                if decision == MonitorDecision.ESCALATE and escalation_enabled:
                    # Log the escalation
                    await workflow.execute_activity(
                        request_human_approval,
                        HumanApprovalInput(
                            mission_id=mission_id,
                            reason=monitor_response,
                        ),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=fast_retry,
                    )
                    # Wait for human decision signal
                    await workflow.wait_condition(
                        lambda: self._human_decision is not None
                    )
                    human_choice = self._human_decision
                    self._human_decision = None

                    if human_choice == "abort":
                        abort_prompt = (
                            f"ABORT: Return courier {courier_id} to base at "
                            f"lat={WAREHOUSE.lat}, lng={WAREHOUSE.lng} for mission {mission_id}. "
                            f"Use navigate_to with courier_id='{courier_id}', mission_id='{mission_id}', "
                            f"target_lat={WAREHOUSE.lat}, target_lng={WAREHOUSE.lng}, leg='pickup', steps=8."
                        )
                        await _run_agent_turn(
                            courier_runner, mission_id, f"{mission_id}-courier", abort_prompt
                        )
                        return f"Mission {mission_id} aborted by human decision — {courier_id} returned to base"

                # CONTINUE — proceed to next step

        return f"Mission {mission_id} completed by {courier_id}"

    async def _run_mock(self, mission_id: str) -> str:
        """Hardcoded fallback when no GOOGLE_API_KEY is set."""
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            maximum_attempts=10,
        )
        fast_retry = RetryPolicy(maximum_attempts=5)

        # Step 1: Assign a courier
        assignment: AssignCourierOutput = await workflow.execute_activity(
            assign_courier,
            AssignCourierInput(mission_id=mission_id),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=fast_retry,
        )
        courier_id = assignment.courier_id

        # Step 2: Navigate to warehouse (pickup point)
        await workflow.execute_activity(
            navigate_to,
            NavigateInput(
                courier_id=courier_id,
                mission_id=mission_id,
                target_lat=WAREHOUSE.lat,
                target_lng=WAREHOUSE.lng,
                leg=LegType.PICKUP,
                steps=2,  # short trip
            ),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=retry_policy,
        )

        # Step 3: Pick up package
        await workflow.execute_activity(
            pickup_package,
            PackageInput(courier_id=courier_id, mission_id=mission_id),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=fast_retry,
        )

        # Step 4: Navigate to delivery destination
        dest = DELIVERY_DESTINATIONS[mission_id]["coords"]
        await workflow.execute_activity(
            navigate_to,
            NavigateInput(
                courier_id=courier_id,
                mission_id=mission_id,
                target_lat=dest.lat,
                target_lng=dest.lng,
                leg=LegType.DELIVERY,
                steps=10,  # ~8 seconds of flight — plenty of time to kill
            ),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=retry_policy,
        )

        # Step 5: Deliver package
        await workflow.execute_activity(
            deliver_package,
            PackageInput(courier_id=courier_id, mission_id=mission_id),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=fast_retry,
        )

        return f"Mission {mission_id} completed by {courier_id}"


@dataclass
class FleetDispatchInput:
    """Input for FleetDispatchWorkflow."""
    mission_ids: list[str]
    escalation_enabled: bool = False


@workflow.defn
class FleetDispatchWorkflow:
    """
    Dispatches all pending missions.

    ADK path: uses Dispatch Agent to reason about assignments, then starts
    child DeliveryMissionWorkflow per mission.
    Mock path: starts child workflows directly (assignment happens inside).
    """

    @workflow.run
    async def run(self, inp: FleetDispatchInput) -> list[str]:
        if _MOCK_MODE:
            return await self._run_mock(inp.mission_ids)
        return await self._run_adk(inp.mission_ids, inp.escalation_enabled)

    async def _run_adk(self, mission_ids: list[str], escalation_enabled: bool) -> list[str]:
        # Create dispatch agent
        dispatch_agent = create_dispatch_agent()
        session_service = InMemorySessionService()
        dispatch_runner = Runner(
            agent=dispatch_agent,
            app_name="fleet_demo",
            session_service=session_service,
        )
        await session_service.create_session(
            app_name="fleet_demo", user_id="dispatch", session_id="dispatch-session"
        )

        # Ask dispatch agent to assign missions
        missions_list = ", ".join(mission_ids)
        dispatch_prompt = (
            f"You have the following pending missions: {missions_list}. "
            f"First check the fleet status, then assign each mission to an "
            f"available courier. Assign courier-1 to {mission_ids[0]} and "
            f"courier-2 to {mission_ids[1] if len(mission_ids) > 1 else mission_ids[0]}."
        )
        await _run_agent_turn(
            dispatch_runner, "dispatch", "dispatch-session", dispatch_prompt
        )

        # Start child workflows for each mission (staggered)
        handles = []
        for i, mid in enumerate(mission_ids):
            if i > 0:
                await workflow.sleep(timedelta(seconds=3))

            handle = await workflow.start_child_workflow(
                DeliveryMissionWorkflow.run,
                DeliveryMissionInput(
                    mission_id=mid,
                    escalation_enabled=escalation_enabled,
                ),
                id=f"delivery-{mid}",
            )
            handles.append(handle)

        results = await asyncio.gather(*handles)
        return list(results)

    async def _run_mock(self, mission_ids: list[str]) -> list[str]:
        """Hardcoded fallback — starts child workflows directly."""
        handles = []
        for i, mid in enumerate(mission_ids):
            if i > 0:
                await workflow.sleep(timedelta(seconds=3))

            handle = await workflow.start_child_workflow(
                DeliveryMissionWorkflow.run,
                DeliveryMissionInput(mission_id=mid),
                id=f"delivery-{mid}",
            )
            handles.append(handle)

        results = await asyncio.gather(*handles)
        return list(results)
