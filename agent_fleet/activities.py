"""
Temporal activities for courier operations.

Each activity is a discrete, retryable unit of work. When the worker dies
mid-activity, Temporal will retry it on the next available worker — this is
the core of the failure/recovery demo.
"""

from __future__ import annotations

import asyncio

from temporalio import activity

from agent_fleet.models import (
    NavigateInput,
    NavigateOutput,
    PackageInput,
    PackageOutput,
    AssignCourierInput,
    AssignCourierOutput,
    AssignCourierToMissionInput,
    AssignCourierToMissionOutput,
    FleetStatusInput,
    FleetStatusOutput,
    CheckBatteryInput,
    CheckBatteryOutput,
    CheckWeatherInput,
    CheckWeatherOutput,
    HumanApprovalInput,
    HumanApprovalOutput,
    GetMissionAssignmentInput,
    GetMissionAssignmentOutput,
    CourierStatus,
    LegType,
    MissionStatus,
)
from agent_fleet.simulation import fleet


@activity.defn(name="assign_courier")
async def assign_courier(inp: AssignCourierInput) -> AssignCourierOutput:
    """Find an idle courier and assign it to a mission."""
    courier_id = await fleet.get_idle_courier()
    if courier_id is None:
        raise RuntimeError("No idle couriers available")

    await fleet.assign_mission(inp.mission_id, courier_id)
    await fleet.set_courier_status(courier_id, CourierStatus.IDLE, inp.mission_id)
    activity.logger.info(f"Assigned {courier_id} to {inp.mission_id}")
    return AssignCourierOutput(courier_id=courier_id, mission_id=inp.mission_id)


@activity.defn(name="navigate_to")
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate courier navigation by interpolating position over N steps.

    This activity heartbeats on each step. If the worker is killed mid-
    navigation, the heartbeat timeout fires, Temporal marks it failed,
    and retries on the next worker — resuming the mission.
    """
    if not await fleet.courier_exists(inp.courier_id):
        raise ValueError(f"Unknown courier: {inp.courier_id}")

    status = (
        CourierStatus.EN_ROUTE_PICKUP
        if inp.leg == LegType.PICKUP
        else CourierStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_courier_status(inp.courier_id, status, inp.mission_id)
    await fleet.update_mission_status(
        inp.mission_id,
        MissionStatus.IN_PROGRESS,
        f"Courier {inp.courier_id} navigating to {inp.leg} point",
    )

    start_lat, start_lng = await fleet.get_courier_position(inp.courier_id)

    for step in range(1, inp.steps + 1):
        # Heartbeat — tells Temporal "I'm still alive"
        activity.heartbeat(f"step {step}/{inp.steps}")

        fraction = step / inp.steps
        new_lat = start_lat + (inp.target_lat - start_lat) * fraction
        new_lng = start_lng + (inp.target_lng - start_lng) * fraction

        await fleet.update_courier_position(inp.courier_id, new_lat, new_lng)

        # Battery drain: ~2% per nav step
        await fleet.drain_battery(inp.courier_id, 2.0)

        # Track nav steps for demo event triggers
        await fleet.increment_nav_step(inp.courier_id)

        # Simulate flight time — 0.8s per step gives ~6s total nav
        await asyncio.sleep(0.8)

    activity.logger.info(
        f"{inp.courier_id} arrived at {inp.leg} "
        f"({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        courier_id=inp.courier_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn(name="pickup_package")
async def pickup_package(inp: PackageInput) -> PackageOutput:
    """Simulate picking up a package at the warehouse."""
    await fleet.set_courier_status(
        inp.courier_id, CourierStatus.PICKING_UP, inp.mission_id
    )
    await fleet.update_mission_status(
        inp.mission_id, MissionStatus.IN_PROGRESS, "Picking up package"
    )

    # Simulate loading time
    await asyncio.sleep(1.5)

    activity.logger.info(f"{inp.courier_id} picked up package for {inp.mission_id}")
    return PackageOutput(
        courier_id=inp.courier_id, mission_id=inp.mission_id, success=True
    )


@activity.defn(name="deliver_package")
async def deliver_package(inp: PackageInput) -> PackageOutput:
    """Simulate delivering a package at the destination."""
    await fleet.set_courier_status(
        inp.courier_id, CourierStatus.DELIVERING, inp.mission_id
    )
    await fleet.update_mission_status(
        inp.mission_id, MissionStatus.IN_PROGRESS, "Delivering package"
    )

    # Simulate drop-off time
    await asyncio.sleep(1.5)

    await fleet.set_courier_status(inp.courier_id, CourierStatus.IDLE)
    await fleet.update_mission_status(
        inp.mission_id, MissionStatus.COMPLETED, "Delivered successfully!"
    )

    activity.logger.info(f"{inp.courier_id} delivered {inp.mission_id}")
    return PackageOutput(
        courier_id=inp.courier_id, mission_id=inp.mission_id, success=True
    )


# --- New activities for agent-driven workflows ---


@activity.defn(name="get_fleet_status")
async def get_fleet_status(inp: FleetStatusInput) -> FleetStatusOutput:
    """Return a formatted fleet status summary for LLM consumption."""
    snapshot = await fleet.snapshot()
    lines = ["Fleet Status:"]
    for cid, c in snapshot["couriers"].items():
        lines.append(
            f"  {cid}: status={c['status']}, battery={c['battery_pct']:.0f}%, "
            f"mission={c['current_mission_id'] or 'none'}"
        )
    lines.append("Missions:")
    for mid, m in snapshot["missions"].items():
        lines.append(
            f"  {mid}: status={m['status']}, "
            f"assigned_courier={m['assigned_courier_id'] or 'unassigned'}, "
            f"label={m['order_label']}"
        )
    return FleetStatusOutput(summary="\n".join(lines))


@activity.defn(name="assign_courier_to_mission")
async def assign_courier_to_mission(
    inp: AssignCourierToMissionInput,
) -> AssignCourierToMissionOutput:
    """Assign a specific courier to a specific mission."""
    if not await fleet.courier_exists(inp.courier_id):
        return AssignCourierToMissionOutput(
            courier_id=inp.courier_id, mission_id=inp.mission_id, success=False
        )

    await fleet.assign_mission(inp.mission_id, inp.courier_id)
    await fleet.set_courier_status(
        inp.courier_id, CourierStatus.IDLE, inp.mission_id
    )
    activity.logger.info(
        f"Assigned {inp.courier_id} to {inp.mission_id} (agent decision)"
    )
    return AssignCourierToMissionOutput(
        courier_id=inp.courier_id, mission_id=inp.mission_id, success=True
    )


@activity.defn(name="check_courier_battery")
async def check_courier_battery(inp: CheckBatteryInput) -> CheckBatteryOutput:
    """Read courier battery level. Critical if below 20%."""
    battery = await fleet.get_battery(inp.courier_id)
    is_critical = battery < 20.0
    activity.logger.info(
        f"Battery check for {inp.courier_id}: {battery:.0f}% "
        f"{'(CRITICAL)' if is_critical else '(OK)'}"
    )
    return CheckBatteryOutput(battery_pct=battery, is_critical=is_critical)


@activity.defn(name="check_weather")
async def check_weather(inp: CheckWeatherInput) -> CheckWeatherOutput:
    """Read weather conditions for a courier's current location."""
    condition = await fleet.get_weather(inp.courier_id)
    safe = condition != "storm"
    activity.logger.info(
        f"Weather check for {inp.courier_id}: {condition} "
        f"({'safe' if safe else 'UNSAFE'})"
    )
    return CheckWeatherOutput(condition=condition, safe_to_fly=safe)


@activity.defn(name="request_human_approval")
async def request_human_approval(
    inp: HumanApprovalInput,
) -> HumanApprovalOutput:
    """Marker activity that logs an escalation request. Actual wait is signal-based."""
    activity.logger.info(
        f"Human approval requested for {inp.mission_id}: {inp.reason}"
    )
    return HumanApprovalOutput(approved=False, decision="pending")


@activity.defn(name="get_mission_assignment")
async def get_mission_assignment(
    inp: GetMissionAssignmentInput,
) -> GetMissionAssignmentOutput:
    """Read which courier is assigned to a mission. Used by workflows to
    avoid reading fleet state directly (determinism-safe)."""
    courier_id = await fleet.get_mission_courier(inp.mission_id)
    return GetMissionAssignmentOutput(
        mission_id=inp.mission_id, courier_id=courier_id
    )
