"""
Temporal activities for the Meltdown ice cream delivery demo.

Each activity is a discrete, retryable unit of work. Activities handle:
- Driver navigation with heartbeats
- Order pickup/delivery
- Fleet status queries (for LLM agents)
- Customer change execution
"""

import asyncio
import math

import httpx
from temporalio import activity

from agent_fleet.config import GOOGLE_MAPS_API_KEY
from agent_fleet.locations import generate_random_order
from agent_fleet.models import (
    DeliverInput,
    DeliverOutput,
    DriverStatus,
    ExecuteCustomerChangeInput,
    ExecuteCustomerChangeOutput,
    GenerateOrderInput,
    GenerateOrderOutput,
    GetFleetStatusInput,
    GetFleetStatusOutput,
    GetOrderPrioritiesInput,
    GetOrderPrioritiesOutput,
    LegType,
    NavigateInput,
    NavigateOutput,
    OrderStatus,
    PickupInput,
    PickupOutput,
    PublishAgentEventInput,
    PublishAgentEventOutput,
)
from agent_fleet.simulation import fleet

# --- Polyline decoding and route fetching ---


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    """Decode a Google Maps encoded polyline string into (lat, lng) tuples."""
    points = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        # Decode latitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # Decode longitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        points.append((lat / 1e5, lng / 1e5))

    return points


@activity.defn
async def get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Fetch route waypoints from Google Maps Directions API (decoded polyline).

    Returns a list of {"lat": float, "lng": float} waypoints.
    Failures propagate to Temporal's retry mechanism.
    """
    origin = f"{origin_lat},{origin_lng}"
    destination = f"{dest_lat},{dest_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    encoded = data["routes"][0]["overview_polyline"]["points"]
    decoded = decode_polyline(encoded)
    activity.logger.info(f"[NAV] Google Maps polyline: {len(decoded)} points")
    return [{"lat": lat, "lng": lng} for lat, lng in decoded]


# --- Flat-signature tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def tool_get_fleet_status() -> str:
    """Check current fleet state: Driver positions, cooler conditions, orders.

    Fails when Fleet Agent is disconnected — Temporal retries until reconnected.
    """
    if await fleet.is_agent_disconnected("fleet_agent"):
        raise RuntimeError("Fleet Agent is disconnected — tool unavailable")
    return await fleet.get_fleet_summary()


@activity.defn
async def tool_get_order_priorities() -> str:
    """Check order priority details: VIP vs standard, deadlines, servings."""
    return await fleet.get_order_priorities_summary()


@activity.defn
async def tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
) -> str:
    """Get driving route info between two points using Google Maps Directions API.

    Fails when Fleet Agent is disconnected — Temporal retries until reconnected.
    Returns distance, duration, and step-by-step directions.
    Use this to assess reroute feasibility and ETAs for Driver dispatching.
    Failures propagate to Temporal's retry mechanism.

    Args:
        origin_lat: Starting latitude
        origin_lng: Starting longitude
        destination_lat: Destination latitude
        destination_lng: Destination longitude
        destination_name: Human-readable name of the destination (e.g. "MGM Grand")
    """
    if await fleet.is_agent_disconnected("fleet_agent"):
        raise RuntimeError("Fleet Agent is disconnected — tool unavailable")

    import re

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{destination_lat},{destination_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    route = data["routes"][0]
    leg = route["legs"][0]
    distance = leg["distance"]["text"]
    duration = leg["duration"]["text"]
    eta_minutes = max(1, leg["duration"]["value"] // 60)

    steps = []
    for i, step in enumerate(leg["steps"][:5], 1):
        instruction = step["html_instructions"]
        instruction = re.sub(r"<[^>]+>", " ", instruction).strip()
        steps.append(f"  {i}. {instruction} ({step['distance']['text']})")

    dest_label = destination_name or f"({destination_lat:.4f}, {destination_lng:.4f})"
    steps_text = "\n".join(steps)
    return (
        f"Route to {dest_label}:\n"
        f"  Distance: {distance}\n"
        f"  ETA: {duration}\n"
        f"  ETA_MINUTES: {eta_minutes}\n"
        f"  Key directions:\n{steps_text}"
    )


# --- Core delivery activities ---


@activity.defn
async def generate_order(inp: GenerateOrderInput) -> GenerateOrderOutput:
    """Generate a random order from the venue pool and register it in fleet state."""
    order_data = generate_random_order(inp.order_number)

    await fleet.register_order(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_coords=order_data["coords"],
        deadline_minutes=order_data["deadline_minutes"],
    )

    activity.logger.info(f"Generated {order_data['order_id']}: {order_data['label']}")
    return GenerateOrderOutput(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_lat=order_data["coords"].lat,
        delivery_lng=order_data["coords"].lng,
        deadline_minutes=order_data["deadline_minutes"],
        event=order_data["event"],
    )


@activity.defn
async def register_assignment(driver_id: str, order_id: str) -> str:
    """Register an ADK-decided assignment in fleet state (UI projection)."""
    await fleet.assign_order_to_driver(driver_id, order_id)
    return f"Assigned {order_id} to {driver_id}"


@activity.defn
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate Driver navigation by interpolating position over N steps.

    The driver always completes navigation (truck keeps moving on the road).
    Disconnect is checked at start (fail-fast on retry while still disconnected)
    and at end (simulates "arrived but can't report back"). Temporal retries
    until reconnected.
    """
    # Fail-fast on retry if still disconnected — don't re-drive the whole route
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} still disconnected — waiting for reconnect")

    leg = inp.leg if isinstance(inp.leg, str) else str(inp.leg)
    status = (
        DriverStatus.EN_ROUTE_PICKUP
        if leg == LegType.PICKUP.value
        else DriverStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_driver_status(inp.driver_id, status)
    # Skip order status update for return-to-base trips (no real order)
    if inp.order_id and inp.order_id != "return":
        await fleet.update_order_status(
            inp.order_id,
            OrderStatus.IN_TRANSIT,
            f"En route to {leg}",
        )

    # Read actual position from FleetState — handles retry after disconnect where
    # the driver may have moved (completed navigation) but the workflow didn't get
    # the result. On first attempt this matches the workflow's position. On retry
    # after disconnect it picks up from where the truck actually is on the map.
    start_lat, start_lng = await fleet.get_driver_position(inp.driver_id)

    # If already at destination (e.g., retry after completing navigation but failing
    # the end check), skip driving — just report arrival.
    dist_to_target = math.sqrt(
        (start_lat - inp.target_lat) ** 2 + (start_lng - inp.target_lng) ** 2
    )
    if dist_to_target < 0.001:
        activity.logger.info(f"{inp.driver_id} already at {leg} destination — skipping navigation")
        return NavigateOutput(
            driver_id=inp.driver_id,
            arrived=True,
            final_lat=inp.target_lat,
            final_lng=inp.target_lng,
        )

    # Build the path to interpolate along
    if inp.waypoints and len(inp.waypoints) >= 2:
        path = [(wp["lat"], wp["lng"]) for wp in inp.waypoints]
    else:
        path = [(start_lat, start_lng), (inp.target_lat, inp.target_lng)]

    # Calculate cumulative distances along the path for proportional interpolation
    segment_dists = []
    for i in range(1, len(path)):
        d = math.sqrt((path[i][0] - path[i - 1][0]) ** 2 + (path[i][1] - path[i - 1][1]) ** 2)
        segment_dists.append(d)
    total_dist = sum(segment_dists) or 1e-9

    # Driver always completes the drive — truck doesn't stop mid-road
    for step in range(1, inp.steps + 1):
        activity.heartbeat(f"step {step}/{inp.steps}")

        fraction = step / inp.steps
        target_dist = fraction * total_dist

        accumulated = 0.0
        new_lat, new_lng = path[-1]
        for i, seg_d in enumerate(segment_dists):
            if accumulated + seg_d >= target_dist:
                remaining = target_dist - accumulated
                seg_frac = remaining / seg_d if seg_d > 0 else 1.0
                new_lat = path[i][0] + (path[i + 1][0] - path[i][0]) * seg_frac
                new_lng = path[i][1] + (path[i + 1][1] - path[i][1]) * seg_frac
                break
            accumulated += seg_d

        await fleet.update_driver_position(inp.driver_id, new_lat, new_lng)
        await asyncio.sleep(0.4)

    # Driver arrived — but if disconnected, can't report back.
    # This simulates "delivery complete but comms lost."
    # Temporal sees the failure and retries until reconnected.
    if await fleet.is_driver_disconnected(inp.driver_id):
        activity.logger.warning(
            f"{inp.driver_id} arrived at {leg} but is disconnected — cannot report"
        )
        raise RuntimeError(
            f"Driver {inp.driver_id} arrived but is disconnected — cannot check in"
        )

    activity.logger.info(
        f"{inp.driver_id} arrived at {leg} ({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        driver_id=inp.driver_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn
async def pickup_orders(inp: PickupInput) -> PickupOutput:
    """Simulate picking up ice cream orders at the kitchen.

    Driver physically picks up, then checks connection to report.
    If disconnected, Temporal retries until reconnected.
    """
    await fleet.set_driver_status(inp.driver_id, DriverStatus.PICKING_UP)
    for oid in inp.order_ids:
        await fleet.update_order_status(oid, OrderStatus.PICKED_UP, "Picked up")

    await asyncio.sleep(1.5)

    # Pickup done — but can't report if disconnected
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} picked up but cannot report — disconnected")

    activity.logger.info(f"{inp.driver_id} picked up orders {inp.order_ids}")
    return PickupOutput(driver_id=inp.driver_id, success=True)


@activity.defn
async def deliver_order(inp: DeliverInput) -> DeliverOutput:
    """Simulate delivering an ice cream order at a hotel.

    Driver physically delivers, then checks connection to report.
    If disconnected, Temporal retries until reconnected.
    """
    await fleet.set_driver_status(inp.driver_id, DriverStatus.DELIVERING)
    await fleet.update_order_status(inp.order_id, OrderStatus.IN_TRANSIT, "Delivering")

    await asyncio.sleep(1.5)

    # UI projection — mark order delivered and update driver status
    remaining_count = await fleet.complete_order_delivery(inp.driver_id, inp.order_id)
    if remaining_count == 0:
        await fleet.set_driver_status(inp.driver_id, DriverStatus.IDLE)

    # Delivery done — but can't report if disconnected
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} delivered but cannot report — disconnected")

    activity.logger.info(f"{inp.driver_id} delivered {inp.order_id}")
    return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=True)


# --- Agent tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def get_fleet_status(inp: GetFleetStatusInput) -> GetFleetStatusOutput:
    """Return fleet status summary for Fleet Agent consumption."""
    summary = await fleet.get_fleet_summary()
    return GetFleetStatusOutput(summary=summary)


@activity.defn
async def get_order_priorities(
    inp: GetOrderPrioritiesInput,
) -> GetOrderPrioritiesOutput:
    """Return order priority details for Customer Agent consumption."""
    summary = await fleet.get_order_priorities_summary()
    return GetOrderPrioritiesOutput(summary=summary)


@activity.defn
async def publish_agent_event(
    inp: PublishAgentEventInput,
) -> PublishAgentEventOutput:
    """Publish an agent reasoning event to the UI panel."""
    await fleet.publish_agent_event(
        inp.agent_name, inp.event_type, inp.content, summary=inp.summary
    )
    return PublishAgentEventOutput(success=True)


# --- Customer change activities ---


@activity.defn
async def execute_customer_change(
    inp: ExecuteCustomerChangeInput,
) -> ExecuteCustomerChangeOutput:
    """Execute a customer-initiated change (address update or cancellation)."""
    if inp.change_type == "cancel":
        await fleet.cancel_order(inp.order_id)
        activity.logger.info(f"Order {inp.order_id} cancelled")
    elif (
        inp.change_type == "address_change" and inp.new_lat is not None and inp.new_lng is not None
    ):
        await fleet.update_order_delivery(inp.order_id, inp.new_lat, inp.new_lng, inp.new_hotel)
        await fleet.update_order_status(
            inp.order_id, OrderStatus.REROUTED, f"Rerouted to {inp.new_hotel or 'new address'}"
        )
        dest = inp.new_hotel or f"({inp.new_lat:.4f}, {inp.new_lng:.4f})"
        activity.logger.info(f"Order {inp.order_id} rerouted to {dest}")

    return ExecuteCustomerChangeOutput(success=True)
