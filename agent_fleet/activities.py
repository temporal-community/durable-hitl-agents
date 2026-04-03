"""
Temporal activities for the Meltdown ice cream delivery demo.

Each activity is a discrete, retryable unit of work. Activities handle:
- AI-Crew navigation with heartbeats
- Order pickup/delivery
- Fleet status queries (for LLM agents)
- Customer change execution
"""

from __future__ import annotations

import asyncio
import math

import httpx
from temporalio import activity

from agent_fleet.config import GOOGLE_API_KEY, GOOGLE_CSE_ID, GOOGLE_MAPS_API_KEY
from agent_fleet.locations import VENUES_BY_HOTEL, generate_random_order
from agent_fleet.models import (
    CrewStatus,
    DeliverInput,
    DeliverOutput,
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
    ReasonAboutAssignmentInput,
    ReasonAboutAssignmentOutput,
    SyncCrewDisconnectInput,
)
from agent_fleet.simulation import fleet

# --- Polyline decoding and route fetching ---

# Known locations for mock waypoint generation (Las Vegas Strip)
_STRIP_POINTS = {
    "warehouse": (36.1280, -115.1530),
    "caesars": (36.1162, -115.1745),
    "mgm": (36.1024, -115.1696),
    "mandalay": (36.0919, -115.1761),
}

# Intermediate points along Las Vegas Blvd from warehouse heading south
# Las Vegas Blvd S — anchor points verified against map tiles + interpolated
# The strip runs SSE from Venetian, bends at Flamingo, then curves SW to Mandalay
# Coordinates placed ON the road centerline as shown on CartoDB/Stadia tiles
_STRIP_CORRIDOR = [
    # --- Paradise Rd to the Strip (new shop location east of the Strip) ---
    # Frosty's Ice Cream on Paradise Rd near Convention Center
    (36.12800, -115.15300),
    # Head west on Convention Center Dr / Desert Inn Rd toward the Strip
    (36.12800, -115.15500),
    (36.12800, -115.15700),
    (36.12800, -115.15900),
    (36.12800, -115.16100),
    (36.12800, -115.16300),
    (36.12800, -115.16500),
    (36.12800, -115.16700),
    (36.12800, -115.16900),
    # Reach Las Vegas Blvd and turn south
    (36.12700, -115.17050),
    (36.12500, -115.17080),
    (36.12350, -115.17090),
    # Venetian / Palazzo — LV Blvd here is at ~-115.1710
    (36.12200, -115.17100),
    (36.12150, -115.17105),
    (36.12100, -115.17110),
    (36.12050, -115.17120),
    (36.12000, -115.17130),
    # LINQ / Harrah's
    (36.11950, -115.17150),
    (36.11900, -115.17170),
    (36.11850, -115.17200),
    (36.11800, -115.17220),
    # Flamingo intersection — road at ~-115.1726
    (36.11750, -115.17240),
    (36.11700, -115.17260),
    # Caesars Palace (marker: 36.1162, -115.1745)
    (36.11670, -115.17300),
    (36.11650, -115.17350),
    (36.11620, -115.17450),  # Caesars marker
    (36.11580, -115.17460),
    (36.11540, -115.17470),
    # Bellagio — road at ~-115.1742
    (36.11500, -115.17420),
    (36.11450, -115.17410),
    (36.11400, -115.17400),
    (36.11350, -115.17390),
    (36.11300, -115.17380),
    (36.11250, -115.17370),
    (36.11200, -115.17360),
    # Cosmopolitan
    (36.11150, -115.17355),
    (36.11100, -115.17350),
    (36.11050, -115.17350),
    (36.11000, -115.17350),
    (36.10940, -115.17350),
    # CityCenter / Aria — road bends slightly east
    (36.10880, -115.17340),
    (36.10830, -115.17330),
    (36.10780, -115.17310),
    (36.10730, -115.17300),
    (36.10680, -115.17290),
    (36.10630, -115.17280),
    # Park MGM
    (36.10580, -115.17270),
    (36.10530, -115.17265),
    (36.10480, -115.17260),
    (36.10430, -115.17255),
    # Tropicana / MGM Grand (marker: 36.1024, -115.1725)
    (36.10380, -115.17250),
    (36.10330, -115.17250),
    (36.10280, -115.17250),
    (36.10240, -115.17250),  # MGM Grand marker
    (36.10200, -115.17250),
    (36.10150, -115.17260),
    (36.10100, -115.17270),
    (36.10050, -115.17290),
    # South of MGM — road curves southwest
    (36.10000, -115.17310),
    (36.09950, -115.17330),
    (36.09900, -115.17360),
    (36.09850, -115.17390),
    (36.09800, -115.17420),
    # Excalibur / Luxor
    (36.09750, -115.17450),
    (36.09700, -115.17480),
    (36.09650, -115.17500),
    (36.09600, -115.17520),
    (36.09550, -115.17540),
    (36.09500, -115.17560),
    # Mandalay Bay approach (marker: 36.0919, -115.1761)
    (36.09450, -115.17570),
    (36.09400, -115.17580),
    (36.09350, -115.17590),
    (36.09300, -115.17600),
    (36.09250, -115.17610),
    (36.09190, -115.17610),  # Mandalay Bay marker
]


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


def _mock_route_waypoints(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Generate mock waypoints that follow the Las Vegas Strip corridor.

    Finds the closest corridor points to origin and destination, then returns
    the slice of the corridor between them (plus origin/dest endpoints).
    """

    def _closest_corridor_idx(lat: float, lng: float) -> int:
        best_idx = 0
        best_dist = float("inf")
        for i, (clat, clng) in enumerate(_STRIP_CORRIDOR):
            d = math.sqrt((lat - clat) ** 2 + (lng - clng) ** 2)
            if d < best_dist:
                best_dist = d
                best_idx = i
        return best_idx

    start_idx = _closest_corridor_idx(origin_lat, origin_lng)
    end_idx = _closest_corridor_idx(dest_lat, dest_lng)

    # Build waypoints: origin -> corridor slice -> destination
    waypoints = [{"lat": origin_lat, "lng": origin_lng}]

    if start_idx <= end_idx:
        corridor_slice = _STRIP_CORRIDOR[start_idx : end_idx + 1]
    else:
        corridor_slice = list(reversed(_STRIP_CORRIDOR[end_idx : start_idx + 1]))

    for clat, clng in corridor_slice:
        # Skip if too close to origin (already added)
        if len(waypoints) == 1:
            d = math.sqrt((clat - origin_lat) ** 2 + (clng - origin_lng) ** 2)
            if d < 0.0005:
                continue
        waypoints.append({"lat": clat, "lng": clng})

    # Add final destination if not already close to last waypoint
    last = waypoints[-1]
    d = math.sqrt((dest_lat - last["lat"]) ** 2 + (dest_lng - last["lng"]) ** 2)
    if d > 0.0005:
        waypoints.append({"lat": dest_lat, "lng": dest_lng})

    return waypoints


@activity.defn(name="get_route_polyline")
async def get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Fetch route waypoints from Google Maps Directions API (decoded polyline).

    Returns a list of {"lat": float, "lng": float} waypoints.
    Falls back to mock corridor waypoints if no API key is set.
    """
    api_key = GOOGLE_MAPS_API_KEY

    if not api_key:
        activity.logger.info("[NAV] No Maps API key — using mock corridor")
        return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{dest_lat},{dest_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": api_key,
        "mode": "driving",
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("routes"):
            activity.logger.warning(
                f"Maps API status for polyline: {data.get('status')}, using mock"
            )
            return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)

        # Decode the overview polyline
        encoded = data["routes"][0]["overview_polyline"]["points"]
        decoded = decode_polyline(encoded)
        activity.logger.info(f"[NAV] Using Google Maps polyline ({len(decoded)} points)")
        return [{"lat": lat, "lng": lng} for lat, lng in decoded]

    except Exception as e:
        activity.logger.warning(f"Maps polyline API error, using mock: {e}")
        return _mock_route_waypoints(origin_lat, origin_lng, dest_lat, dest_lng)


# --- Flat-signature tool activities (called by ADK agents via activity_tool) ---


@activity.defn(name="tool_get_fleet_status")
async def tool_get_fleet_status() -> str:
    """Check current fleet state: AI-Crew positions, cooler conditions, orders."""
    return await fleet.get_fleet_summary()


@activity.defn(name="tool_get_order_priorities")
async def tool_get_order_priorities() -> str:
    """Check order priority details: VIP vs standard, deadlines, servings."""
    return await fleet.get_order_priorities_summary()


@activity.defn(name="tool_publish_agent_event")
async def tool_publish_agent_event(
    agent_name: str, event_type: str, content: str, summary: str = ""
) -> str:
    """Publish a reasoning event to the operator UI panel."""
    await fleet.publish_agent_event(agent_name, event_type, content, summary=summary)
    return "Event published."


@activity.defn(name="tool_get_route_info")
async def tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
) -> str:
    """Get driving route info between two points using Google Maps Directions API.

    Returns distance, duration, and step-by-step directions.
    Use this to assess reroute feasibility and ETAs for AI-Crew dispatching.

    Args:
        origin_lat: Starting latitude
        origin_lng: Starting longitude
        destination_lat: Destination latitude
        destination_lng: Destination longitude
        destination_name: Human-readable name of the destination (e.g. "MGM Grand")
    """
    api_key = GOOGLE_MAPS_API_KEY

    if not api_key:
        # Mock fallback — deterministic response for demo
        return _mock_route_info(
            origin_lat, origin_lng, destination_lat, destination_lng, destination_name
        )

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{destination_lat},{destination_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": api_key,
        "mode": "driving",
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("routes"):
            activity.logger.warning(f"Maps API status: {data.get('status')}")
            return _mock_route_info(
                origin_lat, origin_lng, destination_lat, destination_lng, destination_name
            )

        route = data["routes"][0]
        leg = route["legs"][0]
        distance = leg["distance"]["text"]
        duration = leg["duration"]["text"]
        # Duration in minutes for structured agent reasoning
        eta_minutes = max(1, leg["duration"]["value"] // 60)

        steps = []
        for i, step in enumerate(leg["steps"][:5], 1):
            instruction = step["html_instructions"]
            # Strip HTML tags for clean text
            import re

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

    except Exception as e:
        activity.logger.warning(f"Maps API error, using mock: {e}")
        return _mock_route_info(
            origin_lat, origin_lng, destination_lat, destination_lng, destination_name
        )


def _mock_route_info(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    dest_name: str,
) -> str:
    """Deterministic mock route info for demo without Maps API key."""
    dlat = dest_lat - origin_lat
    dlng = dest_lng - origin_lng
    # Rough distance in miles (Las Vegas scale)
    dist_miles = math.sqrt(dlat**2 + dlng**2) * 69.0
    eta_minutes = max(3, int(dist_miles * 3.5))

    dest_label = dest_name or f"({dest_lat:.4f}, {dest_lng:.4f})"
    return (
        f"Route to {dest_label}:\n"
        f"  Distance: {dist_miles:.1f} mi\n"
        f"  ETA: {eta_minutes} mins\n"
        f"  ETA_MINUTES: {eta_minutes}\n"
        f"  Key directions:\n"
        f"    1. Head south on Las Vegas Blvd (0.5 mi)\n"
        f"    2. Continue on Las Vegas Blvd S ({max(0.1, dist_miles - 0.5):.1f} mi)\n"
        f"    3. Arrive at {dest_label}"
    )


@activity.defn(name="tool_search_hotel_context")
async def tool_search_hotel_context(hotel_name: str) -> str:
    """Search for live context about a Las Vegas hotel — current events, VIP bookings, reputation.

    Use this to understand delivery urgency for a specific hotel destination.

    Args:
        hotel_name: Name of the hotel (e.g. "MGM Grand", "Caesars Palace", "Mandalay Bay")
    """
    return await _search_hotel_context(hotel_name)


async def _search_hotel_context(hotel_name: str) -> str:
    """Search for hotel context — tries Google Search API, falls back to mock.

    This is the shared implementation used by both the tool_search_hotel_context
    activity and the mock resolver. Separated so it can be called from within
    another activity (activities can't call other activities via Temporal).
    """
    api_key = GOOGLE_API_KEY
    search_engine_id = GOOGLE_CSE_ID

    if api_key and search_engine_id:
        try:
            query = f"{hotel_name} Las Vegas current events today"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                "key": api_key,
                "cx": search_engine_id,
                "q": query,
                "num": 3,
            }
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            items = data.get("items", [])
            if items:
                results = []
                for item in items[:3]:
                    title = item.get("title", "")
                    snippet = item.get("snippet", "")
                    results.append(f"- {title}: {snippet}")
                return f"Live search results for {hotel_name}:\n" + "\n".join(results)
        except Exception:
            pass

    return _mock_hotel_context(hotel_name)


def _mock_hotel_context(hotel_name: str) -> str:
    """Deterministic mock hotel context for demo without Search API."""
    contexts = {
        "MGM Grand": (
            f"- {hotel_name}: Currently hosting Wet Republic pool party series. "
            f"High guest volume with VIP catering expectations.\n"
            f"- {hotel_name}: Grand Garden Arena has a major event tonight — "
            f"hotel is at peak occupancy with elevated service standards."
        ),
        "Caesars Palace": (
            f"- {hotel_name}: Banquet halls booked for a corporate gala tonight. "
            f"Caesars is known for premium event standards.\n"
            f"- {hotel_name}: Colosseum show tonight means 4,000+ guests on property."
        ),
        "Mandalay Bay": (
            f"- {hotel_name}: Tech conference in session at the Convention Center. "
            f"Conference catering is time-sensitive — dessert course is scheduled.\n"
            f"- {hotel_name}: VIP-only venue — all orders treated as highest priority."
        ),
    }
    # Fuzzy match hotel name
    for key, context in contexts.items():
        if key.lower() in hotel_name.lower() or hotel_name.lower() in key.lower():
            return f"Hotel intelligence for {hotel_name}:\n{context}"
    return f"No specific intelligence available for {hotel_name}."


# --- Core delivery activities ---


@activity.defn(name="generate_order")
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


@activity.defn(name="reason_about_assignment")
async def reason_about_assignment(
    inp: ReasonAboutAssignmentInput,
) -> ReasonAboutAssignmentOutput:
    """
    Multi-agent reasoning to decide which crew should handle a new order.

    Fleet Agent assesses crew positions and capacity.
    Customer Agent evaluates order priority and urgency.
    Resolver synthesizes and picks the best crew.

    All decision inputs come from inp (workflow state) — not from FleetState.
    FleetState writes are UI projection only.
    """
    # --- Fleet Agent: find best crew from workflow-provided snapshots ---
    fleet_agent_offline = "fleet_agent" in inp.disconnected_agents

    best_crew = None
    best_dist = float("inf")
    fleet_lines = []
    for crew in inp.crew_snapshots:
        available = crew.capacity - crew.current_order_count
        dist = math.sqrt((crew.lat - inp.delivery_lat) ** 2 + (crew.lng - inp.delivery_lng) ** 2)
        dist_miles = dist * 69.0
        eta_min = max(2, int(dist_miles * 3.5))
        status_tag = ""
        if crew.is_disconnected:
            status_tag = " [DISCONNECTED]"

        fleet_lines.append(
            f"  {crew.crew_id}: {available} slots free, ~{eta_min}min ETA, "
            f"status={crew.status}{status_tag}"
        )

        # Skip crews that can't take orders
        if crew.is_disconnected or available <= 0:
            continue
        if dist < best_dist:
            best_dist = dist
            best_crew = crew.crew_id

    fleet_text = "\n".join(fleet_lines)
    if best_crew is None:
        # Fallback: pick any crew with capacity even if busy
        best_crew = "ai-crew-1"

    best_eta = max(2, int(best_dist * 69.0 * 3.5))

    if fleet_agent_offline:
        # Fleet Agent is offline — publish offline notice and skip its assessment
        await fleet.publish_agent_event(
            "fleet_agent",
            "offline",
            "Fleet Agent is OFFLINE — unable to provide fleet assessment. "
            "Resolver will assign based on available data.",
            summary="Fleet Agent offline",
        )
        await asyncio.sleep(0.2)
    else:
        await fleet.publish_agent_event(
            "fleet_agent",
            "tool_call",
            f"New order {inp.order_id} for {inp.hotel} ({inp.event}). "
            f"Checking fleet positions and capacity...",
            summary=f"New order for {inp.hotel} — checking fleet...",
        )
        await asyncio.sleep(0.4)

        await fleet.publish_agent_event(
            "fleet_agent",
            "assessment",
            f"Fleet scan for {inp.order_id} ({inp.hotel}):\n{fleet_text}\n\n"
            f"RECOMMENDATION: {best_crew} — closest with capacity, "
            f"~{best_eta}min ETA.",
            summary=f"{best_crew} recommended — ~{best_eta}min ETA",
        )
        await asyncio.sleep(0.3)

    # --- Customer Agent: priority assessment ---
    customer_agent_offline = "customer_agent" in inp.disconnected_agents
    urgency = (
        "URGENT"
        if inp.deadline_minutes <= 25
        else ("TIGHT" if inp.deadline_minutes <= 35 else "comfortable")
    )
    venue_info = VENUES_BY_HOTEL.get(inp.hotel, {})
    vip_tier = venue_info.get("vip_tier", "standard")

    if customer_agent_offline:
        await fleet.publish_agent_event(
            "customer_agent",
            "offline",
            "Customer Agent is OFFLINE — unable to assess customer priority. "
            "Resolver will use order metadata for prioritization.",
            summary="Customer Agent offline",
        )
        await asyncio.sleep(0.2)
    else:
        await fleet.publish_agent_event(
            "customer_agent",
            "assessment",
            f"Order {inp.order_id}: {inp.hotel} {inp.event}\n"
            f"  Priority: {inp.priority.upper()} ({vip_tier} tier)\n"
            f"  Servings: {inp.servings}, Deadline: "
            f"{inp.deadline_minutes}min [{urgency}]\n\n"
            f"{'High-profile event — on-time delivery critical.' if urgency != 'comfortable' else 'Standard priority — normal delivery timeline.'}",  # noqa: E501
            summary=f"{inp.priority.upper()} order, {urgency} deadline",
        )
        await asyncio.sleep(0.3)

    # --- Resolver: synthesize and assign ---
    offline_agents = []
    if fleet_agent_offline:
        offline_agents.append("Fleet Agent")
    if customer_agent_offline:
        offline_agents.append("Customer Agent")

    if offline_agents:
        offline_list = " and ".join(offline_agents)
        if fleet_agent_offline and customer_agent_offline:
            resolver_context = (
                f"DEGRADED: {offline_list} offline.\n"
                f"  Falling back to last-known crew positions + order metadata.\n"
                f"  Picking nearest crew with capacity as best-effort."
            )
        elif fleet_agent_offline:
            resolver_context = (
                "DEGRADED: Fleet Agent offline — no live fleet scan.\n"
                "  Using last-known positions to pick nearest crew.\n"
                "  Customer priority assessment still available."
            )
        else:
            resolver_context = (
                "DEGRADED: Customer Agent offline — no priority assessment.\n"
                "  Fleet positions confirmed. Using order metadata for priority."
            )
        resolver_summary = f"{inp.order_id} -> {best_crew} (degraded — {offline_list} offline)"
    else:
        resolver_context = ""
        resolver_summary = f"{inp.order_id} assigned to {best_crew}"

    resolver_body = ""
    if resolver_context:
        resolver_body += resolver_context + "\n"
    resolver_body += (
        f"ASSIGNMENT: {inp.order_id} -> {best_crew}\n"
        f"  {inp.hotel} {inp.event} ({inp.servings} servings)\n"
        f"  {best_crew} dispatching — ETA ~{best_eta}min, "
        f"deadline {inp.deadline_minutes}min."
    )

    await fleet.publish_agent_event(
        "resolver",
        "plan",
        resolver_body,
        summary=resolver_summary,
    )

    # Register assignment in fleet state (UI projection)
    await fleet.assign_order_to_crew(best_crew, inp.order_id)

    activity.logger.info(f"Assigned {inp.order_id} ({inp.hotel}) -> {best_crew}")
    return ReasonAboutAssignmentOutput(
        crew_id=best_crew,
        reasoning_summary=f"{best_crew} selected: closest with capacity, ~{best_eta}min ETA",
    )


@activity.defn(name="register_assignment")
async def register_assignment(crew_id: str, order_id: str) -> str:
    """Register an ADK-decided assignment in fleet state (replay-safe mutation)."""
    await fleet.assign_order_to_crew(crew_id, order_id)
    return f"Assigned {order_id} to {crew_id}"


@activity.defn(name="navigate_to")
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate AI-Crew navigation by interpolating position over N steps.

    Heartbeats on each step. Disconnect handling is two-layer:
    - inp.is_crew_disconnected: pre-flight check (set by workflow)
    - Cancellation scope: mid-flight disconnect delivers CancelledError
      on the next heartbeat() call (driven by workflow signal handler)
    """
    # Pre-flight disconnect check — workflow passes current state as input
    if inp.is_crew_disconnected:
        raise RuntimeError(
            f"AI-Crew {inp.crew_id} is disconnected — activity will retry on reconnect"
        )

    leg = inp.leg if isinstance(inp.leg, str) else str(inp.leg)
    status = (
        CrewStatus.EN_ROUTE_PICKUP if leg == LegType.PICKUP.value else CrewStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_crew_status(inp.crew_id, status)
    await fleet.update_order_status(
        inp.order_id,
        OrderStatus.IN_TRANSIT,
        f"AI-Crew {inp.crew_id} navigating to {leg} point",
    )

    # Start position from workflow state (not FleetState)
    start_lat = inp.start_lat if inp.start_lat is not None else inp.target_lat
    start_lng = inp.start_lng if inp.start_lng is not None else inp.target_lng

    # Build the path to interpolate along
    if inp.waypoints and len(inp.waypoints) >= 2:
        # Follow waypoint path from Google Maps polyline (or mock corridor)
        path = [(wp["lat"], wp["lng"]) for wp in inp.waypoints]
    else:
        # Straight line fallback
        path = [(start_lat, start_lng), (inp.target_lat, inp.target_lng)]

    # Calculate cumulative distances along the path for proportional interpolation
    segment_dists = []
    for i in range(1, len(path)):
        d = math.sqrt((path[i][0] - path[i - 1][0]) ** 2 + (path[i][1] - path[i - 1][1]) ** 2)
        segment_dists.append(d)
    total_dist = sum(segment_dists) or 1e-9

    for step in range(1, inp.steps + 1):
        # Heartbeat — if workflow cancelled this activity's scope (disconnect signal),
        # this call raises CancelledError, which propagates up to the workflow.
        activity.heartbeat(f"step {step}/{inp.steps}")

        # Find position along the polyline path at this fraction
        fraction = step / inp.steps
        target_dist = fraction * total_dist

        # Walk along segments to find the interpolation point
        accumulated = 0.0
        new_lat, new_lng = path[-1]  # default to end
        for i, seg_d in enumerate(segment_dists):
            if accumulated + seg_d >= target_dist:
                # Interpolate within this segment
                remaining = target_dist - accumulated
                seg_frac = remaining / seg_d if seg_d > 0 else 1.0
                new_lat = path[i][0] + (path[i + 1][0] - path[i][0]) * seg_frac
                new_lng = path[i][1] + (path[i + 1][1] - path[i][1]) * seg_frac
                break
            accumulated += seg_d

        # UI projection write — position update for frontend WebSocket
        await fleet.update_crew_position(inp.crew_id, new_lat, new_lng)

        # Simulate drive time per step
        await asyncio.sleep(0.4)

    activity.logger.info(
        f"{inp.crew_id} arrived at {leg} ({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        crew_id=inp.crew_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn(name="pickup_orders")
async def pickup_orders(inp: PickupInput) -> PickupOutput:
    """Simulate picking up ice cream orders at the kitchen."""
    if inp.is_crew_disconnected:
        raise RuntimeError(f"AI-Crew {inp.crew_id} is disconnected")
    await fleet.set_crew_status(inp.crew_id, CrewStatus.PICKING_UP)
    for oid in inp.order_ids:
        await fleet.update_order_status(oid, OrderStatus.PICKED_UP, "Ice cream loaded into cooler")

    await asyncio.sleep(1.5)

    activity.logger.info(f"{inp.crew_id} picked up orders {inp.order_ids}")
    return PickupOutput(crew_id=inp.crew_id, success=True)


@activity.defn(name="deliver_order")
async def deliver_order(inp: DeliverInput) -> DeliverOutput:
    """Simulate delivering an ice cream order at a hotel."""
    if inp.is_crew_disconnected:
        raise RuntimeError(f"AI-Crew {inp.crew_id} is disconnected")
    await fleet.set_crew_status(inp.crew_id, CrewStatus.DELIVERING)
    await fleet.update_order_status(inp.order_id, OrderStatus.IN_TRANSIT, "Delivering to hotel")

    await asyncio.sleep(1.5)

    # UI projection — mark order delivered and update crew status
    remaining_count = await fleet.complete_order_delivery(inp.crew_id, inp.order_id)
    if remaining_count == 0:
        await fleet.set_crew_status(inp.crew_id, CrewStatus.IDLE)

    activity.logger.info(f"{inp.crew_id} delivered {inp.order_id}")
    return DeliverOutput(crew_id=inp.crew_id, order_id=inp.order_id, success=True)


# --- Agent tool activities (called by ADK agents via activity_tool) ---


@activity.defn(name="get_fleet_status")
async def get_fleet_status(inp: GetFleetStatusInput) -> GetFleetStatusOutput:
    """Return fleet status summary for Fleet Agent consumption."""
    summary = await fleet.get_fleet_summary()
    return GetFleetStatusOutput(summary=summary)


@activity.defn(name="get_order_priorities")
async def get_order_priorities(
    inp: GetOrderPrioritiesInput,
) -> GetOrderPrioritiesOutput:
    """Return order priority details for Customer Agent consumption."""
    summary = await fleet.get_order_priorities_summary()
    return GetOrderPrioritiesOutput(summary=summary)


@activity.defn(name="publish_agent_event")
async def publish_agent_event(
    inp: PublishAgentEventInput,
) -> PublishAgentEventOutput:
    """Publish an agent reasoning event to the UI panel."""
    await fleet.publish_agent_event(
        inp.agent_name, inp.event_type, inp.content, summary=inp.summary
    )
    return PublishAgentEventOutput(success=True)


# --- Workflow-driven state sync activities ---


@activity.defn(name="sync_crew_disconnect")
async def sync_crew_disconnect(inp: SyncCrewDisconnectInput) -> None:
    """Sync crew disconnect/reconnect state to FleetState for the frontend.

    Called by the workflow after processing a disconnect/reconnect signal.
    Everything flows through Temporal — this is the only path to FleetState.
    """
    if inp.disconnected:
        await fleet.disconnect_crew(inp.crew_id)
    else:
        await fleet.reconnect_crew(inp.crew_id)


@activity.defn(name="sync_crew_recovery_complete")
async def sync_crew_recovery_complete(crew_id: str) -> None:
    """Clear the recovery visual indicator after replay completes."""
    await fleet.mark_crew_recovery_complete(crew_id)


# --- Customer change activities ---


@activity.defn(name="execute_customer_change")
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
        await fleet.update_order_delivery(inp.order_id, inp.new_lat, inp.new_lng)
        activity.logger.info(
            f"Order {inp.order_id} delivery updated to ({inp.new_lat:.4f}, {inp.new_lng:.4f})"
        )

    return ExecuteCustomerChangeOutput(success=True)
