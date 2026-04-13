"""
Mock activity implementations for the Meltdown demo.

These are registered by the mock worker when API keys are not set.
They use the same activity names as the real implementations, so workflows
and ADK agents don't know or care which version is running.

Temporal selects real vs mock at worker startup — not at runtime with
try/except. This keeps the real activities clean (failures propagate to
Temporal's retry mechanism) and makes mock mode an explicit configuration
choice visible in the worker setup.
"""

from __future__ import annotations

import asyncio
import math
import time

from temporalio import activity

from agent_fleet.locations import VENUES_BY_HOTEL
from agent_fleet.models import (
    ReasonAboutAssignmentInput,
    ReasonAboutAssignmentOutput,
)
from agent_fleet.simulation import fleet

# --- Strip corridor for mock navigation waypoints ---

# Las Vegas Blvd S — anchor points on the road centerline
_STRIP_CORRIDOR = [
    # Paradise Rd to the Strip (shop location east of the Strip)
    (36.12800, -115.15300),
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
    # Venetian / Palazzo
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
    # Flamingo intersection
    (36.11750, -115.17240),
    (36.11700, -115.17260),
    # Caesars Palace
    (36.11670, -115.17300),
    (36.11650, -115.17350),
    (36.11620, -115.17450),
    (36.11580, -115.17460),
    (36.11540, -115.17470),
    # Bellagio
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
    # CityCenter / Aria
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
    # MGM Grand
    (36.10380, -115.17250),
    (36.10330, -115.17250),
    (36.10280, -115.17250),
    (36.10240, -115.17250),
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
    # Mandalay Bay
    (36.09450, -115.17570),
    (36.09400, -115.17580),
    (36.09350, -115.17590),
    (36.09300, -115.17600),
    (36.09250, -115.17610),
    (36.09190, -115.17610),
]


# --- Helpers ---


def _closest_corridor_idx(lat: float, lng: float) -> int:
    best_idx = 0
    best_dist = float("inf")
    for i, (clat, clng) in enumerate(_STRIP_CORRIDOR):
        d = math.sqrt((lat - clat) ** 2 + (lng - clng) ** 2)
        if d < best_dist:
            best_dist = d
            best_idx = i
    return best_idx


# --- Mock activities (same names as real ones) ---


@activity.defn(name="get_route_polyline")
async def mock_get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Generate mock waypoints that follow the Las Vegas Strip corridor."""
    start_idx = _closest_corridor_idx(origin_lat, origin_lng)
    end_idx = _closest_corridor_idx(dest_lat, dest_lng)

    waypoints = [{"lat": origin_lat, "lng": origin_lng}]

    if start_idx <= end_idx:
        corridor_slice = _STRIP_CORRIDOR[start_idx : end_idx + 1]
    else:
        corridor_slice = list(reversed(_STRIP_CORRIDOR[end_idx : start_idx + 1]))

    for clat, clng in corridor_slice:
        if len(waypoints) == 1:
            d = math.sqrt((clat - origin_lat) ** 2 + (clng - origin_lng) ** 2)
            if d < 0.0005:
                continue
        waypoints.append({"lat": clat, "lng": clng})

    last = waypoints[-1]
    d = math.sqrt((dest_lat - last["lat"]) ** 2 + (dest_lng - last["lng"]) ** 2)
    if d > 0.0005:
        waypoints.append({"lat": dest_lat, "lng": dest_lng})

    activity.logger.info(f"[MOCK NAV] Corridor waypoints: {len(waypoints)} points")
    return waypoints


@activity.defn(name="tool_get_route_info")
async def mock_tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
    origin_name: str = "",
) -> str:
    """Deterministic mock route info using distance calculation."""
    dlat = destination_lat - origin_lat
    dlng = destination_lng - origin_lng
    dist_miles = math.sqrt(dlat**2 + dlng**2) * 69.0
    eta_minutes = max(3, int(dist_miles * 3.5))

    dest_label = destination_name or f"({destination_lat:.4f}, {destination_lng:.4f})"
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
async def mock_tool_search_hotel_context(hotel_name: str) -> str:
    """Curated hotel context for demo — no external API call."""
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
    for key, context in contexts.items():
        if key.lower() in hotel_name.lower() or hotel_name.lower() in key.lower():
            return f"Hotel intelligence for {hotel_name}:\n{context}"
    return f"No specific intelligence available for {hotel_name}."


@activity.defn(name="reason_about_assignment")
async def mock_reason_about_assignment(
    inp: ReasonAboutAssignmentInput,
) -> ReasonAboutAssignmentOutput:
    """
    Mock multi-agent reasoning to decide which driver should handle a new order.

    Uses deterministic distance math instead of LLM calls.
    Fleet Agent assesses driver positions and capacity.
    Customer Agent evaluates order priority and urgency.
    Dispatch Agent synthesizes and picks the best driver.

    All decision inputs come from inp (workflow state) — not from FleetState.
    FleetState writes are UI projection only.
    Agent events are collected and returned to the workflow via the output.
    """
    collected_events: list[dict] = []

    def _collect_event(agent_name: str, event_type: str, content: str, summary: str = "") -> None:
        collected_events.append(
            {
                "agent_name": agent_name,
                "event_type": event_type,
                "content": content,
                "summary": summary,
                "timestamp": time.time(),
            }
        )

    # --- Fleet Agent: find best driver from workflow-provided snapshots ---
    fleet_agent_offline = "fleet_agent" in inp.disconnected_agents

    best_driver = None
    best_dist = float("inf")
    for driver in inp.driver_snapshots:
        available = driver.capacity - driver.current_order_count
        dist = math.sqrt(
            (driver.lat - inp.delivery_lat) ** 2 + (driver.lng - inp.delivery_lng) ** 2
        )

        # Skip drivers that can't take orders
        if driver.is_disconnected or available <= 0:
            continue
        if dist < best_dist:
            best_dist = dist
            best_driver = driver.driver_id
    if best_driver is None:
        # Fallback: pick any driver with capacity even if busy
        best_driver = "driver-1"

    best_eta = max(2, int(best_dist * 69.0 * 3.5))

    # Map driver IDs to driver labels for display
    def _driver_label(cid: str) -> str:
        if cid.startswith("driver-"):
            return f"Driver {cid.split('-')[-1]}"
        return cid

    if fleet_agent_offline:
        # Fleet Agent is offline — publish offline notice and skip its assessment
        _collect_event(
            "fleet_agent",
            "offline",
            "Fleet Agent offline — Dispatch Agent using last-known data.",
            summary="Fleet Agent offline",
        )
        await fleet.publish_agent_event(
            "fleet_agent",
            "offline",
            "Fleet Agent offline — Dispatch Agent using last-known data.",
            summary="Fleet Agent offline",
        )
        await asyncio.sleep(0.2)
    else:
        _collect_event(
            "fleet_agent",
            "tool_call",
            f"New order — {inp.hotel}. Scanning fleet.",
            summary=f"New order — {inp.hotel}",
        )
        await fleet.publish_agent_event(
            "fleet_agent",
            "tool_call",
            f"New order — {inp.hotel}. Scanning fleet.",
            summary=f"New order — {inp.hotel}",
        )
        await asyncio.sleep(0.4)

        _collect_event(
            "fleet_agent",
            "assessment",
            f"{_driver_label(best_driver)} — closest, ~{best_eta}min ETA.",
            summary=f"{_driver_label(best_driver)} — ETA {best_eta}min",
        )
        await fleet.publish_agent_event(
            "fleet_agent",
            "assessment",
            f"{_driver_label(best_driver)} — closest, ~{best_eta}min ETA.",
            summary=f"{_driver_label(best_driver)} — ETA {best_eta}min",
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
        _collect_event(
            "customer_agent",
            "offline",
            "Customer Agent offline — using order metadata.",
            summary="Customer Agent offline",
        )
        await fleet.publish_agent_event(
            "customer_agent",
            "offline",
            "Customer Agent offline — using order metadata.",
            summary="Customer Agent offline",
        )
        await asyncio.sleep(0.2)
    else:
        urgency_note = "Time-critical" if urgency != "comfortable" else "Standard"
        customer_content = (
            f"{inp.priority.upper()} / {vip_tier} — {inp.servings} servings, "
            f"{inp.deadline_minutes}min deadline. {urgency_note}."
        )
        customer_summary = f"{inp.priority.upper()} — {urgency} deadline"
        _collect_event(
            "customer_agent",
            "assessment",
            customer_content,
            summary=customer_summary,
        )
        await fleet.publish_agent_event(
            "customer_agent",
            "assessment",
            customer_content,
            summary=customer_summary,
        )
        await asyncio.sleep(0.3)

    # --- Dispatch Agent: synthesize and assign ---
    offline_agents = []
    if fleet_agent_offline:
        offline_agents.append("Fleet Agent")
    if customer_agent_offline:
        offline_agents.append("Customer Agent")

    best_label = _driver_label(best_driver)
    order_number = inp.order_id.split("-", 1)[-1] if "-" in inp.order_id else inp.order_id
    reasoning = f"ETA ~{best_eta}min"
    if offline_agents:
        offline_list = " and ".join(offline_agents)
        if fleet_agent_offline and customer_agent_offline:
            reasoning = f"Degraded — {offline_list} offline. Best-effort, ETA ~{best_eta}min"
        elif fleet_agent_offline:
            reasoning = f"Fleet offline, last-known positions, ETA ~{best_eta}min"
        else:
            reasoning = f"Customer offline, using order metadata, ETA ~{best_eta}min"

    resolver_body = f"Order #{order_number} → {best_label} — {inp.hotel} — {reasoning}"
    resolver_summary = f"Order #{order_number} → {best_label} — {inp.hotel}"

    _collect_event(
        "resolver",
        "plan",
        resolver_body,
        summary=resolver_summary,
    )
    await fleet.publish_agent_event(
        "resolver",
        "plan",
        resolver_body,
        summary=resolver_summary,
    )

    # Acknowledge Fleet Agent gap if disconnected
    if fleet_agent_offline:
        _collect_event(
            "resolver",
            "assessment",
            "Fleet Agent offline — assigned with customer data only",
            summary="Fleet Agent offline — customer data only",
        )
        await fleet.publish_agent_event(
            "resolver",
            "assessment",
            "Fleet Agent offline — assigned with customer data only",
            summary="Fleet Agent offline — customer data only",
        )

    # Register assignment in fleet state (UI projection)
    await fleet.assign_order_to_driver(best_driver, inp.order_id)

    activity.logger.info(f"Assigned {inp.order_id} ({inp.hotel}) -> {best_driver}")
    return ReasonAboutAssignmentOutput(
        driver_id=best_driver,
        reasoning_summary=f"{_driver_label(best_driver)} — closest, ~{best_eta}min ETA",
        agent_events=collected_events,
    )
