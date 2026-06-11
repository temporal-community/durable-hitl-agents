"""
Single source of truth for all map locations — downtown San Francisco.

Ice cream shop (Ferry Building) + prominent downtown delivery destinations.
The "hotel" key is a historical field name; values are SF venue names.
"""

from __future__ import annotations

import random

from agent_fleet.models import Coords

# Ziggy's Ice Cream — the Ferry Building, an iconic SF food hall on the
# Embarcadero, central to the downtown delivery spread.
WAREHOUSE = Coords(lat=37.7956, lng=-122.3934)
WAREHOUSE_LABEL = "Ziggy's Ice Cream"

# Delivery destinations — prominent downtown San Francisco venues.
# Moscone Center is the platinum tier: premium conference-catering orders that
# trip the agent-initiated approval gate.
VENUES: list[dict] = [
    {
        "hotel": "Moscone Center",
        "coords": Coords(lat=37.7841, lng=-122.4017),
        "map_label": "Moscone Center",
        "events": ["keynote reception", "expo hall social", "conference catering"],
        "vip_tier": "platinum",
    },
    {
        "hotel": "Fisherman's Wharf",
        "coords": Coords(lat=37.8080, lng=-122.4170),
        "map_label": "Fisherman's Wharf",
        "events": ["pier festival", "seafood fair afterparty", "waterfront gathering"],
        "vip_tier": "silver",
    },
    {
        "hotel": "Chinatown",
        "coords": Coords(lat=37.7946, lng=-122.4059),
        "map_label": "Chinatown",
        "events": ["lantern festival", "night market", "banquet hall event"],
        "vip_tier": "gold",
    },
]

# Venues indexed by name for quick lookup
VENUES_BY_HOTEL: dict[str, dict] = {v["hotel"]: v for v in VENUES}

# Reroute-only destination — only appears on the map during customer change demos.
# Oracle Park, on the waterfront south of downtown (a visible reroute distance).
COSMOPOLITAN = {
    "hotel": "Oracle Park",
    "coords": Coords(lat=37.7786, lng=-122.3893),
    "map_label": "Oracle Park",
    "vip_tier": "standard",
}


def generate_random_order(order_number: int) -> dict:
    """Generate a random order from the venue pool.

    Returns a dict with all fields needed to create an Order in simulation state.
    """
    venue = random.choice(VENUES)
    event = random.choice(venue["events"])
    priority = "vip" if venue["vip_tier"] in ("platinum", "gold") else "standard"
    servings = random.choice([40, 60, 80, 100, 120, 150])
    if priority == "vip":
        deadline = random.choice([20, 25, 30, 35, 40])
    else:
        deadline = random.choice([30, 40, 50])

    # Routine orders stay below the gate's review threshold ($2,000). Only the
    # deliberately injected premium order (/api/inject-order) trips the agent gate,
    # so the agent-in-the-loop demo fires when you choose — not at random.
    order_value = servings * random.choice([9, 11, 13])

    return {
        "order_id": f"order-{order_number}",
        "hotel": venue["hotel"],
        "label": f"{venue['hotel']} {event} — {servings} servings",
        "coords": venue["coords"],
        "map_label": venue["map_label"],
        "priority": priority,
        "servings": servings,
        "deadline_minutes": deadline,
        "event": event,
        "order_value": order_value,
    }
