"""
Single source of truth for all map locations — Las Vegas Strip.

Ice cream shop (off-Strip) + hotel/venue delivery destinations.
"""

from __future__ import annotations

import random

from agent_fleet.models import Coords

# Frosty's Ice Cream — midway between Caesars (36.1162) and Mandalay (36.0919),
# east of the Strip on Paradise Rd
WAREHOUSE = Coords(lat=36.1040, lng=-115.1530)
WAREHOUSE_LABEL = "Frosty's Ice Cream"

# Delivery destinations — 3 hotels on the Strip
VENUES: list[dict] = [
    {
        "hotel": "MGM Grand",
        "coords": Coords(lat=36.1024, lng=-115.1725),
        "map_label": "MGM Grand",
        "events": ["pool party", "Grand Garden Arena concert", "celebrity chef dinner"],
        "vip_tier": "platinum",
    },
    {
        "hotel": "Caesars Palace",
        "coords": Coords(lat=36.1162, lng=-115.1745),
        "map_label": "Caesars",
        "events": ["corporate gala", "Colosseum show afterparty", "Forum Shops VIP event"],
        "vip_tier": "platinum",
    },
    {
        "hotel": "Mandalay Bay",
        "coords": Coords(lat=36.0919, lng=-115.1761),
        "map_label": "Mandalay Bay",
        "events": ["tech conference", "Shark Reef fundraiser", "convention center lunch"],
        "vip_tier": "platinum",
    },
]

# Venues indexed by hotel name for quick lookup
VENUES_BY_HOTEL: dict[str, dict] = {v["hotel"]: v for v in VENUES}


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
    }
