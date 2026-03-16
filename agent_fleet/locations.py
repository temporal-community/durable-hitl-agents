"""
Single source of truth for all map locations.

To add a new delivery destination, add an entry to DELIVERY_DESTINATIONS.
The server, worker, and frontend all derive from this file.
"""

from agent_fleet.models import Coords

WAREHOUSE = Coords(lat=37.7544, lng=-122.4477)
WAREHOUSE_LABEL = "Twin Peaks"

DELIVERY_DESTINATIONS = {
    "mission-1": {
        "label": "Package A -> Presidio",
        "coords": Coords(lat=37.7989, lng=-122.4662),
        "map_label": "Presidio",
    },
    "mission-2": {
        "label": "Package B -> Presidio",
        "coords": Coords(lat=37.7989, lng=-122.4662),
        "map_label": "Presidio",
    },
}
