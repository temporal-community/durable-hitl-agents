"""
Shared simulation state for the courier fleet demo.

This module manages courier positions, mission status, and provides the
"physical world" that activities read/write. It's backed by an in-memory
dict so both the Temporal worker and the FastAPI server can access it
(they run in the same process).
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from agent_fleet.models import (
    Coords, Courier, CourierStatus, Mission, MissionStatus, DemoEventConfig,
)
from agent_fleet.locations import WAREHOUSE, DELIVERY_DESTINATIONS

_EVENT_LOG_MAX = 500


class FleetState:
    """Global mutable state for the courier simulation."""

    def __init__(self) -> None:
        self.couriers: dict[str, Courier] = {}
        self.missions: dict[str, Mission] = {}
        self.event_log: deque[dict[str, Any]] = deque(maxlen=_EVENT_LOG_MAX)
        self._lock = asyncio.Lock()
        self.demo_events = DemoEventConfig()
        self._weather_conditions: dict[str, str] = {}
        self._nav_step_counters: dict[str, int] = {}
        self._init_state()

    def _init_state(self) -> None:
        for i in range(1, 3):
            cid = f"courier-{i}"
            self.couriers[cid] = Courier(
                courier_id=cid,
                position=Coords(lat=WAREHOUSE.lat, lng=WAREHOUSE.lng),
            )
            self._weather_conditions[cid] = "clear"
            self._nav_step_counters[cid] = 0
        for mid, info in DELIVERY_DESTINATIONS.items():
            self.missions[mid] = Mission(
                mission_id=mid,
                order_label=info["label"],
                pickup_coords=Coords(lat=WAREHOUSE.lat, lng=WAREHOUSE.lng),
                delivery_coords=info["coords"],
            )

    def reset(self) -> None:
        """Reset simulation to initial state for a fresh demo run."""
        self.couriers.clear()
        self.missions.clear()
        self.event_log.clear()
        self.demo_events = DemoEventConfig()
        self._weather_conditions.clear()
        self._nav_step_counters.clear()
        self._init_state()

    # --- Courier operations ---

    async def update_courier_position(
        self, courier_id: str, lat: float, lng: float
    ) -> None:
        async with self._lock:
            c = self.couriers[courier_id]
            c.position = Coords(lat=lat, lng=lng)
            c.path_history.append({"lat": lat, "lng": lng, "t": time.time()})

    async def set_courier_status(
        self, courier_id: str, status: CourierStatus, mission_id: str | None = None
    ) -> None:
        async with self._lock:
            c = self.couriers[courier_id]
            c.status = status
            if mission_id is not None:
                c.current_mission_id = mission_id
            self._log(f"Courier {courier_id} -> {status.value}")

    # --- Battery operations ---

    async def drain_battery(self, courier_id: str, amount: float) -> float:
        """Decrement battery, return new value."""
        async with self._lock:
            c = self.couriers[courier_id]
            c.battery_pct = max(0.0, c.battery_pct - amount)
            return c.battery_pct

    async def get_battery(self, courier_id: str) -> float:
        async with self._lock:
            return self.couriers[courier_id].battery_pct

    # --- Weather operations ---

    async def get_weather(self, courier_id: str) -> str:
        """Return weather condition, checking demo events for storm injection."""
        async with self._lock:
            return self._weather_conditions.get(courier_id, "clear")

    # --- Nav step tracking & demo event triggers ---

    async def increment_nav_step(self, courier_id: str) -> None:
        """Increment nav step counter and apply demo event triggers."""
        async with self._lock:
            self._nav_step_counters[courier_id] = (
                self._nav_step_counters.get(courier_id, 0) + 1
            )
            step = self._nav_step_counters[courier_id]

            if not self.demo_events.enabled:
                return

            # Battery drop trigger
            if (
                self.demo_events.battery_drop_at_nav_step is not None
                and step == self.demo_events.battery_drop_at_nav_step
            ):
                c = self.couriers[courier_id]
                c.battery_pct = self.demo_events.battery_drop_to_pct
                self._log(
                    f"[DEMO EVENT] Courier {courier_id} battery dropped to "
                    f"{self.demo_events.battery_drop_to_pct}%"
                )

            # Weather storm trigger
            if (
                self.demo_events.weather_storm_at_nav_step is not None
                and step == self.demo_events.weather_storm_at_nav_step
            ):
                self._weather_conditions[courier_id] = "storm"
                self._log(
                    f"[DEMO EVENT] Storm conditions for courier {courier_id}"
                )

    async def set_demo_events(self, config: DemoEventConfig) -> None:
        """Configure demo events (called from server endpoint)."""
        async with self._lock:
            self.demo_events = config

    # --- Mission operations ---

    async def assign_mission(self, mission_id: str, courier_id: str) -> None:
        async with self._lock:
            m = self.missions[mission_id]
            m.assigned_courier_id = courier_id
            m.status = MissionStatus.ASSIGNED
            m.status_log.append(f"Assigned to {courier_id}")
            self.couriers[courier_id].current_mission_id = mission_id
            self._log(f"Mission {mission_id} assigned to {courier_id}")

    async def update_mission_status(
        self, mission_id: str, status: MissionStatus, note: str = ""
    ) -> None:
        async with self._lock:
            m = self.missions[mission_id]
            m.status = status
            if note:
                m.status_log.append(note)
            self._log(f"Mission {mission_id} -> {status.value}: {note}")

    # --- Query ---

    async def get_idle_courier(self) -> str | None:
        async with self._lock:
            for c in self.couriers.values():
                if c.status == CourierStatus.IDLE:
                    return c.courier_id
        return None

    async def get_courier_position(self, courier_id: str) -> tuple[float, float]:
        """Return (lat, lng) under the lock."""
        async with self._lock:
            c = self.couriers[courier_id]
            return c.position.lat, c.position.lng

    async def get_mission_courier(self, mission_id: str) -> str | None:
        """Return the courier_id assigned to a mission, or None."""
        async with self._lock:
            m = self.missions.get(mission_id)
            if m is None:
                return None
            return m.assigned_courier_id

    async def courier_exists(self, courier_id: str) -> bool:
        """Check if a courier exists, under the lock."""
        async with self._lock:
            return courier_id in self.couriers

    async def snapshot(self) -> dict[str, Any]:
        """Return full state as JSON-serializable dict (for frontend)."""
        async with self._lock:
            return {
                "couriers": {
                    cid: c.to_dict() for cid, c in self.couriers.items()
                },
                "missions": {
                    mid: m.to_dict() for mid, m in self.missions.items()
                },
                "event_log": list(self.event_log),
            }

    # --- Internals ---

    def _log(self, msg: str) -> None:
        self.event_log.append({"t": time.time(), "msg": msg})


# Singleton — shared across worker and server
fleet = FleetState()
