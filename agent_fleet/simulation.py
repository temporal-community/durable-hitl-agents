"""
Shared simulation state for the Meltdown ice cream delivery demo.

Manages crew positions, order status, and agent events.
Backed by in-memory state shared between the Temporal worker and FastAPI server
(they run in the same process).
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from agent_fleet.locations import WAREHOUSE
from agent_fleet.models import (
    AgentEvent,
    Coords,
    Crew,
    CrewStatus,
    Order,
    OrderPriority,
    OrderStatus,
)

_EVENT_LOG_MAX = 500


class FleetState:
    """Global mutable state for the ice cream fleet simulation."""

    def __init__(self) -> None:
        self.crews: dict[str, Crew] = {}
        self.orders: dict[str, Order] = {}
        self.event_log: deque[dict[str, Any]] = deque(maxlen=_EVENT_LOG_MAX)
        self.agent_events: list[AgentEvent] = []
        self._lock = asyncio.Lock()
        # Per-agent health tracking
        self.agent_health: dict[str, bool] = {
            "fleet_agent": True,
            "customer_agent": True,
            "resolver": True,
        }
        self._init_state()

    def _init_state(self) -> None:
        # 3 AI-Crews starting at the ice cream shop
        for i in range(1, 4):
            cid = f"ai-crew-{i}"
            self.crews[cid] = Crew(
                crew_id=cid,
                position=Coords(lat=WAREHOUSE.lat, lng=WAREHOUSE.lng),
            )
        # Orders are registered dynamically as they are generated

    def reset(self) -> None:
        """Reset simulation to initial state for a fresh demo run."""
        self.crews.clear()
        self.orders.clear()
        self.event_log.clear()
        self.agent_events.clear()
        self.agent_health = {
            "fleet_agent": True,
            "customer_agent": True,
            "resolver": True,
        }
        self._init_state()

    # --- Per-crew disconnect / reconnect ---

    async def disconnect_crew(self, crew_id: str) -> None:
        """Mark a single crew as disconnected (UI projection only)."""
        async with self._lock:
            c = self.crews[crew_id]
            c.status_before_disconnect = c.status
            c.disconnected = True
            c.status = CrewStatus.DISCONNECTED
            self._log(f"[DISCONNECT] AI-Crew {crew_id} lost connection")

    async def reconnect_crew(self, crew_id: str) -> None:
        """Clear disconnect flag and enter per-crew recovery phase (UI projection only)."""
        async with self._lock:
            c = self.crews[crew_id]
            c.disconnected = False
            c.recovering = True
            c.status = c.status_before_disconnect
            self._log(f"[RECONNECT] AI-Crew {crew_id} reconnecting — replaying...")

    async def mark_crew_recovery_complete(self, crew_id: str) -> None:
        """Clear the per-crew recovery flag after replay completes."""
        async with self._lock:
            c = self.crews[crew_id]
            c.recovering = False
            self._log(f"[RECONNECT] AI-Crew {crew_id} replay complete — resumed")

    async def is_crew_disconnected(self, crew_id: str) -> bool:
        async with self._lock:
            return self.crews[crew_id].disconnected

    # --- Per-agent health ---

    async def disconnect_agent(self, agent_name: str) -> None:
        """Mark a specific agent as offline."""
        async with self._lock:
            self.agent_health[agent_name] = False
            self._log(f"[AGENT OFFLINE] {agent_name} disconnected")

    async def reconnect_agent(self, agent_name: str) -> None:
        """Bring a specific agent back online (UI projection only)."""
        async with self._lock:
            self.agent_health[agent_name] = True
            self._log(f"[AGENT ONLINE] {agent_name} reconnected")

    async def is_agent_online(self, agent_name: str) -> bool:
        async with self._lock:
            return self.agent_health.get(agent_name, True)

    async def is_agent_disconnected(self, agent_name: str) -> bool:
        async with self._lock:
            return not self.agent_health.get(agent_name, True)

    async def get_agent_health(self) -> dict[str, bool]:
        async with self._lock:
            return dict(self.agent_health)

    # --- Crew operations ---

    async def update_crew_position(self, crew_id: str, lat: float, lng: float) -> None:
        async with self._lock:
            c = self.crews[crew_id]
            c.position = Coords(lat=lat, lng=lng)
            c.path_history.append({"lat": lat, "lng": lng, "t": time.time()})

    async def set_crew_status(self, crew_id: str, status: CrewStatus) -> None:
        async with self._lock:
            self.crews[crew_id].status = status
            self._log(f"AI-Crew {crew_id} -> {status.value}")

    async def get_crew_position(self, crew_id: str) -> tuple[float, float]:
        async with self._lock:
            c = self.crews[crew_id]
            return c.position.lat, c.position.lng

    async def crew_exists(self, crew_id: str) -> bool:
        async with self._lock:
            return crew_id in self.crews

    async def get_crew(self, crew_id: str) -> Crew | None:
        async with self._lock:
            return self.crews.get(crew_id)

    async def get_order(self, order_id: str) -> Order | None:
        async with self._lock:
            return self.orders.get(order_id)

    # --- Order operations ---

    async def register_order(
        self,
        order_id: str,
        hotel: str,
        label: str,
        priority: str,
        servings: int,
        delivery_coords: Coords,
        deadline_minutes: int,
    ) -> None:
        """Register a new dynamically-generated order."""
        async with self._lock:
            self.orders[order_id] = Order(
                order_id=order_id,
                hotel=hotel,
                label=label,
                priority=OrderPriority(priority),
                servings=servings,
                delivery_coords=delivery_coords,
                deadline_minutes=deadline_minutes,
            )
            self._log(f"New order {order_id}: {label}")

    async def assign_order_to_crew(self, crew_id: str, order_id: str) -> None:
        """Assign a single order to a crew."""
        async with self._lock:
            c = self.crews[crew_id]
            o = self.orders[order_id]
            o.assigned_crew_id = crew_id
            o.status = OrderStatus.ASSIGNED
            o.status_log.append(f"Assigned to {crew_id}")
            c.current_orders.append(order_id)
            self._log(f"Order {order_id} assigned to {crew_id}")

    async def update_order_status(self, order_id: str, status: OrderStatus, note: str = "") -> None:
        async with self._lock:
            o = self.orders[order_id]
            o.status = status
            if note:
                o.status_log.append(note)
            self._log(f"Order {order_id} -> {status.value}: {note}")

    async def complete_order_delivery(self, crew_id: str, order_id: str) -> int:
        """Mark an order delivered and remove it from the crew's active queue."""
        async with self._lock:
            o = self.orders[order_id]
            o.status = OrderStatus.DELIVERED
            o.status_log.append("Delivered successfully!")

            crew = self.crews[crew_id]
            if order_id in crew.current_orders:
                crew.current_orders.remove(order_id)

            self._log(f"Order {order_id} -> {OrderStatus.DELIVERED.value}: Delivered successfully!")
            return len(crew.current_orders)

    async def get_order_crew(self, order_id: str) -> str | None:
        async with self._lock:
            o = self.orders.get(order_id)
            if o is None:
                return None
            return o.assigned_crew_id

    async def get_crew_orders(self, crew_id: str) -> list[str]:
        async with self._lock:
            return list(self.crews[crew_id].current_orders)

    async def update_order_delivery(self, order_id: str, new_lat: float, new_lng: float) -> None:
        """Update delivery coordinates for an order (customer change)."""
        async with self._lock:
            o = self.orders[order_id]
            o.delivery_coords = Coords(lat=new_lat, lng=new_lng)
            o.status_log.append(f"Delivery address updated to ({new_lat:.4f}, {new_lng:.4f})")

    async def cancel_order(self, order_id: str) -> None:
        async with self._lock:
            o = self.orders[order_id]
            o.status = OrderStatus.CANCELLED
            o.status_log.append("Cancelled by customer")
            # Remove from crew's list
            if o.assigned_crew_id:
                c = self.crews[o.assigned_crew_id]
                if order_id in c.current_orders:
                    c.current_orders.remove(order_id)
            self._log(f"Order {order_id} cancelled")

    # --- Agent events (for UI panel) ---

    async def publish_agent_event(
        self, agent_name: str, event_type: str, content: str, summary: str = ""
    ) -> None:
        async with self._lock:
            event = AgentEvent(
                agent_name=agent_name,
                event_type=event_type,
                content=content,
                timestamp=time.time(),
                summary=summary,
            )
            self.agent_events.append(event)
            self._log(f"[{agent_name}] {event_type}: {content[:80]}")

    # --- Query ---

    async def snapshot(self) -> dict[str, Any]:
        """Return full state as JSON-serializable dict (for frontend)."""
        async with self._lock:
            return {
                "crews": {cid: c.to_dict() for cid, c in self.crews.items()},
                "orders": {oid: o.to_dict() for oid, o in self.orders.items()},
                "agent_events": [e.to_dict() for e in self.agent_events],
                "event_log": list(self.event_log),
                "agent_health": dict(self.agent_health),
            }

    async def get_fleet_summary(self) -> str:
        """Return a text summary of fleet state for LLM consumption."""
        async with self._lock:
            lines = ["=== Fleet Status ==="]
            for cid, c in self.crews.items():
                orders_str = ", ".join(c.current_orders) if c.current_orders else "none"
                disconnect_tag = " **DISCONNECTED**" if c.disconnected else ""
                recovering_tag = " [recovering]" if c.recovering else ""
                lines.append(
                    f"  {cid}: status={c.status.value}, "
                    f"orders=[{orders_str}]"
                    f"{disconnect_tag}{recovering_tag}"
                )
            lines.append("=== Agent Health ===")
            for agent_name, online in self.agent_health.items():
                status = "ONLINE" if online else "OFFLINE"
                lines.append(f"  {agent_name}: {status}")
            lines.append("=== Orders ===")
            for oid, o in self.orders.items():
                lines.append(
                    f"  {oid}: {o.hotel} ({o.label}), "
                    f"priority={o.priority.value}, status={o.status.value}, "
                    f"AI-Crew={o.assigned_crew_id or 'unassigned'}, "
                    f"deadline={o.deadline_minutes}min"
                )
            return "\n".join(lines)

    async def get_order_priorities_summary(self) -> str:
        """Return order priority details for Customer Agent consumption."""
        async with self._lock:
            lines = ["=== Order Priorities ==="]
            for oid, o in self.orders.items():
                crew_status = ""
                if o.assigned_crew_id:
                    crew = self.crews.get(o.assigned_crew_id)
                    if crew and crew.disconnected:
                        crew_status = f" **CREW {o.assigned_crew_id} DISCONNECTED**"
                lines.append(
                    f"  {oid}: {o.hotel} — {o.priority.value.upper()}, "
                    f"{o.servings} servings, deadline={o.deadline_minutes}min, "
                    f"status={o.status.value}{crew_status}"
                )
            return "\n".join(lines)

    # --- Internals ---

    def _log(self, msg: str) -> None:
        self.event_log.append({"t": time.time(), "msg": msg})


# Singleton — shared across worker and server
fleet = FleetState()
