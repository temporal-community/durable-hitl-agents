"""Data models for the Meltdown ice cream delivery fleet demo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

# --- Enums ---


class CrewStatus(str, enum.Enum):
    IDLE = "idle"
    EN_ROUTE_PICKUP = "en_route_pickup"
    PICKING_UP = "picking_up"
    EN_ROUTE_DELIVERY = "en_route_delivery"
    DELIVERING = "delivering"
    RETURNING = "returning"
    FAILED = "failed"
    RECOVERED = "recovered"
    DISCONNECTED = "disconnected"


class OrderPriority(str, enum.Enum):
    VIP = "vip"
    STANDARD = "standard"


class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    PICKED_UP = "picked_up"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    AT_RISK = "at_risk"
    REROUTED = "rerouted"
    CANCELLED = "cancelled"


class LegType(str, enum.Enum):
    PICKUP = "pickup"
    DELIVERY = "delivery"


# --- Core entities ---


@dataclass
class Coords:
    lat: float
    lng: float

    def to_dict(self) -> dict[str, float]:
        return {"lat": self.lat, "lng": self.lng}


@dataclass
class Crew:
    crew_id: str
    position: Coords
    battery_pct: float = 100.0
    status: CrewStatus = CrewStatus.IDLE
    capacity: int = 3
    current_orders: list[str] = field(default_factory=list)
    path_history: list[dict[str, float]] = field(default_factory=list)
    disconnected: bool = False
    recovering: bool = False
    status_before_disconnect: CrewStatus = CrewStatus.IDLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "crew_id": self.crew_id,
            "position": self.position.to_dict(),
            "battery_pct": self.battery_pct,
            "status": self.status.value,
            "capacity": self.capacity,
            "current_orders": self.current_orders,
            "path_history": self.path_history,
            "disconnected": self.disconnected,
            "recovering": self.recovering,
        }


@dataclass
class Order:
    order_id: str
    hotel: str
    label: str
    priority: OrderPriority
    servings: int
    delivery_coords: Coords
    assigned_crew_id: str | None = None
    status: OrderStatus = OrderStatus.PENDING
    deadline_minutes: int = 45
    status_log: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "hotel": self.hotel,
            "label": self.label,
            "priority": self.priority.value,
            "servings": self.servings,
            "delivery_coords": self.delivery_coords.to_dict(),
            "assigned_crew_id": self.assigned_crew_id,
            "status": self.status.value,
            "deadline_minutes": self.deadline_minutes,
            "status_log": self.status_log,
        }


# --- Temporal activity payloads ---


@dataclass
class GenerateOrderInput:
    order_number: int


@dataclass
class GenerateOrderOutput:
    order_id: str
    hotel: str
    label: str
    priority: str
    servings: int
    delivery_lat: float
    delivery_lng: float
    deadline_minutes: int
    event: str


@dataclass
class ReasonAboutAssignmentInput:
    order_id: str
    hotel: str
    delivery_lat: float
    delivery_lng: float
    priority: str
    servings: int
    deadline_minutes: int
    event: str


@dataclass
class ReasonAboutAssignmentOutput:
    crew_id: str
    reasoning_summary: str


@dataclass
class NavigateInput:
    crew_id: str
    order_id: str
    target_lat: float
    target_lng: float
    leg: str  # LegType value — "pickup" or "delivery"
    steps: int = 8
    waypoints: list[dict] | None = None  # [{"lat": float, "lng": float}, ...]


@dataclass
class NavigateOutput:
    crew_id: str
    arrived: bool
    final_lat: float
    final_lng: float


@dataclass
class PickupInput:
    crew_id: str
    order_ids: list[str]


@dataclass
class PickupOutput:
    crew_id: str
    success: bool


@dataclass
class DeliverInput:
    crew_id: str
    order_id: str


@dataclass
class DeliverOutput:
    crew_id: str
    order_id: str
    success: bool


# --- Agent tool payloads ---


@dataclass
class GetFleetStatusInput:
    pass


@dataclass
class GetFleetStatusOutput:
    summary: str


@dataclass
class GetOrderPrioritiesInput:
    pass


@dataclass
class GetOrderPrioritiesOutput:
    summary: str


@dataclass
class PublishAgentEventInput:
    agent_name: str
    event_type: str
    content: str
    summary: str = ""


@dataclass
class PublishAgentEventOutput:
    success: bool


# --- Customer change payloads ---


@dataclass
class CustomerChangeInput:
    order_id: str
    change_type: str  # "address_change" or "cancel"
    new_details: str
    new_lat: float | None = None
    new_lng: float | None = None


@dataclass
class ExecuteCustomerChangeInput:
    order_id: str
    change_type: str
    new_lat: float | None = None
    new_lng: float | None = None


@dataclass
class ExecuteCustomerChangeOutput:
    success: bool


# --- Crew route workflow payloads ---


@dataclass
class CrewRouteOrder:
    order_id: str
    hotel: str
    delivery_lat: float
    delivery_lng: float


@dataclass
class CrewRouteInput:
    crew_id: str


# --- Agent events (for UI panel) ---


@dataclass
class AgentEvent:
    agent_name: str
    event_type: str
    content: str
    timestamp: float
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "event_type": self.event_type,
            "content": self.content,
            "timestamp": self.timestamp,
            "summary": self.summary,
        }


# --- Workflow inputs ---


@dataclass
class CrewDisconnectInput:
    crew_id: str


@dataclass
class AgentDisconnectInput:
    agent_name: str  # "fleet_agent", "customer_agent", or "resolver"


@dataclass
class MeltdownDemoInput:
    escalation_enabled: bool = False
    max_orders: int = 20
