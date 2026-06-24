"""Data models for the Meltdown ice cream delivery fleet demo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

# --- Enums ---


class DriverStatus(str, enum.Enum):
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
class Driver:
    driver_id: str
    position: Coords
    battery_pct: float = 100.0
    status: DriverStatus = DriverStatus.IDLE
    capacity: int = 3
    current_orders: list[str] = field(default_factory=list)
    path_history: list[dict[str, float]] = field(default_factory=list)
    disconnected: bool = False
    recovering: bool = False
    status_before_disconnect: DriverStatus = DriverStatus.IDLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "driver_id": self.driver_id,
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
    assigned_driver_id: str | None = None
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
            "assigned_driver_id": self.assigned_driver_id,
            "status": self.status.value,
            "deadline_minutes": self.deadline_minutes,
            "status_log": self.status_log,
        }


# --- Temporal activity payloads ---


@dataclass
class DriverSnapshot:
    """Workflow-owned snapshot of a driver's state, passed to activities as input."""

    driver_id: str
    lat: float
    lng: float
    status: str
    capacity: int
    current_order_count: int
    is_disconnected: bool


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
    order_value: int = 0


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
    driver_snapshots: list[DriverSnapshot] = field(default_factory=list)
    disconnected_agents: list[str] = field(default_factory=list)


@dataclass
class ReasonAboutAssignmentOutput:
    driver_id: str
    reasoning_summary: str
    agent_events: list[dict] = field(default_factory=list)


# --- Cross-harness child workflow payloads (3rd tab) ---
#
# The cross-harness tab splits the assignment team across agent harnesses, each
# run as its own Temporal child workflow: an ADK child produces the Fleet+Customer
# assessments, a LangGraph child makes the dispatch decision (and owns its own
# ask_human HITL). Inputs/outputs are plain dataclasses — never LangChain/ADK
# objects — so they cross the child boundary cleanly. All fields default so an
# in-flight workflow started against older code replays without raw-attribute reads.


@dataclass
class AdkAssessmentOutput:
    """Returned by AdkAssessmentWorkflow — the two ADK assessment strings only."""

    fleet_assessment: str = ""
    customer_assessment: str = ""


@dataclass
class LgDispatchInput:
    """Seeds LgDispatchWorkflow's dispatch-only graph with order/fleet context
    plus the ADK-produced assessments (so the LangGraph Dispatch agent reasons on
    them without re-running Fleet/Customer)."""

    order_id: str
    venue: str
    order_value: int
    servings: int
    deadline_minutes: int
    proposed_driver_id: str
    drivers_available: int
    drivers_total: int
    pending_orders: int
    fleet_assessment: str = ""
    customer_assessment: str = ""


@dataclass
class LgDispatchOutput:
    """Returned by LgDispatchWorkflow — the dispatch decision and reasoning."""

    decision: str = "DISPATCH"  # "DISPATCH" | "HOLD"
    reasoning: str = ""
    fleet_assessment: str = ""
    customer_assessment: str = ""
    asked_human: bool = False


@dataclass
class NavigateInput:
    driver_id: str
    order_id: str
    target_lat: float
    target_lng: float
    leg: str  # LegType value — "pickup" or "delivery"
    steps: int = 8
    waypoints: list[dict] | None = None  # [{"lat": float, "lng": float}, ...]
    start_lat: float | None = None
    start_lng: float | None = None


@dataclass
class NavigateOutput:
    driver_id: str
    arrived: bool
    final_lat: float
    final_lng: float


@dataclass
class PickupInput:
    driver_id: str
    order_ids: list[str]


@dataclass
class PickupOutput:
    driver_id: str
    success: bool


@dataclass
class DeliverInput:
    driver_id: str
    order_id: str


@dataclass
class DeliverOutput:
    driver_id: str
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
    new_hotel: str | None = None


@dataclass
class ExecuteCustomerChangeInput:
    order_id: str
    change_type: str
    new_lat: float | None = None
    new_lng: float | None = None
    new_hotel: str | None = None


@dataclass
class ExecuteCustomerChangeOutput:
    success: bool


# --- Driver route workflow payloads ---


@dataclass
class DriverRouteOrder:
    order_id: str
    hotel: str
    delivery_lat: float
    delivery_lng: float


@dataclass
class OrderUpdateInput:
    """Signaled to DriverRouteWorkflow when an order's delivery changes."""

    order_id: str
    change_type: str  # "address_change" or "cancel"
    new_lat: float | None = None
    new_lng: float | None = None
    new_hotel: str | None = None


@dataclass
class DriverRouteInput:
    driver_id: str


@dataclass
class OrderDeliveredInput:
    """Signaled from DriverRouteWorkflow to parent when a delivery completes."""

    driver_id: str
    order_id: str
    delivery_lat: float
    delivery_lng: float


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
class DriverDisconnectInput:
    driver_id: str


@dataclass
class AgentDisconnectInput:
    agent_name: str  # "fleet_agent", "customer_agent", or "resolver"


@dataclass
class MeltdownDemoInput:
    escalation_enabled: bool = False
    max_orders: int = 50
    # "adk" | "langgraph" | "crossharness" — set from the active UI tab at start
    dispatch_mode: str = "adk"


@dataclass
class OrderGenerationInput:
    max_orders: int = 50
    order_interval_seconds: int = 15
    warmup_burst_orders: int = 5  # initial fast orders; set 0 to disable the burst


@dataclass
class OrderAssignmentResult:
    """Signaled from OrderGenerationWorkflow to parent with each new order."""

    order_id: str
    hotel: str
    delivery_lat: float
    delivery_lng: float
    driver_id: str
    reasoning_summary: str
    # Order details carried through for assignment
    priority: str = "standard"
    servings: int = 1
    deadline_minutes: int = 45
    event: str = ""
    order_value: int = 0  # USD; high values lead the Dispatch agent to call ask_human
