"""Data models for the courier fleet demo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class CourierStatus(str, enum.Enum):
    IDLE = "idle"
    EN_ROUTE_PICKUP = "en_route_pickup"
    PICKING_UP = "picking_up"
    EN_ROUTE_DELIVERY = "en_route_delivery"
    DELIVERING = "delivering"
    RETURNING = "returning"
    FAILED = "failed"
    RECOVERED = "recovered"


class LegType(str, enum.Enum):
    PICKUP = "pickup"
    DELIVERY = "delivery"


class MissionStatus(str, enum.Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class MonitorDecision(str, enum.Enum):
    CONTINUE = "CONTINUE"
    REROUTE = "REROUTE"
    RETURN_TO_BASE = "RETURN_TO_BASE"
    ESCALATE = "ESCALATE"


@dataclass
class Coords:
    lat: float
    lng: float

    def to_dict(self) -> dict[str, float]:
        return {"lat": self.lat, "lng": self.lng}


@dataclass
class Courier:
    courier_id: str
    position: Coords
    battery_pct: float = 100.0
    status: CourierStatus = CourierStatus.IDLE
    current_mission_id: str | None = None
    path_history: list[dict[str, float]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "courier_id": self.courier_id,
            "position": self.position.to_dict(),
            "battery_pct": self.battery_pct,
            "status": self.status.value,
            "current_mission_id": self.current_mission_id,
            "path_history": self.path_history,
        }


@dataclass
class Mission:
    mission_id: str
    order_label: str
    pickup_coords: Coords
    delivery_coords: Coords
    assigned_courier_id: str | None = None
    status: MissionStatus = MissionStatus.PENDING
    status_log: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "order_label": self.order_label,
            "pickup_coords": self.pickup_coords.to_dict(),
            "delivery_coords": self.delivery_coords.to_dict(),
            "assigned_courier_id": self.assigned_courier_id,
            "status": self.status.value,
            "status_log": self.status_log,
        }


# --- Serializable payloads for Temporal activities ---


@dataclass
class NavigateInput:
    courier_id: str
    mission_id: str
    target_lat: float
    target_lng: float
    leg: LegType
    steps: int = 8  # number of position updates to simulate


@dataclass
class NavigateOutput:
    courier_id: str
    arrived: bool
    final_lat: float
    final_lng: float


@dataclass
class PackageInput:
    courier_id: str
    mission_id: str


@dataclass
class PackageOutput:
    courier_id: str
    mission_id: str
    success: bool


@dataclass
class AssignCourierInput:
    mission_id: str


@dataclass
class AssignCourierOutput:
    courier_id: str
    mission_id: str


@dataclass
class FleetStatusInput:
    pass


@dataclass
class FleetStatusOutput:
    summary: str


@dataclass
class AssignCourierToMissionInput:
    mission_id: str
    courier_id: str


@dataclass
class AssignCourierToMissionOutput:
    courier_id: str
    mission_id: str
    success: bool


@dataclass
class CheckBatteryInput:
    courier_id: str


@dataclass
class CheckBatteryOutput:
    battery_pct: float
    is_critical: bool


@dataclass
class CheckWeatherInput:
    courier_id: str


@dataclass
class CheckWeatherOutput:
    condition: str
    safe_to_fly: bool


@dataclass
class HumanApprovalInput:
    mission_id: str
    reason: str


@dataclass
class HumanApprovalOutput:
    approved: bool
    decision: str


@dataclass
class GetMissionAssignmentInput:
    mission_id: str


@dataclass
class GetMissionAssignmentOutput:
    mission_id: str
    courier_id: str | None


@dataclass
class DemoEventConfig:
    battery_drop_at_nav_step: int | None = None
    battery_drop_to_pct: float = 15.0
    weather_storm_at_nav_step: int | None = None
    enabled: bool = False


# --- Constants ---
