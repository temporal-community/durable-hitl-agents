"""Centralized configuration for the Meltdown demo."""

from __future__ import annotations

import os
from pathlib import Path

GOOGLE_API_KEY: str | None = os.environ.get("GOOGLE_API_KEY")
GOOGLE_MAPS_API_KEY: str | None = os.environ.get("GOOGLE_MAPS_API_KEY")
DEFAULT_MODEL: str = os.environ.get("DEFAULT_MODEL", "gemini-2.5-flash")
TEMPORAL_ADDRESS: str = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
# Pattern B HITL implementation: "temporal" (default — signal + wait_condition) or
# "interrupt" (LangGraph interrupt() back-pocket). Set HITL_MODE=interrupt to flip.
INTERRUPT_MODE: bool = os.environ.get("HITL_MODE", "temporal").lower() == "interrupt"
FLEET_DB_PATH: str = os.environ.get(
    "FLEET_DB_PATH", str(Path(__file__).parent.parent / "fleet_state.db")
)
