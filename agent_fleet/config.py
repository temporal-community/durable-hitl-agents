"""Centralized configuration for the Meltdown demo."""

from __future__ import annotations

import os

GOOGLE_API_KEY: str | None = os.environ.get("GOOGLE_API_KEY")
GOOGLE_MAPS_API_KEY: str | None = os.environ.get("GOOGLE_MAPS_API_KEY") or GOOGLE_API_KEY
GOOGLE_CSE_ID: str | None = os.environ.get("GOOGLE_CSE_ID")
GEMINI_MODEL: str = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
TEMPORAL_ADDRESS: str = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")

MOCK_MODE: bool = not GOOGLE_API_KEY
