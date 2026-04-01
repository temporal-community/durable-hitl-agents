"""Shared fixtures for Meltdown demo tests."""

import pytest

from agent_fleet.simulation import fleet


@pytest.fixture(autouse=True)
def reset_fleet():
    """Reset fleet state before each test."""
    fleet.reset()
    yield
    fleet.reset()
