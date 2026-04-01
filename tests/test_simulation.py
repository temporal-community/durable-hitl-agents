"""Simulation state tests."""

import pytest

from agent_fleet.models import CoolerStatus, DemoEventConfig
from agent_fleet.simulation import fleet


async def test_demo_event_trigger_marks_cooler_malfunction():
    await fleet.set_demo_events(
        DemoEventConfig(
            cooler_malfunction_at_nav_step=2,
            cooler_malfunction_crew="ai-crew-1",
            enabled=True,
        )
    )

    await fleet.increment_nav_step("ai-crew-1")
    assert await fleet.get_cooler_status("ai-crew-1") == CoolerStatus.OK

    await fleet.increment_nav_step("ai-crew-1")
    assert await fleet.get_cooler_status("ai-crew-1") == CoolerStatus.MALFUNCTION
    assert await fleet.get_cooler_temp("ai-crew-1") == pytest.approx(45.0)
