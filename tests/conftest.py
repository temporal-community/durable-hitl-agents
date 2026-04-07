"""Shared fixtures for Meltdown demo tests."""

import pytest

from agent_fleet.simulation import fleet


@pytest.fixture(autouse=True)
async def reset_fleet(tmp_path):
    """Reset fleet state before each test (uses a temp SQLite DB)."""
    import agent_fleet.config as cfg

    cfg.FLEET_DB_PATH = str(tmp_path / "test_fleet.db")
    fleet._conn = None  # force reconnection to new path
    fleet._db_path = str(tmp_path / "test_fleet.db")
    await fleet.reset()
    yield
    await fleet.close()
