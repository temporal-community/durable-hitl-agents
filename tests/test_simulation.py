"""Simulation state tests."""

from agent_fleet.models import DriverStatus, OrderStatus
from agent_fleet.simulation import fleet


async def test_register_and_get_order():
    from agent_fleet.models import Coords

    await fleet.register_order(
        order_id="order-1",
        hotel="MGM Grand",
        label="MGM Grand test",
        priority="vip",
        servings=60,
        delivery_coords=Coords(lat=36.1024, lng=-115.1725),
        deadline_minutes=30,
    )
    order = await fleet.get_order("order-1")
    assert order.hotel == "MGM Grand"
    assert order.status == OrderStatus.PENDING
    assert order.priority == "vip"


async def test_driver_disconnect_and_reconnect():
    await fleet.disconnect_driver("driver-a")
    assert await fleet.is_driver_disconnected("driver-a") is True

    driver = await fleet.get_driver("driver-a")
    assert driver.status == DriverStatus.DISCONNECTED

    await fleet.reconnect_driver("driver-a")
    assert await fleet.is_driver_disconnected("driver-a") is False


async def test_agent_disconnect_and_reconnect():
    await fleet.disconnect_agent("fleet_agent")
    assert await fleet.is_agent_disconnected("fleet_agent") is True
    assert await fleet.is_agent_online("fleet_agent") is False

    await fleet.reconnect_agent("fleet_agent")
    assert await fleet.is_agent_online("fleet_agent") is True


async def test_assign_order_to_driver():
    from agent_fleet.models import Coords

    await fleet.register_order(
        order_id="order-1",
        hotel="Caesars Palace",
        label="Caesars test",
        priority="standard",
        servings=40,
        delivery_coords=Coords(lat=36.1162, lng=-115.1745),
        deadline_minutes=40,
    )
    await fleet.assign_order_to_driver("driver-a", "order-1")

    order = await fleet.get_order("order-1")
    assert order.assigned_driver_id == "driver-a"
    assert order.status == OrderStatus.ASSIGNED

    driver = await fleet.get_driver("driver-a")
    assert "order-1" in driver.current_orders


async def test_assign_order_degraded_flag():
    """Orders assigned while Fleet Agent is offline should have degraded=True."""
    from agent_fleet.models import Coords

    await fleet.register_order(
        order_id="order-1",
        hotel="MGM Grand",
        label="MGM test",
        priority="vip",
        servings=50,
        delivery_coords=Coords(lat=36.1024, lng=-115.1725),
        deadline_minutes=30,
    )
    # Normal assignment — not degraded
    await fleet.assign_order_to_driver("driver-a", "order-1", degraded=False)
    snapshot = await fleet.snapshot()
    assert snapshot["orders"]["order-1"]["degraded"] is False

    # Degraded assignment
    await fleet.register_order(
        order_id="order-2",
        hotel="Caesars Palace",
        label="Caesars test",
        priority="standard",
        servings=40,
        delivery_coords=Coords(lat=36.1162, lng=-115.1745),
        deadline_minutes=40,
    )
    await fleet.assign_order_to_driver("driver-b", "order-2", degraded=True)
    snapshot = await fleet.snapshot()
    assert snapshot["orders"]["order-2"]["degraded"] is True


async def test_get_driver_position():
    """sync_driver_position relies on get_driver_position returning actual coords."""
    lat, lng = await fleet.get_driver_position("driver-a")
    # Should be at warehouse initially (seeded by reset)
    assert abs(lat - 36.1040) < 0.01
    assert abs(lng - (-115.1530)) < 0.01

    # Move driver and verify position updates
    await fleet.update_driver_position("driver-a", 36.12, -115.18)
    lat, lng = await fleet.get_driver_position("driver-a")
    assert abs(lat - 36.12) < 0.001
    assert abs(lng - (-115.18)) < 0.001
