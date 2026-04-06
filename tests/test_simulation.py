"""Simulation state tests."""

from agent_fleet.models import CrewStatus, OrderStatus
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


async def test_crew_disconnect_and_reconnect():
    await fleet.disconnect_crew("ai-crew-1")
    assert await fleet.is_crew_disconnected("ai-crew-1") is True

    crew = await fleet.get_crew("ai-crew-1")
    assert crew.status == CrewStatus.DISCONNECTED

    await fleet.reconnect_crew("ai-crew-1")
    assert await fleet.is_crew_disconnected("ai-crew-1") is False


async def test_agent_disconnect_and_reconnect():
    await fleet.disconnect_agent("fleet_agent")
    assert await fleet.is_agent_disconnected("fleet_agent") is True
    assert await fleet.is_agent_online("fleet_agent") is False

    await fleet.reconnect_agent("fleet_agent")
    assert await fleet.is_agent_online("fleet_agent") is True


async def test_assign_order_to_crew():
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
    await fleet.assign_order_to_crew("ai-crew-1", "order-1")

    order = await fleet.get_order("order-1")
    assert order.assigned_crew_id == "ai-crew-1"
    assert order.status == OrderStatus.ASSIGNED

    crew = await fleet.get_crew("ai-crew-1")
    assert "order-1" in crew.current_orders
