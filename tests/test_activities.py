"""Unit tests for Temporal activities using ActivityEnvironment."""

import pytest
from temporalio.testing import ActivityEnvironment

from agent_fleet.activities import (
    deliver_order,
    generate_order,
    navigate_to,
    pickup_orders,
    sync_driver_disconnect,
)
from agent_fleet.models import (
    DeliverInput,
    DriverStatus,
    GenerateOrderInput,
    NavigateInput,
    NavigateOutput,
    OrderStatus,
    PickupInput,
    SyncDriverDisconnectInput,
)
from agent_fleet.simulation import fleet


@pytest.fixture
def env():
    return ActivityEnvironment()


async def test_generate_order(env: ActivityEnvironment):
    result = await env.run(generate_order, GenerateOrderInput(order_number=1))
    assert result.order_id == "order-1"
    assert result.hotel  # should have a hotel name
    assert result.servings > 0
    assert result.deadline_minutes > 0

    # Verify order was registered in fleet state
    order = await fleet.get_order("order-1")
    assert order.status == OrderStatus.PENDING
    assert order.hotel == result.hotel


async def test_navigate_to_interpolates_position(env: ActivityEnvironment):
    # Register an order so navigate_to can update its status
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    inp = NavigateInput(
        driver_id="ai-driver-1",
        order_id="order-1",
        target_lat=36.10,
        target_lng=-115.17,
        leg="pickup",
        steps=4,
        start_lat=36.1040,
        start_lng=-115.1530,
    )
    result = await env.run(navigate_to, inp)
    assert isinstance(result, NavigateOutput)
    assert result.final_lat == pytest.approx(36.10, abs=0.01)
    assert result.final_lng == pytest.approx(-115.17, abs=0.01)

    # Driver should be at target position
    lat, lng = await fleet.get_driver_position("ai-driver-1")
    assert lat == pytest.approx(36.10, abs=0.01)
    assert lng == pytest.approx(-115.17, abs=0.01)


async def test_pickup_orders_sets_status(env: ActivityEnvironment):
    # Generate and register an order first
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    await fleet.assign_order_to_driver("ai-driver-1", "order-1")

    result = await env.run(
        pickup_orders,
        PickupInput(driver_id="ai-driver-1", order_ids=["order-1"]),
    )
    assert result.success is True

    c = await fleet.get_driver("ai-driver-1")
    assert c.status == DriverStatus.PICKING_UP

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.PICKED_UP


async def test_deliver_order_sets_status(env: ActivityEnvironment):
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    await fleet.assign_order_to_driver("ai-driver-1", "order-1")
    await env.run(
        pickup_orders,
        PickupInput(driver_id="ai-driver-1", order_ids=["order-1"]),
    )

    result = await env.run(
        deliver_order,
        DeliverInput(driver_id="ai-driver-1", order_id="order-1"),
    )
    assert result.success is True

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.DELIVERED

    c = await fleet.get_driver("ai-driver-1")
    assert "order-1" not in c.current_orders


async def test_sync_driver_disconnect(env: ActivityEnvironment):
    # Disconnect a driver via the sync activity
    await env.run(
        sync_driver_disconnect,
        SyncDriverDisconnectInput(driver_id="ai-driver-1", disconnected=True),
    )
    assert await fleet.is_driver_disconnected("ai-driver-1") is True

    # Reconnect
    await env.run(
        sync_driver_disconnect,
        SyncDriverDisconnectInput(driver_id="ai-driver-1", disconnected=False),
    )
    assert await fleet.is_driver_disconnected("ai-driver-1") is False
