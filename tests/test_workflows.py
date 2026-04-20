"""Integration tests for Temporal workflows using time-skipping test environment.

DriverRouteWorkflow and OrderGenerationWorkflow are tested with mock
activities — no API keys needed. These cover the core Temporal patterns:
signals, activities, cancellation, child workflows.

MeltdownDemoWorkflow requires the full ADK stack (Gemini + GoogleAdkPlugin)
and is tested manually via ./run.sh.
"""

import asyncio
from contextlib import asynccontextmanager

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    set_driver_idle,
    set_warmup_hidden,
    sync_driver_position,
)
from agent_fleet.locations import VENUES
from agent_fleet.mock.activities import mock_get_route_polyline
from agent_fleet.models import (
    DriverRouteInput,
    DriverRouteOrder,
)
from agent_fleet.queues import DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@asynccontextmanager
async def run_delivery_workers(env: WorkflowEnvironment):
    """Start workers for delivery workflow tests (no ADK needed)."""
    from agent_fleet.workflows import DriverRouteWorkflow, OrderGenerationWorkflow

    workflow_worker = Worker(
        env.client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[DriverRouteWorkflow, OrderGenerationWorkflow],
    )
    delivery_worker = Worker(
        env.client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            mock_get_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            set_driver_idle,
            set_warmup_hidden,
            sync_driver_position,
        ],
    )

    async with workflow_worker, delivery_worker:
        yield


async def test_driver_route_completes_with_signal(env: WorkflowEnvironment):
    """DriverRouteWorkflow receives an order via signal, delivers it, then stops."""
    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]

    await fleet.register_order(
        order_id="order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} test delivery",
        priority="standard",
        servings=40,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "order-1")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-driver-a",
            task_queue=WORKFLOWS_QUEUE,
        )

        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        await asyncio.sleep(2)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "driver-a" in result
        assert "1 deliveries" in result or "completed" in result.lower()


async def test_driver_route_handles_multiple_orders(env: WorkflowEnvironment):
    """DriverRouteWorkflow processes multiple orders sequentially."""
    from agent_fleet.workflows import DriverRouteWorkflow

    for i, venue in enumerate(VENUES[:2], 1):
        await fleet.register_order(
            order_id=f"order-{i}",
            hotel=venue["hotel"],
            label=f"{venue['hotel']} test",
            priority="standard",
            servings=40,
            delivery_coords=venue["coords"],
            deadline_minutes=30,
        )
        await fleet.assign_order_to_driver("driver-a", f"order-{i}")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-multi",
            task_queue=WORKFLOWS_QUEUE,
        )

        for i, venue in enumerate(VENUES[:2], 1):
            await handle.signal(
                DriverRouteWorkflow.add_order,
                DriverRouteOrder(
                    order_id=f"order-{i}",
                    hotel=venue["hotel"],
                    delivery_lat=venue["coords"].lat,
                    delivery_lng=venue["coords"].lng,
                ),
            )

        await asyncio.sleep(5)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "2 deliveries" in result


async def test_driver_route_per_order_hitl_holds(env: WorkflowEnvironment):
    """Two update_pending signals for different orders on the same driver
    must each keep their own hold slot — no overwrite, no cross-contamination
    when resolve_update fills in one decision then the other.

    Regression guard for the single-slot _update_pending_order bug: under
    the old single-slot design, the second update_pending would overwrite
    the first and the first order's resolve_update would be silently dropped.
    """
    from agent_fleet.models import OrderUpdateInput
    from agent_fleet.workflows import DriverRouteWorkflow

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-holds",
            task_queue=WORKFLOWS_QUEUE,
        )

        try:
            # Two holds, two different orders — both should coexist.
            await handle.signal(
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(order_id="order-A", change_type="cancel"),
            )
            await handle.signal(
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(order_id="order-B", change_type="address_change"),
            )

            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}, f"expected both holds, got {held}"

            # Resolve order-A only. order-B's hold must remain intact.
            await handle.signal(
                DriverRouteWorkflow.resolve_update,
                OrderUpdateInput(order_id="order-A", change_type="cancel"),
            )
            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}, (
                f"resolve_update shouldn't drop holds — got {held}"
            )

            # Stale resolve_update for an unknown order_id must be a no-op
            # (drops without affecting existing holds — replaces the
            # fragile single-slot guard).
            await handle.signal(
                DriverRouteWorkflow.resolve_update,
                OrderUpdateInput(order_id="order-NONEXISTENT", change_type="cancel"),
            )
            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}
        finally:
            await handle.signal(DriverRouteWorkflow.stop)
            await handle.result()


async def test_deliver_order_cancel_race():
    """If an order was cancelled before deliver_order fires, the activity
    reports success=False so the workflow skips the parent order_delivered
    signal. Direct FleetState + activity test — no workflow env needed.
    """
    from agent_fleet.activities import deliver_order as deliver_order_activity
    from agent_fleet.models import Coords, DeliverInput

    await fleet.reset()
    await fleet.register_order(
        order_id="order-cancel-race",
        hotel="MGM Grand",
        label="MGM — will be cancelled",
        priority="standard",
        servings=10,
        delivery_coords=Coords(lat=36.1024, lng=-115.1725),
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "order-cancel-race")
    await fleet.cancel_order("order-cancel-race")

    # deliver_order is an @activity.defn but it's still a plain async
    # function; invoke directly. The CANCELLED-status short-circuit at the
    # top returns without calling any Temporal activity APIs, so no
    # activity-context is needed for this code path.
    result = await deliver_order_activity(
        DeliverInput(driver_id="driver-a", order_id="order-cancel-race")
    )

    assert result.success is False, (
        "deliver_order must report success=False for a cancelled order — "
        "otherwise the workflow signals the parent order_delivered for a "
        "cancelled order and corrupts bookkeeping"
    )
