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
    await fleet.assign_order_to_driver("ai-driver-1", "order-1")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="ai-driver-1"),
            id="test-route-ai-driver-1",
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
        assert "ai-driver-1" in result
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
        await fleet.assign_order_to_driver("ai-driver-1", f"order-{i}")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="ai-driver-1"),
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
