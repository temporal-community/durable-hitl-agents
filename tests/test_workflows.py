"""Integration tests for Temporal workflows using time-skipping test environment."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta

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
    reason_about_assignment,
    register_assignment,
    sync_driver_disconnect,
    sync_driver_recovery_complete,
)
from agent_fleet.locations import VENUES
from agent_fleet.mock_activities import (
    mock_get_route_polyline,
    mock_tool_get_route_info,
    mock_tool_search_hotel_context,
)
from agent_fleet.models import (
    CustomerChangeInput,
    DriverRouteInput,
    DriverRouteOrder,
    MeltdownDemoInput,
)
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@asynccontextmanager
async def run_workers(env: WorkflowEnvironment):
    """Start three workers matching production topology, using mock API activities."""
    from agent_fleet.workflows import DriverRouteWorkflow, MeltdownDemoWorkflow

    workflow_worker = Worker(
        env.client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow],
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
            mock_get_route_polyline,  # mock — no real Google Maps API in tests
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            sync_driver_disconnect,
            sync_driver_recovery_complete,
        ],
    )
    agents_worker = Worker(
        env.client,
        task_queue=AGENTS_QUEUE,
        activities=[
            reason_about_assignment,
            register_assignment,
            mock_tool_get_route_info,  # mock
            mock_tool_search_hotel_context,  # mock
        ],
    )

    async with workflow_worker, delivery_worker, agents_worker:
        yield


async def test_driver_route_completes_with_signal(env: WorkflowEnvironment):
    """DriverRouteWorkflow receives an order via signal, delivers it, then stops."""
    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]  # MGM Grand

    # Register the order in fleet state so activities can update UI projection
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

    async with run_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="ai-driver-1"),
            id="test-route-ai-driver-1",
            task_queue=WORKFLOWS_QUEUE,
        )

        # Signal the order
        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        # Let it process, then stop
        await asyncio.sleep(2)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "ai-driver-1" in result
        assert "1 deliveries" in result or "completed" in result.lower()


async def test_meltdown_demo_completes(env: WorkflowEnvironment):
    """Full demo workflow generates orders, assigns drivers, and completes."""
    from agent_fleet.workflows import MeltdownDemoWorkflow

    async with run_workers(env):
        result = await env.client.execute_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=2),
            id="test-meltdown-demo",
            task_queue=WORKFLOWS_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )
        assert "complete" in result.lower()


async def test_meltdown_demo_handles_customer_change(env: WorkflowEnvironment):
    from agent_fleet.workflows import MeltdownDemoWorkflow

    async with run_workers(env):
        handle = await env.client.start_workflow(
            MeltdownDemoWorkflow.run,
            MeltdownDemoInput(escalation_enabled=False, max_orders=2),
            id="test-meltdown-demo-customer-changes",
            task_queue=WORKFLOWS_QUEUE,
            execution_timeout=timedelta(minutes=10),
        )

        # Wait for orders to be generated
        await asyncio.sleep(3)

        await handle.signal(
            MeltdownDemoWorkflow.customer_change,
            CustomerChangeInput(
                order_id="order-1",
                change_type="address_change",
                new_details="Move to alternate loading bay",
                new_lat=36.1111,
                new_lng=-115.1666,
            ),
        )

        await handle.signal(MeltdownDemoWorkflow.change_approved, True)

        result = await handle.result()
        assert "complete" in result.lower()
