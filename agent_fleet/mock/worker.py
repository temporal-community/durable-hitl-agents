"""
Self-contained mock worker setup for the Meltdown demo.

Creates 3 workers for mock mode — same queues and workflows as the live
path, but with deterministic mock activities for API-backed services.
No GoogleAdkPlugin needed (no LLM calls in mock mode).

Used by the server when API keys are not set.
"""

from __future__ import annotations

import logging

from temporalio.client import Client
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
    publish_agent_events_batch,
    register_assignment,
    sync_driver_position,
    tool_get_fleet_status,
    tool_get_order_priorities,
)
from agent_fleet.mock.activities import (
    mock_get_route_polyline,
    mock_reason_about_assignment,
    mock_tool_get_route_info,
)
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.workflows import DriverRouteWorkflow, MeltdownDemoWorkflow, OrderGenerationWorkflow

logger = logging.getLogger(__name__)


def create_workflow_worker(client: Client) -> Worker:
    """Workflow worker with local activity support for UI projection.

    No GoogleAdkPlugin needed in mock mode (no sandbox passthroughs required).
    publish_agent_event registered for local activity execution.
    """
    return Worker(
        client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[MeltdownDemoWorkflow, DriverRouteWorkflow, OrderGenerationWorkflow],
        activities=[publish_agent_event, publish_agent_events_batch],
    )


def create_delivery_worker(client: Client) -> Worker:
    """Navigation, pickup, delivery, order generation, and customer change activities.

    Uses real activities that work without API keys (navigation, pickup, delivery)
    plus mock route polyline (needs Google Maps API key in live mode).
    """
    return Worker(
        client,
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
            sync_driver_position,
        ],
        max_concurrent_activities=20,
    )


def create_agents_worker(client: Client) -> Worker:
    """Mock agent activities — deterministic, no LLM calls.

    Registers mock implementations for API-backed activities
    (route info, hotel context, assignment reasoning) plus real
    activities that read FleetState (works without API keys).
    No GoogleAdkPlugin needed in mock mode.
    """
    return Worker(
        client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            tool_get_fleet_status,
            tool_get_order_priorities,
            mock_tool_get_route_info,
            mock_reason_about_assignment,
        ],
        max_concurrent_activities=5,
    )


async def create_worker(client: Client) -> list[Worker]:
    """Create all three mock workers. Same signature as the live worker module."""
    logger.info("Starting workers (MOCK mode — all activities deterministic)")
    return [
        create_workflow_worker(client),
        create_delivery_worker(client),
        create_agents_worker(client),
    ]
