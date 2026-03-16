"""
Temporal worker entry point.

Runs in the same process as the FastAPI server (started from server.py).
Can also be run standalone for testing:
    python worker.py
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

try:
    from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
    _ADK_AVAILABLE = True
except ImportError:
    GoogleAdkPlugin = None
    _ADK_AVAILABLE = False

from agent_fleet.workflows import DeliveryMissionWorkflow, FleetDispatchWorkflow
from agent_fleet.activities import (
    assign_courier,
    navigate_to,
    pickup_package,
    deliver_package,
    get_fleet_status,
    assign_courier_to_mission,
    check_courier_battery,
    check_weather,
    request_human_approval,
    get_mission_assignment,
)

TASK_QUEUE = "courier-fleet"
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
MOCK_MODE = not os.environ.get("GOOGLE_API_KEY") or not _ADK_AVAILABLE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_worker(client: Client) -> Worker:
    """Create a Temporal worker with all workflows and activities registered."""
    if MOCK_MODE:
        logger.info("MOCK MODE: running without Google ADK (no GOOGLE_API_KEY or ADK not available)")
    kwargs = dict(
        task_queue=TASK_QUEUE,
        workflows=[
            DeliveryMissionWorkflow,
            FleetDispatchWorkflow,
        ],
        activities=[
            assign_courier,
            navigate_to,
            pickup_package,
            deliver_package,
            get_fleet_status,
            assign_courier_to_mission,
            check_courier_battery,
            check_weather,
            request_human_approval,
            get_mission_assignment,
        ],
    )
    if not MOCK_MODE:
        kwargs["plugins"] = [GoogleAdkPlugin()]
    return Worker(client, **kwargs)


async def run_worker() -> None:
    """Connect to Temporal and run the worker until interrupted."""
    logger.info(f"Connecting to Temporal at {TEMPORAL_ADDRESS}...")
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = await create_worker(client)
    logger.info(f"Worker started on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
