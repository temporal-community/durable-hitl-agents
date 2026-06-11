"""
Temporal worker entry point for the Meltdown demo.

Runs as a separate process from the FastAPI server.
Three workers on three task queues:
  - meltdown-workflows: workflows only (no activities, dedicated to replay)
  - meltdown-delivery: navigation, pickup, delivery, customer changes
  - meltdown-agents: LLM/ADK tool calls (rate-limited, max 5 concurrent)

Run with:
    python -m agent_fleet.worker
"""

from __future__ import annotations

import asyncio
import logging
import signal

from temporalio.client import Client
from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    get_route_polyline,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    publish_agent_events_batch,
    register_assignment,
    set_driver_idle,
    set_warmup_hidden,
    sync_driver_position,
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
)
from agent_fleet.config import TEMPORAL_ADDRESS
from agent_fleet.dispatch_gate import (
    GRAPH_NAME,
    GRAPH_NAME_INTERRUPT,
    DispatchGateWorkflow,
    build_gate_graph,
)
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.workflows import DriverRouteWorkflow, MeltdownDemoWorkflow, OrderGenerationWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Quiet Temporal SDK internals — "Timer started", replay chatter, etc.
logging.getLogger("temporalio.worker").setLevel(logging.WARNING)
logging.getLogger("temporalio.activity").setLevel(logging.WARNING)
logging.getLogger("temporalio.workflow").setLevel(logging.WARNING)


def create_workflow_worker(client: Client) -> Worker:
    """Workflow worker with local activity support for UI projection.

    GoogleAdkPlugin is needed here for sandbox passthroughs (google.adk,
    google.genai) and deterministic runtime (uuid, time) during replay.
    publish_agent_event registered for local activity execution.
    """
    return Worker(
        client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[
            MeltdownDemoWorkflow,
            DriverRouteWorkflow,
            OrderGenerationWorkflow,
            DispatchGateWorkflow,
        ],
        activities=[publish_agent_event, publish_agent_events_batch],
        # LangGraphPlugin runs the Pattern B dispatch-gate graph; its node
        # activities (incl. the Gemini assess call) execute on this worker.
        # Both gate variants are registered so the UI toggle can pick the HITL impl
        # per dropped order: Temporal-signal (default) or LangGraph interrupt().
        plugins=[
            GoogleAdkPlugin(),
            LangGraphPlugin(
                graphs={
                    GRAPH_NAME: build_gate_graph(use_interrupt=False),
                    GRAPH_NAME_INTERRUPT: build_gate_graph(use_interrupt=True),
                }
            ),
        ],
    )


def create_delivery_worker(client: Client) -> Worker:
    """Navigation, pickup, delivery, order generation, and customer change activities."""
    return Worker(
        client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            get_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            set_driver_idle,
            set_warmup_hidden,
            sync_driver_position,
        ],
        max_concurrent_activities=20,
    )


def create_agents_worker(client: Client) -> Worker:
    """ADK/LLM activities — rate-limited.

    GoogleAdkPlugin registers the invoke_model activity that TemporalModel
    routes LLM calls to.
    """
    return Worker(
        client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            tool_get_fleet_status,
            tool_get_order_priorities,
            tool_get_route_info,
        ],
        max_concurrent_activities=5,
        plugins=[GoogleAdkPlugin()],
    )


async def create_worker(client: Client) -> list[Worker]:
    """Create all three workers. Returns list for server.py to manage."""
    logger.info("Starting workers (LIVE MODE)")
    return [
        create_workflow_worker(client),
        create_delivery_worker(client),
        create_agents_worker(client),
    ]


async def run_worker() -> None:
    """Connect to Temporal and run all workers until interrupted."""
    from agent_fleet.config import GOOGLE_API_KEY, GOOGLE_MAPS_API_KEY

    if not GOOGLE_API_KEY:
        logger.warning("GOOGLE_API_KEY not set — live mode requires it (mock mode removed).")
    _create = create_worker
    mode = "LIVE"

    maps_key = "SET" if GOOGLE_MAPS_API_KEY else "NOT SET"
    gemini_key = "SET" if GOOGLE_API_KEY else "NOT SET"
    logger.info(
        f"Worker mode: {mode} (GOOGLE_MAPS_API_KEY={maps_key}, GOOGLE_API_KEY={gemini_key})"
    )

    logger.info(f"Connecting to Temporal at {TEMPORAL_ADDRESS}...")
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )
    workers = await _create(client)
    logger.info(f"Workers started on queues: {WORKFLOWS_QUEUE}, {DELIVERY_QUEUE}, {AGENTS_QUEUE}")

    # Graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Run all workers; cancel on shutdown signal
    tasks = [asyncio.create_task(w.run()) for w in workers]
    shutdown_task = asyncio.create_task(shutdown_event.wait())
    done, _ = await asyncio.wait([*tasks, shutdown_task], return_when=asyncio.FIRST_COMPLETED)
    if shutdown_event.is_set():
        logger.info("Shutdown signal received, stopping workers...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Workers stopped.")


if __name__ == "__main__":
    asyncio.run(run_worker())
