"""
FastAPI server — serves the frontend, exposes fleet state via WebSocket,
and provides API endpoints to start/stop the demo.

Run with:
    python server.py
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.service import RPCError

from agent_fleet.models import DemoEventConfig
from agent_fleet.simulation import fleet
from agent_fleet.worker import create_worker, TASK_QUEUE, TEMPORAL_ADDRESS
from agent_fleet.workflows import FleetDispatchWorkflow, FleetDispatchInput

from agent_fleet.locations import WAREHOUSE, WAREHOUSE_LABEL, DELIVERY_DESTINATIONS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Escalation state (mutable at runtime, passed to new workflows) ---

_escalation_enabled = False

# --- Worker lifecycle ---

_worker_task: asyncio.Task | None = None
_temporal_client: Client | None = None


async def _start_worker() -> None:
    """Start the Temporal worker as a background task."""
    global _worker_task, _temporal_client
    if _worker_task and not _worker_task.done():
        logger.warning("Worker already running")
        return

    _temporal_client = await Client.connect(TEMPORAL_ADDRESS)
    worker = await create_worker(_temporal_client)

    async def _run():
        try:
            await worker.run()
        except asyncio.CancelledError:
            logger.info("Worker task cancelled")
        except Exception as e:
            logger.error(f"Worker error: {e}")

    _worker_task = asyncio.create_task(_run())
    logger.info("Worker started")


async def _stop_worker() -> None:
    """Stop the worker — simulates a service crash for the demo."""
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None
        logger.info("Worker stopped (simulating service crash)")
    else:
        logger.warning("No worker running to stop")


async def _cancel_running_workflows() -> None:
    """Best-effort cancel of known workflow IDs."""
    if _temporal_client is None:
        return
    for wf_id in ["fleet-dispatch", "delivery-mission-1", "delivery-mission-2"]:
        try:
            handle = _temporal_client.get_workflow_handle(wf_id)
            await handle.cancel()
        except Exception:
            pass  # workflow may not exist or already completed


# --- App lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start worker on boot
    await _start_worker()
    yield
    # Shutdown
    await _stop_worker()


app = FastAPI(title="Courier Fleet Demo", lifespan=lifespan)


# --- API endpoints ---

@app.post("/api/start")
async def start_missions():
    """Kick off all delivery missions via the FleetDispatchWorkflow."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    mission_ids = list(fleet.missions.keys())

    try:
        handle = await _temporal_client.start_workflow(
            FleetDispatchWorkflow.run,
            FleetDispatchInput(
                mission_ids=mission_ids,
                escalation_enabled=_escalation_enabled,
            ),
            id="fleet-dispatch",
            task_queue=TASK_QUEUE,
        )
    except RPCError as e:
        if "already started" in str(e).lower():
            return {
                "error": "Workflow already running. Reset the demo first.",
                "status": "already_running",
            }
        raise

    return {
        "status": "started",
        "workflow_id": handle.id,
        "missions": mission_ids,
    }


@app.post("/api/stop-worker")
async def stop_worker():
    """Stop the Temporal worker to simulate a service crash. Activities in flight
    will time out, and Temporal will retry them when a worker reconnects."""
    await _stop_worker()
    return {
        "status": "worker_stopped",
        "message": "Service crashed. Activities will timeout and retry when service restarts.",
    }


@app.post("/api/restart-worker")
async def restart_worker():
    """Restart the Temporal worker. Temporal will replay workflows and
    retry any failed activities — couriers resume from where they were."""
    await _start_worker()
    return {
        "status": "worker_restarted",
        "message": "Service back online. Temporal replaying workflows...",
    }


@app.post("/api/reset")
async def reset_state():
    """Cancel running workflows and reset simulation state for a fresh demo run."""
    await _cancel_running_workflows()
    fleet.reset()
    return {"status": "reset"}


@app.get("/api/locations")
async def get_locations():
    """Return warehouse and delivery destinations for the frontend map."""
    return {
        "warehouse": {"lat": WAREHOUSE.lat, "lng": WAREHOUSE.lng, "label": WAREHOUSE_LABEL},
        "destinations": {
            mid: {
                "lat": info["coords"].lat,
                "lng": info["coords"].lng,
                "label": info["map_label"],
            }
            for mid, info in DELIVERY_DESTINATIONS.items()
        },
    }


@app.get("/api/state")
async def get_state():
    """Get current fleet state as JSON."""
    return await fleet.snapshot()


# --- New endpoints ---

class HumanDecisionRequest(BaseModel):
    decision: str = "continue"


@app.post("/api/missions/{mission_id}/decide")
async def send_human_decision(mission_id: str, body: HumanDecisionRequest):
    """Send a human decision signal to a running delivery workflow (escalation mode only)."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    try:
        handle = _temporal_client.get_workflow_handle(f"delivery-{mission_id}")
        await handle.signal("human_decision", body.decision)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}", "mission_id": mission_id}

    return {"status": "signal_sent", "mission_id": mission_id, "decision": body.decision}


class DemoEventConfigRequest(BaseModel):
    battery_drop_at_nav_step: int | None = None
    battery_drop_to_pct: float = 15.0
    weather_storm_at_nav_step: int | None = None
    enabled: bool = False


@app.post("/api/demo-events")
async def configure_demo_events(config: DemoEventConfigRequest):
    """Configure demo event injection (battery drops, weather storms)."""
    demo_config = DemoEventConfig(
        battery_drop_at_nav_step=config.battery_drop_at_nav_step,
        battery_drop_to_pct=config.battery_drop_to_pct,
        weather_storm_at_nav_step=config.weather_storm_at_nav_step,
        enabled=config.enabled,
    )
    await fleet.set_demo_events(demo_config)
    return {"status": "configured", "config": config.model_dump()}


@app.post("/api/toggle-escalation")
async def toggle_escalation():
    """Toggle escalation mode. Takes effect on the next workflow start."""
    global _escalation_enabled
    _escalation_enabled = not _escalation_enabled
    status = "escalation_enabled" if _escalation_enabled else "escalation_disabled"
    return {"status": status, "escalation_enabled": _escalation_enabled}


# --- WebSocket for real-time state updates ---

@app.websocket("/ws")
async def websocket_state(ws: WebSocket):
    """Push fleet state to the frontend every 300ms."""
    await ws.accept()
    last_snapshot: str | None = None
    try:
        while True:
            data = json.dumps(await fleet.snapshot())
            if data != last_snapshot:
                await ws.send_text(data)
                last_snapshot = data
            await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket closed: {e}")


# --- Frontend ---

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@app.get("/")
async def index():
    return FileResponse(FRONTEND_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
