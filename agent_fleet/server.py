"""
FastAPI server for the Meltdown ice cream delivery demo.

Serves the frontend, exposes fleet state via WebSocket, and provides
API endpoints for demo control (start, reset, crew/agent disconnect,
customer change, approve/reject).

Run with:
    python -m agent_fleet.server
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.service import RPCError

load_dotenv()

from agent_fleet.locations import VENUES, WAREHOUSE, WAREHOUSE_LABEL
from agent_fleet.models import (
    AgentDisconnectInput,
    CrewDisconnectInput,
    CustomerChangeInput,
    MeltdownDemoInput,
)
from agent_fleet.simulation import fleet
from agent_fleet.worker import TASK_QUEUE, TEMPORAL_ADDRESS, create_worker
from agent_fleet.workflows import MeltdownDemoWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Runtime state ---

_escalation_enabled = False
_worker_tasks: list[asyncio.Task] = []
_temporal_client: Client | None = None


# --- Worker lifecycle ---


async def _start_workers() -> None:
    global _worker_tasks, _temporal_client
    if _worker_tasks and any(not t.done() for t in _worker_tasks):
        logger.warning("Workers already running")
        return

    _temporal_client = await Client.connect(TEMPORAL_ADDRESS)
    workers = await create_worker(_temporal_client)

    def _make_run(w):
        async def _run():
            try:
                await w.run()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Worker error: {e}")
        return _run

    _worker_tasks = [asyncio.create_task(_make_run(w)()) for w in workers]
    maps_key = "SET" if os.environ.get("GOOGLE_MAPS_API_KEY") else "NOT SET"
    gemini_key = "SET" if os.environ.get("GOOGLE_API_KEY") else "NOT SET"
    logger.info(f"Workers started (GOOGLE_MAPS_API_KEY={maps_key}, GOOGLE_API_KEY={gemini_key})")


async def _stop_workers() -> None:
    global _worker_tasks
    running = [t for t in _worker_tasks if not t.done()]
    if not running:
        logger.warning("No workers running to stop")
        return
    for t in running:
        t.cancel()
    await asyncio.gather(*running, return_exceptions=True)
    _worker_tasks = []
    logger.info("Workers stopped")


async def _cancel_running_workflows() -> None:
    """Best-effort terminate of known workflow IDs."""
    if _temporal_client is None:
        return
    # Terminate main workflow and all AI-Crew routes
    workflow_ids = ["meltdown-demo"]
    for i in range(1, 4):
        workflow_ids.append(f"route-ai-crew-{i}")
    for wf_id in workflow_ids:
        try:
            handle = _temporal_client.get_workflow_handle(wf_id)
            await handle.terminate("Demo reset")
        except Exception:
            pass
    # Wait for Temporal to fully close the workflows
    await asyncio.sleep(1.0)


# --- App lifecycle ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _start_workers()
    yield
    await _stop_workers()


app = FastAPI(title="Meltdown Ice Cream Delivery", lifespan=lifespan)


# --- Demo control endpoints ---


@app.post("/api/start")
async def start_demo():
    """Start the Meltdown demo workflow."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    # Try to start — if a stale workflow exists, terminate and retry
    for attempt in range(3):
        try:
            handle = await _temporal_client.start_workflow(
                MeltdownDemoWorkflow.run,
                MeltdownDemoInput(escalation_enabled=_escalation_enabled),
                id="meltdown-demo",
                task_queue=TASK_QUEUE,
            )
            return {
                "status": "started",
                "workflow_id": handle.id,
                "escalation_enabled": _escalation_enabled,
            }
        except RPCError as e:
            if "already started" in str(e).lower() and attempt < 2:
                logger.info(f"Stale workflow detected (attempt {attempt + 1}), terminating...")
                await _cancel_running_workflows()
                continue
            raise


@app.post("/api/reset")
async def reset_demo():
    """Cancel running workflows and reset simulation state."""
    await _cancel_running_workflows()
    fleet.reset()
    return {"status": "reset"}


# --- Per-crew disconnect/reconnect ---


class CrewDisconnectRequest(BaseModel):
    crew_id: str = "ai-crew-1"


@app.post("/api/disconnect-crew")
async def disconnect_crew(body: CrewDisconnectRequest):
    """Disconnect a single crew — its activities will fail and Temporal will retry."""
    await fleet.disconnect_crew(body.crew_id)

    # Signal the workflow so it knows
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.crew_disconnected,
                CrewDisconnectInput(crew_id=body.crew_id),
            )
        except Exception as e:
            logger.error(f"Failed to signal crew disconnect: {e}")

    return {
        "status": "crew_disconnected",
        "crew_id": body.crew_id,
        "message": f"AI-Crew {body.crew_id} disconnected. Other crews continue delivering.",
    }


@app.post("/api/reconnect-crew")
async def reconnect_crew(body: CrewDisconnectRequest):
    """Reconnect a crew — Temporal retries its activities and it resumes."""
    await fleet.reconnect_crew(body.crew_id)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.crew_reconnected,
                CrewDisconnectInput(crew_id=body.crew_id),
            )
        except Exception as e:
            logger.error(f"Failed to signal crew reconnect: {e}")

    # Clear recovery phase after a delay (visual replay indicator)
    async def _clear_crew_recovery():
        try:
            await asyncio.sleep(3)
            await fleet.mark_crew_recovery_complete(body.crew_id)
        except Exception as e:
            logger.error(f"Crew recovery clear failed: {e}")

    asyncio.create_task(_clear_crew_recovery())

    return {
        "status": "crew_reconnected",
        "crew_id": body.crew_id,
        "message": (
            f"AI-Crew {body.crew_id} reconnecting. Temporal replaying — crew will resume delivery."
        ),
    }


# --- Per-agent disconnect/reconnect ---


class AgentDisconnectRequest(BaseModel):
    agent_name: str = "fleet_agent"


@app.post("/api/disconnect-agent")
async def disconnect_agent(body: AgentDisconnectRequest):
    """Take a specific agent offline. Other agents compensate."""
    await fleet.disconnect_agent(body.agent_name)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.agent_disconnected,
                AgentDisconnectInput(agent_name=body.agent_name),
            )
        except Exception as e:
            logger.error(f"Failed to signal agent disconnect: {e}")

    return {
        "status": "agent_disconnected",
        "agent_name": body.agent_name,
        "message": f"{body.agent_name} is offline. Other agents will compensate.",
    }


@app.post("/api/reconnect-agent")
async def reconnect_agent(body: AgentDisconnectRequest):
    """Bring a specific agent back online."""
    await fleet.reconnect_agent(body.agent_name)

    # Signal the workflow
    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.agent_reconnected,
                AgentDisconnectInput(agent_name=body.agent_name),
            )
        except Exception as e:
            logger.error(f"Failed to signal agent reconnect: {e}")

    await fleet.publish_agent_event(
        body.agent_name,
        "reconnected",
        f"{body.agent_name} is back online and ready for reasoning.",
        summary=f"{body.agent_name} reconnected",
    )

    return {
        "status": "agent_reconnected",
        "agent_name": body.agent_name,
        "message": f"{body.agent_name} is back online.",
    }


# --- Customer change endpoints ---


class CustomerChangeRequest(BaseModel):
    order_id: str
    change_type: str = "address_change"  # "address_change" or "cancel"
    new_details: str = ""
    new_lat: float | None = None
    new_lng: float | None = None


@app.post("/api/customer-change")
async def submit_customer_change(body: CustomerChangeRequest):
    """Submit a customer change request (triggers human-in-the-loop)."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    change = CustomerChangeInput(
        order_id=body.order_id,
        change_type=body.change_type,
        new_details=body.new_details,
        new_lat=body.new_lat,
        new_lng=body.new_lng,
    )

    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.customer_change, change)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}

    return {
        "status": "change_submitted",
        "order_id": body.order_id,
        "change_type": body.change_type,
    }


class ChangeDecisionRequest(BaseModel):
    approved: bool


@app.post("/api/approve-change")
async def approve_change(body: ChangeDecisionRequest):
    """Approve or reject a pending customer change."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.change_approved, body.approved)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}

    decision = "approved" if body.approved else "rejected"
    return {"status": f"change_{decision}"}


# --- Demo config endpoints ---


@app.post("/api/toggle-escalation")
async def toggle_escalation():
    """Toggle escalation mode (customer change / human-in-the-loop)."""
    global _escalation_enabled
    _escalation_enabled = not _escalation_enabled
    return {
        "status": "escalation_enabled" if _escalation_enabled else "escalation_disabled",
        "escalation_enabled": _escalation_enabled,
    }


# --- State query endpoints ---


@app.get("/api/state")
async def get_state():
    """Get current fleet state as JSON."""
    return await fleet.snapshot()


@app.get("/api/locations")
async def get_locations():
    """Return kitchen and hotel locations for the frontend map."""
    return {
        "warehouse": {
            "lat": WAREHOUSE.lat,
            "lng": WAREHOUSE.lng,
            "label": WAREHOUSE_LABEL,
        },
        "destinations": {
            venue["hotel"]: {
                "lat": venue["coords"].lat,
                "lng": venue["coords"].lng,
                "label": venue["map_label"],
                "map_label": venue["map_label"],
                "sub": "",
                "hotel": venue["hotel"],
            }
            for venue in VENUES
        },
    }


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


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="frontend-static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, log_level="info")
