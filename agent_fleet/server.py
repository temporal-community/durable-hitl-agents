"""
FastAPI server for the Meltdown ice cream delivery demo.

Serves the frontend, exposes fleet state via WebSocket, and provides
API endpoints for demo control (start, reset, driver/agent disconnect,
customer change, approve/reject).

Workers run in a separate process (python -m agent_fleet.worker).

Run with:
    python -m agent_fleet.server
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter
from temporalio.service import RPCError

load_dotenv()

from agent_fleet.config import TEMPORAL_ADDRESS
from agent_fleet.locations import COSMOPOLITAN, VENUES, WAREHOUSE, WAREHOUSE_LABEL
from agent_fleet.models import (
    AgentDisconnectInput,
    CustomerChangeInput,
    DriverDisconnectInput,
    MeltdownDemoInput,
    OrderAssignmentResult,
    OrderUpdateInput,
)
from agent_fleet.queues import WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet
from agent_fleet.workflows import MeltdownDemoWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Runtime state ---

_escalation_enabled = False
_temporal_client: Client | None = None


# --- Workflow management ---


async def _cancel_running_workflows() -> None:
    """Best-effort terminate of known workflow IDs."""
    if _temporal_client is None:
        return
    # Terminate main workflow, order generation, and all Driver routes
    workflow_ids = ["meltdown-demo", "order-generation"]
    for letter in ["a", "b", "c", "d", "e"]:
        workflow_ids.append(f"route-driver-{letter}")
    for wf_id in workflow_ids:
        try:
            handle = _temporal_client.get_workflow_handle(wf_id)
            await handle.terminate("Demo reset")
        except Exception:
            pass
    # Wait for Temporal to fully close workflows and in-flight activities to drain
    await asyncio.sleep(2.0)


# --- App lifecycle ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _temporal_client
    _temporal_client = await Client.connect(
        TEMPORAL_ADDRESS,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )
    logger.info(f"Connected to Temporal at {TEMPORAL_ADDRESS}")
    yield


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
                task_queue=WORKFLOWS_QUEUE,
                static_summary="Meltdown — ice cream fleet orchestrator",
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
    """Cancel running workflows and reset state."""
    await _cancel_running_workflows()
    await fleet.reset()
    # Second reset catches any writes from activities that drained after first reset
    await asyncio.sleep(0.5)
    await fleet.reset()
    return {"status": "reset"}


# --- Per-driver disconnect/reconnect ---


class DriverDisconnectRequest(BaseModel):
    driver_id: str = "driver-a"


@app.post("/api/disconnect-crew")
async def disconnect_driver(body: DriverDisconnectRequest):
    """Disconnect a driver — sends signals only, everything flows through Temporal."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    # Update FleetState for frontend display
    await fleet.disconnect_driver(body.driver_id)

    # Signal both parent orchestrator and the driver's child workflow
    try:
        parent = _temporal_client.get_workflow_handle("meltdown-demo")
        await parent.signal(
            MeltdownDemoWorkflow.driver_disconnected,
            DriverDisconnectInput(driver_id=body.driver_id),
        )
    except Exception as e:
        logger.error(f"Failed to signal parent workflow: {e}")
    try:
        child = _temporal_client.get_workflow_handle(f"route-{body.driver_id}")
        await child.signal(
            "driver_disconnected",
            DriverDisconnectInput(driver_id=body.driver_id),
        )
    except Exception as e:
        logger.error(f"Failed to signal driver workflow: {e}")

    return {
        "status": "driver_disconnected",
        "driver_id": body.driver_id,
        "message": f"Driver {body.driver_id} disconnected. Other drivers continue delivering.",
    }


@app.post("/api/reconnect-crew")
async def reconnect_driver(body: DriverDisconnectRequest):
    """Reconnect a driver — sends signals only, everything flows through Temporal."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    # Update FleetState for frontend display
    await fleet.reconnect_driver(body.driver_id)

    # Clear recovery indicator after a short delay
    async def _clear_recovery():
        await asyncio.sleep(3)
        await fleet.mark_driver_recovery_complete(body.driver_id)

    asyncio.create_task(_clear_recovery())

    # Signal both workflows
    try:
        parent = _temporal_client.get_workflow_handle("meltdown-demo")
        await parent.signal(
            MeltdownDemoWorkflow.driver_reconnected,
            DriverDisconnectInput(driver_id=body.driver_id),
        )
    except Exception as e:
        logger.error(f"Failed to signal parent workflow: {e}")
    try:
        child = _temporal_client.get_workflow_handle(f"route-{body.driver_id}")
        await child.signal(
            "driver_reconnected",
            DriverDisconnectInput(driver_id=body.driver_id),
        )
    except Exception as e:
        logger.error(f"Failed to signal driver workflow: {e}")

    return {
        "status": "driver_reconnected",
        "driver_id": body.driver_id,
        "message": (
            f"Driver {body.driver_id} reconnecting. "
            f"Temporal replaying — driver will resume delivery."
        ),
    }


# --- Per-agent disconnect/reconnect ---


class AgentDisconnectRequest(BaseModel):
    agent_name: str = "fleet_agent"


@app.post("/api/disconnect-agent")
async def disconnect_agent(body: AgentDisconnectRequest):
    """Take a specific agent offline."""
    await fleet.disconnect_agent(body.agent_name)

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

    if _temporal_client is not None:
        try:
            handle = _temporal_client.get_workflow_handle("meltdown-demo")
            await handle.signal(
                MeltdownDemoWorkflow.agent_reconnected,
                AgentDisconnectInput(agent_name=body.agent_name),
            )
        except Exception as e:
            logger.error(f"Failed to signal agent reconnect: {e}")

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
    new_hotel: str | None = None


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
        new_hotel=body.new_hotel,
    )

    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.customer_change, change)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}

    # Signal the child workflow directly to hold before delivery —
    # same pattern as disconnect/reconnect. Parent handles the approval
    # decision, child handles the operational pause independently.
    order = await fleet.get_order(body.order_id)
    if order and order.assigned_driver_id:
        try:
            child = _temporal_client.get_workflow_handle(f"route-{order.assigned_driver_id}")
            await child.signal(
                "update_pending",
                OrderUpdateInput(
                    order_id=body.order_id,
                    change_type=body.change_type,
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to signal child for hold: {e}")

    return {
        "status": "change_submitted",
        "order_id": body.order_id,
        "change_type": body.change_type,
    }


@app.post("/api/revise-order")
async def revise_order(body: CustomerChangeRequest):
    """Human→agent HITL (ADK, in the reasoning loop): a human revises an order's location/
    details and the ADK assignment agent RE-REASONS how to adjust — re-checking the fleet
    and re-deciding the driver — rather than the system applying a fixed change.
    """
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}

    change = CustomerChangeInput(
        order_id=body.order_id,
        change_type=body.change_type,
        new_details=body.new_details,
        new_lat=body.new_lat,
        new_lng=body.new_lng,
        new_hotel=body.new_hotel,
    )
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.human_revise_order, change)
    except RPCError:
        logger.exception("Failed to signal workflow for revise-order")
        return {"error": "Failed to submit order revision"}

    return {"status": "revision_submitted", "order_id": body.order_id}


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


# --- Pattern B: agent-initiated dispatch approval gate ---


class DispatchDecisionRequest(BaseModel):
    order_id: str
    approved: bool


@app.get("/api/pending-dispatch")
async def pending_dispatch():
    """Orders the dispatch agent escalated that are awaiting a human decision."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        status = await handle.query(MeltdownDemoWorkflow.get_status)
    except RPCError as e:
        return {"error": f"Failed to query workflow: {e}"}
    return {"pending_dispatch": status.get("pending_dispatch", {})}


@app.post("/api/approve-dispatch")
async def approve_dispatch(body: DispatchDecisionRequest):
    """Answer an agent's in-loop ask_human for a high-value dispatch.

    Signals the demo workflow's `answer_dispatch` — the durable async endpoint the
    LangGraph Dispatch/Fleet agent's ask_human tool is parked on (via interrupt()). The
    answer flows back into the agent's reasoning loop.
    """
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    decision = "approve" if body.approved else "reject"
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(
            MeltdownDemoWorkflow.answer_dispatch, args=[body.order_id, decision]
        )
    except RPCError as e:
        logging.exception("Failed to signal dispatch answer")
        return {"error": "Failed to signal dispatch answer"}
    return {
        "status": "dispatch_approved" if body.approved else "dispatch_rejected",
        "order_id": body.order_id,
    }


_injected_order_count = 0


@app.post("/api/inject-order")
async def inject_high_value_order():
    """Drop a premium Moscone catering order on demand — the agent will call ask_human.

    This is the deliberate trigger for the agent-in-the-loop (Pattern B) demo:
    registers the order in FleetState (so it shows on the map) and signals the workflow.
    Because of its value, the LangGraph Dispatch agent calls ask_human mid-reasoning.
    """
    global _injected_order_count
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    _injected_order_count += 1
    oid = f"order-vip-{_injected_order_count}"
    venue = next((v for v in VENUES if v["vip_tier"] == "platinum"), VENUES[0])
    servings, value, deadline, event = 120, 5400, 30, "conference catering"
    await fleet.register_order(
        order_id=oid,
        hotel=venue["hotel"],
        label=f"{venue['hotel']} {event} — {servings} servings",
        priority="vip",
        servings=servings,
        delivery_coords=venue["coords"],
        deadline_minutes=deadline,
    )
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(
            MeltdownDemoWorkflow.new_order,
            OrderAssignmentResult(
                order_id=oid,
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
                driver_id="",
                reasoning_summary="",
                priority="vip",
                servings=servings,
                deadline_minutes=deadline,
                event=event,
                order_value=value,
            ),
        )
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}
    return {
        "status": "injected",
        "order_id": oid,
        "order_value": value,
    }


class DispatchModeRequest(BaseModel):
    mode: str = "adk"  # "adk" (Human → Agent tab) or "langgraph" (Agent → Human tab)


@app.post("/api/dispatch-mode")
async def set_dispatch_mode(body: DispatchModeRequest):
    """The active UI tab sets which framework dispatches all orders."""
    if _temporal_client is None:
        return {"error": "Temporal client not connected"}
    try:
        handle = _temporal_client.get_workflow_handle("meltdown-demo")
        await handle.signal(MeltdownDemoWorkflow.set_dispatch_mode, body.mode)
    except RPCError as e:
        return {"error": f"Failed to signal workflow: {e}"}
    return {"status": "dispatch_mode_set", "mode": body.mode}


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


async def _build_snapshot() -> dict:
    """Build frontend state from FleetState (SQLite, shared across processes).

    Activities write positions, statuses, and agent events to FleetState.
    Server disconnect/reconnect endpoints write disconnect state directly.
    """
    return await fleet.snapshot()


@app.get("/api/state")
async def get_state():
    """Get current fleet state by querying Temporal workflows."""
    return await _build_snapshot()


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
        "reroute_destination": {
            "hotel": COSMOPOLITAN["hotel"],
            "lat": COSMOPOLITAN["coords"].lat,
            "lng": COSMOPOLITAN["coords"].lng,
            "label": COSMOPOLITAN["map_label"],
        },
    }


# --- WebSocket for real-time state updates ---


@app.websocket("/ws")
async def websocket_state(ws: WebSocket):
    """Push fleet state to the frontend every 300ms via Temporal workflow queries."""
    await ws.accept()
    last_snapshot: str | None = None
    try:
        while True:
            snapshot = await _build_snapshot()
            data = json.dumps(snapshot)
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
