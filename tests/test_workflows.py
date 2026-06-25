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
from temporalio import activity
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
from agent_fleet.locations import VENUES, WAREHOUSE
from agent_fleet.models import (
    DriverRouteInput,
    DriverRouteOrder,
)
from agent_fleet.queues import DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet


@activity.defn(name="get_route_polyline")
async def _fake_route_polyline(
    origin_lat: float, origin_lng: float, dest_lat: float, dest_lng: float
) -> list[dict[str, float]]:
    """Test stand-in for the Maps Directions activity (no live API call)."""
    return [
        {"lat": origin_lat, "lng": origin_lng},
        {"lat": dest_lat, "lng": dest_lng},
    ]


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
            _fake_route_polyline,
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


async def test_driver_route_continues_as_new(env: WorkflowEnvironment):
    """DriverRouteWorkflow bounds its own history via continue-as-new (the long-lived
    entity pattern) without losing state.

    With a low history_threshold, delivering an order pushes the run past the threshold, so at
    the next idle loop-top the workflow continues-as-new — same workflow id, fresh history,
    carrying forward only its live state. We assert (a) the run id actually rolled over (a
    continue-as-new happened), (b) the first run ended in a CONTINUED_AS_NEW event, and
    (c) the lifetime delivery count survived into the new generation (queried on the new run,
    which has delivered nothing itself).
    """
    from temporalio.api.enums.v1 import EventType

    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]
    await fleet.register_order(
        order_id="can-order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} c-a-n test",
        priority="standard",
        servings=20,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "can-order-1")

    async with run_delivery_workers(env):
        # Low threshold: one delivery pushes history past it, so the next idle loop-top
        # continues-as-new. A fresh run's baseline (~5 events) stays under it, so a continued
        # run that delivers nothing won't loop on itself.
        await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a", history_threshold=15),
            id="test-route-can",
            task_queue=WORKFLOWS_QUEUE,
        )
        handle = env.client.get_workflow_handle("test-route-can")  # tracks the current run
        first_run_id = (await handle.describe()).run_id

        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="can-order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        # Poll until the workflow delivers and continues-as-new (run id rolls over). The real
        # navigate activity takes ~20s for a full delivery, so give it a generous window.
        new_run_id = first_run_id
        for _ in range(60):
            new_run_id = (await handle.describe()).run_id
            if new_run_id != first_run_id:
                break
            await asyncio.sleep(1)
        # (a) a continue-as-new actually occurred
        assert new_run_id != first_run_id, "workflow should have continued-as-new after a delivery"

        # (b) the first run ended specifically in CONTINUED_AS_NEW
        first = env.client.get_workflow_handle("test-route-can", run_id=first_run_id)
        first_events = [e async for e in first.fetch_history_events()]
        assert any(
            e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
            for e in first_events
        ), "first run should have ended in CONTINUED_AS_NEW"

        # (c) lifetime delivery count carried into the new generation, which delivered nothing
        # itself — so a non-zero count can only have come across the continue-as-new boundary.
        status = await handle.query(DriverRouteWorkflow.get_status)
        assert status["lifetime_deliveries"] == 1, (
            f"lifetime count should survive continue-as-new, got {status['lifetime_deliveries']}"
        )

        await handle.signal(DriverRouteWorkflow.stop)


async def test_parent_continue_as_new_decision():
    """The parent's continue-as-new GUARD (pure logic): only at a quiescent point, and only once
    its own history crosses the threshold. These run without a worker/Gemini — the helpers touch
    no workflow APIs — which is how the orchestrator's c-a-n logic is covered even though the full
    MeltdownDemoWorkflow needs the live ADK stack to run end-to-end."""
    from agent_fleet.workflows import MeltdownDemoWorkflow

    wf = MeltdownDemoWorkflow()
    wf._history_threshold = 100

    # Idle + fresh → quiescent, but only crosses once history reaches the threshold.
    assert wf._parent_quiescent() is True
    assert wf._parent_should_continue_as_new(99) is False
    assert wf._parent_should_continue_as_new(100) is True

    # Any in-flight assignment work blocks continue-as-new, even past the threshold.
    wf._pending_new_orders = [object()]
    assert wf._parent_quiescent() is False
    assert wf._parent_should_continue_as_new(10_000) is False
    wf._pending_new_orders = []

    wf._pending_dispatch = {"order-1": {}}  # a dispatch parked on a human
    assert wf._parent_quiescent() is False
    wf._pending_dispatch = {}

    class _Running:
        def done(self):
            return False

    wf._langgraph_tasks = [_Running()]  # a fire-and-forget assignment still running
    assert wf._parent_quiescent() is False


async def test_parent_continue_as_new_state_round_trips():
    """Carried state survives a parent continue-as-new: build an input from one generation's
    live state, apply it to the next, and confirm the capacity ledger, counters, mode, and
    threshold all come across (and the per-driver positions reset to base)."""
    from agent_fleet.workflows import DRIVER_IDS, MeltdownDemoWorkflow

    gen1 = MeltdownDemoWorkflow()
    gen1._dispatch_mode = "crossframework"
    gen1._history_threshold = 12345
    gen1._driver_orders = {"driver-a": ["o1", "o2"], "driver-b": ["o3"]}
    gen1._orders_generated = 7
    gen1._rereason_count = {"o1": 2}

    carried = gen1._build_continue_as_new_input()
    assert carried.max_orders == 0  # order-gen is a surviving child, not restarted
    assert carried.dispatch_mode == "crossframework"
    assert carried.history_threshold == 12345
    assert carried.driver_orders == {"driver-a": ["o1", "o2"], "driver-b": ["o3"]}
    assert carried.orders_generated == 7
    assert carried.rereason_counts == {"o1": 2}

    gen2 = MeltdownDemoWorkflow()
    gen2._apply_continuation(carried)
    assert gen2._orders_generated == 7
    assert gen2._rereason_count == {"o1": 2}
    assert gen2._driver_orders["driver-a"] == ["o1", "o2"]
    assert gen2._driver_orders["driver-b"] == ["o3"]
    # Every known driver is seeded (capacity ledger intact) and positions reset to base.
    assert set(gen2._driver_orders.keys()) == set(DRIVER_IDS)
    assert all(pos == (WAREHOUSE.lat, WAREHOUSE.lng) for pos in gen2._driver_last_position.values())


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
