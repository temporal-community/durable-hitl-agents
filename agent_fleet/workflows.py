"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts driver and order-generation
child workflows. Owns driver state, runs multi-agent assignment per order,
and signals the chosen driver. Handles customer-change signals concurrently.

OrderGenerationWorkflow — child workflow that generates orders on a timer
and signals the parent with each new order for assignment.

DriverRouteWorkflow — per-driver continuous delivery loop (child workflow).
Waits for orders via signal, picks up at the shop, delivers, repeats.
Uses cancellation scopes for workflow-driven disconnect handling.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part

    from agent_fleet.activities import (
        deliver_order,
        execute_customer_change,
        generate_order,
        get_route_polyline,
        navigate_to,
        pickup_orders,
        publish_agent_event,
        publish_agent_events_batch,
        register_assignment,
        set_driver_idle,
        set_warmup_hidden,
        sync_driver_position,
    )
    from agent_fleet.agents import create_order_assignment_agent
    from agent_fleet.locations import WAREHOUSE
    from agent_fleet.models import (
        AgentDisconnectInput,
        CustomerChangeInput,
        DeliverInput,
        DriverDisconnectInput,
        DriverRouteInput,
        DriverRouteOrder,
        DriverSnapshot,
        ExecuteCustomerChangeInput,
        GenerateOrderInput,
        MeltdownDemoInput,
        NavigateInput,
        NavigateOutput,
        OrderAssignmentResult,
        OrderDeliveredInput,
        OrderGenerationInput,
        OrderUpdateInput,
        PickupInput,
        PublishAgentEventInput,
        ReasonAboutAssignmentInput,
        ReasonAboutAssignmentOutput,
    )
    from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE

FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)
NAV_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)

MAX_ORDERS = 50
ORDER_INTERVAL_SECONDS = 10

DRIVER_CAPACITY = 3
DRIVER_IDS = ["driver-a", "driver-b", "driver-c", "driver-d", "driver-e"]


@dataclass
class PendingHold:
    """Per-order HITL hold state on a DriverRouteWorkflow.

    Replaces the prior single-slot fields (`_update_pending_order`,
    `_update_decision`, `_update_new_*`). Those collapsed any concurrent
    hold onto one slot, so a second `update_pending` for a different
    order on the same driver would overwrite the first — dropping the
    first order's approved decision silently. Keying holds by order_id
    prevents that entirely: each order has its own slot, and signals
    (update_pending, resolve_update) target a specific order.
    """

    decision: str | None = None  # "cancel", "address_change", "release"
    new_lat: float | None = None
    new_lng: float | None = None
    new_hotel: str | None = None


# --- Per-driver continuous delivery workflow ---


@workflow.defn
class DriverRouteWorkflow:
    """
    Continuous delivery loop for a single Driver.

    Waits for orders via signal, picks up at the shop, delivers to hotel,
    then returns to waiting. Loops until told to stop.

    Disconnect handling uses the Temporal-native retry pattern:
    - Activities check FleetState for disconnect status on each heartbeat
    - When disconnected, activities fail — Temporal retries with backoff
    - When reconnected, the next retry succeeds and delivery resumes
    - No workflow-side cancellation needed
    """

    def __init__(self) -> None:
        self._pending_orders: list[DriverRouteOrder] = []
        self._stop = False
        self._is_disconnected: bool = False
        self._current_lat: float = 0.0
        self._current_lng: float = 0.0
        self._driver_id: str = ""
        self._status: str = "idle"
        self._current_orders: list[str] = []
        self._path_history: list[dict] = []
        self._is_recovering: bool = False
        self._position_sync_needed: bool = False
        # HITL state: one hold per order_id. Each entry is created when
        # update_pending arrives for that order and populated when
        # resolve_update arrives. Removed when the hold is processed by the
        # delivery loop (or when cancel_order removes the order from the
        # queue). See PendingHold docstring for why this replaced the prior
        # single-slot design.
        self._active_order_id: str | None = None
        self._pending_holds: dict[str, PendingHold] = {}
        self._cancel_pending: bool = False
        self._batch_orders: list[DriverRouteOrder] = []

    # --- Signals ---

    @workflow.signal
    async def add_order(self, order: DriverRouteOrder) -> None:
        self._pending_orders.append(order)
        self._current_orders.append(order.order_id)

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.signal
    async def driver_disconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = True
        self._status = "disconnected"
        workflow.logger.info(
            f"Driver {inp.driver_id} disconnected — activities will fail and retry"
        )

    @workflow.signal
    async def driver_reconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = False
        self._position_sync_needed = True
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def update_pending(self, inp: OrderUpdateInput) -> None:
        """Register a HITL hold for this order. Idempotent — multiple sends
        for the same order are a no-op. Each order gets its own slot in
        _pending_holds so concurrent holds for different orders on the same
        driver don't collide.
        """
        if inp.order_id not in self._pending_holds:
            self._pending_holds[inp.order_id] = PendingHold()
        workflow.logger.info(f"Order {inp.order_id} — update pending, will hold before delivery")

    @workflow.signal
    async def resolve_update(self, inp: OrderUpdateInput) -> None:
        """Fill in the HITL decision for a specific order's hold.

        Drops if there's no matching hold (stale resolution, e.g. the order
        was cancelled out of the queue by cancel_order before this arrived,
        or the parent is sending resolve_update for a different change than
        the child is tracking). With per-order holds the drop is strictly
        "no hold to resolve"; it can never accidentally clobber another
        order's decision.
        """
        hold = self._pending_holds.get(inp.order_id)
        if hold is None:
            workflow.logger.info(
                f"Order {inp.order_id} — resolve_update ignored (no matching hold)"
            )
            return
        hold.decision = inp.change_type  # "cancel", "address_change", "release"
        if inp.new_lat is not None:
            hold.new_lat = inp.new_lat
        if inp.new_lng is not None:
            hold.new_lng = inp.new_lng
        if inp.new_hotel is not None:
            hold.new_hotel = inp.new_hotel
        workflow.logger.info(f"Order {inp.order_id} — update resolved: {inp.change_type}")

    @workflow.signal
    async def cancel_order(self, inp: OrderUpdateInput) -> None:
        """Cancel an order — works for pending and batched orders.

        Also removes any HITL hold tracked for this order, so a later
        delivery loop iteration doesn't wait on a hold for an order that
        no longer exists in the queue.
        """
        # Check pending orders
        before = len(self._pending_orders)
        self._pending_orders = [o for o in self._pending_orders if o.order_id != inp.order_id]
        if len(self._pending_orders) < before:
            workflow.logger.info(f"Order {inp.order_id} cancelled — removed from pending queue")
            try:
                self._current_orders.remove(inp.order_id)
            except ValueError:
                pass
            self._pending_holds.pop(inp.order_id, None)
            return
        # Check batched orders
        before = len(self._batch_orders)
        self._batch_orders = [o for o in self._batch_orders if o.order_id != inp.order_id]
        if len(self._batch_orders) < before:
            workflow.logger.info(f"Order {inp.order_id} cancelled — removed from batch")
            try:
                self._current_orders.remove(inp.order_id)
            except ValueError:
                pass
            self._pending_holds.pop(inp.order_id, None)
            return
        # Active order cancel is now handled by resolve_update("cancel")
        workflow.logger.info(f"Order {inp.order_id} not in pending/batch — handled by HITL flow")

    @workflow.signal
    async def update_order(self, inp: OrderUpdateInput) -> None:
        """Update delivery coordinates (and hotel label) for pending/batched orders."""
        for order in self._pending_orders + self._batch_orders:
            if order.order_id == inp.order_id:
                if inp.new_lat is not None and inp.new_lng is not None:
                    order.delivery_lat = inp.new_lat
                    order.delivery_lng = inp.new_lng
                if inp.new_hotel is not None:
                    order.hotel = inp.new_hotel
                workflow.logger.info(f"Order {inp.order_id} updated — new destination")
                return
        # Active order reroute is now handled by resolve_update("address_change")
        workflow.logger.info(f"Order {inp.order_id} not in pending/batch — handled by HITL flow")

    # --- Queries ---

    @workflow.query
    def get_position(self) -> dict:
        """Return current driver position. Used by parent workflow for driver snapshots."""
        return {"lat": self._current_lat, "lng": self._current_lng}

    @workflow.query
    def get_status(self) -> dict:
        return {
            "driver_id": self._driver_id,
            "lat": self._current_lat,
            "lng": self._current_lng,
            "status": self._status,
            "is_disconnected": self._is_disconnected,
            "is_recovering": self._is_recovering,
            "current_orders": list(self._current_orders),
            "active_order_id": self._active_order_id,
            "path_history": list(self._path_history),
            "pending_orders": len(self._pending_orders),
            "pending_hold_order_ids": list(self._pending_holds.keys()),
        }

    # --- Helpers ---

    async def _execute_navigate(
        self, driver_id: str, nav_input: NavigateInput, summary: str = ""
    ) -> NavigateOutput:
        """Execute navigate_to — Temporal retries on failure (including disconnect).

        The activity checks FleetState for disconnect status on each heartbeat.
        When disconnected, it fails. Temporal retries with backoff (NAV_RETRY).
        When reconnected, the next retry succeeds and navigation resumes.
        No workflow-side cancellation needed — this is the Temporal-native pattern.
        """
        return await workflow.execute_activity(
            navigate_to,
            nav_input,
            task_queue=DELIVERY_QUEUE,
            summary=summary,
            schedule_to_close_timeout=timedelta(minutes=10),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=15),
            retry_policy=NAV_RETRY,
        )

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: DriverRouteInput) -> str:
        driver_id = inp.driver_id
        self._driver_id = driver_id
        delivered: list[str] = []
        self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

        while not self._stop:
            # Wait for an order to arrive or stop signal
            try:
                await workflow.wait_condition(
                    lambda: len(self._pending_orders) > 0 or self._stop,
                    timeout=timedelta(minutes=10),
                )
            except TimeoutError:
                continue

            if self._stop:
                break

            # Brief pause to let concurrent assignments land before collecting.
            # The real batching happens naturally: orders assigned while the
            # driver is navigating to Ziggy's accumulate in _pending_orders
            # and get scooped into the batch at pickup time.
            await workflow.sleep(timedelta(seconds=2))

            # --- Position sync after reconnect ---
            if self._position_sync_needed:
                self._position_sync_needed = False
                pos = await workflow.execute_activity(
                    sync_driver_position,
                    driver_id,
                    task_queue=DELIVERY_QUEUE,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=FAST_RETRY,
                )
                self._current_lat, self._current_lng = pos[0], pos[1]
                workflow.logger.info(f"Position synced to ({pos[0]:.4f}, {pos[1]:.4f})")

            # --- Batch pickup: collect all pending orders, drive to shop once ---
            self._batch_orders = []
            while self._pending_orders:
                self._batch_orders.append(self._pending_orders.pop(0))
            if not self._batch_orders:
                continue
            order_ids_str = ", ".join(
                f"#{o.order_id.split('-', 1)[-1]}" for o in self._batch_orders
            )

            # Navigate to shop for pickup (skip if already there)
            at_shop = (
                abs(self._current_lat - WAREHOUSE.lat) < 0.001
                and abs(self._current_lng - WAREHOUSE.lng) < 0.001
            )
            if not at_shop:
                self._status = "en_route_pickup"
                pickup_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"[{order_ids_str}] Calculating route to Ziggy's",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=self._batch_orders[0].order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"[{order_ids_str}] Driving to Ziggy's",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng
                self._path_history.append(
                    {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                )

            # Scoop up any orders that arrived while driving to Ziggy's
            while self._pending_orders:
                self._batch_orders.append(self._pending_orders.pop(0))

            # All orders may have been cancelled during navigation
            if not self._batch_orders:
                await workflow.execute_activity(
                    set_driver_idle,
                    driver_id,
                    task_queue=DELIVERY_QUEUE,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=FAST_RETRY,
                )
                self._status = "idle"
                continue

            order_ids_str = ", ".join(
                f"#{o.order_id.split('-', 1)[-1]}" for o in self._batch_orders
            )

            # Batch pickup all orders at once
            self._status = "picking_up"
            await workflow.execute_activity(
                pickup_orders,
                PickupInput(
                    driver_id=driver_id,
                    order_ids=[o.order_id for o in self._batch_orders],
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"[{order_ids_str}] Loading {len(self._batch_orders)} order(s) at Ziggy's",
                schedule_to_close_timeout=timedelta(minutes=5),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NAV_RETRY,
            )
            # Position is at warehouse after pickup
            self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

            # --- Deliver each order sequentially ---
            while self._batch_orders:
                order = self._batch_orders.pop(0)
                self._active_order_id = order.order_id
                self._cancel_pending = False
                onum = order.order_id.split("-", 1)[-1]

                # Position sync after reconnect (may have happened mid-batch)
                if self._position_sync_needed:
                    self._position_sync_needed = False
                    pos = await workflow.execute_activity(
                        sync_driver_position,
                        driver_id,
                        task_queue=DELIVERY_QUEUE,
                        start_to_close_timeout=timedelta(seconds=10),
                        retry_policy=FAST_RETRY,
                    )
                    self._current_lat, self._current_lng = pos[0], pos[1]

                # Check for cancel before navigating
                if self._cancel_pending:
                    workflow.logger.info(f"Order {order.order_id} cancelled — skipping")
                    self._active_order_id = None
                    self._cancel_pending = False
                    try:
                        self._current_orders.remove(order.order_id)
                    except ValueError:
                        pass
                    continue

                # Navigate to hotel
                self._status = "en_route_delivery"
                delivery_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[
                        self._current_lat,
                        self._current_lng,
                        order.delivery_lat,
                        order.delivery_lng,
                    ],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"[#{onum}] Calculating route to {order.hotel}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        target_lat=order.delivery_lat,
                        target_lng=order.delivery_lng,
                        leg="delivery",
                        steps=30,
                        waypoints=delivery_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"[#{onum}] Driving to {order.hotel}",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng
                self._path_history.append(
                    {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                )

                # --- HITL hold: check for pending customer change ---
                # Server signals update_pending directly (same pattern as
                # disconnect). No grace period needed — signal arrives
                # before any parent processing delay.
                if order.order_id in self._pending_holds:
                    self._status = "awaiting_update"
                    workflow.logger.info(
                        f"[{order.order_id}] Holding at {order.hotel} — awaiting HITL decision"
                    )
                    # _stop escape: if the demo is shutting down while this
                    # driver is parked awaiting approval, exit cleanly instead
                    # of blocking forever. The parent's _wait_for_approval
                    # returns None on shutdown without sending resolve_update,
                    # so without this the child would hang and the parent's
                    # `await handle` join would wait on the child indefinitely.
                    await workflow.wait_condition(
                        lambda: (
                            (
                                (h := self._pending_holds.get(order.order_id)) is not None
                                and h.decision is not None
                            )
                            or self._stop
                        )
                    )
                    if self._stop:
                        break

                # Process decision if one was made FOR THIS ORDER
                hold = self._pending_holds.get(order.order_id)
                if hold is not None and hold.decision is not None:
                    decision = hold.decision
                    new_lat = hold.new_lat
                    new_lng = hold.new_lng
                    new_hotel = hold.new_hotel
                    # Pop AFTER capturing so no other coroutine can see a
                    # half-cleared hold.
                    self._pending_holds.pop(order.order_id, None)

                    if decision == "cancel":
                        workflow.logger.info(f"[{order.order_id}] HITL cancel approved")
                        self._cancel_pending = True
                    elif decision == "address_change":
                        # Reroute: update destination and re-navigate
                        if new_lat is not None and new_lng is not None:
                            # If the order was still in pending/batch when the
                            # change was approved, update_order already applied
                            # the new coords — the batch loop navigated to the
                            # new destination on its first trip. Skip the
                            # otherwise-redundant re-navigation here.
                            already_at_new_destination = (
                                abs(order.delivery_lat - new_lat) < 0.0001
                                and abs(order.delivery_lng - new_lng) < 0.0001
                            )
                            order.delivery_lat = new_lat
                            order.delivery_lng = new_lng
                            if new_hotel is not None:
                                order.hotel = new_hotel
                            if already_at_new_destination:
                                workflow.logger.info(
                                    f"[{order.order_id}] HITL reroute approved — "
                                    f"coords already applied, skipping redundant nav"
                                )
                            else:
                                workflow.logger.info(
                                    f"[{order.order_id}] HITL reroute approved — "
                                    f"navigating to new destination"
                                )
                                # Navigate to new destination
                                reroute_waypoints = await workflow.execute_activity(
                                    get_route_polyline,
                                    args=[
                                        self._current_lat,
                                        self._current_lng,
                                        order.delivery_lat,
                                        order.delivery_lng,
                                    ],
                                    task_queue=DELIVERY_QUEUE,
                                    summary=f"[#{onum}] Rerouting to new destination",
                                    schedule_to_close_timeout=timedelta(minutes=5),
                                    start_to_close_timeout=timedelta(seconds=30),
                                    retry_policy=FAST_RETRY,
                                )
                                nav_result = await self._execute_navigate(
                                    driver_id,
                                    NavigateInput(
                                        driver_id=driver_id,
                                        order_id=order.order_id,
                                        target_lat=order.delivery_lat,
                                        target_lng=order.delivery_lng,
                                        leg="delivery",
                                        steps=30,
                                        waypoints=reroute_waypoints,
                                        start_lat=self._current_lat,
                                        start_lng=self._current_lng,
                                    ),
                                    summary=f"[#{onum}] Rerouting to new destination",
                                )
                                self._current_lat = nav_result.final_lat
                                self._current_lng = nav_result.final_lng
                    else:
                        workflow.logger.info(
                            f"[{order.order_id}] HITL change rejected — delivering normally"
                        )

                # Deliver (skip if cancelled)
                if not self._cancel_pending:
                    self._status = "delivering"
                    deliver_result = await workflow.execute_activity(
                        deliver_order,
                        DeliverInput(
                            driver_id=driver_id,
                            order_id=order.order_id,
                        ),
                        task_queue=DELIVERY_QUEUE,
                        summary=f"[#{onum}] Order delivered to {order.hotel}",
                        schedule_to_close_timeout=timedelta(minutes=5),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=NAV_RETRY,
                    )

                    if deliver_result.success:
                        delivered.append(order.order_id)

                        # Signal parent — guarded so a terminated parent (e.g.
                        # during demo reset) doesn't raise and fail this child
                        # mid-delivery. info().parent.workflow_id avoids
                        # coupling to a hardcoded id.
                        try:
                            parent_info = workflow.info().parent
                            if parent_info is not None:
                                parent = workflow.get_external_workflow_handle(
                                    parent_info.workflow_id
                                )
                                await parent.signal(
                                    "order_delivered",
                                    OrderDeliveredInput(
                                        driver_id=driver_id,
                                        order_id=order.order_id,
                                        delivery_lat=order.delivery_lat,
                                        delivery_lng=order.delivery_lng,
                                    ),
                                )
                        except Exception as e:
                            workflow.logger.warning(
                                f"Could not signal parent with order_delivered for "
                                f"{order.order_id}: {e}"
                            )
                    else:
                        # deliver_order returned success=False, meaning a
                        # cancel beat us to it. Don't tell the parent we
                        # delivered — the order is CANCELLED, not DELIVERED.
                        workflow.logger.info(
                            f"[{order.order_id}] Delivery did not commit "
                            f"(cancelled before/during activity) — skipping parent signal"
                        )
                else:
                    workflow.logger.info(f"[{order.order_id}] Delivery skipped — cancelled")

                self._active_order_id = None
                self._cancel_pending = False
                # Clean up any hold tied to this order. Usually the hold was
                # already popped when its decision was processed inside the
                # HITL block above — but there's a race where update_pending
                # arrives AFTER the HITL gate evaluated False (no hold yet)
                # but BEFORE deliver_order completed. That creates a late
                # PendingHold(decision=None) that nothing else clears,
                # leaking the order_id into pending_hold_order_ids for the
                # lifetime of the workflow. Popping here closes the race.
                self._pending_holds.pop(order.order_id, None)

                # Remove from current_orders tracking
                try:
                    self._current_orders.remove(order.order_id)
                except ValueError:
                    pass

            # All orders in batch delivered — drive back to base if not already there
            at_warehouse = (
                abs(self._current_lat - WAREHOUSE.lat) < 0.001
                and abs(self._current_lng - WAREHOUSE.lng) < 0.001
            )
            workflow.logger.info(
                f"Batch done. pos=({self._current_lat:.4f}, {self._current_lng:.4f}), "
                f"at_warehouse={at_warehouse}"
            )
            if not at_warehouse:
                return_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary="Calculating return to Ziggy's",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id="return",
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=return_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary="Returning to Ziggy's",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

            self._status = "idle"
            self._path_history.clear()
            # Update FleetState so the UI shows idle + clear trail
            await workflow.execute_activity(
                set_driver_idle,
                driver_id,
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

        return f"Driver {driver_id} completed {len(delivered)} deliveries: {delivered}"


# --- Order generation child workflow ---


@workflow.defn
class OrderGenerationWorkflow:
    """Generates orders on a timer and signals parent with each new order.

    The parent workflow owns driver state and handles assignment.
    This workflow is purely a timer + order generator.
    """

    def __init__(self) -> None:
        self._stop = False

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.query
    def get_status(self) -> dict:
        return {"stop": self._stop}

    @workflow.run
    async def run(self, inp: OrderGenerationInput) -> str:
        for order_num in range(1, inp.max_orders + 1):
            if self._stop:
                break

            # Generate a new order
            order = await workflow.execute_activity(
                generate_order,
                GenerateOrderInput(order_number=order_num),
                task_queue=DELIVERY_QUEUE,
                summary=f"[#{order_num}] Generate order",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Signal parent with the new order — guarded (same rationale as
            # DriverRouteWorkflow.order_delivered): a terminated parent during
            # demo reset must not fail this child.
            try:
                parent_info = workflow.info().parent
                if parent_info is not None:
                    parent = workflow.get_external_workflow_handle(parent_info.workflow_id)
                    await parent.signal(
                        "new_order",
                        OrderAssignmentResult(
                            order_id=order.order_id,
                            hotel=order.hotel,
                            delivery_lat=order.delivery_lat,
                            delivery_lng=order.delivery_lng,
                            driver_id="",  # Not yet assigned — parent handles assignment
                            reasoning_summary="",
                            priority=order.priority,
                            servings=order.servings,
                            deadline_minutes=order.deadline_minutes,
                            event=order.event,
                        ),
                    )
            except Exception as e:
                workflow.logger.warning(
                    f"Could not signal parent with new order {order.order_id}: {e}"
                )

            workflow.logger.info(
                f"Order {order_num}/{inp.max_orders}: {order.order_id} signaled to parent"
            )

            # Wait before next order — initial burst to fill driver batches, then normal cadence
            if order_num < inp.max_orders:
                if order_num <= 8:
                    # Fast burst: 8 orders in ~10s to get multi-order batches on drivers
                    await workflow.sleep(timedelta(seconds=1))
                else:
                    base = inp.order_interval_seconds
                    jitter = int(workflow.random().random() * base * 0.6)  # 0–60% jitter
                    interval = base + jitter - int(base * 0.3)  # center around base
                    await workflow.sleep(timedelta(seconds=max(5, interval)))

        return f"Order generation complete — {inp.max_orders} orders generated"


# --- Main demo orchestrator ---


@workflow.defn
class MeltdownDemoWorkflow:
    """
    Orchestrates the Meltdown demo with continuous order flow.

    Starts 3 driver child workflows, generates orders on a timer,
    runs multi-agent reasoning per order, and signals the chosen driver.
    Handles customer-change signals concurrently.

    Owns driver state: positions, order assignments, disconnect status.
    Activities receive this state as inputs — they never read FleetState
    for decision-making.
    """

    def __init__(self) -> None:
        self._pending_changes: list[CustomerChangeInput] = []
        self._pending_approvals: list[bool] = []
        self._pending_new_orders: list[OrderAssignmentResult] = []
        self._routes_done: bool = False
        self._use_mock_assignment: bool = False
        self._disconnected_drivers: set[str] = set()
        self._disconnected_agents: set[str] = set()
        # Workflow-owned driver state
        self._driver_orders: dict[str, list[str]] = {}
        self._driver_last_position: dict[str, tuple[float, float]] = {}
        self._orders_generated: int = 0
        self._route_handles: dict = {}
        self._order_gen_handle: workflow.ChildWorkflowHandle | None = None
        # Order tracking for queries
        self._orders: dict[str, dict] = {}
        self._agent_health: dict[str, bool] = {
            "fleet_agent": True,
            "customer_agent": True,
            "resolver": True,
        }

    # --- Signals ---

    @workflow.signal
    async def customer_change(self, change: CustomerChangeInput) -> None:
        self._pending_changes.append(change)

    @workflow.signal
    async def change_approved(self, approved: bool) -> None:
        self._pending_approvals.append(approved)

    @workflow.signal
    async def driver_disconnected(self, inp: DriverDisconnectInput) -> None:
        self._disconnected_drivers.add(inp.driver_id)
        workflow.logger.info(f"Driver {inp.driver_id} disconnected — activities will retry")

    @workflow.signal
    async def driver_reconnected(self, inp: DriverDisconnectInput) -> None:
        self._disconnected_drivers.discard(inp.driver_id)
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def agent_disconnected(self, inp: AgentDisconnectInput) -> None:
        self._disconnected_agents.add(inp.agent_name)
        self._agent_health[inp.agent_name] = False
        workflow.logger.info(f"Agent {inp.agent_name} disconnected")

    @workflow.signal
    async def agent_reconnected(self, inp: AgentDisconnectInput) -> None:
        self._disconnected_agents.discard(inp.agent_name)
        self._agent_health[inp.agent_name] = True
        workflow.logger.info(f"Agent {inp.agent_name} reconnected")

    @workflow.signal
    async def order_delivered(self, inp: OrderDeliveredInput) -> None:
        """Signaled by DriverRouteWorkflow when a delivery completes."""
        driver_id = inp.driver_id
        before = len(self._driver_orders.get(driver_id, []))
        if driver_id in self._driver_orders:
            if inp.order_id in self._driver_orders[driver_id]:
                self._driver_orders[driver_id].remove(inp.order_id)
        after = len(self._driver_orders.get(driver_id, []))
        self._driver_last_position[driver_id] = (inp.delivery_lat, inp.delivery_lng)
        if inp.order_id in self._orders:
            self._orders[inp.order_id]["status"] = "delivered"
        workflow.logger.info(
            f"Order {inp.order_id} delivered by {driver_id} (orders: {before} → {after})"
        )

    @workflow.signal
    async def new_order(self, order: OrderAssignmentResult) -> None:
        """Signaled by OrderGenerationWorkflow with each new order to assign."""
        self._pending_new_orders.append(order)

    # --- Queries ---

    @workflow.query
    def get_status(self) -> dict:
        return {
            "routes_done": self._routes_done,
            "orders_generated": self._orders_generated,
            "pending_changes": len(self._pending_changes),
            "disconnected_drivers": list(self._disconnected_drivers),
            "disconnected_agents": list(self._disconnected_agents),
            "driver_orders": {cid: list(oids) for cid, oids in self._driver_orders.items()},
            "orders": dict(self._orders),
            "agent_health": dict(self._agent_health),
        }

    # --- Helpers ---

    # First N orders use only 3 drivers so A-C warm up with single
    # deliveries while D-E accumulate batched orders naturally.
    _WARMUP_ORDERS = 5

    def _build_driver_snapshots(self) -> list[DriverSnapshot]:
        """Build driver snapshots from workflow state for activity inputs."""
        snapshots = []
        warming_up = self._orders_generated <= self._WARMUP_ORDERS
        for driver_id in DRIVER_IDS:
            # During warmup, hide drivers D-E so agents only assign to A-C
            if warming_up and driver_id in ("driver-d", "driver-e"):
                continue
            pos = self._driver_last_position.get(driver_id, (WAREHOUSE.lat, WAREHOUSE.lng))
            order_count = len(self._driver_orders.get(driver_id, []))
            snapshots.append(
                DriverSnapshot(
                    driver_id=driver_id,
                    lat=pos[0],
                    lng=pos[1],
                    status="disconnected" if driver_id in self._disconnected_drivers else "active",
                    capacity=DRIVER_CAPACITY,
                    current_order_count=order_count,
                    is_disconnected=driver_id in self._disconnected_drivers,
                )
            )
        return snapshots

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: MeltdownDemoInput) -> str:
        workflow.logger.info(f"Meltdown demo starting (escalation={inp.escalation_enabled})")
        self._use_mock_assignment = inp.use_mock_assignment

        # Initialize driver state
        for driver_id in DRIVER_IDS:
            self._driver_orders[driver_id] = []
            self._driver_last_position[driver_id] = (WAREHOUSE.lat, WAREHOUSE.lng)

        # Hide drivers D-E in FleetState during warmup so
        # tool_get_fleet_status only shows A-C to the LLM
        await workflow.execute_activity(
            set_warmup_hidden,
            args=[["driver-d", "driver-e"], True],
            task_queue=DELIVERY_QUEUE,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=FAST_RETRY,
        )

        # Start 5 driver child workflows
        self._route_handles = {}
        for driver_id in DRIVER_IDS:
            handle = await workflow.start_child_workflow(
                DriverRouteWorkflow.run,
                DriverRouteInput(driver_id=driver_id),
                id=f"route-{driver_id}",
                static_summary=f"{driver_id} — delivery loop",
            )
            self._route_handles[driver_id] = handle

        # Start order generation as a child workflow
        self._order_gen_handle = await workflow.start_child_workflow(
            OrderGenerationWorkflow.run,
            OrderGenerationInput(
                max_orders=inp.max_orders,
                order_interval_seconds=ORDER_INTERVAL_SECONDS,
            ),
            id="order-generation",
            static_summary="Order generation + agent assignment",
        )

        # Process new orders and customer changes concurrently
        order_task = asyncio.create_task(self._process_new_orders())
        change_task = asyncio.create_task(self._process_customer_changes())

        # Wait for order generation to complete
        await self._order_gen_handle

        # Drain any remaining orders that arrived via signal before stopping
        while self._pending_new_orders:
            order = self._pending_new_orders.pop(0)
            await self._assign_order(order)

        # Stop all drivers and concurrent loops
        self._routes_done = True
        for handle in self._route_handles.values():
            try:
                await handle.signal(DriverRouteWorkflow.stop)
            except Exception:
                pass

        await change_task
        await order_task

        # Wait for drivers to finish current deliveries
        results = []
        for driver_id, handle in self._route_handles.items():
            try:
                result = await handle
                results.append(result)
            except Exception as e:
                results.append(f"{driver_id}: {e}")

        return f"Meltdown demo complete. Results: {results}"

    # --- ADK inline assignment (live mode) ---

    async def _run_adk_assignment(
        self, inp: ReasonAboutAssignmentInput
    ) -> ReasonAboutAssignmentOutput:
        """Run ADK agents inline in the workflow.

        Each LLM call and tool call is a separate Temporal activity via
        TemporalModel + activity_tool. This gives per-call durability and
        visibility in the Temporal UI. Failures propagate — Temporal retries.
        """
        workflow.logger.info(f"Running ADK assignment for {inp.order_id}")
        agent = create_order_assignment_agent()

        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent,
            app_name="meltdown_demo",
            session_service=session_service,
        )

        session = await session_service.create_session(
            app_name="meltdown_demo",
            user_id="workflow",
        )

        # Build agent status context
        agent_status_lines = []
        if inp.disconnected_agents:
            for agent in inp.disconnected_agents:
                agent_status_lines.append(f"⚠️ {agent} is OFFLINE — compensate with available data.")
        agent_context = "\n".join(agent_status_lines) + "\n\n" if agent_status_lines else ""

        prompt = (
            f"{agent_context}"
            f"NEW ORDER — assign to the best driver:\n"
            f"Order ID: {inp.order_id}\n"
            f"Hotel: {inp.hotel}\n"
            f"Event: {inp.event}\n"
            f"Priority: {inp.priority}\n"
            f"Servings: {inp.servings}\n"
            f"Deadline: {inp.deadline_minutes} minutes\n"
            f"Coordinates: ({inp.delivery_lat}, {inp.delivery_lng})\n\n"
            f"Assess fleet capacity and customer priority, then the resolver "
            f"MUST call tool_submit_assignment with the driver_id and reasoning."
        )

        events_count = 0
        async for event in runner.run_async(
            user_id="workflow",
            session_id=session.id,
            new_message=Content(parts=[Part(text=prompt)]),
        ):
            events_count += 1

        updated_session = await session_service.get_session(
            app_name="meltdown_demo",
            user_id="workflow",
            session_id=session.id,
        )

        state = updated_session.state or {}
        assignment_dict = state.get("assignment")
        if not assignment_dict:
            workflow.logger.warning(
                "ADK did not call tool_submit_assignment — LLM instruction failure"
            )
            assignment_dict = {"driver_id": "", "reasoning_summary": ""}

        driver_id = assignment_dict["driver_id"]
        reasoning = assignment_dict.get("reasoning_summary", "ADK assignment")

        # Extract agent outputs for UI summary events
        fleet_output = state.get("fleet_assessment", "")
        customer_output = state.get("customer_assessment", "")

        # Build short summary events — first sentence only
        def _first_sentence(text: str) -> str:
            text = text.strip()
            for sep in [".", "\n"]:
                idx = text.find(sep)
                if idx > 0:
                    return text[: idx + 1].strip()
            return text[:80]

        events = []
        if fleet_output:
            events.append(
                {
                    "agent_name": "fleet_agent",
                    "event_type": "assessment",
                    "content": fleet_output,
                    "summary": _first_sentence(fleet_output),
                }
            )
        if customer_output:
            events.append(
                {
                    "agent_name": "customer_agent",
                    "event_type": "assessment",
                    "content": customer_output,
                    "summary": _first_sentence(customer_output),
                }
            )
        # Extract order number from order_id (e.g. "order-3" → "3")
        order_number = inp.order_id.split("-", 1)[-1] if "-" in inp.order_id else inp.order_id
        hotel = inp.hotel

        events.append(
            {
                "agent_name": "resolver",
                "event_type": "plan",
                "content": f"Order #{order_number} → {driver_id} — {hotel} — {reasoning}",
                "summary": f"Order #{order_number} → {driver_id} — {hotel}",
            }
        )

        # If Fleet Agent was disconnected, publish a Dispatch Agent note about the gap
        if "fleet_agent" in inp.disconnected_agents:
            events.append(
                {
                    "agent_name": "resolver",
                    "event_type": "assessment",
                    "content": "Fleet Agent offline — assigned with customer data only",
                    "summary": "Fleet Agent offline — customer data only",
                }
            )

        workflow.logger.info(f"ADK assignment complete: {events_count} events, driver={driver_id}")
        return ReasonAboutAssignmentOutput(
            driver_id=driver_id,
            reasoning_summary=reasoning,
            agent_events=events,
        )

    # --- New order processing (triggered by signals from OrderGenerationWorkflow) ---

    async def _process_new_orders(self) -> None:
        """Process new orders as they arrive via signal from OrderGenerationWorkflow."""
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: len(self._pending_new_orders) > 0 or self._routes_done,
            )

            if self._routes_done:
                break

            while self._pending_new_orders:
                order = self._pending_new_orders.pop(0)
                await self._assign_order(order)

    async def _assign_order(self, order: OrderAssignmentResult) -> None:
        """Run ADK assignment for a new order and signal the chosen driver."""
        onum = order.order_id.split("-", 1)[-1]
        self._orders_generated += 1

        # End warmup: unhide drivers D-E when warmup orders are done
        if self._orders_generated == self._WARMUP_ORDERS + 1:
            await workflow.execute_activity(
                set_warmup_hidden,
                args=[["driver-d", "driver-e"], False],
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
            workflow.logger.info("Warmup complete — drivers D-E now visible")

        # Track order in workflow state
        self._orders[order.order_id] = {
            "order_id": order.order_id,
            "hotel": order.hotel,
            "priority": order.priority,
            "servings": order.servings,
            "delivery_lat": order.delivery_lat,
            "delivery_lng": order.delivery_lng,
            "assigned_driver_id": None,
            "status": "pending",
            "deadline_minutes": order.deadline_minutes,
        }

        # Build driver snapshots from workflow state — passed to activity as input
        driver_snapshots = self._build_driver_snapshots()

        assignment_input = ReasonAboutAssignmentInput(
            order_id=order.order_id,
            hotel=order.hotel,
            delivery_lat=order.delivery_lat,
            delivery_lng=order.delivery_lng,
            priority=order.priority,
            servings=order.servings,
            deadline_minutes=order.deadline_minutes,
            event=order.event,
            driver_snapshots=driver_snapshots,
            disconnected_agents=list(self._disconnected_agents),
        )

        if self._use_mock_assignment:
            assignment = await workflow.execute_activity(
                "reason_about_assignment",
                assignment_input,
                task_queue=AGENTS_QUEUE,
                summary=f"[#{onum}] Dispatch Agent — assign {order.order_id}",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )
        else:
            assignment = await self._run_adk_assignment(assignment_input)
        # Determine if this is a degraded assignment (Fleet Agent offline)
        fleet_offline = "fleet_agent" in self._disconnected_agents

        # Publish summary events to FleetState — single batched local activity
        if assignment.agent_events:
            await workflow.execute_local_activity(
                publish_agent_events_batch,
                [
                    PublishAgentEventInput(
                        agent_name=evt["agent_name"],
                        event_type=evt["event_type"],
                        content=evt["content"],
                        summary=evt.get("summary", ""),
                    )
                    for evt in assignment.agent_events
                ],
                start_to_close_timeout=timedelta(seconds=10),
            )

        # Validate and reassign if chosen driver is invalid, full, or disconnected
        driver_id = assignment.driver_id
        warming_up = self._orders_generated <= self._WARMUP_ORDERS
        needs_reassign = (
            driver_id not in self._route_handles
            or driver_id in self._disconnected_drivers
            or len(self._driver_orders.get(driver_id, [])) >= DRIVER_CAPACITY
            or (warming_up and driver_id in ("driver-d", "driver-e"))
        )

        if needs_reassign:
            original = driver_id
            reassigned = False
            for fallback_id in DRIVER_IDS:
                if fallback_id == original:
                    continue
                if warming_up and fallback_id in ("driver-d", "driver-e"):
                    continue
                if fallback_id in self._disconnected_drivers:
                    continue
                if fallback_id not in self._route_handles:
                    continue
                if len(self._driver_orders.get(fallback_id, [])) < DRIVER_CAPACITY:
                    driver_id = fallback_id
                    reassigned = True
                    reason = (
                        "invalid"
                        if original not in self._route_handles
                        else (
                            "disconnected"
                            if original in self._disconnected_drivers
                            else "at capacity"
                        )
                    )
                    workflow.logger.warning(
                        f"Reassigning {order.order_id}: {original} is {reason} → {driver_id}"
                    )
                    break

            if not reassigned:
                # All drivers unavailable — keep original driver_id so the
                # order is queued on its route handle (delivered on reconnect)
                driver_id = original if original in self._route_handles else DRIVER_IDS[0]
                workflow.logger.warning(
                    f"No available drivers — queuing {order.order_id} on {driver_id}"
                )

        # Register final assignment AFTER capacity check
        assigned = await workflow.execute_activity(
            register_assignment,
            args=[driver_id, order.order_id, fleet_offline],
            task_queue=AGENTS_QUEUE,
            summary=f"[#{onum}] Dispatch Agent — {order.order_id} → {driver_id}"
            + (" (degraded)" if fleet_offline else ""),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        # Skip workflow state + child signal if DB write was a no-op
        # (order already cancelled/delivered/assigned)
        if not assigned:
            workflow.logger.info(f"{order.order_id} assignment skipped — already terminal")
            return

        # Update workflow-owned driver state (skip if already assigned —
        # prevents duplicate signals when fallback re-assigns to same driver)
        already_assigned = (
            driver_id in self._driver_orders and order.order_id in self._driver_orders[driver_id]
        )
        if driver_id in self._driver_orders and not already_assigned:
            self._driver_orders[driver_id].append(order.order_id)

        # Update order tracking
        if order.order_id in self._orders:
            self._orders[order.order_id]["assigned_driver_id"] = driver_id
            self._orders[order.order_id]["status"] = "assigned"

        # Signal the chosen driver (skip if already assigned)
        if already_assigned:
            workflow.logger.info(f"{order.order_id} already on {driver_id} — skipping signal")
        elif driver_id in self._route_handles:
            await self._route_handles[driver_id].signal(
                DriverRouteWorkflow.add_order,
                DriverRouteOrder(
                    order_id=order.order_id,
                    hotel=order.hotel,
                    delivery_lat=order.delivery_lat,
                    delivery_lng=order.delivery_lng,
                ),
            )

        workflow.logger.info(f"Order {self._orders_generated}: {order.order_id} → {driver_id}")

    # --- Signal processing loop ---

    def _has_pending_signal(self) -> bool:
        return len(self._pending_changes) > 0

    async def _process_customer_changes(self) -> None:
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: self._has_pending_signal() or self._routes_done,
            )

            if self._routes_done:
                break

            await self._drain_pending_signals()

    async def _drain_pending_signals(self) -> None:
        # Serial processing. With the child's HITL state now keyed per-order
        # (self._pending_holds), concurrent drain would be safe from the
        # overwrite bug that motivated the original serial revert. Keeping
        # serial anyway because it's simpler and matches the demo flow
        # (changes submitted one at a time); the only cost is that driver B
        # stays parked at its HITL hold while driver A's change awaits human
        # approval, which is a minor UX delay in practice.
        while self._pending_changes:
            change = self._pending_changes.pop(0)
            await self._process_customer_change(change)

    async def _wait_for_approval(self) -> bool | None:
        """Pull the next approval off _pending_approvals, or None if the demo
        shuts down while waiting.

        _drain_pending_signals processes serially, so only one caller is ever
        parked here at a time — the loop exists solely to let _routes_done
        (demo shutdown) unblock a parked change without requiring an approval.
        Returns None in that case so the caller can exit cleanly.
        """
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: len(self._pending_approvals) > 0 or self._routes_done,
            )
            if self._routes_done:
                return None
            if self._pending_approvals:
                return self._pending_approvals.pop(0)
        return None

    # --- Customer change handling ---

    async def _process_customer_change(self, change: CustomerChangeInput) -> None:
        cnum = change.order_id.split("-", 1)[-1]

        # Signal child FIRST to hold delivery — before any local activities
        # that could delay the signal past the child's grace period
        driver_id_before_wait = self._find_driver_for_order(change.order_id)
        driver_id = driver_id_before_wait
        if driver_id and driver_id in self._route_handles:
            await self._route_handles[driver_id].signal(
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                ),
            )

        await workflow.execute_local_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=(
                    f"Customer change request for {change.order_id}: "
                    f"{change.change_type} — {change.new_details}"
                ),
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Wait for human approval — workflow pauses here, order generation
        # and other activities keep running. _wait_for_approval returns None
        # if the demo shuts down (routes_done) while we're parked, so a
        # pending change can't block the parent's teardown on `await handle`.
        approved = await self._wait_for_approval()
        if approved is None:
            return

        # Re-resolve driver_id — order may have been assigned during the wait
        driver_id = self._find_driver_for_order(change.order_id)

        if approved:
            await workflow.execute_activity(
                execute_customer_change,
                ExecuteCustomerChangeInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                    new_lat=change.new_lat,
                    new_lng=change.new_lng,
                    new_hotel=change.new_hotel,
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"[#{cnum}] Dispatch Agent — execute change",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Signal child with the approved decision.
            # Send update_pending again ONLY if the driver changed during the
            # approval wait (order was unassigned at submission and got
            # assigned while the human was deciding). With per-order holds
            # this send is always safe — it creates a new hold entry for
            # this specific order on the new driver without touching any
            # other order's hold state.
            if driver_id and driver_id in self._route_handles:
                if driver_id != driver_id_before_wait:
                    await self._route_handles[driver_id].signal(
                        DriverRouteWorkflow.update_pending,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                        ),
                    )
                # For pending/batched orders: also send cancel_order or
                # update_order so the order is immediately removed/updated
                # without a wasted navigation trip
                if change.change_type == "cancel":
                    await self._route_handles[driver_id].signal(
                        DriverRouteWorkflow.cancel_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                        ),
                    )
                elif change.change_type == "address_change":
                    await self._route_handles[driver_id].signal(
                        DriverRouteWorkflow.update_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                            new_lat=change.new_lat,
                            new_lng=change.new_lng,
                            new_hotel=change.new_hotel,
                        ),
                    )
                # Resolve the HITL hold for active orders
                await self._route_handles[driver_id].signal(
                    DriverRouteWorkflow.resolve_update,
                    OrderUpdateInput(
                        order_id=change.order_id,
                        change_type=change.change_type,
                        new_lat=change.new_lat,
                        new_lng=change.new_lng,
                        new_hotel=change.new_hotel,
                    ),
                )
                if change.change_type == "cancel":
                    try:
                        self._driver_orders[driver_id].remove(change.order_id)
                    except (KeyError, ValueError):
                        pass
            await workflow.execute_local_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_executed",
                    content=(
                        f"Customer change approved and executed for "
                        f"{change.order_id}: {change.new_details}"
                    ),
                ),
                start_to_close_timeout=timedelta(seconds=10),
            )
        else:
            # Rejected — signal child to release (deliver normally)
            if driver_id and driver_id in self._route_handles:
                await self._route_handles[driver_id].signal(
                    DriverRouteWorkflow.resolve_update,
                    OrderUpdateInput(
                        order_id=change.order_id,
                        change_type="release",
                    ),
                )
            await workflow.execute_local_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_rejected",
                    content=f"Customer change rejected for {change.order_id}",
                ),
                start_to_close_timeout=timedelta(seconds=10),
            )

    def _find_driver_for_order(self, order_id: str) -> str | None:
        """Find which driver has a given order."""
        for driver_id, orders in self._driver_orders.items():
            if order_id in orders:
                return driver_id
        return None
