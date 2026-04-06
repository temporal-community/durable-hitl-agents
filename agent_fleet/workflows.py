"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts 3 driver child workflows,
generates orders on a timer, runs multi-agent reasoning per order, and
signals the chosen driver. Handles customer-change signals concurrently.

DriverRouteWorkflow — per-driver continuous delivery loop (child workflow).
Waits for orders via signal, picks up at the shop, delivers, repeats.
Uses cancellation scopes for workflow-driven disconnect handling.
"""

from __future__ import annotations

import asyncio
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
        reason_about_assignment,
        register_assignment,
        sync_driver_disconnect,
        sync_driver_recovery_complete,
    )
    from agent_fleet.agents import create_order_assignment_agent
    from agent_fleet.config import MOCK_MODE as _MOCK_MODE
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
        OrderDeliveredInput,
        OrderUpdateInput,
        PickupInput,
        PublishAgentEventInput,
        ReasonAboutAssignmentInput,
        ReasonAboutAssignmentOutput,
        SyncDriverDisconnectInput,
    )
    from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE

FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
NAV_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)

MAX_ORDERS = 20
ORDER_INTERVAL_SECONDS = 15

PARENT_WORKFLOW_ID = "meltdown-demo"
DRIVER_CAPACITY = 3


# --- Per-driver continuous delivery workflow ---


@workflow.defn
class DriverRouteWorkflow:
    """
    Continuous delivery loop for a single AI-Driver.

    Waits for orders via signal, picks up at the shop, delivers to hotel,
    then returns to waiting. Loops until told to stop.

    Disconnect handling uses two Temporal-idiomatic mechanisms:
    - is_driver_disconnected input flag: catches "already disconnected at activity start"
    - Activity handle cancellation: catches "disconnect signal arrives mid-activity" —
      signal handler calls handle.cancel(), activity receives CancelledError on its
      next heartbeat() call.
    """

    def __init__(self) -> None:
        self._pending_orders: list[DriverRouteOrder] = []
        self._stop = False
        self._is_disconnected: bool = False
        self._active_nav_handle: workflow.ActivityHandle | None = None
        self._current_lat: float = 0.0
        self._current_lng: float = 0.0

    # --- Signals ---

    @workflow.signal
    async def add_order(self, order: DriverRouteOrder) -> None:
        self._pending_orders.append(order)

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.signal
    async def driver_disconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = True
        if self._active_nav_handle:
            self._active_nav_handle.cancel()
        workflow.logger.info(f"Driver {inp.driver_id} disconnected — cancelling active activity")

    @workflow.signal
    async def driver_reconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = False
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def update_order(self, inp: OrderUpdateInput) -> None:
        """Update delivery coordinates for a pending order."""
        for order in self._pending_orders:
            if order.order_id == inp.order_id:
                if inp.new_lat is not None and inp.new_lng is not None:
                    order.delivery_lat = inp.new_lat
                    order.delivery_lng = inp.new_lng
                workflow.logger.info(f"Order {inp.order_id} updated — new destination")
                return
        workflow.logger.info(f"Order {inp.order_id} not in pending — may already be in delivery")

    @workflow.signal
    async def cancel_order(self, inp: OrderUpdateInput) -> None:
        """Remove an order from the pending queue."""
        self._pending_orders = [o for o in self._pending_orders if o.order_id != inp.order_id]
        workflow.logger.info(f"Order {inp.order_id} cancelled — removed from queue")

    # --- Queries ---

    @workflow.query
    def get_position(self) -> dict:
        """Return current driver position. Used by parent workflow for driver snapshots."""
        return {"lat": self._current_lat, "lng": self._current_lng}

    @workflow.query
    def get_status(self) -> dict:
        return {
            "lat": self._current_lat,
            "lng": self._current_lng,
            "is_disconnected": self._is_disconnected,
            "pending_orders": len(self._pending_orders),
        }

    # --- Helpers ---

    async def _sync_disconnect_to_ui(self, driver_id: str, disconnected: bool) -> None:
        """Push disconnect/reconnect state to FleetState for the frontend."""
        state = "disconnected" if disconnected else "reconnected"
        await workflow.execute_activity(
            sync_driver_disconnect,
            SyncDriverDisconnectInput(driver_id=driver_id, disconnected=disconnected),
            task_queue=DELIVERY_QUEUE,
            summary=f"{driver_id} — {state}",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=FAST_RETRY,
        )
        if not disconnected:
            # Clear recovery visual after a short delay
            await workflow.sleep(timedelta(seconds=3))
            await workflow.execute_activity(
                sync_driver_recovery_complete,
                driver_id,
                task_queue=DELIVERY_QUEUE,
                summary=f"{driver_id} — recovery complete",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

    async def _await_reconnect(self, driver_id: str) -> None:
        """Block until driver is reconnected, syncing state to FleetState via activities."""
        if self._is_disconnected:
            # Sync disconnect to frontend
            await self._sync_disconnect_to_ui(driver_id, disconnected=True)
            workflow.logger.info(f"{driver_id} disconnected — waiting for reconnect")
            await workflow.wait_condition(lambda: not self._is_disconnected)
            # Sync reconnect to frontend
            await self._sync_disconnect_to_ui(driver_id, disconnected=False)
            workflow.logger.info(f"{driver_id} reconnected — resuming delivery")

    async def _navigate_with_disconnect_guard(
        self, driver_id: str, nav_input: NavigateInput
    ) -> NavigateOutput:
        """Execute navigate_to with activity cancellation for mid-flight disconnect.

        If a driver_disconnected signal arrives during navigation:
        1. Signal handler calls handle.cancel() on the running activity
        2. Activity receives CancelledError on its next heartbeat() call
        3. Workflow catches it, waits for reconnect, retries navigation
        """
        while True:
            await self._await_reconnect(driver_id)
            # Rebuild input with current disconnect state (False — we just confirmed connected)
            nav_input = NavigateInput(
                driver_id=nav_input.driver_id,
                order_id=nav_input.order_id,
                target_lat=nav_input.target_lat,
                target_lng=nav_input.target_lng,
                leg=nav_input.leg,
                steps=nav_input.steps,
                waypoints=nav_input.waypoints,
                is_driver_disconnected=False,
                start_lat=self._current_lat,
                start_lng=self._current_lng,
            )
            try:
                self._active_nav_handle = workflow.start_activity(
                    navigate_to,
                    nav_input,
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=10),
                    start_to_close_timeout=timedelta(seconds=120),
                    heartbeat_timeout=timedelta(seconds=15),
                    retry_policy=NAV_RETRY,
                )
                return await self._active_nav_handle
            except asyncio.CancelledError:
                if self._is_disconnected:
                    workflow.logger.info(
                        f"{driver_id} disconnected mid-navigation — will retry on reconnect"
                    )
                    continue
                raise
            finally:
                self._active_nav_handle = None

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: DriverRouteInput) -> str:
        driver_id = inp.driver_id
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

            # Process all pending orders
            while self._pending_orders:
                order = self._pending_orders.pop(0)

                # Navigate to shop for pickup
                pickup_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — route to shop for {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._navigate_with_disconnect_guard(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                        is_driver_disconnected=self._is_disconnected,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

                # Pick up
                await self._await_reconnect(driver_id)
                await workflow.execute_activity(
                    pickup_orders,
                    PickupInput(
                        driver_id=driver_id,
                        order_ids=[order.order_id],
                        is_driver_disconnected=self._is_disconnected,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — picking up {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

                # Navigate to hotel
                delivery_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[
                        self._current_lat,
                        self._current_lng,
                        order.delivery_lat,
                        order.delivery_lng,
                    ],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — route to {order.hotel} for {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._navigate_with_disconnect_guard(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        target_lat=order.delivery_lat,
                        target_lng=order.delivery_lng,
                        leg="delivery",
                        steps=30,
                        waypoints=delivery_waypoints,
                        is_driver_disconnected=self._is_disconnected,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

                # Deliver
                await self._await_reconnect(driver_id)
                await workflow.execute_activity(
                    deliver_order,
                    DeliverInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        is_driver_disconnected=self._is_disconnected,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — delivered {order.order_id} to {order.hotel}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                delivered.append(order.order_id)

                # Signal parent workflow that delivery is complete
                try:
                    parent = workflow.get_external_workflow_handle(PARENT_WORKFLOW_ID)
                    await parent.signal(
                        "order_delivered",
                        OrderDeliveredInput(
                            driver_id=driver_id,
                            order_id=order.order_id,
                            delivery_lat=order.delivery_lat,
                            delivery_lng=order.delivery_lng,
                        ),
                    )
                except Exception:
                    workflow.logger.warning(
                        f"Could not signal parent for {order.order_id} delivery"
                    )

        return f"AI-Driver {driver_id} completed {len(delivered)} deliveries: {delivered}"


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
        self._routes_done: bool = False
        self._disconnected_drivers: set[str] = set()
        self._disconnected_agents: set[str] = set()
        # Workflow-owned driver state
        self._driver_orders: dict[str, list[str]] = {}
        self._driver_last_position: dict[str, tuple[float, float]] = {}
        self._orders_generated: int = 0
        self._route_handles: dict = {}

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
        workflow.logger.info(f"Agent {inp.agent_name} disconnected")

    @workflow.signal
    async def agent_reconnected(self, inp: AgentDisconnectInput) -> None:
        self._disconnected_agents.discard(inp.agent_name)
        workflow.logger.info(f"Agent {inp.agent_name} reconnected")

    @workflow.signal
    async def order_delivered(self, inp: OrderDeliveredInput) -> None:
        """Signaled by DriverRouteWorkflow when a delivery completes."""
        driver_id = inp.driver_id
        if driver_id in self._driver_orders:
            if inp.order_id in self._driver_orders[driver_id]:
                self._driver_orders[driver_id].remove(inp.order_id)
        self._driver_last_position[driver_id] = (inp.delivery_lat, inp.delivery_lng)
        workflow.logger.info(f"Order {inp.order_id} delivered by {driver_id}")

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
            "driver_positions": {
                cid: {"lat": pos[0], "lng": pos[1]}
                for cid, pos in self._driver_last_position.items()
            },
        }

    # --- Helpers ---

    def _build_driver_snapshots(self) -> list[DriverSnapshot]:
        """Build driver snapshots from workflow state for activity inputs."""
        snapshots = []
        for driver_id in ["ai-driver-1", "ai-driver-2", "ai-driver-3"]:
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

        # Initialize driver state
        for i in range(1, 4):
            driver_id = f"ai-driver-{i}"
            self._driver_orders[driver_id] = []
            self._driver_last_position[driver_id] = (WAREHOUSE.lat, WAREHOUSE.lng)

        # Start 3 empty driver child workflows
        self._route_handles = {}
        for i in range(1, 4):
            driver_id = f"ai-driver-{i}"
            handle = await workflow.start_child_workflow(
                DriverRouteWorkflow.run,
                DriverRouteInput(driver_id=driver_id),
                id=f"route-{driver_id}",
                static_summary=f"{driver_id} — delivery loop",
            )
            self._route_handles[driver_id] = handle

        # Run order generation and signal processing concurrently
        order_task = asyncio.create_task(self._order_generation_loop(inp.max_orders))
        signal_task = asyncio.create_task(self._signal_loop())

        # Wait for order generation to complete (all orders generated + delivered)
        await order_task

        # Stop all drivers
        self._routes_done = True
        for handle in self._route_handles.values():
            try:
                await handle.signal(DriverRouteWorkflow.stop)
            except Exception:
                pass

        await signal_task

        # Wait for drivers to finish current deliveries
        results = []
        for driver_id, handle in self._route_handles.items():
            try:
                result = await handle
                results.append(result)
            except Exception as e:
                results.append(f"{driver_id}: {e}")

        mode = "ADK" if not _MOCK_MODE else "mock"
        return f"Meltdown demo complete ({mode}). Results: {results}"

    # --- Order generation loop ---

    async def _run_adk_assignment(
        self, inp: ReasonAboutAssignmentInput
    ) -> ReasonAboutAssignmentOutput | None:
        """Run ADK agents for order assignment. Returns None on failure."""
        agent = create_order_assignment_agent()
        if agent is None:
            return None

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

        prompt = (
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
        try:
            async for event in runner.run_async(
                user_id="workflow",
                session_id=session.id,
                new_message=Content(parts=[Part(text=prompt)]),
            ):
                events_count += 1
        except Exception as e:
            workflow.logger.error(f"ADK assignment failed: {e}")
            return None

        updated_session = await session_service.get_session(
            app_name="meltdown_demo",
            user_id="workflow",
            session_id=session.id,
        )

        assignment_dict = (updated_session.state or {}).get("assignment")
        if not assignment_dict:
            workflow.logger.warning("ADK assignment resolver did not submit an assignment")
            return None

        workflow.logger.info(
            f"ADK assignment complete: {events_count} events, driver={assignment_dict['driver_id']}"
        )
        return ReasonAboutAssignmentOutput(
            driver_id=assignment_dict["driver_id"],
            reasoning_summary=assignment_dict.get("reasoning_summary", "ADK assignment"),
        )

    async def _order_generation_loop(
        self,
        max_orders: int = MAX_ORDERS,
    ) -> None:
        """Generate orders on a timer and assign via multi-agent reasoning."""
        for order_num in range(1, max_orders + 1):
            if self._routes_done:
                break

            # Generate a new order
            order = await workflow.execute_activity(
                generate_order,
                GenerateOrderInput(order_number=order_num),
                task_queue=DELIVERY_QUEUE,
                summary=f"Generate order #{order_num}",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )
            self._orders_generated = order_num

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

            assignment = None

            # Try ADK agents first when available
            if not _MOCK_MODE:
                assignment = await self._run_adk_assignment(assignment_input)
                if assignment is not None:
                    # ADK succeeded — register the assignment in fleet state via activity
                    await workflow.execute_activity(
                        register_assignment,
                        args=[assignment.driver_id, order.order_id],
                        task_queue=AGENTS_QUEUE,
                        summary=f"Resolver — register {order.order_id} → {assignment.driver_id}",
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=FAST_RETRY,
                    )

            # Fallback to mock if ADK unavailable or failed
            if assignment is None:
                assignment = await workflow.execute_activity(
                    reason_about_assignment,
                    assignment_input,
                    task_queue=AGENTS_QUEUE,
                    summary=f"Fleet + Customer + Resolver — assign {order.order_id}",
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

            # Update workflow-owned driver state
            driver_id = assignment.driver_id
            if driver_id in self._driver_orders:
                self._driver_orders[driver_id].append(order.order_id)

            # Signal the chosen driver
            if driver_id in self._route_handles:
                await self._route_handles[driver_id].signal(
                    DriverRouteWorkflow.add_order,
                    DriverRouteOrder(
                        order_id=order.order_id,
                        hotel=order.hotel,
                        delivery_lat=order.delivery_lat,
                        delivery_lng=order.delivery_lng,
                    ),
                )

            workflow.logger.info(f"Order {order_num}/{MAX_ORDERS}: {order.order_id} -> {driver_id}")

            # Wait before next order
            if order_num < max_orders:
                await workflow.sleep(timedelta(seconds=ORDER_INTERVAL_SECONDS))

    # --- Signal processing loop ---

    def _has_pending_signal(self) -> bool:
        return len(self._pending_changes) > 0

    async def _signal_loop(self) -> None:
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: self._has_pending_signal() or self._routes_done,
            )

            if self._routes_done:
                break

            await self._drain_pending_signals()

    async def _drain_pending_signals(self) -> None:
        while self._pending_changes:
            change = self._pending_changes.pop(0)
            await self._process_customer_change(change)

    # --- Customer change handling ---

    async def _process_customer_change(self, change: CustomerChangeInput) -> None:
        await workflow.execute_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=(
                    f"Customer change request for {change.order_id}: "
                    f"{change.change_type} — {change.new_details}"
                ),
            ),
            task_queue=DELIVERY_QUEUE,
            summary=f"Customer Agent — change request for {change.order_id}",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=FAST_RETRY,
        )

        await workflow.wait_condition(lambda: len(self._pending_approvals) > 0)
        approved = self._pending_approvals.pop(0)

        if approved:
            await workflow.execute_activity(
                execute_customer_change,
                ExecuteCustomerChangeInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                    new_lat=change.new_lat,
                    new_lng=change.new_lng,
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"Resolver — execute change for {change.order_id}",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Signal child workflow with updated coordinates
            driver_id = self._find_driver_for_order(change.order_id)
            if driver_id and driver_id in self._route_handles:
                if change.change_type == "cancel":
                    await self._route_handles[driver_id].signal(
                        DriverRouteWorkflow.cancel_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                        ),
                    )
                else:
                    await self._route_handles[driver_id].signal(
                        DriverRouteWorkflow.update_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                            new_lat=change.new_lat,
                            new_lng=change.new_lng,
                        ),
                    )

            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_executed",
                    content=(
                        f"Customer change approved and executed for "
                        f"{change.order_id}: {change.new_details}"
                    ),
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"Resolver — change approved for {change.order_id}",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
        else:
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_rejected",
                    content=f"Customer change rejected for {change.order_id}",
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"Resolver — change rejected for {change.order_id}",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

    def _find_driver_for_order(self, order_id: str) -> str | None:
        """Find which driver has a given order."""
        for driver_id, orders in self._driver_orders.items():
            if order_id in orders:
                return driver_id
        return None
