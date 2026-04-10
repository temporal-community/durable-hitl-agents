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
        register_assignment,
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

MAX_ORDERS = 20
ORDER_INTERVAL_SECONDS = 15

PARENT_WORKFLOW_ID = "meltdown-demo"
DRIVER_CAPACITY = 3


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
        # Mid-delivery reroute/cancel tracking
        self._active_order_id: str | None = None
        self._reroute_pending: dict | None = None  # {"lat": float, "lng": float}
        self._cancel_pending: bool = False

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
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def update_order(self, inp: OrderUpdateInput) -> None:
        """Update delivery coordinates — works for both pending and active orders."""
        # Check pending orders first
        for order in self._pending_orders:
            if order.order_id == inp.order_id:
                if inp.new_lat is not None and inp.new_lng is not None:
                    order.delivery_lat = inp.new_lat
                    order.delivery_lng = inp.new_lng
                workflow.logger.info(f"Order {inp.order_id} updated (pending) — new destination")
                return
        # If order is currently being delivered, flag for reroute
        if self._active_order_id == inp.order_id:
            if inp.new_lat is not None and inp.new_lng is not None:
                self._reroute_pending = {"lat": inp.new_lat, "lng": inp.new_lng}
                workflow.logger.info(
                    f"Order {inp.order_id} reroute queued — driver will reroute after current leg"
                )
            return
        workflow.logger.info(f"Order {inp.order_id} not found — may already be delivered")

    @workflow.signal
    async def cancel_order(self, inp: OrderUpdateInput) -> None:
        """Cancel an order — works for both pending and active orders."""
        # Check pending orders
        before = len(self._pending_orders)
        self._pending_orders = [o for o in self._pending_orders if o.order_id != inp.order_id]
        if len(self._pending_orders) < before:
            workflow.logger.info(f"Order {inp.order_id} cancelled — removed from pending queue")
            return
        # If order is currently being delivered, flag for cancel
        if self._active_order_id == inp.order_id:
            self._cancel_pending = True
            workflow.logger.info(f"Order {inp.order_id} cancel queued — will skip delivery")
            return
        workflow.logger.info(f"Order {inp.order_id} not found — may already be delivered")

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
        }

    # --- Helpers ---

    async def _execute_navigate(self, driver_id: str, nav_input: NavigateInput) -> NavigateOutput:
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

            # Process all pending orders
            while self._pending_orders:
                order = self._pending_orders.pop(0)
                self._active_order_id = order.order_id
                self._reroute_pending = None
                self._cancel_pending = False

                # Navigate to shop for pickup
                self._status = "en_route_pickup"
                pickup_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — route to shop for {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng
                self._path_history.append(
                    {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                )

                # Check for cancel before pickup
                if self._cancel_pending:
                    workflow.logger.info(
                        f"Order {order.order_id} cancelled before pickup — skipping"
                    )
                    self._active_order_id = None
                    self._cancel_pending = False
                    continue

                # Pick up — activity checks disconnect, Temporal retries
                self._status = "picking_up"
                await workflow.execute_activity(
                    pickup_orders,
                    PickupInput(
                        driver_id=driver_id,
                        order_ids=[order.order_id],
                    ),
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — picking up {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=NAV_RETRY,
                )

                self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

                # Navigate to hotel — with reroute loop
                # If a customer change arrives during delivery navigation, the
                # reroute_pending flag gets set by the update_order signal handler.
                # After navigation completes, we check the flag and re-navigate
                # to the new destination.
                while True:
                    # Check for cancel before navigating
                    if self._cancel_pending:
                        workflow.logger.info(
                            f"Order {order.order_id} cancelled — skipping delivery"
                        )
                        break

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
                        summary=f"{driver_id} — route to {order.hotel} for {order.order_id}",
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
                    )
                    self._current_lat = nav_result.final_lat
                    self._current_lng = nav_result.final_lng
                    self._path_history.append(
                        {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                    )

                    # Check if a reroute signal arrived during navigation
                    if self._reroute_pending:
                        new_dest = self._reroute_pending
                        self._reroute_pending = None
                        order.delivery_lat = new_dest["lat"]
                        order.delivery_lng = new_dest["lng"]
                        workflow.logger.info(
                            f"Order {order.order_id} rerouted — navigating to new destination"
                        )
                        continue  # Re-navigate to new destination

                    break  # No reroute — proceed to deliver

                # Deliver (skip if cancelled)
                if not self._cancel_pending:
                    self._status = "delivering"
                    await workflow.execute_activity(
                        deliver_order,
                        DeliverInput(
                            driver_id=driver_id,
                            order_id=order.order_id,
                        ),
                        task_queue=DELIVERY_QUEUE,
                        summary=f"{driver_id} — delivered {order.order_id} to {order.hotel}",
                        schedule_to_close_timeout=timedelta(minutes=5),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=NAV_RETRY,
                    )
                    delivered.append(order.order_id)

                self._active_order_id = None
                self._cancel_pending = False

                # Remove from current_orders tracking
                try:
                    self._current_orders.remove(order.order_id)
                except ValueError:
                    pass

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
                except Exception as e:
                    workflow.logger.warning(
                        f"Could not signal parent for {order.order_id} delivery: {e}"
                    )

            # All pending orders processed — drive back to base if not already there
            if (
                abs(self._current_lat - WAREHOUSE.lat) > 0.001
                or abs(self._current_lng - WAREHOUSE.lng) > 0.001
            ):
                self._status = "returning"
                return_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{driver_id} — returning to base",
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
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

            self._status = "idle"
            self._path_history.clear()

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
                summary=f"Generate order #{order_num}",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Signal parent with the new order
            parent = workflow.get_external_workflow_handle(PARENT_WORKFLOW_ID)
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

            workflow.logger.info(
                f"Order {order_num}/{inp.max_orders}: {order.order_id} signaled to parent"
            )

            # Wait a randomized interval before next order (deterministic on replay)
            if order_num < inp.max_orders:
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

    def _build_driver_snapshots(self) -> list[DriverSnapshot]:
        """Build driver snapshots from workflow state for activity inputs."""
        snapshots = []
        for driver_id in ["driver-1", "driver-2", "driver-3"]:
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
            driver_id = f"driver-{i}"
            self._driver_orders[driver_id] = []
            self._driver_last_position[driver_id] = (WAREHOUSE.lat, WAREHOUSE.lng)

        # Start 3 driver child workflows
        self._route_handles = {}
        for i in range(1, 4):
            driver_id = f"driver-{i}"
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
            raise RuntimeError("ADK assignment resolver did not submit an assignment")

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
        events.append(
            {
                "agent_name": "resolver",
                "event_type": "plan",
                "content": f"{driver_id} — {reasoning}",
                "summary": f"Assigned {driver_id}",
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
        self._orders_generated += 1

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

        assignment = await self._run_adk_assignment(assignment_input)
        await workflow.execute_activity(
            register_assignment,
            args=[assignment.driver_id, order.order_id],
            task_queue=AGENTS_QUEUE,
            summary=f"Resolver — register {order.order_id} → {assignment.driver_id}",
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        # Publish short summary events to FleetState for the frontend UI panel
        for evt in assignment.agent_events:
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name=evt["agent_name"],
                    event_type=evt["event_type"],
                    content=evt["content"],
                    summary=evt["summary"],
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"{evt['agent_name']} — {evt['event_type']}",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

        # Update workflow-owned driver state
        driver_id = assignment.driver_id
        if driver_id in self._driver_orders:
            self._driver_orders[driver_id].append(order.order_id)

        # Update order tracking
        if order.order_id in self._orders:
            self._orders[order.order_id]["assigned_driver_id"] = driver_id
            self._orders[order.order_id]["status"] = "assigned"

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
                    new_hotel=change.new_hotel,
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
