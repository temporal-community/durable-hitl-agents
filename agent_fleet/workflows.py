"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts 3 crew child workflows,
generates orders on a timer, runs multi-agent reasoning per order, and
signals the chosen crew. Handles customer-change signals concurrently.

CrewRouteWorkflow — per-crew continuous delivery loop (child workflow).
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
        sync_crew_disconnect,
        sync_crew_recovery_complete,
    )
    from agent_fleet.agents import create_order_assignment_agent
    from agent_fleet.config import MOCK_MODE as _MOCK_MODE
    from agent_fleet.locations import WAREHOUSE
    from agent_fleet.models import (
        AgentDisconnectInput,
        CrewDisconnectInput,
        CrewRouteInput,
        CrewRouteOrder,
        CrewSnapshot,
        CustomerChangeInput,
        DeliverInput,
        ExecuteCustomerChangeInput,
        GenerateOrderInput,
        MeltdownDemoInput,
        NavigateInput,
        NavigateOutput,
        OrderDeliveredInput,
        PickupInput,
        PublishAgentEventInput,
        ReasonAboutAssignmentInput,
        ReasonAboutAssignmentOutput,
        SyncCrewDisconnectInput,
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
CREW_CAPACITY = 3


# --- Per-crew continuous delivery workflow ---


@workflow.defn
class CrewRouteWorkflow:
    """
    Continuous delivery loop for a single AI-Crew.

    Waits for orders via signal, picks up at the shop, delivers to hotel,
    then returns to waiting. Loops until told to stop.

    Disconnect handling uses two Temporal-idiomatic mechanisms:
    - is_crew_disconnected input flag: catches "already disconnected at activity start"
    - Cancellation scope: catches "disconnect signal arrives mid-activity" — the
      workflow cancels the running activity, which receives CancelledError on its
      next heartbeat() call.
    """

    def __init__(self) -> None:
        self._pending_orders: list[CrewRouteOrder] = []
        self._stop = False
        self._is_disconnected: bool = False
        self._active_scope: workflow.CancellationScope | None = None
        self._current_lat: float = 0.0
        self._current_lng: float = 0.0

    # --- Signals ---

    @workflow.signal
    async def add_order(self, order: CrewRouteOrder) -> None:
        self._pending_orders.append(order)

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.signal
    async def crew_disconnected(self, inp: CrewDisconnectInput) -> None:
        self._is_disconnected = True
        if self._active_scope:
            self._active_scope.cancel()
        workflow.logger.info(f"Crew {inp.crew_id} disconnected — cancelling active activity")

    @workflow.signal
    async def crew_reconnected(self, inp: CrewDisconnectInput) -> None:
        self._is_disconnected = False
        workflow.logger.info(f"Crew {inp.crew_id} reconnected — resuming")

    # --- Queries ---

    @workflow.query
    def get_position(self) -> dict:
        """Return current crew position. Used by parent workflow for crew snapshots."""
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

    async def _sync_disconnect_to_ui(self, crew_id: str, disconnected: bool) -> None:
        """Push disconnect/reconnect state to FleetState for the frontend."""
        await workflow.execute_activity(
            sync_crew_disconnect,
            SyncCrewDisconnectInput(crew_id=crew_id, disconnected=disconnected),
            task_queue=DELIVERY_QUEUE,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=FAST_RETRY,
        )
        if not disconnected:
            # Clear recovery visual after a short delay
            await workflow.sleep(timedelta(seconds=3))
            await workflow.execute_activity(
                sync_crew_recovery_complete,
                crew_id,
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

    async def _await_reconnect(self, crew_id: str) -> None:
        """Block until crew is reconnected, syncing state to FleetState via activities."""
        if self._is_disconnected:
            # Sync disconnect to frontend
            await self._sync_disconnect_to_ui(crew_id, disconnected=True)
            workflow.logger.info(f"{crew_id} disconnected — waiting for reconnect")
            await workflow.wait_condition(lambda: not self._is_disconnected)
            # Sync reconnect to frontend
            await self._sync_disconnect_to_ui(crew_id, disconnected=False)
            workflow.logger.info(f"{crew_id} reconnected — resuming delivery")

    async def _navigate_with_disconnect_guard(
        self, crew_id: str, nav_input: NavigateInput, summary: str = ""
    ) -> NavigateOutput:
        """Execute navigate_to with cancellation scope for mid-flight disconnect.

        If a crew_disconnected signal arrives during navigation:
        1. Signal handler cancels the active scope
        2. Activity receives CancelledError on its next heartbeat() call
        3. Workflow catches it, waits for reconnect, retries navigation
        """
        while True:
            await self._await_reconnect(crew_id)
            # Rebuild input with current disconnect state (False — we just confirmed connected)
            nav_input = NavigateInput(
                crew_id=nav_input.crew_id,
                order_id=nav_input.order_id,
                target_lat=nav_input.target_lat,
                target_lng=nav_input.target_lng,
                leg=nav_input.leg,
                steps=nav_input.steps,
                waypoints=nav_input.waypoints,
                is_crew_disconnected=False,
                start_lat=self._current_lat,
                start_lng=self._current_lng,
            )
            try:
                self._active_scope = workflow.CancellationScope()
                async with self._active_scope:
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
            except asyncio.CancelledError:
                if self._is_disconnected:
                    workflow.logger.info(
                        f"{crew_id} disconnected mid-navigation — will retry on reconnect"
                    )
                    continue
                raise
            finally:
                self._active_scope = None

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: CrewRouteInput) -> str:
        crew_id = inp.crew_id
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
                    summary=f"{crew_id} — route to shop for {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._navigate_with_disconnect_guard(
                    crew_id,
                    NavigateInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                        is_crew_disconnected=self._is_disconnected,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"{crew_id} — navigating to shop for {order.order_id}",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

                # Pick up
                await self._await_reconnect(crew_id)
                await workflow.execute_activity(
                    pickup_orders,
                    PickupInput(
                        crew_id=crew_id,
                        order_ids=[order.order_id],
                        is_crew_disconnected=self._is_disconnected,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{crew_id} — picking up {order.order_id}",
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
                    summary=f"{crew_id} — route to {order.hotel} for {order.order_id}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._navigate_with_disconnect_guard(
                    crew_id,
                    NavigateInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        target_lat=order.delivery_lat,
                        target_lng=order.delivery_lng,
                        leg="delivery",
                        steps=30,
                        waypoints=delivery_waypoints,
                        is_crew_disconnected=self._is_disconnected,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"{crew_id} — delivering {order.order_id} to {order.hotel}",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

                # Deliver
                await self._await_reconnect(crew_id)
                await workflow.execute_activity(
                    deliver_order,
                    DeliverInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        is_crew_disconnected=self._is_disconnected,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    summary=f"{crew_id} — delivered {order.order_id} to {order.hotel}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                delivered.append(order.order_id)

                # Signal parent workflow that delivery is complete
                parent = workflow.get_external_workflow_handle(PARENT_WORKFLOW_ID)
                await parent.signal(
                    "order_delivered",
                    OrderDeliveredInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        delivery_lat=order.delivery_lat,
                        delivery_lng=order.delivery_lng,
                    ),
                )

        return f"AI-Crew {crew_id} completed {len(delivered)} deliveries: {delivered}"


# --- Main demo orchestrator ---


@workflow.defn
class MeltdownDemoWorkflow:
    """
    Orchestrates the Meltdown demo with continuous order flow.

    Starts 3 crew child workflows, generates orders on a timer,
    runs multi-agent reasoning per order, and signals the chosen crew.
    Handles customer-change signals concurrently.

    Owns crew state: positions, order assignments, disconnect status.
    Activities receive this state as inputs — they never read FleetState
    for decision-making.
    """

    def __init__(self) -> None:
        self._pending_changes: list[CustomerChangeInput] = []
        self._pending_approvals: list[bool] = []
        self._routes_done: bool = False
        self._disconnected_crews: set[str] = set()
        self._disconnected_agents: set[str] = set()
        # Workflow-owned crew state
        self._crew_orders: dict[str, list[str]] = {}
        self._crew_last_position: dict[str, tuple[float, float]] = {}
        self._orders_generated: int = 0

    # --- Signals ---

    @workflow.signal
    async def customer_change(self, change: CustomerChangeInput) -> None:
        self._pending_changes.append(change)

    @workflow.signal
    async def change_approved(self, approved: bool) -> None:
        self._pending_approvals.append(approved)

    @workflow.signal
    async def crew_disconnected(self, inp: CrewDisconnectInput) -> None:
        self._disconnected_crews.add(inp.crew_id)
        workflow.logger.info(f"Crew {inp.crew_id} disconnected — activities will retry")

    @workflow.signal
    async def crew_reconnected(self, inp: CrewDisconnectInput) -> None:
        self._disconnected_crews.discard(inp.crew_id)
        workflow.logger.info(f"Crew {inp.crew_id} reconnected — resuming")

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
        """Signaled by CrewRouteWorkflow when a delivery completes."""
        crew_id = inp.crew_id
        if crew_id in self._crew_orders:
            if inp.order_id in self._crew_orders[crew_id]:
                self._crew_orders[crew_id].remove(inp.order_id)
        self._crew_last_position[crew_id] = (inp.delivery_lat, inp.delivery_lng)
        workflow.logger.info(f"Order {inp.order_id} delivered by {crew_id}")

    # --- Queries ---

    @workflow.query
    def get_status(self) -> dict:
        return {
            "routes_done": self._routes_done,
            "orders_generated": self._orders_generated,
            "pending_changes": len(self._pending_changes),
            "disconnected_crews": list(self._disconnected_crews),
            "disconnected_agents": list(self._disconnected_agents),
            "crew_orders": {cid: list(oids) for cid, oids in self._crew_orders.items()},
            "crew_positions": {
                cid: {"lat": pos[0], "lng": pos[1]} for cid, pos in self._crew_last_position.items()
            },
        }

    # --- Helpers ---

    def _build_crew_snapshots(self) -> list[CrewSnapshot]:
        """Build crew snapshots from workflow state for activity inputs."""
        snapshots = []
        for crew_id in ["ai-crew-1", "ai-crew-2", "ai-crew-3"]:
            pos = self._crew_last_position.get(crew_id, (WAREHOUSE.lat, WAREHOUSE.lng))
            order_count = len(self._crew_orders.get(crew_id, []))
            snapshots.append(
                CrewSnapshot(
                    crew_id=crew_id,
                    lat=pos[0],
                    lng=pos[1],
                    status="disconnected" if crew_id in self._disconnected_crews else "active",
                    capacity=CREW_CAPACITY,
                    current_order_count=order_count,
                    is_disconnected=crew_id in self._disconnected_crews,
                )
            )
        return snapshots

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: MeltdownDemoInput) -> str:
        workflow.logger.info(f"Meltdown demo starting (escalation={inp.escalation_enabled})")

        # Initialize crew state
        for i in range(1, 4):
            crew_id = f"ai-crew-{i}"
            self._crew_orders[crew_id] = []
            self._crew_last_position[crew_id] = (WAREHOUSE.lat, WAREHOUSE.lng)

        # Start 3 empty crew child workflows
        route_handles = {}
        for i in range(1, 4):
            crew_id = f"ai-crew-{i}"
            handle = await workflow.start_child_workflow(
                CrewRouteWorkflow.run,
                CrewRouteInput(crew_id=crew_id),
                id=f"route-{crew_id}",
                static_summary=f"{crew_id} — delivery loop",
            )
            route_handles[crew_id] = handle

        # Run order generation and signal processing concurrently
        order_task = asyncio.create_task(self._order_generation_loop(route_handles, inp.max_orders))
        signal_task = asyncio.create_task(self._signal_loop())

        # Wait for order generation to complete (all orders generated + delivered)
        await order_task

        # Stop all crews
        self._routes_done = True
        for handle in route_handles.values():
            try:
                await handle.signal(CrewRouteWorkflow.stop)
            except Exception:
                pass

        await signal_task

        # Wait for crews to finish current deliveries
        results = []
        for crew_id, handle in route_handles.items():
            try:
                result = await handle
                results.append(result)
            except Exception as e:
                results.append(f"{crew_id}: {e}")

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
            f"NEW ORDER — assign to the best crew:\n"
            f"Order ID: {inp.order_id}\n"
            f"Hotel: {inp.hotel}\n"
            f"Event: {inp.event}\n"
            f"Priority: {inp.priority}\n"
            f"Servings: {inp.servings}\n"
            f"Deadline: {inp.deadline_minutes} minutes\n"
            f"Coordinates: ({inp.delivery_lat}, {inp.delivery_lng})\n\n"
            f"Assess fleet capacity and customer priority, then the resolver "
            f"MUST call tool_submit_assignment with the crew_id and reasoning."
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
            f"ADK assignment complete: {events_count} events, crew={assignment_dict['crew_id']}"
        )
        return ReasonAboutAssignmentOutput(
            crew_id=assignment_dict["crew_id"],
            reasoning_summary=assignment_dict.get("reasoning_summary", "ADK assignment"),
        )

    async def _order_generation_loop(
        self,
        route_handles: dict,
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

            # Build crew snapshots from workflow state — passed to activity as input
            crew_snapshots = self._build_crew_snapshots()

            assignment_input = ReasonAboutAssignmentInput(
                order_id=order.order_id,
                hotel=order.hotel,
                delivery_lat=order.delivery_lat,
                delivery_lng=order.delivery_lng,
                priority=order.priority,
                servings=order.servings,
                deadline_minutes=order.deadline_minutes,
                event=order.event,
                crew_snapshots=crew_snapshots,
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
                        args=[assignment.crew_id, order.order_id],
                        task_queue=AGENTS_QUEUE,
                        summary=f"Register {order.order_id} → {assignment.crew_id}",
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=FAST_RETRY,
                    )

            # Fallback to mock if ADK unavailable or failed
            if assignment is None:
                assignment = await workflow.execute_activity(
                    reason_about_assignment,
                    assignment_input,
                    task_queue=AGENTS_QUEUE,
                    summary=f"Mock assignment for {order.order_id}",
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

            # Update workflow-owned crew state
            crew_id = assignment.crew_id
            if crew_id in self._crew_orders:
                self._crew_orders[crew_id].append(order.order_id)

            # Signal the chosen crew
            if crew_id in route_handles:
                await route_handles[crew_id].signal(
                    CrewRouteWorkflow.add_order,
                    CrewRouteOrder(
                        order_id=order.order_id,
                        hotel=order.hotel,
                        delivery_lat=order.delivery_lat,
                        delivery_lng=order.delivery_lng,
                    ),
                )

            workflow.logger.info(f"Order {order_num}/{MAX_ORDERS}: {order.order_id} -> {crew_id}")

            # Wait before next order
            if order_num < max_orders:
                await workflow.sleep(timedelta(seconds=ORDER_INTERVAL_SECONDS))

    # --- Signal processing loop ---

    def _has_pending_signal(self) -> bool:
        return len(self._pending_changes) > 0

    async def _signal_loop(self) -> None:
        while not self._routes_done:
            try:
                await workflow.wait_condition(
                    lambda: self._has_pending_signal() or self._routes_done,
                    timeout=timedelta(seconds=2),
                )
            except TimeoutError:
                continue

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
            summary=f"Customer change request — {change.order_id}",
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
                summary=f"Execute customer change — {change.order_id}",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
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
                summary=f"Change approved — {change.order_id}",
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
                summary=f"Change rejected — {change.order_id}",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
