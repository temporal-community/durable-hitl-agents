"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts 3 crew child workflows,
generates orders on a timer, runs multi-agent reasoning per order, and
signals the chosen crew. Handles customer-change signals concurrently.

CrewRouteWorkflow — per-crew continuous delivery loop (child workflow).
Waits for orders via signal, picks up at the shop, delivers, repeats.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import os

    from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE
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
    )
    from agent_fleet.locations import WAREHOUSE
    from agent_fleet.models import (
        AgentDisconnectInput,
        CrewDisconnectInput,
        CrewRouteInput,
        CrewRouteOrder,
        CustomerChangeInput,
        DeliverInput,
        ExecuteCustomerChangeInput,
        GenerateOrderInput,
        MeltdownDemoInput,
        NavigateInput,
        PickupInput,
        PublishAgentEventInput,
        ReasonAboutAssignmentInput,
        ReasonAboutAssignmentOutput,
    )

    _MOCK_MODE = not os.environ.get("GOOGLE_API_KEY")

    try:
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai.types import Content, Part

        from agent_fleet.agents import create_order_assignment_agent

        _ADK_IMPORTS_OK = True
    except ImportError:
        _ADK_IMPORTS_OK = False

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


# --- Per-crew continuous delivery workflow ---


@workflow.defn
class CrewRouteWorkflow:
    """
    Continuous delivery loop for a single AI-Crew.

    Waits for orders via signal, picks up at the shop, delivers to hotel,
    then returns to waiting. Loops until told to stop.
    """

    def __init__(self) -> None:
        self._pending_orders: list[CrewRouteOrder] = []
        self._stop = False

    @workflow.signal
    async def add_order(self, order: CrewRouteOrder) -> None:
        self._pending_orders.append(order)

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.run
    async def run(self, inp: CrewRouteInput) -> str:
        crew_id = inp.crew_id
        delivered = []
        current_lat, current_lng = WAREHOUSE.lat, WAREHOUSE.lng

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
                    args=[current_lat, current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                await workflow.execute_activity(
                    navigate_to,
                    NavigateInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=10),
                    start_to_close_timeout=timedelta(seconds=120),
                    heartbeat_timeout=timedelta(seconds=15),
                    retry_policy=NAV_RETRY,
                )

                # Pick up
                await workflow.execute_activity(
                    pickup_orders,
                    PickupInput(crew_id=crew_id, order_ids=[order.order_id]),
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                current_lat, current_lng = WAREHOUSE.lat, WAREHOUSE.lng

                # Navigate to hotel
                delivery_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[
                        current_lat,
                        current_lng,
                        order.delivery_lat,
                        order.delivery_lng,
                    ],
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                await workflow.execute_activity(
                    navigate_to,
                    NavigateInput(
                        crew_id=crew_id,
                        order_id=order.order_id,
                        target_lat=order.delivery_lat,
                        target_lng=order.delivery_lng,
                        leg="delivery",
                        steps=30,
                        waypoints=delivery_waypoints,
                    ),
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=10),
                    start_to_close_timeout=timedelta(seconds=120),
                    heartbeat_timeout=timedelta(seconds=15),
                    retry_policy=NAV_RETRY,
                )

                current_lat, current_lng = order.delivery_lat, order.delivery_lng

                # Deliver
                await workflow.execute_activity(
                    deliver_order,
                    DeliverInput(crew_id=crew_id, order_id=order.order_id),
                    task_queue=DELIVERY_QUEUE,
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                delivered.append(order.order_id)

        return f"AI-Crew {crew_id} completed {len(delivered)} deliveries: {delivered}"


# --- Main demo orchestrator ---


@workflow.defn
class MeltdownDemoWorkflow:
    """
    Orchestrates the Meltdown demo with continuous order flow.

    Starts 3 crew child workflows, generates orders on a timer,
    runs multi-agent reasoning per order, and signals the chosen crew.
    Handles customer-change signals concurrently.
    """

    def __init__(self) -> None:
        self._pending_changes: list[CustomerChangeInput] = []
        self._pending_approvals: list[bool] = []
        self._routes_done: bool = False
        self._disconnected_crews: set[str] = set()
        self._disconnected_agents: set[str] = set()

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

    # --- Queries ---

    @workflow.query
    def get_status(self) -> dict:
        return {
            "routes_done": self._routes_done,
            "pending_changes": len(self._pending_changes),
            "disconnected_crews": list(self._disconnected_crews),
            "disconnected_agents": list(self._disconnected_agents),
        }

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: MeltdownDemoInput) -> str:
        workflow.logger.info(f"Meltdown demo starting (escalation={inp.escalation_enabled})")

        # Start 3 empty crew child workflows
        route_handles = {}
        for i in range(1, 4):
            crew_id = f"ai-crew-{i}"
            handle = await workflow.start_child_workflow(
                CrewRouteWorkflow.run,
                CrewRouteInput(crew_id=crew_id),
                id=f"route-{crew_id}",
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

        mode = "ADK" if (not _MOCK_MODE and _ADK_IMPORTS_OK) else "mock"
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
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            assignment_input = ReasonAboutAssignmentInput(
                order_id=order.order_id,
                hotel=order.hotel,
                delivery_lat=order.delivery_lat,
                delivery_lng=order.delivery_lng,
                priority=order.priority,
                servings=order.servings,
                deadline_minutes=order.deadline_minutes,
                event=order.event,
            )

            assignment = None

            # Try ADK agents first when available
            if not _MOCK_MODE and _ADK_IMPORTS_OK:
                assignment = await self._run_adk_assignment(assignment_input)
                if assignment is not None:
                    # ADK succeeded — register the assignment in fleet state via activity
                    await workflow.execute_activity(
                        register_assignment,
                        args=[assignment.crew_id, order.order_id],
                        task_queue=AGENTS_QUEUE,
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=FAST_RETRY,
                    )

            # Fallback to mock if ADK unavailable or failed
            if assignment is None:
                assignment = await workflow.execute_activity(
                    reason_about_assignment,
                    assignment_input,
                    task_queue=AGENTS_QUEUE,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

            # Signal the chosen crew
            crew_id = assignment.crew_id
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
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )
            await workflow.execute_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_executed",
                    content=f"Customer change approved and executed for {change.order_id}: {change.new_details}",
                ),
                task_queue=DELIVERY_QUEUE,
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
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
