"""DemoTemporalModel — context-aware summaries and smaller payloads.

Subclasses TemporalModel to:
1. Generate dynamic summaries from llm_request (agent name, order, phase)
2. Strip null fields from LlmRequest before serialization
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator

from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from temporalio import workflow
from temporalio.contrib.google_adk_agents import TemporalModel
from temporalio.contrib.google_adk_agents._model import invoke_model

# Agent display names for Temporal UI summaries
_AGENT_LABELS = {
    "assignment_fleet_agent": "Fleet Agent",
    "assignment_customer_agent": "Customer Agent",
    "assignment_dispatch_agent": "Dispatch Agent",
    "google_search_agent": "Customer Agent",
}

# Human-readable labels for tool responses (success vs error)
_TOOL_LABELS = {
    "tool_get_fleet_status": ("LLM reasoning on fleet data", "fleet status FAILED"),
    "tool_get_route_info": ("LLM reasoning on route ETA", "route ETA FAILED"),
    "tool_get_order_priorities": ("LLM reasoning on order priorities", "order priorities FAILED"),
    "tool_submit_assignment": ("assignment submitted", "assignment FAILED"),
    "google_search_agent": ("LLM reasoning on hotel context", "hotel search FAILED"),
}


def _extract_order_context(llm_request: LlmRequest) -> tuple[str | None, str | None]:
    """Extract order number and hotel from the first user message."""
    if not llm_request.contents:
        return None, None
    first = llm_request.contents[0]
    if first.role == "user" and first.parts:
        text = first.parts[0].text or ""
        num_match = re.search(r"Order ID:\s*order-(\d+)", text)
        hotel_match = re.search(r"Hotel:\s*(.+)", text)
        return (
            num_match.group(1) if num_match else None,
            hotel_match.group(1).strip() if hotel_match else None,
        )
    return None, None


def _is_error_response(function_response) -> bool:
    """Check if a tool response indicates an error."""
    if not function_response or not function_response.response:
        return False
    result = function_response.response.get("result", "")
    return isinstance(result, str) and result.startswith("ERROR:")


def _count_tool_responses(contents: list, tool_name: str) -> int:
    """Count how many times a specific tool response appears in the conversation."""
    count = 0
    for content in contents:
        if content.role == "user" and content.parts:
            for part in content.parts:
                if part.function_response and part.function_response.name == tool_name:
                    count += 1
    return count


def _build_summary(llm_request: LlmRequest) -> str:
    """Build a descriptive summary from the llm_request conversation state."""
    # Agent name from ADK labels
    agent_name = "Agent"
    if llm_request.config and llm_request.config.labels:
        adk_name = llm_request.config.labels.get("adk_agent_name", "")
        agent_name = _AGENT_LABELS.get(adk_name, adk_name)

    # Order context
    order_num, hotel = _extract_order_context(llm_request)
    prefix = f"[#{order_num}] " if order_num else ""

    # Determine phase from conversation contents
    contents = llm_request.contents or []
    if len(contents) <= 1:
        hotel_suffix = f" for {hotel}" if hotel else ""
        return f"{prefix}{agent_name} — evaluating order{hotel_suffix}"

    # Check the last content entry for tool responses
    last = contents[-1]
    if last.role == "user" and last.parts:
        for part in last.parts:
            if part.function_response:
                tool_name = part.function_response.name or "tool"
                is_error = _is_error_response(part.function_response)
                labels = _TOOL_LABELS.get(tool_name)
                if labels:
                    label = labels[1] if is_error else labels[0]
                else:
                    label = f"{tool_name} {'FAILED' if is_error else 'received'}"
                # Add occurrence count to distinguish repeated tool calls
                count = _count_tool_responses(contents, tool_name)
                if count > 1:
                    label = f"{label} ({count})"
                return f"{prefix}{agent_name} — {label}"

    # Dispatch Agent with prior context — synthesizing both agents
    if agent_name == "Dispatch Agent":
        return f"{prefix}{agent_name} — weighing fleet + customer input"

    return f"{prefix}{agent_name} — reasoning"


def _strip_nulls(obj: object) -> object:
    """Recursively strip None values from dicts (reduces payload size)."""
    if isinstance(obj, dict):
        return {k: _strip_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_nulls(item) for item in obj]
    return obj


class DemoTemporalModel(TemporalModel):
    """TemporalModel with dynamic summaries and null-stripped payloads."""

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        summary = _build_summary(llm_request)

        # Strip nulls from the request to reduce payload size in workflow history
        clean_data = _strip_nulls(llm_request.model_dump())
        clean_request = LlmRequest.model_validate(clean_data)

        # Override summary for this specific call
        config = self._activity_config.copy()
        config["summary"] = summary

        responses = await workflow.execute_activity(
            invoke_model,
            args=[clean_request],
            **config,
        )
        for response in responses:
            yield response
