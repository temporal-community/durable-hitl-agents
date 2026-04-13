"""Fixed activity_tool wrapper for ADK + Temporal integration.

Two fixes over the upstream temporalio.contrib.google_adk_agents.workflow.activity_tool:

1. Multi-arg handling: upstream unpacks as positional args (*activity_args), but
   workflow.execute_activity only accepts a single positional arg. This version
   uses args=[...] for correct multi-arg handling.

2. Graceful failure: when an activity exhausts its retry policy, the error is
   caught and returned as a string to the LLM instead of crashing the ADK
   pipeline. This lets agents reason about tool failures — the Dispatch Agent can
   assign based on available data when Fleet Agent tools are down. Temporal
   still shows the retry attempts in the UI.

3. Dynamic summaries: if a base summary is provided, tool arguments are appended
   to distinguish multiple calls to the same tool (e.g., route info for different
   destinations).
"""

import inspect
from collections.abc import Callable
from typing import Any

from temporalio import workflow


def activity_tool(activity_def: Callable, **kwargs: Any) -> Callable:
    """Wrap a Temporal Activity as an ADK Tool.

    Preserves the activity's signature for ADK's tool schema generation
    while routing execution through workflow.execute_activity.

    On activity failure (after retries exhausted), returns an error string
    to the LLM instead of raising — so the agent pipeline continues.
    """

    base_summary = kwargs.get("summary", "")

    async def wrapper(*args: Any, **kw: Any):
        sig = inspect.signature(activity_def)
        bound = sig.bind(*args, **kw)
        bound.apply_defaults()

        activity_args = list(bound.arguments.values())
        options = kwargs.copy()

        # Build dynamic summary from arguments
        if base_summary:
            origin = bound.arguments.get("origin_name", "")
            dest = bound.arguments.get("destination_name", "")
            if origin and dest:
                options["summary"] = f"{base_summary} — {origin} → {dest}"
            elif dest:
                options["summary"] = f"{base_summary} — {dest}"
            elif origin:
                options["summary"] = f"{base_summary} — {origin}"

        try:
            if len(activity_args) == 0:
                return await workflow.execute_activity(activity_def, **options)
            elif len(activity_args) == 1:
                return await workflow.execute_activity(activity_def, activity_args[0], **options)
            else:
                return await workflow.execute_activity(activity_def, args=activity_args, **options)
        except Exception as e:
            # Return error to the LLM as a tool response — don't crash the pipeline.
            # The LLM can reason about the failure and adapt (e.g., Dispatch Agent assigns
            # without fleet data when Fleet Agent tools are disconnected).
            return f"ERROR: Tool {activity_def.__name__} failed: {e}"

    wrapper.__name__ = activity_def.__name__
    wrapper.__doc__ = activity_def.__doc__
    setattr(wrapper, "__signature__", inspect.signature(activity_def))

    return wrapper
