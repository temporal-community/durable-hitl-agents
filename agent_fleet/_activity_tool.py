"""Fixed activity_tool wrapper for ADK + Temporal integration.

The upstream temporalio.contrib.google_adk_agents.workflow.activity_tool
unpacks multi-arg activities as positional args (*activity_args), but
workflow.execute_activity only accepts a single positional arg. This
version uses args=[...] for correct multi-arg handling.
"""

import inspect
from collections.abc import Callable
from typing import Any

from temporalio import workflow


def activity_tool(activity_def: Callable, **kwargs: Any) -> Callable:
    """Wrap a Temporal Activity as an ADK Tool.

    Preserves the activity's signature for ADK's tool schema generation
    while routing execution through workflow.execute_activity.
    """

    async def wrapper(*args: Any, **kw: Any):
        sig = inspect.signature(activity_def)
        bound = sig.bind(*args, **kw)
        bound.apply_defaults()

        activity_args = list(bound.arguments.values())
        options = kwargs.copy()

        if len(activity_args) == 0:
            return await workflow.execute_activity(activity_def, **options)
        elif len(activity_args) == 1:
            return await workflow.execute_activity(activity_def, activity_args[0], **options)
        else:
            return await workflow.execute_activity(activity_def, args=activity_args, **options)

    wrapper.__name__ = activity_def.__name__
    wrapper.__doc__ = activity_def.__doc__
    setattr(wrapper, "__signature__", inspect.signature(activity_def))

    return wrapper
