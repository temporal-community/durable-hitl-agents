"""Request-contract tests for the demo control API."""

import pytest
from pydantic import ValidationError

from agent_fleet.server import DispatchModeRequest, StartRequest


@pytest.mark.parametrize("mode", ["adk", "langgraph", "crossframework"])
def test_dispatch_modes_accept_only_supported_values(mode: str) -> None:
    assert StartRequest(mode=mode).mode == mode
    assert DispatchModeRequest(mode=mode).mode == mode


@pytest.mark.parametrize("request_type", [StartRequest, DispatchModeRequest])
def test_dispatch_modes_reject_unknown_values(request_type: type) -> None:
    with pytest.raises(ValidationError):
        request_type(mode="unknown")
