"""Worker startup validation tests."""

import pytest

from agent_fleet import config
from agent_fleet.worker import run_worker


@pytest.mark.parametrize(
    ("gemini_key", "maps_key", "missing_name"),
    [
        (None, "maps-key", "GOOGLE_API_KEY"),
        ("gemini-key", None, "GOOGLE_MAPS_API_KEY"),
        ("your-gemini-api-key-here", "maps-key", "GOOGLE_API_KEY"),
    ],
)
async def test_worker_fails_fast_when_a_required_key_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    gemini_key: str | None,
    maps_key: str | None,
    missing_name: str,
) -> None:
    monkeypatch.setattr(config, "GOOGLE_API_KEY", gemini_key)
    monkeypatch.setattr(config, "GOOGLE_MAPS_API_KEY", maps_key)

    with pytest.raises(RuntimeError, match=missing_name):
        await run_worker()
