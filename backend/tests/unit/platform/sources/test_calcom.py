"""Unit tests for Cal.com (calcom) source, auth config, and connector config."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from airweave.platform.configs.auth import CalComAuthConfig
from airweave.platform.configs.config import CalComConfig
from airweave.platform.sources.calcom import (
    CAL_BOOKINGS_API_VERSION,
    DEFAULT_CAL_API_BASE,
    CalSource,
)


@pytest.mark.asyncio
async def test_create_sets_default_base_url():
    source = await CalSource.create(CalComAuthConfig(api_key="cal_test_1234567890"), None)
    assert source.base_url == DEFAULT_CAL_API_BASE


@pytest.mark.asyncio
async def test_create_normalizes_host_and_accepts_host_without_scheme():
    source = await CalSource.create(
        "cal_test_1234567890",
        {"host": "cal.example.com/"},
    )
    assert source.base_url == "https://cal.example.com"


@pytest.mark.asyncio
async def test_get_with_auth_uses_configured_base_url():
    source = await CalSource.create("cal_test_1234567890", {"host": "https://cal.example.com"})

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value={"ok": True})

    client = MagicMock()
    client.get = AsyncMock(return_value=mock_response)

    result = await source._get_with_auth(client, "/v2/bookings", params={"take": 1})
    assert result == {"ok": True}
    client.get.assert_awaited_once()
    # URL is passed as the first positional argument
    called_url = client.get.call_args.args[0]
    assert called_url == "https://cal.example.com/v2/bookings"

    called_headers = client.get.call_args.kwargs["headers"]
    assert called_headers["Authorization"].startswith("Bearer ")


@pytest.mark.asyncio
async def test_list_bookings_requests_all_statuses():
    source = await CalSource.create("cal_test_1234567890", {"host": "https://cal.example.com"})

    async def fake_get_with_auth(_client, _path, params=None, headers=None):
        # Return no items to stop pagination.
        return {"data": [], "pagination": {"hasNextPage": False, "returnedItems": 0}}

    with patch.object(source, "_get_with_auth", new=AsyncMock(side_effect=fake_get_with_auth)) as m:
        async for _ in source._list_bookings(MagicMock(), after_updated_at="2026-01-01T00:00:00Z"):
            raise AssertionError("Expected no yielded items")

        # Verify we passed status filter for all statuses.
        assert m.await_count == 1
        _, kwargs = m.call_args
        params = kwargs["params"]
        assert params["status"] == "upcoming,recurring,past,cancelled,unconfirmed"
        assert params["afterUpdatedAt"] == "2026-01-01T00:00:00Z"
        assert kwargs["headers"]["cal-api-version"] == CAL_BOOKINGS_API_VERSION


@pytest.mark.asyncio
async def test_validate_uses_configured_host_in_ping_url():
    source = await CalSource.create("cal_test_1234567890", {"host": "https://cal.example.com"})

    with patch.object(source, "_validate_oauth2", new=AsyncMock(return_value=True)) as m:
        ok = await source.validate()
        assert ok is True
        m.assert_awaited_once()
        ping_url = m.call_args.kwargs["ping_url"]
        assert ping_url.startswith("https://cal.example.com/")


# ---------------------------------------------------------------------------
# CalComAuthConfig (auth.py coverage)
# ---------------------------------------------------------------------------


def test_calcom_auth_config_rejects_empty_api_key():
    """CalComAuthConfig.validate_api_key raises when api_key is empty."""
    with pytest.raises(ValidationError):
        CalComAuthConfig(api_key="")


def test_calcom_auth_config_rejects_whitespace_only_api_key():
    """CalComAuthConfig.validate_api_key raises when api_key is only whitespace."""
    with pytest.raises(ValidationError):
        CalComAuthConfig(api_key="   \t  ")


# ---------------------------------------------------------------------------
# CalComConfig.normalize_host (config.py coverage)
# ---------------------------------------------------------------------------


def test_calcom_config_default_host():
    """CalComConfig uses default host when not provided."""
    cfg = CalComConfig()
    assert cfg.host == "https://api.cal.com"


def test_calcom_config_normalize_host_none():
    """normalize_host returns default when host is None."""
    cfg = CalComConfig(host=None)
    assert cfg.host == "https://api.cal.com"


def test_calcom_config_normalize_host_non_string_raises():
    """normalize_host raises when host is not a string."""
    with pytest.raises(ValidationError):
        CalComConfig(host=123)


def test_calcom_config_normalize_host_empty_string_returns_default():
    """normalize_host returns default when host is empty or whitespace."""
    cfg = CalComConfig(host="   ")
    assert cfg.host == "https://api.cal.com"


def test_calcom_config_normalize_host_adds_https_when_no_scheme():
    """normalize_host adds https:// when host has no scheme."""
    cfg = CalComConfig(host="cal.example.com")
    assert cfg.host == "https://cal.example.com"


def test_calcom_config_normalize_host_strips_trailing_slash():
    """normalize_host strips trailing slash."""
    cfg = CalComConfig(host="https://cal.example.com/api/")
    assert cfg.host == "https://cal.example.com/api"
