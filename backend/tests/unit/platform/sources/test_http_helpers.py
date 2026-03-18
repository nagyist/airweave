"""Tests for http_helpers — HTTP response → domain exception translation."""

import httpx
import pytest

from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceEntityForbiddenError,
    SourceEntityNotFoundError,
    SourceError,
    SourceRateLimitError,
    SourceServerError,
)
from airweave.platform.sources.http_helpers import raise_for_status


def _response(status: int, body: str = "", headers: dict | None = None) -> httpx.Response:
    """Build a fake httpx.Response for testing."""
    return httpx.Response(
        status_code=status,
        text=body,
        headers=headers or {},
        request=httpx.Request("GET", "https://api.example.com/test"),
    )


class TestRaiseForStatus:
    """Tests for raise_for_status()."""

    def test_2xx_does_not_raise(self):
        raise_for_status(_response(200))
        raise_for_status(_response(201))
        raise_for_status(_response(204))

    def test_401_raises_source_auth_error(self):
        with pytest.raises(SourceAuthError, match="401"):
            raise_for_status(_response(401))

    def test_403_without_entity_id_raises_auth_error(self):
        with pytest.raises(SourceAuthError, match="403"):
            raise_for_status(_response(403))

    def test_403_with_entity_id_raises_entity_forbidden(self):
        with pytest.raises(SourceEntityForbiddenError) as exc_info:
            raise_for_status(_response(403), entity_id="item-1")
        assert exc_info.value.entity_id == "item-1"

    def test_404_without_entity_id_raises_source_error(self):
        with pytest.raises(SourceError, match="404"):
            raise_for_status(_response(404))

    def test_404_with_entity_id_raises_entity_not_found(self):
        with pytest.raises(SourceEntityNotFoundError) as exc_info:
            raise_for_status(_response(404), entity_id="item-2")
        assert exc_info.value.entity_id == "item-2"

    def test_429_raises_rate_limit_error(self):
        resp = _response(429, headers={"Retry-After": "15"})
        with pytest.raises(SourceRateLimitError) as exc_info:
            raise_for_status(resp)
        assert exc_info.value.retry_after == 15.0

    def test_429_default_retry_after(self):
        with pytest.raises(SourceRateLimitError) as exc_info:
            raise_for_status(_response(429))
        assert exc_info.value.retry_after == 30.0

    def test_500_raises_server_error(self):
        with pytest.raises(SourceServerError) as exc_info:
            raise_for_status(_response(500))
        assert exc_info.value.status_code == 500

    def test_502_raises_server_error(self):
        with pytest.raises(SourceServerError) as exc_info:
            raise_for_status(_response(502))
        assert exc_info.value.status_code == 502

    def test_301_without_entity_id_raises_source_error(self):
        resp = _response(301, headers={"Location": "https://new.example.com"})
        with pytest.raises(SourceError, match="redirect"):
            raise_for_status(resp)

    def test_400_generic_raises_source_error(self):
        with pytest.raises(SourceError, match="400"):
            raise_for_status(_response(400, body="bad request"))

    def test_400_zoho_rate_limit_disguised(self):
        body = '{"error_description": "You have made too many requests", "error": "Access Denied"}'
        with pytest.raises(SourceRateLimitError, match="disguised"):
            raise_for_status(_response(400, body=body))

    def test_source_short_name_propagated(self):
        with pytest.raises(SourceAuthError) as exc_info:
            raise_for_status(_response(401), source_short_name="asana")
        assert exc_info.value.source_short_name == "asana"

    def test_context_in_message(self):
        with pytest.raises(SourceAuthError, match="fetching projects"):
            raise_for_status(_response(401), context="fetching projects")
