"""Unit tests for TokenProvider implementations.

Tests:
- OAuthTokenProvider: timer/cache behavior, get_token, force_refresh
- StaticTokenProvider: get_token, force_refresh raises
- AuthProviderTokenProvider: delegates to auth provider
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from airweave.core.logging import ContextualLogger
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceTokenRefreshError,
)
from airweave.domains.sources.token_providers.auth_provider import AuthProviderTokenProvider
from airweave.domains.sources.token_providers.oauth import OAuthTokenProvider
from airweave.domains.sources.token_providers.static import StaticTokenProvider


def _mock_logger() -> ContextualLogger:
    """Create a mock ContextualLogger."""
    return MagicMock(spec=ContextualLogger)


def _oauth_provider(initial_token="test_token", can_refresh=False, **overrides):
    """Create an OAuthTokenProvider with sensible defaults."""
    return OAuthTokenProvider(
        initial_token=initial_token,
        oauth2_service=overrides.get("oauth2_service", MagicMock()),
        source_short_name=overrides.get("source_short_name", "test_source"),
        connection_id=overrides.get("connection_id", uuid4()),
        ctx=overrides.get("ctx", MagicMock()),
        logger=overrides.get("logger", _mock_logger()),
        config_fields=overrides.get("config_fields", None),
        can_refresh=can_refresh,
    )


# ---------------------------------------------------------------------------
# OAuthTokenProvider
# ---------------------------------------------------------------------------


class TestOAuthGetToken:
    """Tests for OAuthTokenProvider.get_token."""

    @pytest.mark.asyncio
    async def test_returns_token_when_no_refresh(self):
        """When can_refresh=False, get_token returns the initial token."""
        p = _oauth_provider("my_token", can_refresh=False)
        assert await p.get_token() == "my_token"

    @pytest.mark.asyncio
    async def test_returns_token_when_recently_refreshed(self):
        """When last refresh was recent, returns cached token without calling service."""
        mock_service = MagicMock()
        mock_service.refresh_and_persist = AsyncMock(return_value="refreshed")

        p = _oauth_provider("initial", can_refresh=True, oauth2_service=mock_service)

        token = await p.get_token()
        assert token == "refreshed"
        assert mock_service.refresh_and_persist.call_count == 1

        token2 = await p.get_token()
        assert token2 == "refreshed"
        assert mock_service.refresh_and_persist.call_count == 1

    @pytest.mark.asyncio
    async def test_raises_on_refresh_failure(self):
        """When refresh fails, raises SourceTokenRefreshError."""
        from airweave.core.exceptions import TokenRefreshError

        mock_service = MagicMock()
        mock_service.refresh_and_persist = AsyncMock(
            side_effect=TokenRefreshError("network error")
        )

        p = _oauth_provider("fallback_token", can_refresh=True, oauth2_service=mock_service)

        with pytest.raises(SourceTokenRefreshError):
            await p.get_token()


class TestOAuthForceRefresh:
    """Tests for OAuthTokenProvider.force_refresh."""

    @pytest.mark.asyncio
    async def test_raises_when_no_refresh(self):
        """force_refresh raises SourceAuthError when can_refresh=False."""
        p = _oauth_provider(can_refresh=False)
        with pytest.raises(SourceAuthError):
            await p.force_refresh()

    @pytest.mark.asyncio
    async def test_returns_fresh_token(self):
        """force_refresh calls service and returns new token."""
        mock_service = MagicMock()
        mock_service.refresh_and_persist = AsyncMock(return_value="forced_token")

        p = _oauth_provider("old", can_refresh=True, oauth2_service=mock_service)
        token = await p.force_refresh()
        assert token == "forced_token"

    @pytest.mark.asyncio
    async def test_raises_on_failure(self):
        """force_refresh raises SourceTokenRefreshError when service fails."""
        from airweave.core.exceptions import TokenRefreshError

        mock_service = MagicMock()
        mock_service.refresh_and_persist = AsyncMock(
            side_effect=TokenRefreshError("fail")
        )

        p = _oauth_provider("old", can_refresh=True, oauth2_service=mock_service)
        with pytest.raises(SourceTokenRefreshError):
            await p.force_refresh()


class TestOAuthHelpers:
    """Tests for OAuthTokenProvider static helpers."""

    def test_check_has_refresh_token_dict_present(self):
        assert OAuthTokenProvider.check_has_refresh_token(
            {"access_token": "at", "refresh_token": "rt"}
        ) is True

    def test_check_has_refresh_token_dict_missing(self):
        assert OAuthTokenProvider.check_has_refresh_token({"access_token": "at"}) is False

    def test_check_has_refresh_token_dict_empty(self):
        assert OAuthTokenProvider.check_has_refresh_token(
            {"access_token": "at", "refresh_token": ""}
        ) is False

    def test_check_has_refresh_token_dict_whitespace(self):
        assert OAuthTokenProvider.check_has_refresh_token(
            {"access_token": "at", "refresh_token": "   "}
        ) is False

    def test_check_has_refresh_token_object_present(self):
        class C:
            access_token = "at"
            refresh_token = "rt"
        assert OAuthTokenProvider.check_has_refresh_token(C()) is True

    def test_check_has_refresh_token_object_missing(self):
        class C:
            access_token = "at"
        assert OAuthTokenProvider.check_has_refresh_token(C()) is False

    def test_extract_token_string(self):
        assert OAuthTokenProvider.extract_token("raw_token") == "raw_token"

    def test_extract_token_dict(self):
        assert OAuthTokenProvider.extract_token({"access_token": "at"}) == "at"

    def test_extract_token_object(self):
        class C:
            access_token = "obj_at"
        assert OAuthTokenProvider.extract_token(C()) == "obj_at"

    def test_extract_token_none(self):
        assert OAuthTokenProvider.extract_token(42) is None


# ---------------------------------------------------------------------------
# StaticTokenProvider
# ---------------------------------------------------------------------------


class TestStaticTokenProvider:
    """Tests for StaticTokenProvider."""

    @pytest.mark.asyncio
    async def test_get_token_returns_value(self):
        p = StaticTokenProvider("api_key_123")
        assert await p.get_token() == "api_key_123"

    @pytest.mark.asyncio
    async def test_force_refresh_raises(self):
        p = StaticTokenProvider("api_key_123", source_short_name="attio")
        with pytest.raises(SourceAuthError):
            await p.force_refresh()

    def test_empty_token_raises(self):
        with pytest.raises(ValueError):
            StaticTokenProvider("")


# ---------------------------------------------------------------------------
# AuthProviderTokenProvider
# ---------------------------------------------------------------------------


class TestAuthProviderTokenProvider:
    """Tests for AuthProviderTokenProvider."""

    def _make_provider(self, access_token: str = "fresh_token"):
        mock_auth_provider = MagicMock()
        mock_auth_provider.get_creds_for_source = AsyncMock(
            return_value={"access_token": access_token}
        )

        mock_registry = MagicMock()
        entry = MagicMock()
        entry.runtime_auth_all_fields = ["access_token"]
        entry.runtime_auth_optional_fields = []
        mock_registry.get.return_value = entry

        return AuthProviderTokenProvider(
            auth_provider_instance=mock_auth_provider,
            source_short_name="test_source",
            source_registry=mock_registry,
            logger=_mock_logger(),
        )

    @pytest.mark.asyncio
    async def test_get_token_delegates_to_provider(self):
        p = self._make_provider("fresh_at")
        assert await p.get_token() == "fresh_at"

    @pytest.mark.asyncio
    async def test_force_refresh_same_as_get_token(self):
        p = self._make_provider("refreshed_at")
        assert await p.force_refresh() == "refreshed_at"
