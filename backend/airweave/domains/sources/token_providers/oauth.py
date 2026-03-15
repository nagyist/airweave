"""OAuthTokenProvider — proactive token refresh for OAuth2 sources.

Thin wrapper: timer + lock + cache. The actual refresh is delegated to
``oauth2_service.refresh_and_persist()``.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from airweave.core.exceptions import TokenRefreshError
from airweave.core.logging import ContextualLogger
from airweave.domains.sources.exceptions import SourceAuthError, SourceTokenRefreshError
from airweave.domains.sources.token_providers.protocol import TokenProviderProtocol

if TYPE_CHECKING:
    from airweave.api.context import ApiContext
    from airweave.domains.oauth.protocols import OAuth2ServiceProtocol

_REFRESH_INTERVAL_SECONDS = 25 * 60


class OAuthTokenProvider(TokenProviderProtocol):
    """TokenProvider backed by OAuth2 refresh_token grant.

    Handles:
    - Proactive refresh before token expiry (25-min interval)
    - Concurrent-refresh prevention via asyncio.Lock
    - Token caching between refreshes

    All credential loading, decryption, and persistence is handled by
    ``oauth2_service.refresh_and_persist()``.
    """

    def __init__(
        self,
        initial_token: str,
        *,
        oauth2_service: OAuth2ServiceProtocol,
        source_short_name: str,
        connection_id: UUID,
        ctx: ApiContext,
        logger: ContextualLogger,
        config_fields: Optional[dict] = None,
        can_refresh: bool = True,
    ):
        """Initialize the OAuth token provider.

        Args:
            initial_token: The current access token.
            oauth2_service: Service that handles the actual refresh + persistence.
            source_short_name: Source identifier.
            connection_id: Connection UUID (passed to oauth2_service for refresh).
            ctx: API context (passed to oauth2_service for refresh).
            logger: Contextual logger with sync metadata.
            config_fields: Optional config fields for templated backend URLs.
            can_refresh: Whether refresh is possible (False if no refresh_token).
        """
        self._token = initial_token
        self._oauth2_service = oauth2_service
        self._source_short_name = source_short_name
        self._connection_id = connection_id
        self._ctx = ctx
        self._logger = logger
        self._config_fields = config_fields
        self._can_refresh = can_refresh
        self._last_refresh: float = 0
        self._lock = asyncio.Lock()

        if not self._can_refresh:
            self._logger.debug(
                f"Token refresh disabled for {self._source_short_name}: "
                "no refresh token in credentials"
            )

    # ------------------------------------------------------------------
    # TokenProvider protocol
    # ------------------------------------------------------------------

    async def get_token(self) -> str:
        """Return a valid access token, refreshing proactively if stale.

        Raises:
            SourceAuthError: On persistent refresh failure (disables further refresh).
        """
        if not self._can_refresh:
            return self._token

        now = time.time()
        if (now - self._last_refresh) < _REFRESH_INTERVAL_SECONDS:
            return self._token

        async with self._lock:
            now = time.time()
            if (now - self._last_refresh) < _REFRESH_INTERVAL_SECONDS:
                return self._token

            if self._last_refresh == 0:
                self._logger.info(f"Performing initial token refresh for {self._source_short_name}")
            else:
                self._logger.debug(
                    f"Refreshing token for {self._source_short_name} "
                    f"(last refresh: {now - self._last_refresh:.0f}s ago)"
                )

            try:
                self._token = await self._refresh()
                self._last_refresh = now
                return self._token
            except TokenRefreshError as e:
                raise SourceTokenRefreshError(
                    f"Token refresh failed for {self._source_short_name}: {e}",
                    source_short_name=self._source_short_name,
                ) from e

    async def force_refresh(self) -> str:
        """Force an immediate token refresh (e.g. after a 401).

        Raises:
            SourceAuthError: If refresh is not supported or fails.
        """
        if not self._can_refresh:
            raise SourceAuthError(
                f"Token refresh not supported for {self._source_short_name}",
                source_short_name=self._source_short_name,
            )

        async with self._lock:
            self._logger.warning(f"Forcing token refresh for {self._source_short_name} due to 401")
            try:
                self._token = await self._refresh()
                self._last_refresh = time.time()
                return self._token
            except TokenRefreshError as e:
                self._logger.error(f"Failed to refresh token for {self._source_short_name}: {e}")
                raise SourceTokenRefreshError(
                    f"Token refresh failed after 401: {e}",
                    source_short_name=self._source_short_name,
                ) from e

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    async def _refresh(self) -> str:
        """Delegate to oauth2_service.refresh_and_persist."""
        from airweave.db.session import get_db_context

        async with get_db_context() as db:
            return await self._oauth2_service.refresh_and_persist(
                db=db,
                integration_short_name=self._source_short_name,
                connection_id=self._connection_id,
                ctx=self._ctx,
                config_fields=self._config_fields,
            )

    # ------------------------------------------------------------------
    # Static helpers (used by lifecycle service at construction time)
    # ------------------------------------------------------------------

    @staticmethod
    def check_has_refresh_token(creds: object) -> bool:
        """Check if credentials contain a non-empty refresh token."""
        if isinstance(creds, dict):
            rt = creds.get("refresh_token")
            return bool(rt and str(rt).strip())
        if hasattr(creds, "refresh_token"):
            rt = creds.refresh_token
            return bool(rt and str(rt).strip())
        return False

    @staticmethod
    def extract_token(creds: object) -> Optional[str]:
        """Extract access token from credentials (str, dict, or object)."""
        if isinstance(creds, str):
            return creds
        if isinstance(creds, dict):
            return creds.get("access_token")
        if hasattr(creds, "access_token"):
            return creds.access_token
        return None
