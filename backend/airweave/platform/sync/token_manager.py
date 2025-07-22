"""Token manager for handling OAuth2 token refresh during sync operations."""

import asyncio
import time
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.core import credentials
from airweave.core.exceptions import TokenRefreshError
from airweave.core.logging import logger
from airweave.platform.auth.services import oauth2_service
from airweave.schemas.auth import AuthContext


class TokenManager:
    """Manages OAuth2 token refresh for sources during sync operations.

    This class provides centralized token management to ensure sources always
    have valid access tokens during long-running sync jobs. It handles:
    - Automatic token refresh before expiry
    - Concurrent refresh prevention
    - White label support
    - Direct token injection scenarios
    - Auth provider token refresh
    """

    # Token refresh interval (25 minutes to be safe with 1-hour tokens)
    REFRESH_INTERVAL_SECONDS = 25 * 60

    def __init__(
        self,
        db: AsyncSession,
        source_short_name: str,
        source_connection: schemas.SourceConnection,
        auth_context: AuthContext,
        initial_credentials: Any,
        white_label: Optional[schemas.WhiteLabel] = None,
        is_direct_injection: bool = False,
        logger_instance=None,
        auth_provider_instance: Optional[Any] = None,
    ):
        """Initialize the token manager.

        Args:
            db: Database session
            source_short_name: Short name of the source
            source_connection: Source connection configuration
            auth_context: Authentication context
            initial_credentials: The initial credentials (dict, string token, or auth config object)
            white_label: Optional white label configuration
            is_direct_injection: Whether token was directly injected (no refresh)
            logger_instance: Optional logger instance for contextual logging
            auth_provider_instance: Optional auth provider instance for token refresh
        """
        self.db = db
        self.source_short_name = source_short_name
        self.connection_id = source_connection.id
        self.integration_credential_id = source_connection.integration_credential_id
        self.auth_context = auth_context

        self.white_label_source_short_name = white_label.source_short_name if white_label else None
        self.white_label_client_id = white_label.client_id if white_label else None
        self.white_label_client_secret = white_label.client_secret if white_label else None

        self.is_direct_injection = is_direct_injection
        self.logger = logger_instance or logger

        # Auth provider instance
        self.auth_provider_instance = auth_provider_instance

        # Extract the token from credentials
        self._current_token = self._extract_token_from_credentials(initial_credentials)
        if not self._current_token:
            raise ValueError(
                f"No token found in credentials for source '{source_short_name}'. "
                f"TokenManager requires a token to manage."
            )

        self._last_refresh_time = time.time()
        self._refresh_lock = asyncio.Lock()

        # For sources without refresh tokens, we can't refresh
        self._can_refresh = self._determine_refresh_capability()

    def _determine_refresh_capability(self) -> bool:
        """Determine if this source supports token refresh."""
        # Direct injection tokens should not be refreshed
        if self.is_direct_injection:
            return False

        # If auth provider instance is available, we can always refresh through it
        if self.auth_provider_instance:
            return True

        # For standard OAuth (without auth provider), we assume refresh is possible
        # The actual refresh capability will be determined when attempting refresh
        return True

    async def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        This method ensures the token is fresh and handles refresh logic
        with proper concurrency control.

        Returns:
            A valid access token

        Raises:
            TokenRefreshError: If token refresh fails
        """
        # If we can't refresh, just return the current token
        if not self._can_refresh:
            return self._current_token

        # Check if token needs refresh (proactive refresh before expiry)
        current_time = time.time()
        time_since_refresh = current_time - self._last_refresh_time

        if time_since_refresh < self.REFRESH_INTERVAL_SECONDS:
            return self._current_token

        # Token needs refresh - use lock to prevent concurrent refreshes
        async with self._refresh_lock:
            # Double-check after acquiring lock (another worker might have refreshed)
            current_time = time.time()
            time_since_refresh = current_time - self._last_refresh_time

            if time_since_refresh < self.REFRESH_INTERVAL_SECONDS:
                return self._current_token

            # Perform the refresh
            self.logger.info(
                f"Refreshing token for {self.source_short_name} "
                f"(last refresh: {time_since_refresh:.0f}s ago)"
            )

            try:
                new_token = await self._refresh_token()
                self._current_token = new_token
                self._last_refresh_time = current_time

                self.logger.info(f"Successfully refreshed token for {self.source_short_name}")
                return new_token

            except Exception as e:
                self.logger.error(f"Failed to refresh token for {self.source_short_name}: {str(e)}")
                raise TokenRefreshError(f"Token refresh failed: {str(e)}") from e

    async def refresh_on_unauthorized(self) -> str:
        """Force a token refresh after receiving an unauthorized error.

        This method is called when a source receives a 401 error, indicating
        the token has expired unexpectedly.

        Returns:
            A fresh access token

        Raises:
            TokenRefreshError: If token refresh fails or is not supported
        """
        if not self._can_refresh:
            raise TokenRefreshError(f"Token refresh not supported for {self.source_short_name}")

        async with self._refresh_lock:
            self.logger.warning(
                f"Forcing token refresh for {self.source_short_name} due to 401 error"
            )

            try:
                new_token = await self._refresh_token()
                self._current_token = new_token
                self._last_refresh_time = time.time()

                self.logger.info(
                    f"Successfully refreshed token for {self.source_short_name} after 401"
                )
                return new_token

            except Exception as e:
                self.logger.error(
                    f"Failed to refresh token for {self.source_short_name} after 401: {str(e)}"
                )
                raise TokenRefreshError(f"Token refresh failed after 401: {str(e)}") from e

    async def _refresh_token(self) -> str:
        """Internal method to perform the actual token refresh.

        Returns:
            The new access token

        Raises:
            Exception: If refresh fails
        """
        # If auth provider instance is available, refresh through it
        if self.auth_provider_instance:
            return await self._refresh_via_auth_provider()

        # Otherwise use standard OAuth refresh
        return await self._refresh_via_oauth()

    async def _refresh_via_auth_provider(self) -> str:
        """Refresh token using auth provider instance.

        Returns:
            The new access token

        Raises:
            TokenRefreshError: If refresh fails
        """
        self.logger.info(
            f"Refreshing token via auth provider instance for source '{self.source_short_name}'"
        )

        try:
            # Get the runtime auth fields required by the source (excluding BYOC fields)
            from airweave.core.auth_provider_service import auth_provider_service

            source_auth_config_fields = (
                await auth_provider_service.get_runtime_auth_fields_for_source(
                    self.db, self.source_short_name
                )
            )

            # Get fresh credentials from auth provider instance
            fresh_credentials = await self.auth_provider_instance.get_creds_for_source(
                source_short_name=self.source_short_name,
                source_auth_config_fields=source_auth_config_fields,
            )

            # Extract access token
            access_token = fresh_credentials.get("access_token")
            if not access_token:
                raise TokenRefreshError("No access token in credentials from auth provider")

            # Update the stored credentials in the database
            if self.integration_credential_id:
                credential_update = schemas.IntegrationCredentialUpdate(
                    encrypted_credentials=credentials.encrypt(fresh_credentials)
                )
                await crud.integration_credential.update_by_id(
                    self.db,
                    id=self.integration_credential_id,
                    obj_in=credential_update,
                    auth_context=self.auth_context,
                )

            return access_token

        except Exception as e:
            self.logger.error(f"Failed to refresh token via auth provider instance: {str(e)}")
            raise TokenRefreshError(f"Auth provider refresh failed: {str(e)}") from e

    async def _refresh_via_oauth(self) -> str:
        """Refresh token using standard OAuth flow.

        Returns:
            The new access token

        Raises:
            TokenRefreshError: If refresh fails
        """
        # Get the stored credentials
        if not self.integration_credential_id:
            raise TokenRefreshError("No integration credential found for token refresh")

        credential = await crud.integration_credential.get(
            self.db, self.integration_credential_id, self.auth_context
        )
        if not credential:
            raise TokenRefreshError("Integration credential not found")

        decrypted_credential = credentials.decrypt(credential.encrypted_credentials)

        # Reconstruct white_label object only if we have white label values
        white_label = None
        if self.white_label_source_short_name:
            # Create a minimal white label object with only the fields needed by oauth2_service
            white_label = type(
                "WhiteLabel",
                (),
                {
                    "source_short_name": self.white_label_source_short_name,
                    "client_id": self.white_label_client_id,
                    "client_secret": self.white_label_client_secret,
                },
            )()

        # Use the oauth2_service to refresh the token
        oauth2_response = await oauth2_service.refresh_access_token(
            db=self.db,
            integration_short_name=self.source_short_name,
            auth_context=self.auth_context,
            connection_id=self.connection_id,
            decrypted_credential=decrypted_credential,
            white_label=white_label,
        )

        return oauth2_response.access_token

    def _extract_token_from_credentials(self, credentials: Any) -> Optional[str]:
        """Extract OAuth access token from credentials.

        This method only handles OAuth tokens, not API keys or other auth types.
        """
        # If it's already a string, assume it's the token
        if isinstance(credentials, str):
            return credentials

        # If it's a dict, look for access_token (OAuth standard)
        if isinstance(credentials, dict):
            return credentials.get("access_token")

        # If it's an object with attributes, try to get access_token
        if hasattr(credentials, "access_token"):
            return credentials.access_token

        return None
