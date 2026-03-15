"""AuthProviderTokenProvider — delegates to Pipedream / Composio.

The auth provider is the source of truth for credentials. Every
``get_token()`` call fetches fresh credentials from the provider.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airweave.core.logging import ContextualLogger
from airweave.domains.sources.exceptions import SourceTokenRefreshError
from airweave.domains.sources.token_providers.protocol import TokenProviderProtocol
from airweave.platform.auth_providers._base import BaseAuthProvider

if TYPE_CHECKING:
    from airweave.domains.sources.protocols import SourceRegistryProtocol


class AuthProviderTokenProvider(TokenProviderProtocol):
    """TokenProvider backed by an external auth provider (Pipedream / Composio).

    In *direct* mode the auth provider holds the user's OAuth connection
    and can vend fresh access tokens on demand.
    """

    def __init__(
        self,
        auth_provider_instance: BaseAuthProvider,
        source_short_name: str,
        source_registry: SourceRegistryProtocol,
        *,
        logger: ContextualLogger,
    ):
        """Initialize with an auth provider instance.

        Args:
            auth_provider_instance: A ``BaseAuthProvider`` subclass instance.
            source_short_name: Source identifier.
            source_registry: Registry to look up runtime auth field names.
            logger: Contextual logger with sync metadata.
        """
        self._provider = auth_provider_instance
        self._source_short_name = source_short_name
        self._source_registry = source_registry
        self._logger = logger

    async def _fetch_token(self) -> str:
        """Call the auth provider and extract the access token.

        Raises:
            SourceTokenRefreshError: If the provider call fails or returns no token.
        """
        entry = self._source_registry.get(self._source_short_name)

        try:
            creds = await self._provider.get_creds_for_source(
                source_short_name=self._source_short_name,
                source_auth_config_fields=entry.runtime_auth_all_fields,
                optional_fields=entry.runtime_auth_optional_fields,
            )
        except Exception as e:
            raise SourceTokenRefreshError(
                f"Auth provider failed for {self._source_short_name}: {e}",
                source_short_name=self._source_short_name,
            ) from e

        if not isinstance(creds, dict) or "access_token" not in creds:
            raise SourceTokenRefreshError(
                f"No access_token in auth provider response for {self._source_short_name}",
                source_short_name=self._source_short_name,
            )

        return creds["access_token"]

    async def get_token(self) -> str:
        """Return a fresh token from the auth provider.

        Raises:
            SourceTokenRefreshError: If the provider call fails.
        """
        return await self._fetch_token()

    async def force_refresh(self) -> str:
        """Force-refresh by re-calling the auth provider.

        Auth providers always return the latest token, so this is
        identical to ``get_token()``.

        Raises:
            SourceTokenRefreshError: If the provider call fails.
        """
        return await self._fetch_token()
