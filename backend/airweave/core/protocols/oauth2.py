"""Protocol for OAuth2 token operations."""

from typing import Any, Optional, Protocol
from uuid import UUID


class OAuth2ServiceProtocol(Protocol):
    """OAuth2 token refresh capability."""

    async def refresh_access_token(
        self,
        db: Any,
        integration_short_name: str,
        ctx: Any,
        connection_id: UUID,
        decrypted_credential: dict,
        config_fields: Optional[dict] = None,
    ) -> Any:
        """Refresh an OAuth2 access token.

        Returns an object with an `access_token` attribute.
        """
        ...
