"""Adapter wrapping the oauth2_service singleton behind OAuth2ServiceProtocol."""

from typing import Any, Optional
from uuid import UUID

from airweave.core.protocols.oauth2 import OAuth2ServiceProtocol
from airweave.platform.auth.oauth2_service import oauth2_service


class OAuth2Service(OAuth2ServiceProtocol):
    """Delegates to the oauth2_service module-level singleton."""

    async def refresh_access_token(
        self,
        db: Any,
        integration_short_name: str,
        ctx: Any,
        connection_id: UUID,
        decrypted_credential: dict,
        config_fields: Optional[dict] = None,
    ) -> Any:
        return await oauth2_service.refresh_access_token(
            db,
            integration_short_name,
            ctx,
            connection_id,
            decrypted_credential,
            config_fields,
        )
