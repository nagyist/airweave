"""Legacy auth provider service -- [code blue] pending deletion.

Only get_runtime_auth_fields_for_source remains
(sole caller: TokenManager._refresh_via_auth_provider).
"""

from dataclasses import dataclass
from typing import List, Set

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

import airweave.core.container as _container_module
from airweave.core.logging import logger

auth_provider_logger = logger.with_prefix("Auth Provider Service: ").with_context(
    component="auth_provider_service"
)


@dataclass
class AuthFieldsResponse:
    """Auth fields required for a source."""

    all_fields: List[str]
    optional_fields: Set[str]

    @property
    def required_fields(self) -> Set[str]:
        """Get the set of required (non-optional) fields."""
        return set(self.all_fields) - self.optional_fields


class AuthProviderService:
    """Legacy service -- will be deleted once TokenManager is refactored."""

    async def get_runtime_auth_fields_for_source(
        self, db: AsyncSession, source_short_name: str
    ) -> AuthFieldsResponse:
        """Get the runtime auth fields required from an auth provider for a source.

        Returns precomputed auth field metadata from the source registry so auth
        providers can skip missing optional fields instead of hard-failing.

        Args:
            db: The database session (unused, kept for API compat)
            source_short_name: The short name of the source

        Returns:
            AuthFieldsResponse with all field names and optional field names.

        Raises:
            HTTPException: If source not found in registry
        """
        if _container_module.container is None:
            raise RuntimeError("Container not initialized")

        try:
            entry = _container_module.container.source_registry.get(source_short_name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Source '{source_short_name}' not found")

        auth_provider_logger.debug(
            f"Source '{source_short_name}' auth fields: {entry.runtime_auth_all_fields}, "
            f"optional: {entry.runtime_auth_optional_fields}"
        )
        return AuthFieldsResponse(
            all_fields=entry.runtime_auth_all_fields,
            optional_fields=entry.runtime_auth_optional_fields,
        )


# Singleton instance
auth_provider_service = AuthProviderService()
