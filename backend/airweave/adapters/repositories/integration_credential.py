"""Adapter wrapping crud.integration_credential behind IntegrationCredentialRepositoryProtocol."""

from typing import Any, Optional
from uuid import UUID

from airweave import crud
from airweave.core.protocols.integration_credential_repository import (
    IntegrationCredentialRepositoryProtocol,
)


class IntegrationCredentialRepository(IntegrationCredentialRepositoryProtocol):
    """Delegates to the crud.integration_credential singleton."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        return await crud.integration_credential.get(db, id, ctx)
