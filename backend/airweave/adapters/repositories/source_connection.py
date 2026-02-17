"""Adapter wrapping crud.source_connection behind SourceConnectionRepositoryProtocol."""

from typing import Any, Optional
from uuid import UUID

from airweave import crud
from airweave.core.protocols.source_connection_repository import (
    SourceConnectionRepositoryProtocol,
)


class SourceConnectionRepository(SourceConnectionRepositoryProtocol):
    """Delegates to the crud.source_connection singleton."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        return await crud.source_connection.get(db, id, ctx)
