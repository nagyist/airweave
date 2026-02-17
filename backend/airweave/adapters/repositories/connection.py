"""Adapter wrapping crud.connection behind ConnectionRepositoryProtocol."""

from typing import Any, Optional
from uuid import UUID

from airweave import crud
from airweave.core.protocols.connection_repository import ConnectionRepositoryProtocol


class ConnectionRepository(ConnectionRepositoryProtocol):
    """Delegates to the crud.connection singleton."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        return await crud.connection.get(db, id, ctx)

    async def get_by_readable_id(self, db: Any, readable_id: str, ctx: Any) -> Optional[Any]:
        return await crud.connection.get_by_readable_id(db, readable_id=readable_id, ctx=ctx)
