"""Source connection repository wrapping crud.source_connection."""

from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.api.context import ApiContext
from airweave.models.source_connection import SourceConnection


class SourceConnectionRepository:
    """Delegates to the crud.source_connection singleton."""

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        """Get a source connection by ID within org scope."""
        return await crud.source_connection.get(db, id, ctx)

    async def get_schedule_info(
        self, db: AsyncSession, source_connection: SourceConnection
    ) -> Optional[Dict[str, Any]]:
        """Get schedule info for a source connection."""
        return await crud.source_connection.get_schedule_info(db, source_connection)
