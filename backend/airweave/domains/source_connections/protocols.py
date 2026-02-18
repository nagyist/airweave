"""Protocols for source connection domain."""

from typing import Any, Dict, Optional, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.models.source_connection import SourceConnection
from airweave.schemas.source_connection import (
    SourceConnection as SourceConnectionSchema,
)
from airweave.schemas.source_connection import (
    SourceConnectionJob,
    SourceConnectionListItem,
)


class SourceConnectionRepositoryProtocol(Protocol):
    """Data access for source connections.

    Wraps crud.source_connection for testability.
    """

    async def get(self, db: AsyncSession, id: UUID, ctx: ApiContext) -> Optional[SourceConnection]:
        """Get a source connection by ID within org scope."""
        ...

    async def get_schedule_info(
        self, db: AsyncSession, source_connection: SourceConnection
    ) -> Optional[Dict[str, Any]]:
        """Get schedule info for a source connection."""
        ...


class ResponseBuilderProtocol(Protocol):
    """Builds API response schemas for source connections."""

    async def build_response(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> SourceConnectionSchema:
        """Build full SourceConnection response from ORM object."""
        ...

    def build_list_item(self, data: Dict[str, Any]) -> SourceConnectionListItem:
        """Build a SourceConnectionListItem from a stats dict."""
        ...

    def map_sync_job(self, job: Any, source_connection_id: UUID) -> SourceConnectionJob:
        """Convert sync job to SourceConnectionJob schema."""
        ...
