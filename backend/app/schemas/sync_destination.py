"""Sync destination schemas."""

from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class SyncDestinationBase(BaseModel):
    """Base schema for SyncDestination."""

    connection_id: Optional[UUID] = None
    is_native: bool = False
    destination_type: str  # 'weaviate_native', 'neo4j_native', etc.


class SyncDestinationCreate(SyncDestinationBase):
    """Schema for creating a new SyncDestination."""

    sync_id: UUID


class SyncDestinationUpdate(SyncDestinationBase):
    """Schema for updating a SyncDestination."""

    pass


class SyncDestination(SyncDestinationBase):
    """Schema for a SyncDestination."""

    id: UUID
    sync_id: UUID

    class Config:
        """Pydantic config."""

        from_attributes = True
