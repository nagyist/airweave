"""Sync schemas."""

import re
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, field_validator

from app import schemas
from app.core.shared_models import SyncStatus


class SyncBase(BaseModel):
    """Base schema for Sync."""

    name: str
    description: Optional[str] = None
    source_connection_id: UUID
    destination_connection_id: Optional[UUID] = None
    destination_connection_ids: Optional[List[UUID]] = None
    embedding_model_connection_id: Optional[UUID] = None
    cron_schedule: Optional[str] = None  # Actual cron expression
    white_label_id: Optional[UUID] = None
    white_label_user_identifier: Optional[str] = None
    sync_metadata: Optional[dict] = None

    @property
    def destination_connections(self) -> List[UUID]:
        """Get all destination connections.

        This property combines destination_connection_id and destination_connection_ids
        for backwards compatibility.
        """
        connections = []
        if self.destination_connection_id:
            connections.append(self.destination_connection_id)
        if self.destination_connection_ids:
            connections.extend(self.destination_connection_ids)
        return connections

    @property
    def use_native_weaviate(self) -> bool:
        """Check if the sync uses native Weaviate.

        Returns:
            bool: True if native Weaviate is used, False otherwise
        """
        if not self.sync_metadata:
            return False
        return self.sync_metadata.get("use_native_weaviate", False)

    @property
    def use_native_neo4j(self) -> bool:
        """Check if the sync uses native Neo4j.

        Returns:
            bool: True if native Neo4j is used, False otherwise
        """
        if not self.sync_metadata:
            return False
        return self.sync_metadata.get("use_native_neo4j", False)

    @field_validator("cron_schedule")
    def validate_cron_schedule(cls, v: str) -> str:
        """Validate cron schedule format.

        Format: * * * * *
        minute (0-59)
        hour (0-23)
        day of month (1-31)
        month (1-12 or JAN-DEC)
        day of week (0-6 or SUN-SAT)

        * * * * *
        │ │ │ │ │
        │ │ │ │ └─ Day of week (0-6 or SUN-SAT)
        │ │ │ └─── Month (1-12 or JAN-DEC)
        │ │ └───── Day of month (1-31)
        │ └─────── Hour (0-23)
        └───────── Minute (0-59)
        """
        if v is None:
            return None
        cron_pattern = r"^(\*|[0-9]{1,2}|[0-9]{1,2}-[0-9]{1,2}|[0-9]{1,2}/[0-9]{1,2}|[0-9]{1,2},[0-9]{1,2}|\*\/[0-9]{1,2}) (\*|[0-9]{1,2}|[0-9]{1,2}-[0-9]{1,2}|[0-9]{1,2}/[0-9]{1,2}|[0-9]{1,2},[0-9]{1,2}|\*\/[0-9]{1,2}) (\*|[0-9]{1,2}|[0-9]{1,2}-[0-9]{1,2}|[0-9]{1,2}/[0-9]{1,2}|[0-9]{1,2},[0-9]{1,2}|\*\/[0-9]{1,2}) (\*|[0-9]{1,2}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|[0-9]{1,2}-[0-9]{1,2}|[0-9]{1,2}/[0-9]{1,2}|[0-9]{1,2},[0-9]{1,2}|\*\/[0-9]{1,2}) (\*|[0-6]|SUN|MON|TUE|WED|THU|FRI|SAT|[0-6]-[0-6]|[0-6]/[0-6]|[0-6],[0-6]|\*\/[0-6])$"  # noqa: E501
        if not re.match(cron_pattern, v):
            raise ValueError("Invalid cron schedule format")
        return v

    class Config:
        """Pydantic config."""

        from_attributes = True


class SyncCreate(SyncBase):
    """Schema for creating a Sync object."""

    run_immediately: bool = False

    def to_base(self) -> SyncBase:
        """Convert to base schema."""
        return SyncBase(**self.model_dump(exclude={"run_immediately"}))


class SyncUpdate(BaseModel):
    """Schema for updating a Sync object."""

    name: Optional[str] = None
    schedule: Optional[str] = None
    source_connection_id: Optional[UUID] = None
    destination_connection_id: Optional[UUID] = None
    embedding_model_connection_id: Optional[UUID] = None
    cron_schedule: Optional[str] = None
    white_label_id: Optional[UUID] = None
    white_label_user_identifier: Optional[str] = None
    sync_metadata: Optional[dict] = None
    status: Optional[SyncStatus] = None


class SyncInDBBase(SyncBase):
    """Base schema for Sync stored in DB."""

    id: UUID
    organization_id: UUID
    created_at: datetime
    modified_at: datetime
    created_by_email: EmailStr
    modified_by_email: EmailStr
    status: SyncStatus

    class Config:
        """Pydantic config."""

        from_attributes = True


class Sync(SyncInDBBase):
    """Schema for Sync."""

    pass


class SyncWithSourceConnection(SyncInDBBase):
    """Schema for Sync with source connection."""

    source_connection: Optional[schemas.Connection] = None
