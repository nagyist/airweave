"""Sync schemas."""

import re
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator

from airweave import schemas
from airweave.core.constants.native_connections import NATIVE_TEXT2VEC_UUID
from airweave.core.shared_models import SyncStatus


class SyncBase(BaseModel):
    """Base schema for Sync."""

    name: str
    source_connection_id: UUID  # system connection id
    embedding_model_connection_id: UUID = Field(default=NATIVE_TEXT2VEC_UUID)
    destination_connection_ids: list[UUID]
    description: Optional[str] = None
    cron_schedule: Optional[str] = None  # Actual cron expression
    next_scheduled_run: Optional[datetime] = None
    sync_metadata: Optional[dict] = None
    status: Optional[SyncStatus] = SyncStatus.ACTIVE

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
    cron_schedule: Optional[str] = None
    next_scheduled_run: Optional[datetime] = None
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


class SyncWithoutConnections(BaseModel):
    """Schema for Sync without connections."""

    name: str
    description: Optional[str] = None
    cron_schedule: Optional[str] = None
    next_scheduled_run: Optional[datetime] = None
    status: SyncStatus

    sync_metadata: Optional[dict] = None

    id: UUID
    organization_id: UUID
    created_at: datetime
    modified_at: datetime
    created_by_email: EmailStr
    modified_by_email: EmailStr

    class Config:
        """Pydantic config."""

        from_attributes = True


class SyncWithSourceConnection(SyncInDBBase):
    """Schema for Sync with source connection."""

    source_connection: Optional[schemas.Connection] = None
