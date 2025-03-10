"""Sync schemas."""

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, field_validator

from app import schemas
from app.core.shared_models import SyncStatus

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from app.schemas.sync_destination import SyncDestination


class SyncBase(BaseModel):
    """Base schema for Sync."""

    name: str
    description: Optional[str] = None
    source_connection_id: UUID
    destination_connection_id: Optional[UUID] = None
    embedding_model_connection_id: Optional[UUID] = None
    cron_schedule: Optional[str] = None  # Actual cron expression
    white_label_id: Optional[UUID] = None
    white_label_user_identifier: Optional[str] = None
    sync_metadata: Optional[dict] = None

    @property
    def destination_connections(self) -> List[UUID]:
        """Get all destination connections.

        This property returns the destination_connection_id
        for backwards compatibility.
        """
        connections = []
        if self.destination_connection_id:
            connections.append(self.destination_connection_id)
        return connections

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

    # Additional fields for the response
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None

    # Destination relationships - use string literal for type annotation to avoid circular imports
    destinations: Optional[List["SyncDestination"]] = None

    def has_destination_type(self, destination_type: str) -> bool:
        """Check if the sync has a destination of the specified type.

        Args:
            destination_type: The type of destination to check for

        Returns:
            bool: True if the sync has a destination of the specified type
        """
        if not self.destinations:
            return False

        return any(d.destination_type == destination_type for d in self.destinations)

    @property
    def native_destinations(self) -> List[Any]:  # Use Any to avoid circular imports
        """Get all native destinations for this sync.

        Returns:
            List[SyncDestination]: List of native destinations
        """
        if not self.destinations:
            return []
        return [d for d in self.destinations if d.is_native]

    @property
    def connection_destinations(self) -> List[Any]:  # Use Any to avoid circular imports
        """Get all connection-based destinations for this sync.

        Returns:
            List[SyncDestination]: List of connection destinations
        """
        if not self.destinations:
            return []
        return [d for d in self.destinations if not d.is_native]


class SyncWithSourceConnection(SyncInDBBase):
    """Schema for Sync with source connection."""

    source_connection: Optional[schemas.Connection] = None


# Import SyncDestination after all classes are defined to avoid circular imports
from app.schemas.sync_destination import SyncDestination  # noqa

# Update forward references
Sync.update_forward_refs()
