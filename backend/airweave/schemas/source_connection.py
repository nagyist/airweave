"""Source connection schemas."""

import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator

from airweave.core.shared_models import SourceConnectionStatus, SyncJobStatus
from airweave.platform.configs._base import ConfigValues


class SourceConnectionBase(BaseModel):
    """Base schema for source connection."""

    name: str = Field(..., description="Name of the source connection", min_length=4, max_length=42)
    description: Optional[str] = None
    config_fields: Optional[ConfigValues] = None  # stored in core table
    short_name: str  # Short name of the source
    white_label_id: Optional[UUID] = None  # ID of the white label integration

    class Config:
        """Pydantic config for SourceConnectionBase."""

        from_attributes = True


class SourceConnectionCreate(SourceConnectionBase):
    """Schema for creating a source connection.

    Contains all fields that are required to create a source connection.
    - Sync specific fields are included here.
    """

    collection: Optional[str] = None
    cron_schedule: Optional[str] = None
    auth_fields: Optional[ConfigValues] = None
    credential_id: Optional[UUID] = None
    sync_immediately: bool = True

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

    def map_to_core_and_auxiliary_attributes(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Map the source connection create schema to core and auxiliary attributes.

        This separates the attributes in the schema into two groups:
        1. Core attributes: These are used to create the SourceConnection model directly
        2. Auxiliary attributes: These are used in the creation process but aren't part of the model

        Returns:
            A tuple containing (core_attributes, auxiliary_attributes)
        """
        data = self.model_dump(exclude_unset=True)

        # Auxiliary attributes used in the creation process but not directly in the model
        auxiliary_attrs = {
            "auth_fields": data.pop("auth_fields", None),
            "credential_id": data.pop("credential_id", None),
            "cron_schedule": data.pop("cron_schedule", None),
            "sync_immediately": data.pop("sync_immediately", True),
        }

        # Everything else is a core attribute for the SourceConnection model
        core_attrs = data

        return core_attrs, auxiliary_attrs


class SourceConnectionUpdate(BaseModel):
    """Schema for updating a source connection."""

    name: Optional[str] = Field(
        None, description="Name of the source connection", min_length=4, max_length=42
    )
    description: Optional[str] = None
    auth_fields: Optional[Union[ConfigValues, str]] = None
    config_fields: Optional[ConfigValues] = None
    cron_schedule: Optional[str] = None
    connection_id: Optional[UUID] = None
    white_label_id: Optional[UUID] = None


class SourceConnectionInDBBase(SourceConnectionBase):
    """Core schema for source connection stored in DB."""

    id: UUID
    sync_id: Optional[UUID] = None
    organization_id: UUID
    created_at: datetime
    modified_at: datetime
    connection_id: Optional[UUID] = None  # ID of the underlying connection object
    collection: str
    white_label_id: Optional[UUID] = None
    created_by_email: EmailStr
    modified_by_email: EmailStr

    class Config:
        """Pydantic config for SourceConnectionInDBBase."""

        from_attributes = True


class SourceConnection(SourceConnectionInDBBase):
    """Schema for source connection."""

    # str if encrypted/masked, ConfigValues if not
    # comes from integration_credential
    auth_fields: Optional[Union[ConfigValues, str]] = None

    # Ephemeral status derived from the latest sync job
    status: Optional[SourceConnectionStatus] = None

    # sync job info
    latest_sync_job_status: Optional[SyncJobStatus] = None
    latest_sync_job_id: Optional[UUID] = None
    latest_sync_job_started_at: Optional[datetime] = None
    latest_sync_job_completed_at: Optional[datetime] = None
    latest_sync_job_error: Optional[str] = None

    # Sync schedule information
    cron_schedule: Optional[str] = None
    next_scheduled_run: Optional[datetime] = None

    @classmethod
    def from_orm_with_collection_mapping(cls, obj):
        """Create a SourceConnection from a source_connection ORM model."""
        # Convert to dict and filter out SQLAlchemy internal attributes
        obj_dict = {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}

        # Map the readable_collection_id to collection if needed
        if hasattr(obj, "readable_collection_id"):
            obj_dict["collection"] = obj.readable_collection_id

        return cls.model_validate(obj_dict)


class SourceConnectionListItem(BaseModel):
    """Simplified schema for source connection list item.

    This is a compact representation containing only core attributes
    directly from the source connection model.
    """

    id: UUID
    name: str
    description: Optional[str] = None
    short_name: str
    status: SourceConnectionStatus
    created_at: datetime
    modified_at: datetime
    sync_id: UUID
    collection: str
    white_label_id: Optional[UUID] = None

    class Config:
        """Pydantic config for SourceConnectionListItem."""

        from_attributes = True
