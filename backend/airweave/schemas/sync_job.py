"""SyncJob schema."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field

from airweave.models.sync_job import SyncJobStatus


class SyncJobBase(BaseModel):
    """Base schema for SyncJob."""

    sync_id: UUID
    status: SyncJobStatus = SyncJobStatus.PENDING
    entities_inserted: Optional[int] = 0
    entities_updated: Optional[int] = 0
    entities_deleted: Optional[int] = 0
    entities_kept: Optional[int] = 0
    entities_skipped: Optional[int] = 0
    entities_encountered: Optional[dict[str, int]] = {}
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error: Optional[str] = None
    access_token: Optional[str] = None

    class Config:
        """Pydantic config for SyncJobBase."""

        from_attributes = True


class SyncJobCreate(SyncJobBase):
    """Schema for creating a SyncJob object."""

    pass


class SyncJobUpdate(BaseModel):
    """Schema for updating a SyncJob object."""

    status: Optional[SyncJobStatus] = None
    entities_inserted: Optional[int] = None
    entities_updated: Optional[int] = None
    entities_deleted: Optional[int] = None
    entities_kept: Optional[int] = None
    entities_skipped: Optional[int] = None
    entities_encountered: Optional[dict[str, int]] = {}
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error: Optional[str] = None


class SyncJobInDBBase(SyncJobBase):
    """Base schema for SyncJob stored in DB."""

    id: UUID
    organization_id: UUID
    created_by_email: EmailStr
    modified_by_email: EmailStr
    created_at: datetime
    modified_at: datetime
    sync_name: Optional[str] = Field(
        None, description="Name of the sync, populated from join query"
    )

    class Config:
        """Pydantic config for SyncJobInDBBase."""

        from_attributes = True


class SyncJob(SyncJobInDBBase):
    """Schema for SyncJob."""

    def to_source_connection_job(self, source_connection_id: UUID) -> "SourceConnectionJob":
        """Convert SyncJob to SourceConnectionJob."""
        return SourceConnectionJob(
            source_connection_id=source_connection_id,
            id=self.id,
            organization_id=self.organization_id,
            created_by_email=self.created_by_email,
            modified_by_email=self.modified_by_email,
            created_at=self.created_at,
            modified_at=self.modified_at,
            status=self.status,
            entities_inserted=self.entities_inserted,
            entities_updated=self.entities_updated,
            entities_deleted=self.entities_deleted,
            entities_kept=self.entities_kept,
            entities_skipped=self.entities_skipped,
            entities_encountered=self.entities_encountered,
            started_at=self.started_at,
            completed_at=self.completed_at,
            failed_at=self.failed_at,
            error=self.error,
        )


class SourceConnectionJob(BaseModel):
    """Schema for SourceConnectionJob.

    This is a public schema that is used to return sync jobs for a source connection.
    Sync / sync jobs are system tables, and are not exposed to the public API.
    """

    source_connection_id: UUID
    id: UUID
    organization_id: UUID
    created_by_email: EmailStr
    modified_by_email: EmailStr
    created_at: datetime
    modified_at: datetime
    status: SyncJobStatus = SyncJobStatus.PENDING
    entities_inserted: Optional[int] = 0
    entities_updated: Optional[int] = 0
    entities_deleted: Optional[int] = 0
    entities_kept: Optional[int] = 0
    entities_skipped: Optional[int] = 0
    entities_encountered: Optional[dict[str, int]] = {}
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error: Optional[str] = None
