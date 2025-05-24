"""White label schema."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr


class WhiteLabelBase(BaseModel):
    """Base schema for WhiteLabel."""

    name: str
    source_short_name: str
    redirect_url: str
    client_id: str
    client_secret: str
    allowed_origins: str

    class Config:
        """Pydantic config for WhiteLabelBase."""

        from_attributes = True


class WhiteLabelCreate(WhiteLabelBase):
    """Schema for creating a WhiteLabel object."""


class WhiteLabelUpdate(BaseModel):
    """Schema for updating a WhiteLabel object."""

    name: Optional[str] = None
    redirect_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    allowed_origins: Optional[str] = None


class WhiteLabelInDBBase(WhiteLabelBase):
    """Base schema for WhiteLabel stored in DB."""

    id: UUID
    organization_id: UUID
    created_at: datetime
    modified_at: datetime
    created_by_email: EmailStr
    modified_by_email: EmailStr

    class Config:
        """Pydantic config for WhiteLabelInDBBase."""

        from_attributes = True


class WhiteLabel(WhiteLabelInDBBase):
    """Schema for WhiteLabel."""

    pass
