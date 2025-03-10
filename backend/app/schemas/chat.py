"""Chat schemas."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ChatMessageBase(BaseModel):
    """Base schema for chat messages."""

    content: str
    role: Optional[str] = Field(default="user")


class ChatMessageCreate(ChatMessageBase):
    """Schema for creating a chat message."""

    pass


class ChatMessage(ChatMessageBase):
    """Schema for chat message responses."""

    id: UUID
    chat_id: UUID
    tokens_used: Optional[int] = None
    created_at: datetime
    modified_at: datetime
    created_by_email: str
    modified_by_email: str
    organization_id: UUID

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
        },
    )


class ChatBase(BaseModel):
    """Base schema for chats."""

    name: str
    sync_id: UUID
    description: Optional[str] = None
    model_name: Optional[str] = "gpt-4o"
    model_settings: Optional[dict] = {
        "temperature": 0.7,
        "max_tokens": 1000,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0,
        "search_type": "vector",  # Default to vector search
    }
    search_settings: dict = {}


class ChatCreate(ChatBase):
    """Schema for creating a chat."""

    pass


class ChatUpdate(BaseModel):
    """Schema for updating a chat."""

    name: Optional[str] = None
    description: Optional[str] = None
    model_settings: Optional[dict] = None
    search_settings: Optional[dict] = None


class Chat(ChatBase):
    """Schema for chat responses."""

    id: UUID
    messages: list[ChatMessage] = []  # Default empty list
    created_at: datetime
    modified_at: datetime
    created_by_email: str
    modified_by_email: str
    organization_id: UUID

    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
        },
    )

    # Add alias for modified_at to match updated_at in response
    @property
    def updated_at(self) -> str:
        """Alias for modified_at to maintain compatibility."""
        return self.modified_at
