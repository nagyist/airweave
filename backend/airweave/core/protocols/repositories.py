"""Protocols for repositories."""

from typing import Optional, Protocol, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.db.unit_of_work import UnitOfWork
from airweave.models._base import Base

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class BasePublicRepositoryProtocol(Protocol):
    """Protocol for base repositories."""

    async def get(self, db_session: AsyncSession, id: UUID) -> ModelType:
        """Get a base by ID."""
        ...

    async def get_multi(
        self, db_session: AsyncSession, *, skip: int = 0, limit: int | None = None
    ) -> list[ModelType]:
        """List all bases."""
        ...

    async def create(
        self,
        db_session: AsyncSession,
        *,
        obj_in: CreateSchemaType,
        uow: Optional[UnitOfWork] = None,
    ) -> ModelType:
        """Create a base."""
        ...

    async def update(
        self,
        db_session: AsyncSession,
        *,
        db_obj: ModelType,
        obj_in: UpdateSchemaType,
        uow: Optional[UnitOfWork] = None,
    ) -> ModelType:
        """Update a base."""
        ...

    async def remove(
        self, db_session: AsyncSession, *, id: UUID, uow: Optional[UnitOfWork] = None
    ) -> ModelType:
        """Delete a base."""
        ...


class SourceRepositoryProtocol(BasePublicRepositoryProtocol, Protocol):
    """Protocol for source repositories."""

    async def get_by_short_name(self, db_session: AsyncSession, short_name: str) -> schemas.Source:
        """Get a source by short name."""
        ...
