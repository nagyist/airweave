"""Base public repository."""

from typing import Any, Generic, Optional, Type, Union
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.exceptions import NotFoundException
from airweave.core.protocols.repositories import (
    BasePublicRepositoryProtocol,
    CreateSchemaType,
    ModelType,
    UpdateSchemaType,
)
from airweave.db.unit_of_work import UnitOfWork


class BasePublicRepository(
    BasePublicRepositoryProtocol, Generic[ModelType, CreateSchemaType, UpdateSchemaType]
):
    """Base public repository."""

    def __init__(self, model: Type[ModelType]):
        """Initialize the base public repository."""
        self.model = model

    async def get(self, db_session: AsyncSession, id: UUID) -> ModelType:
        """Get a base by ID."""
        result = await db_session.execute(select(self.model).where(self.model.id == id))
        db_obj = result.unique().scalar_one_or_none()
        if not db_obj:
            raise NotFoundException(f"Object with ID {id} not found")
        return db_obj

    async def get_multi(
        self, db_session: AsyncSession, *, skip: int = 0, limit: int | None = None
    ) -> list[ModelType]:
        """Get multiple objects."""
        query = select(self.model).offset(skip)
        if limit:
            query = query.limit(limit)
        result = await db_session.execute(query)
        return list(result.unique().scalars().all())

    async def create(
        self,
        db_session: AsyncSession,
        *,
        obj_in: CreateSchemaType,
        uow: Optional[UnitOfWork] = None,
    ) -> ModelType:
        """Create an object."""
        if not isinstance(obj_in, dict):
            obj_in = obj_in.model_dump(exclude_unset=True)

        db_obj = self.model(**obj_in)
        db_session.add(db_obj)

        if not uow:
            await db_session.commit()
            await db_session.refresh(db_obj)

        return db_obj

    async def update(
        self,
        db_session: AsyncSession,
        *,
        db_obj: ModelType,
        obj_in: Union[UpdateSchemaType, dict[str, Any]],
        uow: Optional[UnitOfWork] = None,
    ) -> ModelType:
        """Update an object."""
        if not isinstance(obj_in, dict):
            obj_in = obj_in.model_dump(exclude_unset=True)

        for field, value in obj_in.items():
            setattr(db_obj, field, value)

        if not uow:
            await db_session.commit()
            await db_session.refresh(db_obj)

        return db_obj

    async def remove(
        self, db_session: AsyncSession, *, id: UUID, uow: Optional[UnitOfWork] = None
    ) -> ModelType:
        """Remove an object."""
        result = await db_session.execute(select(self.model).where(self.model.id == id))
        db_obj = result.unique().scalar_one_or_none()

        if db_obj is None:
            raise NotFoundException(f"Object with ID {id} not found")

        await db_session.delete(db_obj)

        if not uow:
            await db_session.commit()

        return db_obj
