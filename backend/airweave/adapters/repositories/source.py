"""Source repository."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.adapters.repositories.base_public import BasePublicRepository
from airweave.core.exceptions import NotFoundException
from airweave.core.protocols.repositories import SourceRepositoryProtocol


class SourceRepository(
    SourceRepositoryProtocol,
    BasePublicRepository[schemas.Source, schemas.SourceCreate, schemas.SourceUpdate],
):
    """Source repository."""

    async def get_by_short_name(self, db_session: AsyncSession, short_name: str) -> schemas.Source:
        """Get a source by short name."""
        result = await db_session.execute(
            select(self.model).where(self.model.short_name == short_name)
        )
        db_obj = result.unique().scalar_one_or_none()
        if not db_obj:
            raise NotFoundException(f"Object with short name {short_name} not found")
        return db_obj
