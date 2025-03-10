"""CRUD for sync destinations."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import logger
from app.crud._base import CRUDBase
from app.models.sync_destination import SyncDestination
from app.schemas.sync_destination import SyncDestinationCreate, SyncDestinationUpdate
from app.schemas.user import User


class CRUDSyncDestination(CRUDBase[SyncDestination, SyncDestinationCreate, SyncDestinationUpdate]):
    """CRUD for sync destinations."""

    async def get_by_sync_id(self, db: AsyncSession, sync_id: UUID) -> list[SyncDestination]:
        """Get all destinations for a sync.

        Args:
            db (AsyncSession): The database session
            sync_id (UUID): The ID of the sync

        Returns:
            list[SyncDestination]: The destinations
        """
        stmt = select(SyncDestination).where(SyncDestination.sync_id == sync_id)
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def create_for_sync(
        self,
        db: AsyncSession,
        sync_id: UUID,
        destinations: list[SyncDestinationCreate],
        current_user: User,
    ) -> list[SyncDestination]:
        """Create multiple destinations for a sync.

        Args:
            db (AsyncSession): The database session
            sync_id (UUID): The ID of the sync
            destinations (list[SyncDestinationCreate]): The destinations to create
            current_user (User): The current user

        Returns:
            list[SyncDestination]: The created destinations
        """
        logger.info(f"Creating {len(destinations)} destinations for sync {sync_id}")
        for i, dest in enumerate(destinations):
            logger.info(
                f"Destination {i+1}: connection_id={dest.connection_id}, is_native={dest.is_native}, destination_type={dest.destination_type}"
            )

        db_objs = []
        for dest in destinations:
            dest_data = dest.model_dump()
            dest_data["sync_id"] = sync_id
            db_obj = SyncDestination(**dest_data)

            # Set organization and user tracking
            db_obj.organization_id = current_user.organization_id
            db_obj.created_by_email = current_user.email
            db_obj.modified_by_email = current_user.email

            db.add(db_obj)
            db_objs.append(db_obj)

        await db.flush()
        return db_objs


sync_destination = CRUDSyncDestination(SyncDestination)
