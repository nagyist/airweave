"""Service for data synchronization."""

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core.dag_service import dag_service
from app.core.logging import logger
from app.db.session import get_db_context
from app.db.unit_of_work import UnitOfWork
from app.platform.sync.context import SyncContextFactory
from app.platform.sync.orchestrator import sync_orchestrator


class SyncService:
    """Main service for data synchronization."""

    async def create(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        current_user: schemas.User,
        uow: UnitOfWork,
    ) -> schemas.Sync:
        """Create a new sync."""
        # Create the sync record
        sync = await crud.sync.create(db=db, obj_in=sync, current_user=current_user, uow=uow)
        await uow.session.flush()

        # Create destinations if specified
        await self._handle_destination_connections(db, sync, uow)

        # Create initial DAG
        await dag_service.create_initial_dag(
            db=db, sync_id=sync.id, current_user=current_user, uow=uow
        )
        return sync

    async def _handle_destination_connections(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        uow: UnitOfWork,
    ) -> None:
        """Handle destination connections for a sync.

        This handles both the legacy single destination_connection_id and
        native destinations from sync_metadata.
        """
        destinations = []

        # Handle legacy destination_connection_id
        if sync.destination_connection_id:
            destinations.append(
                schemas.SyncDestinationCreate(
                    sync_id=sync.id,
                    connection_id=sync.destination_connection_id,
                    is_native=False,
                    destination_type="connection",
                )
            )

        # Handle native Weaviate if specified in sync_metadata
        if sync.sync_metadata and sync.sync_metadata.get("use_native_weaviate", False):
            destinations.append(
                schemas.SyncDestinationCreate(
                    sync_id=sync.id, is_native=True, destination_type="weaviate_native"
                )
            )

        # Handle native Neo4j if specified in sync_metadata
        if sync.sync_metadata and sync.sync_metadata.get("use_native_neo4j", False):
            destinations.append(
                schemas.SyncDestinationCreate(
                    sync_id=sync.id, is_native=True, destination_type="neo4j_native"
                )
            )

        # Create all destinations
        if destinations:
            await crud.sync_destination.create_for_sync(db, sync.id, destinations)

    async def run(
        self,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        dag: schemas.SyncDag,
        current_user: schemas.User,
    ) -> schemas.Sync:
        """Run a sync."""
        try:
            async with get_db_context() as db:
                sync_context = await SyncContextFactory.create(
                    db, sync, sync_job, dag, current_user
                )
            return await sync_orchestrator.run(sync_context)
        except Exception as e:
            logger.error(f"Error during sync: {e}")
            raise e


sync_service = SyncService()
