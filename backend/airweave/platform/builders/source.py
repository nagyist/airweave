"""Source context builder for sync operations.

Handles sync-specific orchestration on top of SourceLifecycleService:
- Resolves source_connection from sync
- Snapshot guard
- File downloader setup
- Cursor creation
- ARF replay mode
"""

from typing import Any, List, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api.context import ApiContext
from airweave.core.exceptions import NotFoundException
from airweave.core.logging import ContextualLogger
from airweave.domains.browse_tree.repository import NodeSelectionRepository
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.sources.protocols import SourceLifecycleServiceProtocol
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.domains.syncs.cursors.service import SyncCursorService
from airweave.platform.contexts.infra import InfraContext
from airweave.platform.contexts.source import SourceContext
from airweave.platform.sources._base import BaseSource
from airweave.platform.sync.config import SyncConfig


class SourceContextBuilder:
    """Builds source context with all required configuration.

    All dependencies injected via constructor — no container imports.
    """

    def __init__(
        self,
        source_lifecycle_service: SourceLifecycleServiceProtocol,
        sync_cursor_service: SyncCursorService,
        storage_backend=None,
    ) -> None:
        """Initialize with injected dependencies.

        Args:
            source_lifecycle_service: Creates configured source instances.
            sync_cursor_service: Cursor CRUD operations.
            storage_backend: Storage backend for file downloads and ARF replay.
        """
        self._source_lifecycle = source_lifecycle_service
        self._sync_cursor_service = sync_cursor_service
        self._storage_backend = storage_backend

    async def build(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        infra: InfraContext,
        access_token: Optional[str] = None,
        force_full_sync: bool = False,
        execution_config: Optional[SyncConfig] = None,
    ) -> SourceContext:
        """Build complete source context.

        Args:
            db: Database session
            sync: Sync configuration
            sync_job: The sync job (needed for file downloader)
            infra: Infrastructure context (provides ctx and logger)
            access_token: Optional direct token (skips credential loading)
            force_full_sync: If True, skip cursor loading
            execution_config: Optional execution config

        Returns:
            SourceContext with configured source and cursor.
        """
        ctx = infra.ctx
        logger = infra.logger

        if execution_config and execution_config.behavior.replay_from_arf:
            return await self._build_arf_replay_context(
                db=db,
                sync=sync,
                infra=infra,
                execution_config=execution_config,
            )

        source_connection_obj = await crud.source_connection.get_by_sync_id(
            db, sync_id=sync.id, ctx=ctx
        )
        if not source_connection_obj:
            raise NotFoundException(
                f"Source connection record not found for sync {sync.id}. "
                f"This typically occurs when a source connection is deleted while a "
                f"scheduled workflow is queued. The workflow should self-destruct and "
                f"clean up orphaned schedules."
            )

        self._validate_not_completed_snapshot(source_connection_obj)

        source = await self._source_lifecycle.create(
            db=db,
            source_connection_id=UUID(str(source_connection_obj.id)),
            ctx=ctx,
            access_token=access_token,
        )

        self._setup_file_downloader(source, sync_job, logger)

        cursor = await self._create_cursor(
            db=db,
            sync=sync,
            source_class=type(source),
            ctx=ctx,
            logger=logger,
            force_full_sync=force_full_sync,
            execution_config=execution_config,
        )

        source.set_cursor(cursor)

        node_selections = await self._load_node_selections(
            db, UUID(str(source_connection_obj.id)), ctx
        )
        if node_selections:
            source.set_node_selections(node_selections)
            logger.info(f"Loaded {len(node_selections)} node selections for targeted sync")

        return SourceContext(source=source, cursor=cursor)

    async def _build_arf_replay_context(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        infra: InfraContext,
        execution_config: SyncConfig,
    ) -> SourceContext:
        """Build source context for ARF replay mode."""
        from airweave.domains.arf.replay_source import ArfReplaySource

        ctx = infra.ctx
        logger = infra.logger

        source_connection = await crud.source_connection.get_by_sync_id(
            db, sync_id=sync.id, ctx=ctx
        )
        original_short_name = source_connection.short_name if source_connection else None

        logger.info(
            f"ARF Replay mode: Creating ArfReplaySource for sync {sync.id} "
            f"(masquerading as '{original_short_name}')"
        )

        source = await ArfReplaySource.create(
            sync_id=sync.id,
            storage=self._storage_backend,
            logger=logger,
            restore_files=True,
            original_short_name=original_short_name,
        )

        if hasattr(source, "set_logger"):
            source.set_logger(logger)

        if not await source.validate():
            raise NotFoundException(
                f"ARF data not found for sync {sync.id}. "
                f"Cannot replay - ensure ARF capture was enabled for previous syncs."
            )

        cursor = SyncCursor(
            sync_id=sync.id,
            cursor_schema=None,
            cursor_data=None,
        )

        return SourceContext(source=source, cursor=cursor)

    @staticmethod
    async def get_source_connection_id(
        db: AsyncSession,
        sync: schemas.Sync,
        ctx: ApiContext,
    ) -> UUID:
        """Get user-facing source connection ID for logging and scoping."""
        source_connection_obj = await crud.source_connection.get_by_sync_id(
            db, sync_id=sync.id, ctx=ctx
        )
        if not source_connection_obj:
            raise NotFoundException(f"Source connection record not found for sync {sync.id}")
        return UUID(str(source_connection_obj.id))

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _validate_not_completed_snapshot(source_connection_obj) -> None:
        """Guard: completed snapshots that had their short_name restored cannot re-sync."""
        if source_connection_obj.short_name != "snapshot":
            from pydantic import ValidationError

            from airweave.platform.configs.config import SnapshotConfig

            try:
                SnapshotConfig(**(source_connection_obj.config_fields or {}))
                from airweave.platform.sync.exceptions import SyncFailureError

                raise SyncFailureError(
                    f"Cannot re-sync a completed snapshot source connection "
                    f"('{source_connection_obj.name}'). Snapshot data is immutable — "
                    f"create a new snapshot source connection instead."
                )
            except ValidationError:
                pass

    def _setup_file_downloader(
        self, source: BaseSource, sync_job: Optional[Any], logger: ContextualLogger
    ) -> None:
        """Setup file downloader for file-based sources."""
        from airweave.domains.storage.file_service import FileService

        if not sync_job or not hasattr(sync_job, "id"):
            raise ValueError(
                "sync_job is required for file downloader initialization. "
                "This method should only be called from create_orchestrator() "
                "where sync_job exists."
            )

        file_downloader = FileService(
            sync_job_id=sync_job.id,
            storage_backend=self._storage_backend,
        )
        source.set_file_downloader(file_downloader)
        logger.debug(
            f"File downloader configured for {source.__class__.__name__} "
            f"(sync_job_id: {sync_job.id})"
        )

    async def _create_cursor(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        source_class: type,
        ctx: ApiContext,
        logger: ContextualLogger,
        force_full_sync: bool,
        execution_config: Optional[SyncConfig],
    ) -> SyncCursor:
        """Create sync cursor with optional data loading."""
        cursor_schema = None
        if hasattr(source_class, "cursor_class") and source_class.cursor_class:
            cursor_schema = source_class.cursor_class
            logger.debug(f"Source has typed cursor: {cursor_schema.__name__}")

        if force_full_sync:
            logger.info(
                "FORCE FULL SYNC: Skipping cursor data to ensure all entities are fetched "
                "for accurate orphaned entity cleanup. Will still track cursor for next sync."
            )
            cursor_data = None
        elif execution_config and execution_config.cursor.skip_load:
            logger.info(
                "SKIP CURSOR LOAD: Fetching all entities "
                "(execution_config.cursor.skip_load=True)"
            )
            cursor_data = None
        else:
            cursor_data = await self._sync_cursor_service.get_cursor_data(
                db=db, sync_id=sync.id, ctx=ctx
            )
            if cursor_data:
                logger.info(f"Incremental sync: Using cursor data with {len(cursor_data)} keys")

        return SyncCursor(
            sync_id=sync.id,
            cursor_schema=cursor_schema,
            cursor_data=cursor_data,
        )

    @staticmethod
    async def _load_node_selections(
        db: AsyncSession,
        source_connection_id: UUID,
        ctx: ApiContext,
    ) -> List[NodeSelectionData]:
        """Load node selections for a source connection (for targeted sync)."""
        repo = NodeSelectionRepository()
        rows = await repo.get_by_source_connection(db, source_connection_id, ctx.organization.id)
        return [
            NodeSelectionData(
                source_node_id=row.source_node_id,
                node_type=row.node_type,
                node_title=row.node_title,
                node_metadata=row.node_metadata,
            )
            for row in rows
        ]
