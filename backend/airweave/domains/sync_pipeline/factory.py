"""Sync factory - builds orchestrator with SyncContext (data) and SyncRuntime (services).

The factory is responsible for:
1. Building SyncContext (data) via SyncContextBuilder
2. Building live services (source, destinations, trackers) via sub-builders
3. Building per-sync event emitter with subscribers (progress relay, billing)
4. Assembling SyncRuntime from the services
5. Wiring everything into SyncOrchestrator

Instance-based with injected deps (code blue architecture).
"""

import asyncio
import time
from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.core.context import BaseContext
from airweave.core.exceptions import NotFoundException
from airweave.core.logging import LoggerConfigurator, logger
from airweave.core.protocols.event_bus import EventBus
from airweave.domains.access_control.protocols import AccessControlMembershipRepositoryProtocol
from airweave.domains.arf.protocols import ArfServiceProtocol
from airweave.domains.embedders.protocols import DenseEmbedderProtocol, SparseEmbedderProtocol
from airweave.domains.entities.protocols import EntityRepositoryProtocol
from airweave.domains.source_connections.protocols import SourceConnectionRepositoryProtocol
from airweave.domains.sync_pipeline.access_control_dispatcher import ACActionDispatcher
from airweave.domains.sync_pipeline.access_control_pipeline import AccessControlPipeline
from airweave.domains.sync_pipeline.access_control_resolver import ACActionResolver
from airweave.domains.sync_pipeline.builders import SyncContextBuilder
from airweave.domains.sync_pipeline.builders.tracking import TrackingContextBuilder
from airweave.domains.sync_pipeline.config import SyncConfig, SyncConfigBuilder
from airweave.domains.sync_pipeline.contexts.runtime import SyncRuntime
from airweave.domains.sync_pipeline.entity_dispatcher_builder import EntityDispatcherBuilder
from airweave.domains.sync_pipeline.handlers import ACPostgresHandler
from airweave.domains.sync_pipeline.orchestrator import SyncOrchestrator
from airweave.domains.sync_pipeline.pipeline.acl_membership_tracker import ACLMembershipTracker
from airweave.domains.sync_pipeline.pipeline.entity_tracker import EntityTracker
from airweave.domains.sync_pipeline.protocols import ChunkEmbedProcessorProtocol
from airweave.domains.sync_pipeline.stream import AsyncSourceStream
from airweave.domains.sync_pipeline.worker_pool import AsyncWorkerPool
from airweave.domains.usage.protocols import UsageLimitCheckerProtocol

from .entity_action_resolver import EntityActionResolver
from .entity_pipeline import EntityPipeline


class SyncFactory:
    """Factory for sync orchestrator.

    Builds SyncContext (data), SyncRuntime (services), and wires them
    into the orchestrator and pipeline components.
    """

    def __init__(
        self,
        sc_repo: SourceConnectionRepositoryProtocol,
        event_bus: EventBus,
        usage_checker: UsageLimitCheckerProtocol,
        dense_embedder: DenseEmbedderProtocol,
        sparse_embedder: SparseEmbedderProtocol,
        entity_repo: EntityRepositoryProtocol,
        acl_repo: AccessControlMembershipRepositoryProtocol,
        processor: ChunkEmbedProcessorProtocol,
        arf_service: Optional[ArfServiceProtocol] = None,
    ) -> None:
        """Initialize with all required service and repository dependencies."""
        self._sc_repo = sc_repo
        self._event_bus = event_bus
        self._usage_checker = usage_checker
        self._dense_embedder = dense_embedder
        self._sparse_embedder = sparse_embedder
        self._entity_repo = entity_repo
        self._acl_repo = acl_repo
        self._processor = processor
        self._arf_service = arf_service

    async def create_orchestrator(
        self,
        db: AsyncSession,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        collection: schemas.CollectionRecord,
        connection: schemas.Connection,
        ctx: BaseContext,
        force_full_sync: bool = False,
        execution_config: Optional[SyncConfig] = None,
    ) -> SyncOrchestrator:
        """Create a dedicated orchestrator instance for a sync run."""
        init_start = time.time()
        logger.info("Creating sync orchestrator...")

        resolved_config = SyncConfigBuilder.build(
            collection_overrides=collection.sync_config,
            sync_overrides=sync.sync_config,
            job_overrides=sync_job.sync_config or execution_config,
        )
        logger.debug(
            f"Resolved layered sync config: handlers={resolved_config.handlers.model_dump()}, "
            f"destinations={resolved_config.destinations.model_dump()}"
        )

        # Direct repo call — replaces SyncContextBuilder -> SourceContextBuilder chain
        sc = await self._sc_repo.get_by_sync_id(db, sync_id=sync.id, ctx=ctx)
        if not sc:
            from airweave.core.exceptions import NotFoundException

            raise NotFoundException(f"Source connection record not found for sync {sync.id}")
        source_connection_id = sc.id

        source_result, destinations_result, entity_tracker_result = await asyncio.gather(
            self._build_source(
                db=db,
                sync=sync,
                sync_job=sync_job,
                ctx=ctx,
                force_full_sync=force_full_sync,
                execution_config=resolved_config,
            ),
            self._build_destinations(
                db=db,
                sync=sync,
                collection=collection,
                ctx=ctx,
                execution_config=resolved_config,
            ),
            self._build_tracking(
                db=db,
                sync=sync,
                sync_job=sync_job,
                ctx=ctx,
            ),
        )

        source, cursor, files, node_selections = source_result
        destinations, entity_map = destinations_result

        sync_context = await SyncContextBuilder.build(
            db=db,
            sync=sync,
            sync_job=sync_job,
            collection=collection,
            connection=connection,
            ctx=ctx,
            source_connection_id=source_connection_id,
            source_short_name=getattr(source, "short_name", "") or "",
            entity_map=entity_map,
            force_full_sync=force_full_sync,
            execution_config=resolved_config,
        )

        runtime = SyncRuntime(
            source=source,
            cursor=cursor,
            dense_embedder=self._dense_embedder,
            sparse_embedder=self._sparse_embedder,
            destinations=destinations,
            entity_tracker=entity_tracker_result,
            event_bus=self._event_bus,
            usage_checker=self._usage_checker,
        )

        logger.debug(f"Context + runtime built in {time.time() - init_start:.2f}s")

        dispatcher_builder = EntityDispatcherBuilder(
            processor=self._processor,
            entity_repo=self._entity_repo,
            arf_service=self._arf_service,
        )
        dispatcher = dispatcher_builder.build(
            destinations=runtime.destinations,
            execution_config=resolved_config,
            logger=sync_context.logger,
        )

        action_resolver = EntityActionResolver(
            entity_map=sync_context.entity_map,
            entity_repo=self._entity_repo,
        )

        entity_pipeline = EntityPipeline(
            entity_tracker=runtime.entity_tracker,
            event_bus=self._event_bus,
            action_resolver=action_resolver,
            action_dispatcher=dispatcher,
            entity_repo=self._entity_repo,
        )

        access_control_pipeline = AccessControlPipeline(
            resolver=ACActionResolver(),
            dispatcher=ACActionDispatcher(handlers=[ACPostgresHandler(acl_repo=self._acl_repo)]),
            tracker=ACLMembershipTracker(
                source_connection_id=sync_context.source_connection_id,
                organization_id=sync_context.organization_id,
                logger=sync_context.logger,
            ),
            acl_repo=self._acl_repo,
        )

        worker_pool = AsyncWorkerPool(logger=sync_context.logger)

        stream = AsyncSourceStream(
            source_generator=runtime.source.generate_entities(
                cursor=runtime.cursor,
                files=files,
                node_selections=node_selections,
            ),
            queue_size=10000,
            logger=sync_context.logger,
        )

        orchestrator = SyncOrchestrator(
            entity_pipeline=entity_pipeline,
            worker_pool=worker_pool,
            stream=stream,
            sync_context=sync_context,
            runtime=runtime,
            access_control_pipeline=access_control_pipeline,
            sync_cursor_service=container_mod.container.sync_cursor_service,
        )

        logger.info(f"Total orchestrator initialization took {time.time() - init_start:.2f}s")
        return orchestrator

    # -------------------------------------------------------------------------
    # Private: Service builders (delegate to sub-builders)
    # -------------------------------------------------------------------------

    @staticmethod
    async def _build_source(db, sync, sync_job, ctx, force_full_sync, execution_config):
        """Build source and cursor. Returns (source, cursor) tuple."""
        from airweave.domains.sync_pipeline.builders.source import SourceContextBuilder
        from airweave.domains.sync_pipeline.contexts.infra import InfraContext

        Returns (source, cursor, files, node_selections) tuple.
        """
        sync_logger = LoggerConfigurator.configure_logger(
            "airweave.platform.sync.source_build",
            dimensions={
                "sync_id": str(sync.id),
                "organization_id": str(ctx.organization.id),
            },
        )

        if execution_config and execution_config.behavior.replay_from_arf:
            source, cursor = await cls._build_arf_replay_source(
                db=db,
                sync=sync,
                ctx=ctx,
                logger=sync_logger,
            )
            return source, cursor, None, None

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

        cls._validate_not_completed_snapshot(source_connection_obj)

        source = await container_mod.container.source_lifecycle_service.create(
            db=db,
            source_connection_id=UUID(str(source_connection_obj.id)),
            ctx=ctx,
            access_token=access_token,
        )

        files = FileService(
            sync_job_id=sync_job.id,
            storage_backend=container_mod.container.storage_backend,
        )

        cursor = await cls._create_cursor(
            db=db,
            sync=sync,
            sync_job=sync_job,
            infra=infra,
            force_full_sync=force_full_sync,
            execution_config=execution_config,
        )

        node_selections = await cls._load_node_selections(
            db, UUID(str(source_connection_obj.id)), ctx
        )
        if node_selections:
            sync_logger.info(f"Loaded {len(node_selections)} node selections for targeted sync")

        return source, cursor, files, node_selections

    @classmethod
    async def _build_arf_replay_source(cls, db, sync, ctx, logger):
        """Build source for ARF replay mode. Returns (source, cursor)."""
        from airweave.domains.arf.replay_source import ArfReplaySource

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
            storage=container_mod.container.storage_backend,
            logger=logger,
            restore_files=True,
            original_short_name=original_short_name,
        )

        await source.validate()

        cursor = SyncCursor(sync_id=sync.id, cursor_schema=None, cursor_data=None)
        return source, cursor

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

    @classmethod
    async def _create_cursor(
        cls, db, sync, source_class, ctx, logger, force_full_sync, execution_config
    ) -> Optional[SyncCursor]:
        """Create sync cursor, or None if source doesn't support cursors.

        Returns:
            None — source has no cursor support (every sync is full).
            SyncCursor(cursor_data=None) — supports cursors, but first run or force full.
            SyncCursor(cursor_data={...}) — incremental sync with loaded cursor.
        """
        registry_entry = container_mod.container.source_registry.get(source_class.short_name)
        if not registry_entry.supports_cursor:
            return None

        cursor_schema = source_class.cursor_class

        if force_full_sync:
            logger.info(
                "FORCE FULL SYNC: Skipping cursor data to ensure all entities are fetched "
                "for accurate orphaned entity cleanup."
            )
            cursor_data = None
        elif execution_config and execution_config.cursor.skip_load:
            logger.info("SKIP CURSOR LOAD: Fetching all entities (skip_load=True)")
            cursor_data = None
        else:
            cursor_data = await container_mod.container.sync_cursor_service.get_cursor_data(
                db=db, sync_id=sync.id, ctx=ctx
            )
            if cursor_data:
                logger.info(f"Incremental sync: Using cursor data with {len(cursor_data)} keys")

        return SyncCursor(sync_id=sync.id, cursor_schema=cursor_schema, cursor_data=cursor_data)

    @staticmethod
    async def _load_node_selections(db, source_connection_id, ctx):
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

    @staticmethod
    async def _build_destinations(db, sync, collection, ctx, execution_config):
        """Build destinations and entity map. Returns (destinations, entity_map) tuple."""
        from airweave.domains.sync_pipeline.builders.destinations import DestinationsContextBuilder

        dest_logger = LoggerConfigurator.configure_logger(
            "airweave.platform.sync.dest_build",
            dimensions={
                "sync_id": str(sync.id),
                "organization_id": str(ctx.organization.id),
            },
        )

        return await DestinationsContextBuilder.build(
            db=db,
            sync=sync,
            collection=collection,
            ctx=ctx,
            logger=dest_logger,
            execution_config=execution_config,
        )

    @staticmethod
    async def _build_tracking(
        db: AsyncSession,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        ctx: BaseContext,
    ) -> EntityTracker:
        """Build tracking components. Returns EntityTracker."""
        track_logger = LoggerConfigurator.configure_logger(
            "airweave.platform.sync.tracking_build",
            dimensions={
                "sync_id": str(sync.id),
                "organization_id": str(ctx.organization.id),
            },
        )

        return await TrackingContextBuilder.build(
            db=db,
            sync=sync,
            sync_job=sync_job,
            ctx=ctx,
            logger=track_logger,
        )
