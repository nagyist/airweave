"""Module for data synchronization."""

import asyncio
import importlib

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core.logging import logger
from app.db.session import get_db_context
from app.platform.destinations._base import GraphDBDestination
from app.platform.entities._base import BaseEntity, DestinationAction
from app.platform.sync.context import SyncContext
from app.platform.sync.stream import AsyncSourceStream

MAX_WORKERS: int = 20


class SyncOrchestrator:
    """Main service for data synchronization."""

    async def run(self, sync_context: SyncContext) -> schemas.Sync:
        """Run a sync with full async processing.

        Args:
            sync_context: The sync context
        """
        try:
            async with get_db_context() as db:
                # Get source node from DAG
                source_node = sync_context.dag.get_source_node()

                # Process entities through the stream
                await self._process_entity_stream(source_node, sync_context, db)

                # Finalize and return sync
                await sync_context.progress.finalize()
                return sync_context.sync

        except Exception as e:
            logger.error(f"Error during sync: {e}")
            raise

    async def _process_entity_stream(
        self, source_node: schemas.DagNode, sync_context: SyncContext, db: AsyncSession
    ) -> None:
        """Process the stream of entities coming from the source.

        Args:
            source_node: The source node from the DAG
            sync_context: The sync context
            db: Database session
        """
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(MAX_WORKERS)

        # Create async stream and use it as a context manager
        async with AsyncSourceStream(sync_context.source.generate_entities()) as stream:
            try:
                # Process entities with controlled concurrency
                await self._process_stream_with_concurrency(
                    stream, semaphore, source_node, sync_context, db
                )
            except Exception as e:
                logger.error(f"Error during sync: {e}")
                raise
            finally:
                # Ensure we finalize progress
                await sync_context.progress.finalize()

    async def _process_stream_with_concurrency(
        self,
        stream: AsyncSourceStream,
        semaphore: asyncio.Semaphore,
        source_node: schemas.DagNode,
        sync_context: SyncContext,
        db: AsyncSession,
    ) -> None:
        """Process the entity stream with controlled concurrency.

        Args:
            stream: The async source stream
            semaphore: Semaphore to control concurrency
            source_node: The source node from the DAG
            sync_context: The sync context
            db: Database session
        """
        # Process entities as they come in, with controlled concurrency
        pending_tasks = set()

        # Process entities as they come in from the stream
        async for entity in stream.get_entities():
            # Create task for this entity
            task = asyncio.create_task(
                self._process_entity_with_semaphore(
                    semaphore,
                    entity,
                    source_node,
                    sync_context,
                )
            )
            # Store the original entity with the task for error reporting
            task.entity = entity
            pending_tasks.add(task)

            task.add_done_callback(lambda t: self._handle_task_completion(t, pending_tasks))

            # If we have too many pending tasks, wait for some to complete
            if len(pending_tasks) >= MAX_WORKERS * 2:
                await self._wait_for_tasks(pending_tasks, asyncio.FIRST_COMPLETED, 0.5)

        # Wait for any remaining tasks with proper error handling
        if pending_tasks:
            logger.info(f"Waiting for {len(pending_tasks)} remaining tasks")
            await self._wait_for_all_pending_tasks(pending_tasks)

    def _handle_task_completion(self, completed_task: asyncio.Task, pending_tasks: set) -> None:
        """Handle task completion and remove it from pending tasks.

        Args:
            completed_task: The completed task
            pending_tasks: Set of pending tasks
        """
        pending_tasks.discard(completed_task)
        # Handle any exceptions
        if not completed_task.cancelled() and completed_task.exception():
            entity_id = getattr(completed_task.entity, "entity_id", "unknown")
            logger.error(f"Task for entity {entity_id} failed: {completed_task.exception()}")

    async def _wait_for_tasks(
        self, pending_tasks: set, return_when: asyncio.Future, timeout: float
    ) -> None:
        """Wait for tasks to complete with error handling.

        Args:
            pending_tasks: Set of pending tasks
            return_when: Wait policy (e.g., FIRST_COMPLETED)
            timeout: Maximum time to wait
        """
        done, _ = await asyncio.wait(
            pending_tasks,
            return_when=return_when,
            timeout=timeout,
        )

        # Process completed tasks and handle errors
        for completed_task in done:
            try:
                await completed_task
            except Exception as e:
                entity_id = getattr(
                    getattr(completed_task, "entity", None),
                    "entity_id",
                    "unknown",
                )
                logger.error(f"Entity {entity_id} processing error: {e}")
                # Re-raise critical exceptions
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise

    async def _wait_for_all_pending_tasks(self, pending_tasks: set) -> None:
        """Wait for all pending tasks to complete.

        Args:
            pending_tasks: Set of pending tasks
        """
        while pending_tasks:
            wait_tasks = list(pending_tasks)[: MAX_WORKERS * 2]
            done, pending_tasks_remaining = await asyncio.wait(
                wait_tasks,
                return_when=asyncio.ALL_COMPLETED,
                timeout=10,
            )

            # Update pending tasks
            for task in wait_tasks:
                pending_tasks.discard(task)

            # Add back any tasks that didn't complete
            pending_tasks.update(pending_tasks_remaining)

            # Check for exceptions
            for completed_task in done:
                try:
                    await completed_task
                except Exception as e:
                    entity_id = getattr(
                        getattr(completed_task, "entity", None),
                        "entity_id",
                        "unknown",
                    )
                    logger.error(f"Entity {entity_id} processing error: {e}")
                    # Re-raise critical exceptions
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise

    async def _process_entity_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        entity: BaseEntity,
        source_node: schemas.DagNode,
        sync_context: SyncContext,
    ) -> tuple[list[BaseEntity], DestinationAction]:
        """Process a single entity with semaphore control.

        Args:
            semaphore: Semaphore to control concurrency
            entity: Entity to process
            source_node: The source node from the DAG
            sync_context: The sync context

        Returns:
            Tuple of (processed entities, action)
        """
        async with semaphore:
            # Create a new session for this task
            async with get_db_context() as db:
                # First, enrich the entity with sync metadata
                entity = await self._enrich_entity(entity, sync_context)

                # Then determine the entity action (without processing through DAG)
                db_entity, action = await self._determine_entity_action(entity, sync_context, db)

                # If the action is KEEP, we can skip further processing
                if action == DestinationAction.KEEP:
                    # Update progress counter for KEEP action
                    await sync_context.progress.increment("already_sync", 1)
                    return [], action

                # Process the entity through the DAG
                processed_entities = await sync_context.router.process_entity(
                    db=db,
                    producer_id=source_node.id,
                    entity=entity,
                )

                # Persist the parent entity and its processed children
                await self._persist_entities(
                    entity, processed_entities, db_entity, action, sync_context, db
                )

                return processed_entities, action

    async def _enrich_entity(
        self,
        entity: BaseEntity,
        sync_context: SyncContext,
    ) -> BaseEntity:
        """Enrich an entity with information from the sync context.

        Adds metadata from the sync context to the entity, including source name,
        sync IDs, and white label information when applicable.

        Args:
            entity: The entity to be enriched
            sync_context: The sync context containing metadata to add to the entity

        Returns:
            The enriched entity with added metadata
        """
        entity.source_name = sync_context.source._name
        entity.sync_id = sync_context.sync.id
        entity.sync_job_id = sync_context.sync_job.id
        entity.sync_metadata = sync_context.sync.sync_metadata
        if sync_context.sync.white_label_id:
            entity.white_label_user_identifier = sync_context.sync.white_label_user_identifier
            entity.white_label_id = sync_context.sync.white_label_id
            entity.white_label_name = sync_context.white_label.name
        return entity

    async def _determine_entity_action(
        self, entity: BaseEntity, sync_context: SyncContext, db: AsyncSession
    ) -> tuple[schemas.Entity, DestinationAction]:
        """Determine what action should be taken for an entity.

        Args:
            entity: Entity to check
            sync_context: The sync context
            db: Database session

        Returns:
            Tuple of (database entity if exists, action to take)
        """
        # Check if the entity already exists in the database
        db_entity = await crud.entity.get_by_entity_and_sync_id(
            db=db, entity_id=entity.entity_id, sync_id=sync_context.sync.id
        )

        if db_entity:
            # Check if the entity has been updated
            if db_entity.hash != entity.hash():
                # Entity has been updated, so we need to update it
                action = DestinationAction.UPDATE
            else:
                # Entity is the same, so we keep it
                action = DestinationAction.KEEP
        else:
            # Entity does not exist in the database, so we need to insert it
            action = DestinationAction.INSERT

        return db_entity, action

    async def _persist_entities(
        self,
        parent_entity: BaseEntity,
        processed_entities: list[BaseEntity],
        db_entity: schemas.Entity,
        action: DestinationAction,
        sync_context: SyncContext,
        db: AsyncSession,
    ) -> None:
        """Persist the parent entity and its processed children.

        Args:
            parent_entity: The parent entity
            processed_entities: List of processed entities
            db_entity: Existing database entity if any
            action: The action to take (INSERT, UPDATE, KEEP)
            sync_context: The sync context
            db: Database session
        """
        # No processing needed for KEEP action
        if action == DestinationAction.KEEP:
            # Update the progress for kept entities
            await sync_context.progress.increment(already_sync=1)
            return

        if len(processed_entities) == 0:
            raise ValueError("No processed entities to persist")

        # Prepare the processed entities
        for processed_entity in processed_entities:
            # Set the parent ID if not already set
            if not processed_entity.parent_id and parent_entity:
                processed_entity.parent_id = parent_entity.id

            # Set the sync ID
            processed_entity.sync_id = sync_context.sync.id

            # Set the entity definition ID
            entity_type = type(processed_entity)
            if entity_type in sync_context.entity_map:
                processed_entity.entity_definition_id = sync_context.entity_map[entity_type]

        # Persist to all configured destinations
        for destination_name, destination in sync_context.destinations.items():
            try:
                # Bulk insert/update entities
                if action == DestinationAction.INSERT:
                    await destination.bulk_insert(processed_entities)
                elif action == DestinationAction.UPDATE:
                    # For update, first delete then insert
                    await destination.delete(db_entity.id)
                    await destination.bulk_insert(processed_entities)

                # Process relationships for graph databases
                if isinstance(destination, GraphDBDestination):
                    await self._process_relationships(processed_entities, destination, sync_context)

            except Exception as e:
                logger.error(f"Error persisting to {destination_name}: {str(e)}")
                # Continue with other destinations even if one fails

        # Update progress
        if action == DestinationAction.INSERT:
            await sync_context.progress.increment(inserted=len(processed_entities))
        elif action == DestinationAction.UPDATE:
            await sync_context.progress.increment(updated=len(processed_entities))

    async def _process_relationships(
        self,
        entities: list[BaseEntity],
        destination: GraphDBDestination,
        sync_context: SyncContext,
    ) -> None:
        """Process entity relationships for graph databases.

        This method extracts relationships from entity modules and creates them in the graph database.

        Args:
            entities: List of entities to process relationships for
            destination: The graph database destination
            sync_context: The sync context
        """
        # Skip if no entities
        if not entities:
            return

        # Get the module where the entity class is defined
        entity_module = entities[0].__class__.__module__

        try:
            # Import the module
            module = importlib.import_module(entity_module)

            # Check if the module has a RELATIONS list
            if not hasattr(module, "RELATIONS"):
                return

            relations = module.RELATIONS
            if not relations:
                return

            # Process each relation
            for relation in relations:
                # Find entities that match the source entity type
                source_entities = [
                    entity for entity in entities if isinstance(entity, relation.source_entity_type)
                ]

                if not source_entities:
                    continue

                # For each source entity, create relationships
                relationships_to_create = []

                for source_entity in source_entities:
                    # Get the source ID attribute value
                    source_id = getattr(source_entity, relation.source_entity_id_attribute, None)
                    if not source_id:
                        continue

                    # Handle both single values and lists
                    target_ids = []
                    if isinstance(source_id, list):
                        target_ids.extend(source_id)
                    else:
                        target_ids.append(source_id)

                    # Create a relationship for each target ID
                    for target_id in target_ids:
                        if not target_id:
                            continue

                        relationships_to_create.append(
                            {
                                "from_node_id": str(source_entity.id),
                                "to_node_id": str(target_id),
                                "rel_type": relation.relation_type,
                                "properties": {
                                    "sync_id": str(sync_context.sync.id),
                                    "source_type": source_entity.__class__.__name__,
                                    "target_type": relation.target_entity_type.__name__,
                                },
                            }
                        )

                # Bulk create relationships if any
                if relationships_to_create:
                    try:
                        await destination.bulk_create_relationships(relationships_to_create)
                        logger.info(
                            f"Created {len(relationships_to_create)} relationships of type {relation.relation_type}"
                        )
                    except Exception as e:
                        logger.error(f"Error creating relationships: {str(e)}")

        except Exception as e:
            logger.error(f"Error processing relationships: {str(e)}")


sync_orchestrator = SyncOrchestrator()
