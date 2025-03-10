"""Module for sync context."""

import importlib
from typing import Optional, Type
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core import credentials
from app.core.exceptions import NotFoundException
from app.core.logging import logger
from app.platform.auth.schemas import AuthType
from app.platform.auth.services import oauth2_service
from app.platform.destinations._base import BaseDestination
from app.platform.destinations.weaviate import WeaviateDestination
from app.platform.embedding_models._base import BaseEmbeddingModel
from app.platform.embedding_models.local_text2vec import LocalText2Vec
from app.platform.entities._base import BaseEntity
from app.platform.locator import resource_locator
from app.platform.sources._base import BaseSource
from app.platform.sync.pubsub import SyncProgress
from app.platform.sync.router import SyncDAGRouter


class SyncContext:
    """Context container for a sync.

    Contains all the necessary components for a sync:
    - source - the source instance
    - destinations - the destination instances
    - embedding model - the embedding model used for the sync
    - transformers - a dictionary of transformer callables
    - sync - the main sync object
    - sync job - the sync job that is created for the sync
    - dag - the DAG that is created for the sync
    - progress - the progress tracker, interfaces with PubSub
    - router - the DAG router
    - white label (optional)
    """

    source: BaseSource
    destinations: dict[str, BaseDestination]  # Map of destination name to destination instance
    embedding_model: BaseEmbeddingModel
    transformers: dict[str, callable]
    sync: schemas.Sync
    sync_job: schemas.SyncJob
    dag: schemas.SyncDag
    progress: SyncProgress
    router: SyncDAGRouter
    entity_map: dict[type[BaseEntity], UUID]

    white_label: Optional[schemas.WhiteLabel] = None

    def __init__(
        self,
        source: BaseSource,
        destinations: dict[str, BaseDestination],  # Map of destination name to destination instance
        embedding_model: BaseEmbeddingModel,
        transformers: dict[str, callable],
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        dag: schemas.SyncDag,
        progress: SyncProgress,
        router: SyncDAGRouter,
        entity_map: dict[type[BaseEntity], UUID],
        white_label: Optional[schemas.WhiteLabel] = None,
    ):
        """Initialize sync context.

        Args:
            source: Source instance
            destinations: Map of destination name to destination instances
            embedding_model: Embedding model instance
            transformers: Dictionary of transformer callables
            sync: Sync instance
            sync_job: Sync job instance
            dag: DAG instance
            progress: Progress tracker instance
            router: DAG router instance
            entity_map: Map of entity type to entity definition ID
            white_label: Optional white label configuration
        """
        self.source = source
        self.destinations = destinations
        self.embedding_model = embedding_model
        self.transformers = transformers
        self.sync = sync
        self.sync_job = sync_job
        self.dag = dag
        self.progress = progress
        self.router = router
        self.entity_map = entity_map
        self.white_label = white_label

    @property
    def destination(self) -> BaseDestination:
        """Return the first destination for backward compatibility.

        This property is maintained for backward compatibility with existing code
        that expects a single destination.

        Returns:
            BaseDestination: The first destination instance
        """
        if not self.destinations:
            raise ValueError("No destinations configured")
        return next(iter(self.destinations.values()))


class SyncContextFactory:
    """Factory for sync context."""

    @classmethod
    async def create(
        cls,
        db: AsyncSession,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        dag: schemas.SyncDag,
        current_user: schemas.User,
        white_label: Optional[schemas.WhiteLabel] = None,
    ) -> SyncContext:
        """Create a sync context.

        Args:
            db: Database connection
            sync: Sync instance
            sync_job: Sync job instance
            dag: DAG instance
            current_user: Current user
            white_label: Optional white label configuration

        Returns:
            SyncContext: Sync context instance
        """
        # Create source instance
        source = await cls._create_source_instance(db, sync, current_user)

        # Create embedding model
        embedding_model = cls._get_embedding_model(sync)

        # Create destination instances
        destinations = await cls._create_destination_instances(
            db, sync, embedding_model, current_user
        )

        # Create transformers
        transformers = await cls._get_transformer_callables(db, sync)

        # Create entity map
        entity_map = await cls._get_entity_definition_map(db)

        # Create sync progress - only use the job_id parameter as required by the current SyncProgress API
        progress = SyncProgress(job_id=sync_job.id)

        # Create router
        router = SyncDAGRouter(dag=dag, entity_map=entity_map)

        # Create sync context
        return SyncContext(
            source=source,
            destinations=destinations,
            embedding_model=embedding_model,
            transformers=transformers,
            sync=sync,
            sync_job=sync_job,
            dag=dag,
            progress=progress,
            router=router,
            entity_map=entity_map,
            white_label=white_label,
        )

    @classmethod
    async def _create_source_instance(
        cls,
        db: AsyncSession,
        sync: schemas.Sync,
        current_user: schemas.User,
    ) -> BaseSource:
        """Create and configure the source instance based on authentication type."""
        source_connection = await crud.connection.get(db, sync.source_connection_id, current_user)
        if not source_connection:
            raise NotFoundException("Source connection not found")

        source_model = await crud.source.get_by_short_name(db, source_connection.short_name)
        if not source_model:
            raise NotFoundException("Source not found")

        source_class = resource_locator.get_source(source_model)

        if source_model.auth_type == AuthType.none:
            return await source_class.create()

        if source_model.auth_type in [
            AuthType.oauth2_with_refresh,
            AuthType.oauth2_with_refresh_rotating,
        ]:
            return await cls._create_oauth2_with_refresh_source(
                db, source_model, source_class, current_user, source_connection
            )

        if source_model.auth_type == AuthType.oauth2:
            return await cls._create_oauth2_source(
                db, source_class, current_user, source_connection
            )

        return await cls._create_other_auth_source(
            db, source_model, source_class, current_user, source_connection
        )

    @classmethod
    async def _create_oauth2_with_refresh_source(
        cls,
        db: AsyncSession,
        source_model: schemas.Source,
        source_class,
        current_user: schemas.User,
        source_connection: schemas.Connection,
    ) -> BaseSource:
        """Create source instance for OAuth2 with refresh token."""
        oauth2_response = await oauth2_service.refresh_access_token(
            db, source_model.short_name, current_user, source_connection.id
        )
        return await source_class.create(oauth2_response.access_token)

    @classmethod
    async def _create_oauth2_source(
        cls,
        db: AsyncSession,
        source_class,
        current_user: schemas.User,
        source_connection: schemas.Connection,
    ) -> BaseSource:
        """Create source instance for regular OAuth2."""
        if not source_connection.integration_credential_id:
            raise NotFoundException("Source connection has no integration credential")

        credential = await cls._get_integration_credential(db, source_connection, current_user)
        decrypted_credential = credentials.decrypt(credential.encrypted_credentials)
        return await source_class.create(decrypted_credential["access_token"])

    @classmethod
    async def _create_other_auth_source(
        cls,
        db: AsyncSession,
        source_model: schemas.Source,
        source_class,
        current_user: schemas.User,
        source_connection: schemas.Connection,
    ) -> BaseSource:
        """Create source instance for other authentication types."""
        if not source_connection.integration_credential_id:
            raise NotFoundException("Source connection has no integration credential")

        credential = await cls._get_integration_credential(db, source_connection, current_user)
        decrypted_credential = credentials.decrypt(credential.encrypted_credentials)

        if not source_model.auth_config_class:
            raise ValueError(f"Auth config class required for auth type {source_model.auth_type}")

        auth_config = resource_locator.get_auth_config(source_model.auth_config_class)
        source_credentials = auth_config.model_validate(decrypted_credential)
        return await source_class.create(source_credentials)

    @classmethod
    async def _get_integration_credential(
        cls,
        db: AsyncSession,
        source_connection: schemas.Connection,
        current_user: schemas.User,
    ) -> schemas.IntegrationCredential:
        """Get integration credential."""
        credential = await crud.integration_credential.get(
            db, source_connection.integration_credential_id, current_user
        )
        if not credential:
            raise NotFoundException("Source integration credential not found")
        return credential

    @classmethod
    def _get_embedding_model(cls, sync: schemas.Sync) -> BaseEmbeddingModel:
        """Get embedding model instance."""
        if not sync.embedding_model_connection_id:
            return LocalText2Vec()
        return LocalText2Vec()  # TODO: Handle other embedding models

    @classmethod
    async def _create_destination_instances(
        cls,
        db: AsyncSession,
        sync: schemas.Sync,
        embedding_model: BaseEmbeddingModel,
        current_user: schemas.User,
    ) -> dict[str, BaseDestination]:
        """Create destination instances for the sync context.

        Args:
            db: The database session
            sync: The sync object
            embedding_model: The embedding model
            current_user: The current user

        Returns:
            dict[str, BaseDestination]: Map of destination name to destination instance
        """
        destinations = {}

        # Get the sync destinations from the database
        sync_destinations = await crud.sync_destination.get_by_sync_id(db, sync.id)

        # Process each sync destination
        for sync_destination in sync_destinations:
            # For native Weaviate
            if (
                sync_destination.is_native
                and sync_destination.destination_type == "weaviate_native"
            ):
                destinations["weaviate"] = await WeaviateDestination.create(
                    sync.id, embedding_model
                )

            # For native Neo4j
            elif sync_destination.is_native and sync_destination.destination_type == "neo4j_native":
                try:
                    from app.platform.destinations.neo4j import Neo4jDestination

                    destinations["neo4j"] = await Neo4jDestination.create(sync.id, embedding_model)
                except (ImportError, Exception) as e:
                    logger.warning(f"Failed to initialize native Neo4j destination: {e}")

            # For connection-based destinations
            elif sync_destination.connection_id:
                try:
                    connection = await crud.connection.get(
                        db, sync_destination.connection_id, current_user
                    )
                    if not connection:
                        continue

                    destination_class = get_destination_class(connection.type)
                    destinations[connection.name] = await destination_class.create(
                        sync.id, embedding_model, connection
                    )
                except Exception as e:
                    logger.error(f"Failed to create destination instance: {e}")
                    continue

        # Default to Weaviate if no destination specified
        if not destinations:
            destinations["weaviate"] = await WeaviateDestination.create(sync.id, embedding_model)

        return destinations

    @classmethod
    async def _get_transformer_callables(
        cls, db: AsyncSession, sync: schemas.Sync
    ) -> dict[str, callable]:
        """Get transformers instance."""
        transformers = {}

        transformer_functions = await crud.transformer.get_all(db)
        for transformer in transformer_functions:
            transformers[transformer.method_name] = resource_locator.get_transformer(transformer)
        return transformers

    @classmethod
    async def _get_entity_definition_map(cls, db: AsyncSession) -> dict[type[BaseEntity], UUID]:
        """Get entity definition map.

        Map entity class to entity definition id.

        Example key-value pair:
            <class 'app.platform.entities.trello.TrelloBoard'>: entity_definition_id
        """
        entity_definitions = await crud.entity_definition.get_all(db)

        entity_definition_map = {}
        for entity_definition in entity_definitions:
            full_module_name = f"app.platform.entities.{entity_definition.module_name}"
            module = importlib.import_module(full_module_name)
            entity_class = getattr(module, entity_definition.class_name)
            entity_definition_map[entity_class] = entity_definition.id

        return entity_definition_map


def get_destination_class(short_name: str) -> Type[BaseDestination]:
    """Get the destination class for a given short name.

    Args:
        short_name: Short name of the destination

    Returns:
        The destination class or None if not found
    """
    return resource_locator.get_destination_by_short_name(short_name)
