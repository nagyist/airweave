"""Service for integrating Temporal workflows."""

from typing import Optional
from uuid import uuid4

from temporalio.client import WorkflowHandle

from airweave import schemas
from airweave.core.config import settings
from airweave.core.logging import logger
from airweave.platform.temporal.client import temporal_client
from airweave.platform.temporal.workflows import RunSourceConnectionWorkflow


class TemporalService:
    """Service for managing Temporal workflows."""

    async def run_source_connection_workflow(
        self,
        sync: schemas.Sync,
        sync_job: schemas.SyncJob,
        sync_dag: schemas.SyncDag,
        collection: schemas.Collection,
        source_connection: schemas.SourceConnection,
        user: schemas.User,
        access_token: Optional[str] = None,
    ) -> WorkflowHandle:
        """Start a source connection sync workflow.

        Args:
            sync: The sync configuration
            sync_job: The sync job
            sync_dag: The sync DAG
            collection: The collection
            source_connection: The source connection
            user: The current user
            access_token: Optional access token

        Returns:
            The workflow handle
        """
        client = await temporal_client.get_client()
        task_queue = settings.TEMPORAL_TASK_QUEUE

        # Generate a unique workflow ID
        workflow_id = f"sync-{sync_job.id}-{uuid4()}"

        logger.info(f"Starting Temporal workflow {workflow_id} for sync job {sync_job.id}")
        logger.info(f"Source: {source_connection.name} | Collection: {collection.name}")

        # Convert Pydantic models to dicts for JSON serialization
        handle = await client.start_workflow(
            RunSourceConnectionWorkflow.run,
            args=[
                sync.model_dump(mode="json"),
                sync_job.model_dump(mode="json"),
                sync_dag.model_dump(mode="json"),
                collection.model_dump(mode="json"),
                source_connection.model_dump(mode="json"),
                user.model_dump(mode="json"),
                access_token,
            ],
            id=workflow_id,
            task_queue=task_queue,
        )

        logger.info("✅ Temporal workflow started successfully!")
        logger.info(
            f"📊 Track progress at: http://localhost:8233/namespaces/default/workflows/{workflow_id}"
        )
        logger.info(
            f"🔍 View in CLI: docker exec airweave-temporal-dev tctl --address temporal:7233 "
            f"workflow describe -w {workflow_id}"
        )

        return handle

    async def is_temporal_enabled(self) -> bool:
        """Check if Temporal is enabled and available.

        Returns:
            True if Temporal is enabled, False otherwise
        """
        temporal_enabled = settings.TEMPORAL_ENABLED

        if not temporal_enabled:
            return False

        try:
            _ = await temporal_client.get_client()
            return True
        except Exception as e:
            logger.warning(f"Temporal not available: {e}")
            return False


temporal_service = TemporalService()
