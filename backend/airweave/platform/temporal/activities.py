"""Temporal activities for Airweave."""

from typing import Any, Dict, Optional

from temporalio import activity


# Import inside the activity to avoid issues with Temporal's sandboxing
@activity.defn
async def run_sync_activity(
    sync_dict: Dict[str, Any],
    sync_job_dict: Dict[str, Any],
    sync_dag_dict: Dict[str, Any],
    collection_dict: Dict[str, Any],
    source_connection_dict: Dict[str, Any],
    user_dict: Dict[str, Any],
    access_token: Optional[str] = None,
) -> None:
    """Activity to run a sync job.

    This activity wraps the existing sync_service.run method.

    Args:
        sync_dict: The sync configuration as dict
        sync_job_dict: The sync job as dict
        sync_dag_dict: The sync DAG as dict
        collection_dict: The collection as dict
        source_connection_dict: The source connection as dict
        user_dict: The current user as dict
        access_token: Optional access token
    """
    # Import here to avoid Temporal sandboxing issues
    from airweave import schemas
    from airweave.core.sync_service import sync_service

    # Convert dicts back to Pydantic models
    sync = schemas.Sync(**sync_dict)
    sync_job = schemas.SyncJob(**sync_job_dict)
    sync_dag = schemas.SyncDag(**sync_dag_dict)
    collection = schemas.Collection(**collection_dict)
    source_connection = schemas.SourceConnection(**source_connection_dict)
    user = schemas.User(**user_dict)

    activity.logger.info(f"Starting sync activity for job {sync_job.id}")

    try:
        await sync_service.run(
            sync=sync,
            sync_job=sync_job,
            dag=sync_dag,
            collection=collection,
            source_connection=source_connection,
            current_user=user,
            access_token=access_token,
        )
        activity.logger.info(f"Completed sync activity for job {sync_job.id}")
    except Exception as e:
        activity.logger.error(f"Failed sync activity for job {sync_job.id}: {e}")
        raise
