"""Temporal workflows for Airweave."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn
class RunSourceConnectionWorkflow:
    """Workflow for running a source connection sync."""

    @workflow.run
    async def run(
        self,
        sync_dict: Dict[str, Any],
        sync_job_dict: Dict[str, Any],
        sync_dag_dict: Dict[str, Any],
        collection_dict: Dict[str, Any],
        source_connection_dict: Dict[str, Any],
        user_dict: Dict[str, Any],
        access_token: Optional[str] = None,
    ) -> None:
        """Run the source connection sync workflow.

        Args:
            sync_dict: The sync configuration as dict
            sync_job_dict: The sync job as dict
            sync_dag_dict: The sync DAG as dict
            collection_dict: The collection as dict
            source_connection_dict: The source connection as dict
            user_dict: The current user as dict
            access_token: Optional access token
        """
        # Import inside the workflow to avoid issues
        from airweave.platform.temporal.activities import run_sync_activity

        await workflow.execute_activity(
            run_sync_activity,
            args=[
                sync_dict,
                sync_job_dict,
                sync_dag_dict,
                collection_dict,
                source_connection_dict,
                user_dict,
                access_token,
            ],
            start_to_close_timeout=timedelta(hours=1),  # 1 hour timeout for sync
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(minutes=5),
                maximum_attempts=3,
            ),
        )
