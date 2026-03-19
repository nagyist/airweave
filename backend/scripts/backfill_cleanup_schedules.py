#!/usr/bin/env python3
"""Backfill daily cleanup schedules for incremental syncs.

Minute-level syncs (GitHub, Slack, etc.) run incremental syncs that skip
orphan cleanup. A daily forced-full-sync companion schedule ensures
orphans are cleaned up. This script creates the missing cleanup schedules
in Temporal for any existing incremental syncs that don't have one.

Usage:
    # Dry-run (default) — shows what would be created
    kubectl exec -it deploy/airweave-backend -- python -m scripts.backfill_cleanup_schedules

    # Apply
    kubectl exec -it deploy/airweave-backend -- python -m scripts.backfill_cleanup_schedules --apply
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleSpec,
    ScheduleState,
)
from temporalio.service import RPCError, RPCStatusCode

from airweave import schemas
from airweave.core.config import settings
from airweave.db.session import AsyncSessionLocal
from airweave.models.sync import Sync as SyncModel
from airweave.platform.temporal.client import temporal_client
from airweave.platform.temporal.workflows import RunSourceConnectionWorkflow


async def check_schedule_exists(client, schedule_id: str) -> bool:
    """Check if a schedule already exists in Temporal."""
    try:
        handle = client.get_schedule_handle(schedule_id)
        await handle.describe()
        return True
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            return False
        raise


async def _gather_sync_data(sync) -> tuple[dict, dict, dict] | None:
    """Load source connection, collection, and connection for a sync.

    Returns (sync_dict, collection_dict, connection_dict) or None if
    any required record is missing.
    """
    from airweave.models.collection import Collection
    from airweave.models.connection import Connection
    from airweave.models.source_connection import SourceConnection

    async with AsyncSessionLocal() as db:
        sc_result = await db.execute(
            select(SourceConnection).where(SourceConnection.sync_id == sync.id)
        )
        source_connection = sc_result.scalar_one_or_none()
        if not source_connection:
            return None

        col_result = await db.execute(
            select(Collection).where(
                Collection.readable_id == source_connection.readable_collection_id
            )
        )
        collection = col_result.scalar_one_or_none()
        if not collection:
            return None

        conn_result = await db.execute(
            select(Connection).where(Connection.id == source_connection.connection_id)
        )
        connection = conn_result.scalar_one_or_none()
        if not connection:
            return None

        sync_result = await db.execute(select(SyncModel).where(SyncModel.id == sync.id))
        sync_obj = sync_result.scalar_one()

        sync_dict = schemas.Sync.model_validate(
            sync_obj, from_attributes=True
        ).model_dump(mode="json")
        collection_dict = schemas.CollectionRecord.model_validate(
            collection, from_attributes=True
        ).model_dump(mode="json")
        connection_dict = schemas.Connection.model_validate(
            connection, from_attributes=True
        ).model_dump(mode="json")

        return sync_dict, collection_dict, connection_dict


async def main(apply: bool) -> None:
    """Scan incremental syncs and backfill missing cleanup schedules."""
    client = await temporal_client.get_client()

    async with AsyncSessionLocal() as db:
        # Find all incremental syncs
        result = await db.execute(
            select(SyncModel).where(SyncModel.sync_type == "incremental")
        )
        incremental_syncs = result.scalars().all()

    print(f"Found {len(incremental_syncs)} incremental sync(s)")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"{'=' * 60}")

    created = 0
    skipped = 0
    errors = 0

    for sync in incremental_syncs:
        schedule_id = f"daily-cleanup-{sync.id}"

        # Check if cleanup schedule already exists
        try:
            exists = await check_schedule_exists(client, schedule_id)
        except Exception as e:
            print(f"  ERROR checking {schedule_id}: {e}")
            errors += 1
            continue

        if exists:
            print(f"  SKIP {schedule_id} (already exists)")
            skipped += 1
            continue

        # Gather related data for the schedule
        try:
            data = await _gather_sync_data(sync)
        except Exception as e:
            print(f"  ERROR gathering data for {schedule_id}: {e}")
            errors += 1
            continue

        if data is None:
            print(f"  SKIP {schedule_id} (missing related records)")
            skipped += 1
            continue

        sync_dict, collection_dict, connection_dict = data

        # Build cleanup cron: daily, offset 12 hours from now
        now = datetime.now(timezone.utc)
        daily_cleanup_cron = f"{now.minute} {(now.hour + 12) % 24} * * *"

        print(f"  CREATE {schedule_id} (cron: {daily_cleanup_cron})")

        if apply:
            workflow_args: list = [
                sync_dict,
                None,  # no pre-created sync job
                collection_dict,
                connection_dict,
                {},  # empty ctx dict for system-initiated schedules
                None,  # no access token
                True,  # force_full_sync
            ]

            try:
                await client.create_schedule(
                    schedule_id,
                    Schedule(
                        action=ScheduleActionStartWorkflow(
                            RunSourceConnectionWorkflow.run,
                            args=workflow_args,
                            id=f"daily-cleanup-workflow-{sync.id}",
                            task_queue=settings.TEMPORAL_TASK_QUEUE,
                        ),
                        spec=ScheduleSpec(
                            cron_expressions=[daily_cleanup_cron],
                            start_at=datetime.now(timezone.utc),
                            end_at=None,
                            jitter=timedelta(minutes=30),
                        ),
                        state=ScheduleState(
                            note=f"Daily cleanup schedule for sync {sync.id}",
                            paused=False,
                        ),
                    ),
                )
                created += 1
            except Exception as e:
                print(f"  ERROR creating {schedule_id}: {e}")
                errors += 1

    print(f"\n{'=' * 60}")
    print(f"Results: {created} created, {skipped} skipped, {errors} errors")
    if not apply:
        print("DRY-RUN complete. Re-run with --apply to create schedules.")
    else:
        print("APPLY complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill daily cleanup schedules for incremental syncs"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Actually create schedules (default is dry-run)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.apply))
