"""Smoke tests for schedule pausing and unpausing (credential error flows).

Tests the end-to-end behaviour:
- Paused schedules appear paused via Temporal REST API
- Unpaused schedules appear unpaused via Temporal REST API
- Credential update triggers unpause
- A paused schedule does NOT fire during a wait window

NOTE: Requires local environment with Temporal running on localhost:8088.
"""

import asyncio
import time
from typing import Dict, Optional

import httpx
import pytest  # noqa: I001

TEMPORAL_REST = "http://localhost:8088"
SCHEDULE_PREFIXES = ("sync-", "minute-sync-", "daily-cleanup-")


@pytest.mark.asyncio
@pytest.mark.requires_temporal
class TestSchedulePauseUnpause:
    """Tests for pausing and unpausing Temporal schedules via credential error flows."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _get_sync_id(
        self, api_client: httpx.AsyncClient, source_connection_id: str
    ) -> Optional[str]:
        response = await api_client.get(f"/source-connections/{source_connection_id}/sync-id")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["sync_id"]

    async def _get_schedule_detail(self, schedule_id: str) -> Optional[dict]:
        """Fetch a single schedule from the Temporal REST API."""
        url = f"{TEMPORAL_REST}/api/v1/namespaces/default/schedules/{schedule_id}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
        except httpx.ConnectError:
            pytest.skip("Temporal REST API not available")

    async def _is_schedule_paused(self, schedule_id: str) -> Optional[bool]:
        """Return True if the schedule is paused, False if running, None if not found."""
        detail = await self._get_schedule_detail(schedule_id)
        if detail is None:
            return None
        return detail.get("schedule", {}).get("state", {}).get("paused", False)

    async def _get_paused_states(self, sync_id: str) -> Dict[str, Optional[bool]]:
        """Return {prefix: paused_bool} for all schedule prefixes."""
        states = {}
        for prefix in SCHEDULE_PREFIXES:
            sid = f"{prefix}{sync_id}"
            states[sid] = await self._is_schedule_paused(sid)
        return states

    async def _trigger_sync_and_wait_for_failure(
        self,
        api_client: httpx.AsyncClient,
        conn_id: str,
        timeout: int = 60,
    ) -> dict:
        """Trigger a sync and wait for it to reach a terminal state."""
        resp = await api_client.post(f"/source-connections/{conn_id}/trigger-sync")
        resp.raise_for_status()

        elapsed = 0
        while elapsed < timeout:
            resp = await api_client.get(f"/source-connections/{conn_id}/jobs")
            resp.raise_for_status()
            jobs = resp.json()
            if jobs and jobs[0]["status"] in ("completed", "failed", "cancelled"):
                return jobs[0]
            await asyncio.sleep(2)
            elapsed += 2

        pytest.fail(f"Sync did not reach terminal state within {timeout}s")

    async def _create_stripe_connection_with_schedule(
        self,
        api_client: httpx.AsyncClient,
        collection: Dict,
        api_key: str,
        cron: str = "0 0 * * *",
    ) -> dict:
        """Create a Stripe connection with a cron schedule."""
        payload = {
            "name": f"Pause Test {int(time.time())}",
            "short_name": "stripe",
            "readable_collection_id": collection["readable_id"],
            "authentication": {"credentials": {"api_key": api_key}},
            "schedule": {"cron": cron},
            "sync_immediately": False,
        }
        resp = await api_client.post("/source-connections", json=payload)
        assert resp.status_code == 200, f"Failed to create connection: {resp.text}"
        return resp.json()

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    async def test_credential_error_pauses_schedules(
        self, api_client: httpx.AsyncClient, collection: Dict, config
    ):
        """A sync that fails with a credential error pauses the Temporal schedules."""
        if not config.is_local:
            pytest.skip("Temporal tests only run locally")

        # Create a connection with an invalid API key so the sync will fail
        # with a credential error (SourceAuthError / API_KEY_INVALID).
        connection = await self._create_stripe_connection_with_schedule(
            api_client, collection, api_key="sk_test_invalid_key_that_will_401"
        )
        conn_id = connection["id"]
        sync_id = await self._get_sync_id(api_client, conn_id)
        assert sync_id is not None

        # Wait for schedule to be registered in Temporal
        await asyncio.sleep(3)

        # Verify schedule exists and is NOT paused before the sync
        states_before = await self._get_paused_states(sync_id)
        primary_schedule = f"sync-{sync_id}"
        assert states_before[primary_schedule] is not None, "Schedule should exist"
        assert states_before[primary_schedule] is False, "Schedule should be running"

        # Trigger a sync — it should fail with a credential error
        job = await self._trigger_sync_and_wait_for_failure(api_client, conn_id)
        assert job["status"] == "failed"

        # Refetch the source connection to check status and error_category
        resp = await api_client.get(f"/source-connections/{conn_id}")
        resp.raise_for_status()
        sc = resp.json()
        assert sc["status"] == "needs_reauth", f"Expected needs_reauth, got {sc['status']}"
        assert sc["error_category"] is not None

        # Wait for the orchestrator to pause schedules
        await asyncio.sleep(3)

        # Verify schedule is now paused
        states_after = await self._get_paused_states(sync_id)
        assert states_after[primary_schedule] is True, (
            f"Schedule should be paused after credential error, got {states_after}"
        )

        # Cleanup
        await api_client.delete(f"/source-connections/{conn_id}")

    async def test_credential_update_unpauses_schedules(
        self, api_client: httpx.AsyncClient, collection: Dict, config
    ):
        """Updating credentials on a paused connection unpauses the schedules."""
        if not config.is_local:
            pytest.skip("Temporal tests only run locally")

        # Create connection with invalid key → sync fails → schedules pause
        connection = await self._create_stripe_connection_with_schedule(
            api_client, collection, api_key="sk_test_invalid_key_that_will_401"
        )
        conn_id = connection["id"]
        sync_id = await self._get_sync_id(api_client, conn_id)
        assert sync_id is not None

        await asyncio.sleep(3)

        # Trigger sync to cause credential failure and pausing
        await self._trigger_sync_and_wait_for_failure(api_client, conn_id)
        await asyncio.sleep(3)

        primary_schedule = f"sync-{sync_id}"
        paused = await self._is_schedule_paused(primary_schedule)
        assert paused is True, "Schedule should be paused after credential error"

        # Update credentials (still invalid, but the update itself triggers unpause)
        resp = await api_client.patch(
            f"/source-connections/{conn_id}",
            json={"authentication": {"credentials": {"api_key": "sk_test_new_key"}}},
        )
        assert resp.status_code == 200

        # Wait for unpause
        await asyncio.sleep(3)

        paused_after = await self._is_schedule_paused(primary_schedule)
        assert paused_after is False, "Schedule should be unpaused after credential update"

        # Cleanup
        await api_client.delete(f"/source-connections/{conn_id}")

    async def test_paused_schedule_does_not_fire(
        self, api_client: httpx.AsyncClient, collection: Dict, config
    ):
        """A paused minute-level schedule does not create new sync jobs."""
        if not config.is_local:
            pytest.skip("Temporal tests only run locally")

        # Create connection with invalid key and every-minute schedule
        connection = await self._create_stripe_connection_with_schedule(
            api_client,
            collection,
            api_key="sk_test_invalid_key_that_will_401",
            cron="* * * * *",
        )
        conn_id = connection["id"]
        sync_id = await self._get_sync_id(api_client, conn_id)
        assert sync_id is not None

        await asyncio.sleep(3)

        # Trigger sync to cause credential failure and pausing
        await self._trigger_sync_and_wait_for_failure(api_client, conn_id)
        await asyncio.sleep(3)

        # Verify paused
        primary_schedule = f"sync-{sync_id}"
        assert await self._is_schedule_paused(primary_schedule) is True

        # Count jobs before waiting
        resp = await api_client.get(f"/source-connections/{conn_id}/jobs")
        resp.raise_for_status()
        jobs_before = len(resp.json())

        # Wait 90 seconds — if the schedule were running, it would fire at least once
        await asyncio.sleep(90)

        resp = await api_client.get(f"/source-connections/{conn_id}/jobs")
        resp.raise_for_status()
        jobs_after = len(resp.json())

        assert jobs_after == jobs_before, (
            f"Paused schedule should not create new jobs. "
            f"Before: {jobs_before}, after: {jobs_after}"
        )

        # Cleanup
        await api_client.delete(f"/source-connections/{conn_id}")

    async def test_source_connection_status_needs_reauth(
        self, api_client: httpx.AsyncClient, collection: Dict, config
    ):
        """After credential failure, source connection shows needs_reauth + error fields."""
        if not config.is_local:
            pytest.skip("Temporal tests only run locally")

        connection = await self._create_stripe_connection_with_schedule(
            api_client, collection, api_key="sk_test_invalid_key_that_will_401"
        )
        conn_id = connection["id"]

        await asyncio.sleep(3)

        # Trigger sync to cause credential failure
        await self._trigger_sync_and_wait_for_failure(api_client, conn_id)

        # Fetch the connection
        resp = await api_client.get(f"/source-connections/{conn_id}")
        resp.raise_for_status()
        sc = resp.json()

        assert sc["status"] == "needs_reauth"
        assert sc["error_category"] == "api_key_invalid"
        assert sc["error_message"] is not None
        assert "API key" in sc["error_message"]

        # Cleanup
        await api_client.delete(f"/source-connections/{conn_id}")

    async def test_list_endpoint_shows_needs_reauth(
        self, api_client: httpx.AsyncClient, collection: Dict, config
    ):
        """The list endpoint derives NEEDS_REAUTH status for connections with credential errors."""
        if not config.is_local:
            pytest.skip("Temporal tests only run locally")

        connection = await self._create_stripe_connection_with_schedule(
            api_client, collection, api_key="sk_test_invalid_key_that_will_401"
        )
        conn_id = connection["id"]

        await asyncio.sleep(3)
        await self._trigger_sync_and_wait_for_failure(api_client, conn_id)

        # Fetch the list for this collection
        readable_id = collection["readable_id"]
        resp = await api_client.get(f"/source-connections?readable_collection_id={readable_id}")
        resp.raise_for_status()
        items = resp.json()

        matching = [item for item in items if item["id"] == conn_id]
        assert len(matching) == 1
        assert matching[0]["status"] == "needs_reauth"

        # Cleanup
        await api_client.delete(f"/source-connections/{conn_id}")
