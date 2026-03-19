"""Unit tests for CRUDSourceConnection.get_schedule_info()."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from airweave.crud.crud_source_connection import CRUDSourceConnection


def _make_source_connection(sync_id=None):
    sc = MagicMock()
    sc.sync_id = sync_id
    return sc


def _make_sync(cron_schedule=None):
    sync = MagicMock()
    sync.cron_schedule = cron_schedule
    sync.is_continuous = False
    sync.cursor_field = None
    sync.cursor_value = None
    return sync


def _make_db(sync=None):
    db = AsyncMock()
    db.get = AsyncMock(return_value=sync)
    return db


@pytest.fixture
def crud():
    return CRUDSourceConnection(MagicMock())


@pytest.mark.unit
class TestGetScheduleInfo:
    async def test_returns_none_when_no_sync_id(self, crud):
        sc = _make_source_connection(sync_id=None)
        result = await crud.get_schedule_info(AsyncMock(), sc)
        assert result is None

    async def test_returns_none_when_sync_not_found(self, crud):
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=None)
        result = await crud.get_schedule_info(db, sc)
        assert result is None

    async def test_next_run_is_none_when_no_cron_schedule(self, crud):
        sync = _make_sync(cron_schedule=None)
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert result is not None
        assert result["next_run_at"] is None
        assert result["cron_expression"] is None

    async def test_every_5_minutes_next_run_within_5_minutes(self, crud):
        sync = _make_sync(cron_schedule="*/5 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        now = datetime.now(timezone.utc)
        assert result["next_run_at"] is not None
        assert result["cron_expression"] == "*/5 * * * *"
        # next_run must be in the future and at most 5 minutes away
        assert result["next_run_at"] > now
        assert result["next_run_at"] <= now + timedelta(minutes=5, seconds=1)

    async def test_hourly_next_run_within_1_hour(self, crud):
        sync = _make_sync(cron_schedule="0 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        now = datetime.now(timezone.utc)
        assert result["next_run_at"] > now
        assert result["next_run_at"] <= now + timedelta(hours=1, seconds=1)

    async def test_daily_next_run_within_24_hours(self, crud):
        sync = _make_sync(cron_schedule="0 0 * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        now = datetime.now(timezone.utc)
        assert result["next_run_at"] > now
        assert result["next_run_at"] <= now + timedelta(hours=24, seconds=1)

    async def test_next_run_is_timezone_aware_utc(self, crud):
        sync = _make_sync(cron_schedule="*/5 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert result["next_run_at"].tzinfo is not None

    async def test_invalid_cron_expression_raises(self, crud):
        sync = _make_sync(cron_schedule="not a cron expression")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        with pytest.raises(ValueError):
            await crud.get_schedule_info(db, sc)

    async def test_too_few_fields_raises(self, crud):
        sync = _make_sync(cron_schedule="*/5 * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        with pytest.raises(ValueError):
            await crud.get_schedule_info(db, sc)

    async def test_empty_string_cron_returns_none_next_run(self, crud):
        sync = _make_sync(cron_schedule="")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        # Empty string is falsy, so cron_schedule block is skipped
        assert result["next_run_at"] is None

    async def test_result_includes_all_expected_keys(self, crud):
        sync = _make_sync(cron_schedule="*/10 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert set(result.keys()) == {
            "cron_expression",
            "next_run_at",
            "is_continuous",
            "cursor_field",
            "cursor_value",
        }

    async def test_next_run_always_fresh_not_stale(self, crud):
        """Calling get_schedule_info twice should return up-to-date next_run values,
        not a stale stored value."""
        sync = _make_sync(cron_schedule="*/5 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result1 = await crud.get_schedule_info(db, sc)
        result2 = await crud.get_schedule_info(db, sc)

        # Both calls should return the same next_run since time barely advanced
        assert result1["next_run_at"] == result2["next_run_at"]
        # And both should be in the future
        assert result1["next_run_at"] > datetime.now(timezone.utc) - timedelta(seconds=1)
