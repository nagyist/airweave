"""Unit tests for CRUDSourceConnection.get_schedule_info()."""

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

    async def test_cron_expression_is_none_when_no_schedule(self, crud):
        sync = _make_sync(cron_schedule=None)
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert result is not None
        assert result["cron_expression"] is None

    async def test_empty_string_cron_returns_empty_string(self, crud):
        sync = _make_sync(cron_schedule="")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert result["cron_expression"] == ""

    async def test_result_includes_all_expected_keys(self, crud):
        sync = _make_sync(cron_schedule="*/10 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert set(result.keys()) == {
            "cron_expression",
            "is_continuous",
            "cursor_field",
            "cursor_value",
        }

    async def test_returns_cron_expression_from_sync(self, crud):
        sync = _make_sync(cron_schedule="0 * * * *")
        sc = _make_source_connection(sync_id=uuid4())
        db = _make_db(sync=sync)

        result = await crud.get_schedule_info(db, sc)

        assert result["cron_expression"] == "0 * * * *"
        assert result["is_continuous"] is False
        assert result["cursor_field"] is None
        assert result["cursor_value"] is None
