"""Unit tests for ResponseBuilder._build_schedule_details()."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from airweave.domains.source_connections.response import ResponseBuilder


def _make_builder(schedule_info=None):
    """Create a ResponseBuilder with a mocked source connection repo."""
    sc_repo = MagicMock()
    sc_repo.get_schedule_info = AsyncMock(return_value=schedule_info)
    builder = ResponseBuilder(
        sc_repo=sc_repo,
        connection_repo=MagicMock(),
        credential_repo=MagicMock(),
        source_registry=MagicMock(),
        entity_count_repo=MagicMock(),
        sync_job_repo=MagicMock(),
    )
    return builder


def _make_source_conn(sync_id=None):
    sc = MagicMock()
    sc.sync_id = sync_id
    return sc


def _make_ctx():
    ctx = MagicMock()
    ctx.logger = MagicMock()
    return ctx


@pytest.mark.unit
class TestBuildScheduleDetails:
    async def test_returns_none_when_no_sync_id(self):
        builder = _make_builder()
        sc = _make_source_conn(sync_id=None)

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result is None

    async def test_returns_none_when_no_schedule_info(self):
        builder = _make_builder(schedule_info=None)
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result is None

    async def test_next_run_is_none_when_no_cron_expression(self):
        builder = _make_builder(schedule_info={
            "cron_expression": None,
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result is not None
        assert result.next_run is None
        assert result.cron is None

    async def test_computes_next_run_from_cron(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "*/5 * * * *",
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        now = datetime.now(timezone.utc)
        assert result is not None
        assert result.cron == "*/5 * * * *"
        assert result.next_run is not None
        assert result.next_run > now
        assert result.next_run <= now + timedelta(minutes=5, seconds=1)

    async def test_hourly_next_run_within_1_hour(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "0 * * * *",
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        now = datetime.now(timezone.utc)
        assert result.next_run > now
        assert result.next_run <= now + timedelta(hours=1, seconds=1)

    async def test_next_run_is_timezone_aware(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "*/5 * * * *",
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result.next_run.tzinfo is not None

    async def test_invalid_cron_expression_logs_warning(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "not a cron expression",
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())
        ctx = _make_ctx()

        result = await builder._build_schedule_details(AsyncMock(), sc, ctx)

        # Invalid cron is caught by the except block and logged as warning
        assert result is None
        ctx.logger.warning.assert_called_once()

    async def test_passes_through_continuous_and_cursor_fields(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "0 0 * * *",
            "is_continuous": True,
            "cursor_field": "updated_at",
            "cursor_value": "2026-01-01",
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result.continuous is True
        assert result.cursor_field == "updated_at"
        assert result.cursor_value == "2026-01-01"

    async def test_empty_string_cron_returns_none_next_run(self):
        builder = _make_builder(schedule_info={
            "cron_expression": "",
            "is_continuous": False,
            "cursor_field": None,
            "cursor_value": None,
        })
        sc = _make_source_conn(sync_id=uuid4())

        result = await builder._build_schedule_details(AsyncMock(), sc, _make_ctx())

        assert result is not None
        assert result.next_run is None
