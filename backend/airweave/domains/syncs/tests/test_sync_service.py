"""Table-driven tests for SyncService.

Covers the happy path (factory → orchestrator → run) and the error path
(factory raises → job marked FAILED → re-raised).
"""

from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from airweave.core.shared_models import SyncJobStatus
from airweave.domains.syncs.fakes.sync_job_service import FakeSyncJobService
from airweave.domains.syncs.service import SyncService


def _mock_ctx():
    ctx = MagicMock()
    ctx.organization = MagicMock()
    ctx.organization.id = uuid4()
    ctx.logger = MagicMock()
    ctx.logger.error = MagicMock()
    return ctx


def _mock_sync(sync_id=None):
    s = MagicMock()
    s.id = sync_id or uuid4()
    return s


def _mock_sync_job(job_id=None):
    j = MagicMock()
    j.id = job_id or uuid4()
    return j


# ---------------------------------------------------------------------------
# run() — table-driven
# ---------------------------------------------------------------------------


@dataclass
class RunCase:
    name: str
    factory_error: Optional[Exception] = None
    orchestrator_result: Optional[MagicMock] = field(default=None)
    expect_job_failed: bool = False
    expect_raises: bool = False

    def __post_init__(self):
        """Default orchestrator_result to a MagicMock when no factory error."""
        if self.orchestrator_result is None and self.factory_error is None:
            self.orchestrator_result = MagicMock()


RUN_CASES = [
    RunCase(
        name="happy_path",
    ),
    RunCase(
        name="factory_raises_marks_job_failed",
        factory_error=RuntimeError("bad config"),
        expect_job_failed=True,
        expect_raises=True,
    ),
    RunCase(
        name="factory_value_error",
        factory_error=ValueError("missing field"),
        expect_job_failed=True,
        expect_raises=True,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", RUN_CASES, ids=lambda c: c.name)
async def test_run(case: RunCase):
    fake_job_svc = FakeSyncJobService()
    fake_factory = MagicMock()

    mock_orchestrator = MagicMock()
    mock_orchestrator.run = AsyncMock(return_value=case.orchestrator_result)

    if case.factory_error:
        fake_factory.create_orchestrator = AsyncMock(
            side_effect=case.factory_error,
        )
    else:
        fake_factory.create_orchestrator = AsyncMock(
            return_value=mock_orchestrator,
        )

    svc = SyncService(
        sync_job_service=fake_job_svc,
        sync_factory=fake_factory,
        temporal_schedule_service=MagicMock(),
    )

    sync = _mock_sync()
    sync_job = _mock_sync_job()
    collection = MagicMock()
    source_connection = MagicMock()
    ctx = _mock_ctx()

    mock_db = AsyncMock()

    with patch(
        "airweave.domains.syncs.service.get_db_context",
    ) as mock_db_ctx:
        mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        if case.expect_raises:
            with pytest.raises(type(case.factory_error)):
                await svc.run(
                    sync=sync,
                    sync_job=sync_job,
                    collection=collection,
                    source_connection=source_connection,
                    ctx=ctx,
                )
        else:
            result = await svc.run(
                sync=sync,
                sync_job=sync_job,
                collection=collection,
                source_connection=source_connection,
                ctx=ctx,
            )
            assert result is case.orchestrator_result
            mock_orchestrator.run.assert_awaited_once()

    if case.expect_job_failed:
        assert len(fake_job_svc._calls) == 1
        call = fake_job_svc._calls[0]
        assert call[0] == "update_status"
        assert call[1] == sync_job.id
        assert call[2] == SyncJobStatus.FAILED
        assert call[5] == str(case.factory_error)
    else:
        assert len(fake_job_svc._calls) == 0


# ---------------------------------------------------------------------------
# run() — optional kwargs forwarded to factory
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_forwards_optional_kwargs():
    """force_full_sync, execution_config reach the factory."""
    fake_job_svc = FakeSyncJobService()
    fake_factory = MagicMock()

    mock_orchestrator = MagicMock()
    mock_orchestrator.run = AsyncMock(return_value=_mock_sync())
    fake_factory.create_orchestrator = AsyncMock(
        return_value=mock_orchestrator,
    )

    svc = SyncService(
        sync_job_service=fake_job_svc,
        sync_factory=fake_factory,
        temporal_schedule_service=MagicMock(),
    )

    mock_db = AsyncMock()
    exec_config = MagicMock()

    with patch(
        "airweave.domains.syncs.service.get_db_context",
    ) as mock_db_ctx:
        mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        await svc.run(
            sync=_mock_sync(),
            sync_job=_mock_sync_job(),
            collection=MagicMock(),
            source_connection=MagicMock(),
            ctx=_mock_ctx(),
            force_full_sync=True,
            execution_config=exec_config,
        )

        _, kwargs = fake_factory.create_orchestrator.call_args
        assert kwargs["force_full_sync"] is True
        assert kwargs["execution_config"] is exec_config


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_credential_error_propagates_error_category():
    """Factory raising a credential error → error_category set on job update."""
    from airweave.core.shared_models import SourceConnectionErrorCategory
    from airweave.domains.sources.exceptions import SourceValidationError
    from airweave.domains.sources.token_providers.exceptions import TokenExpiredError
    from airweave.domains.sources.token_providers.protocol import AuthProviderKind

    cause = TokenExpiredError(
        "JWT expired", source_short_name="github", provider_kind=AuthProviderKind.OAUTH
    )
    wrapper = SourceValidationError(short_name="github", reason="credential validation failed")
    wrapper.__cause__ = cause

    fake_job_svc = FakeSyncJobService()
    fake_factory = MagicMock()
    fake_factory.create_orchestrator = AsyncMock(side_effect=wrapper)

    svc = SyncService(sync_job_service=fake_job_svc, sync_factory=fake_factory, temporal_schedule_service=MagicMock())

    with patch("airweave.domains.syncs.service.get_db_context") as mock_db_ctx:
        mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(SourceValidationError):
            await svc.run(
                sync=_mock_sync(),
                sync_job=_mock_sync_job(),
                collection=MagicMock(),
                source_connection=MagicMock(),
                ctx=_mock_ctx(),
            )

    assert len(fake_job_svc._calls) == 1
    call = fake_job_svc._calls[0]
    # error_category is the last element in the tuple
    assert call[-1] == SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED


@pytest.mark.asyncio
async def test_non_credential_error_has_no_error_category():
    """Non-auth factory error → error_category=None on job update."""
    fake_job_svc = FakeSyncJobService()
    fake_factory = MagicMock()
    fake_factory.create_orchestrator = AsyncMock(side_effect=RuntimeError("bad config"))

    svc = SyncService(sync_job_service=fake_job_svc, sync_factory=fake_factory, temporal_schedule_service=MagicMock())

    with patch("airweave.domains.syncs.service.get_db_context") as mock_db_ctx:
        mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(RuntimeError):
            await svc.run(
                sync=_mock_sync(),
                sync_job=_mock_sync_job(),
                collection=MagicMock(),
                source_connection=MagicMock(),
                ctx=_mock_ctx(),
            )

    call = fake_job_svc._calls[0]
    assert call[-1] is None


def test_stores_injected_deps():
    fake_job = FakeSyncJobService()
    fake_factory = MagicMock()
    svc = SyncService(sync_job_service=fake_job, sync_factory=fake_factory, temporal_schedule_service=MagicMock())
    assert svc._sync_job_service is fake_job
    assert svc._sync_factory is fake_factory
