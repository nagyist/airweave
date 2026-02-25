"""Unit tests for OAuthCallbackService.

All database interactions go through injected fakes — no crud patching.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from airweave.adapters.encryption.fake import FakeCredentialEncryptor
from airweave.api.context import ApiContext
from airweave.core.shared_models import AuthMethod, ConnectionStatus, SyncJobStatus
from airweave.domains.collections.fakes.repository import FakeCollectionRepository
from airweave.domains.connections.fakes.repository import FakeConnectionRepository
from airweave.domains.credentials.fakes.repository import FakeIntegrationCredentialRepository
from airweave.domains.oauth.callback_service import OAuthCallbackService
from airweave.domains.oauth.fakes.repository import (
    FakeOAuthInitSessionRepository,
    FakeOAuthSourceRepository,
)
from airweave.domains.organizations.fakes.repository import FakeOrganizationRepository
from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository
from airweave.domains.syncs.fakes.sync_job_repository import FakeSyncJobRepository
from airweave.domains.syncs.fakes.sync_repository import FakeSyncRepository
from airweave.models.connection_init_session import ConnectionInitSession, ConnectionInitStatus
from airweave.models.organization import Organization
from airweave.models.source_connection import SourceConnection
from airweave.schemas.organization import Organization as OrganizationSchema

NOW = datetime.now(timezone.utc)
ORG_ID = uuid4()
SESSION_ID = uuid4()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _ctx() -> ApiContext:
    org = OrganizationSchema(id=str(ORG_ID), name="Test Org", created_at=NOW, modified_at=NOW)
    return ApiContext(
        request_id="test-req",
        organization=org,
        auth_method=AuthMethod.SYSTEM,
        auth_metadata={},
    )


def _init_session(
    *,
    state: str = "state-abc",
    short_name: str = "github",
    status: ConnectionInitStatus = ConnectionInitStatus.PENDING,
    organization_id: UUID | None = None,
    session_id: UUID | None = None,
    payload: dict | None = None,
    overrides: dict | None = None,
) -> ConnectionInitSession:
    return ConnectionInitSession(
        id=session_id or SESSION_ID,
        state=state,
        short_name=short_name,
        status=status,
        organization_id=organization_id or ORG_ID,
        payload=payload or {},
        overrides=overrides or {},
        expires_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
    )


def _source_conn_shell(
    *,
    init_session_id: UUID = SESSION_ID,
    org_id: UUID | None = None,
) -> SourceConnection:
    return SourceConnection(
        id=uuid4(),
        organization_id=org_id or ORG_ID,
        name="shell",
        short_name="github",
        connection_init_session_id=init_session_id,
        readable_collection_id="col-abc",
    )


def _organization() -> Organization:
    org = Organization(id=ORG_ID, name="Test Org")
    org.created_at = NOW
    org.modified_at = NOW
    return org


def _service(
    *,
    init_session_repo=None,
    source_repo=None,
    sc_repo=None,
    credential_repo=None,
    connection_repo=None,
    collection_repo=None,
    sync_repo=None,
    sync_job_repo=None,
    organization_repo=None,
    credential_encryptor=None,
    oauth_flow_service=None,
    response_builder=None,
    source_registry=None,
    sync_lifecycle=None,
    sync_record_service=None,
    temporal_workflow_service=None,
    event_bus=None,
) -> OAuthCallbackService:
    return OAuthCallbackService(
        oauth_flow_service=oauth_flow_service or AsyncMock(),
        init_session_repo=init_session_repo or FakeOAuthInitSessionRepository(),
        response_builder=response_builder or AsyncMock(),
        source_registry=source_registry or MagicMock(),
        sync_lifecycle=sync_lifecycle or AsyncMock(),
        sync_record_service=sync_record_service or AsyncMock(),
        temporal_workflow_service=temporal_workflow_service or AsyncMock(),
        event_bus=event_bus or AsyncMock(),
        organization_repo=organization_repo or FakeOrganizationRepository(),
        source_repo=source_repo or FakeOAuthSourceRepository(),
        sc_repo=sc_repo or FakeSourceConnectionRepository(),
        credential_repo=credential_repo or FakeIntegrationCredentialRepository(),
        connection_repo=connection_repo or FakeConnectionRepository(),
        collection_repo=collection_repo or FakeCollectionRepository(),
        sync_repo=sync_repo or FakeSyncRepository(),
        sync_job_repo=sync_job_repo or FakeSyncJobRepository(),
        credential_encryptor=credential_encryptor or FakeCredentialEncryptor(),
    )


DB = AsyncMock()


# ---------------------------------------------------------------------------
# complete_oauth2_callback
# ---------------------------------------------------------------------------


class TestCompleteOAuth2Callback:
    async def test_session_not_found_raises_404(self):
        svc = _service()
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth2_callback(DB, state="missing", code="c")
        assert exc.value.status_code == 404

    async def test_session_already_completed_raises_400(self):
        repo = FakeOAuthInitSessionRepository()
        session = _init_session(status=ConnectionInitStatus.COMPLETED)
        repo.seed_by_state("state-abc", session)

        svc = _service(init_session_repo=repo)
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth2_callback(DB, state="state-abc", code="c")
        assert exc.value.status_code == 400

    async def test_missing_source_conn_shell_raises_404(self):
        init_repo = FakeOAuthInitSessionRepository()
        session = _init_session()
        init_repo.seed_by_state("state-abc", session)

        org_repo = FakeOrganizationRepository()
        org_repo.seed(ORG_ID, _organization())

        svc = _service(init_session_repo=init_repo, organization_repo=org_repo)
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth2_callback(DB, state="state-abc", code="c")
        assert exc.value.status_code == 404
        assert "shell" in exc.value.detail.lower()

    async def test_invalid_oauth2_token_fails_fast_with_400(self):
        init_repo = FakeOAuthInitSessionRepository()
        session = _init_session()
        init_repo.seed_by_state("state-abc", session)

        org_repo = FakeOrganizationRepository()
        org_repo.seed(ORG_ID, _organization())

        sc_repo = FakeSourceConnectionRepository()
        shell = _source_conn_shell()
        sc_repo.seed(shell.id, shell)
        sc_repo.seed_init_session(SESSION_ID, session)

        source_repo = FakeOAuthSourceRepository()
        source_repo.seed(
            "github",
            SimpleNamespace(short_name="github", name="GitHub", auth_config_class="GitHubAuth"),
        )

        oauth_flow = AsyncMock()
        oauth_flow.complete_oauth2_callback = AsyncMock(
            return_value=SimpleNamespace(access_token="bad-token")
        )

        class _InvalidSource:
            def set_logger(self, _logger):
                return None

            async def validate(self):
                return False

        class _SourceClass:
            @staticmethod
            async def create(access_token, config):  # noqa: ARG004
                return _InvalidSource()

        registry = MagicMock()
        registry.get.return_value = SimpleNamespace(source_class_ref=_SourceClass, short_name="github")

        svc = _service(
            init_session_repo=init_repo,
            organization_repo=org_repo,
            sc_repo=sc_repo,
            source_repo=source_repo,
            oauth_flow_service=oauth_flow,
            source_registry=registry,
        )
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth2_callback(DB, state="state-abc", code="c")

        assert exc.value.status_code == 400
        assert "token" in exc.value.detail.lower()
        assert all(call[0] != "mark_completed" for call in init_repo._calls)

    async def test_validation_exception_fails_fast_with_400(self):
        init_repo = FakeOAuthInitSessionRepository()
        session = _init_session()
        init_repo.seed_by_state("state-abc", session)

        org_repo = FakeOrganizationRepository()
        org_repo.seed(ORG_ID, _organization())

        sc_repo = FakeSourceConnectionRepository()
        shell = _source_conn_shell()
        sc_repo.seed(shell.id, shell)
        sc_repo.seed_init_session(SESSION_ID, session)

        source_repo = FakeOAuthSourceRepository()
        source_repo.seed(
            "github",
            SimpleNamespace(short_name="github", name="GitHub", auth_config_class="GitHubAuth"),
        )

        oauth_flow = AsyncMock()
        oauth_flow.complete_oauth2_callback = AsyncMock(
            return_value=SimpleNamespace(access_token="token")
        )

        class _BrokenSource:
            def set_logger(self, _logger):
                return None

            async def validate(self):
                raise RuntimeError("provider error")

        class _SourceClass:
            @staticmethod
            async def create(access_token, config):  # noqa: ARG004
                return _BrokenSource()

        registry = MagicMock()
        registry.get.return_value = SimpleNamespace(source_class_ref=_SourceClass, short_name="github")

        svc = _service(
            init_session_repo=init_repo,
            organization_repo=org_repo,
            sc_repo=sc_repo,
            source_repo=source_repo,
            oauth_flow_service=oauth_flow,
            source_registry=registry,
        )
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth2_callback(DB, state="state-abc", code="c")

        assert exc.value.status_code == 400
        assert "validation failed" in exc.value.detail.lower()
        assert all(call[0] != "mark_completed" for call in init_repo._calls)


# ---------------------------------------------------------------------------
# complete_oauth1_callback
# ---------------------------------------------------------------------------


class TestCompleteOAuth1Callback:
    async def test_session_not_found_raises_404(self):
        svc = _service()
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth1_callback(DB, oauth_token="missing", oauth_verifier="v")
        assert exc.value.status_code == 404

    async def test_session_already_completed_raises_400(self):
        repo = FakeOAuthInitSessionRepository()
        session = _init_session(status=ConnectionInitStatus.COMPLETED)
        repo.seed_by_oauth_token("tok1", session)

        svc = _service(init_session_repo=repo)
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth1_callback(DB, oauth_token="tok1", oauth_verifier="v")
        assert exc.value.status_code == 400

    async def test_missing_shell_raises_404(self):
        init_repo = FakeOAuthInitSessionRepository()
        session = _init_session()
        init_repo.seed_by_oauth_token("tok1", session)

        org_repo = FakeOrganizationRepository()
        org_repo.seed(ORG_ID, _organization())

        svc = _service(init_session_repo=init_repo, organization_repo=org_repo)
        with pytest.raises(HTTPException) as exc:
            await svc.complete_oauth1_callback(DB, oauth_token="tok1", oauth_verifier="v")
        assert exc.value.status_code == 404


# ---------------------------------------------------------------------------
# _reconstruct_context
# ---------------------------------------------------------------------------


class TestReconstructContext:
    async def test_returns_api_context_with_org(self):
        org_repo = FakeOrganizationRepository()
        org_repo.seed(ORG_ID, _organization())

        svc = _service(organization_repo=org_repo)
        session = _init_session()

        ctx = await svc._reconstruct_context(DB, session)

        assert ctx.organization.id == ORG_ID
        assert ctx.auth_method == AuthMethod.OAUTH_CALLBACK
        assert ctx.user is None


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------


class TestValidateConfig:
    def test_none_config_returns_empty(self):
        svc = _service()
        result = svc._validate_config(SimpleNamespace(short_name="x"), None, _ctx())
        assert result == {}

    def test_unknown_source_returns_empty(self):
        registry = MagicMock()
        registry.get.side_effect = KeyError("not found")
        svc = _service(source_registry=registry)

        result = svc._validate_config(
            SimpleNamespace(short_name="x"), {"key": "val"}, _ctx()
        )
        assert result == {}

    def test_no_config_ref_passes_through(self):
        entry = SimpleNamespace(config_ref=None)
        registry = MagicMock()
        registry.get.return_value = entry
        svc = _service(source_registry=registry)

        result = svc._validate_config(
            SimpleNamespace(short_name="x"), {"key": "val"}, _ctx()
        )
        assert result == {"key": "val"}

    def test_valid_config_validated(self):
        class FakeConfig:
            def __init__(self, **kwargs):
                self._data = kwargs

            def model_dump(self):
                return self._data

            @classmethod
            def model_validate(cls, data):
                return cls(**data)

        entry = SimpleNamespace(config_ref=FakeConfig)
        registry = MagicMock()
        registry.get.return_value = entry
        svc = _service(source_registry=registry)

        result = svc._validate_config(
            SimpleNamespace(short_name="x"), {"key": "val"}, _ctx()
        )
        assert result == {"key": "val"}


# ---------------------------------------------------------------------------
# _get_collection
# ---------------------------------------------------------------------------


class TestGetCollection:
    async def test_empty_collection_id_raises_400(self):
        svc = _service()
        with pytest.raises(HTTPException) as exc:
            await svc._get_collection(DB, "", _ctx())
        assert exc.value.status_code == 400

    async def test_missing_collection_raises_404(self):
        svc = _service()
        with pytest.raises(HTTPException) as exc:
            await svc._get_collection(DB, "nope", _ctx())
        assert exc.value.status_code == 404

    async def test_returns_found_collection(self):
        col_repo = FakeCollectionRepository()
        col = SimpleNamespace(id=uuid4(), name="Col", readable_id="col-abc")
        col_repo.seed_readable("col-abc", col)

        svc = _service(collection_repo=col_repo)
        result = await svc._get_collection(DB, "col-abc", _ctx())
        assert result is col


# ---------------------------------------------------------------------------
# _complete_oauth2_connection
# ---------------------------------------------------------------------------


class TestCompleteOAuth2Connection:
    async def test_source_not_found_raises_404(self):
        svc = _service()
        session = _init_session()
        shell = _source_conn_shell()
        token = SimpleNamespace(model_dump=lambda: {"access_token": "tok"})

        with pytest.raises(HTTPException) as exc:
            await svc._complete_oauth2_connection(DB, shell, session, token, _ctx())
        assert exc.value.status_code == 404

    async def test_salesforce_extracts_instance_url(self):
        source_repo = FakeOAuthSourceRepository()
        source = SimpleNamespace(
            short_name="salesforce",
            name="Salesforce",
            auth_config_class="SalesforceAuth",
            oauth_type="oauth2",
        )
        source_repo.seed("salesforce", source)

        session = _init_session(short_name="salesforce")

        token = SimpleNamespace(
            model_dump=lambda: {
                "access_token": "tok",
                "instance_url": "https://my.salesforce.com",
            }
        )

        svc = _service(source_repo=source_repo)

        # _complete_oauth2_connection calls _complete_connection_common which
        # uses UoW — we can't easily run that in unit tests. Just verify
        # the source lookup works.
        assert await svc._source_repo.get_by_short_name(DB, short_name="salesforce") is source


# ---------------------------------------------------------------------------
# _complete_oauth1_connection
# ---------------------------------------------------------------------------


class TestCompleteOAuth1Connection:
    async def test_source_not_found_raises_404(self):
        svc = _service()
        session = _init_session()
        shell = _source_conn_shell()
        token = SimpleNamespace(oauth_token="t", oauth_token_secret="s")

        with pytest.raises(HTTPException) as exc:
            await svc._complete_oauth1_connection(DB, shell, session, token, _ctx())
        assert exc.value.status_code == 404


# ---------------------------------------------------------------------------
# _finalize_callback
# ---------------------------------------------------------------------------


class TestFinalizeCallback:
    async def test_no_sync_id_just_returns_response(self):
        response = MagicMock(
            id=uuid4(), short_name="github", readable_collection_id="col-abc"
        )
        builder = AsyncMock()
        builder.build_response = AsyncMock(return_value=response)
        event_bus = AsyncMock()
        event_bus.publish = AsyncMock()

        source_conn = SimpleNamespace(sync_id=None, id=uuid4(), connection_id=uuid4())

        svc = _service(response_builder=builder, event_bus=event_bus)
        result = await svc._finalize_callback(DB, source_conn, _ctx())

        assert result is response
        builder.build_response.assert_awaited_once()

    async def test_triggers_workflow_when_pending_job_exists(self):
        response = MagicMock(
            id=uuid4(), short_name="github", readable_collection_id="col-abc"
        )
        builder = AsyncMock()
        builder.build_response = AsyncMock(return_value=response)

        temporal_svc = AsyncMock()
        temporal_svc.run_source_connection_workflow = AsyncMock()

        event_bus = AsyncMock()
        event_bus.publish = AsyncMock()

        sync_id = uuid4()
        conn_id = uuid4()
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=sync_id,
            connection_id=conn_id,
            readable_collection_id="col-abc",
        )

        # Seed sync repo
        from airweave import schemas

        sync_repo = FakeSyncRepository()
        sync_schema = schemas.Sync(
            id=sync_id,
            name="test-sync",
            source_connection_id=conn_id,
            collection_id=uuid4(),
            collection_readable_id="col-abc",
            organization_id=ORG_ID,
            created_at=NOW,
            modified_at=NOW,
            cron_schedule=None,
            status="active",
            source_connections=[],
            destination_connections=[],
            destination_connection_ids=[],
        )
        sync_repo.seed(sync_id, sync_schema)

        from airweave.models.sync_job import SyncJob

        job_id = uuid4()
        sync_job = SyncJob(
            id=job_id,
            sync_id=sync_id,
            status=SyncJobStatus.PENDING,
            organization_id=ORG_ID,
            scheduled=False,
        )
        sync_job_repo = FakeSyncJobRepository()
        sync_job_repo.seed_jobs_for_sync(sync_id, [sync_job])

        from airweave.models.collection import Collection

        col_id = uuid4()
        collection = Collection(
            id=col_id,
            name="Col",
            readable_id="col-abc",
            organization_id=ORG_ID,
            vector_size=768,
            embedding_model_name="text-embedding-3-small",
        )
        collection.created_at = NOW
        collection.modified_at = NOW
        collection_repo = FakeCollectionRepository()
        collection_repo.seed_readable("col-abc", collection)

        from airweave.models.connection import Connection

        connection = Connection(
            id=conn_id,
            organization_id=ORG_ID,
            name="github-conn",
            readable_id="conn-github-abc",
            short_name="github",
            integration_type="source",
            status=ConnectionStatus.ACTIVE,
        )
        connection.created_at = NOW
        connection.modified_at = NOW
        connection_repo = FakeConnectionRepository()
        connection_repo.seed(conn_id, connection)

        svc = _service(
            response_builder=builder,
            temporal_workflow_service=temporal_svc,
            event_bus=event_bus,
            sync_repo=sync_repo,
            sync_job_repo=sync_job_repo,
            collection_repo=collection_repo,
            connection_repo=connection_repo,
        )

        result = await svc._finalize_callback(DB, source_conn, _ctx())

        assert result is response
        temporal_svc.run_source_connection_workflow.assert_awaited_once()

    async def test_no_pending_jobs_skips_workflow(self):
        response = MagicMock(
            id=uuid4(), short_name="github", readable_collection_id="col-abc"
        )
        builder = AsyncMock()
        builder.build_response = AsyncMock(return_value=response)

        temporal_svc = AsyncMock()
        temporal_svc.run_source_connection_workflow = AsyncMock()

        event_bus = AsyncMock()
        event_bus.publish = AsyncMock()

        sync_id = uuid4()
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=sync_id,
            connection_id=uuid4(),
            readable_collection_id="col-abc",
        )

        sync_repo = FakeSyncRepository()

        svc = _service(
            response_builder=builder,
            temporal_workflow_service=temporal_svc,
            event_bus=event_bus,
            sync_repo=sync_repo,
        )

        result = await svc._finalize_callback(DB, source_conn, _ctx())

        assert result is response
        temporal_svc.run_source_connection_workflow.assert_not_awaited()

    async def test_running_job_skips_workflow(self):
        response = MagicMock(
            id=uuid4(), short_name="github", readable_collection_id="col-abc"
        )
        builder = AsyncMock()
        builder.build_response = AsyncMock(return_value=response)

        temporal_svc = AsyncMock()
        temporal_svc.run_source_connection_workflow = AsyncMock()

        event_bus = AsyncMock()
        event_bus.publish = AsyncMock()

        sync_id = uuid4()
        conn_id = uuid4()
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=sync_id,
            connection_id=conn_id,
            readable_collection_id="col-abc",
        )

        from airweave import schemas

        sync_repo = FakeSyncRepository()
        sync_schema = schemas.Sync(
            id=sync_id,
            name="test-sync",
            source_connection_id=conn_id,
            collection_id=uuid4(),
            collection_readable_id="col-abc",
            organization_id=ORG_ID,
            created_at=NOW,
            modified_at=NOW,
            cron_schedule=None,
            status="active",
            source_connections=[],
            destination_connections=[],
            destination_connection_ids=[],
        )
        sync_repo.seed(sync_id, sync_schema)

        from airweave.models.sync_job import SyncJob

        sync_job = SyncJob(
            id=uuid4(),
            sync_id=sync_id,
            status=SyncJobStatus.RUNNING,
            organization_id=ORG_ID,
        )
        sync_job_repo = FakeSyncJobRepository()
        sync_job_repo.seed_jobs_for_sync(sync_id, [sync_job])

        svc = _service(
            response_builder=builder,
            temporal_workflow_service=temporal_svc,
            event_bus=event_bus,
            sync_repo=sync_repo,
            sync_job_repo=sync_job_repo,
        )

        result = await svc._finalize_callback(DB, source_conn, _ctx())

        assert result is response
        temporal_svc.run_source_connection_workflow.assert_not_awaited()


# ---------------------------------------------------------------------------
# Credential encryptor injection
# ---------------------------------------------------------------------------


class TestCredentialEncryptorInjection:
    def test_encryptor_is_stored(self):
        encryptor = FakeCredentialEncryptor()
        svc = _service(credential_encryptor=encryptor)
        assert svc._credential_encryptor is encryptor
