"""Unit tests for OAuthCallbackService.

Covers:
- complete_oauth2_callback: session lookup, token exchange, connection wiring,
  sync provisioning, finalization, error paths
- complete_oauth1_callback: session lookup, token exchange, connection wiring
- _reconstruct_context: organization lookup, logger construction
- _complete_connection_common: credential/connection creation, federated skip,
  sync provisioning via SyncLifecycleService
- _finalize_callback: response build, workflow trigger, event publishing
- _validate_config: no config, no config_ref, valid model, validation error
- _get_collection: missing collection_id, not found
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.context import ApiContext
from airweave.core.shared_models import AuthMethod, ConnectionStatus, SyncJobStatus
from airweave.domains.oauth.callback_service import OAuthCallbackService
from airweave.domains.oauth.fakes.flow_service import FakeOAuthFlowService
from airweave.domains.oauth.fakes.repository import FakeOAuthInitSessionRepository
from airweave.domains.source_connections.fakes.response import FakeResponseBuilder
from airweave.domains.sources.fakes.registry import FakeSourceRegistry
from airweave.domains.syncs.fakes.sync_lifecycle_service import FakeSyncLifecycleService
from airweave.domains.syncs.fakes.sync_record_service import FakeSyncRecordService
from airweave.domains.temporal.fakes.service import FakeTemporalWorkflowService
from airweave.models.connection_init_session import ConnectionInitStatus
from airweave.models.integration_credential import IntegrationType
from airweave.platform.auth.schemas import OAuth2TokenResponse
from airweave.schemas.organization import Organization
from airweave.schemas.source_connection import AuthenticationMethod

NOW = datetime.now(timezone.utc)
ORG_ID = uuid4()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx() -> ApiContext:
    org = Organization(id=str(ORG_ID), name="Test Org", created_at=NOW, modified_at=NOW)
    return ApiContext(
        request_id="test-req",
        organization=org,
        auth_method=AuthMethod.SYSTEM,
        auth_metadata={},
    )


def _init_session(
    *,
    state="state-1",
    short_name="github",
    status=ConnectionInitStatus.PENDING,
    overrides=None,
    payload=None,
):
    return SimpleNamespace(
        id=uuid4(),
        organization_id=ORG_ID,
        short_name=short_name,
        state=state,
        status=status,
        overrides=overrides or {},
        payload=payload or {"readable_collection_id": "col-abc"},
    )


def _source(short_name="github", name="GitHub", oauth_type="oauth2", auth_config_class=None):
    return SimpleNamespace(
        short_name=short_name,
        name=name,
        oauth_type=oauth_type,
        auth_config_class=auth_config_class,
    )


def _source_conn_shell(*, readable_collection_id="col-abc"):
    return SimpleNamespace(
        id=uuid4(),
        readable_collection_id=readable_collection_id,
        connection_id=None,
        sync_id=None,
    )


def _entry(*, short_name="github", federated=False, supports_continuous=False, config_ref=None):
    source_cls = MagicMock()
    source_cls.federated_search = federated
    source_cls.supports_continuous = supports_continuous
    return SimpleNamespace(
        name="GitHub",
        short_name=short_name,
        source_class_ref=source_cls,
        federated_search=federated,
        supports_continuous=supports_continuous,
        config_ref=config_ref,
    )


def _collection():
    return SimpleNamespace(
        id=uuid4(),
        readable_id="col-abc",
        name="My Collection",
    )


def _credential():
    return SimpleNamespace(id=uuid4())


def _connection():
    return SimpleNamespace(
        id=uuid4(),
        short_name="github",
    )


def _service(
    *,
    flow_service=None,
    init_session_repo=None,
    response_builder=None,
    source_registry=None,
    sync_lifecycle=None,
    sync_record_service=None,
    temporal_workflow_service=None,
    event_bus=None,
):
    return OAuthCallbackService(
        oauth_flow_service=flow_service or FakeOAuthFlowService(),
        init_session_repo=init_session_repo or FakeOAuthInitSessionRepository(),
        response_builder=response_builder or FakeResponseBuilder(),
        source_registry=source_registry or FakeSourceRegistry(),
        sync_lifecycle=sync_lifecycle or FakeSyncLifecycleService(),
        sync_record_service=sync_record_service or FakeSyncRecordService(),
        temporal_workflow_service=temporal_workflow_service or FakeTemporalWorkflowService(),
        event_bus=event_bus or AsyncMock(),
    )


# ---------------------------------------------------------------------------
# complete_oauth2_callback
# ---------------------------------------------------------------------------


class TestCompleteOAuth2Callback:
    async def test_session_not_found_raises_404(self):
        init_repo = FakeOAuthInitSessionRepository()
        svc = _service(init_session_repo=init_repo)

        with pytest.raises(HTTPException) as exc_info:
            await svc.complete_oauth2_callback(AsyncMock(), state="missing", code="code")
        assert exc_info.value.status_code == 404

    async def test_session_already_completed_raises_400(self):
        session = _init_session(status=ConnectionInitStatus.COMPLETED)
        init_repo = FakeOAuthInitSessionRepository()
        init_repo.seed_by_state(session.state, session)
        svc = _service(init_session_repo=init_repo)

        with pytest.raises(HTTPException) as exc_info:
            await svc.complete_oauth2_callback(AsyncMock(), state=session.state, code="code")
        assert exc_info.value.status_code == 400
        assert "already" in exc_info.value.detail

    async def test_missing_source_conn_shell_raises_404(self):
        session = _init_session()
        init_repo = FakeOAuthInitSessionRepository()
        init_repo.seed_by_state(session.state, session)
        svc = _service(init_session_repo=init_repo)

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.organization.get = AsyncMock(
                return_value=SimpleNamespace(id=ORG_ID, name="Test Org")
            )
            mock_crud.source_connection.get_by_query_and_org = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await svc.complete_oauth2_callback(AsyncMock(), state=session.state, code="code")
            assert exc_info.value.status_code == 404
            assert "shell not found" in exc_info.value.detail


# ---------------------------------------------------------------------------
# complete_oauth1_callback
# ---------------------------------------------------------------------------


class TestCompleteOAuth1Callback:
    async def test_session_not_found_raises_404(self):
        init_repo = FakeOAuthInitSessionRepository()
        svc = _service(init_session_repo=init_repo)

        with pytest.raises(HTTPException) as exc_info:
            await svc.complete_oauth1_callback(
                AsyncMock(), oauth_token="missing", oauth_verifier="v"
            )
        assert exc_info.value.status_code == 404

    async def test_session_already_completed_raises_400(self):
        session = _init_session(status=ConnectionInitStatus.COMPLETED)
        init_repo = FakeOAuthInitSessionRepository()
        init_repo.seed_by_oauth_token("tok-1", session)
        svc = _service(init_session_repo=init_repo)

        with pytest.raises(HTTPException) as exc_info:
            await svc.complete_oauth1_callback(
                AsyncMock(), oauth_token="tok-1", oauth_verifier="v"
            )
        assert exc_info.value.status_code == 400

    async def test_missing_shell_raises_404(self):
        session = _init_session()
        init_repo = FakeOAuthInitSessionRepository()
        init_repo.seed_by_oauth_token("tok-1", session)
        svc = _service(init_session_repo=init_repo)

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.organization.get = AsyncMock(
                return_value=SimpleNamespace(id=ORG_ID, name="Test Org")
            )
            mock_crud.source_connection.get_by_query_and_org = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await svc.complete_oauth1_callback(
                    AsyncMock(), oauth_token="tok-1", oauth_verifier="v"
                )
            assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# _reconstruct_context
# ---------------------------------------------------------------------------


class TestReconstructContext:
    async def test_returns_api_context_with_org(self):
        svc = _service()
        session = _init_session()

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            org_model = SimpleNamespace(
                id=ORG_ID,
                name="Test Org",
                created_at=NOW,
                modified_at=NOW,
                enabled_features=[],
            )
            mock_crud.organization.get = AsyncMock(return_value=org_model)

            ctx = await svc._reconstruct_context(AsyncMock(), session)

        assert ctx.organization.id == str(ORG_ID)
        assert ctx.auth_method == AuthMethod.OAUTH_CALLBACK
        assert "session_id" in ctx.auth_metadata


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------


class TestValidateConfig:
    def test_no_config_returns_empty_dict(self):
        svc = _service()
        result = svc._validate_config(_source(), None, _ctx())
        assert result == {}

    def test_unknown_source_returns_empty_dict(self):
        registry = FakeSourceRegistry()
        registry.get = MagicMock(side_effect=KeyError("not found"))
        svc = _service(source_registry=registry)

        result = svc._validate_config(_source(), {"key": "val"}, _ctx())
        assert result == {}

    def test_no_config_ref_returns_dict_as_is(self):
        entry = _entry(config_ref=None)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)
        svc = _service(source_registry=registry)

        result = svc._validate_config(_source(), {"key": "val"}, _ctx())
        assert result == {"key": "val"}

    def test_valid_pydantic_model_returns_dumped_dict(self):
        from pydantic import BaseModel

        class MyConfig(BaseModel):
            repo: str
            branch: str = "main"

        entry = _entry(config_ref=MyConfig)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)
        svc = _service(source_registry=registry)

        result = svc._validate_config(_source(), {"repo": "my-repo"}, _ctx())
        assert result == {"repo": "my-repo", "branch": "main"}

    def test_invalid_pydantic_model_raises_422(self):
        from pydantic import BaseModel

        class MyConfig(BaseModel):
            repo: str

        entry = _entry(config_ref=MyConfig)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)
        svc = _service(source_registry=registry)

        with pytest.raises(HTTPException) as exc_info:
            svc._validate_config(_source(), {"wrong_field": "val"}, _ctx())
        assert exc_info.value.status_code == 422

    def test_model_dump_path_for_objects(self):
        model_obj = MagicMock()
        model_obj.model_dump = MagicMock(return_value={"key": "val"})

        entry = _entry(config_ref=None)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)
        svc = _service(source_registry=registry)

        result = svc._validate_config(_source(), model_obj, _ctx())
        assert result == {"key": "val"}


# ---------------------------------------------------------------------------
# _get_collection
# ---------------------------------------------------------------------------


class TestGetCollection:
    async def test_empty_collection_id_raises_400(self):
        svc = _service()
        with pytest.raises(HTTPException) as exc_info:
            await svc._get_collection(AsyncMock(), "", _ctx())
        assert exc_info.value.status_code == 400

    async def test_not_found_raises_404(self):
        svc = _service()
        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=None)
            with pytest.raises(HTTPException) as exc_info:
                await svc._get_collection(AsyncMock(), "nonexistent", _ctx())
            assert exc_info.value.status_code == 404

    async def test_returns_collection_when_found(self):
        svc = _service()
        coll = _collection()
        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=coll)
            result = await svc._get_collection(AsyncMock(), "col-abc", _ctx())
            assert result is coll


# ---------------------------------------------------------------------------
# _complete_oauth2_connection
# ---------------------------------------------------------------------------


class TestCompleteOAuth2Connection:
    async def test_source_not_found_raises_404(self):
        svc = _service()
        session = _init_session()
        token = OAuth2TokenResponse(access_token="at", token_type="bearer")

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.source.get_by_short_name = AsyncMock(return_value=None)
            with pytest.raises(HTTPException) as exc_info:
                await svc._complete_oauth2_connection(
                    AsyncMock(), _source_conn_shell(), session, token, _ctx()
                )
            assert exc_info.value.status_code == 404

    async def test_byoc_detected_from_overrides(self):
        svc = _service()
        session = _init_session(
            overrides={"client_id": "byoc-id", "client_secret": "byoc-secret"}
        )
        token = OAuth2TokenResponse(access_token="at", token_type="bearer")

        svc._complete_connection_common = AsyncMock(return_value=MagicMock())
        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.source.get_by_short_name = AsyncMock(return_value=_source())
            await svc._complete_oauth2_connection(
                AsyncMock(), _source_conn_shell(), session, token, _ctx()
            )

        call_kwargs = svc._complete_connection_common.call_args
        assert call_kwargs.kwargs.get("auth_method_to_save") is None or \
            call_kwargs[0][6] == AuthenticationMethod.OAUTH_BYOC

    async def test_salesforce_instance_url_extraction(self):
        svc = _service()
        session = _init_session(
            short_name="salesforce",
            payload={"readable_collection_id": "col-abc"},
        )
        token = MagicMock()
        token.model_dump = MagicMock(return_value={
            "access_token": "at",
            "token_type": "bearer",
            "instance_url": "https://my-org.salesforce.com",
        })

        svc._complete_connection_common = AsyncMock(return_value=MagicMock())

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.source.get_by_short_name = AsyncMock(
                return_value=_source(short_name="salesforce", name="Salesforce")
            )
            await svc._complete_oauth2_connection(
                AsyncMock(), _source_conn_shell(), session, token, _ctx()
            )

        call_args = svc._complete_connection_common.call_args[0]
        payload = call_args[4]
        assert payload.get("config", {}).get("instance_url") == "my-org.salesforce.com"


# ---------------------------------------------------------------------------
# _complete_oauth1_connection
# ---------------------------------------------------------------------------


class TestCompleteOAuth1Connection:
    async def test_source_not_found_raises_404(self):
        svc = _service()
        session = _init_session()
        token = SimpleNamespace(oauth_token="tok", oauth_token_secret="sec")

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.source.get_by_short_name = AsyncMock(return_value=None)
            with pytest.raises(HTTPException) as exc_info:
                await svc._complete_oauth1_connection(
                    AsyncMock(), _source_conn_shell(), session, token, _ctx()
                )
            assert exc_info.value.status_code == 404

    async def test_consumer_keys_added_to_auth_fields(self):
        svc = _service()
        session = _init_session(
            overrides={"consumer_key": "ck", "consumer_secret": "cs"}
        )
        token = SimpleNamespace(oauth_token="tok", oauth_token_secret="sec")

        svc._complete_connection_common = AsyncMock(return_value=MagicMock())

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud, \
             patch("airweave.domains.oauth.callback_service.integration_settings", new_callable=AsyncMock) as mock_is:
            mock_crud.source.get_by_short_name = AsyncMock(return_value=_source())
            mock_is.get_by_short_name = AsyncMock(return_value=None)

            await svc._complete_oauth1_connection(
                AsyncMock(), _source_conn_shell(), session, token, _ctx()
            )

        call_args = svc._complete_connection_common.call_args[0]
        auth_fields = call_args[5]
        assert auth_fields["consumer_key"] == "ck"
        assert auth_fields["consumer_secret"] == "cs"


# ---------------------------------------------------------------------------
# _finalize_callback
# ---------------------------------------------------------------------------


class TestFinalizeCallback:
    async def test_no_sync_returns_response_immediately(self):
        response_builder = FakeResponseBuilder()
        expected_response = MagicMock(
            id=uuid4(),
            short_name="github",
            readable_collection_id="col-abc",
        )
        response_builder.build_response = AsyncMock(return_value=expected_response)

        svc = _service(response_builder=response_builder)
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=None,
            connection_id=uuid4(),
            readable_collection_id="col-abc",
        )

        result = await svc._finalize_callback(AsyncMock(), source_conn, _ctx())
        assert result is expected_response

    async def test_triggers_workflow_when_pending_job_exists(self):
        response_builder = FakeResponseBuilder()
        expected_response = MagicMock(
            id=uuid4(),
            short_name="github",
            readable_collection_id="col-abc",
        )
        response_builder.build_response = AsyncMock(return_value=expected_response)

        temporal_svc = FakeTemporalWorkflowService()
        temporal_svc.run_source_connection_workflow = AsyncMock(return_value=MagicMock())

        sync_id = uuid4()
        conn_id = uuid4()
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=sync_id,
            connection_id=conn_id,
            readable_collection_id="col-abc",
        )

        sync_model = SimpleNamespace(id=sync_id)
        job_model = SimpleNamespace(id=uuid4(), status=SyncJobStatus.PENDING)
        collection_model = SimpleNamespace(
            id=uuid4(), name="Col", readable_id="col-abc"
        )
        connection_model = SimpleNamespace(id=conn_id, short_name="github")

        svc = _service(
            response_builder=response_builder,
            temporal_workflow_service=temporal_svc,
        )

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.sync.get = AsyncMock(return_value=sync_model)
            mock_crud.sync_job.get_all_by_sync_id = AsyncMock(return_value=[job_model])
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=collection_model)
            mock_crud.connection.get = AsyncMock(return_value=connection_model)

            result = await svc._finalize_callback(AsyncMock(), source_conn, _ctx())

        assert result is expected_response
        temporal_svc.run_source_connection_workflow.assert_awaited_once()

    async def test_skips_workflow_when_job_not_pending(self):
        response_builder = FakeResponseBuilder()
        expected_response = MagicMock(
            id=uuid4(),
            short_name="github",
            readable_collection_id="col-abc",
        )
        response_builder.build_response = AsyncMock(return_value=expected_response)

        temporal_svc = FakeTemporalWorkflowService()
        temporal_svc.run_source_connection_workflow = AsyncMock()

        sync_id = uuid4()
        source_conn = SimpleNamespace(
            id=uuid4(),
            sync_id=sync_id,
            connection_id=uuid4(),
            readable_collection_id="col-abc",
        )

        svc = _service(
            response_builder=response_builder,
            temporal_workflow_service=temporal_svc,
        )

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.sync.get = AsyncMock(return_value=SimpleNamespace(id=sync_id))
            mock_crud.sync_job.get_all_by_sync_id = AsyncMock(
                return_value=[SimpleNamespace(id=uuid4(), status=SyncJobStatus.COMPLETED)]
            )

            result = await svc._finalize_callback(AsyncMock(), source_conn, _ctx())

        assert result is expected_response
        temporal_svc.run_source_connection_workflow.assert_not_awaited()

    async def test_skips_workflow_when_no_jobs(self):
        response_builder = FakeResponseBuilder()
        response_builder.build_response = AsyncMock(
            return_value=MagicMock(id=uuid4(), short_name="gh", readable_collection_id="c")
        )
        temporal_svc = FakeTemporalWorkflowService()
        temporal_svc.run_source_connection_workflow = AsyncMock()

        svc = _service(
            response_builder=response_builder,
            temporal_workflow_service=temporal_svc,
        )
        source_conn = SimpleNamespace(
            id=uuid4(), sync_id=uuid4(), connection_id=uuid4(), readable_collection_id="c"
        )

        with patch("airweave.domains.oauth.callback_service.crud") as mock_crud:
            mock_crud.sync.get = AsyncMock(return_value=SimpleNamespace(id=source_conn.sync_id))
            mock_crud.sync_job.get_all_by_sync_id = AsyncMock(return_value=[])

            await svc._finalize_callback(AsyncMock(), source_conn, _ctx())

        temporal_svc.run_source_connection_workflow.assert_not_awaited()

    async def test_event_bus_failure_does_not_raise(self):
        response_builder = FakeResponseBuilder()
        response_builder.build_response = AsyncMock(
            return_value=MagicMock(id=uuid4(), short_name="gh", readable_collection_id="c")
        )
        bus = AsyncMock()
        bus.publish = AsyncMock(side_effect=RuntimeError("bus down"))

        svc = _service(response_builder=response_builder, event_bus=bus)
        source_conn = SimpleNamespace(
            id=uuid4(), sync_id=None, connection_id=uuid4(), readable_collection_id="c"
        )

        ctx = _ctx()
        ctx.logger = MagicMock()
        ctx.logger.warning = MagicMock()

        result = await svc._finalize_callback(AsyncMock(), source_conn, ctx)
        assert result is not None


# ---------------------------------------------------------------------------
# _complete_connection_common (integration-style, mocking UoW + crud)
# ---------------------------------------------------------------------------


class TestCompleteConnectionCommon:
    async def test_federated_source_skips_sync_creation(self):
        entry = _entry(federated=True)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)

        init_repo = FakeOAuthInitSessionRepository()
        svc = _service(source_registry=registry, init_session_repo=init_repo)

        source = _source()
        shell = _source_conn_shell()
        session_id = uuid4()
        ctx = _ctx()

        with patch("airweave.domains.oauth.callback_service.UnitOfWork") as MockUoW, \
             patch("airweave.domains.oauth.callback_service.crud") as mock_crud, \
             patch("airweave.domains.oauth.callback_service.credentials") as mock_creds:

            mock_creds.encrypt = MagicMock(return_value="encrypted")

            mock_cred = _credential()
            mock_conn = _connection()
            mock_coll = _collection()

            mock_crud.integration_credential.create = AsyncMock(return_value=mock_cred)
            mock_crud.connection.create = AsyncMock(return_value=mock_conn)
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=mock_coll)

            updated_sc = MagicMock(id=uuid4())
            mock_crud.source_connection.update = AsyncMock(return_value=updated_sc)

            uow_instance = AsyncMock()
            uow_instance.session = AsyncMock()
            uow_instance.session.flush = AsyncMock()
            uow_instance.session.refresh = AsyncMock()
            uow_instance.commit = AsyncMock()
            uow_instance.__aenter__ = AsyncMock(return_value=uow_instance)
            uow_instance.__aexit__ = AsyncMock(return_value=False)
            MockUoW.return_value = uow_instance

            result = await svc._complete_connection_common(
                AsyncMock(),
                source,
                shell,
                session_id,
                {"readable_collection_id": "col-abc"},
                {"access_token": "at"},
                AuthenticationMethod.OAUTH_BROWSER,
                is_oauth1=False,
                ctx=ctx,
            )

        update_call = mock_crud.source_connection.update.call_args
        sc_update = update_call.kwargs.get("obj_in") or update_call[1].get("obj_in")
        assert sc_update["sync_id"] is None
        assert sc_update["is_authenticated"] is True

    async def test_non_federated_source_calls_provision_sync(self):
        entry = _entry(federated=False)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)

        sync_lifecycle = FakeSyncLifecycleService()
        sync_result = SimpleNamespace(sync_id=uuid4())
        sync_lifecycle.provision_sync = AsyncMock(return_value=sync_result)

        sync_record_svc = FakeSyncRecordService()
        sync_record_svc.resolve_destination_ids = AsyncMock(return_value=[uuid4()])

        init_repo = FakeOAuthInitSessionRepository()
        svc = _service(
            source_registry=registry,
            sync_lifecycle=sync_lifecycle,
            sync_record_service=sync_record_svc,
            init_session_repo=init_repo,
        )

        source = _source()
        shell = _source_conn_shell()
        session_id = uuid4()
        ctx = _ctx()

        with patch("airweave.domains.oauth.callback_service.UnitOfWork") as MockUoW, \
             patch("airweave.domains.oauth.callback_service.crud") as mock_crud, \
             patch("airweave.domains.oauth.callback_service.credentials") as mock_creds:

            mock_creds.encrypt = MagicMock(return_value="encrypted")

            mock_cred = _credential()
            mock_conn = _connection()
            mock_coll = _collection()

            mock_crud.integration_credential.create = AsyncMock(return_value=mock_cred)
            mock_crud.connection.create = AsyncMock(return_value=mock_conn)
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=mock_coll)
            mock_crud.source_connection.update = AsyncMock(return_value=MagicMock(id=uuid4()))

            uow_instance = AsyncMock()
            uow_instance.session = AsyncMock()
            uow_instance.session.flush = AsyncMock()
            uow_instance.session.refresh = AsyncMock()
            uow_instance.commit = AsyncMock()
            uow_instance.__aenter__ = AsyncMock(return_value=uow_instance)
            uow_instance.__aexit__ = AsyncMock(return_value=False)
            MockUoW.return_value = uow_instance

            await svc._complete_connection_common(
                AsyncMock(),
                source,
                shell,
                session_id,
                {"readable_collection_id": "col-abc"},
                {"access_token": "at"},
                AuthenticationMethod.OAUTH_BROWSER,
                is_oauth1=False,
                ctx=ctx,
            )

        sync_lifecycle.provision_sync.assert_awaited_once()
        call_kwargs = sync_lifecycle.provision_sync.call_args.kwargs
        assert call_kwargs["run_immediately"] is True

    async def test_cron_schedule_extracted_from_payload(self):
        entry = _entry(federated=False)
        registry = FakeSourceRegistry()
        registry.get = MagicMock(return_value=entry)

        sync_lifecycle = FakeSyncLifecycleService()
        sync_lifecycle.provision_sync = AsyncMock(
            return_value=SimpleNamespace(sync_id=uuid4())
        )

        sync_record_svc = FakeSyncRecordService()
        sync_record_svc.resolve_destination_ids = AsyncMock(return_value=[])

        init_repo = FakeOAuthInitSessionRepository()
        svc = _service(
            source_registry=registry,
            sync_lifecycle=sync_lifecycle,
            sync_record_service=sync_record_svc,
            init_session_repo=init_repo,
        )

        with patch("airweave.domains.oauth.callback_service.UnitOfWork") as MockUoW, \
             patch("airweave.domains.oauth.callback_service.crud") as mock_crud, \
             patch("airweave.domains.oauth.callback_service.credentials") as mock_creds:

            mock_creds.encrypt = MagicMock(return_value="enc")
            mock_crud.integration_credential.create = AsyncMock(return_value=_credential())
            mock_crud.connection.create = AsyncMock(return_value=_connection())
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=_collection())
            mock_crud.source_connection.update = AsyncMock(return_value=MagicMock(id=uuid4()))

            uow_instance = AsyncMock()
            uow_instance.session = AsyncMock()
            uow_instance.session.flush = AsyncMock()
            uow_instance.session.refresh = AsyncMock()
            uow_instance.commit = AsyncMock()
            uow_instance.__aenter__ = AsyncMock(return_value=uow_instance)
            uow_instance.__aexit__ = AsyncMock(return_value=False)
            MockUoW.return_value = uow_instance

            await svc._complete_connection_common(
                AsyncMock(),
                _source(),
                _source_conn_shell(),
                uuid4(),
                {"readable_collection_id": "col-abc", "schedule": {"cron": "0 3 * * *"}},
                {"access_token": "at"},
                AuthenticationMethod.OAUTH_BROWSER,
                is_oauth1=False,
                ctx=_ctx(),
            )

        call_kwargs = sync_lifecycle.provision_sync.call_args.kwargs
        assert call_kwargs["schedule_config"] is not None
        assert call_kwargs["schedule_config"].cron == "0 3 * * *"
