"""Unit tests for SourceConnectionCreationService."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from airweave.api.context import ApiContext
from airweave.core.shared_models import AuthMethod
from airweave.domains.collections.fakes.repository import FakeCollectionRepository
from airweave.domains.connections.fakes.repository import FakeConnectionRepository
from airweave.domains.credentials.fakes.repository import FakeIntegrationCredentialRepository
from airweave.domains.source_connections.create import SourceConnectionCreationService
from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository
from airweave.domains.source_connections.fakes.response import FakeResponseBuilder
from airweave.domains.source_connections.fakes.service import FakeSourceConnectionService
from airweave.domains.sources.fakes.lifecycle import FakeSourceLifecycleService
from airweave.domains.sources.fakes.registry import FakeSourceRegistry
from airweave.domains.sources.fakes.validation import FakeSourceValidationService
from airweave.domains.syncs.fakes.sync_lifecycle_service import FakeSyncLifecycleService
from airweave.domains.syncs.fakes.sync_record_service import FakeSyncRecordService
from airweave.domains.temporal.fakes.service import FakeTemporalWorkflowService
from airweave.schemas.organization import Organization
from airweave.schemas.source_connection import (
    AuthenticationMethod,
    DirectAuthentication,
    OAuthBrowserAuthentication,
    SourceConnectionCreate,
)


NOW = datetime.now(timezone.utc)


def _ctx() -> ApiContext:
    org = Organization(id=str(uuid4()), name="Test Org", created_at=NOW, modified_at=NOW)
    return ApiContext(
        request_id="test-req",
        organization=org,
        auth_method=AuthMethod.SYSTEM,
    )


def _entry(*, oauth_type=None, requires_byoc=False, supports_continuous=False):
    source_cls = MagicMock()
    source_cls.requires_byoc = requires_byoc
    source_cls.supports_auth_method.return_value = True
    source_cls.get_supported_auth_methods.return_value = [
        AuthenticationMethod.DIRECT,
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
        AuthenticationMethod.OAUTH_BYOC,
    ]
    return SimpleNamespace(
        name="GitHub",
        short_name="github",
        source_class_ref=source_cls,
        oauth_type=oauth_type,
        supports_continuous=supports_continuous,
        federated_search=False,
        config_ref=None,
        auth_config_ref=None,
        supported_auth_providers=["pipedream", "composio"],
    )


def _service(entry) -> SourceConnectionCreationService:
    registry = FakeSourceRegistry()
    registry.get = MagicMock(return_value=entry)
    return SourceConnectionCreationService(
        sc_repo=FakeSourceConnectionRepository(),
        collection_repo=FakeCollectionRepository(),
        connection_repo=FakeConnectionRepository(),
        credential_repo=FakeIntegrationCredentialRepository(),
        source_registry=registry,
        source_validation=FakeSourceValidationService(),
        source_lifecycle=FakeSourceLifecycleService(),
        sync_lifecycle=FakeSyncLifecycleService(),
        sync_record_service=FakeSyncRecordService(),
        response_builder=FakeResponseBuilder(),
        oauth1_service=AsyncMock(),
        oauth2_service=AsyncMock(),
        credential_encryptor=MagicMock(),
        temporal_workflow_service=FakeTemporalWorkflowService(),
        event_bus=AsyncMock(),
    )


async def test_create_dispatches_direct_branch():
    svc = _service(_entry())
    expected = MagicMock(id=uuid4(), short_name="github")
    svc._create_with_direct_auth = AsyncMock(return_value=expected)
    svc._create_with_oauth_token = AsyncMock()
    svc._create_with_auth_provider = AsyncMock()
    svc._create_with_oauth_browser = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=DirectAuthentication(credentials={"token": "abc"}),
    )
    result = await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())
    assert result is expected
    svc._create_with_direct_auth.assert_awaited_once()


async def test_create_rejects_browser_sync_immediately_true():
    svc = _service(_entry())
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        sync_immediately=True,
        authentication=OAuthBrowserAuthentication(),
    )
    with pytest.raises(HTTPException, match="cannot use sync_immediately"):
        await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())


async def test_create_rejects_missing_byoc_for_required_source():
    svc = _service(_entry(requires_byoc=True))
    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(),
    )
    with pytest.raises(HTTPException, match="requires custom OAuth client credentials"):
        await svc.create(AsyncMock(), obj_in=obj_in, ctx=_ctx())


async def test_create_oauth2_init_session_contract(monkeypatch):
    entry = _entry(oauth_type="access_only")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={"instance_url": "acme"})
    svc._extract_template_configs = MagicMock(return_value={"instance_url": "acme"})
    svc._oauth2_service.generate_auth_url_with_redirect = AsyncMock(
        return_value=("https://provider/auth", "verifier-123")
    )
    from airweave.platform.auth.schemas import OAuth2Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth2Settings(
                integration_short_name="github",
                url="https://provider/authorize",
                backend_url="https://provider/token",
                grant_type="authorization_code",
                client_id="platform-client-id",
                client_secret="platform-client-secret",
                content_type="application/x-www-form-urlencoded",
                client_credential_location="payload",
            )
        ),
    )

    shell_sc = MagicMock(id=uuid4(), connection_init_session_id=None, is_authenticated=False)
    svc._sc_repo.create = AsyncMock(return_value=shell_sc)
    svc._response_builder.build_response = AsyncMock(return_value=MagicMock(id=shell_sc.id))

    from airweave.domains.source_connections import create as create_module

    captured = {}

    async def _fake_create_redirect_session(db, provider_auth_url, ctx, uow):
        return uuid4()

    async def _fake_create_init_session(db, obj_in, state, ctx, uow, **kwargs):
        captured.update(kwargs)
        return MagicMock(id=uuid4())

    monkeypatch.setattr(svc, "_create_redirect_session", _fake_create_redirect_session)
    monkeypatch.setattr(svc, "_create_init_session", _fake_create_init_session)

    class _FakeUOW:
        def __init__(self, db):
            self.session = db

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(create_module, "UnitOfWork", _FakeUOW)

    db = AsyncMock()
    db.add = MagicMock()
    db.refresh = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(),
    )
    await svc._create_with_oauth_browser(db, obj_in=obj_in, entry=entry, ctx=_ctx())

    assert captured["template_configs"] == {"instance_url": "acme"}
    assert captured["additional_overrides"]["code_verifier"] == "verifier-123"
    assert "redirect_session_id" in captured


async def test_create_oauth1_init_session_contract(monkeypatch):
    entry = _entry(oauth_type="oauth1")
    svc = _service(entry)
    svc._source_validation.validate_config = MagicMock(return_value={})
    svc._oauth1_service.get_request_token = AsyncMock(
        return_value=SimpleNamespace(oauth_token="req-token", oauth_token_secret="req-secret")
    )
    svc._oauth1_service.build_authorization_url = MagicMock(return_value="https://provider/oauth1-auth")

    shell_sc = MagicMock(id=uuid4(), connection_init_session_id=None, is_authenticated=False)
    svc._sc_repo.create = AsyncMock(return_value=shell_sc)
    svc._response_builder.build_response = AsyncMock(return_value=MagicMock(id=shell_sc.id))

    from airweave.platform.auth.schemas import OAuth1Settings

    monkeypatch.setattr(
        "airweave.domains.source_connections.create.integration_settings.get_by_short_name",
        AsyncMock(
            return_value=OAuth1Settings(
                integration_short_name="github",
                request_token_url="https://provider/request-token",
                authorization_url="https://provider/authorize",
                access_token_url="https://provider/access-token",
                consumer_key="platform-key",
                consumer_secret="platform-secret",
            )
        ),
    )

    from airweave.domains.source_connections import create as create_module

    captured = {}

    async def _fake_create_redirect_session(db, provider_auth_url, ctx, uow):
        return uuid4()

    async def _fake_create_init_session(db, obj_in, state, ctx, uow, **kwargs):
        captured.update(kwargs)
        return MagicMock(id=uuid4())

    monkeypatch.setattr(svc, "_create_redirect_session", _fake_create_redirect_session)
    monkeypatch.setattr(svc, "_create_init_session", _fake_create_init_session)

    class _FakeUOW:
        def __init__(self, db):
            self.session = db

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(create_module, "UnitOfWork", _FakeUOW)

    db = AsyncMock()
    db.add = MagicMock()
    db.refresh = AsyncMock()

    obj_in = SourceConnectionCreate(
        short_name="github",
        readable_collection_id="col-1",
        authentication=OAuthBrowserAuthentication(
            consumer_key="custom-key",
            consumer_secret="custom-secret",
        ),
    )
    await svc._create_with_oauth_browser(db, obj_in=obj_in, entry=entry, ctx=_ctx())

    overrides = captured["additional_overrides"]
    assert overrides["oauth_token"] == "req-token"
    assert overrides["oauth_token_secret"] == "req-secret"
    assert overrides["consumer_key"] == "custom-key"
    assert overrides["consumer_secret"] == "custom-secret"


def test_determine_auth_method_rejects_unknown_auth_shape():
    obj_in = SimpleNamespace(authentication=object())
    with pytest.raises(HTTPException, match="Invalid authentication configuration"):
        SourceConnectionCreationService._determine_auth_method(obj_in)  # type: ignore[arg-type]


async def test_trigger_sync_workflow_logs_warning_when_event_publish_fails():
    svc = _service(_entry())
    svc._event_bus.publish = AsyncMock(side_effect=RuntimeError("bus down"))
    svc._temporal_workflow_service.run_source_connection_workflow = AsyncMock()

    ctx = _ctx()
    ctx.logger.warning = MagicMock()

    sync_job_id = uuid4()
    sync_id = uuid4()
    sync_result = SimpleNamespace(
        sync_job=SimpleNamespace(id=sync_job_id),
        sync_id=sync_id,
        sync=SimpleNamespace(id=sync_id),
    )
    connection = SimpleNamespace(short_name="github")
    collection = SimpleNamespace(id=uuid4(), name="c", readable_id="col-1")

    await svc._trigger_sync_workflow(
        connection=connection,
        sync_result=sync_result,
        collection=collection,
        source_connection_id=uuid4(),
        ctx=ctx,
    )

    ctx.logger.warning.assert_called_once()
    svc._temporal_workflow_service.run_source_connection_workflow.assert_awaited_once()
