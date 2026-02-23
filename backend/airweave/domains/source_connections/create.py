"""Source connection creation service."""

import secrets
from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.analytics import business_events
from airweave.api.context import ApiContext
from airweave.core.auth_provider_service import auth_provider_service
from airweave.core.config import settings as core_settings
from airweave.core.events.source_connection import SourceConnectionLifecycleEvent
from airweave.core.events.sync import SyncLifecycleEvent
from airweave.core.exceptions import NotFoundException
from airweave.core.protocols.encryption import CredentialEncryptor
from airweave.core.protocols.event_bus import EventBus
from airweave.core.shared_models import ConnectionStatus, IntegrationType
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.collections.protocols import CollectionRepositoryProtocol
from airweave.domains.connections.protocols import ConnectionRepositoryProtocol
from airweave.domains.credentials.protocols import IntegrationCredentialRepositoryProtocol
from airweave.domains.oauth.protocols import OAuth1ServiceProtocol, OAuth2ServiceProtocol
from airweave.domains.source_connections.protocols import (
    ResponseBuilderProtocol,
    SourceConnectionCreateServiceProtocol,
    SourceConnectionRepositoryProtocol,
)
from airweave.domains.sources.protocols import (
    SourceLifecycleServiceProtocol,
    SourceRegistryProtocol,
    SourceValidationServiceProtocol,
)
from airweave.domains.syncs.protocols import (
    SyncLifecycleServiceProtocol,
    SyncRecordServiceProtocol,
)
from airweave.domains.temporal.protocols import TemporalWorkflowServiceProtocol
from airweave.platform.auth.schemas import OAuth1Settings, OAuth2Settings
from airweave.platform.auth.settings import integration_settings
from airweave.schemas.connection import ConnectionCreate
from airweave.schemas.integration_credential import IntegrationCredentialCreateEncrypted
from airweave.schemas.source_connection import (
    AuthenticationMethod,
    AuthProviderAuthentication,
    DirectAuthentication,
    OAuthBrowserAuthentication,
    OAuthTokenAuthentication,
    SourceConnectionCreate,
)
from airweave.schemas.source_connection import (
    OAuthType as SourceOAuthType,
)
from airweave.schemas.source_connection import (
    SourceConnection as SourceConnectionSchema,
)


class SourceConnectionCreationService(SourceConnectionCreateServiceProtocol):
    """Creates source connections for direct, OAuth, token, and auth-provider flows."""

    def __init__(
        self,
        sc_repo: SourceConnectionRepositoryProtocol,
        collection_repo: CollectionRepositoryProtocol,
        connection_repo: ConnectionRepositoryProtocol,
        credential_repo: IntegrationCredentialRepositoryProtocol,
        source_registry: SourceRegistryProtocol,
        source_validation: SourceValidationServiceProtocol,
        source_lifecycle: SourceLifecycleServiceProtocol,
        sync_lifecycle: SyncLifecycleServiceProtocol,
        sync_record_service: SyncRecordServiceProtocol,
        response_builder: ResponseBuilderProtocol,
        oauth1_service: OAuth1ServiceProtocol,
        oauth2_service: OAuth2ServiceProtocol,
        credential_encryptor: CredentialEncryptor,
        temporal_workflow_service: TemporalWorkflowServiceProtocol,
        event_bus: EventBus,
    ) -> None:
        """Initialize with injected repositories, validators, and orchestration services."""
        self._sc_repo = sc_repo
        self._collection_repo = collection_repo
        self._connection_repo = connection_repo
        self._credential_repo = credential_repo
        self._source_registry = source_registry
        self._source_validation = source_validation
        self._source_lifecycle = source_lifecycle
        self._sync_lifecycle = sync_lifecycle
        self._sync_record_service = sync_record_service
        self._response_builder = response_builder
        self._oauth1_service = oauth1_service
        self._oauth2_service = oauth2_service
        self._credential_encryptor = credential_encryptor
        self._temporal_workflow_service = temporal_workflow_service
        self._event_bus = event_bus

    async def create(
        self, db: AsyncSession, *, obj_in: SourceConnectionCreate, ctx: ApiContext
    ) -> SourceConnectionSchema:
        """Create a source connection from the auth configuration."""
        entry = self._get_source_entry(obj_in.short_name)
        source_class = entry.source_class_ref

        if obj_in.name is None:
            obj_in.name = f"{entry.name} Connection"

        auth_method = self._determine_auth_method(obj_in)
        self._validate_auth_compatibility(source_class, entry.short_name, auth_method)

        if source_class.requires_byoc and auth_method == AuthenticationMethod.OAUTH_BROWSER:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Source {obj_in.short_name} requires custom OAuth client credentials. "
                    "Provide BYOC credentials in authentication."
                ),
            )

        if obj_in.sync_immediately is None:
            obj_in.sync_immediately = auth_method not in (
                AuthenticationMethod.OAUTH_BROWSER,
                AuthenticationMethod.OAUTH_BYOC,
            )

        if auth_method in (AuthenticationMethod.OAUTH_BROWSER, AuthenticationMethod.OAUTH_BYOC):
            if obj_in.sync_immediately:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "OAuth browser connections cannot use sync_immediately. "
                        "Sync starts after OAuth callback."
                    ),
                )

        match auth_method:
            case AuthenticationMethod.DIRECT:
                result = await self._create_with_direct_auth(
                    db, obj_in=obj_in, entry=entry, ctx=ctx
                )
            case AuthenticationMethod.OAUTH_TOKEN:
                result = await self._create_with_oauth_token(
                    db, obj_in=obj_in, entry=entry, ctx=ctx
                )
            case AuthenticationMethod.AUTH_PROVIDER:
                result = await self._create_with_auth_provider(
                    db, obj_in=obj_in, entry=entry, ctx=ctx
                )
            case _:
                result = await self._create_with_oauth_browser(
                    db, obj_in=obj_in, entry=entry, ctx=ctx
                )

        business_events.track_source_connection_created(
            ctx=ctx,
            connection_id=result.id,
            source_short_name=result.short_name,
        )
        try:
            await self._event_bus.publish(
                SourceConnectionLifecycleEvent.created(
                    organization_id=ctx.organization.id,
                    source_connection_id=result.id,
                    source_type=result.short_name,
                    collection_readable_id=result.readable_collection_id,
                    is_authenticated=result.is_authenticated,
                )
            )
        except Exception as e:
            ctx.logger.warning(f"Failed to publish source_connection.created event: {e}")
        return result

    async def _create_with_direct_auth(
        self, db: AsyncSession, *, obj_in: SourceConnectionCreate, entry, ctx: ApiContext
    ) -> SourceConnectionSchema:
        if not obj_in.authentication or not isinstance(obj_in.authentication, DirectAuthentication):
            raise HTTPException(
                status_code=400, detail="Direct authentication requires credentials"
            )

        validated_auth = self._source_validation.validate_auth_schema(
            obj_in.short_name, obj_in.authentication.credentials
        )
        validated_config = self._source_validation.validate_config(
            obj_in.short_name, obj_in.config, ctx
        )

        await self._source_lifecycle.validate(
            short_name=obj_in.short_name,
            credentials=validated_auth,
            config=validated_config,
        )
        auth_fields = validated_auth.model_dump()
        return await self._create_authenticated_connection(
            db,
            obj_in=obj_in,
            entry=entry,
            validated_config=validated_config,
            credential_payload=auth_fields,
            auth_method=AuthenticationMethod.DIRECT,
            ctx=ctx,
        )

    async def _create_with_oauth_token(
        self, db: AsyncSession, *, obj_in: SourceConnectionCreate, entry, ctx: ApiContext
    ) -> SourceConnectionSchema:
        if not obj_in.authentication or not isinstance(
            obj_in.authentication, OAuthTokenAuthentication
        ):
            raise HTTPException(status_code=400, detail="OAuth token authentication requires token")

        validated_config = self._source_validation.validate_config(
            obj_in.short_name, obj_in.config, ctx
        )

        await self._source_lifecycle.validate(
            short_name=obj_in.short_name,
            credentials=obj_in.authentication.access_token,
            config=validated_config,
        )

        token_payload: dict[str, Any] = {
            "access_token": obj_in.authentication.access_token,
            "token_type": "Bearer",
        }
        if obj_in.authentication.refresh_token:
            token_payload["refresh_token"] = obj_in.authentication.refresh_token
        if obj_in.authentication.expires_at:
            token_payload["expires_at"] = obj_in.authentication.expires_at.isoformat()

        return await self._create_authenticated_connection(
            db,
            obj_in=obj_in,
            entry=entry,
            validated_config=validated_config,
            credential_payload=token_payload,
            auth_method=AuthenticationMethod.OAUTH_TOKEN,
            ctx=ctx,
        )

    async def _create_with_auth_provider(
        self, db: AsyncSession, *, obj_in: SourceConnectionCreate, entry, ctx: ApiContext
    ) -> SourceConnectionSchema:
        if not obj_in.authentication or not isinstance(
            obj_in.authentication, AuthProviderAuthentication
        ):
            raise HTTPException(
                status_code=400,
                detail="Auth provider authentication requires provider configuration",
            )

        auth_provider_conn = await self._connection_repo.get_by_readable_id(
            db, readable_id=obj_in.authentication.provider_readable_id, ctx=ctx
        )
        if not auth_provider_conn:
            raise NotFoundException(
                f"Auth provider '{obj_in.authentication.provider_readable_id}' not found"
            )

        supported = set(entry.supported_auth_providers)
        if auth_provider_conn.short_name not in supported:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Source '{obj_in.short_name}' does not support "
                    f"'{auth_provider_conn.short_name}' as auth provider. "
                    f"Supported providers: {sorted(supported)}"
                ),
            )

        validated_auth_provider_config = None
        if obj_in.authentication.provider_config is not None:
            validated_auth_provider_config = (
                await auth_provider_service.validate_auth_provider_config(
                    db,
                    auth_provider_conn.short_name,
                    obj_in.authentication.provider_config,
                )
            )

        validated_config = self._source_validation.validate_config(
            obj_in.short_name, obj_in.config, ctx
        )

        async with UnitOfWork(db) as uow:
            collection = await self._get_collection(uow.session, obj_in.readable_collection_id, ctx)
            connection = await self._create_connection_record(
                uow.session,
                name=obj_in.name,
                short_name=obj_in.short_name,
                credential_id=None,
                ctx=ctx,
                uow=uow,
            )
            destination_ids = await self._sync_record_service.resolve_destination_ids(
                uow.session, ctx
            )
            sync_result = await self._sync_lifecycle.provision_sync(
                uow.session,
                name=obj_in.name,
                source_connection_id=connection.id,
                destination_connection_ids=destination_ids,
                collection_id=collection.id,
                collection_readable_id=collection.readable_id,
                source_entry=entry,
                schedule_config=obj_in.schedule,
                run_immediately=bool(obj_in.sync_immediately),
                ctx=ctx,
                uow=uow,
            )

            source_conn = await self._sc_repo.create(
                uow.session,
                obj_in={
                    "name": obj_in.name,
                    "description": obj_in.description,
                    "short_name": obj_in.short_name,
                    "config_fields": validated_config,
                    "connection_id": connection.id,
                    "readable_collection_id": collection.readable_id,
                    "sync_id": sync_result.sync_id if sync_result else None,
                    "is_authenticated": True,
                    "readable_auth_provider_id": auth_provider_conn.readable_id,
                    "auth_provider_config": validated_auth_provider_config,
                },
                ctx=ctx,
                uow=uow,
            )
            await uow.commit()
            await uow.session.refresh(source_conn)

        response = await self._response_builder.build_response(db, source_conn, ctx)
        if sync_result and sync_result.sync_job and obj_in.sync_immediately:
            await self._trigger_sync_workflow(
                connection=schemas.Connection.model_validate(connection, from_attributes=True),
                sync_result=sync_result,
                collection=schemas.Collection.model_validate(collection, from_attributes=True),
                source_connection_id=response.id,
                ctx=ctx,
            )
        return response

    async def _create_with_oauth_browser(
        self, db: AsyncSession, *, obj_in: SourceConnectionCreate, entry, ctx: ApiContext
    ) -> SourceConnectionSchema:
        auth = obj_in.authentication
        if auth is not None and not isinstance(auth, OAuthBrowserAuthentication):
            raise HTTPException(status_code=400, detail="OAuth browser authentication expected")

        oauth_auth = auth if auth is not None else OAuthBrowserAuthentication()
        validated_config = self._source_validation.validate_config(
            obj_in.short_name, obj_in.config, ctx
        )
        template_configs = self._extract_template_configs(entry, validated_config)
        state = secrets.token_urlsafe(24)
        callback_url = f"{core_settings.api_url}/source-connections/callback"

        additional_overrides: dict[str, Any] = {}
        if entry.oauth_type == SourceOAuthType.OAUTH1.value:
            oauth_settings = await integration_settings.get_by_short_name(obj_in.short_name)
            if not isinstance(oauth_settings, OAuth1Settings):
                raise HTTPException(
                    status_code=400, detail=f"Source '{obj_in.short_name}' is not OAuth1"
                )

            consumer_key = oauth_auth.consumer_key or oauth_settings.consumer_key
            consumer_secret = oauth_auth.consumer_secret or oauth_settings.consumer_secret
            token_response = await self._oauth1_service.get_request_token(
                request_token_url=oauth_settings.request_token_url,
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                callback_url=callback_url,
                logger=ctx.logger,
            )
            provider_auth_url = self._oauth1_service.build_authorization_url(
                authorization_url=oauth_settings.authorization_url,
                oauth_token=token_response.oauth_token,
                scope=oauth_settings.scope,
                expiration=oauth_settings.expiration,
            )
            additional_overrides.update(
                {
                    "oauth_token": token_response.oauth_token,
                    "oauth_token_secret": token_response.oauth_token_secret,
                    "consumer_key": consumer_key,
                    "consumer_secret": consumer_secret,
                }
            )
        else:
            oauth_settings = await integration_settings.get_by_short_name(obj_in.short_name)
            if not isinstance(oauth_settings, OAuth2Settings):
                raise HTTPException(
                    status_code=400, detail=f"Source '{obj_in.short_name}' is not OAuth2"
                )
            (
                provider_auth_url,
                code_verifier,
            ) = await self._oauth2_service.generate_auth_url_with_redirect(
                oauth_settings,
                redirect_uri=callback_url,
                client_id=oauth_auth.client_id or None,
                state=state,
                template_configs=template_configs,
            )
            if code_verifier:
                additional_overrides["code_verifier"] = code_verifier

        from airweave.core.source_connection_service_helpers import source_connection_helpers

        async with UnitOfWork(db) as uow:
            source_conn = await self._sc_repo.create(
                uow.session,
                obj_in={
                    "name": obj_in.name,
                    "description": obj_in.description,
                    "short_name": obj_in.short_name,
                    "config_fields": validated_config,
                    "connection_id": None,
                    "readable_collection_id": obj_in.readable_collection_id,
                    "sync_id": None,
                    "is_authenticated": False,
                },
                ctx=ctx,
                uow=uow,
            )

            _, _, redirect_session_id = await source_connection_helpers.create_proxy_url(
                uow.session, provider_auth_url, ctx, uow
            )
            init_session = await source_connection_helpers.create_init_session(
                uow.session,
                obj_in=obj_in,
                state=state,
                ctx=ctx,
                uow=uow,
                redirect_session_id=redirect_session_id,
                template_configs=template_configs,
                additional_overrides=additional_overrides,
            )
            source_conn.connection_init_session_id = init_session.id
            uow.session.add(source_conn)
            await uow.commit()
            await uow.session.refresh(source_conn)

        return await self._response_builder.build_response(db, source_conn, ctx)

    async def _create_authenticated_connection(
        self,
        db: AsyncSession,
        *,
        obj_in: SourceConnectionCreate,
        entry,
        validated_config: dict[str, Any],
        credential_payload: dict[str, Any],
        auth_method: AuthenticationMethod,
        ctx: ApiContext,
    ) -> SourceConnectionSchema:
        async with UnitOfWork(db) as uow:
            collection = await self._get_collection(uow.session, obj_in.readable_collection_id, ctx)
            credential = await self._create_credential_record(
                uow.session,
                short_name=obj_in.short_name,
                source_name=entry.name,
                auth_payload=credential_payload,
                auth_method=auth_method,
                oauth_type=entry.oauth_type,
                auth_config_name=entry.auth_config_ref.__name__ if entry.auth_config_ref else None,
                ctx=ctx,
                uow=uow,
            )
            connection = await self._create_connection_record(
                uow.session,
                name=obj_in.name,
                short_name=obj_in.short_name,
                credential_id=credential.id,
                ctx=ctx,
                uow=uow,
            )
            destination_ids = await self._sync_record_service.resolve_destination_ids(
                uow.session, ctx
            )
            sync_result = await self._sync_lifecycle.provision_sync(
                uow.session,
                name=obj_in.name,
                source_connection_id=connection.id,
                destination_connection_ids=destination_ids,
                collection_id=collection.id,
                collection_readable_id=collection.readable_id,
                source_entry=entry,
                schedule_config=obj_in.schedule,
                run_immediately=bool(obj_in.sync_immediately),
                ctx=ctx,
                uow=uow,
            )
            source_conn = await self._sc_repo.create(
                uow.session,
                obj_in={
                    "name": obj_in.name,
                    "description": obj_in.description,
                    "short_name": obj_in.short_name,
                    "config_fields": validated_config,
                    "connection_id": connection.id,
                    "readable_collection_id": collection.readable_id,
                    "sync_id": sync_result.sync_id if sync_result else None,
                    "is_authenticated": True,
                },
                ctx=ctx,
                uow=uow,
            )
            await uow.commit()
            await uow.session.refresh(source_conn)

        response = await self._response_builder.build_response(db, source_conn, ctx)
        if sync_result and sync_result.sync_job and obj_in.sync_immediately:
            await self._trigger_sync_workflow(
                connection=schemas.Connection.model_validate(connection, from_attributes=True),
                sync_result=sync_result,
                collection=schemas.Collection.model_validate(collection, from_attributes=True),
                source_connection_id=response.id,
                ctx=ctx,
            )
        return response

    async def _create_credential_record(
        self,
        db: AsyncSession,
        *,
        short_name: str,
        source_name: str,
        auth_payload: dict[str, Any],
        auth_method: AuthenticationMethod,
        oauth_type: Optional[str],
        auth_config_name: Optional[str],
        ctx: ApiContext,
        uow: UnitOfWork,
    ):
        oauth_type_value: Optional[SourceOAuthType] = None
        if oauth_type:
            try:
                oauth_type_value = SourceOAuthType(oauth_type)
            except ValueError:
                oauth_type_value = None

        credential_create = IntegrationCredentialCreateEncrypted(
            name=f"{source_name} - {ctx.organization.id}",
            description=f"Credentials for {source_name} - {ctx.organization.id}",
            integration_short_name=short_name,
            integration_type=IntegrationType.SOURCE,
            authentication_method=auth_method,
            oauth_type=oauth_type_value,
            auth_config_class=auth_config_name,
            encrypted_credentials=self._credential_encryptor.encrypt(auth_payload),
        )
        return await self._credential_repo.create(db, obj_in=credential_create, ctx=ctx, uow=uow)

    async def _create_connection_record(
        self,
        db: AsyncSession,
        *,
        name: str,
        short_name: str,
        credential_id: Optional[UUID],
        ctx: ApiContext,
        uow: UnitOfWork,
    ):
        connection_in = ConnectionCreate(
            name=f"Connection to {name}",
            integration_type=IntegrationType.SOURCE,
            status=ConnectionStatus.ACTIVE,
            integration_credential_id=credential_id,
            short_name=short_name,
        )
        return await self._connection_repo.create(db, obj_in=connection_in, ctx=ctx, uow=uow)

    async def _get_collection(self, db: AsyncSession, readable_id: str, ctx: ApiContext):
        collection = await self._collection_repo.get_by_readable_id(
            db, readable_id=readable_id, ctx=ctx
        )
        if not collection:
            raise NotFoundException("Collection not found")
        return collection

    def _get_source_entry(self, short_name: str):
        try:
            return self._source_registry.get(short_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Source '{short_name}' not found") from exc

    @staticmethod
    def _determine_auth_method(obj_in: SourceConnectionCreate) -> AuthenticationMethod:
        auth = obj_in.authentication
        match auth:
            case None:
                return AuthenticationMethod.OAUTH_BROWSER
            case DirectAuthentication():
                return AuthenticationMethod.DIRECT
            case OAuthTokenAuthentication():
                return AuthenticationMethod.OAUTH_TOKEN
            case AuthProviderAuthentication():
                return AuthenticationMethod.AUTH_PROVIDER
            case OAuthBrowserAuthentication():
                if (auth.client_id and auth.client_secret) or (
                    auth.consumer_key and auth.consumer_secret
                ):
                    return AuthenticationMethod.OAUTH_BYOC
                return AuthenticationMethod.OAUTH_BROWSER
            case _:
                raise HTTPException(status_code=400, detail="Invalid authentication configuration")

    @staticmethod
    def _validate_auth_compatibility(
        source_class, short_name: str, auth_method: AuthenticationMethod
    ) -> None:
        if source_class.supports_auth_method(auth_method):
            return
        supported = [m.value for m in source_class.get_supported_auth_methods()]
        raise HTTPException(
            status_code=400,
            detail=(
                f"Source {short_name} does not support this authentication method. "
                f"Supported methods: {supported}"
            ),
        )

    @staticmethod
    def _extract_template_configs(
        entry, validated_config: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        config_class = entry.config_ref
        if not config_class:
            return None
        if not hasattr(config_class, "get_template_config_fields"):
            return None
        template_fields = config_class.get_template_config_fields()
        if not template_fields:
            return None
        config_class.validate_template_configs(validated_config)
        return config_class.extract_template_configs(validated_config)

    async def _trigger_sync_workflow(
        self,
        *,
        connection: schemas.Connection,
        sync_result,
        collection: schemas.Collection,
        source_connection_id: UUID,
        ctx: ApiContext,
    ) -> None:
        if not sync_result.sync_job:
            return

        await self._temporal_workflow_service.run_source_connection_workflow(
            sync=sync_result.sync,
            sync_job=sync_result.sync_job,
            collection=collection,
            connection=connection,
            ctx=ctx,
        )
        try:
            from airweave.core.container import container

            if container is not None:
                await container.event_bus.publish(
                    SyncLifecycleEvent.pending(
                        organization_id=ctx.organization.id,
                        source_connection_id=source_connection_id,
                        sync_job_id=sync_result.sync_job.id,
                        sync_id=sync_result.sync_id,
                        collection_id=collection.id,
                        source_type=connection.short_name,
                        collection_name=collection.name,
                        collection_readable_id=collection.readable_id,
                    )
                )
        except Exception:
            pass
