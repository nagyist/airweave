"""Response builder for source connections.

Assembles the rich SourceConnection and SourceConnectionListItem
response schemas from multiple data sources.

Extracted line-for-line from
core.source_connection_service_helpers.build_source_connection_response
(lines 668-902) and the inline list-building in
core.source_connection_service.list (lines 414-432).
"""

from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.api.context import ApiContext
from airweave.core.config import settings as core_settings
from airweave.core.shared_models import SyncJobStatus
from airweave.domains.connections.protocols import ConnectionRepositoryProtocol
from airweave.domains.credentials.protocols import IntegrationCredentialRepositoryProtocol
from airweave.domains.entities.protocols import EntityCountRepositoryProtocol
from airweave.domains.source_connections.protocols import SourceConnectionRepositoryProtocol
from airweave.domains.source_connections.types import SourceConnectionStats
from airweave.domains.sources.protocols import SourceRegistryProtocol
from airweave.domains.syncs.protocols import SyncJobRepositoryProtocol
from airweave.models.source_connection import SourceConnection
from airweave.schemas.source_connection import (
    AuthenticationDetails,
    AuthenticationMethod,
    SourceConnectionJob,
    SourceConnectionListItem,
    compute_status,
    determine_auth_method,
)
from airweave.schemas.source_connection import (
    SourceConnection as SourceConnectionSchema,
)


class ResponseBuilder:
    """Builds API response schemas for source connections."""

    def __init__(
        self,
        sc_repo: SourceConnectionRepositoryProtocol,
        connection_repo: ConnectionRepositoryProtocol,
        credential_repo: IntegrationCredentialRepositoryProtocol,
        source_registry: SourceRegistryProtocol,
        entity_count_repo: EntityCountRepositoryProtocol,
        sync_job_repo: SyncJobRepositoryProtocol,
    ) -> None:
        """Initialize with all dependencies."""
        self._sc_repo = sc_repo
        self._connection_repo = connection_repo
        self._credential_repo = credential_repo
        self._source_registry = source_registry
        self._entity_count_repo = entity_count_repo
        self._sync_job_repo = sync_job_repo

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def build_response(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> SourceConnectionSchema:
        """Build complete SourceConnection response from an ORM object.

        Mirrors core.source_connection_service_helpers
        .build_source_connection_response lines 668-902.
        """
        auth = await self._build_auth_details(db, source_conn, ctx)
        schedule = await self._build_schedule_details(db, source_conn, ctx)
        sync_details = await self._build_sync_details(db, source_conn, ctx)
        entities = await self._build_entity_summary(db, source_conn, ctx)
        federated_search = self._get_federated_search(source_conn)

        last_job_status = None
        if sync_details and sync_details.last_job:
            last_job_status = sync_details.last_job.status

        return SourceConnectionSchema(
            id=source_conn.id,
            organization_id=source_conn.organization_id,
            name=source_conn.name,
            description=source_conn.description,
            short_name=source_conn.short_name,
            readable_collection_id=source_conn.readable_collection_id,
            status=compute_status(source_conn, last_job_status),
            created_at=source_conn.created_at,
            modified_at=source_conn.modified_at,
            auth=auth,
            config=source_conn.config_fields if hasattr(source_conn, "config_fields") else None,
            schedule=schedule,
            sync=sync_details,
            sync_id=getattr(source_conn, "sync_id", None),
            entities=entities,
            federated_search=federated_search,
        )

    def build_list_item(self, stats: SourceConnectionStats) -> SourceConnectionListItem:
        """Build a SourceConnectionListItem from a typed stats object.

        Mirrors core.source_connection_service.list lines 414-432.
        Strict field access — no defensive defaults.
        """
        last_job_status = stats.last_job.status if stats.last_job else None

        return SourceConnectionListItem(
            id=stats.id,
            name=stats.name,
            short_name=stats.short_name,
            readable_collection_id=stats.readable_collection_id,
            created_at=stats.created_at,
            modified_at=stats.modified_at,
            is_authenticated=stats.is_authenticated,
            authentication_method=stats.authentication_method,
            entity_count=stats.entity_count,
            is_active=stats.is_active,
            last_job_status=last_job_status,
        )

    def map_sync_job(self, job: Any, source_connection_id: UUID) -> SourceConnectionJob:
        """Convert a sync job ORM object to a SourceConnectionJob schema.

        Mirrors core.source_connection_service_helpers
        .sync_job_to_source_connection_job lines 1057-1077.
        """
        return SourceConnectionJob(
            id=job.id,
            source_connection_id=source_connection_id,
            status=job.status,
            started_at=job.started_at,
            completed_at=job.completed_at,
            duration_seconds=(
                (job.completed_at - job.started_at).total_seconds()
                if job.completed_at and job.started_at
                else None
            ),
            entities_inserted=getattr(job, "entities_inserted", 0),
            entities_updated=getattr(job, "entities_updated", 0),
            entities_deleted=getattr(job, "entities_deleted", 0),
            entities_failed=getattr(job, "entities_failed", 0),
            error=job.error if hasattr(job, "error") else None,
        )

    # ------------------------------------------------------------------
    # Private helpers — auth
    # ------------------------------------------------------------------

    async def _build_auth_details(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> AuthenticationDetails:
        """Build authentication details section.

        Mirrors lines 683-781 of the singleton.
        """
        actual_auth_method = await self._resolve_auth_method(db, source_conn, ctx)

        auth_info: Dict[str, Any] = {
            "method": actual_auth_method,
            "authenticated": source_conn.is_authenticated,
        }

        if source_conn.is_authenticated:
            auth_info["authenticated_at"] = source_conn.created_at

        if (
            hasattr(source_conn, "readable_auth_provider_id")
            and source_conn.readable_auth_provider_id
        ):
            auth_info["provider_id"] = source_conn.readable_auth_provider_id
            auth_info["provider_readable_id"] = source_conn.readable_auth_provider_id

        if (
            hasattr(source_conn, "connection_init_session_id")
            and source_conn.connection_init_session_id
        ):
            await self._attach_oauth_pending_info(db, source_conn, ctx, auth_info)
        elif hasattr(source_conn, "authentication_url") and source_conn.authentication_url:
            auth_info["auth_url"] = source_conn.authentication_url
            if hasattr(source_conn, "authentication_url_expiry"):
                auth_info["auth_url_expires"] = source_conn.authentication_url_expiry

        return AuthenticationDetails(**auth_info)

    async def _resolve_auth_method(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> AuthenticationMethod:
        """Resolve the auth method from auth provider, credential, or fallback.

        Mirrors lines 684-720 of the singleton (if/elif chain replaced with dict).
        """
        if (
            hasattr(source_conn, "readable_auth_provider_id")
            and source_conn.readable_auth_provider_id
        ):
            return AuthenticationMethod.AUTH_PROVIDER

        if source_conn.connection_id:
            connection = await self._connection_repo.get(db, source_conn.connection_id, ctx)
            if connection and connection.integration_credential_id:
                credential = await self._credential_repo.get(
                    db, connection.integration_credential_id, ctx
                )
                if credential and hasattr(credential, "authentication_method"):
                    method_map = {
                        "oauth_token": AuthenticationMethod.OAUTH_TOKEN,
                        "oauth_browser": AuthenticationMethod.OAUTH_BROWSER,
                        "oauth_byoc": AuthenticationMethod.OAUTH_BYOC,
                        "direct": AuthenticationMethod.DIRECT,
                        "auth_provider": AuthenticationMethod.AUTH_PROVIDER,
                    }
                    resolved = method_map.get(credential.authentication_method)
                    if resolved:
                        return resolved

        return determine_auth_method(source_conn)

    async def _attach_oauth_pending_info(
        self,
        db: AsyncSession,
        source_conn: SourceConnection,
        ctx: ApiContext,
        auth_info: dict,
    ) -> None:
        """Attach OAuth pending auth_url and redirect_url from init session.

        Mirrors lines 740-773 of the singleton — same SQLAlchemy query.
        """
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        from airweave.models import ConnectionInitSession

        stmt = (
            select(ConnectionInitSession)
            .where(ConnectionInitSession.id == source_conn.connection_init_session_id)
            .where(ConnectionInitSession.organization_id == ctx.organization.id)
            .options(selectinload(ConnectionInitSession.redirect_session))
        )
        result = await db.execute(stmt)
        init_session = result.scalar_one_or_none()
        if init_session:
            if init_session.overrides:
                redirect_url = init_session.overrides.get("redirect_url")
                if redirect_url:
                    auth_info["redirect_url"] = redirect_url

            if init_session.redirect_session and not source_conn.is_authenticated:
                auth_info["auth_url"] = (
                    f"{core_settings.api_url}/source-connections/authorize/"
                    f"{init_session.redirect_session.code}"
                )
                auth_info["auth_url_expires"] = init_session.redirect_session.expires_at

    # ------------------------------------------------------------------
    # Private helpers — schedule, sync, entities, federated search
    # ------------------------------------------------------------------

    async def _build_schedule_details(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> Optional[schemas.ScheduleDetails]:
        """Build schedule section. Mirrors lines 783-797."""
        if not hasattr(source_conn, "sync_id") or not source_conn.sync_id:
            return None
        try:
            schedule_info = await self._sc_repo.get_schedule_info(db, source_connection=source_conn)
            if schedule_info:
                return schemas.ScheduleDetails(
                    cron=schedule_info.get("cron_expression"),
                    next_run=schedule_info.get("next_run_at"),
                    continuous=schedule_info.get("is_continuous", False),
                    cursor_field=schedule_info.get("cursor_field"),
                    cursor_value=schedule_info.get("cursor_value"),
                )
        except Exception as e:
            ctx.logger.warning(f"Failed to get schedule info: {e}")
        return None

    async def _build_sync_details(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> Optional[schemas.SyncDetails]:
        """Build sync/job details section. Mirrors lines 799-844."""
        if not hasattr(source_conn, "sync_id") or not source_conn.sync_id:
            return None
        try:
            job = await self._sync_job_repo.get_latest_by_sync_id(db, sync_id=source_conn.sync_id)
            if job:
                duration_seconds = None
                if job.completed_at and job.started_at:
                    duration_seconds = (job.completed_at - job.started_at).total_seconds()

                entities_inserted = getattr(job, "entities_inserted", 0) or 0
                entities_updated = getattr(job, "entities_updated", 0) or 0
                entities_deleted = getattr(job, "entities_deleted", 0) or 0
                entities_skipped = getattr(job, "entities_skipped", 0) or 0

                entities_failed = entities_skipped

                last_job = schemas.SyncJobDetails(
                    id=job.id,
                    status=job.status,
                    started_at=getattr(job, "started_at", None),
                    completed_at=getattr(job, "completed_at", None),
                    duration_seconds=duration_seconds,
                    entities_inserted=entities_inserted,
                    entities_updated=entities_updated,
                    entities_deleted=entities_deleted,
                    entities_failed=entities_failed,
                    error=getattr(job, "error", None),
                )

                return schemas.SyncDetails(
                    total_runs=1,
                    successful_runs=1 if job.status == SyncJobStatus.COMPLETED else 0,
                    failed_runs=1 if job.status == SyncJobStatus.FAILED else 0,
                    last_job=last_job,
                )

        except Exception as e:
            ctx.logger.warning(f"Failed to get sync details: {e}")
        return None

    async def _build_entity_summary(
        self, db: AsyncSession, source_conn: SourceConnection, ctx: ApiContext
    ) -> Optional[schemas.EntitySummary]:
        """Build entity summary section. Mirrors lines 846-868."""
        if not hasattr(source_conn, "sync_id") or not source_conn.sync_id:
            return None
        try:
            entity_counts = await self._entity_count_repo.get_counts_per_sync_and_type(
                db, source_conn.sync_id
            )

            if entity_counts:
                total_entities = sum(count_data.count for count_data in entity_counts)
                by_type = {}
                for count_data in entity_counts:
                    by_type[count_data.entity_definition_name] = schemas.EntityTypeStats(
                        count=count_data.count,
                        last_updated=count_data.modified_at,
                    )

                return schemas.EntitySummary(
                    total_entities=total_entities,
                    by_type=by_type,
                )
        except Exception as e:
            ctx.logger.warning(f"Failed to get entity summary: {e}")
        return None

    def _get_federated_search(self, source_conn: SourceConnection) -> bool:
        """Get federated_search flag from the source registry.

        Singleton used crud.source.get_by_short_name (DB query).
        We use the in-memory registry — same data, no roundtrip.
        """
        try:
            entry = self._source_registry.get(source_conn.short_name)
            return getattr(entry, "federated_search", False)
        except KeyError:
            return False
