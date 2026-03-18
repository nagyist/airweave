"""Linear source implementation for Airweave platform."""

from __future__ import annotations

import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from uuid import uuid4

import httpx
from tenacity import retry, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceError,
    SourceRateLimitError,
)
from airweave.domains.sources.token_providers.protocol import TokenProviderProtocol
from airweave.domains.storage import FileSkippedException
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.config import LinearConfig
from airweave.platform.cursors.linear import LinearCursor
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity, Breadcrumb
from airweave.platform.entities.linear import (
    LinearAttachmentEntity,
    LinearCommentEntity,
    LinearIssueEntity,
    LinearProjectEntity,
    LinearTeamEntity,
    LinearUserEntity,
)
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType

_GRAPHQL_URL = "https://api.linear.app/graphql"


def _is_graphql_rate_limited(body: Dict) -> bool:
    """Check if a Linear GraphQL response contains a RATELIMITED error."""
    for err in body.get("errors", []):
        if err.get("extensions", {}).get("code") == "RATELIMITED":
            return True
    return False


def _parse_reset_header(response: httpx.Response, default: float = 60.0) -> float:
    """Compute seconds-until-reset from ``X-RateLimit-Requests-Reset`` (epoch ms)."""
    raw = response.headers.get("X-RateLimit-Requests-Reset")
    if raw:
        try:
            reset_epoch_s = int(raw) / 1000.0
            return max(1.0, reset_epoch_s - time.time())
        except (ValueError, TypeError):
            pass
    return default


@source(
    name="Linear",
    short_name="linear",
    auth_methods=[
        AuthenticationMethod.OAUTH_BROWSER,
        AuthenticationMethod.OAUTH_TOKEN,
        AuthenticationMethod.AUTH_PROVIDER,
    ],
    oauth_type=OAuthType.ACCESS_ONLY,
    auth_config_class=None,
    config_class=LinearConfig,
    labels=["Project Management"],
    supports_continuous=True,
    cursor_class=LinearCursor,
    rate_limit_level=RateLimitLevel.ORG,
)
class LinearSource(BaseSource):
    """Linear source connector — syncs teams, projects, users, issues, comments, attachments."""

    @classmethod
    async def create(
        cls,
        *,
        auth: TokenProviderProtocol,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: LinearConfig,
    ) -> LinearSource:
        """Create a new Linear source instance."""
        return cls(auth=auth, logger=logger, http_client=http_client)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout,
        wait=wait_rate_limit_with_backoff,
        reraise=True,
    )
    async def _post(self, query: str) -> Dict:
        """Authenticated GraphQL POST with 401 refresh and raise_for_status.

        Linear returns rate-limit errors as HTTP 400 with a GraphQL
        ``RATELIMITED`` error code (not HTTP 429). We detect this before
        ``raise_for_status`` so the tenacity retry fires correctly.
        """
        token = await self.auth.get_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        response = await self.http_client.post(_GRAPHQL_URL, headers=headers, json={"query": query})

        if response.status_code == 401 and self.auth.supports_refresh:
            new_token = await self.auth.force_refresh()
            headers["Authorization"] = f"Bearer {new_token}"
            response = await self.http_client.post(
                _GRAPHQL_URL, headers=headers, json={"query": query}
            )

        body = response.json()

        if response.status_code == 400 and _is_graphql_rate_limited(body):
            retry_after = _parse_reset_header(response)
            raise SourceRateLimitError(
                retry_after=retry_after,
                source_short_name=self.short_name,
                message=f"Linear RATELIMITED (400). Retry after {retry_after:.0f}s",
            )

        raise_for_status(
            response,
            source_short_name=self.short_name,
            token_provider_kind=self.auth.provider_kind,
        )

        return body

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse Linear ISO8601 timestamps into timezone-aware datetimes."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _build_issue_context(
        self, issue: Dict[str, Any]
    ) -> tuple[List[Breadcrumb], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Assemble breadcrumb trail and related metadata for an issue."""
        breadcrumbs: List[Breadcrumb] = []
        team = issue.get("team") or {}
        team_id = team.get("id")
        team_name = team.get("name")
        if team_id:
            breadcrumbs.append(
                Breadcrumb(
                    entity_id=team_id,
                    name=team_name or "Team",
                    entity_type=LinearTeamEntity.__name__,
                )
            )

        project = issue.get("project") or {}
        project_id = project.get("id")
        project_name = project.get("name")
        if project_id:
            breadcrumbs.append(
                Breadcrumb(
                    entity_id=project_id,
                    name=project_name or "Project",
                    entity_type=LinearProjectEntity.__name__,
                )
            )

        return breadcrumbs, team_id, team_name, project_id, project_name

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    async def _paginated_query(
        self,
        query_template: str,
        process_node_func,
        page_size: int = 50,
        entity_type: str = "items",
    ) -> AsyncGenerator:
        """Paginate through Linear GraphQL, yielding entities from process_node_func.

        SourceAuthError propagates immediately (abort sync).
        Other errors propagate to the caller (typically _generate_entities_safe).
        """
        has_next_page = True
        after_cursor = None
        items_processed = 0

        while has_next_page:
            pagination = f"first: {page_size}"
            if after_cursor:
                pagination += f', after: "{after_cursor}"'

            query = query_template.format(pagination=pagination)
            response = await self._post(query)

            data = response.get("data", {})
            collection_key = next(iter(data.keys()), None)

            if not collection_key:
                self.logger.error(f"Unexpected response structure: {response}")
                break

            collection_data = data[collection_key]
            nodes = collection_data.get("nodes", [])

            batch_count = len(nodes)
            items_processed += batch_count
            self.logger.debug(
                f"Processing batch of {batch_count} {entity_type} (total: {items_processed})"
            )

            for node in nodes:
                async for entity in process_node_func(node):
                    if entity:
                        yield entity

            page_info = collection_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            if not nodes or not has_next_page:
                break

    # ------------------------------------------------------------------
    # Entity generators
    # ------------------------------------------------------------------

    async def _generate_team_entities(
        self, since: Optional[str] = None
    ) -> AsyncGenerator[LinearTeamEntity, None]:
        """Generate entities for all teams in the workspace."""
        filter_clause = f'filter: {{{{ updatedAt: {{{{ gte: "{since}" }}}} }}}}, ' if since else ""
        query_template = (
            """
        {{
          teams("""
            + filter_clause
            + """{pagination}) {{
            nodes {{
              id
              name
              key
              description
              color
              icon
              private
              timezone
              createdAt
              updatedAt
              parent {{
                id
                name
              }}
              issueCount
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
        )

        async def process_team(team):
            team_id = team.get("id")
            team_name = team.get("name")
            team_key = team.get("key")
            parent = team.get("parent")
            team_parent_id = parent.get("id", "") if parent else ""
            team_parent_name = parent.get("name", "") if parent else ""

            self.logger.debug(f"Processing team: {team_name} ({team_key})")

            breadcrumbs = [
                Breadcrumb(
                    entity_id=team_id,
                    name=team_name or team_key or "Team",
                    entity_type=LinearTeamEntity.__name__,
                )
            ]
            created_time = self._parse_datetime(team.get("createdAt")) or datetime.utcnow()
            updated_time = self._parse_datetime(team.get("updatedAt")) or created_time

            yield LinearTeamEntity(
                entity_id=team_id,
                breadcrumbs=breadcrumbs,
                name=team_name or "",
                created_at=created_time,
                updated_at=updated_time,
                team_id=team_id,
                team_name=team_name or "",
                created_time=created_time,
                updated_time=updated_time,
                key=team_key,
                description=team.get("description", ""),
                color=team.get("color", ""),
                icon=team.get("icon", ""),
                private=team.get("private", False),
                timezone=team.get("timezone", ""),
                parent_id=team_parent_id,
                parent_name=team_parent_name,
                issue_count=team.get("issueCount", 0),
                web_url_value=f"https://linear.app/team/{team_key}",
            )

        async for entity in self._paginated_query(
            query_template, process_team, entity_type="teams"
        ):
            yield entity

    async def _generate_project_entities(
        self, since: Optional[str] = None
    ) -> AsyncGenerator[LinearProjectEntity, None]:
        """Generate entities for all projects in the workspace."""
        filter_clause = f'filter: {{{{ updatedAt: {{{{ gte: "{since}" }}}} }}}}, ' if since else ""
        query_template = (
            """
        {{
          projects("""
            + filter_clause
            + """{pagination}) {{
            nodes {{
              id
              name
              slugId
              description
              priority
              startDate
              targetDate
              state
              createdAt
              updatedAt
              completedAt
              startedAt
              progress
              teams {{
                nodes {{
                  id
                  name
                }}
              }}
              lead {{
                name
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
        )

        async def process_project(project):
            project_id = project.get("id")
            project_name = project.get("name")

            self.logger.debug(f"Processing project: {project_name}")

            team_ids: List[Optional[str]] = []
            team_names: List[Optional[str]] = []
            for team in project.get("teams", {}).get("nodes", []):
                team_ids.append(team.get("id"))
                team_names.append(team.get("name"))

            breadcrumbs: List[Breadcrumb] = []
            for t_id, t_name in zip(team_ids, team_names, strict=False):
                if t_id:
                    breadcrumbs.append(
                        Breadcrumb(
                            entity_id=t_id,
                            name=t_name or "Team",
                            entity_type=LinearTeamEntity.__name__,
                        )
                    )

            created_time = self._parse_datetime(project.get("createdAt")) or datetime.utcnow()
            updated_time = self._parse_datetime(project.get("updatedAt")) or created_time

            yield LinearProjectEntity(
                entity_id=project_id,
                breadcrumbs=breadcrumbs,
                name=project_name or "",
                created_at=created_time,
                updated_at=updated_time,
                project_id=project_id,
                project_name=project_name or "",
                created_time=created_time,
                updated_time=updated_time,
                slug_id=project.get("slugId"),
                description=project.get("description"),
                priority=project.get("priority"),
                state=project.get("state"),
                completed_at=self._parse_datetime(project.get("completedAt")),
                started_at=self._parse_datetime(project.get("startedAt")),
                target_date=project.get("targetDate"),
                start_date=project.get("startDate"),
                team_ids=team_ids if team_ids else None,
                team_names=team_names if team_names else None,
                progress=project.get("progress"),
                lead=project.get("lead", {}).get("name") if project.get("lead") else None,
                web_url_value=f"https://linear.app/project/{project.get('slugId')}",
            )

        async for entity in self._paginated_query(
            query_template, process_project, entity_type="projects"
        ):
            yield entity

    async def _generate_user_entities(
        self, since: Optional[str] = None
    ) -> AsyncGenerator[LinearUserEntity, None]:
        """Generate entities for all users in the workspace."""
        filter_clause = f'filter: {{{{ updatedAt: {{{{ gte: "{since}" }}}} }}}}, ' if since else ""
        query_template = (
            """
        {{
          users("""
            + filter_clause
            + """{pagination}) {{
            nodes {{
              id
              name
              displayName
              email
              avatarUrl
              description
              timezone
              active
              admin
              guest
              lastSeen
              statusEmoji
              statusLabel
              statusUntilAt
              createdIssueCount
              createdAt
              updatedAt
              teams {{
                nodes {{
                  id
                  name
                  key
                }}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
        )

        async def process_user(user):
            user_id = user.get("id")
            user_name = user.get("name")
            display_name = user.get("displayName")

            self.logger.debug(f"Processing user: {user_name} ({display_name})")

            team_ids = []
            team_names = []
            for team in user.get("teams", {}).get("nodes", []):
                team_ids.append(team.get("id"))
                team_names.append(team.get("name"))

            breadcrumbs: List[Breadcrumb] = []
            for t_id, t_name in zip(team_ids, team_names, strict=False):
                if t_id:
                    breadcrumbs.append(
                        Breadcrumb(
                            entity_id=t_id,
                            name=t_name or "Team",
                            entity_type=LinearTeamEntity.__name__,
                        )
                    )

            display_label = display_name or user_name or user.get("email") or user_id
            created_time = self._parse_datetime(user.get("createdAt")) or datetime.utcnow()
            updated_time = self._parse_datetime(user.get("updatedAt")) or created_time

            yield LinearUserEntity(
                entity_id=user_id,
                breadcrumbs=breadcrumbs,
                name=user_name or display_label,
                created_at=created_time,
                updated_at=updated_time,
                user_id=user_id,
                display_name=display_label,
                created_time=created_time,
                updated_time=updated_time,
                email=user.get("email"),
                avatar_url=user.get("avatarUrl"),
                description=user.get("description"),
                timezone=user.get("timezone"),
                active=user.get("active"),
                admin=user.get("admin"),
                guest=user.get("guest"),
                last_seen=user.get("lastSeen"),
                status_emoji=user.get("statusEmoji"),
                status_label=user.get("statusLabel"),
                status_until_at=user.get("statusUntilAt"),
                created_issue_count=user.get("createdIssueCount"),
                team_ids=team_ids if team_ids else None,
                team_names=team_names if team_names else None,
                web_url_value=f"https://linear.app/u/{user_id}",
            )

        async for entity in self._paginated_query(
            query_template, process_user, entity_type="users"
        ):
            yield entity

    # ------------------------------------------------------------------
    # Issues, comments, and attachments
    # ------------------------------------------------------------------

    async def _process_issue_comments(
        self,
        comments: List[Dict],
        issue_id: str,
        issue_identifier: str,
        issue_breadcrumbs: List[Breadcrumb],
        team_id: Optional[str],
        team_name: Optional[str],
        project_id: Optional[str],
        project_name: Optional[str],
    ) -> AsyncGenerator[LinearCommentEntity, None]:
        """Yield comment entities for an issue."""
        for comment in comments:
            comment_id = comment.get("id")
            comment_body = comment.get("body", "")

            if not comment_body.strip():
                continue

            user = comment.get("user", {})
            user_id = user.get("id") if user else None
            user_name = user.get("name") if user else None

            comment_url = f"https://linear.app/issue/{issue_identifier}#comment-{comment_id}"
            comment_preview = comment_body[:50] + "..." if len(comment_body) > 50 else comment_body
            created_time = self._parse_datetime(comment.get("createdAt")) or datetime.utcnow()
            updated_time = self._parse_datetime(comment.get("updatedAt")) or created_time

            yield LinearCommentEntity(
                entity_id=comment_id,
                breadcrumbs=issue_breadcrumbs.copy(),
                name=comment_preview,
                created_at=created_time,
                updated_at=updated_time,
                comment_id=comment_id,
                body_preview=comment_preview,
                created_time=created_time,
                updated_time=updated_time,
                issue_id=issue_id,
                issue_identifier=issue_identifier,
                body=comment_body,
                user_id=user_id,
                user_name=user_name,
                team_id=team_id,
                team_name=team_name,
                project_id=project_id,
                project_name=project_name,
                web_url_value=comment_url,
            )

    async def _download_description_attachment(
        self,
        entity: LinearAttachmentEntity,
        files: FileService,
    ) -> LinearAttachmentEntity | None:
        """Download an inline attachment via FileService. Returns None on expected skips.

        401 after refresh propagates (token is dead → abort sync).
        Infrastructure failures (IOError, OSError) propagate.
        Other HTTP errors skip the file.
        """
        try:
            await files.download_from_url(
                entity=entity,
                client=self.http_client,
                auth=self.auth,
                logger=self.logger,
            )
            if not entity.local_path:
                self.logger.warning(f"Download produced no local path for {entity.name}")
                return None
            return entity
        except FileSkippedException as e:
            self.logger.debug(f"Skipping attachment {entity.attachment_id}: {e.reason}")
            return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise
            self.logger.warning(
                f"HTTP {e.response.status_code} downloading attachment {entity.attachment_id}: {e}"
            )
            return None

    async def _generate_attachment_entities_from_description(
        self,
        issue_id: str,
        issue_identifier: str,
        issue_description: str,
        breadcrumbs: List[Breadcrumb],
        files: FileService,
    ) -> AsyncGenerator[LinearAttachmentEntity, None]:
        """Extract and download attachments from markdown links in issue descriptions."""
        if not issue_description:
            return

        markdown_link_pattern = r"\[([^\]]+)\]\(([^)]+)\)"
        matches = re.findall(markdown_link_pattern, issue_description)

        self.logger.debug(
            f"Found {len(matches)} potential attachments in description "
            f"for issue {issue_identifier}"
        )

        for file_name, url in matches:
            if "uploads.linear.app" not in url:
                continue

            self.logger.debug(f"Processing attachment from description: {file_name} - URL: {url}")

            attachment_id = str(uuid4())
            mime_type = mimetypes.guess_type(file_name)[0]
            if mime_type and "/" in mime_type:
                file_type = mime_type.split("/")[0]
            else:
                ext = os.path.splitext(file_name)[1].lower().lstrip(".")
                file_type = ext if ext else "file"

            attachment_entity = LinearAttachmentEntity(
                entity_id=attachment_id,
                breadcrumbs=breadcrumbs.copy(),
                name=file_name,
                created_at=None,
                updated_at=None,
                url=url,
                size=0,
                file_type=file_type,
                mime_type=mime_type or "application/octet-stream",
                local_path=None,
                attachment_id=attachment_id,
                issue_id=issue_id,
                issue_identifier=issue_identifier,
                title=file_name,
                subtitle="Extracted from issue description",
                source={"type": "description_link"},
                web_url_value=url,
            )

            downloaded = await self._download_description_attachment(attachment_entity, files)
            if downloaded:
                yield downloaded

    async def _generate_issue_entities(  # noqa: C901
        self,
        since: Optional[str] = None,
        files: FileService | None = None,
    ) -> AsyncGenerator[
        Union[LinearIssueEntity, LinearCommentEntity, LinearAttachmentEntity], None
    ]:
        """Generate entities for all issues, their comments, and their attachments."""
        updated_filter = f', updatedAt: {{{{ gte: "{since}" }}}}' if since else ""
        query_template = (
            """
        {{
          issues(filter: {{ archivedAt: {{ null: true }}"""
            + updated_filter
            + """ }}, {pagination}) {{
            nodes {{
              id
              identifier
              title
              description
              priority
              completedAt
              createdAt
              updatedAt
              dueDate
              archivedAt
              state {{
                name
              }}
              team {{
                id
                name
              }}
              project {{
                id
                name
              }}
              assignee {{
                name
              }}
              comments {{
                nodes {{
                  id
                  body
                  createdAt
                  updatedAt
                  user {{
                    id
                    name
                  }}
                }}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
        )

        async def process_issue(issue):
            issue_identifier = issue.get("identifier")

            if issue.get("archivedAt"):
                self.logger.warning(
                    f"Archived issue {issue_identifier} passed GraphQL filter — skipping"
                )
                return

            issue_title = issue.get("title")
            issue_description = issue.get("description", "")

            self.logger.debug(f"Processing issue: {issue_identifier} — '{issue_title}'")

            (
                breadcrumbs,
                team_id,
                team_name,
                project_id,
                project_name,
            ) = self._build_issue_context(issue)

            issue_id = issue.get("id")
            issue_url = f"https://linear.app/issue/{issue_identifier}"
            issue_title = issue.get("title", "") or issue_identifier
            created_time = self._parse_datetime(issue.get("createdAt")) or datetime.utcnow()
            updated_time = self._parse_datetime(issue.get("updatedAt")) or created_time
            completed_at = self._parse_datetime(issue.get("completedAt"))

            yield LinearIssueEntity(
                entity_id=issue_id,
                breadcrumbs=breadcrumbs,
                name=issue_title,
                created_at=created_time,
                updated_at=updated_time,
                issue_id=issue_id,
                identifier=issue_identifier,
                title=issue_title,
                created_time=created_time,
                updated_time=updated_time,
                description=issue_description,
                priority=issue.get("priority"),
                state=issue.get("state", {}).get("name"),
                completed_at=completed_at,
                due_date=issue.get("dueDate"),
                team_id=team_id,
                team_name=team_name,
                project_id=project_id,
                project_name=project_name,
                assignee=(issue.get("assignee", {}).get("name") if issue.get("assignee") else None),
                web_url_value=issue_url,
            )

            issue_breadcrumb = Breadcrumb(
                entity_id=issue_id,
                name=issue_title,
                entity_type=LinearIssueEntity.__name__,
            )
            issue_breadcrumbs = breadcrumbs + [issue_breadcrumb]

            comments = issue.get("comments", {}).get("nodes", [])
            self.logger.debug(f"Processing {len(comments)} comments for issue {issue_identifier}")

            async for comment_entity in self._process_issue_comments(
                comments,
                issue_id,
                issue_identifier,
                issue_breadcrumbs,
                team_id,
                team_name,
                project_id,
                project_name,
            ):
                yield comment_entity

            if issue_description and files:
                async for attachment in self._generate_attachment_entities_from_description(
                    issue_id, issue_identifier, issue_description, issue_breadcrumbs, files
                ):
                    yield attachment

        async for entity in self._paginated_query(
            query_template, process_issue, entity_type="issues"
        ):
            yield entity

    # ------------------------------------------------------------------
    # Error isolation
    # ------------------------------------------------------------------

    async def _generate_entities_safe(
        self,
        generator: AsyncGenerator,
        entity_type: str,
    ) -> AsyncGenerator:
        """Yield entities with error isolation per entity type.

        SourceAuthError propagates (abort sync — credentials dead).
        Other exceptions are logged and stop that entity type only.
        """
        try:
            self.logger.debug(f"Starting {entity_type} entity generation")
            async for entity in generator:
                yield entity
        except SourceAuthError:
            raise
        except SourceError as exc:
            self.logger.error(f"Failed to generate {entity_type} entities: {exc}")
        except Exception as exc:
            self.logger.error(f"Unexpected error generating {entity_type} entities: {exc}")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Generate all entities from Linear.

        On first sync (no cursor), all entities are fetched. On subsequent syncs,
        only entities updated since the last sync are fetched.
        """
        cursor_data = cursor.data if cursor else {}
        last_synced_at = cursor_data.get("last_synced_at") or None
        sync_start = datetime.now(tz=timezone.utc).isoformat()

        if last_synced_at:
            self.logger.info(f"Incremental sync from {last_synced_at}")
        else:
            self.logger.info("Full sync (first run)")

        async for entity in self._generate_entities_safe(
            self._generate_team_entities(since=last_synced_at), "team"
        ):
            yield entity

        async for entity in self._generate_entities_safe(
            self._generate_project_entities(since=last_synced_at), "project"
        ):
            yield entity

        async for entity in self._generate_entities_safe(
            self._generate_user_entities(since=last_synced_at), "user"
        ):
            yield entity

        async for entity in self._generate_entities_safe(
            self._generate_issue_entities(since=last_synced_at, files=files),
            "issue, comment, and attachment",
        ):
            yield entity

        if cursor:
            cursor.update(last_synced_at=sync_start)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    async def validate(self) -> bool:
        """Verify Linear OAuth2 token by POSTing a minimal GraphQL query."""
        try:
            token = await self.auth.get_token()
            if not token:
                self.logger.error("Linear validation failed: no access token available.")
                return False

            query = {"query": "query { viewer { id } }"}
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(_GRAPHQL_URL, headers=headers, json=query)

                if resp.status_code == 401 and self.auth.supports_refresh:
                    new_token = await self.auth.force_refresh()
                    headers["Authorization"] = f"Bearer {new_token}"
                    resp = await client.post(_GRAPHQL_URL, headers=headers, json=query)

                if not resp.is_success:
                    self.logger.warning(
                        f"Linear validate failed: HTTP {resp.status_code} — {resp.text[:200]}"
                    )
                    return False

                body = resp.json()
                if body.get("errors"):
                    self.logger.warning(f"Linear validate GraphQL errors: {body['errors']}")
                    return False

                viewer = (body.get("data") or {}).get("viewer") or {}
                return bool(viewer.get("id"))

        except httpx.RequestError as e:
            self.logger.error(f"Linear validation request error: {e}")
            return False
