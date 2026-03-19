"""Fireflies source implementation.

Syncs meeting transcripts from the Fireflies GraphQL API.
See https://docs.fireflies.ai/graphql-api/query/transcripts and
https://docs.fireflies.ai/schema/transcript.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

from tenacity import retry, stop_after_attempt

from airweave.core.logging import ContextualLogger
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.sources.exceptions import SourceAuthError, SourceError
from airweave.domains.sources.token_providers.protocol import TokenProviderProtocol
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.configs.auth import FirefliesAuthConfig
from airweave.platform.configs.config import FirefliesConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import BaseEntity
from airweave.platform.entities.fireflies import FirefliesTranscriptEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.platform.sources._base import BaseSource
from airweave.platform.sources.http_helpers import raise_for_status
from airweave.platform.sources.retry_helpers import (
    retry_if_rate_limit_or_timeout,
    wait_rate_limit_with_backoff,
)
from airweave.schemas.source_connection import AuthenticationMethod

FIREFLIES_GRAPHQL_URL = "https://api.fireflies.ai/graphql"
TRANSCRIPTS_PAGE_SIZE = 50


@source(
    name="Fireflies",
    short_name="fireflies",
    auth_methods=[AuthenticationMethod.DIRECT, AuthenticationMethod.AUTH_PROVIDER],
    oauth_type=None,
    auth_config_class=FirefliesAuthConfig,
    config_class=FirefliesConfig,
    labels=["Meetings", "Transcription", "Productivity"],
    supports_continuous=False,
)
class FirefliesSource(BaseSource):
    """Fireflies source connector.

    Syncs meeting transcripts from Fireflies.ai. Uses the GraphQL API with
    Bearer token (API key) authentication.
    """

    @classmethod
    async def create(
        cls,
        *,
        auth: TokenProviderProtocol,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: FirefliesConfig,
    ) -> FirefliesSource:
        """Create and configure the Fireflies source."""
        from airweave.domains.sources.token_providers.credential import DirectCredentialProvider

        instance = cls(auth=auth, logger=logger, http_client=http_client)
        if isinstance(auth, DirectCredentialProvider):
            instance._api_key = auth.credentials.api_key
        else:
            instance._api_key = await auth.get_token()
        return instance

    @retry(
        stop=stop_after_attempt(5),
        retry=retry_if_rate_limit_or_timeout,
        wait=wait_rate_limit_with_backoff,
        reraise=True,
    )
    async def _graphql(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL request against the Fireflies API.

        Args:
            query: GraphQL query or mutation string (will be stripped).
            variables: Optional variables dict.

        Returns:
            JSON response body (data or errors).

        Raises:
            SourceAuthError: On 401 (credentials invalid or revoked).
            SourceError: On GraphQL-level errors.
        """
        payload: Dict[str, Any] = {"query": query.strip()}
        if variables:
            payload["variables"] = variables

        token = self._api_key
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        response = await self.http_client.post(
            FIREFLIES_GRAPHQL_URL,
            json=payload,
            headers=headers,
            timeout=30.0,
        )

        if response.status_code == 401 and self.auth.supports_refresh:
            new_token = await self.auth.force_refresh()
            headers["Authorization"] = f"Bearer {new_token}"
            response = await self.http_client.post(
                FIREFLIES_GRAPHQL_URL,
                json=payload,
                headers=headers,
                timeout=30.0,
            )

        raise_for_status(
            response,
            source_short_name=self.short_name,
            token_provider_kind=self.auth.provider_kind,
        )

        data = response.json()
        if "errors" in data and data["errors"]:
            msg = "; ".join(e.get("message", str(e)) for e in data["errors"])
            raise SourceError(
                f"Fireflies GraphQL errors: {msg}",
                source_short_name=self.short_name,
            )
        return data

    @staticmethod
    def _parse_date(ms: Optional[float]) -> Optional[datetime]:
        """Convert milliseconds since epoch to UTC datetime."""
        if ms is None:
            return None
        try:
            return datetime.utcfromtimestamp(ms / 1000.0)
        except (OSError, ValueError):
            return None

    @staticmethod
    def _normalize_action_items(value: Any) -> Optional[List[str]]:
        """Normalize action_items from API (string or list) to List[str]."""
        if value is None:
            return None
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x).strip()]
        if isinstance(value, str):
            return [s.strip() for s in value.split("\n") if s.strip()] or None
        return None

    def _transcript_to_entity(self, t: Dict[str, Any]) -> FirefliesTranscriptEntity:
        """Map a raw transcript object from the API to FirefliesTranscriptEntity."""
        transcript_id = t.get("id") or ""
        title = t.get("title") or "Untitled meeting"
        date_ms = t.get("date")
        created_time = self._parse_date(date_ms)
        summary = t.get("summary") or {}
        sentences = t.get("sentences") or []
        content_parts = []
        for s in sentences:
            raw = (s.get("raw_text") or s.get("text") or "").strip()
            if raw:
                content_parts.append(raw)
        content = "\n".join(content_parts) if content_parts else None

        return FirefliesTranscriptEntity(
            entity_id=transcript_id,
            breadcrumbs=[],
            name=title,
            created_at=created_time,
            updated_at=created_time,
            transcript_id=transcript_id,
            title=title,
            organizer_email=t.get("organizer_email"),
            transcript_url=t.get("transcript_url"),
            participants=t.get("participants") or [],
            duration=t.get("duration"),
            date=date_ms,
            date_string=t.get("dateString"),
            created_time=created_time,
            speakers=t.get("speakers") or [],
            summary_overview=summary.get("overview") or summary.get("short_summary"),
            summary_keywords=summary.get("keywords") or [],
            summary_action_items=self._normalize_action_items(summary.get("action_items")),
            content=content,
            fireflies_users=t.get("fireflies_users") or [],
        )

    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Generate transcript entities from the Fireflies API.

        Paginates through the transcripts query (limit 50 per request).
        """
        query = """
        query Transcripts($limit: Int, $skip: Int) {
          transcripts(limit: $limit, skip: $skip, mine: true) {
            id
            title
            organizer_email
            transcript_url
            participants
            duration
            date
            dateString
            fireflies_users
            speakers { id name }
            summary {
              overview
              short_summary
              keywords
              action_items
            }
            sentences {
              raw_text
              text
              speaker_name
            }
          }
        }
        """
        skip = 0
        while True:
            variables = {"limit": TRANSCRIPTS_PAGE_SIZE, "skip": skip}
            try:
                data = await self._graphql(query, variables)
            except SourceAuthError:
                raise
            except SourceError:
                raise
            except Exception as e:
                self.logger.error(f"Error fetching transcripts at skip={skip}: {e}")
                break
            transcripts = (data.get("data") or {}).get("transcripts") or []
            if not transcripts:
                break
            for t in transcripts:
                yield self._transcript_to_entity(t)
            if len(transcripts) < TRANSCRIPTS_PAGE_SIZE:
                break
            skip += TRANSCRIPTS_PAGE_SIZE

    async def validate(self) -> bool:
        """Validate credentials by running a minimal transcripts query.

        Returns:
            True if the API key is valid and the request succeeds.
        """
        query = """
        query Validate {
          transcripts(limit: 1, mine: true) {
            id
          }
        }
        """
        try:
            await self._graphql(query)
            return True
        except Exception:
            return False
