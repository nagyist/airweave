"""Protocol for source lifecycle operations.

Cross-cutting: used by sync pipeline, search factory, credential service.
"""

from typing import Any, Dict, Optional, Protocol
from uuid import UUID


class SourceLifecycleServiceProtocol(Protocol):
    """Manages source instance creation, configuration, and validation.

    Replaces scattered resource_locator.get_source() + manual
    .create()/.validate()/set_*() calls across source connections, sync,
    and search.
    """

    async def create(
        self,
        db: Any,
        source_connection_id: UUID,
        ctx: Any,
        *,
        access_token: Optional[str] = None,
        sync_job: Optional[Any] = None,
    ) -> Any:
        """Create a fully configured source instance for sync or search.

        Loads the source connection, resolves the source class from the
        registry, decrypts credentials, creates the instance, and configures:
        - Contextual logger
        - Token manager (OAuth sources with refresh)
        - HTTP client (vanilla httpx or Pipedream proxy)
        - Rate limiting wrapper (AirweaveHttpClient)
        - File downloader (if sync_job provided)
        - Sync identifiers

        Args:
            db: Database session
            source_connection_id: The source connection to build from
            ctx: API context (provides org, logger)
            access_token: Direct token injection (skips credential loading
                          and token manager)
            sync_job: Sync job schema (required for file downloader)

        Returns:
            Fully configured BaseSource instance.
        """
        ...

    async def validate(
        self,
        short_name: str,
        credentials: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Validate credentials by creating a lightweight source and
        calling .validate().

        No token manager, no HTTP wrapping, no rate limiting.
        Used during source connection creation to verify credentials
        before persisting.

        Args:
            short_name: Source short name (e.g., "github", "slack")
            credentials: Auth credentials (dict, string token, or
                         Pydantic config object)
            config: Optional source-specific config

        Raises:
            SourceNotFoundError: If source short_name is not in the registry.
            SourceCreationError: If source_class.create() fails.
            SourceValidationError: If source.validate() returns False or raises.
        """
        ...
