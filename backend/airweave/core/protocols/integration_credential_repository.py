"""Protocol for integration credential repository."""

from typing import Any, Optional, Protocol
from uuid import UUID


class IntegrationCredentialRepositoryProtocol(Protocol):
    """Read-only access to integration credential records."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        """Get an integration credential by ID within an organization."""
        ...
