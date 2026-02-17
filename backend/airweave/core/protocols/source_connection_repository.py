"""Protocol for source connection repository."""

from typing import Any, Optional, Protocol
from uuid import UUID


class SourceConnectionRepositoryProtocol(Protocol):
    """Read-only access to source connection records."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        """Get a source connection by ID within an organization."""
        ...
