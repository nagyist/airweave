"""Protocol for connection repository."""

from typing import Any, Optional, Protocol
from uuid import UUID


class ConnectionRepositoryProtocol(Protocol):
    """Read-only access to connection records."""

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        """Get a connection by ID within an organization."""
        ...

    async def get_by_readable_id(self, db: Any, readable_id: str, ctx: Any) -> Optional[Any]:
        """Get a connection by human-readable ID within an organization."""
        ...
