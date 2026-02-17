"""Fake source connection repository for testing."""

from typing import Any, Optional
from uuid import UUID


class FakeSourceConnectionRepository:
    """In-memory fake for SourceConnectionRepositoryProtocol."""

    def __init__(self) -> None:
        self._store: dict[UUID, Any] = {}
        self._calls: list[tuple] = []

    def seed(self, id: UUID, obj: Any) -> None:
        self._store[id] = obj

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        self._calls.append(("get", db, id, ctx))
        return self._store.get(id)
