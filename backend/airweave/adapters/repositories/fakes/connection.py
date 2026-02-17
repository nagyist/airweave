"""Fake connection repository for testing."""

from typing import Any, Optional
from uuid import UUID


class FakeConnectionRepository:
    """In-memory fake for ConnectionRepositoryProtocol."""

    def __init__(self) -> None:
        self._store: dict[UUID, Any] = {}
        self._readable_store: dict[str, Any] = {}
        self._calls: list[tuple] = []

    def seed(self, id: UUID, obj: Any) -> None:
        self._store[id] = obj

    def seed_readable(self, readable_id: str, obj: Any) -> None:
        self._readable_store[readable_id] = obj

    async def get(self, db: Any, id: UUID, ctx: Any) -> Optional[Any]:
        self._calls.append(("get", db, id, ctx))
        return self._store.get(id)

    async def get_by_readable_id(self, db: Any, readable_id: str, ctx: Any) -> Optional[Any]:
        self._calls.append(("get_by_readable_id", db, readable_id, ctx))
        return self._readable_store.get(readable_id)
