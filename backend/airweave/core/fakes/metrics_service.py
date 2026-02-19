"""Fake metrics service for testing."""

from __future__ import annotations

from typing import Any


class FakeMetricsService:
    """In-memory MetricsService stand-in for testing.

    Structurally satisfies the ``MetricsService`` protocol — matching
    every other fake in the codebase.
    """

    def __init__(self, http: Any, agentic_search: Any, db_pool: Any) -> None:
        self.http = http
        self.agentic_search = agentic_search
        self.db_pool = db_pool

    async def start(self, *, pool: Any, host: str, port: int) -> None:
        pass

    async def stop(self) -> None:
        pass
