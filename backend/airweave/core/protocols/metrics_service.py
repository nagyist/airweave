"""MetricsService protocol for the metrics facade.

Abstracts the metrics facade so the Container field and endpoint injection
depend on a protocol rather than the concrete Prometheus-backed class.
Production uses ``PrometheusMetricsService``; tests inject a standalone fake.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from airweave.core.protocols.db_pool_metrics import DbPoolMetrics
from airweave.core.protocols.http_metrics import HttpMetrics
from airweave.search.agentic_search.protocols import AgenticSearchMetrics


@runtime_checkable
class MetricsService(Protocol):
    """Protocol for the metrics facade.

    Public attributes (``http``, ``agentic_search``, ``db_pool``) are typed
    with their respective protocols so ``Inject()`` in deps.py can resolve
    them via nested attribute lookup.
    """

    http: HttpMetrics
    agentic_search: AgenticSearchMetrics
    db_pool: DbPoolMetrics

    async def start(self, *, pool: Any, host: str, port: int) -> None:
        """Start the metrics sidecar server and background samplers."""
        ...

    async def stop(self) -> None:
        """Stop all background services."""
        ...
