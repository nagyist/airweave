"""Prometheus-backed MetricsService implementation.

Composes the metrics adapters, the sidecar HTTP server, and the DB pool
sampler behind a single lifecycle API so callers (main.py, tests) only
deal with one object instead of four adapters + two background services.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airweave.core.protocols.db_pool_metrics import DbPoolMetrics
from airweave.core.protocols.http_metrics import HttpMetrics
from airweave.core.protocols.metrics_renderer import MetricsRenderer
from airweave.search.agentic_search.protocols import AgenticSearchMetrics

if TYPE_CHECKING:
    from airweave.api.metrics import MetricsServer
    from airweave.core.db_pool_sampler import DbPoolSampler


class PrometheusMetricsService:
    """Prometheus-backed facade that owns all metrics adapters and background services.

    Satisfies the ``MetricsService`` protocol structurally.

    Public attributes (``http``, ``agentic_search``, ``db_pool``) are
    typed with their respective protocols so ``Inject()`` in deps.py can
    resolve them via nested attribute lookup.

    ``_renderer`` is private to prevent accidental injection — it is an
    implementation detail of the sidecar server.
    """

    http: HttpMetrics
    agentic_search: AgenticSearchMetrics
    db_pool: DbPoolMetrics

    def __init__(
        self,
        http: HttpMetrics,
        agentic_search: AgenticSearchMetrics,
        db_pool: DbPoolMetrics,
        renderer: MetricsRenderer,
    ) -> None:
        self.http = http
        self.agentic_search = agentic_search
        self.db_pool = db_pool
        self._renderer = renderer
        self._server: MetricsServer | None = None
        self._sampler: DbPoolSampler | None = None

    async def start(self, *, pool: Any, host: str, port: int) -> None:
        """Start the sidecar metrics server and the DB pool sampler."""
        from airweave.api.metrics import MetricsServer
        from airweave.core.db_pool_sampler import DbPoolSampler

        self._server = MetricsServer(self._renderer, port, host)
        await self._server.start()
        self._sampler = DbPoolSampler(pool=pool, metrics=self.db_pool)
        await self._sampler.start()

    async def stop(self) -> None:
        """Stop sampler then sidecar (reverse start order)."""
        try:
            if self._sampler:
                await self._sampler.stop()
        finally:
            if self._server:
                await self._server.stop()
