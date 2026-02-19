"""Prometheus implementation of the DbPoolMetrics protocol.

Five gauges on the shared CollectorRegistry expose connection pool state
for Prometheus scraping.  ``max_overflow`` is set once at construction
since it is a static engine configuration value.
"""

from prometheus_client import CollectorRegistry, Gauge

from airweave.core.protocols.db_pool_metrics import DbPoolMetrics


class PrometheusDbPoolMetrics(DbPoolMetrics):
    """Prometheus-backed DB connection pool metrics."""

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        max_overflow: int = 0,
    ) -> None:
        self._registry = registry or CollectorRegistry()

        self._pool_size = Gauge(
            "airweave_db_pool_size",
            "Current size of the connection pool",
            registry=self._registry,
        )

        self._max_overflow = Gauge(
            "airweave_db_pool_max_overflow",
            "Maximum overflow connections allowed",
            registry=self._registry,
        )

        self._checked_out = Gauge(
            "airweave_db_pool_checked_out",
            "Connections currently checked out from the pool",
            registry=self._registry,
        )

        self._checked_in = Gauge(
            "airweave_db_pool_checked_in",
            "Idle connections available in the pool",
            registry=self._registry,
        )

        self._overflow = Gauge(
            "airweave_db_pool_overflow",
            "Connections currently in overflow",
            registry=self._registry,
        )

        # Static value — set once.
        self._max_overflow.set(max_overflow)

    # -- DbPoolMetrics protocol method --

    def update(
        self,
        *,
        pool_size: int,
        checked_out: int,
        checked_in: int,
        overflow: int,
    ) -> None:
        self._pool_size.set(pool_size)
        self._checked_out.set(checked_out)
        self._checked_in.set(checked_in)
        self._overflow.set(overflow)
