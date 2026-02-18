"""DbPoolMetrics protocol for database connection pool instrumentation.

Abstracts pool-gauge collection so the sampler depends on a protocol rather
than a concrete library.  Production uses Prometheus; tests inject a fake
that records the latest values in memory.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class DbPoolMetrics(Protocol):
    """Protocol for database connection pool metrics collection."""

    def update(
        self,
        *,
        pool_size: int,
        checked_out: int,
        checked_in: int,
        overflow: int,
    ) -> None:
        """Push a snapshot of pool gauges from a single sampling tick.

        Args:
            pool_size: Current pool size (``pool.size()``).
            checked_out: Connections currently checked out.
            checked_in: Idle connections available in the pool.
            overflow: Connections currently in overflow.
        """
        ...
