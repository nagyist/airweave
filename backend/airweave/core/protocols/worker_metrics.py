"""WorkerMetrics protocol for Temporal worker gauge instrumentation.

Abstracts the Prometheus gauge layer so the control server depends on a
protocol rather than module-level globals.  Production uses Prometheus;
tests inject a fake that records snapshots in memory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from airweave.platform.temporal.worker_metrics_snapshot import WorkerMetricsSnapshot


@runtime_checkable
class WorkerMetrics(Protocol):
    """Protocol for updating Temporal worker Prometheus gauges."""

    def update(self, snapshot: WorkerMetricsSnapshot) -> None:
        """Push a complete worker metrics snapshot to the gauge backend."""
        ...
