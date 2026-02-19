"""Fake WorkerMetrics for testing.

Records snapshots in memory so tests can assert on worker gauge values
without depending on prometheus-client.
"""

from __future__ import annotations

from airweave.core.protocols.worker_metrics import WorkerMetrics
from airweave.platform.temporal.worker_metrics_snapshot import WorkerMetricsSnapshot


class FakeWorkerMetrics(WorkerMetrics):
    """In-memory spy implementing the WorkerMetrics protocol."""

    def __init__(self) -> None:
        self.snapshots: list[WorkerMetricsSnapshot] = []

    def update(self, snapshot: WorkerMetricsSnapshot) -> None:
        self.snapshots.append(snapshot)

    # -- test helpers --

    @property
    def last_snapshot(self) -> WorkerMetricsSnapshot | None:
        return self.snapshots[-1] if self.snapshots else None

    def clear(self) -> None:
        self.snapshots.clear()
