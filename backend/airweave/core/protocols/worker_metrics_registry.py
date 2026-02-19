"""WorkerMetricsRegistry protocol for reading worker metrics state.

Captures the read surface of WorkerMetricsRegistry that
WorkerControlServer depends on, so the control server can accept any
implementation (real registry, mock, or fake).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class WorkerMetricsRegistryProtocol(Protocol):
    """Protocol for the read surface of the worker metrics registry."""

    def get_pod_ordinal(self) -> str: ...

    async def get_metrics_summary(self) -> dict[str, Any]: ...

    async def get_per_connector_metrics(self) -> dict[str, dict[str, int]]: ...

    async def get_total_active_and_pending_workers(self) -> int: ...

    async def get_detailed_sync_metrics(self) -> list[dict[str, Any]]: ...

    async def get_per_sync_worker_counts(self) -> list[dict[str, Any]]: ...
