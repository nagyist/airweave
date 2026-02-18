"""MetricsRenderer protocol for serializing collected metrics.

Separates metrics *serialization* (serving /metrics) from metrics
*collection* (HttpMetrics, AgenticSearchMetrics).  This keeps
collection protocols free of Prometheus-specific concerns like
generate() and content_type.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsRenderer(Protocol):
    """Protocol for rendering collected metrics into a scrapeable format."""

    @property
    def content_type(self) -> str:
        """MIME type for the serialized metrics output."""
        ...

    def generate(self) -> bytes:
        """Serialize all collected metrics into the wire format."""
        ...
