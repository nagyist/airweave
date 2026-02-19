"""Prometheus implementation of the MetricsRenderer protocol.

Wraps a CollectorRegistry so the metrics server can serialize all
registered collectors — HTTP metrics, agentic search metrics, or any
future metric families — into Prometheus text exposition format.
"""

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

from airweave.core.protocols.metrics_renderer import MetricsRenderer


class PrometheusMetricsRenderer(MetricsRenderer):
    """Render all metrics in a shared CollectorRegistry."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self._registry = registry

    @property
    def content_type(self) -> str:
        return CONTENT_TYPE_LATEST

    def generate(self) -> bytes:
        return generate_latest(self._registry)
