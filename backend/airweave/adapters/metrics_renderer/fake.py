"""Fake MetricsRenderer for testing.

Records generate() calls so tests can assert on metrics-server behaviour
without depending on prometheus-client.
"""

from airweave.core.protocols.metrics_renderer import MetricsRenderer


class FakeMetricsRenderer(MetricsRenderer):
    """In-memory spy implementing the MetricsRenderer protocol."""

    def __init__(self) -> None:
        self.generate_calls: int = 0

    @property
    def content_type(self) -> str:
        return "text/plain"

    def generate(self) -> bytes:
        self.generate_calls += 1
        return b"# fake metrics\n"
