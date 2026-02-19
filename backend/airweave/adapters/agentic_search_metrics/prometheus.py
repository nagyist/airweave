"""Prometheus implementation of the AgenticSearchMetrics protocol.

Uses a caller-supplied CollectorRegistry so these metrics are served
alongside the HTTP metrics on the same ``/metrics`` endpoint.
"""

from prometheus_client import CollectorRegistry, Counter, Histogram

from airweave.search.agentic_search.protocols import AgenticSearchMetrics

_ITERATION_BUCKETS = (1, 2, 3, 4, 5, 7, 10)
_STEP_DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_RESULTS_BUCKETS = (0, 1, 5, 10, 25, 50, 100, 250)
_DURATION_BUCKETS = (0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 120.0)


class PrometheusAgenticSearchMetrics(AgenticSearchMetrics):
    """Prometheus-backed agentic search metrics collection."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._registry = registry or CollectorRegistry()

        self._requests_total = Counter(
            "airweave_agentic_search_requests_total",
            "Total agentic search requests",
            ["mode", "streaming"],
            registry=self._registry,
        )

        self._errors_total = Counter(
            "airweave_agentic_search_errors_total",
            "Total agentic search errors",
            ["mode", "streaming"],
            registry=self._registry,
        )

        self._iterations = Histogram(
            "airweave_agentic_search_iterations",
            "Number of iterations per agentic search",
            ["mode"],
            buckets=_ITERATION_BUCKETS,
            registry=self._registry,
        )

        self._step_duration = Histogram(
            "airweave_agentic_search_step_duration_seconds",
            "Duration of individual pipeline steps in seconds",
            ["step"],
            buckets=_STEP_DURATION_BUCKETS,
            registry=self._registry,
        )

        self._results_per_search = Histogram(
            "airweave_agentic_search_results_per_search",
            "Number of results returned per search",
            buckets=_RESULTS_BUCKETS,
            registry=self._registry,
        )

        self._duration = Histogram(
            "airweave_agentic_search_duration_seconds",
            "End-to-end agentic search duration in seconds",
            ["mode"],
            buckets=_DURATION_BUCKETS,
            registry=self._registry,
        )

    # -- AgenticSearchMetrics protocol methods --

    def inc_search_requests(self, mode: str, streaming: bool) -> None:
        self._requests_total.labels(mode=mode, streaming=str(streaming).lower()).inc()

    def inc_search_errors(self, mode: str, streaming: bool) -> None:
        self._errors_total.labels(mode=mode, streaming=str(streaming).lower()).inc()

    def observe_iterations(self, mode: str, count: int) -> None:
        self._iterations.labels(mode=mode).observe(count)

    def observe_step_duration(self, step: str, duration: float) -> None:
        self._step_duration.labels(step=step).observe(duration)

    def observe_results_per_search(self, count: int) -> None:
        self._results_per_search.observe(count)

    def observe_duration(self, mode: str, duration: float) -> None:
        self._duration.labels(mode=mode).observe(duration)
