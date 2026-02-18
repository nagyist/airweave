"""HttpMetrics protocol for HTTP request/response instrumentation.

Abstracts metric collection so the middleware depends on a protocol rather
than a concrete library.  Production uses Prometheus; tests inject a fake
that records calls in memory.

Serialization (generate / content_type) lives in the separate
MetricsRenderer protocol so this protocol stays backend-agnostic.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class HttpMetrics(Protocol):
    """Protocol for HTTP request/response metrics collection."""

    def inc_in_progress(self, method: str) -> None:
        """Increment the in-progress gauge for the given HTTP method."""
        ...

    def dec_in_progress(self, method: str) -> None:
        """Decrement the in-progress gauge for the given HTTP method."""
        ...

    def observe_request(
        self,
        method: str,
        endpoint: str,
        status_code: str,
        duration: float,
    ) -> None:
        """Record a completed request (count + latency).

        Args:
            method: HTTP method (GET, POST, …).
            endpoint: Route path template.
            status_code: Response status code as a string.
            duration: Request duration in seconds.
        """
        ...

    def observe_response_size(
        self,
        method: str,
        endpoint: str,
        size: int,
    ) -> None:
        """Record the response body size in bytes."""
        ...
