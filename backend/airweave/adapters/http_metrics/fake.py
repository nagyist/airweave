"""Fake HttpMetrics for testing.

Records all calls in memory so tests can assert on metrics behaviour
without reaching into prometheus-client internals.
"""

from dataclasses import dataclass


@dataclass
class RequestRecord:
    """Single observed request."""

    method: str
    endpoint: str
    status_code: str
    duration: float


@dataclass
class ResponseSizeRecord:
    """Single observed response size."""

    method: str
    endpoint: str
    size: int


class FakeHttpMetrics:
    """In-memory spy implementing the HttpMetrics protocol.

    Usage:
        fake = FakeHttpMetrics()
        # … inject into middleware / server …
        assert fake.in_progress == {"GET": 0, "POST": 0}
        assert len(fake.requests) == 1
    """

    def __init__(self) -> None:
        self.in_progress: dict[str, int] = {}
        self.requests: list[RequestRecord] = []
        self.response_sizes: list[ResponseSizeRecord] = []

    def inc_in_progress(self, method: str) -> None:
        self.in_progress[method] = self.in_progress.get(method, 0) + 1

    def dec_in_progress(self, method: str) -> None:
        self.in_progress[method] = self.in_progress.get(method, 0) - 1

    def observe_request(
        self,
        method: str,
        endpoint: str,
        status_code: str,
        duration: float,
    ) -> None:
        self.requests.append(RequestRecord(method, endpoint, status_code, duration))

    def observe_response_size(self, method: str, endpoint: str, size: int) -> None:
        self.response_sizes.append(ResponseSizeRecord(method, endpoint, size))

    # -- test helpers --

    def clear(self) -> None:
        """Reset all recorded state."""
        self.in_progress.clear()
        self.requests.clear()
        self.response_sizes.clear()
