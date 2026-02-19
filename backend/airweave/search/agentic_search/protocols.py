"""AgenticSearchMetrics protocol for agentic search instrumentation.

Abstracts metric collection so the search agent depends on a protocol
rather than a concrete library.  Production uses Prometheus; tests inject
a fake that records calls in memory.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class AgenticSearchMetrics(Protocol):
    """Protocol for agentic search metrics collection."""

    def inc_search_requests(self, mode: str, streaming: bool) -> None:
        """Increment the search-requests counter."""
        ...

    def inc_search_errors(self, mode: str, streaming: bool) -> None:
        """Increment the search-errors counter."""
        ...

    def observe_iterations(self, mode: str, count: int) -> None:
        """Record how many iterations an agentic search took."""
        ...

    def observe_step_duration(self, step: str, duration: float) -> None:
        """Record the duration of a single pipeline step in seconds.

        Args:
            step: One of ``plan``, ``embed``, ``search``, ``evaluate``,
                ``compose``.
            duration: Duration in seconds.
        """
        ...

    def observe_results_per_search(self, count: int) -> None:
        """Record the number of results returned by a search."""
        ...

    def observe_duration(self, mode: str, duration: float) -> None:
        """Record end-to-end search duration in seconds."""
        ...
