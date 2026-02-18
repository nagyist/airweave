"""Fake AgenticSearchMetrics for testing.

Records all calls in memory so tests can assert on metrics behaviour
without reaching into prometheus-client internals.
"""

from dataclasses import dataclass


@dataclass
class StepDurationRecord:
    """Single observed step duration."""

    step: str
    duration: float


class FakeAgenticSearchMetrics:
    """In-memory spy implementing the AgenticSearchMetrics protocol."""

    def __init__(self) -> None:
        self.search_requests: list[tuple[str, bool]] = []
        self.search_errors: list[tuple[str, bool]] = []
        self.iterations: list[tuple[str, int]] = []
        self.step_durations: list[StepDurationRecord] = []
        self.results_counts: list[int] = []
        self.durations: list[tuple[str, float]] = []

    def inc_search_requests(self, mode: str, streaming: bool) -> None:
        self.search_requests.append((mode, streaming))

    def inc_search_errors(self, mode: str, streaming: bool) -> None:
        self.search_errors.append((mode, streaming))

    def observe_iterations(self, mode: str, count: int) -> None:
        self.iterations.append((mode, count))

    def observe_step_duration(self, step: str, duration: float) -> None:
        self.step_durations.append(StepDurationRecord(step, duration))

    def observe_results_per_search(self, count: int) -> None:
        self.results_counts.append(count)

    def observe_duration(self, mode: str, duration: float) -> None:
        self.durations.append((mode, duration))

    # -- test helpers --

    def clear(self) -> None:
        """Reset all recorded state."""
        self.search_requests.clear()
        self.search_errors.clear()
        self.iterations.clear()
        self.step_durations.clear()
        self.results_counts.clear()
        self.durations.clear()
