"""Fake DbPoolMetrics for testing.

Records the latest ``update()`` kwargs so tests can assert on pool gauge
values without reaching into prometheus-client internals.
"""


class FakeDbPoolMetrics:
    """In-memory spy implementing the DbPoolMetrics protocol.

    Usage:
        fake = FakeDbPoolMetrics()
        fake.update(pool_size=20, checked_out=5, checked_in=15, overflow=0)
        assert fake.pool_size == 20
    """

    def __init__(self) -> None:
        self.pool_size: int | None = None
        self.checked_out: int | None = None
        self.checked_in: int | None = None
        self.overflow: int | None = None
        self.update_count: int = 0

    def update(
        self,
        *,
        pool_size: int,
        checked_out: int,
        checked_in: int,
        overflow: int,
    ) -> None:
        self.pool_size = pool_size
        self.checked_out = checked_out
        self.checked_in = checked_in
        self.overflow = overflow
        self.update_count += 1

    # -- test helpers --

    def clear(self) -> None:
        """Reset all recorded state."""
        self.pool_size = None
        self.checked_out = None
        self.checked_in = None
        self.overflow = None
        self.update_count = 0
