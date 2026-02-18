"""Unit tests for DB pool metrics adapter and sampler."""

import asyncio

import pytest

from airweave.adapters.db_pool_metrics import FakeDbPoolMetrics, PrometheusDbPoolMetrics
from airweave.core.db_pool_sampler import DbPoolSampler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakePool:
    """Minimal stand-in for a SQLAlchemy pool."""

    def __init__(
        self,
        size: int = 20,
        checkedout: int = 5,
        checkedin: int = 15,
        overflow: int = 0,
    ) -> None:
        self._size = size
        self._checkedout = checkedout
        self._checkedin = checkedin
        self._overflow = overflow

    def size(self) -> int:
        return self._size

    def checkedout(self) -> int:
        return self._checkedout

    def checkedin(self) -> int:
        return self._checkedin

    def overflow(self) -> int:
        return self._overflow


class BrokenPool:
    """Pool that raises on every read."""

    def size(self) -> int:
        raise RuntimeError("pool gone")

    def checkedout(self) -> int:
        raise RuntimeError("pool gone")

    def checkedin(self) -> int:
        raise RuntimeError("pool gone")

    def overflow(self) -> int:
        raise RuntimeError("pool gone")


# ---------------------------------------------------------------------------
# FakeDbPoolMetrics
# ---------------------------------------------------------------------------


class TestFakeDbPoolMetrics:
    """Tests for the FakeDbPoolMetrics test helper."""

    def test_update_records_values(self):
        fake = FakeDbPoolMetrics()
        fake.update(pool_size=20, checked_out=5, checked_in=15, overflow=0)

        assert fake.pool_size == 20
        assert fake.checked_out == 5
        assert fake.checked_in == 15
        assert fake.overflow == 0
        assert fake.update_count == 1

    def test_clear_resets_all_state(self):
        fake = FakeDbPoolMetrics()
        fake.update(pool_size=20, checked_out=5, checked_in=15, overflow=0)
        fake.clear()

        assert fake.pool_size is None
        assert fake.checked_out is None
        assert fake.checked_in is None
        assert fake.overflow is None
        assert fake.update_count == 0


# ---------------------------------------------------------------------------
# PrometheusDbPoolMetrics
# ---------------------------------------------------------------------------


class TestPrometheusDbPoolMetrics:
    """Tests for the Prometheus adapter."""

    def test_registry_is_separate_from_default(self):
        from prometheus_client import REGISTRY

        adapter = PrometheusDbPoolMetrics()
        assert adapter._registry is not REGISTRY

    def test_update_sets_gauges(self):
        from prometheus_client import CollectorRegistry, generate_latest

        registry = CollectorRegistry()
        adapter = PrometheusDbPoolMetrics(registry=registry, max_overflow=40)

        adapter.update(pool_size=20, checked_out=5, checked_in=15, overflow=2)
        output = generate_latest(registry).decode()

        assert "airweave_db_pool_size 20.0" in output
        assert "airweave_db_pool_checked_out 5.0" in output
        assert "airweave_db_pool_checked_in 15.0" in output
        assert "airweave_db_pool_overflow 2.0" in output
        assert "airweave_db_pool_max_overflow 40.0" in output

    def test_max_overflow_set_once(self):
        """max_overflow gauge should reflect the constructor arg."""
        from prometheus_client import CollectorRegistry, generate_latest

        registry = CollectorRegistry()
        PrometheusDbPoolMetrics(registry=registry, max_overflow=99)

        output = generate_latest(registry).decode()
        assert "airweave_db_pool_max_overflow 99.0" in output

    def test_update_overwrites_previous(self):
        from prometheus_client import CollectorRegistry, generate_latest

        registry = CollectorRegistry()
        adapter = PrometheusDbPoolMetrics(registry=registry)

        adapter.update(pool_size=10, checked_out=1, checked_in=9, overflow=0)
        adapter.update(pool_size=20, checked_out=8, checked_in=12, overflow=3)

        output = generate_latest(registry).decode()
        assert "airweave_db_pool_size 20.0" in output
        assert "airweave_db_pool_checked_out 8.0" in output


# ---------------------------------------------------------------------------
# DbPoolSampler
# ---------------------------------------------------------------------------


class TestDbPoolSampler:
    """Tests for the background pool sampler."""

    @pytest.mark.asyncio
    async def test_samples_pool_after_one_tick(self):
        """After one sampling tick the fake metrics should reflect pool state."""
        pool = FakePool(size=20, checkedout=3, checkedin=17, overflow=1)
        fake = FakeDbPoolMetrics()
        sampler = DbPoolSampler(pool, fake, interval=0.01)

        await sampler.start()
        # Give the loop enough time for at least one tick.
        await asyncio.sleep(0.05)
        await sampler.stop()

        assert fake.pool_size == 20
        assert fake.checked_out == 3
        assert fake.checked_in == 17
        assert fake.overflow == 1
        assert fake.update_count >= 1

    @pytest.mark.asyncio
    async def test_stop_cancels_cleanly(self):
        """stop() should cancel the task without raising."""
        pool = FakePool()
        fake = FakeDbPoolMetrics()
        sampler = DbPoolSampler(pool, fake, interval=0.01)

        await sampler.start()
        await sampler.stop()

        assert sampler._task is None

    @pytest.mark.asyncio
    async def test_stop_is_safe_when_not_started(self):
        """stop() before start() should not raise."""
        pool = FakePool()
        fake = FakeDbPoolMetrics()
        sampler = DbPoolSampler(pool, fake)

        await sampler.stop()  # no-op

    @pytest.mark.asyncio
    async def test_pool_error_does_not_crash_loop(self):
        """A transient pool error should be swallowed; the loop continues."""
        broken = BrokenPool()
        fake = FakeDbPoolMetrics()
        sampler = DbPoolSampler(broken, fake, interval=0.01)

        await sampler.start()
        await asyncio.sleep(0.05)
        await sampler.stop()

        # Metrics were never successfully updated.
        assert fake.update_count == 0
