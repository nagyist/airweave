"""Unit tests for the readiness-check orchestrator."""

import asyncio
import errno

import pytest

from airweave.core.health_service import HealthService
from airweave.schemas.health import CheckStatus, DependencyCheck


# ---------------------------------------------------------------------------
# Stub probes
# ---------------------------------------------------------------------------


class _StubProbe:
    """Configurable stub that satisfies the HealthProbe protocol."""

    def __init__(self, name: str, *, latency_ms: float = 1.0):
        self._name = name
        self._latency_ms = latency_ms

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> DependencyCheck:
        return DependencyCheck(status=CheckStatus.up, latency_ms=self._latency_ms)


class _FailingProbe:
    """Probe that always raises."""

    def __init__(self, name: str, exc: Exception):
        self._name = name
        self._exc = exc

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> DependencyCheck:
        raise self._exc


class _SlowProbe:
    """Probe that blocks longer than the orchestrator timeout."""

    def __init__(self, name: str, delay: float = 10.0):
        self._name = name
        self._delay = delay

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> DependencyCheck:
        await asyncio.sleep(self._delay)
        return DependencyCheck(status=CheckStatus.up)


class _SkippedProbe:
    """Probe that returns skipped (e.g. Temporal not yet connected)."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> DependencyCheck:
        return DependencyCheck(status=CheckStatus.skipped)


# ---------------------------------------------------------------------------
# check_readiness
# ---------------------------------------------------------------------------


class TestCheckReadiness:
    """Tests for the readiness orchestrator."""

    @pytest.mark.asyncio
    async def test_all_up(self):
        svc = HealthService(
            critical=[_StubProbe("postgres")],
            informational=[_StubProbe("redis"), _StubProbe("temporal")],
        )
        result = await svc.check_readiness(debug=False)

        assert result.status == "ready"
        assert result.checks["postgres"].status == CheckStatus.up
        assert result.checks["redis"].status == CheckStatus.up
        assert result.checks["temporal"].status == CheckStatus.up

    @pytest.mark.asyncio
    async def test_critical_down_means_not_ready(self):
        svc = HealthService(
            critical=[_FailingProbe("postgres", ConnectionRefusedError())],
            informational=[_StubProbe("redis")],
        )
        result = await svc.check_readiness(debug=False)

        assert result.status == "not_ready"
        assert result.checks["postgres"].status == CheckStatus.down
        assert result.checks["redis"].status == CheckStatus.up

    @pytest.mark.asyncio
    async def test_informational_down_still_ready(self):
        svc = HealthService(
            critical=[_StubProbe("postgres")],
            informational=[_FailingProbe("redis", ConnectionError("gone"))],
        )
        result = await svc.check_readiness(debug=False)

        assert result.status == "ready"
        assert result.checks["postgres"].status == CheckStatus.up
        assert result.checks["redis"].status == CheckStatus.down

    @pytest.mark.asyncio
    async def test_timeout_reported_as_down(self):
        svc = HealthService(
            critical=[_SlowProbe("postgres", delay=10.0)],
            informational=[],
        )
        result = await svc.check_readiness(debug=False)

        assert result.status == "not_ready"
        assert result.checks["postgres"].status == CheckStatus.down
        assert result.checks["postgres"].error == "timeout"

    @pytest.mark.asyncio
    async def test_shutting_down_skips_all(self):
        svc = HealthService(
            critical=[_StubProbe("postgres")],
            informational=[_StubProbe("redis")],
        )
        svc.shutting_down = True
        result = await svc.check_readiness(debug=False)

        assert result.status == "not_ready"
        assert result.checks["postgres"].status == CheckStatus.skipped
        assert result.checks["redis"].status == CheckStatus.skipped

    @pytest.mark.asyncio
    async def test_no_probes(self):
        svc = HealthService(critical=[], informational=[])
        result = await svc.check_readiness(debug=False)

        assert result.status == "ready"
        assert result.checks == {}

    @pytest.mark.asyncio
    async def test_skipped_probe_does_not_gate_readiness(self):
        svc = HealthService(
            critical=[_StubProbe("postgres")],
            informational=[_SkippedProbe("temporal")],
        )
        result = await svc.check_readiness(debug=False)

        assert result.status == "ready"
        assert result.checks["temporal"].status == CheckStatus.skipped

    @pytest.mark.asyncio
    async def test_latency_reported(self):
        svc = HealthService(
            critical=[_StubProbe("postgres", latency_ms=2.5)],
            informational=[],
        )
        result = await svc.check_readiness(debug=False)

        assert result.checks["postgres"].latency_ms == 2.5


# ---------------------------------------------------------------------------
# shutting_down property
# ---------------------------------------------------------------------------


class TestShuttingDown:
    """Tests for the shutting_down property."""

    def test_default_is_false(self):
        svc = HealthService(critical=[], informational=[])
        assert svc.shutting_down is False

    def test_setter(self):
        svc = HealthService(critical=[], informational=[])
        svc.shutting_down = True
        assert svc.shutting_down is True


# ---------------------------------------------------------------------------
# _sanitize_error
# ---------------------------------------------------------------------------


class TestSanitizeError:
    """Tests for error message sanitization."""

    def test_debug_shows_full_message(self):
        err = RuntimeError("host=db.internal port=5432 connection refused")
        assert "db.internal" in HealthService._sanitize_error(err, debug=True)

    def test_production_hides_details(self):
        err = RuntimeError("host=db.internal port=5432 connection refused")
        sanitized = HealthService._sanitize_error(err, debug=False)
        assert "db.internal" not in sanitized
        assert sanitized == "unavailable"

    def test_timeout(self):
        assert HealthService._sanitize_error(asyncio.TimeoutError(), debug=False) == "timeout"

    def test_connection_refused(self):
        err = OSError(errno.ECONNREFUSED, "Connection refused")
        assert HealthService._sanitize_error(err, debug=False) == "connection_refused"

    def test_connection_refused_on_cause(self):
        inner = OSError(errno.ECONNREFUSED, "Connection refused")
        outer = RuntimeError("wrapper")
        outer.__cause__ = inner
        assert HealthService._sanitize_error(outer, debug=False) == "connection_refused"
