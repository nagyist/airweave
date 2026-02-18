"""HealthProbe protocol for dependency readiness checks.

Each probe wraps a single infrastructure dependency (database, cache,
message broker, etc.) and exposes a uniform ``check()`` coroutine that
the readiness orchestrator can call without knowing the concrete type.
"""

from typing import Protocol, runtime_checkable

from airweave.schemas.health import DependencyCheck


@runtime_checkable
class HealthProbe(Protocol):
    """Protocol for a single infrastructure health check.

    Implementations return a ``DependencyCheck`` on success and **raise**
    on failure.  The orchestrator handles timeouts and error sanitization,
    so probes can stay simple.

    The critical-vs-informational distinction is a deployment concern and
    lives in the wiring layer, not in the probe itself.
    """

    @property
    def name(self) -> str:
        """Human-readable identifier surfaced in the readiness response."""
        ...

    async def check(self) -> DependencyCheck:
        """Probe the dependency and return its status.

        Returns:
            A ``DependencyCheck`` with ``status=up`` and measured latency.

        Raises:
            Any exception on failure — the orchestrator will catch it.
        """
        ...
