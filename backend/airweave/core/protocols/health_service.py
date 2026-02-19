"""HealthService protocol for the readiness-check facade.

The HealthService owns shutdown state and orchestrates individual
``HealthProbe`` instances.  The protocol is deliberately thin — no
``start()``/``stop()`` lifecycle — because the health domain has no
background services.
"""

from typing import Protocol, runtime_checkable

from airweave.schemas.health import ReadinessResponse


@runtime_checkable
class HealthServiceProtocol(Protocol):
    """Facade that orchestrates health probes and owns shutdown state."""

    @property
    def shutting_down(self) -> bool:
        """Whether the application is shutting down."""
        ...

    @shutting_down.setter
    def shutting_down(self, value: bool) -> None: ...

    async def check_readiness(self, *, debug: bool) -> ReadinessResponse:
        """Evaluate readiness by probing dependencies concurrently."""
        ...
