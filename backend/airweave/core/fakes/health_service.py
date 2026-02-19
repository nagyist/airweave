"""Fake HealthService for testing Container consumers."""

from airweave.schemas.health import CheckStatus, DependencyCheck, ReadinessResponse


class FakeHealthService:
    """In-memory fake satisfying the ``HealthService`` protocol.

    Records calls and supports canned responses via ``set_response()``.
    """

    def __init__(self) -> None:
        """Initialise with a default ``ready`` canned response."""
        self._shutting_down = False
        self._response = ReadinessResponse(
            status="ready",
            checks={"fake": DependencyCheck(status=CheckStatus.up)},
        )
        self.check_readiness_calls: list[dict] = []

    # -- shutdown flag -------------------------------------------------------

    @property
    def shutting_down(self) -> bool:
        """Whether the application is shutting down."""
        return self._shutting_down

    @shutting_down.setter
    def shutting_down(self, value: bool) -> None:
        self._shutting_down = value

    # -- readiness check -----------------------------------------------------

    async def check_readiness(self, *, debug: bool) -> ReadinessResponse:
        """Return the canned response and record the call."""
        self.check_readiness_calls.append({"debug": debug})
        return self._response

    # -- test helpers --------------------------------------------------------

    def set_response(self, response: ReadinessResponse) -> None:
        """Set the canned response returned by ``check_readiness``."""
        self._response = response
