"""Readiness check logic for the health probe."""

import asyncio
import errno
from collections.abc import Sequence

from airweave.core.protocols.health import HealthProbe
from airweave.schemas.health import CheckStatus, DependencyCheck, ReadinessResponse

# Timeout applied to each individual dependency check.
_CHECK_TIMEOUT: float = 5.0


def _sanitize_error(exc: Exception, *, debug: bool) -> str:
    """Return an error string safe for external consumption.

    In debug mode the full exception text is returned.  In production the
    message is reduced to a generic category so that internal hostnames,
    ports, or stack traces are never leaked.
    """
    if debug:
        return str(exc)

    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"

    os_err = getattr(exc, "errno", None) or (
        getattr(exc.__cause__, "errno", None) if exc.__cause__ else None
    )
    if os_err == errno.ECONNREFUSED:
        return "connection_refused"

    return "unavailable"


async def _run_probe(probe: HealthProbe) -> tuple[str, DependencyCheck | Exception]:
    """Execute a single probe with a timeout, returning ``(name, result)``."""
    try:
        result = await asyncio.wait_for(probe.check(), timeout=_CHECK_TIMEOUT)
    except Exception as exc:
        return probe.name, exc
    return probe.name, result


async def check_readiness(
    *,
    critical: Sequence[HealthProbe],
    informational: Sequence[HealthProbe],
    shutting_down: bool,
    debug: bool,
) -> ReadinessResponse:
    """Evaluate readiness by probing dependencies concurrently.

    *critical* probes gate the HTTP status — any failure flips the
    response to ``not_ready``.  *informational* probes are surfaced in
    the response body but do not affect the status code.
    """
    all_probes = [*critical, *informational]
    critical_names = {p.name for p in critical}

    skipped = DependencyCheck(status=CheckStatus.skipped)

    if shutting_down:
        return ReadinessResponse(
            status="not_ready",
            checks={p.name: skipped for p in all_probes},
        )

    results = await asyncio.gather(
        *(_run_probe(p) for p in all_probes),
        return_exceptions=True,
    )

    checks: dict[str, DependencyCheck] = {}
    ready = True

    for entry in results:
        # asyncio.gather with return_exceptions=True should not raise here,
        # but guard against unexpected gather-level errors defensively.
        if isinstance(entry, BaseException):
            continue

        name, outcome = entry

        if isinstance(outcome, BaseException):
            checks[name] = DependencyCheck(
                status=CheckStatus.down,
                error=_sanitize_error(outcome, debug=debug),
            )
            if name in critical_names:
                ready = False
        else:
            checks[name] = outcome

    return ReadinessResponse(
        status="ready" if ready else "not_ready",
        checks=checks,
    )
