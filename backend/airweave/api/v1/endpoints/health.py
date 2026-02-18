"""Health check endpoints."""

from fastapi import Request, Response

from airweave.api.router import TrailingSlashRouter
from airweave.core.config import settings
from airweave.core.health import check_readiness
from airweave.schemas.health import LivenessResponse, ReadinessResponse

router = TrailingSlashRouter()


@router.get("")
async def health_check() -> dict[str, str]:
    """Check if the API is healthy.

    Returns:
    --------
        dict: A dictionary containing the status of the API.
    """
    return {"status": "healthy"}


@router.get("/live")
async def liveness() -> LivenessResponse:
    """Liveness probe — confirms the process is running."""
    return LivenessResponse()


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    responses={503: {"model": ReadinessResponse}},
)
async def readiness(request: Request, response: Response) -> ReadinessResponse:
    """Readiness probe — checks critical dependencies.

    Only critical probes (Postgres) gate the HTTP status code.  Informational
    probes (Redis, Temporal) are reported for observability but do not cause
    a 503.
    """
    result = await check_readiness(
        critical=getattr(request.app.state, "health_probes_critical", []),
        informational=getattr(request.app.state, "health_probes_informational", []),
        shutting_down=getattr(request.app.state, "shutting_down", False),
        debug=settings.DEBUG,
    )

    if result.status != "ready":
        response.status_code = 503
    return result
