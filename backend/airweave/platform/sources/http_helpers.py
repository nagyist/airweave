"""HTTP response → domain exception translation for source connectors.

Provides ``raise_for_status()`` and ``handle_response()`` that convert
raw ``httpx`` responses into the typed exception hierarchy from
``airweave.domains.sources.exceptions``, so individual sources don't
duplicate status-code triage logic.

Usage in a source::

    from airweave.platform.sources.http_helpers import raise_for_status

    resp = await self._http_client.get(url, headers=headers)
    raise_for_status(resp, source_short_name=self.short_name)
    data = resp.json()

Or for per-entity calls where 403/404 should skip, not abort::

    from airweave.platform.sources.http_helpers import raise_for_status

    resp = await self._http_client.get(entity_url, headers=headers)
    raise_for_status(resp, source_short_name=self.short_name, entity_id=item_id)
"""

from __future__ import annotations

import httpx

from airweave.domains.sources.exceptions import (
    SourceAuthError,
    SourceEntityForbiddenError,
    SourceEntityNotFoundError,
    SourceEntitySkippedError,
    SourceError,
    SourceRateLimitError,
    SourceServerError,
)


def raise_for_status(
    response: httpx.Response,
    *,
    source_short_name: str = "",
    entity_id: str = "",
    context: str = "",
) -> None:
    """Raise a domain exception if the response indicates an error.

    Args:
        response: The httpx response to check.
        source_short_name: Source identifier for the exception.
        entity_id: If set, 403/404 raise per-entity exceptions (skip, don't abort).
        context: Optional string appended to the error message (e.g. "fetching page list").
    """
    if response.is_success:
        return

    status = response.status_code
    detail = _extract_detail(response)
    ctx = f" while {context}" if context else ""

    _STATUS_HANDLERS.get(status, _handle_fallback)(
        response, status, detail, ctx, source_short_name, entity_id
    )


def _handle_401(_resp: httpx.Response, _s: int, detail: str, ctx: str, sn: str, _eid: str) -> None:
    raise SourceAuthError(
        f"Unauthorized (401){ctx} — credentials invalid or revoked. {detail}",
        source_short_name=sn,
        status_code=401,
    )


def _handle_403(_resp: httpx.Response, _s: int, detail: str, ctx: str, sn: str, eid: str) -> None:
    if eid:
        raise SourceEntityForbiddenError(
            f"Forbidden (403){ctx}: {detail}", source_short_name=sn, entity_id=eid
        )
    raise SourceAuthError(
        f"Forbidden (403){ctx} — insufficient permissions. {detail}",
        source_short_name=sn,
        status_code=403,
    )


def _handle_404(_resp: httpx.Response, _s: int, detail: str, ctx: str, sn: str, eid: str) -> None:
    if eid:
        raise SourceEntityNotFoundError(
            f"Not found (404){ctx}: {detail}", source_short_name=sn, entity_id=eid
        )
    raise SourceError(f"Not found (404){ctx}: {detail}", source_short_name=sn)


def _handle_429(resp: httpx.Response, _s: int, _detail: str, ctx: str, sn: str, _eid: str) -> None:
    retry_after = _parse_retry_after(resp)
    raise SourceRateLimitError(
        retry_after=retry_after,
        source_short_name=sn,
        message=f"Rate limited (429){ctx}. Retry after {retry_after:.1f}s",
    )


def _handle_redirect(
    resp: httpx.Response, status: int, _detail: str, ctx: str, sn: str, eid: str
) -> None:
    location = resp.headers.get("Location", "<unknown>")
    if eid:
        raise SourceEntitySkippedError(
            f"Redirect ({status}){ctx} → {location}",
            source_short_name=sn,
            entity_id=eid,
            reason=f"redirect_{status}",
        )
    raise SourceError(f"Unexpected redirect ({status}){ctx} → {location}", source_short_name=sn)


def _handle_fallback(
    resp: httpx.Response, status: int, detail: str, ctx: str, sn: str, _eid: str
) -> None:
    if status >= 500:
        raise SourceServerError(
            f"Server error ({status}){ctx}: {detail}",
            source_short_name=sn,
            status_code=status,
        )
    if status == 400 and _is_rate_limit_disguised_as_400(resp):
        retry_after = _parse_retry_after(resp, default=30.0)
        raise SourceRateLimitError(
            retry_after=retry_after,
            source_short_name=sn,
            message=f"Rate limited (400 disguised){ctx}. Retry after {retry_after:.1f}s",
        )
    raise SourceError(f"HTTP {status}{ctx}: {detail}", source_short_name=sn)


_STATUS_HANDLERS = {
    401: _handle_401,
    403: _handle_403,
    404: _handle_404,
    429: _handle_429,
    301: _handle_redirect,
    302: _handle_redirect,
    307: _handle_redirect,
    308: _handle_redirect,
}


def translate_httpx_error(
    exc: httpx.HTTPStatusError,
    *,
    source_short_name: str = "",
    entity_id: str = "",
    context: str = "",
) -> None:
    """Re-raise an ``httpx.HTTPStatusError`` as the correct domain exception.

    Convenience for sources that still use ``response.raise_for_status()``
    and catch the httpx error::

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            translate_httpx_error(exc, source_short_name=self.short_name)
    """
    raise_for_status(
        exc.response,
        source_short_name=source_short_name,
        entity_id=entity_id,
        context=context,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _extract_detail(response: httpx.Response, max_length: int = 200) -> str:
    """Pull a human-readable snippet from the response body."""
    try:
        data = response.json()
        for key in ("message", "error_description", "error", "detail", "errors"):
            val = data.get(key)
            if val:
                return str(val)[:max_length]
        return str(data)[:max_length]
    except Exception:
        text = response.text[:max_length] if response.text else ""
        return text or f"(no body, status {response.status_code})"


def _parse_retry_after(
    response: httpx.Response,
    default: float = 30.0,
) -> float:
    """Parse Retry-After header, fall back to *default*."""
    raw = response.headers.get("Retry-After")
    if raw:
        try:
            return max(float(raw), 1.0)
        except (ValueError, TypeError):
            pass
    return default


def _is_rate_limit_disguised_as_400(response: httpx.Response) -> bool:
    """Detect APIs that return 400 instead of 429 for rate limits (e.g. Zoho)."""
    try:
        data = response.json()
        error_desc = str(data.get("error_description", "")).lower()
        error_type = str(data.get("error", "")).lower()
        if "too many requests" in error_desc and error_type == "access denied":
            return True
        if "rate limit" in error_desc:
            return True
    except Exception:
        pass
    return False
