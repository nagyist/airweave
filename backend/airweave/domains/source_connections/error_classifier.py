"""Error classifier for credential/auth failures on source connections.

Maps source exceptions to SourceConnectionErrorCategory values using
token_provider_kind from SourceAuthError as the primary signal.
"""

from __future__ import annotations

from airweave.core.shared_models import SourceConnectionErrorCategory
from airweave.domains.source_connections.types import ErrorClassification
from airweave.domains.sources.exceptions import SourceAuthError, SourceTokenRefreshError
from airweave.domains.sources.token_providers.protocol import AuthProviderKind


def classify_error(exc: Exception, auth_method: str = "") -> ErrorClassification:
    """Classify an exception into an error category for UI remediation.

    Args:
        exc: The exception that caused the sync failure.
        auth_method: The authentication_method string from the source connection
            (fallback when token_provider_kind is not available).

    Returns:
        ErrorClassification with category and message, or empty if not a
        credential error.
    """
    # Unwrap chained exceptions (e.g. SourceValidationError wrapping SourceAuthError)
    if (
        not isinstance(exc, (SourceAuthError, SourceTokenRefreshError))
        and exc.__cause__
        and isinstance(exc.__cause__, (SourceAuthError, SourceTokenRefreshError))
    ):
        return classify_error(exc.__cause__, auth_method)

    if isinstance(exc, SourceTokenRefreshError):
        return ErrorClassification(
            category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
            message=str(exc),
        )

    if isinstance(exc, SourceAuthError):
        kind = exc.token_provider_kind

        if kind == AuthProviderKind.CREDENTIAL:
            return ErrorClassification(
                category=SourceConnectionErrorCategory.API_KEY_INVALID,
                message=str(exc),
            )

        if kind == AuthProviderKind.OAUTH:
            # BYOC (bring your own credentials) OAuth — client secret may be invalid
            if auth_method == "oauth_byoc":
                return ErrorClassification(
                    category=SourceConnectionErrorCategory.CLIENT_CREDENTIALS_INVALID,
                    message=str(exc),
                )
            # Regular OAuth — token expired
            return ErrorClassification(
                category=SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
                message=str(exc),
            )

        if kind == AuthProviderKind.AUTH_PROVIDER:
            # Distinguish "account gone" from "credentials invalid" by message heuristic
            msg_lower = str(exc).lower()
            if any(
                term in msg_lower
                for term in ("not found", "deleted", "removed", "gone", "deactivated")
            ):
                return ErrorClassification(
                    category=SourceConnectionErrorCategory.AUTH_PROVIDER_ACCOUNT_GONE,
                    message=str(exc),
                )
            return ErrorClassification(
                category=SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
                message=str(exc),
            )

        if kind == AuthProviderKind.STATIC:
            return ErrorClassification(
                category=SourceConnectionErrorCategory.API_KEY_INVALID,
                message=str(exc),
            )

    # Not a credential error — return empty classification
    return ErrorClassification(category=None, message=None)
