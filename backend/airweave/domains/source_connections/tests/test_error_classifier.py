"""Tests for error_classifier.classify_error()."""

import pytest

from airweave.core.shared_models import SourceConnectionErrorCategory
from airweave.domains.source_connections.error_classifier import classify_error
from airweave.domains.sources.exceptions import SourceAuthError, SourceTokenRefreshError
from airweave.domains.sources.token_providers.protocol import AuthProviderKind


@pytest.mark.parametrize(
    "exc, auth_method, expected_category",
    [
        # SourceTokenRefreshError → OAUTH_CREDENTIALS_EXPIRED
        (
            SourceTokenRefreshError(
                "token expired",
                source_short_name="github",
                token_provider_kind=AuthProviderKind.OAUTH,
            ),
            "",
            SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
        ),
        # SourceAuthError + CREDENTIAL → API_KEY_INVALID
        (
            SourceAuthError(
                "invalid key",
                source_short_name="stripe",
                status_code=401,
                token_provider_kind=AuthProviderKind.CREDENTIAL,
            ),
            "direct",
            SourceConnectionErrorCategory.API_KEY_INVALID,
        ),
        # SourceAuthError + STATIC → API_KEY_INVALID
        (
            SourceAuthError(
                "bad token",
                source_short_name="openai",
                status_code=401,
                token_provider_kind=AuthProviderKind.STATIC,
            ),
            "direct",
            SourceConnectionErrorCategory.API_KEY_INVALID,
        ),
        # SourceAuthError + OAUTH (non-BYOC) → OAUTH_CREDENTIALS_EXPIRED
        (
            SourceAuthError(
                "oauth failed",
                source_short_name="gmail",
                status_code=401,
                token_provider_kind=AuthProviderKind.OAUTH,
            ),
            "oauth_token",
            SourceConnectionErrorCategory.OAUTH_CREDENTIALS_EXPIRED,
        ),
        # SourceAuthError + OAUTH + BYOC → CLIENT_CREDENTIALS_INVALID
        (
            SourceAuthError(
                "oauth failed",
                source_short_name="custom",
                status_code=401,
                token_provider_kind=AuthProviderKind.OAUTH,
            ),
            "oauth_byoc",
            SourceConnectionErrorCategory.CLIENT_CREDENTIALS_INVALID,
        ),
        # SourceAuthError + AUTH_PROVIDER (account gone) → AUTH_PROVIDER_ACCOUNT_GONE
        (
            SourceAuthError(
                "account not found",
                source_short_name="jira",
                status_code=401,
                token_provider_kind=AuthProviderKind.AUTH_PROVIDER,
            ),
            "",
            SourceConnectionErrorCategory.AUTH_PROVIDER_ACCOUNT_GONE,
        ),
        # SourceAuthError + AUTH_PROVIDER (credentials invalid) → AUTH_PROVIDER_CREDENTIALS_INVALID
        (
            SourceAuthError(
                "unauthorized",
                source_short_name="jira",
                status_code=401,
                token_provider_kind=AuthProviderKind.AUTH_PROVIDER,
            ),
            "",
            SourceConnectionErrorCategory.AUTH_PROVIDER_CREDENTIALS_INVALID,
        ),
    ],
    ids=[
        "token_refresh_error",
        "credential_api_key",
        "static_api_key",
        "oauth_expired",
        "oauth_byoc",
        "auth_provider_gone",
        "auth_provider_invalid",
    ],
)
def test_classify_error(exc, auth_method, expected_category):
    result = classify_error(exc, auth_method)
    assert result.category == expected_category
    assert result.message is not None


def test_classify_error_non_auth_returns_empty():
    result = classify_error(RuntimeError("something broke"))
    assert result.category is None
    assert result.message is None
