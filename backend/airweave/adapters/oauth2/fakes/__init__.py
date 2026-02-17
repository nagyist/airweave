"""Fake OAuth2 implementations for testing."""

from airweave.adapters.oauth2.fakes.oauth2_service import (
    FakeOAuth2Service,
    FakeOAuth2TokenResponse,
)

__all__ = [
    "FakeOAuth2Service",
    "FakeOAuth2TokenResponse",
]
