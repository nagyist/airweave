"""Fake repository implementations for testing."""

from airweave.adapters.repositories.fakes.connection import FakeConnectionRepository
from airweave.adapters.repositories.fakes.integration_credential import (
    FakeIntegrationCredentialRepository,
)
from airweave.adapters.repositories.fakes.source_connection import (
    FakeSourceConnectionRepository,
)

__all__ = [
    "FakeConnectionRepository",
    "FakeIntegrationCredentialRepository",
    "FakeSourceConnectionRepository",
]
