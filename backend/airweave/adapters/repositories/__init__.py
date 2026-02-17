"""Repository adapters wrapping CRUD singletons behind protocols."""

from airweave.adapters.repositories.connection import ConnectionRepository
from airweave.adapters.repositories.integration_credential import (
    IntegrationCredentialRepository,
)
from airweave.adapters.repositories.source_connection import (
    SourceConnectionRepository,
)

__all__ = [
    "ConnectionRepository",
    "IntegrationCredentialRepository",
    "SourceConnectionRepository",
]
