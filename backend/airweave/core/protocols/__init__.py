"""Core protocols for dependency injection."""

from airweave.core.protocols.circuit_breaker import CircuitBreaker
from airweave.core.protocols.connection_repository import ConnectionRepositoryProtocol
from airweave.core.protocols.event_bus import DomainEvent, EventBus, EventHandler
from airweave.core.protocols.integration_credential_repository import (
    IntegrationCredentialRepositoryProtocol,
)
from airweave.core.protocols.oauth2 import OAuth2ServiceProtocol
from airweave.core.protocols.ocr import OcrProvider
from airweave.core.protocols.source_connection_repository import (
    SourceConnectionRepositoryProtocol,
)
from airweave.core.protocols.source_lifecycle import SourceLifecycleServiceProtocol
from airweave.core.protocols.webhooks import (
    EndpointVerifier,
    WebhookAdmin,
    WebhookPublisher,
    WebhookServiceProtocol,
)

__all__ = [
    "CircuitBreaker",
    "ConnectionRepositoryProtocol",
    "DomainEvent",
    "EndpointVerifier",
    "EventBus",
    "EventHandler",
    "IntegrationCredentialRepositoryProtocol",
    "OAuth2ServiceProtocol",
    "OcrProvider",
    "SourceConnectionRepositoryProtocol",
    "SourceLifecycleServiceProtocol",
    "WebhookAdmin",
    "WebhookPublisher",
    "WebhookServiceProtocol",
]
