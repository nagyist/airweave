"""Core protocols for dependency injection."""

from airweave.core.protocols.circuit_breaker import CircuitBreaker
from airweave.core.protocols.event_bus import DomainEvent, EventBus, EventHandler
from airweave.core.protocols.ocr import OcrProvider
from airweave.core.protocols.repositories import (
    BasePublicRepositoryProtocol,
    SourceRepositoryProtocol,
)
from airweave.core.protocols.webhooks import WebhookAdmin, WebhookPublisher

__all__ = [
    "CircuitBreaker",
    "DomainEvent",
    "EventBus",
    "EventHandler",
    "OcrProvider",
    "BasePublicRepositoryProtocol",
    "SourceRepositoryProtocol",
    "WebhookAdmin",
    "WebhookPublisher",
]
