"""Core protocols for dependency injection.

Domain-specific protocols (repositories, OAuth2, source lifecycle) have moved
to their respective domains/ directories. This module keeps cross-cutting
infrastructure protocols only.
"""

from airweave.core.health.protocols import HealthProbe, HealthServiceProtocol
from airweave.core.protocols.circuit_breaker import CircuitBreaker
from airweave.core.protocols.db_pool_metrics import DbPoolMetrics
from airweave.core.protocols.encryption import CredentialEncryptor
from airweave.core.protocols.event_bus import DomainEvent, EventBus, EventHandler
from airweave.core.protocols.http_metrics import HttpMetrics
from airweave.core.protocols.metrics_renderer import MetricsRenderer
from airweave.core.protocols.metrics_service import MetricsService
from airweave.core.protocols.ocr import OcrProvider
from airweave.core.protocols.webhooks import (
    EndpointVerifier,
    WebhookAdmin,
    WebhookPublisher,
    WebhookServiceProtocol,
)
from airweave.core.protocols.worker_metrics import WorkerMetrics
from airweave.core.protocols.worker_metrics_registry import WorkerMetricsRegistryProtocol

__all__ = [
    "CircuitBreaker",
    "CredentialEncryptor",
    "DbPoolMetrics",
    "DomainEvent",
    "EndpointVerifier",
    "EventBus",
    "EventHandler",
    "HealthProbe",
    "HealthServiceProtocol",
    "HttpMetrics",
    "MetricsRenderer",
    "MetricsService",
    "OcrProvider",
    "WebhookAdmin",
    "WebhookPublisher",
    "WebhookServiceProtocol",
    "WorkerMetrics",
    "WorkerMetricsRegistryProtocol",
]
