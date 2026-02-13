"""Protocols for registries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from airweave.adapters.registries.auth_provider import AuthProviderRegistryEntry
    from airweave.adapters.registries.entity_definition import EntityDefinitionEntry
    from airweave.adapters.registries.source import SourceRegistryEntry


class SourceRegistryProtocol(Protocol):
    """Source registry protocol."""

    def get(self, short_name: str) -> SourceRegistryEntry:
        """Get an entry by short name. Raises KeyError if not found."""
        ...

    def list_all(self) -> list[SourceRegistryEntry]:
        """List all registered entries."""
        ...


class AuthProviderRegistryProtocol(Protocol):
    """Auth provider registry protocol."""

    def get(self, short_name: str) -> AuthProviderRegistryEntry:
        """Get an entry by short name. Raises KeyError if not found."""
        ...

    def list_all(self) -> list[AuthProviderRegistryEntry]:
        """List all registered entries."""
        ...


class EntityDefinitionRegistryProtocol(Protocol):
    """Entity definition registry protocol."""

    def get(self, short_name: str) -> EntityDefinitionEntry:
        """Get an entry by short name. Raises KeyError if not found."""
        ...

    def list_all(self) -> list[EntityDefinitionEntry]:
        """List all registered entries."""
        ...

    def list_for_source(self, source_short_name: str) -> list[EntityDefinitionEntry]:
        """List all entity definitions for a given source."""
        ...
