"""Protocols for registries."""

from typing import Protocol, TypeVar

from airweave.adapters.registries.auth_provider import AuthProviderRegistryEntry
from airweave.adapters.registries.base import BaseRegistryEntry
from airweave.adapters.registries.source import SourceRegistryEntry

EntryT = TypeVar("EntryT", bound=BaseRegistryEntry, covariant=True)


class RegistryProtocol(Protocol[EntryT]):
    """Base protocol for in-memory registries.

    Built once at startup. All lookups are synchronous dict reads.
    """

    def get(self, short_name: str) -> EntryT:
        """Get an entry by short name. Raises KeyError if not found."""
        ...

    def list_all(self) -> list[EntryT]:
        """List all registered entries."""
        ...


class SourceRegistryProtocol(RegistryProtocol[SourceRegistryEntry], Protocol):
    """Source registry protocol."""

    pass


class AuthProviderRegistryProtocol(RegistryProtocol[AuthProviderRegistryEntry], Protocol):
    """Auth provider registry protocol."""

    pass
