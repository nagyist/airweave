"""Protocols for the embedder registries."""

from typing import Protocol

from airweave.core.protocols.registry import RegistryProtocol
from airweave.domains.embedders.types import DenseEmbedderEntry, SparseEmbedderEntry


class DenseEmbedderRegistryProtocol(RegistryProtocol[DenseEmbedderEntry], Protocol):
    """Dense embedder registry protocol."""

    def list_for_provider(self, provider: str) -> list[DenseEmbedderEntry]:
        """List all dense embedder entries for a provider."""
        ...


class SparseEmbedderRegistryProtocol(RegistryProtocol[SparseEmbedderEntry], Protocol):
    """Sparse embedder registry protocol."""

    def list_for_provider(self, provider: str) -> list[SparseEmbedderEntry]:
        """List all sparse embedder entries for a provider."""
        ...
