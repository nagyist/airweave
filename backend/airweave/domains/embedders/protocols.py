"""Protocols for embedders and embedder registries."""

from typing import Protocol

from airweave.core.protocols.registry import RegistryProtocol
from airweave.domains.embedders.types import (
    DenseEmbedderEntry,
    DenseEmbedding,
    SparseEmbedderEntry,
    SparseEmbedding,
)

# ---------------------------------------------------------------------------
# Embedder protocols
# ---------------------------------------------------------------------------


class DenseEmbedderProtocol(Protocol):
    """Protocol for dense embedding models."""

    async def embed(self, text: str) -> DenseEmbedding:
        """Embed a single text into a dense vector."""
        ...

    async def embed_many(self, texts: list[str]) -> list[DenseEmbedding]:
        """Embed a batch of texts into dense vectors."""
        ...

    async def close(self) -> None:
        """Release any held resources (HTTP clients, etc.)."""
        ...


class SparseEmbedderProtocol(Protocol):
    """Protocol for sparse embedding models."""

    async def embed(self, text: str) -> SparseEmbedding:
        """Embed a single text into a sparse vector."""
        ...

    async def embed_many(self, texts: list[str]) -> list[SparseEmbedding]:
        """Embed a batch of texts into sparse vectors."""
        ...

    async def close(self) -> None:
        """Release any held resources (HTTP clients, etc.)."""
        ...


# ---------------------------------------------------------------------------
# Registry protocols
# ---------------------------------------------------------------------------


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
