"""Types for the embedder registries."""

from airweave.core.protocols.registry import BaseRegistryEntry


class DenseEmbedderEntry(BaseRegistryEntry):
    """A registered dense embedding model.

    Each entry represents one (provider, model) pair — e.g.
    ("openai", "text-embedding-3-small") is a separate entry from
    ("openai", "text-embedding-3-large").
    """

    provider: str
    api_model_name: str
    max_dimensions: int
    max_tokens: int
    supports_matryoshka: bool
    embedder_class_ref: type
    required_setting: str | None = None


class SparseEmbedderEntry(BaseRegistryEntry):
    """A registered sparse embedding model."""

    provider: str
    api_model_name: str
    embedder_class_ref: type
    required_setting: str | None = None
