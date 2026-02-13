from dataclasses import dataclass

from airweave.adapters.registries.base import BaseRegistryEntry


@dataclass(frozen=True)
class EntityDefinitionEntry(BaseRegistryEntry):
    """Precomputed entity definition metadata."""

    entity_class_ref: type
    module_name: str  # source short_name this entity belongs to (e.g. "asana")
