"""Base registry entry."""

from dataclasses import dataclass

from pydantic import BaseModel


@dataclass(frozen=True)
class BaseRegistryEntry(BaseModel):
    """Registry entry."""

    short_name: str
    name: str
    description: str | None
    class_name: str
