"""Base registry entry."""

from pydantic import BaseModel, ConfigDict


class BaseRegistryEntry(BaseModel):
    """Registry entry."""

    model_config = ConfigDict(frozen=True)

    short_name: str
    name: str
    description: str | None
    class_name: str
