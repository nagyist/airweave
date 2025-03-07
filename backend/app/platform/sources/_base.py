"""Base source class."""

from abc import abstractmethod
from typing import Any, AsyncGenerator, Optional, Type

from pydantic import BaseModel

from app.platform.entities._base import BaseEntity, ChunkEntity


class BaseSource:
    """Base source class."""

    @classmethod
    @abstractmethod
    async def create(cls, credentials: Optional[Any] = None) -> "BaseSource":
        """Create a new source instance.

        Args:
            credentials: Optional credentials for authenticated sources.
                       For AuthType.none sources, this can be None.
        """
        pass

    @abstractmethod
    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate entities for the source."""
        pass


class Relation(BaseModel):
    """A relation between two entities."""

    source_entity_type: Type[ChunkEntity] | Type[BaseEntity]
    source_entity_id_attribute: str
    target_entity_type: Type[ChunkEntity] | Type[BaseEntity]
    target_entity_id_attribute: str
    relation_type: str
