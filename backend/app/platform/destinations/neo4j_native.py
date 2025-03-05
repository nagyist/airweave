"""Neo4j native destination implementation."""

from uuid import UUID

from app.platform.auth.schemas import AuthType
from app.platform.decorators import destination
from app.platform.destinations.neo4j import Neo4jDestination
from app.platform.embedding_models._base import BaseEmbeddingModel


@destination("Neo4j Native", "neo4j_native", AuthType.config_class, "Neo4jAuthConfig")
class Neo4jNativeDestination(Neo4jDestination):
    """Neo4j native destination implementation.

    This class extends the Neo4j destination for natively hosted Neo4j instances.
    """

    @classmethod
    async def create(
        cls,
        sync_id: UUID,
        embedding_model: BaseEmbeddingModel,
    ) -> "Neo4jNativeDestination":
        """Create a new Neo4j Native destination.

        Args:
            sync_id: The ID of the sync
            embedding_model: The embedding model to use for vectorization

        Returns:
            Neo4jNativeDestination: The initialized Neo4j Native destination
        """
        # Leverage the parent class's create method for most functionality
        instance = await super().create(sync_id, embedding_model)
        return instance
