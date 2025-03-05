"""Neo4j destination implementation."""

import os
from typing import Dict, List, Optional
from uuid import UUID

from neo4j import AsyncDriver, AsyncGraphDatabase

from app.core.logging import logger
from app.platform.auth.schemas import AuthType
from app.platform.configs.auth import Neo4jAuthConfig
from app.platform.decorators import destination
from app.platform.destinations._base import GraphDBDestination
from app.platform.embedding_models._base import BaseEmbeddingModel
from app.platform.entities._base import ChunkEntity


@destination("Neo4j", "neo4j", AuthType.config_class, "Neo4jAuthConfig")
class Neo4jDestination(GraphDBDestination):
    """Neo4j destination implementation."""

    def __init__(self):
        """Initialize the Neo4j destination."""
        self.driver: Optional[AsyncDriver] = None
        self.uri: Optional[str] = None
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        self.sync_id: Optional[UUID] = None
        self.embedding_model: Optional[BaseEmbeddingModel] = None

    @classmethod
    async def create(
        cls,
        sync_id: UUID,
        embedding_model: BaseEmbeddingModel,
    ) -> "Neo4jDestination":
        """Create a new Neo4j destination.

        Args:
            sync_id: The ID of the sync
            embedding_model: The embedding model to use for vectorization

        Returns:
            Neo4jDestination: The initialized Neo4j destination
        """
        instance = cls()
        instance.sync_id = sync_id
        instance.embedding_model = embedding_model

        # Get credentials from environment variables
        instance.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        instance.username = os.getenv("NEO4J_USER", "neo4j")
        instance.password = os.getenv("NEO4J_PASSWORD", "password")

        # Initialize driver
        instance.driver = AsyncGraphDatabase.driver(
            instance.uri, auth=(instance.username, instance.password)
        )

        # Test connection
        try:
            async with instance.driver.session() as session:
                result = await session.run("RETURN 1 as test")
                record = await result.single()
                if record and record["test"] == 1:
                    logger.info(f"Successfully connected to Neo4j at {instance.uri}")
                else:
                    logger.error("Connection test to Neo4j failed")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise

        return instance

    @classmethod
    async def get_credentials(cls) -> Optional[Neo4jAuthConfig]:
        """Get Neo4j credentials from environment variables.

        Returns:
            Optional[Neo4jAuthConfig]: The Neo4j credentials if found
        """
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")

        if not all([uri, username, password]):
            return None

        return Neo4jAuthConfig(uri=uri, username=username, password=password)

    async def setup_collection(self, sync_id: UUID) -> None:
        """Set up the graph by creating necessary constraints.

        Args:
            sync_id: The ID of the sync
        """
        self.sync_id = sync_id

        # Create unique constraint on db_entity_id
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.db_entity_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.sync_id)",
            "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.entity_type)",
        ]

        async with self.driver.session() as session:
            for constraint in constraints:
                try:
                    await session.run(constraint)
                except Exception as e:
                    logger.error(f"Error creating constraint: {str(e)}")
                    raise

    async def insert(self, entity: ChunkEntity) -> None:
        """Insert a single entity as a node in Neo4j.

        Args:
            entity: The entity to insert
        """
        # Convert entity to dict
        entity_data = entity.model_dump()

        # Add sync_id and entity type
        entity_data["sync_id"] = str(self.sync_id)
        entity_data["entity_type"] = entity.__class__.__name__

        # Generate label based on entity class name
        label = entity.__class__.__name__

        # Create the node
        await self.create_node(entity_data, label)

    async def bulk_insert(self, entities: list[ChunkEntity]) -> None:
        """Bulk insert entities as nodes in Neo4j.

        Args:
            entities: The entities to insert
        """
        if not entities:
            return

        # Prepare nodes for bulk creation
        nodes = []
        for entity in entities:
            entity_data = entity.model_dump()
            entity_data["sync_id"] = str(self.sync_id)
            entity_data["entity_type"] = entity.__class__.__name__

            nodes.append({"label": entity.__class__.__name__, "properties": entity_data})

        # Bulk create nodes
        await self.bulk_create_nodes(nodes)

    async def delete(self, db_entity_id: UUID) -> None:
        """Delete a single entity node from Neo4j.

        Args:
            db_entity_id: The ID of the entity to delete
        """
        query = "MATCH (e:Entity {db_entity_id: $id}) DETACH DELETE e"
        async with self.driver.session() as session:
            await session.run(query, id=str(db_entity_id))

    async def bulk_delete(self, entity_ids: list[str]) -> None:
        """Bulk delete entity nodes from Neo4j.

        Args:
            entity_ids: The IDs of the entities to delete
        """
        if not entity_ids:
            return

        query = "MATCH (e:Entity) WHERE e.db_entity_id IN $ids DETACH DELETE e"
        async with self.driver.session() as session:
            await session.run(query, ids=entity_ids)

    async def bulk_delete_by_parent_id(self, parent_id: UUID) -> None:
        """Bulk delete entity nodes with a specific parent_id.

        Args:
            parent_id: The parent ID to match
        """
        query = "MATCH (e:Entity {parent_id: $parent_id}) DETACH DELETE e"
        async with self.driver.session() as session:
            await session.run(query, parent_id=str(parent_id))

    async def search_for_sync_id(self, text: str, sync_id: UUID, limit: int = 10) -> List[Dict]:
        """Search for entities by text within a specific sync.

        This basic implementation searches for node properties containing the search text.
        For more advanced semantic search, this could be extended to use embeddings.

        Args:
            text: The text to search for
            sync_id: The ID of the sync to search in
            limit: Maximum number of results to return

        Returns:
            List[Dict]: The search results
        """
        # Basic text search across properties
        query = """
        MATCH (e:Entity)
        WHERE e.sync_id = $sync_id
        AND (
            e.name CONTAINS $text OR
            e.content CONTAINS $text OR
            e.text CONTAINS $text OR
            e.description CONTAINS $text OR
            e.notes CONTAINS $text
        )
        RETURN e
        LIMIT $limit
        """

        async with self.driver.session() as session:
            result = await session.run(query, sync_id=str(sync_id), text=text, limit=limit)

            records = await result.data()
            return [record["e"] for record in records]

    async def create_node(self, node_properties: dict, label: str) -> None:
        """Create a node with the specified label and properties.

        Args:
            node_properties: The properties of the node
            label: The label of the node
        """
        # Sanitize the label - Neo4j labels must be alphanumeric
        safe_label = "".join(c for c in label if c.isalnum())

        query = f"CREATE (n:{safe_label} $props) RETURN n"
        async with self.driver.session() as session:
            await session.run(query, props=node_properties)

    async def create_relationship(
        self, from_node_id: str, to_node_id: str, rel_type: str, properties: dict = None
    ) -> None:
        """Create relationship between two nodes.

        Args:
            from_node_id: The ID of the source node
            to_node_id: The ID of the target node
            rel_type: The type of relationship
            properties: Optional properties for the relationship
        """
        # Sanitize relationship type name
        safe_rel_type = "".join(c for c in rel_type if c.isalnum() or c == "_")

        query = (
            f"MATCH (a:Entity), (b:Entity) "
            f"WHERE a.id = $from_id AND b.id = $to_id "
            f"CREATE (a)-[r:{safe_rel_type} $props]->(b) "
            f"RETURN r"
        )

        async with self.driver.session() as session:
            await session.run(query, from_id=from_node_id, to_id=to_node_id, props=properties or {})

    async def bulk_create_nodes(self, nodes: list[dict]) -> None:
        """Bulk create nodes.

        Args:
            nodes: List of node definitions with 'label' and 'properties'
        """
        if not nodes:
            return

        # Using UNWIND for efficient bulk creation
        query = """
        UNWIND $nodes as node
        CREATE (n)
        SET n = node.properties
        SET n:`Entity`
        SET n:`{label}`
        """

        # Need to execute for each label type since Neo4j doesn't support dynamic labels in bulk
        label_groups = {}
        for node in nodes:
            label = node.get("label", "Entity")
            if label not in label_groups:
                label_groups[label] = []
            label_groups[label].append(node)

        async with self.driver.session() as session:
            for label, group in label_groups.items():
                # Sanitize the label
                safe_label = "".join(c for c in label if c.isalnum())
                await session.run(query.replace("{label}", safe_label), nodes=group)

    async def bulk_create_relationships(self, relationships: list[dict]) -> None:
        """Bulk create relationships.

        Args:
            relationships: List of relationship definitions
        """
        if not relationships:
            return

        # Group by relationship type for efficient processing
        rel_type_groups = {}
        for rel in relationships:
            rel_type = rel.get("rel_type", "RELATED_TO")
            if rel_type not in rel_type_groups:
                rel_type_groups[rel_type] = []
            rel_type_groups[rel_type].append(rel)

        async with self.driver.session() as session:
            for rel_type, group in rel_type_groups.items():
                # Sanitize the relationship type
                safe_rel_type = "".join(c for c in rel_type if c.isalnum() or c == "_")

                query = f"""
                UNWIND $rels as rel
                MATCH (a:Entity), (b:Entity)
                WHERE a.id = rel.from_node_id AND b.id = rel.to_node_id
                CREATE (a)-[r:{safe_rel_type}]->(b)
                SET r = rel.properties
                """

                await session.run(query, rels=group)

    async def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            await self.driver.close()
