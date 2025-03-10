"""Neo4j destination implementation."""

import json
import os
from typing import Dict, List, Optional
from uuid import UUID

import asyncpg
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

    def _convert_uuids_to_strings(self, data):
        """Recursively convert UUID objects to strings and handle nested structures for Neo4j.

        Neo4j only supports primitive types and arrays of primitive types as node/relationship
        properties. This method handles:
        1. Converting UUID objects to strings
        2. Converting nested dictionaries/objects to JSON strings

        Args:
            data: The data structure to convert (dict, list, or scalar value)

        Returns:
            The data structure with all UUIDs converted to strings and nested structures
            flattened to be compatible with Neo4j
        """
        if isinstance(data, (UUID, asyncpg.pgproto.pgproto.UUID)):
            return str(data)
        elif isinstance(data, dict):
            # Convert all dictionary values
            converted_dict = {}
            for k, v in data.items():
                # Check if the value is a nested dictionary or a list that might contain dictionaries
                if isinstance(v, dict):
                    # Serialize nested dictionaries to JSON string
                    converted_dict[k] = json.dumps(self._convert_uuids_to_strings(v))
                elif isinstance(v, (list, tuple)) and any(isinstance(item, dict) for item in v):
                    # If the list contains dictionaries, serialize the whole list
                    converted_dict[k] = json.dumps(self._convert_uuids_to_strings(v))
                else:
                    # Regular recursive conversion
                    converted_dict[k] = self._convert_uuids_to_strings(v)
            return converted_dict
        elif isinstance(data, list):
            return [self._convert_uuids_to_strings(item) for item in data]
        elif isinstance(data, tuple):
            return tuple(self._convert_uuids_to_strings(item) for item in data)
        else:
            return data

    def _ensure_neo4j_compatible(self, data):
        """Ensure all values are compatible with Neo4j property types.

        Neo4j properties can only be:
        - Null
        - String
        - Boolean
        - Integer
        - Float
        - Array of primitive types

        This method validates and filters out incompatible types.

        Args:
            data: Dictionary of properties

        Returns:
            Dictionary with only Neo4j compatible property values
        """
        if not isinstance(data, dict):
            return data

        compatible_props = {}
        for key, value in data.items():
            # Skip None values (Neo4j can store nulls)
            if value is None:
                compatible_props[key] = value
                continue

            # Check primitive types
            if isinstance(value, (str, bool, int, float)):
                compatible_props[key] = value
            # Handle arrays of primitives
            elif isinstance(value, (list, tuple)):
                # Skip arrays containing complex types
                if all(isinstance(item, (str, bool, int, float, type(None))) for item in value):
                    compatible_props[key] = list(value)
            # Skip other types
            else:
                # Log warning for skipped properties
                logger.debug(f"Skipping property {key} with incompatible type {type(value)}")

        return compatible_props

    async def insert(self, entity: ChunkEntity) -> None:
        """Insert a single entity as a node in Neo4j.

        Args:
            entity: The entity to insert
        """
        try:
            # Convert entity to dict
            entity_data = entity.model_dump()

            # Convert all UUIDs to strings and handle nested structures
            entity_data = self._convert_uuids_to_strings(entity_data)

            # Ensure all properties are Neo4j compatible
            entity_data = self._ensure_neo4j_compatible(entity_data)

            # Add sync_id and entity type
            entity_data["sync_id"] = str(self.sync_id)
            entity_data["entity_type"] = entity.__class__.__name__

            # Ensure entity_id is set
            if "entity_id" not in entity_data and hasattr(entity, "entity_id"):
                entity_data["entity_id"] = entity.entity_id

            # Generate label based on entity class name
            label = entity.__class__.__name__

            # Create the node
            await self.create_node(entity_data, label)
        except Exception as e:
            logger.error(
                f"Error inserting entity {getattr(entity, 'entity_id', 'unknown')}: {str(e)}"
            )
            raise

    async def bulk_insert(self, entities: list[ChunkEntity]) -> None:
        """Bulk insert entities as nodes in Neo4j.

        Args:
            entities: The entities to insert
        """
        if not entities:
            return

        try:
            # Prepare nodes for bulk creation
            nodes = []
            for entity in entities:
                entity_data = entity.model_dump()

                # Convert all UUIDs to strings and handle nested structures
                entity_data = self._convert_uuids_to_strings(entity_data)

                # Ensure all properties are Neo4j compatible
                entity_data = self._ensure_neo4j_compatible(entity_data)

                entity_data["sync_id"] = str(self.sync_id)
                entity_data["entity_type"] = entity.__class__.__name__

                # Ensure entity_id is set
                if "entity_id" not in entity_data and hasattr(entity, "entity_id"):
                    entity_data["entity_id"] = entity.entity_id

                nodes.append({"label": entity.__class__.__name__, "properties": entity_data})

            # Bulk create nodes
            await self.bulk_create_nodes(nodes)
        except Exception as e:
            logger.error(f"Error in bulk_insert: {str(e)}")
            raise

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
            sync_id: The sync ID to restrict search to
            limit: Maximum number of results to return

        Returns:
            List of matching entity dictionaries
        """
        # Convert sync_id to string to ensure compatibility with Neo4j
        sync_id_str = str(sync_id)

        query = """
        MATCH (e:Entity)
        WHERE e.sync_id = $sync_id
        AND (
            e.name CONTAINS $text
            OR e.content CONTAINS $text
            OR e.title CONTAINS $text
            OR e.description CONTAINS $text
        )
        RETURN e
        LIMIT $limit
        """

        results = []
        async with self.driver.session() as session:
            result = await session.run(query, sync_id=sync_id_str, text=text, limit=limit)
            async for record in result:
                node = record.get("e")
                if node:
                    results.append(dict(node.items()))

        return results

    async def create_node(self, node_properties: dict, label: str) -> None:
        """Create a single node in Neo4j.

        Args:
            node_properties: The properties for the node
            label: The label for the node
        """
        try:
            # Convert UUIDs to strings for Neo4j compatibility
            node_properties = self._convert_uuids_to_strings(node_properties)

            # Ensure properties are compatible with Neo4j
            node_properties = self._ensure_neo4j_compatible(node_properties)

            # Sanitize the label
            safe_label = "".join(c for c in label if c.isalnum())

            query = f"""
            CREATE (n:Entity:{safe_label})
            SET n = $properties
            """

            async with self.driver.session() as session:
                await session.run(query, properties=node_properties)
        except Exception as e:
            logger.error(f"Error creating Neo4j node: {str(e)}")
            # For debugging, show the problematic properties
            if hasattr(
                e, "message"
            ) and "Property values can only be of primitive types" in getattr(e, "message", ""):
                for key, value in node_properties.items():
                    if not isinstance(value, (str, bool, int, float, type(None))) and not (
                        isinstance(value, (list, tuple))
                        and all(
                            isinstance(item, (str, bool, int, float, type(None))) for item in value
                        )
                    ):
                        logger.error(
                            f"Problematic property: {key} = {type(value)} ({value if str(value)[:100] else None})"
                        )
            raise

    async def create_relationship(
        self, from_id: str, to_id: str, rel_type: str, properties: dict = None
    ) -> None:
        """Create a relationship between two nodes.

        Args:
            from_id: ID of the source node (db_entity_id)
            to_id: ID of the target node (entity_id)
            rel_type: Type of relationship
            properties: Optional properties for the relationship
        """
        try:
            if properties is None:
                properties = {}

            # Convert any UUIDs in properties to strings and handle nested structures
            properties = self._convert_uuids_to_strings(properties)

            # Ensure properties are compatible with Neo4j
            properties = self._ensure_neo4j_compatible(properties)

            # Sanitize relationship type
            safe_rel_type = "".join(c for c in rel_type if c.isalnum() or c == "_")

            query = f"""
            MATCH (a:Entity {{db_entity_id: $from_id}}), (b:Entity {{entity_id: $to_id}})
            CREATE (a)-[r:{safe_rel_type}]->(b)
            SET r = $properties
            """

            async with self.driver.session() as session:
                await session.run(query, from_id=from_id, to_id=to_id, properties=properties)
        except Exception as e:
            logger.error(f"Error creating relationship: {str(e)}")
            raise

    async def bulk_create_relationships(self, relationships: list[dict]) -> None:
        """Bulk create relationships between nodes.

        Args:
            relationships: List of relationship definitions with 'from_id', 'to_id', 'type', and 'properties'
                Each relationship should have:
                - from_id: ID of the source node (db_entity_id)
                - to_id: ID of the target node (db_entity_id)
                - type: Type of relationship
                - properties: Optional properties dictionary
        """
        if not relationships:
            return

        try:
            # Convert any UUIDs in relationship properties to strings and ensure Neo4j compatibility
            for rel in relationships:
                if "properties" in rel:
                    rel["properties"] = self._convert_uuids_to_strings(rel["properties"])
                    rel["properties"] = self._ensure_neo4j_compatible(rel["properties"])

            # Group by relationship type for efficient batching
            rel_type_groups = {}
            for rel in relationships:
                rel_type = rel.get("type", "RELATED_TO")
                if rel_type not in rel_type_groups:
                    rel_type_groups[rel_type] = []
                rel_type_groups[rel_type].append(rel)

            errors = []
            # Create relationships by type
            for rel_type, group in rel_type_groups.items():
                # Sanitize relationship type
                safe_rel_type = "".join(c for c in rel_type if c.isalnum() or c == "_")

                query = f"""
                UNWIND $rels as rel
                MATCH 
                  (a:Entity {{db_entity_id: rel.from_id}}), 
                  (b:Entity {{entity_id: rel.to_id}})
                CREATE (a)-[r:{safe_rel_type}]->(b)
                SET r = rel.properties
                """

                try:
                    async with self.driver.session() as session:
                        await session.run(query, rels=group)
                except Exception as e:
                    logger.error(
                        f"Error in bulk create relationships for type {rel_type}: {str(e)}"
                    )
                    errors.append((rel_type, str(e)))

                    # If batch fails, try creating relationships individually
                    logger.info(
                        f"Attempting individual relationship creation for failed batch of {len(group)} relationships"
                    )
                    for rel in group:
                        try:
                            await self.create_relationship(
                                rel.get("from_id"),
                                rel.get("to_id"),
                                rel_type,
                                rel.get("properties", {}),
                            )
                        except Exception as inner_e:
                            logger.error(
                                f"Failed to create individual relationship: {str(inner_e)}"
                            )

            if errors:
                logger.warning(f"Completed with {len(errors)} relationship type errors")

        except Exception as e:
            logger.error(f"Error in bulk_create_relationships: {str(e)}")
            raise

    async def bulk_create_nodes(self, nodes: list[dict]) -> None:
        """Bulk create nodes.

        Args:
            nodes: List of node definitions with 'label' and 'properties'
        """
        if not nodes:
            return

        try:
            # Convert any UUIDs in node properties to strings
            for node in nodes:
                if "properties" in node:
                    node["properties"] = self._convert_uuids_to_strings(node["properties"])
                    node["properties"] = self._ensure_neo4j_compatible(node["properties"])

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
                    try:
                        await session.run(query.replace("{label}", safe_label), nodes=group)
                    except Exception as e:
                        logger.error(f"Error in bulk create for label {label}: {str(e)}")
                        # If one batch fails, try inserting nodes individually
                        logger.info(
                            f"Attempting individual node insertion for failed batch of {len(group)} nodes"
                        )
                        for node in group:
                            try:
                                await self.create_node(node["properties"], label)
                            except Exception as inner_e:
                                logger.error(f"Failed to insert individual node: {str(inner_e)}")
        except Exception as e:
            logger.error(f"Error in bulk_create_nodes: {str(e)}")
            raise

    async def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            await self.driver.close()
