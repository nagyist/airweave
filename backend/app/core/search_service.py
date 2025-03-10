"""Search service for vector and graph database integrations."""

import logging
from typing import Dict, List, Union
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.core.exceptions import NotFoundException
from app.core.search_type import SearchType
from app.platform.destinations._base import GraphDBDestination, VectorDBDestination
from app.platform.destinations.neo4j import Neo4jDestination
from app.platform.destinations.weaviate import WeaviateDestination
from app.platform.embedding_models.local_text2vec import LocalText2Vec
from app.platform.locator import resource_locator

logger = logging.getLogger(__name__)


class SearchService:
    """Service for handling database searches."""

    async def search(
        self,
        db: AsyncSession,
        query: str,
        sync_id: UUID,
        current_user: schemas.User,
        search_type: SearchType = SearchType.VECTOR,
        limit: int = 10,
    ) -> Union[List[Dict], Dict[str, List[Dict]]]:
        """Search across databases using existing connections.

        Args:
            db (AsyncSession): Database session
            query (str): Search query text
            sync_id (UUID): ID of the sync to search within
            current_user (schemas.User): Current user performing the search
            search_type (SearchType): Type of search to perform (vector, graph, or hybrid)
            limit (int): Maximum number of results to return

        Returns:
            Union[List[Dict], Dict[str, List[Dict]]]: Search results, either as a list or
                as a dictionary mapping destination types to results

        Raises:
            NotFoundException: If sync or connections not found
            ValueError: If invalid search type is specified
        """
        try:
            # Get sync configuration
            sync = await crud.sync.get(db, id=sync_id, current_user=current_user)
            if not sync:
                raise NotFoundException("Sync not found")

            # Map to store destinations by type
            vector_destinations = []
            graph_destinations = []

            # Get destinations from the sync_destination relationship
            if not sync.destinations:
                logger.warning(f"No destinations found for sync {sync_id}")
                return []

            # Process each destination
            for sync_destination in sync.destinations:
                try:
                    # Handle native destinations
                    if sync_destination.is_native:
                        if sync_destination.destination_type == "weaviate_native":
                            # Add native Weaviate destination for vector search
                            vector_destinations.append(
                                {"type": "weaviate", "sync_id": sync.id, "is_native": True}
                            )
                        elif sync_destination.destination_type == "neo4j_native":
                            # Add native Neo4j destination for graph search
                            graph_destinations.append(
                                {"type": "neo4j", "sync_id": sync.id, "is_native": True}
                            )
                    # Handle connection-based destinations
                    elif sync_destination.connection_id:
                        connection = await crud.connection.get(
                            db, sync_destination.connection_id, current_user
                        )
                        if not connection:
                            continue

                        destination_model = await crud.destination.get_by_short_name(
                            db, connection.short_name
                        )
                        if not destination_model:
                            continue

                        # Get the destination class
                        destination_class = resource_locator.get_destination(destination_model)
                        if not destination_class:
                            continue

                        # Create destination instance
                        destination = await destination_class.create(
                            sync_id=sync_id,
                            embedding_model=LocalText2Vec(),  # Default model
                        )

                        # Categorize by destination type
                        if isinstance(destination, VectorDBDestination):
                            vector_destinations.append(destination)
                        elif isinstance(destination, GraphDBDestination):
                            graph_destinations.append(destination)

                except Exception as e:
                    logger.error(
                        f"Error initializing destination {sync_destination.connection_id}: {str(e)}"
                    )

            # If no destinations found from connections, use defaults
            if not vector_destinations and not graph_destinations:
                # Default to Weaviate for vector search
                try:
                    vector_destinations.append(
                        await WeaviateDestination.create(
                            sync_id=sync_id,
                            embedding_model=LocalText2Vec(),
                        )
                    )
                except Exception as e:
                    logger.warning(f"Could not initialize default Weaviate destination: {str(e)}")

                # Try to initialize Neo4j if environment variables are set
                try:
                    graph_destinations.append(
                        await Neo4jDestination.create(
                            sync_id=sync_id,
                            embedding_model=LocalText2Vec(),
                        )
                    )
                except Exception as e:
                    logger.warning(f"Could not initialize Neo4j destination: {str(e)}")

            # Perform search based on the requested type
            if search_type == SearchType.VECTOR:
                if not vector_destinations:
                    logger.warning("No vector destinations available for search")
                    return []

                # Search across all vector destinations
                all_results = []
                for dest in vector_destinations:
                    # Handle both destination objects and native destination info dicts
                    if isinstance(dest, dict) and dest.get("is_native"):
                        # Create a native destination instance
                        if dest["type"] == "weaviate":
                            dest_instance = await WeaviateDestination.create(
                                sync_id=sync_id,
                                embedding_model=LocalText2Vec(),
                            )
                            results = await dest_instance.search_for_sync_id(
                                query_text=query,
                                sync_id=sync_id,
                            )
                            all_results.extend(results)
                    else:
                        # Use the destination instance directly
                        results = await dest.search_for_sync_id(
                            query_text=query,
                            sync_id=sync_id,
                        )
                        all_results.extend(results)

                return all_results

            elif search_type == SearchType.GRAPH:
                if not graph_destinations:
                    logger.warning("No graph destinations available for search")
                    return []

                # Search across all graph destinations
                all_results = []
                for dest in graph_destinations:
                    # Handle both destination objects and native destination info dicts
                    if isinstance(dest, dict) and dest.get("is_native"):
                        # Create a native destination instance
                        if dest["type"] == "neo4j":
                            try:
                                dest_instance = await Neo4jDestination.create(
                                    sync_id=sync_id,
                                    embedding_model=LocalText2Vec(),
                                )
                                results = await dest_instance.search_for_sync_id(
                                    query_text=query,
                                    sync_id=sync_id,
                                )
                                all_results.extend(results)
                            except Exception as e:
                                logger.error(f"Error searching Neo4j: {str(e)}")
                    else:
                        # Use the destination instance directly
                        results = await dest.search_for_sync_id(
                            query_text=query,
                            sync_id=sync_id,
                        )
                        all_results.extend(results)

                return all_results

            elif search_type == SearchType.HYBRID:
                # For hybrid search, we return results from both vector and graph search
                results = {"vector": [], "graph": []}

                # Vector search
                if vector_destinations:
                    all_vector_results = []
                    for dest in vector_destinations:
                        try:
                            # Handle both destination objects and native destination info dicts
                            if isinstance(dest, dict) and dest.get("is_native"):
                                # Create a native destination instance
                                if dest["type"] == "weaviate":
                                    dest_instance = await WeaviateDestination.create(
                                        sync_id=sync_id,
                                        embedding_model=LocalText2Vec(),
                                    )
                                    vector_results = await dest_instance.search_for_sync_id(
                                        query_text=query,
                                        sync_id=sync_id,
                                    )
                                    all_vector_results.extend(vector_results)
                            else:
                                # Use the destination instance directly
                                vector_results = await dest.search_for_sync_id(
                                    query_text=query,
                                    sync_id=sync_id,
                                )
                                all_vector_results.extend(vector_results)
                        except Exception as e:
                            logger.error(f"Vector search error: {str(e)}")

                    results["vector"] = all_vector_results

                # Graph search
                if graph_destinations:
                    all_graph_results = []
                    for dest in graph_destinations:
                        try:
                            # Handle both destination objects and native destination info dicts
                            if isinstance(dest, dict) and dest.get("is_native"):
                                # Create a native destination instance
                                if dest["type"] == "neo4j":
                                    dest_instance = await Neo4jDestination.create(
                                        sync_id=sync_id,
                                        embedding_model=LocalText2Vec(),
                                    )
                                    graph_results = await dest_instance.search_for_sync_id(
                                        query_text=query,
                                        sync_id=sync_id,
                                    )
                                    all_graph_results.extend(graph_results)
                            else:
                                # Use the destination instance directly
                                graph_results = await dest.search_for_sync_id(
                                    query_text=query,
                                    sync_id=sync_id,
                                )
                                all_graph_results.extend(graph_results)
                        except Exception as e:
                            logger.error(f"Graph search error: {str(e)}")

                    results["graph"] = all_graph_results

                return results

            else:
                raise ValueError(f"Invalid search type: {search_type}")

        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise


# Create singleton instance
search_service = SearchService()
