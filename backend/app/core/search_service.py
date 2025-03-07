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

            # Get destinations from connections
            for connection_id in sync.destination_connections:
                try:
                    connection = await crud.connection.get(db, connection_id, current_user)
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
                    logger.error(f"Error initializing destination {connection_id}: {str(e)}")

            # If no destinations found from connections, use defaults
            if not vector_destinations and not graph_destinations:
                # Default to Weaviate for vector search
                vector_destinations.append(
                    await WeaviateDestination.create(
                        sync_id=sync_id,
                        embedding_model=LocalText2Vec(),
                    )
                )

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
                    raise ValueError("No vector destinations configured for this sync")

                # Use the first vector destination
                results = await vector_destinations[0].search_for_sync_id(
                    query_text=query,
                    sync_id=sync_id,
                    limit=limit,
                )
                return results

            elif search_type == SearchType.GRAPH:
                if not graph_destinations:
                    raise ValueError("No graph destinations configured for this sync")

                # Use the first graph destination
                results = await graph_destinations[0].search_for_sync_id(
                    query_text=query,
                    sync_id=sync_id,
                    limit=limit,
                )
                return results

            elif search_type == SearchType.HYBRID:
                # Perform both searches and combine results
                results = {}

                if vector_destinations:
                    try:
                        vector_results = await vector_destinations[0].search_for_sync_id(
                            query_text=query,
                            sync_id=sync_id,
                            limit=limit,
                        )
                        results["vector"] = vector_results
                    except Exception as e:
                        logger.error(f"Vector search error: {str(e)}")
                        results["vector"] = []

                if graph_destinations:
                    try:
                        graph_results = await graph_destinations[0].search_for_sync_id(
                            query_text=query,
                            sync_id=sync_id,
                            limit=limit,
                        )
                        results["graph"] = graph_results
                    except Exception as e:
                        logger.error(f"Graph search error: {str(e)}")
                        results["graph"] = []

                return results

            else:
                raise ValueError(f"Invalid search type: {search_type}")

        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise


# Create singleton instance
search_service = SearchService()
