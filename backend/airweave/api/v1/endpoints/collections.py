"""API endpoints for collections."""

from typing import List, Optional

from fastapi import BackgroundTasks, Depends, HTTPException, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api import deps
from airweave.api.examples import (
    create_collection_list_response,
    create_job_list_response,
    create_search_response,
)
from airweave.api.router import TrailingSlashRouter
from airweave.core.collection_service import collection_service
from airweave.core.logging import ContextualLogger
from airweave.core.source_connection_service import source_connection_service
from airweave.core.sync_service import sync_service
from airweave.core.temporal_service import temporal_service
from airweave.schemas.auth import AuthContext
from airweave.schemas.search import QueryExpansionStrategy, ResponseType, SearchRequest
from airweave.search.search_service import search_service

router = TrailingSlashRouter()


@router.get(
    "/",
    response_model=List[schemas.Collection],
    responses=create_collection_list_response(
        ["finance_data"],
        "Finance data collection",
    ),
)
async def list_collections(
    skip: int = Query(0, description="Number of collections to skip for pagination"),
    limit: int = Query(
        100, description="Maximum number of collections to return (1-1000)", le=1000, ge=1
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> List[schemas.Collection]:
    """List all collections that belong to your organization."""
    return await crud.collection.get_multi(
        db,
        auth_context=auth_context,
        skip=skip,
        limit=limit,
    )


@router.post("/", response_model=schemas.Collection)
async def create_collection(
    collection: schemas.CollectionCreate,
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.Collection:
    """Create a new collection.

    <br/><br/>
    The newly created collection is initially empty and does not contain any data
    until you explicitly add source connections to it.
    """
    return await collection_service.create(db, collection_in=collection, auth_context=auth_context)


@router.get("/{readable_id}", response_model=schemas.Collection)
async def get_collection(
    readable_id: str = Path(
        ...,
        description="The unique readable identifier of the collection (e.g., 'finance-data-ab123')",
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.Collection:
    """Retrieve a specific collection by its readable ID."""
    db_obj = await crud.collection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )
    if db_obj is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    return db_obj


@router.put("/{readable_id}", response_model=schemas.Collection)
async def update_collection(
    collection: schemas.CollectionUpdate,
    readable_id: str = Path(
        ..., description="The unique readable identifier of the collection to update"
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.Collection:
    """Update a collection's properties.

    <br/><br/>
    Modifies the display name of an existing collection.
    Note that the readable ID cannot be changed after creation to maintain stable
    API endpoints and preserve any existing integrations or bookmarks.
    """
    db_obj = await crud.collection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )
    if db_obj is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    return await crud.collection.update(
        db, db_obj=db_obj, obj_in=collection, auth_context=auth_context
    )


@router.delete("/{readable_id}", response_model=schemas.Collection)
async def delete_collection(
    readable_id: str = Path(
        ..., description="The unique readable identifier of the collection to delete"
    ),
    delete_data: bool = Query(
        False,
        description="Whether to also delete all associated data from destination systems",
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.Collection:
    """Delete a collection and optionally its associated data.

    <br/><br/>
    Permanently removes a collection from your organization. By default, this only
    deletes the collection metadata while preserving the actual data in the
    destination systems.<br/><br/>All source connections within this collection
    will also be deleted as part of the cleanup process.
    """
    # Find the collection
    db_obj = await crud.collection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )
    if db_obj is None:
        raise HTTPException(status_code=404, detail="Collection not found")

    # If delete_data is true, we need to delete data in destination systems
    # before deleting the collection (which will cascade delete source connections)
    if delete_data:
        # Note: This should be moved to a service method that can properly
        # handle the destination data deletion without requiring multiple queries
        pass

    # Delete the collection - CASCADE will handle all child objects
    return await crud.collection.remove(db, id=db_obj.id, auth_context=auth_context)


@router.get(
    "/{readable_id}/search",
    response_model=schemas.SearchResponse,
    responses=create_search_response("raw_results", "Raw search results with metadata"),
)
async def search_collection(
    readable_id: str = Path(
        ..., description="The unique readable identifier of the collection to search"
    ),
    query: str = Query(
        ...,
        description="The search query text to find relevant documents and data",
        examples=["customer payment issues", "Q4 revenue trends", "support tickets about billing"],
    ),
    response_type: ResponseType = Query(
        ResponseType.RAW,
        description=(
            "Format of the response: 'raw' returns search results, "
            "'completion' returns AI-generated answers"
        ),
        examples=["raw", "completion"],
    ),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip for pagination"),
    score_threshold: Optional[float] = Query(
        None, ge=0.0, le=1.0, description="Minimum similarity score threshold"
    ),
    expansion_strategy: QueryExpansionStrategy = Query(
        QueryExpansionStrategy.AUTO,
        description="Query expansion strategy (auto, llm, or no_expansion)",
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
    logger: ContextualLogger = Depends(deps.get_logger),
) -> schemas.SearchResponse:
    """Search across all data sources within the specified collection.

    This GET endpoint provides basic search functionality. For advanced filtering
    and options, use the POST /search endpoint.
    """
    logger.info(
        f"Searching collection {readable_id} with query: {query} "
        f"with response_type: {response_type}, limit: {limit}, offset: {offset}"
    )

    # Create a SearchRequest from the query parameters
    search_request = SearchRequest(
        query=query,
        response_type=response_type,
        limit=limit,
        offset=offset,
        score_threshold=score_threshold,
        expansion_strategy=expansion_strategy,
    )

    try:
        return await search_service.search_with_request(
            db,
            readable_id=readable_id,
            search_request=search_request,
            auth_context=auth_context,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Search error for collection {readable_id}: {str(e)}")

        # Check if it's a connection error
        error_message = str(e).lower()
        if (
            "connection" in error_message
            or "refused" in error_message
            or "timeout" in error_message
        ):
            raise HTTPException(
                status_code=503,
                detail="Vector database service is currently unavailable. Please try again later.",
            ) from e
        elif "not found" in error_message:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{readable_id}' not found or you don't have access to it.",
            ) from e
        else:
            # For other errors, return a generic message but with 500 status
            raise HTTPException(
                status_code=500, detail=f"An error occurred while searching: {str(e)}"
            ) from e


@router.post(
    "/{readable_id}/search",
    response_model=schemas.SearchResponse,
    responses=create_search_response("completion_response", "Search with AI-generated completion"),
)
async def search_collection_advanced(
    readable_id: str = Path(
        ..., description="The unique readable identifier of the collection to search"
    ),
    search_request: SearchRequest = ...,
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
    logger: ContextualLogger = Depends(deps.get_logger),
) -> schemas.SearchResponse:
    """Advanced search with comprehensive filtering and options.

    This endpoint supports:
    - Metadata filtering using Qdrant's native filter syntax
    - Pagination with offset and limit
    - Score threshold filtering
    - Query expansion strategies
    """
    logger.info(
        f"Advanced search in collection {readable_id} with query: {search_request.query} "
        f"and filter: {search_request.filter}"
    )

    try:
        return await search_service.search_with_request(
            db,
            readable_id=readable_id,
            search_request=search_request,
            auth_context=auth_context,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Advanced search error for collection {readable_id}: {str(e)}")

        # Check if it's a connection error
        error_message = str(e).lower()
        if (
            "connection" in error_message
            or "refused" in error_message
            or "timeout" in error_message
        ):
            raise HTTPException(
                status_code=503,
                detail="Vector database service is currently unavailable. Please try again later.",
            ) from e
        elif "not found" in error_message:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{readable_id}' not found or you don't have access to it.",
            ) from e
        elif "invalid filter" in error_message:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid filter format: {str(e)}",
            ) from e
        else:
            # For other errors, return a generic message but with 500 status
            raise HTTPException(
                status_code=500, detail=f"An error occurred while searching: {str(e)}"
            ) from e


@router.post(
    "/{readable_id}/refresh_all",
    response_model=list[schemas.SourceConnectionJob],
    responses=create_job_list_response(["completed"], "Multiple sync jobs triggered"),
)
async def refresh_all_source_connections(
    *,
    readable_id: str = Path(
        ..., description="The unique readable identifier of the collection to refresh"
    ),
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
    background_tasks: BackgroundTasks,
) -> list[schemas.SourceConnectionJob]:
    """Trigger data synchronization for all source connections in the collection.

    <br/><br/>The sync jobs run asynchronously in the background, so this endpoint
    returns immediately with job details that you can use to track progress. You can
    monitor the status of individual data synchronization using the source connection
    endpoints.
    """
    # Check if collection exists
    collection = await crud.collection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Convert to Pydantic model immediately
    collection_obj = schemas.Collection.model_validate(collection, from_attributes=True)

    # Get all source connections for this collection
    source_connections = await source_connection_service.get_source_connections_by_collection(
        db=db, collection=readable_id, auth_context=auth_context
    )

    if not source_connections:
        return []

    # Create a sync job for each source connection and run it in the background
    sync_jobs = []

    for sc in source_connections:
        # Create the sync job
        sync_job = await source_connection_service.run_source_connection(
            db=db, source_connection_id=sc.id, auth_context=auth_context
        )

        # Get necessary objects for running the sync
        sync = await crud.sync.get(
            db=db, id=sync_job.sync_id, auth_context=auth_context, with_connections=True
        )
        sync_dag = await sync_service.get_sync_dag(
            db=db, sync_id=sync_job.sync_id, auth_context=auth_context
        )

        # Get source connection with auth_fields for temporal processing
        source_connection = await source_connection_service.get_source_connection(
            db=db,
            source_connection_id=sc.id,
            show_auth_fields=True,  # Important: Need actual auth_fields for temporal
            auth_context=auth_context,
        )

        # Prepare objects for background task
        sync = schemas.Sync.model_validate(sync, from_attributes=True)
        sync_dag = schemas.SyncDag.model_validate(sync_dag, from_attributes=True)
        source_connection = schemas.SourceConnection.from_orm_with_collection_mapping(
            source_connection
        )

        # Add to jobs list
        sync_jobs.append(sync_job.to_source_connection_job(sc.id))

        # Start the sync job in the background or via Temporal
        if await temporal_service.is_temporal_enabled():
            # Use Temporal workflow
            await temporal_service.run_source_connection_workflow(
                sync=sync,
                sync_job=sync_job,
                sync_dag=sync_dag,
                collection=collection_obj,  # Use the already converted object
                source_connection=source_connection,
                auth_context=auth_context,
            )
        else:
            # Fall back to background tasks
            background_tasks.add_task(
                sync_service.run,
                sync,
                sync_job,
                sync_dag,
                collection_obj,  # Use the already converted object
                source_connection,
                auth_context,
            )

    return sync_jobs
