"""The API module that contains the endpoints for search."""

import asyncio
import uuid
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app.api import deps

router = APIRouter()


class MetadataSearchFilter(BaseModel):
    """The filter for the search."""

    key: str
    value: str
    operator: str


class SearchResult(BaseModel):
    """The result of the search."""

    id: UUID
    title: str
    description: str


class SearchSummary(BaseModel):
    """The summary of the search."""

    summary: str
    found_entity_ids: list[UUID]
    search_metadata: dict


@router.get("/summary", response_model=SearchSummary)
async def search_summary(
    db: AsyncSession = Depends(deps.get_db),
    query: str = Query(..., description="Query to search for"),
    top_k: int = Query(default=10, description="Number of results to return"),
    search_method: str = Query(default="vector", description="Search method to use"),
    sync_id: Optional[UUID] = Query(default=None, description="Sync ID to filter by"),
    meta_data_filter: Optional[list[str]] = Query(default=None, description="Metadata filters"),
    user: schemas.User = Depends(deps.get_user),
) -> SearchSummary:
    """Search for summarized information."""
    await asyncio.sleep(1.5)

    two_entity_ids = [
        uuid.uuid4(),
        uuid.uuid4(),
    ]
    return SearchSummary(
        summary="Across multiple synchronizations, I can see two uncompleted tasks related to the Python programming language.\n The first is titled 'Refactor Pydantic models' and the second is titled 'Make use of structured outputs instead of free-form text'.\n Would you like me to give you a detailed explanation of the tasks?",
        found_entity_ids=two_entity_ids,
        search_metadata={
            "query": query,
            "top_k": top_k,
            "search_method": search_method,
            "metadata_filter": meta_data_filter,
            "sync_id": sync_id,
        },
    )


@router.get("/objects", response_model=list[SearchResult])
async def search(
    *,
    db: AsyncSession = Depends(deps.get_db),
    query: str = Query(..., description="Query to search for"),
    sync_id: Optional[UUID] = Query(default=None, description="Sync ID to filter by"),
    metadata_filter: Optional[list[str]] = Query(default=None, description="Metadata filters"),
    user: schemas.User = Depends(deps.get_user),
) -> list[SearchResult]:
    """Search for a specific item.

    Args:
    ----
        db (AsyncSession): The database session.
        query (str): The query to search for.
        sync_id (Optional[UUID]): The sync ID to search for.
        metadata_filter (Optional[list[MetadataSearchFilter]]): The filter to search for.
        user (schemas.User): The user to search for.

    Returns:
    -------
        list[SearchResult]: The search results.
    """
    return []
