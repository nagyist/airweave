"""API endpoints for managing sync destinations."""

from typing import List
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud, schemas
from app.api import deps
from app.core.logging import logger
from app.db.session import get_db_context

router = APIRouter()


@router.get("/{sync_id}/destinations", response_model=List[schemas.SyncDestination])
async def get_sync_destinations(
    *,
    db: AsyncSession = Depends(deps.get_db),
    sync_id: UUID,
    user: schemas.User = Depends(deps.get_user),
) -> List[schemas.SyncDestination]:
    """Get all destinations for a sync.

    Args:
    -----
        db: The database session
        sync_id: The ID of the sync
        user: The current user

    Returns:
    --------
        List[schemas.SyncDestination]: The destinations
    """
    try:
        async with get_db_context() as db:
            # Verify user has access to the sync
            sync = await crud.sync.get(db=db, id=sync_id, current_user=user)
            if not sync:
                raise HTTPException(status_code=404, detail="Sync not found")

            destinations = await crud.sync_destination.get_by_sync_id(db=db, sync_id=sync_id)
            result = [schemas.SyncDestination.model_validate(d) for d in destinations]

        return result
    except Exception as e:
        logger.error(f"Error getting destinations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting destinations: {str(e)}") from e


@router.post("/{sync_id}/destinations", response_model=List[schemas.SyncDestination])
async def create_sync_destinations(
    *,
    db: AsyncSession = Depends(deps.get_db),
    sync_id: UUID,
    destinations_in: List[schemas.SyncDestinationCreate] = Body(...),
    user: schemas.User = Depends(deps.get_user),
) -> List[schemas.SyncDestination]:
    """Create destinations for a sync.

    Args:
    -----
        db: The database session
        sync_id: The ID of the sync
        destinations_in: The destinations to create
        user: The current user

    Returns:
    --------
        List[schemas.SyncDestination]: The created destinations
    """
    logger.info(f"Creating destinations for sync {sync_id}")
    logger.info(f"Received {len(destinations_in)} destinations")

    # Verify user has access to the sync
    sync = await crud.sync.get(db=db, id=sync_id, current_user=user)
    if not sync:
        raise HTTPException(status_code=404, detail="Sync not found")

    try:
        async with get_db_context() as db:
            # Ensure all destinations have the correct sync_id
            for dest in destinations_in:
                dest.sync_id = sync_id

            destinations = await crud.sync_destination.create_for_sync(
                db=db, sync_id=sync_id, destinations=destinations_in, current_user=user
            )

            # Convert to Pydantic models BEFORE the async context ends
            result = [schemas.SyncDestination.model_validate(d) for d in destinations]

        logger.info(f"Successfully created {len(destinations)} destinations")
        return result
    except Exception as e:
        logger.error(f"Error creating destinations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating destinations: {str(e)}") from e


@router.delete("/{sync_id}/destinations/{destination_id}", response_model=schemas.SyncDestination)
async def delete_sync_destination(
    *,
    db: AsyncSession = Depends(deps.get_db),
    sync_id: UUID,
    destination_id: UUID,
    user: schemas.User = Depends(deps.get_user),
) -> schemas.SyncDestination:
    """Delete a sync destination.

    Args:
    -----
        db: The database session
        sync_id: The ID of the sync
        destination_id: The ID of the destination to delete
        user: The current user

    Returns:
    --------
        schemas.SyncDestination: The deleted destination
    """
    try:
        async with get_db_context() as db:
            # Verify user has access to the sync
            sync = await crud.sync.get(db=db, id=sync_id, current_user=user)
            if not sync:
                raise HTTPException(status_code=404, detail="Sync not found")

            destination = await crud.sync_destination.get(db=db, id=destination_id)
            if not destination:
                raise HTTPException(status_code=404, detail="Destination not found")

            if destination.sync_id != sync_id:
                raise HTTPException(
                    status_code=400, detail="Destination does not belong to specified sync"
                )

            destination = await crud.sync_destination.remove(db=db, id=destination_id)
            result = schemas.SyncDestination.model_validate(destination)

        return result
    except Exception as e:
        logger.error(f"Error deleting destination: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting destination: {str(e)}") from e
