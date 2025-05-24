"""The API module that contains the endpoints for connections."""

from typing import Optional
from uuid import UUID

from fastapi import Body, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import schemas
from airweave.api import deps
from airweave.api.router import TrailingSlashRouter
from airweave.core.connection_service import connection_service
from airweave.models.integration_credential import IntegrationType

router = TrailingSlashRouter()


@router.get("/detail/{connection_id}", response_model=schemas.Connection)
async def get_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(deps.get_db),
    user: schemas.User = Depends(deps.get_user),
) -> schemas.Connection:
    """Get a specific connection.

    Args:
    -----
        connection_id: The ID of the connection to get.
        db: The database session.
        user: The current user.

    Returns:
    -------
        schemas.Connection: The connection.
    """
    return await connection_service.get_connection(db, connection_id, user)


@router.get(
    "/list",
    response_model=list[schemas.Connection],
)
async def list_all_connected_integrations(
    db: AsyncSession = Depends(deps.get_db),
    user: schemas.User = Depends(deps.get_user),
) -> list[schemas.Connection]:
    """Get all active connections for the current user across all integration types.

    Args:
    -----
        db: The database session.
        user: The current user.

    Returns:
    -------
        list[schemas.Connection]: The list of connections.
    """
    return await connection_service.get_all_connections(db, user)


@router.get(
    "/list/{integration_type}",
    response_model=list[schemas.Connection],
)
async def list_connected_integrations(
    integration_type: IntegrationType,
    db: AsyncSession = Depends(deps.get_db),
    user: schemas.User = Depends(deps.get_user),
) -> list[schemas.Connection]:
    """Get all integrations of specified type connected to the current user.

    Args:
    -----
        integration_type (IntegrationType): The type of integration to get connections for.
        db (AsyncSession): The database session.
        user (schemas.User): The current user.

    Returns:
    -------
        list[schemas.Connection]: The list of connections.
    """
    return await connection_service.get_connections_by_type(db, integration_type, user)


# @router.post(
#     "/connect/{integration_type}/{short_name}",
#     response_model=schemas.Connection,
# )
# async def connect_integration(
#     *,
#     db: AsyncSession = Depends(deps.get_db),
#     integration_type: IntegrationType,
#     short_name: str,
#     name: Optional[str] = Body(default=None),
#     auth_fields: dict = Body(..., exclude={"name"}),
#     user: schemas.User = Depends(deps.get_user),
# ) -> schemas.Connection:
#     """Connect to a source, destination, or embedding model.

#     Expects a POST body with:
#     ```json
#     {
#         "name": "required connection name",
#         ... other config fields specific to the integration type ...
#     }
#     ```

#     Args:
#     -----
#         db: The database session.
#         integration_type: The type of integration to connect to.
#         short_name: The short name of the integration to connect to.
#         name: The name of the connection.
#         auth_fields: The config fields for the integration.
#         user: The current user.

#     Returns:
#     -------
#         schemas.Connection: The connection.
#     """
#     return await connection_service.connect_with_config(
#         db, integration_type, short_name, name, auth_fields, user
#     )


@router.get("/credentials/{connection_id}", response_model=dict)
async def get_connection_credentials(
    connection_id: UUID,
    db: AsyncSession = Depends(deps.get_db),
    user: schemas.User = Depends(deps.get_user),
) -> dict:
    """Get the credentials for a connection.

    Args:
    -----
        connection_id (UUID): The ID of the connection to get credentials for
        db (AsyncSession): The database session
        user (schemas.User): The current user

    Returns:
    -------
        decrypted_credentials (dict): The credentials for the connection
    """
    return await connection_service.get_connection_credentials(db, connection_id, user)


@router.delete("/delete/source/{connection_id}", response_model=schemas.Connection)
async def delete_connection(
    *,
    db: AsyncSession = Depends(deps.get_db),
    connection_id: UUID,
    user: schemas.User = Depends(deps.get_user),
) -> schemas.Connection:
    """Delete a connection.

    Deletes the connection and integration credential.

    Args:
    -----
        db (AsyncSession): The database session
        connection_id (UUID): The ID of the connection to delete
        user (schemas.User): The current user

    Returns:
    --------
        connection (schemas.Connection): The deleted connection
    """
    return await connection_service.delete_connection(db, connection_id, user)


@router.put("/disconnect/source/{connection_id}", response_model=schemas.Connection)
async def disconnect_source_connection(
    *,
    db: AsyncSession = Depends(deps.get_db),
    connection_id: UUID,
    user: schemas.User = Depends(deps.get_user),
) -> schemas.Connection:
    """Disconnect from a source connection.

    Args:
    -----
        db (AsyncSession): The database session
        connection_id (UUID): The ID of the connection to disconnect
        user (schemas.User): The current user

    Returns:
    --------
        connection (schemas.Connection): The disconnected connection
    """
    connection = await connection_service.disconnect_source(db, connection_id, user)
    # Ensure we return something that is compatible with the response_model
    return connection


# @router.get("/oauth2/source/auth_url")
# async def get_oauth2_auth_url(
#     *,
#     short_name: str,
#     auth_fields: Optional[str] = None,
# ) -> str:
#     """Get the OAuth2 authorization URL for a source.

#     Args:
#     -----
#         short_name: The short name of the source
#         auth_fields: Optional JSON string containing authentication fields
#     """
#     parsed_auth_fields = None
#     if auth_fields:
#         try:
#             parsed_auth_fields = json.loads(auth_fields)
#         except json.JSONDecodeError as err:
#             raise HTTPException(
#                 status_code=400, detail="Invalid auth_fields format. Must be valid JSON."
#             ) from err

#     return await connection_service.get_oauth2_auth_url(short_name, parsed_auth_fields)


# @router.post("/oauth2/source/code", response_model=schemas.Connection)
# async def send_oauth2_code(
#     *,
#     db: AsyncSession = Depends(deps.get_db),
#     short_name: str = Body(...),
#     code: str = Body(...),
#     user: schemas.User = Depends(deps.get_user),
#     connection_name: Optional[str] = Body(default=None),
#     auth_fields: Optional[dict] = Body(default=None),
# ) -> schemas.Connection:
#     """Send the OAuth2 authorization code for a source.

#     This will:
#     1. Get the OAuth2 settings for the source
#     2. Exchange the authorization code for a token
#     3. Create an integration credential with the token

#     Args:
#     -----
#         db: The database session
#         short_name: The short name of the source
#         code: The authorization code
#         user: The current user
#         connection_name: Optional custom name for the connection
#         auth_fields: Optional additional authentication fields for the connection

#     Returns:
#     --------
#         connection (schemas.Connection): The created connection
#     """
#     return await connection_service.connect_with_oauth2_code(
#         db, short_name, code, user, connection_name, auth_fields
#     )


# @router.post("/create-source-connection-from-oauth", response_model=schemas.SourceConnection)
# async def create_source_connection_from_oauth(
#     *,
#     db: AsyncSession = Depends(deps.get_db),
#     connection_id: UUID = Body(...),
#     source_connection_in: schemas.SourceConnectionCreate = Body(...),
#     user: schemas.User = Depends(deps.get_user),
#     background_tasks: BackgroundTasks,
# ) -> schemas.SourceConnection:
#     """Create a source connection from an existing OAuth connection.

#     This endpoint is specifically for the OAuth flow where the connection is created first,
#     and then a source connection needs to be created with that connection.

#     Args:
#     -----
#         db: The database session
#         connection_id: The ID of the existing connection
#         source_connection_in: The source connection to create
#         user: The current user
#         background_tasks: Background tasks for async operations

#     Returns:
#     --------
#         SourceConnection: The created source connection
#     """
#     # Verify the connection exists and get its details
#     connection = await connection_service.get_connection(db, connection_id, user)
#     if not connection:
#         raise HTTPException(status_code=404, detail="Connection not found")

#     # Ensure the short_name from the connection is used
#     if connection.short_name != source_connection_in.short_name:
#         raise HTTPException(
#             status_code=400,
#             detail=(
#                 f"Short name mismatch: connection has '{connection.short_name}' "
#                 f"but request has '{source_connection_in.short_name}'"
#             ),
#         )

#     (
#         source_connection,
#         sync_job,
#     ) = await source_connection_service.create_source_connection_from_oauth(
#         db=db,
#         source_connection_in=source_connection_in,
#         connection_id=connection.id,
#         current_user=user,
#     )

#     # If job was created and sync_immediately is True, start it in background
#     if sync_job and source_connection_in.sync_immediately:
#         async with get_db_context() as sync_db:
#             sync_dag = await sync_service.get_sync_dag(
#                 db=sync_db, sync_id=source_connection.sync_id, current_user=user
#             )

#             # Get the sync object
#             sync = await crud.sync.get(
#                 db=sync_db,
#                 id=source_connection.sync_id,
#                 current_user=user
#             )
#             sync = schemas.Sync.model_validate(sync, from_attributes=True)
#             sync_job = schemas.SyncJob.model_validate(sync_job, from_attributes=True)
#             sync_dag = schemas.SyncDag.model_validate(sync_dag, from_attributes=True)
#             collection = await crud.collection.get_by_readable_id(
#                 db=sync_db, readable_id=source_connection.collection, current_user=user
#             )
#             collection = schemas.Collection.model_validate(collection, from_attributes=True)

#         background_tasks.add_task(
#             sync_service.run, sync, sync_job, sync_dag, collection, source_connection, user
#         )

#     return source_connection


@router.post(
    "/direct-token/slack",
    response_model=schemas.Connection,
)
async def connect_slack_with_token(
    *,
    db: AsyncSession = Depends(deps.get_db),
    token: str = Body(...),
    name: Optional[str] = Body(None),
    user: schemas.User = Depends(deps.get_user),
) -> schemas.Connection:
    """Connect to Slack using a direct API token (for local development only).

    Args:
    -----
        db: The database session.
        token: The Slack API token.
        name: The name of the connection.
        user: The current user.

    Returns:
    -------
        schemas.Connection: The connection.
    """
    return await connection_service.connect_with_direct_token(
        db, "slack", token, name, user, validate_token=True
    )


@router.post(
    "/credentials/{integration_type}/{short_name}", response_model=schemas.IntegrationCredentialInDB
)
async def create_integration_credential(
    *,
    db: AsyncSession = Depends(deps.get_db),
    integration_type: IntegrationType,
    short_name: str,
    credential_in: schemas.IntegrationCredentialRawCreate = Body(...),
    user: schemas.User = Depends(deps.get_user),
) -> schemas.IntegrationCredentialInDB:
    """Create integration credentials with validation.

    1. Takes auth_fields and validates them against the auth_config_class
    2. Encrypts and stores them in integration_credentials
    3. Returns the integration credential with ID

    Args:
        db: The database session
        integration_type: Type of integration (SOURCE, DESTINATION, etc.)
        short_name: Short name of the integration
        credential_in: The credential data with auth_fields
        user: The current user

    Returns:
        The created integration credential

    Raises:
        HTTPException: If validation fails
    """
    return await connection_service.create_integration_credential(
        db, integration_type, short_name, credential_in, user
    )
