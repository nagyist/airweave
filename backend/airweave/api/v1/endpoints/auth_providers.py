"""Auth Provider endpoints for managing auth provider connections."""

from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud, schemas
from airweave.api import deps
from airweave.api.router import TrailingSlashRouter
from airweave.core import credentials
from airweave.core.logging import logger
from airweave.core.shared_models import ConnectionStatus, IntegrationType
from airweave.db.unit_of_work import UnitOfWork
from airweave.platform.configs._base import Fields
from airweave.platform.locator import resource_locator
from airweave.schemas.auth import AuthContext

router = TrailingSlashRouter()


async def _validate_auth_fields(
    db: AsyncSession, auth_provider_short_name: str, auth_fields: Optional[Dict[str, Any]]
) -> dict:
    """Validate auth fields based on auth type.

    Args:
        db: The database session
        auth_provider_short_name: The short name of the auth provider
        auth_fields: The auth fields to validate

    Returns:
        The validated auth fields as a dict

    Raises:
        HTTPException: If auth fields are invalid or not supported
    """
    # Get the auth provider info
    auth_provider = await crud.auth_provider.get_by_short_name(
        db, short_name=auth_provider_short_name
    )
    if not auth_provider:
        raise HTTPException(
            status_code=404, detail=f"Auth provider '{auth_provider_short_name}' not found"
        )

    # Check if auth_config_class is defined for the auth provider
    if not auth_provider.auth_config_class:
        raise HTTPException(
            status_code=422,
            detail=f"Auth provider {auth_provider.name} does not have an auth config defined.",
        )

    if auth_fields is None:
        raise HTTPException(
            status_code=422, detail=f"Auth provider {auth_provider.name} requires auth fields."
        )

    # Convert ConfigValues to dict if needed
    if hasattr(auth_fields, "model_dump"):
        auth_fields_dict = auth_fields.model_dump()
    else:
        auth_fields_dict = auth_fields

    # Create and validate auth config
    try:
        auth_config_class = resource_locator.get_auth_config(auth_provider.auth_config_class)
        auth_config = auth_config_class(**auth_fields_dict)
        return auth_config.model_dump()
    except Exception as e:
        logger.error(f"Failed to validate auth fields: {e}")

        # Check if it's a Pydantic validation error and format it nicely
        from pydantic import ValidationError

        if isinstance(e, ValidationError):
            # Extract the field names and error messages
            error_messages = []
            for error in e.errors():
                field = ".".join(str(loc) for loc in error.get("loc", []))
                msg = error.get("msg", "")
                error_messages.append(f"Field '{field}': {msg}")

            error_detail = (
                f"Invalid configuration for {auth_provider.auth_config_class}:\n"
                + "\n".join(error_messages)
            )
            raise HTTPException(
                status_code=422, detail=f"Invalid auth fields: {error_detail}"
            ) from e
        else:
            # For other types of errors
            raise HTTPException(status_code=422, detail=f"Invalid auth fields: {str(e)}") from e


@router.get("/list", response_model=List[schemas.AuthProvider])
async def list_auth_providers(
    *,
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
    skip: int = 0,
    limit: int = 100,
) -> List[schemas.AuthProvider]:
    """Get all available auth providers.

    Args:
    -----
        db: The database session
        auth_context: The current authentication context
        skip: Number of auth providers to skip
        limit: Maximum number of auth providers to return

    Returns:
    --------
        List[schemas.AuthProvider]: List of available auth providers
    """
    auth_providers = await crud.auth_provider.get_multi(db, skip=skip, limit=limit)

    # Populate auth_fields for each auth provider
    result_providers = []
    for provider in auth_providers:
        try:
            provider_dict = {
                key: getattr(provider, key) for key in provider.__dict__ if not key.startswith("_")
            }

            # Skip if no auth config class
            if not provider.auth_config_class:
                logger.warning(f"Auth provider {provider.short_name} has no auth_config_class")
                result_providers.append(provider)
                continue

            # Get auth fields from auth config class
            auth_config_class = resource_locator.get_auth_config(provider.auth_config_class)
            auth_fields = Fields.from_config_class(auth_config_class)
            provider_dict["auth_fields"] = auth_fields

            # Get config fields from config class if it exists
            if provider.config_class:
                try:
                    config_class = resource_locator.get_config(provider.config_class)
                    config_fields = Fields.from_config_class(config_class)
                    provider_dict["config_fields"] = config_fields
                except Exception as e:
                    logger.error(f"Error getting config fields for {provider.short_name}: {str(e)}")
                    # Still include the provider without config_fields
                    provider_dict["config_fields"] = None
            else:
                provider_dict["config_fields"] = None

            provider_model = schemas.AuthProvider.model_validate(provider_dict)
            result_providers.append(provider_model)

        except Exception as e:
            logger.error(f"Error processing auth provider {provider.short_name}: {str(e)}")
            # Still include the provider without auth_fields or config_fields
            result_providers.append(provider)

    return result_providers


@router.get("/connections/", response_model=List[schemas.AuthProviderConnection])
async def list_auth_provider_connections(
    *,
    db: AsyncSession = Depends(deps.get_db),
    auth_context: AuthContext = Depends(deps.get_auth_context),
    skip: int = 0,
    limit: int = 100,
) -> List[schemas.AuthProviderConnection]:
    """Get all auth provider connections for the current organization.

    This endpoint returns all active auth provider connections that belong to the
    user's organization. While not enforced at the backend level, the frontend
    should typically ensure only one connection per auth provider type.

    Args:
    -----
        db: The database session
        auth_context: The current authentication context
        skip: Number of connections to skip
        limit: Maximum number of connections to return

    Returns:
    --------
        List[schemas.AuthProviderConnection]: List of auth provider connections
    """
    # Get all connections with integration_type = AUTH_PROVIDER for the current organization
    connections = await crud.connection.get_by_integration_type(
        db,
        integration_type=IntegrationType.AUTH_PROVIDER,
        auth_context=auth_context,
    )

    # Apply skip and limit manually since get_by_integration_type doesn't support them
    connections = connections[skip : skip + limit]

    # Convert to AuthProviderConnection schema
    return [
        schemas.AuthProviderConnection(
            id=connection.id,
            name=connection.name,
            readable_id=connection.readable_id,
            short_name=connection.short_name,
            description=connection.description,
            created_by_email=connection.created_by_email,
            modified_by_email=connection.modified_by_email,
            created_at=connection.created_at,
            modified_at=connection.modified_at,
        )
        for connection in connections
    ]


@router.get("/connections/{readable_id}", response_model=schemas.AuthProviderConnection)
async def get_auth_provider_connection(
    *,
    db: AsyncSession = Depends(deps.get_db),
    readable_id: str,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.AuthProviderConnection:
    """Get details of a specific auth provider connection.

    Args:
    -----
        db: The database session
        readable_id: The readable ID of the auth provider connection
        auth_context: The current authentication context

    Returns:
    --------
        schemas.AuthProviderConnection: The auth provider connection details
    """
    # Find the connection by readable_id
    connection = await crud.connection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )

    if not connection:
        raise HTTPException(
            status_code=404,
            detail=f"Auth provider connection not found: {readable_id}",
        )

    # Verify it's an auth provider connection
    if connection.integration_type != IntegrationType.AUTH_PROVIDER:
        raise HTTPException(
            status_code=400,
            detail=f"Connection {readable_id} is not an auth provider connection",
        )

    # Return as AuthProviderConnection schema
    return schemas.AuthProviderConnection(
        id=connection.id,
        name=connection.name,
        readable_id=connection.readable_id,
        short_name=connection.short_name,
        description=connection.description,
        created_by_email=connection.created_by_email,
        modified_by_email=connection.modified_by_email,
        created_at=connection.created_at,
        modified_at=connection.modified_at,
    )


@router.get("/detail/{short_name}", response_model=schemas.AuthProvider)
async def get_auth_provider(
    *,
    db: AsyncSession = Depends(deps.get_db),
    short_name: str,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.AuthProvider:
    """Get details of a specific auth provider.

    Args:
    -----
        db: The database session
        short_name: The short name of the auth provider
        auth_context: The current authentication context

    Returns:
    --------
        schemas.AuthProvider: The auth provider details
    """
    auth_provider = await crud.auth_provider.get_by_short_name(db, short_name=short_name)
    if not auth_provider:
        raise HTTPException(
            status_code=404,
            detail=f"Auth provider not found: {short_name}",
        )

    # Populate auth_fields if auth_config_class exists
    if auth_provider.auth_config_class:
        try:
            auth_config_class = resource_locator.get_auth_config(auth_provider.auth_config_class)
            auth_fields = Fields.from_config_class(auth_config_class)

            # Create provider dict with auth_fields
            provider_dict = {
                **{
                    key: getattr(auth_provider, key)
                    for key in auth_provider.__dict__
                    if not key.startswith("_")
                },
                "auth_fields": auth_fields,
            }

            # Add config_fields if config_class exists
            if auth_provider.config_class:
                try:
                    config_class = resource_locator.get_config(auth_provider.config_class)
                    config_fields = Fields.from_config_class(config_class)
                    provider_dict["config_fields"] = config_fields
                except Exception as e:
                    logger.error(f"Error getting config fields for {short_name}: {str(e)}")
                    provider_dict["config_fields"] = None
            else:
                provider_dict["config_fields"] = None

            return schemas.AuthProvider.model_validate(provider_dict)
        except Exception as e:
            logger.error(f"Failed to get auth config for {short_name}: {str(e)}")
            # Return without auth_fields if there's an error

    return auth_provider


@router.post("/", response_model=schemas.AuthProviderConnection)
async def connect_auth_provider(
    *,
    db: AsyncSession = Depends(deps.get_db),
    auth_provider_connection_in: schemas.AuthProviderConnectionCreate,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.AuthProviderConnection:
    """Create a new auth provider connection with credentials.

    Args:
    -----
        db: The database session
        auth_context: The current authentication context
        auth_provider_connection_in: The auth provider connection data

    Returns:
    --------
        schemas.AuthProviderConnection: The created connection
    """
    async with UnitOfWork(db) as uow:
        try:
            # 1. Validate auth provider exists
            auth_provider = await crud.auth_provider.get_by_short_name(
                uow.session, auth_provider_connection_in.short_name
            )
            if not auth_provider:
                raise HTTPException(
                    status_code=400,
                    detail=f"Auth provider not found: {auth_provider_connection_in.short_name}",
                )

            # 2. Validate auth fields
            validated_auth_fields = await _validate_auth_fields(
                uow.session,
                auth_provider_connection_in.short_name,
                auth_provider_connection_in.auth_fields,
            )

            # 3. Create integration credential with encrypted auth credentials
            integration_credential_data = schemas.IntegrationCredentialCreateEncrypted(
                name=f"{auth_provider_connection_in.name} Credentials",
                integration_short_name=auth_provider_connection_in.short_name,
                description=f"Credentials for {auth_provider_connection_in.name}",
                integration_type=IntegrationType.AUTH_PROVIDER,
                auth_type=auth_provider.auth_type,
                encrypted_credentials=credentials.encrypt(validated_auth_fields),
                auth_config_class=auth_provider.auth_config_class,
            )

            integration_credential = await crud.integration_credential.create(
                uow.session,
                obj_in=integration_credential_data,
                auth_context=auth_context,
                uow=uow,
            )
            await uow.session.flush()

            # 4. Create connection without config fields
            connection_data = schemas.ConnectionCreate(
                name=auth_provider_connection_in.name,
                readable_id=auth_provider_connection_in.readable_id,
                description=f"Auth provider connection for {auth_provider_connection_in.name}",
                integration_type=IntegrationType.AUTH_PROVIDER,
                status=ConnectionStatus.ACTIVE,
                integration_credential_id=integration_credential.id,
                short_name=auth_provider_connection_in.short_name,
            )

            connection = await crud.connection.create(
                uow.session,
                obj_in=connection_data,
                auth_context=auth_context,
                uow=uow,
            )
            await uow.session.flush()

            # 6. Return response
            return schemas.AuthProviderConnection(
                id=connection.id,
                name=connection.name,
                readable_id=connection.readable_id,
                short_name=connection.short_name,
                description=connection.description,
                created_by_email=connection.created_by_email,
                modified_by_email=connection.modified_by_email,
                created_at=connection.created_at,
                modified_at=connection.modified_at,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to create auth provider connection: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to create auth provider connection: {str(e)}"
            ) from e


@router.delete("/{readable_id}", response_model=schemas.AuthProviderConnection)
async def delete_auth_provider_connection(
    *,
    db: AsyncSession = Depends(deps.get_db),
    readable_id: str,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.AuthProviderConnection:
    """Delete an auth provider connection.

    This will cascade delete:
    - The associated integration credential
    - All source connections that were created using this auth provider
    - All connections and credentials associated with those source connections

    Args:
    -----
        db: The database session
        readable_id: The readable ID of the auth provider connection to delete
        auth_context: The current authentication context

    Returns:
    --------
        schemas.AuthProviderConnection: The deleted connection information
    """
    # Find the connection by readable_id and integration_type
    connection = await crud.connection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )

    if not connection:
        raise HTTPException(
            status_code=404,
            detail=f"Auth provider connection not found: {readable_id}",
        )

    # Verify it's an auth provider connection
    if connection.integration_type != IntegrationType.AUTH_PROVIDER:
        raise HTTPException(
            status_code=400,
            detail=f"Connection {readable_id} is not an auth provider connection",
        )

    # Create response before deletion
    response = schemas.AuthProviderConnection(
        id=connection.id,
        name=connection.name,
        readable_id=connection.readable_id,
        short_name=connection.short_name,
        description=connection.description,
        created_by_email=connection.created_by_email,
        modified_by_email=connection.modified_by_email,
        created_at=connection.created_at,
        modified_at=connection.modified_at,
    )

    # Delete the connection - this will cascade to:
    # 1. integration_credential (via before_delete event in Connection model)
    # 2. source_connections that use this auth provider (via foreign key CASCADE)
    # 3. connections and syncs of those source_connections (via before_delete event in SourceConn)
    await crud.connection.remove(db, id=connection.id, auth_context=auth_context)

    return response


async def _update_auth_credentials(
    uow: UnitOfWork, connection: Any, auth_fields: dict, auth_context: AuthContext
) -> None:
    """Update the encrypted credentials for a connection."""
    # Convert to dict if it's a Pydantic model
    if hasattr(auth_fields, "model_dump"):
        auth_fields_dict = auth_fields.model_dump()
    else:
        auth_fields_dict = auth_fields

    logger.info(f"[UPDATE AUTH CREDENTIALS] Auth fields to update: {list(auth_fields_dict.keys())}")

    validated_auth_fields = await _validate_auth_fields(
        uow.session, connection.short_name, auth_fields
    )
    logger.info("[UPDATE AUTH CREDENTIALS] Auth fields validated successfully")

    if not connection.integration_credential_id:
        raise HTTPException(status_code=500, detail="Connection missing integration credential")

    integration_credential = await crud.integration_credential.get(
        uow.session, id=connection.integration_credential_id, auth_context=auth_context
    )

    if not integration_credential:
        raise HTTPException(status_code=404, detail="Integration credential not found")

    encrypted_credentials = credentials.encrypt(validated_auth_fields)

    integration_credential_update = schemas.IntegrationCredentialUpdate(
        encrypted_credentials=encrypted_credentials
    )

    await crud.integration_credential.update(
        uow.session,
        db_obj=integration_credential,
        obj_in=integration_credential_update,
        auth_context=auth_context,
        uow=uow,
    )
    await uow.session.flush()


async def _update_connection_fields(
    uow: UnitOfWork,
    connection: Any,
    update_data: schemas.AuthProviderConnectionUpdate,
    auth_context: AuthContext,
    auth_fields_updated: bool,
) -> None:
    """Update connection fields and ensure timestamps are updated."""
    connection_update_data = {}
    if update_data.name is not None:
        connection_update_data["name"] = update_data.name
    if update_data.description is not None:
        connection_update_data["description"] = update_data.description

    # Update connection if we have fields to update
    if connection_update_data:
        connection_update = schemas.ConnectionUpdate(**connection_update_data)
        await crud.connection.update(
            uow.session,
            db_obj=connection,
            obj_in=connection_update,
            auth_context=auth_context,
            uow=uow,
        )

    # Ensure timestamps are updated when auth fields change
    if auth_fields_updated:
        from airweave.core.datetime_utils import utc_now_naive

        connection.modified_at = utc_now_naive()
        if auth_context.has_user_context:
            connection.modified_by_email = auth_context.tracking_email
        uow.session.add(connection)

    if connection_update_data or auth_fields_updated:
        await uow.session.flush()
        await uow.session.refresh(connection)


async def _validate_auth_provider_connection(
    db: AsyncSession, readable_id: str, auth_context: AuthContext
) -> Any:
    """Validate and return auth provider connection."""
    connection = await crud.connection.get_by_readable_id(
        db, readable_id=readable_id, auth_context=auth_context
    )

    if not connection:
        raise HTTPException(
            status_code=404,
            detail=f"Auth provider connection not found: {readable_id}",
        )

    if connection.integration_type != IntegrationType.AUTH_PROVIDER:
        raise HTTPException(
            status_code=400,
            detail=f"Connection {readable_id} is not an auth provider connection",
        )

    return connection


@router.put("/{readable_id}", response_model=schemas.AuthProviderConnection)
async def update_auth_provider_connection(
    *,
    db: AsyncSession = Depends(deps.get_db),
    readable_id: str,
    auth_provider_connection_update: schemas.AuthProviderConnectionUpdate,
    auth_context: AuthContext = Depends(deps.get_auth_context),
) -> schemas.AuthProviderConnection:
    """Update an existing auth provider connection.

    This endpoint allows updating:
    - The connection name and description
    - The authentication credentials (which will be re-validated and re-encrypted)

    Args:
    -----
        db: The database session
        readable_id: The readable ID of the auth provider connection to update
        auth_provider_connection_update: The fields to update
        auth_context: The current authentication context

    Returns:
    --------
        schemas.AuthProviderConnection: The updated connection information
    """
    async with UnitOfWork(db) as uow:
        try:
            # 1. Validate connection
            connection = await _validate_auth_provider_connection(
                uow.session, readable_id, auth_context
            )

            # 2. Update auth fields if provided
            auth_fields_updated = auth_provider_connection_update.auth_fields is not None
            if auth_fields_updated:
                await _update_auth_credentials(
                    uow, connection, auth_provider_connection_update.auth_fields, auth_context
                )

            # 3. Update connection fields
            await _update_connection_fields(
                uow, connection, auth_provider_connection_update, auth_context, auth_fields_updated
            )

            # 4. Return updated connection
            return schemas.AuthProviderConnection(
                id=connection.id,
                name=connection.name,
                readable_id=connection.readable_id,
                short_name=connection.short_name,
                description=connection.description,
                created_by_email=connection.created_by_email,
                modified_by_email=connection.modified_by_email,
                created_at=connection.created_at,
                modified_at=connection.modified_at,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to update auth provider connection: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to update auth provider connection: {str(e)}"
            ) from e
