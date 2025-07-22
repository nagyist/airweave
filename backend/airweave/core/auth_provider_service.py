"""Service for managing auth provider operations."""

from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from airweave import crud
from airweave.core.logging import logger
from airweave.platform.locator import resource_locator

auth_provider_logger = logger.with_prefix("Auth Provider Service: ").with_context(
    component="auth_provider_service"
)

# function to get most recent connection for an auth provider short name
# function to get credentials of most recent connection


class AuthProviderService:
    """Service for managing auth provider operations."""

    async def get_runtime_auth_fields_for_source(
        self, db: AsyncSession, source_short_name: str
    ) -> List[str]:
        """Get the runtime auth fields required from an auth provider for a source.

        This filters out BYOC (Bring Your Own Credentials) fields like client_id
        and client_secret, which are managed by the auth provider internally.

        Args:
            db: The database session
            source_short_name: The short name of the source

        Returns:
            List of auth field names that should be requested from auth providers

        Raises:
            HTTPException: If source not found or has no auth config
        """
        # Get the source model
        source_model = await crud.source.get_by_short_name(db, short_name=source_short_name)
        if not source_model:
            raise HTTPException(status_code=404, detail=f"Source '{source_short_name}' not found")

        if not source_model.auth_config_class:
            raise HTTPException(
                status_code=422,
                detail=f"Source '{source_short_name}' has no auth config class defined",
            )

        # Get the auth config class
        auth_config_class = resource_locator.get_auth_config(source_model.auth_config_class)

        # Get all fields from the auth config
        all_fields = list(auth_config_class.model_fields.keys())

        # Dynamically determine BYOC-specific fields based on class hierarchy
        from airweave.platform.configs.auth import OAuth2BYOCAuthConfig, OAuth2WithRefreshAuthConfig

        # Check if this is a BYOC auth config
        if issubclass(auth_config_class, OAuth2BYOCAuthConfig):
            # Get fields defined specifically in OAuth2BYOCAuthConfig (not inherited)
            # These are the BYOC-specific fields that auth providers manage internally
            byoc_specific_fields = set(OAuth2BYOCAuthConfig.model_fields.keys()) - set(
                OAuth2WithRefreshAuthConfig.model_fields.keys()
            )
            runtime_fields = [field for field in all_fields if field not in byoc_specific_fields]

            auth_provider_logger.debug(
                f"Source '{source_short_name}' is BYOC - "
                f"All fields: {all_fields}, Runtime fields: {runtime_fields}, "
                f"BYOC-specific fields filtered: {list(byoc_specific_fields)}"
            )
        else:
            # Not a BYOC source, return all fields
            runtime_fields = all_fields
            auth_provider_logger.debug(
                f"Source '{source_short_name}' is not BYOC - returning all fields: {all_fields}"
            )

        return runtime_fields

    async def validate_auth_provider_config(
        self,
        db: AsyncSession,
        auth_provider_short_name: str,
        auth_provider_config: Optional[Dict[str, Any]],
    ) -> dict:
        """Validate auth provider config fields based on auth provider config class.

        Args:
            db: The database session
            auth_provider_short_name: The short name of the auth provider
            auth_provider_config: The auth provider config fields to validate

        Returns:
            The validated auth provider config fields as a dict

        Raises:
            HTTPException: If config fields are invalid or required but not provided
        """
        # Get the auth provider info
        auth_provider = await crud.auth_provider.get_by_short_name(
            db, short_name=auth_provider_short_name
        )
        if not auth_provider:
            raise HTTPException(
                status_code=404, detail=f"Auth provider '{auth_provider_short_name}' not found"
            )

        BASE_ERROR_MESSAGE = (
            "For more information about auth provider configuration, "
            "check [this page](https://docs.airweave.ai/auth-providers)."
        )

        # Check if auth provider has a config class defined - it MUST be defined
        if not hasattr(auth_provider, "config_class") or auth_provider.config_class is None:
            raise HTTPException(
                status_code=422,
                detail=f"Auth provider {auth_provider.name} does not have a "
                "configuration class defined. " + BASE_ERROR_MESSAGE,
            )

        # Config class exists but no config fields provided - check if that's allowed
        if auth_provider_config is None:
            try:
                # Get config class to check if it has required fields
                config_class = resource_locator.get_config(auth_provider.config_class)
                # Create an empty instance to see if it accepts no fields
                config = config_class()
                return config.model_dump()
            except Exception:
                # If it fails with no fields, config is required
                raise HTTPException(
                    status_code=422,
                    detail=f"Auth provider {auth_provider.name} requires config fields "
                    "but none were provided. " + BASE_ERROR_MESSAGE,
                ) from None

        # Both config class and config fields exist, validate them
        try:
            config_class = resource_locator.get_config(auth_provider.config_class)
            config = config_class(**auth_provider_config)
            return config.model_dump()
        except Exception as e:
            auth_provider_logger.error(f"Failed to validate auth provider config fields: {e}")

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
                    f"Invalid configuration for {auth_provider.config_class}:\n"
                    + "\n".join(error_messages)
                )
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid auth provider config fields: {error_detail}. "
                    + BASE_ERROR_MESSAGE,
                ) from e
            else:
                # For other types of errors
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid auth provider config fields: {str(e)}. " + BASE_ERROR_MESSAGE,
                ) from e


# Singleton instance
auth_provider_service = AuthProviderService()
