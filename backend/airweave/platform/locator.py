"""Resource locator for platform resources."""

import importlib
from typing import Type

from airweave import schemas
from airweave.platform.configs._base import BaseConfig
from airweave.platform.destinations._base import BaseDestination

PLATFORM_PATH = "airweave.platform"


class ResourceLocator:
    """Resource locator for destination lookups.

    All source-related lookups have been moved to the SourceRegistry.
    Destination lookups will move to a DestinationRegistry in a future change.
    """

    @staticmethod
    def get_auth_config(auth_config_class: str) -> Type[BaseConfig]:
        """Get the auth config class for a destination.

        Args:
            auth_config_class: Auth config class name

        Returns:
            The resolved auth config class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.configs.auth")
        return getattr(module, auth_config_class)  # type: ignore[no-any-return]

    @staticmethod
    def get_destination(destination: schemas.Destination) -> Type[BaseDestination]:
        """Get the destination class.

        Args:
            destination: Destination schema

        Returns:
            The resolved destination class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.destinations.{destination.short_name}")
        return getattr(module, destination.class_name)  # type: ignore[no-any-return]


resource_locator = ResourceLocator()
