"""Resource locator for platform resources."""

import importlib
from typing import Callable, Type

from airweave import schemas
from airweave.platform.auth_providers._base import BaseAuthProvider
from airweave.platform.configs._base import BaseConfig
from airweave.platform.destinations._base import BaseDestination
from airweave.platform.embedding_models._base import BaseEmbeddingModel
from airweave.platform.entities._base import BaseEntity
from airweave.platform.sources._base import BaseSource

PLATFORM_PATH = "airweave.platform"


class ResourceLocator:
    """Resource locator for platform resources.

    Gets the following:
    - embedding models
    - destinations
    - sources
    - auth providers
    - configs
    - transformers
    """

    @staticmethod
    def get_embedding_model(model: schemas.EmbeddingModel) -> Type[BaseEmbeddingModel]:
        """Get the embedding model class.

        Args:
            model (schemas.EmbeddingModel): Embedding model schema

        Returns:
            Type[BaseEmbeddingModel]: Instantiated embedding model
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.embedding_models.{model.short_name}")
        return getattr(module, model.class_name)

    @staticmethod
    def get_source(source: schemas.Source) -> Type[BaseSource]:
        """Get the source class.

        Args:
            source (schemas.Source): Source schema

        Returns:
            Type[BaseSource]: Source class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.sources.{source.short_name}")
        return getattr(module, source.class_name)

    @staticmethod
    def get_destination(destination: schemas.Destination) -> Type[BaseDestination]:
        """Get the destination class.

        Args:
            destination (schemas.Destination): Destination schema

        Returns:
            Type[BaseDestination]: Destination class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.destinations.{destination.short_name}")
        return getattr(module, destination.class_name)

    @staticmethod
    def get_auth_provider(auth_provider: schemas.AuthProvider) -> Type[BaseAuthProvider]:
        """Get the auth provider class.

        Args:
            auth_provider (schemas.AuthProvider): Auth provider schema

        Returns:
            Type[BaseAuthProvider]: Auth provider class
        """
        module = importlib.import_module(
            f"{PLATFORM_PATH}.auth_providers.{auth_provider.short_name}"
        )
        return getattr(module, auth_provider.class_name)

    @staticmethod
    def get_auth_config(auth_config_class: str) -> Type[BaseConfig]:
        """Get the auth config class.

        Args:
            auth_config_class (str): Auth config class name

        Returns:
            Type[BaseConfig]: Auth config class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.configs.auth")
        auth_config_class = getattr(module, auth_config_class)
        return auth_config_class

    @staticmethod
    def get_config(config_class: str) -> Type[BaseConfig]:
        """Get the config class.

        Args:
            config_class (str): Config class name

        Returns:
            Type[BaseConfig]: Config class
        """
        module = importlib.import_module(f"{PLATFORM_PATH}.configs.config")
        config_class = getattr(module, config_class)
        return config_class

    @staticmethod
    def get_transformer(transformer: schemas.Transformer) -> Callable:
        """Get the transformer function.

        Args:
            transformer (schemas.Transformer): Transformer schema

        Returns:
            Callable: Transformer function
        """
        module = importlib.import_module(transformer.module_name)
        return getattr(module, transformer.method_name)

    @staticmethod
    def get_entity_definition(entity_definition: schemas.EntityDefinition) -> Type[BaseEntity]:
        """Get the entity definition class.

        Args:
            entity_definition (schemas.EntityDefinition): Entity definition schema

        Returns:
            Type[BaseEntity]: Entity definition class
        """
        module = importlib.import_module(
            f"{PLATFORM_PATH}.entities.{entity_definition.module_name}"
        )
        return getattr(module, entity_definition.class_name)


resource_locator = ResourceLocator()
