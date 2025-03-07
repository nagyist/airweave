"""Resource locator for platform resources."""

import importlib
from typing import Callable, Type

from app import schemas
from app.platform.configs._base import BaseConfig
from app.platform.destinations._base import BaseDestination
from app.platform.embedding_models._base import BaseEmbeddingModel
from app.platform.entities._base import BaseEntity
from app.platform.sources._base import BaseSource

PLATFORM_PATH = "app.platform"


class ResourceLocator:
    """Resource locator for platform resources.

    Gets the following:
    - embedding models
    - destinations
    - sources
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

    @staticmethod
    def get_destination_by_short_name(short_name: str) -> Type[BaseDestination]:
        """Get the destination class by short name.

        Args:
            short_name (str): Short name of the destination

        Returns:
            Type[BaseDestination]: Destination class
        """
        try:
            module = importlib.import_module(f"{PLATFORM_PATH}.destinations.{short_name}")
            # Try to find a class that ends with "Destination"
            for attr_name in dir(module):
                if attr_name.endswith("Destination"):
                    return getattr(module, attr_name)
            return None
        except (ImportError, AttributeError):
            return None


resource_locator = ResourceLocator()
