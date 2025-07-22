"""Decorators for the platform integrations."""

from functools import wraps
from typing import Callable, List, Optional, TypeVar

from airweave.platform.auth.schemas import AuthType
from airweave.platform.entities._base import ChunkEntity


def source(
    name: str,
    short_name: str,
    auth_type: AuthType,
    auth_config_class: str,
    config_class: Optional[str] = None,
    labels: Optional[List[str]] = None,
) -> Callable[[type], type]:
    """Class decorator to mark a class as representing an Airweave source.

    Args:
    ----
        name (str): The name of the source.
        short_name (str): The short name of the source.
        auth_type (AuthType): The authentication type of the source.
        auth_config_class (str): The authentication config class of the source.
        config_class (Optional[str]): The configuration class for the source.
        labels (Optional[List[str]]): Labels categorizing this source (e.g., "CRM", "Database").

    Returns:
    -------
        Callable[[type], type]: The decorated class.

    """

    def decorator(cls: type) -> type:
        cls._is_source = True
        cls._name = name
        cls._short_name = short_name
        cls._auth_type = auth_type
        cls._auth_config_class = auth_config_class
        cls._config_class = config_class
        cls._labels = labels or []
        return cls

    return decorator


def destination(
    name: str,
    short_name: str,
    auth_type: Optional[AuthType] = None,
    auth_config_class: Optional[str] = None,
    labels: Optional[List[str]] = None,
) -> Callable[[type], type]:
    """Class decorator to mark a class as representing an Airweave destination.

    Args:
    ----
        name (str): The name of the destination.
        short_name (str): The short name of the destination.
        auth_type (AuthType): The authentication type of the destination.
        auth_config_class (str): The authentication config class of the destination.
        labels (Optional[List[str]]): Labels categorizing this destination (e.g., "Vector", "Graph")

    Returns:
    -------
        Callable[[type], type]: The decorated class.

    """

    def decorator(cls: type) -> type:
        cls._is_destination = True
        cls._name = name
        cls._short_name = short_name
        cls._auth_type = auth_type
        cls._auth_config_class = auth_config_class
        cls._labels = labels or []
        return cls

    return decorator


def embedding_model(
    name: str,
    short_name: str,
    provider: str,
    auth_type: Optional[AuthType] = None,
    auth_config_class: Optional[str] = None,
    model_name: Optional[str] = None,
    model_version: Optional[str] = None,
) -> Callable[[type], type]:
    """Class decorator to mark a class as representing an Airweave embedding model.

    Args:
    ----
        name (str): The name of the embedding model.
        short_name (str): The short name of the embedding model.
        provider (str): The provider of the embedding model.
        auth_type (AuthType): The authentication type of the embedding model.
        auth_config_class (Optional[str]): The authentication config class of the embedding model.
        model_name (Optional[str]): The name of the embedding model.
        model_version (Optional[str]): The version of the embedding model.

    Returns:
    -------
        Callable[[type], type]: The decorated class.

    """

    def decorator(cls: type) -> type:
        cls._is_embedding_model = True
        cls._name = name
        cls._short_name = short_name
        cls._auth_type = auth_type
        cls._auth_config_class = auth_config_class
        cls._model_name = model_name if model_name else cls._name
        cls._model_version = model_version if model_version else "1.0"
        cls._provider = provider
        return cls

    return decorator


def auth_provider(
    name: str,
    short_name: str,
    auth_type: AuthType,
    auth_config_class: str,
    config_class: str,
) -> Callable[[type], type]:
    """Class decorator to mark a class as representing an Airweave auth provider.

    Args:
    ----
        name (str): The name of the auth provider.
        short_name (str): The short name of the auth provider.
        auth_type (AuthType): The authentication type of the auth provider.
        auth_config_class (str): The authentication config class of the auth provider.
        config_class (str): The configuration class for the auth provider.

    Returns:
    -------
        Callable[[type], type]: The decorated class.

    """

    def decorator(cls: type) -> type:
        cls._is_auth_provider = True
        cls._name = name
        cls._short_name = short_name
        cls._auth_type = auth_type
        cls._auth_config_class = auth_config_class
        cls._config_class = config_class
        return cls

    return decorator


T = TypeVar("T", bound=ChunkEntity)
U = TypeVar("U", bound=ChunkEntity)


def transformer(
    name: str,
    auth_type: Optional[AuthType] = None,
    auth_config_class: Optional[str] = None,
    config_schema: Optional[dict] = None,
) -> Callable[[Callable], Callable]:
    """Method decorator to mark a function as an Airweave transformer.

    Args:
        name (str): The name of the transformer.
        short_name (str): The short name of the transformer.
        auth_type (AuthType, optional): The authentication type.
        auth_config_class (str, optional): The auth config class.
        config_schema (dict, optional): Configuration schema for the transformer.

    Returns:
        Callable: The decorated function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        # Add transformer metadata
        wrapper._is_transformer = True
        wrapper._name = name
        wrapper._auth_type = auth_type
        wrapper._auth_config_class = auth_config_class
        wrapper._config_schema = config_schema or {}
        wrapper.__doc__ = func.__doc__
        wrapper._method_name = func.__name__

        return wrapper

    return decorator
