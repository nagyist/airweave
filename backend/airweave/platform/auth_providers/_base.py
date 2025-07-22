"""Base auth provider class."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from airweave.core.logging import logger


class BaseAuthProvider(ABC):
    """Base class for all auth providers."""

    def __init__(self):
        """Initialize the base auth provider."""
        self._logger: Optional[Any] = None  # Store contextual logger as instance variable

    @property
    def logger(self):
        """Get the logger for this auth provider, falling back to default if not set."""
        if self._logger is not None:
            return self._logger
        # Fall back to default logger
        return logger

    def set_logger(self, logger) -> None:
        """Set a contextual logger for this auth provider."""
        self._logger = logger

    @classmethod
    @abstractmethod
    async def create(
        cls, credentials: Optional[Any] = None, config: Optional[Dict[str, Any]] = None
    ) -> "BaseAuthProvider":
        """Create a new auth provider instance.

        Args:
            credentials: Optional credentials for authenticated auth providers.
            config: Optional configuration parameters

        Returns:
            A configured auth provider instance
        """
        pass

    # TODO something like: get_creds_for_source
    @abstractmethod
    async def get_creds_for_source(
        self, source_short_name: str, source_auth_config_fields: List[str]
    ) -> Dict[str, Any]:
        """Get credentials for a source.

        Args:
            source_short_name: The short name of the source to get credentials for
            source_auth_config_fields: The fields required for the source auth config
        """
        pass
