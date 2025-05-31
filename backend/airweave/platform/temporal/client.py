"""Temporal client configuration and utilities."""

from typing import Optional

from temporalio.client import Client

from airweave.core.config import settings
from airweave.core.logging import logger


class TemporalClient:
    """Temporal client wrapper."""

    _client: Optional[Client] = None

    @classmethod
    async def get_client(cls) -> Client:
        """Get or create the Temporal client."""
        if cls._client is None:
            logger.info(
                f"Connecting to Temporal at {settings.temporal_address}, "
                f"namespace: {settings.TEMPORAL_NAMESPACE}"
            )

            cls._client = await Client.connect(
                target_host=settings.temporal_address,
                namespace=settings.TEMPORAL_NAMESPACE,
            )

        return cls._client

    @classmethod
    async def close(cls) -> None:
        """Close the Temporal client."""
        if cls._client is not None:
            await cls._client.close()
            cls._client = None


# Global instance
temporal_client = TemporalClient()
