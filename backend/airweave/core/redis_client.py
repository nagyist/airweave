"""Redis client for Airweave."""

import redis.asyncio as redis

from airweave.core.config import settings
from airweave.core.logging import logger


class RedisClient:
    """Redis client singleton for pubsub operations."""

    def __init__(self):
        """Initialize the Redis client."""
        self.client = self._create_client()

    def _create_client(self) -> redis.Redis:
        """Create and configure the Redis client.

        Returns:
            redis.Redis: Configured Redis client instance.
        """
        client_kwargs = {
            "host": settings.REDIS_HOST,
            "port": settings.REDIS_PORT,
            "decode_responses": True,
            "socket_keepalive": True,
            "socket_keepalive_options": {},
            "health_check_interval": 30,
            "max_connections": 50,
        }

        # Add password if configured
        if settings.REDIS_PASSWORD:
            client_kwargs["password"] = settings.REDIS_PASSWORD

        return redis.Redis(**client_kwargs)

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel.

        Args:
            channel: Channel name to publish to.
            message: Message to publish (should be JSON string).

        Returns:
            int: Number of subscribers that received the message.
        """
        try:
            return await self.client.publish(channel, message)
        except Exception as e:
            logger.warning(f"Redis publish failed for channel '{channel}': {e}")
            return 0

    async def subscribe(self, channel: str) -> redis.client.PubSub:
        """Subscribe to a channel.

        Args:
            channel: Channel name to subscribe to.

        Returns:
            redis.client.PubSub: PubSub instance for listening to messages.
        """
        pubsub = self.client.pubsub()
        await pubsub.subscribe(channel)
        return pubsub

    async def ping(self) -> bool:
        """Test Redis connection.

        Returns:
            bool: True if connected, False otherwise.
        """
        try:
            await self.client.ping()
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Close Redis connection gracefully."""
        await self.client.close()


# Global singleton instance
redis_client = RedisClient()
