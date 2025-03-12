"""Setup for Redis."""

from contextlib import asynccontextmanager

import certifi
from arq.connections import RedisSettings, create_pool

from airweave.core.config import settings


def construct_redis_settings() -> RedisSettings:
    """Construct the Redis settings.

    Returns:
        RedisSettings: The Redis settings.
    """
    return RedisSettings(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        conn_timeout=30,
        conn_retries=5,
        conn_retry_delay=2,
        ssl=True,
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        ssl_ca_certs=certifi.where() if settings.LOCAL_DEVELOPMENT else None,
    )


redis_settings = construct_redis_settings()


@asynccontextmanager
async def redis_connection():
    """Context manager for Redis connection.

    Example:
    ```python
    async with redis_connection() as pool:
        # Use the Redis pool here
        await pool.enqueue_job("my_job", "arg1", "arg2")
        await pool.enqueue_job("my_job", "arg1", "arg2")

    # The pool is closed automatically when the context is exited
    ```
    """
    redis = await create_pool(redis_settings)
    yield redis
    await redis.close()
