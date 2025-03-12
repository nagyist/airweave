"""Health server for ARQ workers."""

import traceback
from typing import Optional

from aiohttp import web
from arq.worker import RedisSettings, async_check_health

from airweave.core.logging import logger


class HealthServer:
    """Async-based health server that checks ARQ worker status.

    Call async_check_health(...) with a specific health_check_key.
    """

    def __init__(
        self,
        redis_settings: RedisSettings,
        health_check_key: str,
    ):
        """Initialize the health server.

        Args:
            redis_settings: The Redis settings to use for the health check.
            health_check_key: The key to use for the health check, unique for each worker.
        """
        self.redis_settings = redis_settings
        self.health_check_key = health_check_key
        self.app = web.Application()
        self.app.add_routes([web.get("/", self.health_handler)])
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.logger = logger.with_context(
            context_base="arq_worker",
            operation="health_server",
            health_check_key=health_check_key,
        )

    async def health_handler(self, request: web.Request) -> web.Response:
        """A simple GET handler that calls 'async_check_health' on each request.

        Args:
            request: The request to handle.

        Returns:
            A response with a status of 200 if the worker is healthy, 500 otherwise.
        """
        try:
            is_healthy = await self.is_healthy_async()
            if is_healthy:
                self.logger.info("Worker is healthy")
                return web.Response(text="OK\n", status=200)
            self.logger.error("Worker is unhealthy")
            return web.Response(text="Unhealthy\n", status=500)
        except Exception as e:
            self.logger.error(f"Error checking health: {e}\n{traceback.format_exc()}")
            return web.Response(text="Error\n", status=500)

    async def is_healthy_async(self) -> bool:
        """This calls arq.worker.async_check_health(...).

        Returns:
            True for healthy, False otherwise.
        """
        result = await async_check_health(
            redis_settings=self.redis_settings,
            health_check_key=self.health_check_key,
        )
        return result == 0

    async def start(self, host: str = "0.0.0.0", port: int = 8000) -> None:
        """Start the AIOHTTP server. We can run this in a background task next to the worker.

        Args:
            host: The host to listen on.
            port: The port to listen on.
        """
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=host, port=port)
        await self.site.start()
        self.logger.info(f"Health server listening on http://{host}:{port}/")

    async def stop(self) -> None:
        """Stops the server gracefully."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
