"""Runner for ARQ worker and health server."""

import asyncio

from arq.worker import create_worker

from airweave.arq.health_server import HealthServer
from airweave.arq.redis import redis_settings
from airweave.arq.worker import ArqWorkerSettings
from airweave.core.config import settings
from airweave.core.logging import logger as global_logger


async def main() -> None:
    """Main function to run the ARQ worker and health server.

    Raises:
        e (Exception): If there is an error building the worker or health server.
    """
    logger = global_logger.with_context(context_base="arq_worker", operation="runner")

    logger.info(
        "Starting ARQ worker and health server for container %s",
        settings.WORKER_NAME,
    )
    # Build Worker
    try:
        worker_settings = ArqWorkerSettings(settings.WORKER_NAME)
        worker = create_worker(worker_settings)

    except Exception as e:
        logger.error(f"Error building worker: {e}")
        raise e

    # Build Health Server
    try:
        srv = HealthServer(
            redis_settings=redis_settings,
            health_check_key=worker_settings.worker_name,
        )
    except Exception as e:
        logger.error(f"Error building health server: {e}")
        raise e

    # Run them together in a single event loop, this allows us to run the worker and health server
    # in the same container.
    await asyncio.gather(
        worker.async_run(),  # The ARQ worker
        srv.start(host="0.0.0.0", port=8000),  # The health server
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutdown requested... exiting.")
