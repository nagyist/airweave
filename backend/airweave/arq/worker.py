"""Arq worker for polling and trigger runs."""

from typing import Any

from arq import cron

from airweave.arq.decorator import arq_job
from airweave.arq.redis import redis_settings
from airweave.core.logging import LoggerConfigurator
from airweave.db.session import get_db_context


async def on_startup(ctx: dict[str, Any]) -> None:
    """On startup function for the worker.

    Args:
        ctx (dict[str, Any]): The context dictionary.
    """
    ctx["db_session_maker"] = get_db_context
    ctx["logger"] = LoggerConfigurator.configure_logger(__name__)


@arq_job
async def process_sync_job(ctx: dict[str, Any], sync_job_id: str) -> None:
    """Process a sync job.

    Args:
        ctx (dict[str, Any]): The context dictionary.
        sync_job_id (str): The ID of the sync job to process.
    """
    pass


class ArqWorkerSettings:
    """Settings for the Arq worker.

    Attributes:
        functions (list[Callable]): The functions to register with the worker.
        EVERY_5_SECONDS_SET (set[int]): The set of seconds to run the cron job every 5 seconds.
        cron_jobs (list[cron]): The cron jobs to register with the worker.
        redis_settings (RedisSettings): The Redis settings.
        handle_signals (bool): Whether to handle signals.
        health_check_interval (int): The interval to check the health of the worker.
        worker_name (str): The name of the worker. For example, "airweave-worker".
        on_startup (Callable): The on startup function.
    """

    def __init__(self, worker_name: str):
        """Initialize the worker settings.

        Args:
            worker_name (str): The name of the worker.
        """
        self.worker_name = worker_name

        self.functions = [
            process_sync_job,
        ]

        self.EVERY_5_SECONDS_SET = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}
        self.cron_jobs = [
            cron(
                second=self.EVERY_5_SECONDS_SET,
            )
        ]

        # Other settings as instance variables
        self.redis_settings = redis_settings
        self.handle_signals = True
        self.health_check_interval = 10
        self.on_startup = on_startup
