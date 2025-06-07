"""Worker pool implementation for controlling async concurrency."""

import asyncio
import threading
from typing import Any, Callable

from airweave.core.logging import logger


class AsyncWorkerPool:
    """Manages a pool of workers with controlled concurrency.

    This class limits how many async tasks can run at once using a semaphore,
    preventing system overload when processing many items in parallel.
    """

    def __init__(self, max_workers: int = 100):
        """Initialize worker pool with concurrency control.

        Args:
            max_workers: Maximum number of tasks allowed to run concurrently
        """
        self.semaphore = asyncio.Semaphore(max_workers)
        self.pending_tasks = set()
        self.max_workers = max_workers

    async def submit(self, coro: Callable, *args, **kwargs) -> asyncio.Task:
        """Submit a coroutine to be executed by the worker pool.

        Creates a task, adds it to our tracking set, and returns it.
        Tasks run with controlled concurrency through the semaphore.
        """
        task_id = f"task_{len(self.pending_tasks) + 1}"

        logger.info(
            f"🔄 WORKER_SUBMIT [{task_id}] Submitting task to worker pool "
            f"(pending: {len(self.pending_tasks)}/{self.max_workers})"
        )

        task = asyncio.create_task(self._run_with_semaphore(coro, task_id, *args, **kwargs))
        task.task_id = task_id  # Store task ID for logging
        self.pending_tasks.add(task)
        task.add_done_callback(self._handle_task_completion)
        return task

    async def _run_with_semaphore(self, coro: Callable, task_id: str, *args, **kwargs) -> Any:
        """Run a coroutine with semaphore control.

        Acquires a semaphore before running the coroutine, limiting concurrency.
        Semaphore is automatically released when coroutine completes.
        """
        thread_id = threading.get_ident()

        logger.info(
            f"⏳ WORKER_WAIT [{task_id}] Waiting for semaphore "
            f"(thread: {thread_id}, available: {self.semaphore._value})"
        )

        async with self.semaphore:
            logger.info(
                f"🚀 WORKER_START [{task_id}] Acquired semaphore, starting execution "
                f"(thread: {thread_id})"
            )

            start_time = asyncio.get_event_loop().time()
            try:
                result = await coro(*args, **kwargs)
                elapsed = asyncio.get_event_loop().time() - start_time

                logger.info(
                    f"✅ WORKER_COMPLETE [{task_id}] Task completed successfully "
                    f"in {elapsed:.2f}s (thread: {thread_id})"
                )
                return result

            except Exception as e:
                elapsed = asyncio.get_event_loop().time() - start_time
                logger.error(
                    f"❌ WORKER_ERROR [{task_id}] Task failed after {elapsed:.2f}s "
                    f"(thread: {thread_id}): {type(e).__name__}: {str(e)}"
                )
                raise

    def _handle_task_completion(self, task: asyncio.Task) -> None:
        """Handle task completion and clean up."""
        task_id = getattr(task, "task_id", "unknown")
        self.pending_tasks.discard(task)

        if task.cancelled():
            logger.warning(f"🚫 WORKER_CANCELLED [{task_id}] Task was cancelled")
        elif task.exception() is not None:
            logger.error(
                f"💥 WORKER_EXCEPTION [{task_id}] Task completed with exception: {task.exception()}"
            )
        else:
            logger.info(f"🏁 WORKER_CLEANUP [{task_id}] Task cleaned up successfully")

    async def wait_for_batch(self, timeout: float = 0.5) -> None:
        """Wait for some tasks to complete, processing them as they finish."""
        if not self.pending_tasks:
            return

        logger.info(
            f"⏸️  WORKER_BATCH_WAIT Waiting for batch completion "
            f"(pending: {len(self.pending_tasks)}, timeout: {timeout}s)"
        )

        done, _ = await asyncio.wait(
            self.pending_tasks, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
        )

        logger.info(
            f"📦 WORKER_BATCH_DONE {len(done)} tasks completed in batch "
            f"(remaining: {len(self.pending_tasks)})"
        )

    async def wait_for_completion(self) -> None:
        """Wait for all tasks to complete."""
        if self.pending_tasks:
            logger.info(
                f"⏳ WORKER_FINAL_WAIT Waiting for all {len(self.pending_tasks)} tasks to complete"
            )
            await asyncio.wait(self.pending_tasks, return_when=asyncio.ALL_COMPLETED)
            logger.info("🎯 WORKER_ALL_COMPLETE All tasks have completed")
