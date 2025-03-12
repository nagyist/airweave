"""Decorator for Arq jobs."""

import traceback
from functools import wraps
from typing import Any, Callable


def arq_job(func: Callable) -> Callable:
    """Decorator to wrap a function with injected job context.

    Args:
        func (Callable): The function to wrap.

    Returns:
        Callable: The wrapped function.
    """

    @wraps(func)
    async def wrapper(ctx: dict[str, Any], *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Wrapper function for an Arq job.

        Args:
            ctx (dict[str, Any]): The context dictionary.
            args (Any): The arguments to pass to the function.
            kwargs (Any): The keyword arguments to pass to the function.

        Returns:
            Any: The result of the wrapped function.

        Raises:
            e (Exception): If there is an unhandled exception.
        """
        job_logger = ctx["logger"].with_context(
            context_base="arq_worker",
            job_id=ctx["job_id"],
            queue_job_id=kwargs.get("job_id") if kwargs.get("job_id") else None,
            score=ctx["score"],
            enqueue_time=ctx["enqueue_time"].isoformat(),
        )
        ctx["logger"] = job_logger
        try:
            ctx["logger"].info(f"Starting job {ctx['job_id']}")
            return await func(ctx, *args, **kwargs)
        except Exception as e:
            job_logger.error(
                f"Unhandled exception running job {ctx['job_id']}: {e}\n{traceback.format_exc()}"
            )
            raise e
        finally:
            job_logger.info(f"Finished job {ctx['job_id']}")

    return wrapper
