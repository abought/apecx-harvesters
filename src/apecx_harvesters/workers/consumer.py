"""Worker pool consumer: processes TaskSpec items from a queue."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncIterable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from apecx_harvesters.workers.producer import TaskSpec, _SENTINEL, produce


@dataclass
class TaskResult:
    """Outcome of a single task execution."""
    task: TaskSpec
    result: Any = None
    error: Exception | None = None

    @property
    def ok(self) -> bool:
        """True when the task completed without an exception."""
        return self.error is None


class _RateLimiter:
    """
    Enforces a minimum inter-call interval for a single worker.

    Each worker holds its own instance so limits are independent across workers.
    """

    def __init__(self, rate: float) -> None:
        self._interval = 1.0 / rate
        self._last: float = 0.0

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        wait = self._interval - (loop.time() - self._last)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last = asyncio.get_running_loop().time()


async def _run_worker(
    fn: Callable,
    queue: asyncio.Queue,
    results: list[TaskResult],
    rate_limiter: _RateLimiter | None,
    is_async: bool,
) -> None:
    loop = asyncio.get_running_loop()
    while True:
        item = await queue.get()
        if item is _SENTINEL:
            return

        task: TaskSpec = item
        if rate_limiter is not None:
            await rate_limiter.acquire()

        try:
            if is_async:
                value = await fn(*task.args, **task.kwargs)
            else:
                call = functools.partial(fn, *task.args, **task.kwargs)
                value = await loop.run_in_executor(None, call)
            results.append(TaskResult(task=task, result=value))
        except Exception as exc:
            results.append(TaskResult(task=task, error=exc))


async def run_workers(
    fn: Callable,
    tasks: Iterable[TaskSpec] | AsyncIterable[TaskSpec],
    *,
    num_workers: int = 1,
    queue_size: int = 0,
    rate_limit: float | None = None,
) -> list[TaskResult]:
    """
    Execute *fn* for each task using *num_workers* concurrent worker coroutines.

    :param fn: Callable (sync or async). Called as ``fn(*task.args, **task.kwargs)``.
    :param tasks: Tasks to process. Any :class:`Iterable` or
        :class:`AsyncIterable` of :class:`TaskSpec`. Use an async generator
        for large sources to avoid loading all tasks into memory at once.
    :param num_workers: Number of concurrent worker coroutines.
    :param queue_size: Maximum tasks buffered between producer and workers.
        ``0`` means unbounded.  Set to a small multiple of *num_workers* to
        bound memory when consuming from a large async source.
    :param rate_limit: Maximum requests per second **per worker**.
        ``None`` disables rate limiting.
    :returns: :class:`TaskResult` list in completion order (not input order).
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
    results: list[TaskResult] = []
    is_async = asyncio.iscoroutinefunction(fn)

    producer = asyncio.create_task(produce(tasks, queue, num_workers))
    workers = [
        asyncio.create_task(
            _run_worker(
                fn,
                queue,
                results,
                _RateLimiter(rate_limit) if rate_limit is not None else None,
                is_async,
            )
        )
        for _ in range(num_workers)
    ]

    await asyncio.gather(producer, *workers)
    return results
