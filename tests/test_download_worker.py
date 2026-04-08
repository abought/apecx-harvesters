"""Tests for the async bulk workers worker pool."""

from __future__ import annotations

import asyncio

import pytest

from apecx_harvesters.workers import run_workers
from apecx_harvesters.workers import TaskSpec


# ---------------------------------------------------------------------------
# Fixture functions
# ---------------------------------------------------------------------------

def sync_double(x: int) -> int:
    return x * 2


async def async_double(x: int) -> int:
    return x * 2


def failing_fn(x: int) -> int:
    raise ValueError(f"Failed: {x}")


async def async_timed(x: int, call_times: list[float]) -> int:
    call_times.append(asyncio.get_running_loop().time())
    return x


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sync_fn_all_tasks_processed():
    tasks = [TaskSpec(args=(i,)) for i in range(5)]
    results = await run_workers(sync_double, tasks, num_workers=2)
    assert len(results) == 5
    assert all(r.ok for r in results)
    assert sorted(r.result for r in results) == [0, 2, 4, 6, 8]


@pytest.mark.asyncio
async def test_async_fn_all_tasks_processed():
    tasks = [TaskSpec(args=(i,)) for i in range(5)]
    results = await run_workers(async_double, tasks, num_workers=2)
    assert len(results) == 5
    assert all(r.ok for r in results)
    assert sorted(r.result for r in results) == [0, 2, 4, 6, 8]


@pytest.mark.asyncio
async def test_kwargs_forwarded():
    async def fn(*, value: int) -> int:
        return value

    tasks = [TaskSpec(kwargs={"value": 7})]
    results = await run_workers(fn, tasks)
    assert results[0].result == 7


@pytest.mark.asyncio
async def test_empty_task_list():
    results = await run_workers(sync_double, [])
    assert results == []


@pytest.mark.asyncio
async def test_single_worker_processes_all():
    tasks = [TaskSpec(args=(i,)) for i in range(10)]
    results = await run_workers(async_double, tasks, num_workers=1)
    assert len(results) == 10


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_tasks_captured():
    tasks = [TaskSpec(args=(i,)) for i in range(3)]
    results = await run_workers(failing_fn, tasks, num_workers=1)
    assert len(results) == 3
    assert all(not r.ok for r in results)
    assert all(isinstance(r.error, ValueError) for r in results)


@pytest.mark.asyncio
async def test_mixed_success_and_failure():
    def sometimes_fails(x: int) -> int:
        if x % 2 == 0:
            raise RuntimeError("even")
        return x

    tasks = [TaskSpec(args=(i,)) for i in range(4)]
    results = await run_workers(sometimes_fails, tasks, num_workers=2)
    assert len(results) == 4
    successes = [r for r in results if r.ok]
    failures = [r for r in results if not r.ok]
    assert len(successes) == 2
    assert len(failures) == 2


@pytest.mark.asyncio
async def test_task_result_carries_original_task():
    tasks = [TaskSpec(args=(42,))]
    results = await run_workers(sync_double, tasks)
    assert results[0].task is tasks[0]


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limiting_enforces_interval():
    call_times: list[float] = []
    tasks = [TaskSpec(args=(i, call_times)) for i in range(4)]
    rate = 20.0  # 20 req/s → 0.05 s minimum interval
    await run_workers(async_timed, tasks, num_workers=1, rate_limit=rate)

    assert len(call_times) == 4
    min_interval = 1.0 / rate
    intervals = [call_times[i + 1] - call_times[i] for i in range(3)]
    # Allow 10 % tolerance for timer jitter
    assert all(iv >= min_interval * 0.9 for iv in intervals)


@pytest.mark.asyncio
async def test_rate_limit_is_per_worker():
    """Two workers at 20 req/s each should handle 4 tasks faster than one worker."""
    call_times_1w: list[float] = []
    call_times_2w: list[float] = []

    rate = 20.0  # 0.05 s per call
    n = 4

    tasks_1 = [TaskSpec(args=(i, call_times_1w)) for i in range(n)]
    t0 = asyncio.get_event_loop().time()
    await run_workers(async_timed, tasks_1, num_workers=1, rate_limit=rate)
    elapsed_1w = asyncio.get_event_loop().time() - t0

    tasks_2 = [TaskSpec(args=(i, call_times_2w)) for i in range(n)]
    t0 = asyncio.get_event_loop().time()
    await run_workers(async_timed, tasks_2, num_workers=2, rate_limit=rate)
    elapsed_2w = asyncio.get_event_loop().time() - t0

    # Two workers should finish in roughly half the time
    assert elapsed_2w < elapsed_1w * 0.75
