"""Task producers: sources that yield TaskSpec items into a worker queue."""

from __future__ import annotations

import asyncio
import csv
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class TaskSpec:
    """Arguments for a single harvester invocation."""
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


_SENTINEL = object()


async def produce(
    tasks: Iterable[TaskSpec] | AsyncIterable[TaskSpec],
    queue: asyncio.Queue,
    num_workers: int,
) -> None:
    """Feed task specs into *queue*, then emit one sentinel per worker."""
    if isinstance(tasks, AsyncIterable):
        async for task in tasks:
            await queue.put(task)
    else:
        for task in tasks:
            await queue.put(task)
    for _ in range(num_workers):
        await queue.put(_SENTINEL)


async def id_producer(
    path: Path | str,
    id_col: str = "id",
) -> AsyncIterator[TaskSpec]:
    """Yield one :class:`TaskSpec` per row, using *id_col* as the single positional argument."""
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            yield TaskSpec(args=(row[id_col],))


async def search_producer(ids: AsyncIterator[str]) -> AsyncIterator[TaskSpec]:
    """Wrap an async iterator of ID strings as :class:`TaskSpec` items."""
    async for id_ in ids:
        yield TaskSpec(args=(id_,))


async def csv_producer(
    path: Path | str,
    arg_col: str,
    kwarg_col: str,
) -> AsyncIterator[TaskSpec]:
    """
    Yield one :class:`TaskSpec` per row of a two-column CSV file.

    The value under *arg_col* becomes the single positional argument;
    the value under *kwarg_col* becomes a keyword argument keyed by the
    column name.
    """
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            yield TaskSpec(args=(row[arg_col],), kwargs={kwarg_col: row[kwarg_col]})
