"""Pipeline runner: drives a source through transforms into a sink."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from apecx_harvesters.loaders.base import DataCite
from apecx_harvesters.loaders.base.retrieve import RetrievalResult

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=DataCite)

# Process the stream of returned results in some useful way.
Sink = Callable[[AsyncIterator[RetrievalResult[Any]]], Awaitable[Any]]


@dataclass
class PipelineSpec(Generic[T]):
    """Arguments for a single run() call, for use with run_parallel()."""
    source: AsyncIterator[RetrievalResult[T]]
    sink: Sink
    transforms: list[Callable[[T], Awaitable[T]]] = field(default_factory=list)
    name: str = ""


async def run(
    source: AsyncIterator[RetrievalResult[T]],
    sink: Sink,
    transforms: list[Callable[[T], Awaitable[T]]] | None = None,
) -> Any:
    """
    Implement a pipeline for scraping+harmonization:
    - Data is discovered/retrieved from some `source`
    - Cleanup steps can call external tools on each record via `transforms`
    - A `sink` receives the stream of results, and does whatever it wants (ranging from saving on disk to
        caching in a public search index)
    """
    _transforms = transforms or []

    async def _pipe() -> AsyncIterator[RetrievalResult[T]]:
        async for result in source:
            if result.ok and _transforms:
                assert result.record is not None
                record: T = result.record
                try:
                    for t in _transforms:
                        record = await t(record)
                    yield dataclasses.replace(result, record=record)
                except Exception as exc:
                    yield dataclasses.replace(
                        result,
                        record=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
            else:
                yield result

    return await sink(_pipe())


async def run_parallel(*specs: PipelineSpec[Any]) -> list[Any]:
    """Run multiple pipelines concurrently and return their results in order."""
    return list(
        await asyncio.gather(*[
            run(s.source, s.sink, s.transforms)
            for s in specs
        ])
    )