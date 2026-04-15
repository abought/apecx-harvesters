"""
Aggregate cached records from all harvesters into Globus Search ingest chunks.

Run this after one or more search scripts have populated the local cache.  Each
invocation creates a new timestamped subdirectory containing only records that
were fetched or updated since the previous aggregation run.

Usage
-----
    uv run aggregate-gsearch
    uv run aggregate-gsearch --output output --cache-root .cache

Output
------
    output/<timestamp>/pubmed/chunk00001.json.gz
    output/<timestamp>/pdb/chunk00001.json.gz
    ...

Each file is a gzip-compressed Globus Search GMetaList ingest document (≤ 10 MB uncompressed).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import gzip
import json
import logging
from pathlib import Path
from typing import Any

import apecx_harvesters.loaders  # noqa: F401  — register all harvester subclasses
from apecx_harvesters.loaders.base import BaseHarvester
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.pipeline import to_gmetalist

logger = logging.getLogger(__name__)

_TIMESTAMP_FMT = "%Y%m%dT%H%M%S"


def _last_aggregation(output_root: Path) -> datetime.datetime | None:
    """Return the datetime of the most recent aggregation run under output_root, or None."""
    if not output_root.exists():
        return None
    candidates = []
    for d in output_root.iterdir():
        if d.is_dir():
            try:
                candidates.append(datetime.datetime.strptime(d.name, _TIMESTAMP_FMT))
            except ValueError:
                pass
    return max(candidates) if candidates else None


async def _aggregate(
    harvester: BaseHarvester[Any],
    output_dir: Path,
    source: str,
    since: datetime.datetime | None,
) -> None:
    """Read new/updated records from cache and write Globus Search ingest chunks."""
    output_dir.mkdir(parents=True, exist_ok=True)
    label = f"since {since.isoformat()}" if since else "full export"

    i = 0
    async for batch in to_gmetalist(harvester.iter_cached(since=since)):
        i += 1
        path = output_dir / f"chunk{i:05d}.json.gz"
        path.write_bytes(gzip.compress(json.dumps(batch).encode()))
        logger.info("[%s] wrote %s (%s)", source, path, label)


async def _run(output_root: Path, cache_root: Path) -> None:
    since = _last_aggregation(output_root)
    run_dir = output_root / datetime.datetime.now().strftime(_TIMESTAMP_FMT)

    harvesters: list[tuple[BaseHarvester[Any], str]] = [
        (PubMedHarvester(cache_root=cache_root), "pubmed"),
        (PDBHarvester(cache_root=cache_root), "pdb"),
    ]

    await asyncio.gather(
        *[_aggregate(h, run_dir / source, source, since) for h, source in harvesters]
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate cached records into Globus Search ingest chunks."
    )
    parser.add_argument(
        "--output",
        default="output",
        metavar="DIR",
        help="Root output directory (default: %(default)s).",
    )
    parser.add_argument(
        "--cache-root",
        default=".cache",
        metavar="DIR",
        help="Cache root directory (default: %(default)s).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("apecx_harvesters").setLevel(logging.INFO)
    asyncio.run(_run(Path(args.output), Path(args.cache_root)))


if __name__ == "__main__":
    main()
