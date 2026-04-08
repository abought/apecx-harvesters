"""Generic pipeline sources."""

from __future__ import annotations

import csv
from collections.abc import AsyncIterator
from pathlib import Path


async def csv_ids(path: Path | str, col: str = "id") -> AsyncIterator[str]:
    """
    Yield IDs from a single column of a CSV file.

    This is a proof of concept to show that harvesters don't have to locate records from a free-text search.
        A list of IDs can come from anywhere.
    """
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            yield row[col]