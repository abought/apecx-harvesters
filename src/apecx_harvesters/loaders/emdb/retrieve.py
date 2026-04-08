"""EMDB harvester — single-record and parallel multi-record retrieval."""

from __future__ import annotations

import orjson
from typing import ClassVar

from apecx_harvesters.loaders.base import BaseHarvester
from .constants import rate_limit
from .model import EMDBContainer
from .parser import _parse_entry

_API_BASE = "https://www.ebi.ac.uk/emdb/api/entry"


class EMDBHarvester(BaseHarvester):
    """Fetch structural data from the EMDB"""
    _CACHE_DIR = "emdb"
    _BATCH_SIZE = 1
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        return f"{_API_BASE}/{ids[0]}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        return {ids[0]: content}

    async def _parse_item(self, content: str) -> EMDBContainer:
        return _parse_entry(orjson.loads(content))