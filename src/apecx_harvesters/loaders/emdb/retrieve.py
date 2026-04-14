"""EMDB harvester — single-record and parallel multi-record retrieval."""

from __future__ import annotations

import urllib.parse
from typing import ClassVar

import orjson

from apecx_harvesters.loaders.base import BaseHarvester
from .constants import rate_limit
from .model import EMDBContainer
from .parser import _parse_entry

_SEARCH_BASE = "https://www.ebi.ac.uk/emdb/api/search"

# Each encoded ID adds ~33 chars to the URL; 25 IDs → ~870-char URL.
_BATCH_SIZE = 25


class EMDBHarvester(BaseHarvester):
    """Fetch structural data from the EMDB."""
    _CACHE_DIR = "emdb"
    _BATCH_SIZE = _BATCH_SIZE
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        query = " OR ".join(f'emdb_id:"{id_}"' for id_ in ids)
        url = f"{_SEARCH_BASE}/{urllib.parse.quote(query)}?rows={len(ids)}&page=1"
        return url, None, {"Accept": "application/json"}

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        entries: list[dict] = orjson.loads(content)
        return {entry["emdb_id"]: orjson.dumps(entry).decode() for entry in entries}

    async def _parse_item(self, content: str) -> EMDBContainer:
        return _parse_entry(orjson.loads(content))
