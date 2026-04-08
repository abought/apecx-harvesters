"""DataCite DOI harvester."""

from __future__ import annotations

import orjson
from typing import ClassVar

from apecx_harvesters.loaders.base import BaseHarvester
from ..base import DataCite
from .constants import rate_limit
from .parser import _parse_work

_API_BASE = "https://api.datacite.org/dois"


class DataCiteHarvester(BaseHarvester):
    """Fetch data for a datacite DOI. Includes many dataset publishers"""
    _CACHE_DIR = "datacite"
    _BATCH_SIZE = 1
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        return f"{_API_BASE}/{ids[0]}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        return {ids[0]: content}

    async def _parse_item(self, content: str) -> DataCite:
        attrs = orjson.loads(content)["data"]["attributes"]
        return _parse_work(attrs)