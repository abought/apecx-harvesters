"""Crossref harvester."""

from __future__ import annotations

import urllib.parse
import orjson
from typing import ClassVar

from apecx_harvesters.loaders.base import BaseHarvester
from ..base import DataCite
from .constants import rate_limit
from .parser import _parse_work

_BASE_URL = "https://api.crossref.org/works"


class CrossrefHarvester(BaseHarvester):
    """Fetch records for a CrossRef DOI (most articles and preprints)"""
    # Crossref /works?filter= accepts comma-separated doi: clauses; rows=1000
    # comfortably covers any chunk ≤ _BATCH_SIZE in one page.
    _BATCH_SIZE: ClassVar[int] = 100
    _CACHE_DIR = "crossref"
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    def _normalize_id(self, id_: str) -> str:
        return id_.lower()

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        params = urllib.parse.urlencode({
            "filter": ",".join(f"doi:{doi}" for doi in ids),
            "rows": 1000,
        })
        return f"{_BASE_URL}?{params}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        payload = orjson.loads(content)
        result: dict[str, str] = {}
        for item in payload["message"].get("items", []):
            doi = (item.get("DOI") or "").lower()
            if doi:
                result[doi] = orjson.dumps(item).decode()
        return result

    async def _parse_item(self, content: str) -> DataCite:
        return _parse_work(orjson.loads(content))