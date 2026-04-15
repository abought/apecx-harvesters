"""bioRxiv/medRxiv harvester."""

from __future__ import annotations

import orjson
import re
from pathlib import Path
from typing import ClassVar

from ..base import BaseHarvester
from .constants import rate_limit
from .model import BiorXivContainer
from .parser import _parse_preprint

_BASE_URL = "https://api.biorxiv.org/details"


class BiorxivHarvester(BaseHarvester[BiorXivContainer]):
    """Fetch a biorxiv or medRxiv preprint. In practice, most people may prefer DOI harvesters instead
        as a more generic solution"""
    _BATCH_SIZE = 1
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    async def _cache_path(self, id_: str, server: str = "biorxiv") -> Path:
        safe = re.sub(r'[/\\:*?"<>|\s]', "_", id_)
        return self._cache_root / "biorxiv" / f"{safe}__{server}.json.gz"

    async def _build_request(self, ids: list[str], server: str = "biorxiv") -> tuple[str, str | None, dict | None]:
        return f"{_BASE_URL}/{server}/{ids[0]}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        return {ids[0]: content}

    async def _parse_item(self, content: str) -> BiorXivContainer:
        return _parse_preprint(orjson.loads(content))