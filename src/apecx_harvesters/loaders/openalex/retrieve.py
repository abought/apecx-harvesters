"""OpenAlex harvester."""

from __future__ import annotations

import urllib.parse
import orjson
from typing import ClassVar

from ..base import BaseHarvester, DataCite
from .constants import rate_limit
from .parser import _BATCH_SELECT, _parse_work, _strip_doi

_API_BASE = "https://api.openalex.org/works"


class OpenAlexHarvester(BaseHarvester):
    """
    Retrieve information about a DOI from the generalist OpenAlex service.

    We don't use them as first-resort because they attempt to do some AI harmonization/annotations-
        pretty cool, but we'd need to validate before we made them first source of truth.
    """

    # Pipe-separated DOI filter; keep chunks small to avoid overly long URLs.
    _BATCH_SIZE: ClassVar[int] = 50
    _CACHE_DIR = "openalex"
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    def _normalize_id(self, id_: str) -> str:
        return id_.lower()

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        # filter=doi:A|B|C requests exact matches; per_page=200 covers any chunk ≤ _BATCH_SIZE.
        # select= is required to include abstract_inverted_index, which is omitted from list
        # responses by default.
        params = urllib.parse.urlencode({
            "filter": "doi:" + "|".join(ids),
            "per_page": 200,
            "select": _BATCH_SELECT,
        })
        return f"{_API_BASE}?{params}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        """Split an OpenAlex list response into per-DOI raw item strings.

        select= in the request ensures abstract_inverted_index is present,
        making batch items equivalent to single-record responses.
        """
        payload = orjson.loads(content)
        result: dict[str, str] = {}
        for work in payload.get("results", []):
            doi = _strip_doi(work.get("doi"))
            if doi:
                result[doi.lower()] = orjson.dumps(work).decode()
        return result

    async def _parse_item(self, content: str) -> DataCite:
        return _parse_work(orjson.loads(content))