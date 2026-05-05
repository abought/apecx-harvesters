"""IEDB harvester."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import orjson

from apecx_harvesters.loaders.base import BaseHarvester
from apecx_harvesters.loaders.base.retrieve import RetrievalResult
from .model import IEDBContainer
from .parser import _parse_export
from .search import search as iedb_search


class IEDBHarvester(BaseHarvester[IEDBContainer]):
    """Fetch IEDB epitope export data by source organism."""

    _CACHE_DIR = "iedb"
    _BATCH_SIZE = 1

    async def iter_query_results(
        self,
        term: str,
    ) -> AsyncIterator[RetrievalResult[IEDBContainer]]:
        owned = self._client is None
        if owned:
            self._client = httpx.AsyncClient()

        try:
            raw = {
                "query": term,
                "rows": [
                    row async for row in iedb_search(
                        term,
                        client=self._client,
                        rate_limiter=self._rate_limiter,
                    )
                ],
            }

            content = orjson.dumps(raw).decode()
            record = await self._parse_item(content)

            if self._use_cache:
                path = await self._cache_path(term)
                await self._cache_save(path, content)

            yield RetrievalResult(id=term, record=record)

        except Exception as exc:
            yield RetrievalResult(id=term, error=str(exc))

        finally:
            if owned:
                assert self._client is not None
                await self._client.aclose()
                self._client = None

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        raise NotImplementedError(
            "IEDBHarvester does not support ID-based retrieval. "
            "Use iter_query_results() because IEDB search returns complete rows."
        )

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        raise NotImplementedError(
            "IEDBHarvester does not support ID-based batch splitting. "
            "Use iter_query_results() because IEDB search returns complete rows."
        )

    async def _parse_item(self, content: str) -> IEDBContainer:
        return _parse_export(orjson.loads(content))
