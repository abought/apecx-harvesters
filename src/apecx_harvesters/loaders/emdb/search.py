"""Batch search against the EBI Search API for EMDB entries."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import orjson

from .constants import rate_limit as _default_rate_limit

_SEARCH_URL = "https://www.ebi.ac.uk/ebisearch/ws/rest/emdb"
_DEFAULT_PAGE_SIZE = 500


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requests_per_second: float | None = _default_rate_limit,
) -> AsyncIterator[str]:
    """
    Yield EMDB entry IDs matching *term*, transparently paginating through all results.

    :param term: Query string passed directly to EBI Search.
    :param client: Optional shared HTTP client.
    :param page_size: Results per page.
    :param requests_per_second: Maximum request rate. EBI does not publish a hard limit;
        pass a lower value when sharing the budget with concurrent retrieval.
    """
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        start = 0
        total: int | None = None
        while total is None or start < total:
            response = await client.get(
                _SEARCH_URL,
                params={
                    "query": term,
                    "format": "json",
                    "fields": "id",
                    "size": page_size,
                    "start": start,
                },
            )
            response.raise_for_status()
            if requests_per_second is not None:
                await asyncio.sleep(1.0 / requests_per_second)
            data = orjson.loads(response.content)

            if total is None:
                total = int(data["hitCount"])

            entries: list[dict] = data.get("entries") or []
            for entry in entries:
                yield entry["id"]

            start += len(entries)
            if not entries:
                break
    finally:
        if owned:
            await client.aclose()