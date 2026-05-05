from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import orjson

from ..base.http_retry import http_request as _http_request
from ..base.rate_limit import RateLimiter

_EXPORT_URL = "https://query-api.iedb.org/epitope_export"


def source_organism_term(term: str) -> dict[str, str]:
    """
    Build IEDB query parameters for epitope source organism matching.

    Example:
        source_organism_term("influenza")
        -> {"epitope__source_organism": "like.*influenza*"}
    """
    return {
        "epitope__source_organism": f"like.*{term}*",
    }


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    rate_limiter: RateLimiter | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """
    Yield IEDB epitope export rows matching a term or biological entity.

    Unlike PubMed/PDB/EMDB search functions, this yields full row dictionaries,
    not IDs, because IEDB's epitope_export endpoint already returns the data.
    """
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        assert client is not None

        response = await _http_request(
            client,
            "GET",
            _EXPORT_URL,
            rate_limiter=rate_limiter,
            params=source_organism_term(term),
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()

        data = orjson.loads(response.content)

        if not isinstance(data, list):
            raise ValueError(f"Unexpected IEDB response type: {type(data).__name__}")

        for row in data:
            if isinstance(row, dict):
                yield row

    finally:
        if owned:
            await client.aclose()
