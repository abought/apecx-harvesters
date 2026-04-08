"""Batch search against the PubMed eSearch API."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator

import httpx
import orjson

from .constants import rate_limit as _default_rate_limit

# Observed: PubMed eSearch responses can contain bare control characters (including \t, \n, \r)
# inside JSON string values, which strict parsers reject.  Strip all ASCII control characters;
# this is safe for compact API responses where structural whitespace is not meaningful.
_CONTROL_CHARS_RE = re.compile(rb"[\x00-\x1f\x7f]")

_log = logging.getLogger(__name__)
# eSearch hard limit: retstart cannot exceed 9,998 (0-indexed), so at most 9,999 records
# are reachable via this API. Use EDirect for larger result sets (see OPEN_QUESTIONS.md).
_RESULT_LIMIT = 9_999

_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_DEFAULT_PAGE_SIZE = 500


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requests_per_second: float | None = _default_rate_limit,
) -> AsyncIterator[str]:
    """
    Yield PubMed IDs matching *term*, transparently paginating through all results.

    *term* is passed directly to the eSearch ``term`` parameter, so any PubMed
    query syntax is supported — for example::

        "HIV"
        "influenza AND 2010:2020[pdat]"
        "SARS-CoV-2[Title/Abstract]"

    :param term: PubMed query string.
    :param client: Optional shared HTTP client.
    :param page_size: IDs per page (max 10,000).
    :param requests_per_second: Maximum request rate. NCBI allows 3 req/s without an API key;
        pass a lower value when sharing the budget with concurrent retrieval.
    """
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        start = 0
        total: int | None = None
        while (total is None or start < total) and start < _RESULT_LIMIT:
            response = await client.get(
                _ESEARCH_URL,
                params={
                    "db": "pubmed",
                    "term": term,
                    "retmode": "json",
                    "retstart": start,
                    "retmax": page_size,
                },
            )
            response.raise_for_status()
            clean = _CONTROL_CHARS_RE.sub(b"", response.content)
            result = orjson.loads(clean)["esearchresult"]

            if requests_per_second is not None:
                await asyncio.sleep(1.0 / requests_per_second)

            if "ERROR" in result:
                raise ValueError(f"PubMed eSearch error: {result['ERROR']}")

            if total is None:
                total = int(result["count"])
                _log.info(f"Total results: {total}")

                if total > _RESULT_LIMIT:
                    _log.warning(
                        "PubMed query returned %d results; only the first %d will be retrieved. "
                        "Consider narrowing the query (e.g. add a date range).",
                        total,
                        _RESULT_LIMIT,
                    )

            ids: list[str] = result["idlist"]
            for pmid in ids:
                yield pmid

            start += len(ids)
            if not ids:
                break
    finally:
        if owned:
            await client.aclose()
