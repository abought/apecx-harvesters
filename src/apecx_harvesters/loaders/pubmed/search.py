"""Batch search against the PubMed eSearch API."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from datetime import date, timedelta

import httpx
import orjson

from .constants import rate_limit as _default_rate_limit

# Observed: PubMed eSearch responses can contain bare control characters (including \t, \n, \r)
# inside JSON string values, which strict parsers reject.  Strip all ASCII control characters;
# this is safe for compact API responses where structural whitespace is not meaningful.
_CONTROL_CHARS_RE = re.compile(rb"[\x00-\x1f\x7f]")

_log = logging.getLogger(__name__)
# eSearch hard limit: retstart cannot exceed 9,998 (0-indexed), so at most 9,999 records
# are reachable per query. Queries exceeding this are handled via date-range segmentation.
_RESULT_LIMIT = 9_999

_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_DEFAULT_PAGE_SIZE = 500

# PubMed's practical earliest records; used as the lower bound for date segmentation.
# Note: records lacking a publication date (pdat) fall outside any bounded date range
# and will not be returned when segmentation is active.
_PUBMED_EPOCH = date(1800, 1, 1)


async def _esearch(
    term: str,
    *,
    client: httpx.AsyncClient,
    retstart: int = 0,
    retmax: int = 0,
    requests_per_second: float | None,
) -> dict:
    """Make a single eSearch request and return the ``esearchresult`` dict."""
    response = await client.get(
        _ESEARCH_URL,
        params={
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retstart": retstart,
            "retmax": retmax,
        },
    )
    response.raise_for_status()
    clean = _CONTROL_CHARS_RE.sub(b"", response.content)
    result = orjson.loads(clean)["esearchresult"]
    if requests_per_second is not None:
        await asyncio.sleep(1.0 / requests_per_second)
    if "ERROR" in result:
        raise ValueError(f"PubMed eSearch error: {result['ERROR']}")
    if "querytranslation" in result:
        _log.debug("eSearch querytranslation: %s", result["querytranslation"])
    return result


async def _count(
    term: str,
    *,
    client: httpx.AsyncClient,
    requests_per_second: float | None,
) -> int:
    """Return the total result count for *term* without fetching any IDs."""
    result = await _esearch(term, client=client, retmax=0, requests_per_second=requests_per_second)
    return int(result["count"])


async def _fetch_ids(
    term: str,
    *,
    client: httpx.AsyncClient,
    page_size: int,
    requests_per_second: float | None,
) -> AsyncIterator[str]:
    """Yield all IDs for *term*, paginating up to _RESULT_LIMIT records."""
    start = 0
    total: int | None = None
    while True:
        result = await _esearch(
            term,
            client=client,
            retstart=start,
            retmax=page_size,
            requests_per_second=requests_per_second,
        )
        if total is None:
            total = int(result["count"])
        ids: list[str] = result["idlist"]
        for pmid in ids:
            yield pmid
        start += len(ids)
        if not ids or start >= min(total, _RESULT_LIMIT):
            break


async def _search_bounded(
    term: str,
    start_date: date,
    end_date: date,
    *,
    client: httpx.AsyncClient,
    page_size: int,
    requests_per_second: float | None,
) -> AsyncIterator[str]:
    """
    Yield IDs for *term* within [start_date, end_date], recursively bisecting
    the date range whenever a segment exceeds _RESULT_LIMIT.

    If a single-day window still exceeds the limit, a warning is logged and
    only the first _RESULT_LIMIT results are returned.
    """
    date_term = f"({term}) AND {start_date:%Y/%m/%d}:{end_date:%Y/%m/%d}[pdat]"
    n = await _count(date_term, client=client, requests_per_second=requests_per_second)

    if n == 0:
        return

    if n <= _RESULT_LIMIT:
        async for pmid in _fetch_ids(
            date_term,
            client=client,
            page_size=page_size,
            requests_per_second=requests_per_second,
        ):
            yield pmid
        return

    if start_date == end_date:
        _log.warning(
            "Single-day query on %s has %d results; only the first %d will be retrieved.",
            start_date,
            n,
            _RESULT_LIMIT,
        )
        async for pmid in _fetch_ids(
            date_term,
            client=client,
            page_size=page_size,
            requests_per_second=requests_per_second,
        ):
            yield pmid
        return

    # Bisect the date range and recurse into each half.
    mid = start_date + (end_date - start_date) // 2
    async for pmid in _search_bounded(
        term,
        start_date,
        mid,
        client=client,
        page_size=page_size,
        requests_per_second=requests_per_second,
    ):
        yield pmid
    async for pmid in _search_bounded(
        term,
        mid + timedelta(days=1),
        end_date,
        client=client,
        page_size=page_size,
        requests_per_second=requests_per_second,
    ):
        yield pmid


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requests_per_second: float | None = _default_rate_limit,
) -> AsyncIterator[str]:
    """
    Yield PubMed IDs matching *term*, transparently paginating through all results.

    For result sets exceeding 9,999 records (the eSearch per-query ceiling), the
    query is automatically subdivided into date-bounded segments.

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
        total = await _count(term, client=client, requests_per_second=requests_per_second)
        _log.info("Total results: %d", total)

        if total <= _RESULT_LIMIT:
            async for pmid in _fetch_ids(
                term,
                client=client,
                page_size=page_size,
                requests_per_second=requests_per_second,
            ):
                yield pmid
        else:
            _log.info(
                "Query has %d results (exceeds %d limit); using date-range segmentation.",
                total,
                _RESULT_LIMIT,
            )
            today = date.today()
            async for pmid in _search_bounded(
                term,
                _PUBMED_EPOCH,
                today,
                client=client,
                page_size=page_size,
                requests_per_second=requests_per_second,
            ):
                yield pmid
    finally:
        if owned:
            await client.aclose()