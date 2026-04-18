"""Batch search against the PubMed eSearch API."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from datetime import date, timedelta

import httpx
import orjson

from ..base.http_retry import http_request as _http_request
from ..base.parser import parse_author_name as _parse_author_name
from ..base.rate_limit import RateLimiter
from .constants import rate_limit as _default_rate_limit

def pubmed_author_term(name: str | None = None, orcid: str | None = None) -> str:
    """
    Build a PubMed eSearch author query string.  At least one of *name* or
    *orcid* must be supplied.

    PubMed indexes author names as ``"LastName FirstName"`` or ``"LastName F"``
    (last name first, no comma, space-separated).  Both the full-name and
    initial forms are OR'd together to cover older records that lack full given
    names.  When only *orcid* is supplied, the query matches exclusively on the
    ``[auid]`` field.  When both are supplied, ORCID and name variants are OR'd.

    Examples::

        pubmed_author_term("Andrzej Joachimiak")
        # → ("Joachimiak Andrzej"[Author] OR "Joachimiak A"[Author])

        pubmed_author_term(orcid="0000-0002-1234-5678")
        # → ("0000-0002-1234-5678"[auid])

        pubmed_author_term("Jane Smith", orcid="0000-0002-1234-5678")
        # → ("Smith Jane"[Author] OR "Smith J"[Author] OR "0000-0002-1234-5678"[auid])
    """
    if name is None and orcid is None:
        raise ValueError("At least one of 'name' or 'orcid' must be provided.")

    clauses: list[str] = []

    if name is not None:
        family, given = _parse_author_name(name)
        if given:
            initial = given[0]
            if len(given) > 1:
                clauses.append(f'"{family} {given}"[Author]')
            clauses.append(f'"{family} {initial}"[Author]')
        else:
            clauses.append(f'"{family}"[Author]')

    if orcid is not None:
        orcid_clean = orcid.removeprefix("https://orcid.org/").strip()
        clauses.append(f'"{orcid_clean}"[auid]')

    return f"({' OR '.join(clauses)})"


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

# PubMed occasionally returns HTTP 200 with an ERROR field whose text describes a backend
# failure (e.g. "Search Backend failed: ... HTTP request returned 502 status").  These are
# transient; permanent query errors (bad syntax, unknown field) do not match this pattern.
_TRANSIENT_ESEARCH_RE = re.compile(r"Search Backend failed|HTTP request returned [45]\d\d", re.IGNORECASE)
_ESEARCH_MAX_RETRIES = 3

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
    rate_limiter: RateLimiter | None,
    api_key: str | None = None,
) -> dict:
    """Make a single eSearch request and return the ``esearchresult`` dict."""
    params: dict[str, str | int] = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retstart": retstart,
        "retmax": retmax,
    }
    if api_key is not None:
        params["api_key"] = api_key
    result: dict = {}
    for attempt in range(_ESEARCH_MAX_RETRIES + 1):
        response = await _http_request(
            client,
            "GET",
            _ESEARCH_URL,
            rate_limiter=rate_limiter,
            params=params,
        )
        response.raise_for_status()
        clean = _CONTROL_CHARS_RE.sub(b"", response.content)
        result = orjson.loads(clean)["esearchresult"]
        if "ERROR" not in result:
            break
        msg = result["ERROR"]
        if not _TRANSIENT_ESEARCH_RE.search(msg) or attempt == _ESEARCH_MAX_RETRIES:
            raise ValueError(f"PubMed eSearch error: {msg}")
        wait = min(2.0 ** attempt, 60.0)
        _log.warning(
            "PubMed eSearch application-level error (attempt %d/%d); retrying in %.1fs: %s",
            attempt + 1, _ESEARCH_MAX_RETRIES, wait, msg,
        )
        await asyncio.sleep(wait)
    if "querytranslation" in result:
        _log.debug("eSearch querytranslation: %s", result["querytranslation"])
    return result


async def _count(
    term: str,
    *,
    client: httpx.AsyncClient,
    rate_limiter: RateLimiter | None,
    api_key: str | None = None,
) -> int:
    """Return the total result count for *term* without fetching any IDs."""
    result = await _esearch(term, client=client, retmax=0, rate_limiter=rate_limiter, api_key=api_key)
    return int(result["count"])


async def _fetch_ids(
    term: str,
    *,
    client: httpx.AsyncClient,
    page_size: int,
    rate_limiter: RateLimiter | None,
    api_key: str | None = None,
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
            rate_limiter=rate_limiter,
            api_key=api_key,
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
    rate_limiter: RateLimiter | None,
    api_key: str | None = None,
) -> AsyncIterator[str]:
    """
    Yield IDs for *term* within [start_date, end_date], recursively bisecting
    the date range whenever a segment exceeds _RESULT_LIMIT.

    If a single-day window still exceeds the limit, a warning is logged and
    only the first _RESULT_LIMIT results are returned.
    """
    date_term = f"({term}) AND {start_date:%Y/%m/%d}:{end_date:%Y/%m/%d}[pdat]"
    n = await _count(date_term, client=client, rate_limiter=rate_limiter, api_key=api_key)

    if n == 0:
        return

    if n <= _RESULT_LIMIT:
        async for pmid in _fetch_ids(
            date_term,
            client=client,
            page_size=page_size,
            rate_limiter=rate_limiter,
            api_key=api_key,
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
            rate_limiter=rate_limiter,
            api_key=api_key,
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
        rate_limiter=rate_limiter,
        api_key=api_key,
    ):
        yield pmid
    async for pmid in _search_bounded(
        term,
        mid + timedelta(days=1),
        end_date,
        client=client,
        page_size=page_size,
        rate_limiter=rate_limiter,
        api_key=api_key,
    ):
        yield pmid


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    rate_limiter: RateLimiter | None = None,
    api_key: str | None = None,
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
    :param rate_limiter: Shared rate limit 
    :param api_key: NCBI API key. Allows higher request limits.
    """
    if rate_limiter is None:
        rate_limiter = RateLimiter(_default_rate_limit)
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        total = await _count(term, client=client, rate_limiter=rate_limiter, api_key=api_key)
        _log.info("Total results: %d", total)

        if total <= _RESULT_LIMIT:
            async for pmid in _fetch_ids(
                term,
                client=client,
                page_size=page_size,
                rate_limiter=rate_limiter,
                api_key=api_key,
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
                rate_limiter=rate_limiter,
                api_key=api_key,
            ):
                yield pmid
    finally:
        if owned:
            await client.aclose()