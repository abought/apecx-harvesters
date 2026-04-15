"""Batch search against the EMDB native search API for EMDB entries."""

from __future__ import annotations

import urllib.parse
from collections.abc import AsyncIterator

import httpx

from ..base.parser import parse_author_name as _parse_author_name
from ..base.rate_limit import RateLimiter
from .constants import rate_limit as _default_rate_limit

_SEARCH_BASE = "https://www.ebi.ac.uk/emdb/api/search"
_DEFAULT_PAGE_SIZE = 500


def emdb_author_term(name: str | None = None, *, orcid: str | None = None) -> str:
    """
    Build an EMDB native search Lucene query string that filters by author.

    Uses the ``author`` field (any citation author) for name-based searches and
    ``author_orcid`` for ORCID-based searches.  At least one of *name* or
    *orcid* must be supplied.  When both are supplied they are OR'd so that
    records predating ORCID adoption are still captured via name matching::

        emdb_author_term("Jane Smith")
        → 'author:"Smith J"'

        emdb_author_term(orcid="0000-0002-1234-5678")
        → 'author_orcid:"0000-0002-1234-5678"'

        emdb_author_term("Jane Smith", orcid="0000-0002-1234-5678")
        → 'author:"Smith J" OR author_orcid:"0000-0002-1234-5678"'
    """
    if name is None and orcid is None:
        raise ValueError("At least one of 'name' or 'orcid' must be provided.")

    clauses: list[str] = []

    if name is not None:
        family, given = _parse_author_name(name)
        if given:
            clauses.append(f'author:"{family} {given[0]}"')
        else:
            clauses.append(f'author:"{family}"')

    if orcid is not None:
        orcid_clean = orcid.removeprefix("https://orcid.org/").strip()
        clauses.append(f'author_orcid:"{orcid_clean}"')

    return " OR ".join(clauses)


async def search(
    term: str,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    rate_limiter: RateLimiter | None = None,
) -> AsyncIterator[str]:
    """
    Yield EMDB entry IDs matching *term*, transparently paginating through all results.

    The Lucene *term* is embedded in the request URL path; encoding is handled
    internally and the caller should pass the raw query string.

    :param term: Lucene query string (e.g. ``'author:"Smith J"'``).
    :param client: Optional shared HTTP client.
    :param page_size: Results per page.
    :param rate_limiter: Shared rate limiter. Pass the same instance to the harvester to share the
        EBI request budget across search and retrieval. A default-rate limiter is created
        automatically when none is provided.
    """
    limiter: RateLimiter = rate_limiter if rate_limiter is not None else RateLimiter(_default_rate_limit)
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        url = f"{_SEARCH_BASE}/{urllib.parse.quote(term)}"
        page = 1
        while True:
            await limiter.acquire()
            response = await client.get(
                url,
                params={"rows": page_size, "page": page, "fl": "emdb_id"},
                headers={"Accept": "text/csv"},
            )
            response.raise_for_status()

            # Response is CSV: first line is the header, remaining lines are IDs.
            lines = response.text.splitlines()
            data_lines = lines[1:]
            if not data_lines:
                break
            for line in data_lines:
                emdb_id = line.strip()
                if emdb_id:
                    yield emdb_id
            page += 1
    finally:
        if owned:
            await client.aclose()
