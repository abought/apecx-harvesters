"""
Tests for pubmed.search — pagination and date-range segmentation.

Uses respx to mock HTTP responses at the transport layer, so tests reflect
actual outgoing requests rather than patching internal functions.

Date arithmetic reference (for bisection assertions):
    START = 2020-01-01, END = 2020-12-31
    MID     = START + timedelta(182) = 2020-07-01   (first bisection midpoint)
    SUB_MID = START + timedelta(90)  = 2020-04-01   (midpoint of [START, MID])
"""

from __future__ import annotations

from datetime import date, timedelta

import httpx
import pytest
import respx

from apecx_harvesters.loaders.pubmed.search import (
    _ESEARCH_URL,
    _search_bounded,
    search,
)

# Fixed date window used throughout — bisection midpoints are fully deterministic.
_START = date(2020, 1, 1)
_END = date(2020, 12, 31)
_MID = date(2020, 7, 1)      # _START + (END - START) // 2 = +182 days


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _response(count: int, ids: list[str] | None = None) -> httpx.Response:
    """Build a minimal esearch JSON response."""
    return httpx.Response(200, json={
        "esearchresult": {"count": str(count), "idlist": ids or []}
    })


def _sequential(*responses: httpx.Response):
    """
    Return a respx side_effect callable that serves *responses* in order.
    Each incoming request consumes the next response from the sequence.
    """
    it = iter(responses)
    return lambda _req: next(it)


def _params(request: httpx.Request) -> dict[str, str]:
    return dict(request.url.params)


# ---------------------------------------------------------------------------
# _search_bounded — recursive date-range logic
# ---------------------------------------------------------------------------

class TestSearchBounded:
    @pytest.mark.asyncio
    async def test_segment_within_limit_yields_ids(self):
        """A segment whose count fits within _RESULT_LIMIT is fetched directly."""
        with respx.mock:
            respx.get(_ESEARCH_URL).mock(side_effect=_sequential(
                _response(3),                    # count probe
                _response(3, ["1", "2", "3"]),   # fetch page
            ))
            async with httpx.AsyncClient() as client:
                ids = [x async for x in _search_bounded(
                    "HIV", _START, _END,
                    client=client, page_size=500, requests_per_second=None,
                )]
        assert ids == ["1", "2", "3"]

    @pytest.mark.asyncio
    async def test_empty_segment_yields_nothing(self):
        """A count of zero makes no further requests and yields nothing."""
        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(return_value=_response(0))
            async with httpx.AsyncClient() as client:
                ids = [x async for x in _search_bounded(
                    "HIV", _START, _END,
                    client=client, page_size=500, requests_per_second=None,
                )]
        assert ids == []
        assert route.call_count == 1  # only the count probe

    @pytest.mark.asyncio
    async def test_oversized_segment_bisects_at_midpoint(self):
        """
        A segment over the limit is split at its date midpoint and each half
        fetched separately.

        Call sequence (5 requests):
          1. count [START, END]        → 15 000 (bisect)
          2. count [START, MID]        → 2     (fits)
          3. fetch [START, MID]
          4. count [MID+1, END]        → 2     (fits)
          5. fetch [MID+1, END]
        """
        first_ids = ["a", "b"]
        second_ids = ["c", "d"]
        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(side_effect=_sequential(
                _response(15_000),               # [START, END]   → bisect
                _response(2),                    # [START, MID]   → fits
                _response(2, first_ids),         # fetch first half
                _response(2),                    # [MID+1, END]   → fits
                _response(2, second_ids),        # fetch second half
            ))
            async with httpx.AsyncClient() as client:
                ids = [x async for x in _search_bounded(
                    "HIV", _START, _END,
                    client=client, page_size=500, requests_per_second=None,
                )]

        assert ids == first_ids + second_ids
        assert route.call_count == 5

        # Confirm the exact date ranges sent in each request
        calls = route.calls
        assert f"{_START:%Y/%m/%d}:{_END:%Y/%m/%d}[pdat]" in _params(calls[0].request)["term"]
        assert f"{_START:%Y/%m/%d}:{_MID:%Y/%m/%d}[pdat]" in _params(calls[1].request)["term"]
        assert (
            f"{_MID + timedelta(days=1):%Y/%m/%d}:{_END:%Y/%m/%d}[pdat]"
            in _params(calls[3].request)["term"]
        )

    @pytest.mark.asyncio
    async def test_recursive_bisection_when_half_still_oversized(self):
        """
        A half-segment that is itself over the limit is bisected again.

        Call sequence (8 requests):
          1. count [START,    END]     → 15 000 (bisect)
          2. count [START,    MID]     → 15 000 (bisect again)
          3. count [START,    SUB_MID] → 2      (fits)
          4. fetch [START,    SUB_MID]
          5. count [SUB_MID+1, MID]   → 2      (fits)
          6. fetch [SUB_MID+1, MID]
          7. count [MID+1,   END]     → 2      (fits)
          8. fetch [MID+1,   END]
        """
        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(side_effect=_sequential(
                _response(15_000),              # [START, END]       → bisect
                _response(15_000),              # [START, MID]       → bisect again
                _response(2),                   # [START, SUB_MID]   → fits
                _response(2, ["1", "2"]),       # fetch first quarter
                _response(2),                   # [SUB_MID+1, MID]   → fits
                _response(2, ["3", "4"]),       # fetch second quarter
                _response(2),                   # [MID+1, END]       → fits
                _response(2, ["5", "6"]),       # fetch second half
            ))
            async with httpx.AsyncClient() as client:
                ids = [x async for x in _search_bounded(
                    "HIV", _START, _END,
                    client=client, page_size=500, requests_per_second=None,
                )]

        assert ids == ["1", "2", "3", "4", "5", "6"]
        assert route.call_count == 8

    @pytest.mark.asyncio
    async def test_single_day_floor_warns_and_caps(self, caplog):
        """
        When start == end and count still exceeds the limit, a warning is logged
        and retrieval is capped at _RESULT_LIMIT.
        """
        day = date(2023, 3, 15)

        def handler(request: httpx.Request) -> httpx.Response:
            p = _params(request)
            retmax = int(p["retmax"])
            retstart = int(p.get("retstart", 0))
            if retmax == 0:
                return _response(15_000)
            return _response(15_000, [f"id_{retstart + i}" for i in range(retmax)])

        with respx.mock:
            respx.get(_ESEARCH_URL).mock(side_effect=handler)
            with caplog.at_level("WARNING", logger="apecx_harvesters.loaders.pubmed.search"):
                async with httpx.AsyncClient() as client:
                    ids = [x async for x in _search_bounded(
                        "HIV", day, day,
                        client=client, page_size=500, requests_per_second=None,
                    )]

        assert 0 < len(ids) < 15_000
        assert any("Single-day" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_multi_page_segment_paginates(self):
        """A segment within the limit that spans multiple pages yields all IDs."""
        with respx.mock:
            respx.get(_ESEARCH_URL).mock(side_effect=_sequential(
                _response(5),                # count probe
                _response(5, ["1", "2"]),    # page 1 (page_size=2)
                _response(5, ["3", "4"]),    # page 2
                _response(5, ["5"]),         # page 3
            ))
            async with httpx.AsyncClient() as client:
                ids = [x async for x in _search_bounded(
                    "HIV", _START, _END,
                    client=client, page_size=2, requests_per_second=None,
                )]
        assert ids == ["1", "2", "3", "4", "5"]


# ---------------------------------------------------------------------------
# search() — top-level routing and client lifecycle
# ---------------------------------------------------------------------------

class TestSearch:
    @pytest.mark.asyncio
    async def test_small_query_paginates_directly(self):
        """Queries within the limit are fetched without any date filter."""
        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(side_effect=_sequential(
                _response(3),                    # initial count
                _response(3, ["1", "2", "3"]),   # fetch page
            ))
            ids = [x async for x in search("HIV", requests_per_second=None)]

        assert ids == ["1", "2", "3"]
        for call in route.calls:
            assert "[pdat]" not in _params(call.request)["term"]

    @pytest.mark.asyncio
    async def test_large_query_activates_date_segmentation(self):
        """
        Queries over _RESULT_LIMIT are transparently segmented by date.
        'flavivirus' is a known example returning ~15 000 results as of 2026.
        """
        def handler(request: httpx.Request) -> httpx.Response:
            p = _params(request)
            term = p["term"]
            retmax = int(p["retmax"])
            retstart = int(p.get("retstart", 0))
            if "[pdat]" not in term:
                return _response(15_000)   # initial probe — triggers segmentation
            if retmax == 0:
                return _response(50)       # each date segment fits
            return _response(50, [f"pmid_{retstart}_{i}" for i in range(min(retmax, 50))])

        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(side_effect=handler)
            ids = [x async for x in search("flavivirus", requests_per_second=None)]

        assert len(ids) > 0
        assert all(id.startswith("pmid_") for id in ids)
        # At least some requests must have carried a date filter
        date_filtered = [
            c for c in route.calls if "[pdat]" in _params(c.request)["term"]
        ]
        assert len(date_filtered) > 0

    @pytest.mark.asyncio
    async def test_empty_query_yields_nothing(self):
        # search() always enters _fetch_ids for the ≤ limit branch; _fetch_ids makes
        # one request, sees an empty idlist, and exits — so total calls is 2.
        with respx.mock:
            route = respx.get(_ESEARCH_URL).mock(return_value=_response(0))
            ids = [x async for x in search("zzz_no_results", requests_per_second=None)]

        assert ids == []
        assert route.call_count == 2  # count probe + one fetch that returns []

    @pytest.mark.asyncio
    async def test_api_error_propagates(self):
        with respx.mock:
            respx.get(_ESEARCH_URL).mock(return_value=httpx.Response(
                200, json={"esearchresult": {"count": "0", "idlist": [], "ERROR": "Invalid term"}}
            ))
            with pytest.raises(ValueError, match="Invalid term"):
                async for _ in search("bad[term", requests_per_second=None):
                    pass