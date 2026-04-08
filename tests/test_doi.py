"""
Unit tests for the DOI dispatcher harvester.

The dispatcher queries doi.org/ra/{doi} to determine the Registration Agency,
then delegates to the appropriate specialist harvester.  Network calls in
_lookup_ra are mocked; routing is verified via _resolve without any mocking.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from apecx_harvesters.loaders.crossref import CrossrefHarvester
from apecx_harvesters.loaders.datacite import DataCiteHarvester
from apecx_harvesters.loaders.doi.retrieve import _lookup_ra, _resolve
from apecx_harvesters.loaders.openalex import OpenAlexHarvester


def _mock_client(ra_response: list | None = None) -> AsyncMock:
    """Return an AsyncClient mock whose get() returns a canned RA response."""
    mock_response = MagicMock()
    if ra_response is not None:
        mock_response.json.return_value = ra_response
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.return_value = mock_response
    return client


class TestLookupRA:
    @pytest.mark.asyncio
    async def test_returns_ra_name(self):
        client = _mock_client([{"DOI": "10.1038/test", "RA": "Crossref"}])
        ra = await _lookup_ra("10.1038/test", client)
        assert ra == "Crossref"


class TestResolve:
    def test_crossref_ra(self):
        assert _resolve("Crossref") is CrossrefHarvester

    def test_datacite_ra(self):
        assert _resolve("DataCite") is DataCiteHarvester

    def test_unknown_ra_falls_back_to_openalex(self):
        for ra in ("mEDRA", "JALC", "ISTIC", "op.europa.eu"):
            assert _resolve(ra) is OpenAlexHarvester
