"""DOI dispatcher harvester."""

from __future__ import annotations

import asyncio
import gzip
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import ClassVar

import httpx

from .. import crossref, datacite, openalex
from ..base import BaseHarvester, RetrievalResult
from ..base import DataCite
from .constants import rate_limit

_RA_API = "https://doi.org/ra"

# doi.org/ra accepts comma-separated DOIs in a single GET request.
_RA_BATCH_SIZE = 100


def _resolve(ra: str) -> type[BaseHarvester[DataCite]]:
    """Return the harvester class for *ra*; falls back to OpenAlex for unknown agencies."""
    if ra == "Crossref":
        return crossref.CrossrefHarvester
    elif ra == "DataCite":
        return datacite.DataCiteHarvester
    else:
        return openalex.OpenAlexHarvester


class DOIHarvester(BaseHarvester[DataCite]):
    """
    Fetch an item by DOI. An internal quirk is that there are multiple independent DOI registries.

    This will find and delegate to the registered authority first, but fall back to a general service (OpenAlex)
        if the DOI came from a specialist registry.
    """
    _BATCH_SIZE = _RA_BATCH_SIZE
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    def _normalize_id(self, id_: str) -> str:
        return id_.lower()

    async def _cache_path(self, id_: str) -> Path:
        safe = re.sub(r'[/\\:*?"<>|\s]', "_", id_)
        return self._cache_root / "doi" / f"{safe}.txt.gz"

    async def _parse_item(self, content: str) -> DataCite:
        # iter_results is fully handled by the base class; parsing is delegated to specialist harvesters.
        raise NotImplementedError

    async def _iter_chunk(self, chunk: list[str]) -> AsyncIterator[RetrievalResult[DataCite]]:
        """Resolve RAs for *chunk*, then delegate per-RA to specialist ``iter_results``."""
        ra_map: dict[str, str] = {}
        unknown: list[str] = []

        for doi in chunk:
            if self._use_cache:
                cache_path = await self._cache_path(doi)
                if cache_path.exists():
                    try:
                        with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                            ra = f.read().strip()
                        if ra:
                            ra_map[doi] = ra
                            continue
                    except Exception:
                        pass  # corrupt RA cache → re-lookup
            unknown.append(doi)

        ra_errors: dict[str, str] = {}
        if unknown:
            try:
                assert self._client is not None
                response = await self._client.get(f"{_RA_API}/{','.join(unknown)}")
                response.raise_for_status()
                if self._requests_per_second is not None:
                    await asyncio.sleep(1.0 / self._requests_per_second)
                for entry in response.json():
                    doi = entry.get("DOI", "").lower()
                    ra = entry.get("RA")
                    if doi and ra:
                        ra_map[doi] = ra
                        if self._use_cache:
                            await self._cache_save(await self._cache_path(doi), ra)
                    elif doi:
                        ra_errors[doi] = entry.get("status", "RA not found")
            except Exception as exc:
                for doi in unknown:
                    ra_errors[doi] = str(exc)

        by_ra: dict[str, list[str]] = {}
        for doi in chunk:
            if doi in ra_map:
                by_ra.setdefault(ra_map[doi], []).append(doi)

        # Results are yielded grouped by RA rather than in input order.
        for ra, group in by_ra.items():
            specialist = _resolve(ra)(
                client=self._client,
                use_cache=self._use_cache,
                cache_root=self._cache_root,
            )
            async for result in specialist.iter_results(group):
                yield result

        for doi in chunk:
            if doi in ra_errors:
                yield RetrievalResult(id=doi, error=ra_errors[doi])
            elif doi not in ra_map:
                yield RetrievalResult(id=doi, error="not returned by API")


async def _lookup_ra(doi: str, client: httpx.AsyncClient) -> str:
    response = await client.get(f"{_RA_API}/{doi}")
    response.raise_for_status()
    data = response.json()
    if not data or "RA" not in data[0]:
        raise ValueError(f"Could not determine registration agency for DOI: {doi!r}")
    return data[0]["RA"]