"""
Search for publications and structures by biological entity or keyword and populate the local cache.

Run aggregate_gsearch.py after this script to produce Globus Search ingest chunks.

Usage
-----
    uv run search-topic --term "SARS-CoV-2"
    uv run search-topic --term "influenza" --begin-year 2015 --end-year 2020
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date

import httpx

import apecx_harvesters.loaders  # noqa: F401  — register all harvester subclasses
from apecx_harvesters.loaders.emdb import EMDBHarvester
from apecx_harvesters.loaders.emdb.constants import rate_limit as _EMDB_RATE_LIMIT
from apecx_harvesters.loaders.emdb.search import search as emdb_search
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.search import search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel


async def _run(term: str, begin_year: int | None, end_year: int | None) -> None:
    if begin_year is not None or end_year is not None:
        start = begin_year or 1800
        end = end_year or date.today().year
        pubmed_term = f"{term} AND {start}:{end}[pdat]"
    else:
        pubmed_term = term
    pdb_query = SearchQuery.full_text(term)

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(client=client, requests_per_second=_PUBMED_RATE_LIMIT / 2)
        pdb = PDBHarvester(client=client, requests_per_second=_PDB_RATE_LIMIT / 2)
        emdb = EMDBHarvester(client=client, requests_per_second=_EMDB_RATE_LIMIT / 2)

        await run_parallel(
            PipelineSpec(
                source=pubmed.iter_results(pubmed_search(pubmed_term, client=client, requests_per_second=_PUBMED_RATE_LIMIT / 2)),
                sink=report("pubmed"),
                name="pubmed",
            ),
            PipelineSpec(
                source=pdb.iter_results(pdb_search(pdb_query, client=client, requests_per_second=_PDB_RATE_LIMIT / 2)),
                sink=report("pdb"),
                name="pdb",
            ),
            PipelineSpec(
                source=emdb.iter_results(emdb_search(term, client=client, requests_per_second=_EMDB_RATE_LIMIT / 2)),
                sink=report("emdb"),
                name="emdb",
            ),
        )

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Search PubMed, PDB, and EMDB by biological entity or keyword "
            "and populate the local cache."
        )
    )
    parser.add_argument(
        "--term",
        required=True,
        help="Search term or biological entity name (e.g. 'SARS-CoV-2', 'influenza hemagglutinin').",
    )
    parser.add_argument(
        "--begin-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Earliest publication year for PubMed results. Omit to search all years.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Latest publication year for PubMed results. Omit to search all years.",
    )
    args = parser.parse_args()

    begin, end = args.begin_year, args.end_year
    if begin is not None and end is not None and begin > end:
        begin, end = end, begin

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("apecx_harvesters").setLevel(logging.INFO)
    asyncio.run(_run(args.term, begin, end))


if __name__ == "__main__":
    main()