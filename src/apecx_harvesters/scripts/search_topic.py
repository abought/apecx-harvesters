"""
Search for publications and structures by biological entity or keyword and populate the local cache.

Run aggregate_gsearch.py after this script to produce Globus Search ingest chunks.

- PubMed: free-text term with optional publication-date range (defaults to the last 10 years).
- PDB: full-text search across all PDB fields.

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
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.search import search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel


async def _run(term: str, begin_year: int, end_year: int) -> None:
    pubmed_term = f"{term} AND {begin_year}:{end_year}[pdat]"
    pdb_query = SearchQuery.full_text(term)

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(client=client, requests_per_second=_PUBMED_RATE_LIMIT / 2)
        pdb = PDBHarvester(client=client, requests_per_second=_PDB_RATE_LIMIT / 2)

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
        )

def main() -> None:
    current_year = date.today().year
    parser = argparse.ArgumentParser(
        description=(
            "Search PubMed and PDB by biological entity or keyword "
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
        default=current_year - 10,
        metavar="YEAR",
        help="Earliest publication year for PubMed results (default: %(default)s).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=current_year,
        metavar="YEAR",
        help="Latest publication year for PubMed results (default: %(default)s).",
    )
    args = parser.parse_args()

    begin, end = args.begin_year, args.end_year
    if begin > end:
        begin, end = end, begin

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("apecx_harvesters").setLevel(logging.INFO)
    asyncio.run(_run(args.term, begin, end))


if __name__ == "__main__":
    main()