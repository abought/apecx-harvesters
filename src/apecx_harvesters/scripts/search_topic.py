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
from apecx_harvesters.loaders.base import RateLimiter
from apecx_harvesters.loaders.emdb import EMDBHarvester
from apecx_harvesters.loaders.emdb.constants import rate_limit as _EMDB_RATE_LIMIT
from apecx_harvesters.loaders.emdb.search import search as emdb_search
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.constants import rate_limit_with_key as _PUBMED_RATE_LIMIT_WITH_KEY
from apecx_harvesters.loaders.pubmed.search import search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel

logger = logging.getLogger(__name__)


async def _run(
    term: str,
    begin_year: int | None,
    end_year: int | None,
    api_key: str | None,
) -> None:
    if begin_year is not None or end_year is not None:
        start = begin_year or 1800
        end = end_year or date.today().year
        pubmed_term = f"{term} AND {start}:{end}[pdat]"
    else:
        pubmed_term = term
    pdb_query = SearchQuery.full_text(term)

    pubmed_rate = _PUBMED_RATE_LIMIT_WITH_KEY if api_key is not None else _PUBMED_RATE_LIMIT
    pubmed_limiter = RateLimiter(pubmed_rate, name="pubmed")
    pdb_limiter = RateLimiter(_PDB_RATE_LIMIT, name="pdb")
    emdb_limiter = RateLimiter(_EMDB_RATE_LIMIT, name="emdb")

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(client=client, rate_limiter=pubmed_limiter, api_key=api_key)
        pdb = PDBHarvester(client=client, rate_limiter=pdb_limiter)
        emdb = EMDBHarvester(client=client, rate_limiter=emdb_limiter)

        await run_parallel(
            PipelineSpec(
                source=pubmed.iter_results(pubmed_search(pubmed_term, client=client, rate_limiter=pubmed_limiter, api_key=api_key)),
                sink=report("pubmed"),
                name="pubmed",
            ),
            PipelineSpec(
                source=pdb.iter_results(pdb_search(pdb_query, client=client, rate_limiter=pdb_limiter)),
                sink=report("pdb"),
                name="pdb",
            ),
            PipelineSpec(
                source=emdb.iter_results(emdb_search(term, client=client, rate_limiter=emdb_limiter)),
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
        default=None,
        help="Search term or biological entity name (e.g. 'SARS-CoV-2', 'influenza hemagglutinin').",
    )
    parser.add_argument(
        "--file",
        default=None,
        metavar="FILE",
        help="Text file with one search term per line. Mutually exclusive with --term.",
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
    # For now only pubmed has a key, revisit if more services need separate keys
    parser.add_argument(
        "--api-key",
        default=None,
        metavar="KEY",
        help="NCBI API key. Raises the PubMed rate limit to 10 req/s (vs 3 req/s without).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug logging (rate limiter timing, HTTP details).",
    )
    args = parser.parse_args()
    if (args.term is None) == (args.file is None):
        parser.error("Exactly one of --term or --file is required.")

    begin, end = args.begin_year, args.end_year
    if begin is not None and end is not None and begin > end:
        begin, end = end, begin

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.getLogger("apecx_harvesters").setLevel(log_level)

    if args.term:
        terms = [args.term]
    else:
        with open(args.file) as f:
            terms = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for term in terms:
        logger.info("Searching: %s", term)
        asyncio.run(_run(term, begin, end, args.api_key))


if __name__ == "__main__":
    main()