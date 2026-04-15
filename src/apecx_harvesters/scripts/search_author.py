"""
Search for publications and structures by author and populate the local cache.

Run aggregate_gsearch.py after this script to produce Globus Search ingest chunks.

Usage
-----
    uv run search-author --author "Firstname Lastname"
    uv run search-author --orcid "0000-0001-2345-6789"
    uv run search-author --author "Firstname Lastname" --orcid "0000-0001-2345-6789" --institution "MIT"
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import httpx

import apecx_harvesters.loaders  # noqa: F401  — register all harvester subclasses
from apecx_harvesters.loaders.base import RateLimiter
from apecx_harvesters.loaders.emdb import EMDBHarvester
from apecx_harvesters.loaders.emdb.constants import rate_limit as _EMDB_RATE_LIMIT
from apecx_harvesters.loaders.emdb.search import emdb_author_term, search as emdb_search
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.constants import rate_limit_with_key as _PUBMED_RATE_LIMIT_WITH_KEY
from apecx_harvesters.loaders.pubmed.search import pubmed_author_term, search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel


async def _run(
    author: str | None,
    orcid: str | None,
    institution: str | None,
    api_key: str | None,
) -> None:
    pdb_query = SearchQuery.by_author(author, orcid=orcid, institution=institution)
    pubmed_term = pubmed_author_term(author, orcid=orcid)
    emdb_term = emdb_author_term(author, orcid=orcid) if (author is not None or orcid is not None) else None

    pubmed_rate = _PUBMED_RATE_LIMIT_WITH_KEY if api_key is not None else _PUBMED_RATE_LIMIT
    pubmed_limiter = RateLimiter(pubmed_rate, name="pubmed")
    pdb_limiter = RateLimiter(_PDB_RATE_LIMIT, name="pdb")
    emdb_limiter = RateLimiter(_EMDB_RATE_LIMIT, name="emdb")

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(client=client, rate_limiter=pubmed_limiter, api_key=api_key)
        pdb = PDBHarvester(client=client, rate_limiter=pdb_limiter)
        emdb = EMDBHarvester(client=client, rate_limiter=emdb_limiter)

        specs = [
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
        ]
        if emdb_term is not None:
            specs.append(PipelineSpec(
                source=emdb.iter_results(emdb_search(emdb_term, client=client, rate_limiter=emdb_limiter)),
                sink=report("emdb"),
                name="emdb",
            ))

        await run_parallel(*specs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search for an author across PubMed, PDB, and EMDB and populate the local cache."
    )
    parser.add_argument(
        "--author",
        default=None,
        help="Author name. Accepted formats: 'Jane Smith', 'Smith, Jane', 'J. Smith'.",
    )
    parser.add_argument(
        "--orcid",
        default=None,
        metavar="ORCID",
        help=(
            "Author ORCID (e.g. 0000-0002-1234-5678). OR'd with name variants so that "
            "records predating ORCID adoption are still retrieved via name matching."
        ),
    )
    parser.add_argument(
        "--institution",
        default=None,
        metavar="NAME",
        help=(
            "Institution name to narrow PDB and PubMed results (e.g. 'University of Michigan'). "
            "Matched against PubMed affiliation data; entries without a linked "
            "PubMed record may be excluded. Not supported for EMDB."
        ),
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
    if args.author is None and args.orcid is None:
        parser.error("At least one of --author or --orcid is required.")

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.getLogger("apecx_harvesters").setLevel(log_level)

    asyncio.run(_run(args.author, args.orcid, args.institution, args.api_key))


if __name__ == "__main__":
    main()