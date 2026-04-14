"""
Search for publications and structures by author and populate the local cache.

Run aggregate_gsearch.py after this script to produce Globus Search ingest chunks.

Usage
-----
    uv run search-author --author "Firstname Lastname"
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import httpx

import apecx_harvesters.loaders  # noqa: F401  — register all harvester subclasses
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.search import pubmed_author_term, search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel


async def _run(author: str | None, orcid: str | None, institution: str | None) -> None:
    pdb_query = SearchQuery.by_author(author, orcid=orcid, institution=institution)
    term = pubmed_author_term(author, orcid=orcid)

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(client=client, requests_per_second=_PUBMED_RATE_LIMIT / 2)
        pdb = PDBHarvester(client=client, requests_per_second=_PDB_RATE_LIMIT / 2)

        await run_parallel(
            PipelineSpec(
                source=pubmed.iter_results(pubmed_search(term, client=client, requests_per_second=_PUBMED_RATE_LIMIT / 2)),
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
    parser = argparse.ArgumentParser(
        description="Search for an author across PubMed and PDB and populate the local cache."
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
            "Institution name to narrow results (e.g. 'University of Michigan'). "
            "Matched against PubMed affiliation data; entries without a linked "
            "PubMed record may be excluded."
        ),
    )
    args = parser.parse_args()
    if args.author is None and args.orcid is None:
        parser.error("At least one of --author or --orcid is required.")

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("apecx_harvesters").setLevel(logging.INFO)
    asyncio.run(_run(args.author, args.orcid, args.institution))


if __name__ == "__main__":
    main()