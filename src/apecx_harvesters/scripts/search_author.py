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
from apecx_harvesters.loaders.pubmed.search import search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel


async def _run(author: str) -> None:
    family_name = author.split()[0] if " " in author else author
    pdb_query = SearchQuery(
        value=family_name,
        attribute="audit_author.name",
        operator="contains_words",
    )
    term = f'"{author}"[Author]'

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
        required=True,
        help="Author name to search for.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("apecx_harvesters").setLevel(logging.INFO)
    asyncio.run(_run(args.author))


if __name__ == "__main__":
    main()