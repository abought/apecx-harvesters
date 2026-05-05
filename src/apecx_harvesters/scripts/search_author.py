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

import sys
import argparse
import asyncio
import logging
import pathlib
import httpx

import apecx_harvesters.loaders  # noqa: F401  — register all harvester subclasses
from apecx_harvesters.loaders.base import RateLimiter
from apecx_harvesters.loaders.emdb import EMDBHarvester
from apecx_harvesters.loaders.emdb.constants import rate_limit as _EMDB_RATE_LIMIT
from apecx_harvesters.loaders.emdb.search import count as emdb_count
from apecx_harvesters.loaders.emdb.search import emdb_author_term, search as emdb_search
from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.constants import rate_limit as _PDB_RATE_LIMIT
from apecx_harvesters.loaders.pdb.search import SearchQuery
from apecx_harvesters.loaders.pdb.search import count as pdb_count
from apecx_harvesters.loaders.pdb.search import search as pdb_search
from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.pubmed.constants import rate_limit as _PUBMED_RATE_LIMIT
from apecx_harvesters.loaders.pubmed.constants import rate_limit_with_key as _PUBMED_RATE_LIMIT_WITH_KEY
from apecx_harvesters.loaders.pubmed.search import count as pubmed_count
from apecx_harvesters.loaders.pubmed.search import pubmed_author_term, search as pubmed_search
from apecx_harvesters.pipeline import PipelineSpec, report, run_parallel

logger = logging.getLogger(__name__)


def _optional_field(value: str) -> str | None:
    value = value.strip()
    if value in {"", '""'}:
        return None
    return value


def _parse_author_file_line(line: str) -> tuple[str | None, str | None, str | None]:
    parts = line.rstrip("\n\r").split("\t")
    if len(parts) == 1:
        author = _optional_field(parts[0])
        orcid = None
        institution = None
    elif len(parts) == 2:
        author = _optional_field(parts[0])
        orcid = _optional_field(parts[1])
        institution = None
    elif len(parts) == 3:
        author = _optional_field(parts[0])
        orcid = _optional_field(parts[1])
        institution = _optional_field(parts[2])
    else:
        raise ValueError(
            f"Expected 1, 2, or 3 tab-separated columns, got {len(parts)}"
        )

    if author is None and orcid is None:
        raise ValueError("Each row must provide at least name or orcid")

    return author, orcid, institution


async def _count_results(
        author: str | None,
        orcid: str | None,
        institution: str | None,
        api_key: str | None,
) -> None:
    pdb_query = SearchQuery.by_author(author, orcid=orcid, institution=institution)
    pubmed_term = pubmed_author_term(author, orcid=orcid)
    emdb_term = emdb_author_term(author, orcid=orcid) if (author is not None or orcid is not None) else None

    tasks = [
        pubmed_count(pubmed_term, api_key=api_key),
        pdb_count(pdb_query),
    ]
    if emdb_term is not None:
        tasks.append(emdb_count(emdb_term))
    results = await asyncio.gather(*tasks)

    pubmed_n, pdb_n = results[0], results[1]
    emdb_n = results[2] if emdb_term is not None else None

    logger.debug(f"  pubmed: {pubmed_n:,}")
    logger.debug(f"     pdb: {pdb_n:,}")
    if emdb_n is not None:
        logger.debug(f"    emdb: {emdb_n:,}")


async def _run(
        author: str | None,
        orcid: str | None,
        institution: str | None,
        api_key: str | None,
        cache_root: pathlib.Path,
) -> None:
    logger.debug("Author: %s", author)
    logger.debug("ORCID: %s", orcid)
    logger.debug("Institution: %s", institution)

    pdb_query = SearchQuery.by_author(author, orcid=orcid, institution=institution)
    pubmed_term = pubmed_author_term(author, orcid=orcid)
    emdb_term = emdb_author_term(author, orcid=orcid) if (author is not None or orcid is not None) else None

    logger.debug("PubMed Term: %s", pubmed_term)
    logger.debug("PDB Term: %s", pdb_query)
    logger.debug("EMDB Term: %s", emdb_term)

    pubmed_rate = _PUBMED_RATE_LIMIT_WITH_KEY if api_key is not None else _PUBMED_RATE_LIMIT
    pubmed_limiter = RateLimiter(pubmed_rate, name="pubmed")
    pdb_limiter = RateLimiter(_PDB_RATE_LIMIT, name="pdb")
    emdb_limiter = RateLimiter(_EMDB_RATE_LIMIT, name="emdb")

    async with httpx.AsyncClient() as client:
        pubmed = PubMedHarvester(
            client=client,
            rate_limiter=pubmed_limiter,
            api_key=api_key,
            cache_root=cache_root,
        )
        pdb = PDBHarvester(client=client, rate_limiter=pdb_limiter, cache_root=cache_root)
        emdb = EMDBHarvester(client=client, rate_limiter=emdb_limiter, cache_root=cache_root)

        specs = [
            PipelineSpec(
                source=pubmed.iter_results(
                    pubmed_search(pubmed_term, client=client, rate_limiter=pubmed_limiter, api_key=api_key)
                ),
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
        "--file",
        default=None,
        metavar="FILE",
        help=(
            "Tab-separated file with 1, 2, or 3 columns: name; name and orcid; "
            "or name, orcid, and institution. Use empty fields or \"\" for missing values. "
            "Mutually exclusive with --author/--orcid."
        ),
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
        "--dry-run",
        action="store_true",
        default=False,
        help="Report the number of matches per source without scraping any records.",
    )
    parser.add_argument(
        "--output",
        "--cache-root",
        dest="cache_root",
        default=".cache",
        metavar="DIR",
        help="Directory where fetched records are cached (default: %(default)s).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug logging (rate limiter timing, HTTP details).",
    )

    args = parser.parse_args()

    using_file = args.file is not None
    using_single = args.author is not None or args.orcid is not None
    if using_file and using_single:
        parser.error("--file is mutually exclusive with --author/--orcid.")
    if not using_file and not using_single:
        parser.error("At least one of --author, --orcid, or --file is required.")
    cache_root = pathlib.Path(args.cache_root)
    try:
        cache_root.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Cache folder cannot be created: {e}")
        sys.exit(1)

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logger.setLevel(log_level)
    logging.getLogger("apecx_harvesters").setLevel(log_level)

    if using_file:
        with open(args.file) as f:
            queries = []
            for lineno, line in enumerate(f, start=1):
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                try:
                    queries.append(_parse_author_file_line(line))
                except ValueError as exc:
                    parser.error(f"{args.file}:{lineno}: {exc}")

        for author, orcid, institution in queries:
            label = author or orcid or ""
            if institution:
                label = f"{label} [{institution}]"
            logger.debug(label)
            if args.dry_run:
                asyncio.run(_count_results(author, orcid, institution, args.api_key))
            else:
                logger.info(
                    "Searching author query: author=%r orcid=%r institution=%r",
                    author,
                    orcid,
                    institution,
                )
                asyncio.run(_run(author, orcid, institution, args.api_key, cache_root))
    else:
        if args.dry_run:
            label = args.author or args.orcid or ""
            logger.debug(label)
            asyncio.run(_count_results(args.author, args.orcid, args.institution, args.api_key))
        else:
            asyncio.run(_run(args.author, args.orcid, args.institution, args.api_key, cache_root))


if __name__ == "__main__":
    main()
