"""RCSB PDB harvester — GraphQL multi-record retrieval."""

from __future__ import annotations

import orjson
from typing import ClassVar

from ..base import BaseHarvester
from .constants import rate_limit
from .model import PDBContainer
from .parser import _parse_entry

_GRAPHQL_URL = "https://data.rcsb.org/graphql"

# RCSB does not publish a hard limit for entries(entry_ids:[...]), but their
# own Python library chunks requests and their docs warn that large batches are
# "resource intensive". 200 is a conservative default; raise if needed.
_GRAPHQL_BATCH_SIZE = 200

# Additional data available via GraphQL not captured by the REST /core/entry endpoint:
#   audit_author.identifier_ORCID  — author ORCID
#   rcsb_primary_citation          — primary citation (vs citation[] filtered for rcsb_is_primary="Y")
_ENTRIES_QUERY = """
query($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    struct { title }
    audit_author { name identifier_ORCID }
    exptl { method }
    rcsb_entry_info { resolution_combined }
    struct_keywords { pdbx_keywords text }
    rcsb_accession_info {
      deposit_date
      initial_release_date
      revision_date
    }
    rcsb_primary_citation {
      pdbx_database_id_DOI
      pdbx_database_id_PubMed
      title
      year
    }
    database_2 { database_id pdbx_DOI }
    polymer_entities {
      rcsb_id
      entity_poly { rcsb_entity_polymer_type }
      rcsb_entity_source_organism { scientific_name }
    }
  }
}
"""


class PDBHarvester(BaseHarvester[PDBContainer]):
    _BATCH_SIZE: ClassVar[int] = _GRAPHQL_BATCH_SIZE
    _CACHE_DIR = "pdb"
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = rate_limit

    def _normalize_id(self, id_: str) -> str:
        return id_.upper()

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        body = orjson.dumps({"query": _ENTRIES_QUERY, "variables": {"ids": ids}}).decode()
        return _GRAPHQL_URL, body, {"Content-Type": "application/json"}

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        """Split a GraphQL entries response into per-ID raw entry strings."""
        data = orjson.loads(content)
        return {
            entry["rcsb_id"]: orjson.dumps(entry).decode()
            for entry in (data["data"]["entries"] or [])
            if entry is not None
        }

    async def _parse_item(self, content: str) -> PDBContainer:
        return _parse_entry(orjson.loads(content))