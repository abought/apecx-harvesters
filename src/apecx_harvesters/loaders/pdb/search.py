"""Batch search against the RCSB PDB Search API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import orjson

from ..base.parser import parse_author_name as _parse_author_name
from .constants import rate_limit as _default_rate_limit

_SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"
_DEFAULT_PAGE_SIZE = 250


def _author_name_nodes(family: str, given: str | None) -> list[SearchQuery]:
    """
    Return ``SearchQuery`` terminal nodes covering all expected PDB name variants.

    PDB stores author names as ``"Smith, Jane"`` or ``"Smith, J."``.  When a
    full given name is available, both the full form and the initial form are
    included so that either storage convention is matched.
    """
    if given is None:
        return [SearchQuery(value=family, attribute="audit_author.name", operator="contains_words")]

    initial = given[0]
    nodes: list[SearchQuery] = []

    if len(given) > 1:
        # Full given name — search for the full "Smith, Jane" form
        nodes.append(SearchQuery(
            value=f"{family}, {given}",
            attribute="audit_author.name",
            operator="contains_phrase",
        ))

    # Always include the initial form "Smith, J" to catch abbreviated entries
    nodes.append(SearchQuery(
        value=f"{family}, {initial}",
        attribute="audit_author.name",
        operator="contains_phrase",
    ))
    return nodes


@dataclass
class GroupQuery:
    """
    A boolean combination of ``SearchQuery`` or nested ``GroupQuery`` nodes.

    Example — AND two conditions::

        GroupQuery([query_a, query_b], logical_operator="and")
    """

    nodes: list[SearchQuery | GroupQuery]
    logical_operator: str = "and"

    def _to_node(self) -> dict[str, Any]:
        return {
            "type": "group",
            "logical_operator": self.logical_operator,
            "nodes": [n._to_node() for n in self.nodes],
        }


@dataclass
class SearchQuery:
    """
    A single-attribute PDB search query.

    Build one directly or use a factory method::

        SearchQuery.by_organism("Homo sapiens")
        SearchQuery.by_entity_description("kinase")
        SearchQuery.by_keyword("MEMBRANE PROTEIN")
        SearchQuery.full_text("influenza")
        SearchQuery.by_author("Jane Smith", orcid="0000-0002-1234-5678", institution="MIT")
    """

    value: str | list[str]
    attribute: str = ""
    operator: str = "exact_match"
    service: str = "text"

    def _to_node(self) -> dict[str, Any]:
        if self.service == "full_text":
            return {
                "type": "terminal",
                "service": "full_text",
                "parameters": {"value": self.value},
            }
        return {
            "type": "terminal",
            "service": self.service,
            "parameters": {
                "attribute": self.attribute,
                "value": self.value,
                "operator": self.operator,
            },
        }

    @classmethod
    def full_text(cls, value: str) -> SearchQuery:
        """Search across all PDB text fields (title, keywords, entity description, organism, etc.)."""
        return cls(value=value, service="full_text")

    @classmethod
    def by_organism(cls, name: str) -> SearchQuery:
        """Match entries whose source organism equals *name* (e.g. ``"Homo sapiens"``)."""
        return cls(value=name, attribute="rcsb_entity_source_organism.organism_scientific_name")

    @classmethod
    def by_entity_description(cls, description: str) -> SearchQuery:
        """Full-text search against the polymer entity description field."""
        return cls(
            value=description,
            attribute="rcsb_polymer_entity.pdbx_description",
            operator="contains_words",
        )

    @classmethod
    def by_keyword(cls, keyword: str) -> SearchQuery:
        """Full-text search against the entry keyword field."""
        return cls(
            value=keyword,
            attribute="struct_keywords.pdbx_keywords",
            operator="contains_words",
        )

    @classmethod
    def by_author(
        cls,
        name: str | None = None,
        *,
        orcid: str | None = None,
        institution: str | None = None,
    ) -> SearchQuery | GroupQuery:
        """
        Build a query for a PDB depositing author, with optional ORCID and
        institution filters.  At least one of *name* or *orcid* must be given.

        *name* is accepted in any of these formats::

            "Jane Smith"  /  "Smith, Jane"  /  "J. Smith"  /  "Smith"

        When a full given name is available, both ``"Smith, Jane"`` and
        ``"Smith, J"`` are searched (OR) to account for variant storage
        conventions across PDB records.

        When only *orcid* is supplied, the query matches exclusively on
        ``audit_author.identifier_ORCID`` — useful for precise lookups where
        name ambiguity must be avoided.  When both are supplied, the ORCID and
        name variants are OR'd so that records predating ORCID adoption are
        still captured.

        *institution* is AND'd against ``rcsb_pubmed_affiliation_info``, which
        is populated from the linked PubMed record.  Entries without a PubMed
        link may lack this field and will be excluded by this filter.
        """
        if name is None and orcid is None:
            raise ValueError("At least one of 'name' or 'orcid' must be provided.")

        identity_nodes: list[SearchQuery | GroupQuery] = (
            list(_author_name_nodes(*_parse_author_name(name))) if name is not None else []
        )

        if orcid is not None:
            orcid_clean = orcid.removeprefix("https://orcid.org/").strip()
            identity_nodes.insert(0, SearchQuery(
                value=orcid_clean,
                attribute="audit_author.identifier_ORCID",
                operator="exact_match",
            ))

        identity: SearchQuery | GroupQuery = (
            identity_nodes[0] if len(identity_nodes) == 1
            else GroupQuery(identity_nodes, logical_operator="or")
        )

        if institution is None:
            return identity

        return GroupQuery(
            [
                identity,
                SearchQuery(
                    value=institution,
                    attribute="rcsb_pubmed_affiliation_info",
                    operator="contains_words",
                ),
            ],
            logical_operator="and",
        )


async def search(
    query: SearchQuery | GroupQuery,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requests_per_second: float | None = _default_rate_limit,
) -> AsyncIterator[str]:
    """
    Yield PDB entry IDs matching *query*, transparently paginating through all results.

    :param query: Search criteria — a ``SearchQuery`` or a ``GroupQuery`` combining multiple conditions.
    :param client: Optional shared HTTP client. A new one is created (and
        closed) if not provided.
    :param page_size: Results per page (max 10,000; smaller values are more
        resilient to timeouts on large result sets).
    :param requests_per_second: Maximum request rate. RCSB does not publish a hard limit;
        pass a lower value when sharing the budget with concurrent retrieval.
    """
    owned = client is None
    if owned:
        client = httpx.AsyncClient()

    try:
        start = 0
        while True:
            payload = {
                "return_type": "entry",
                "query": query._to_node(),
                "request_options": {
                    "paginate": {"start": start, "rows": page_size},
                },
            }
            response = await client.post(
                _SEARCH_URL,
                content=orjson.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            if requests_per_second is not None:
                await asyncio.sleep(1.0 / requests_per_second)
            data = orjson.loads(response.content)

            results = data.get("result_set") or []
            for item in results:
                yield item["identifier"]

            start += len(results)
            if not results or start >= data.get("total_count", 0):
                break
    finally:
        if owned:
            await client.aclose()