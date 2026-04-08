"""Batch search against the RCSB PDB Search API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import orjson

from .constants import rate_limit as _default_rate_limit

_SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"
_DEFAULT_PAGE_SIZE = 250


@dataclass
class SearchQuery:
    """
    A single-attribute PDB search query.

    Build one directly or use a factory method::

        SearchQuery.by_organism("Homo sapiens")
        SearchQuery.by_entity_description("kinase")
        SearchQuery.by_keyword("MEMBRANE PROTEIN")
        SearchQuery.full_text("influenza")
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


async def search(
    query: SearchQuery,
    *,
    client: httpx.AsyncClient | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requests_per_second: float | None = _default_rate_limit,
) -> AsyncIterator[str]:
    """
    Yield PDB entry IDs matching *query*, transparently paginating through all results.

    :param query: Search criteria.
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