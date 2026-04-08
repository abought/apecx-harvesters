"""
PubMed-specific schema extension.

Extends the base DataCite schema with bibliographic fields available from the
NCBI PubMed E-utilities API (efetch, db=pubmed, retmode=xml).
"""

from __future__ import annotations

from typing import Annotated, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from ..base import DataCite
from ..base.registry import SchemaRegistry


class PubMedFields(BaseModel):
    """Bibliographic metadata specific to a PubMed article record."""
    model_config = ConfigDict(strict=True, extra="forbid")

    publication_types: Annotated[list[str], Field(
        title="Publication Types",
        description="NLM publication type labels, e.g. ['Journal Article', 'Review']",
    )]


@SchemaRegistry.register
class PubMedContainer(DataCite):
    """
    A DataCite record for an article retrieved from the PubMed E-utilities API.

    Construct via the harvester::

        from apecx_harvesters.loaders.pubmed import fetch, parse
        record = fetch("33594067")

    The PMID is stored as a `RelatedIdentifier` with type ``PMID`` and
    relation ``IsIdenticalTo``.  A DOI, when present in the PubMed record, is
    added alongside it using the same relation.
    """

    _schema_title: ClassVar[str] = "PubMed article metadata"
    _schema_description: ClassVar[str] = (
        "Extends the base DataCite schema with PubMed/MEDLINE bibliographic fields."
    )

    pubmed: Annotated[PubMedFields, Field(
        title="PubMed",
        description="PubMed/MEDLINE-specific bibliographic metadata fields",
    )]

    @property
    def canonical_uri(self) -> str:
        """PMID preferred; falls back to DOI from identifier."""
        pmid = next(
            (a.alternateIdentifier for a in self.alternateIdentifiers
             if a.alternateIdentifierType == "PMID"),
            None,
        )
        if pmid:
            return f"pubmed:{pmid}"
        if self.identifier:
            return f"pubmed:{self.identifier.identifier}"
        raise ValueError("PubMedContainer has neither DOI nor PMID")
