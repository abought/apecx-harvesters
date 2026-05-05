"""IEDB metadata model."""

from __future__ import annotations

import urllib.parse

from pydantic import BaseModel, ConfigDict, Field

from apecx_harvesters.loaders.base import DataCite


class IEDBEpitopeRow(BaseModel):
    """One row from the IEDB epitope_export endpoint."""

    model_config = ConfigDict(strict=True, extra="allow")

    structure_id: int | None = None
    epitope_id__iedb_iri: str | None = None
    epitope__object_type: str | None = None
    epitope__name: str | None = None
    epitope__modified_residues: str | None = None
    epitope__modifications: str | None = None
    epitope__starting_position: int | None = None
    epitope__ending_position: int | None = None
    epitope__iri: str | None = None
    epitope__synonyms: str | None = None
    epitope__source_molecule: str | None = None
    epitope__source_molecule_iri: str | None = None
    epitope__molecule_parent: str | None = None
    epitope__molecule_parent_iri: str | None = None
    epitope__source_organism: str | None = None
    epitope__source_organism_iri: str | None = None
    epitope__species: str | None = None
    epitope__species_iri: str | None = None

    related_object__epitope_relation: str | None = None
    related_object__object_type: str | None = None
    related_object__name: str | None = None
    related_object__starting_position: int | None = None
    related_object__ending_position: int | None = None
    related_object__iri: str | None = None
    related_object__synonyms: str | None = None
    related_object__source_molecule: str | None = None
    related_object__source_molecule_iri: str | None = None
    related_object__molecule_parent: str | None = None
    related_object__molecule_parent_iri: str | None = None
    related_object__source_organism: str | None = None
    related_object__source_organism_iri: str | None = None
    related_object__species: str | None = None
    related_object__species_iri: str | None = None


class IEDBFields(BaseModel):
    """IEDB-specific payload stored inside the common DataCite container."""

    model_config = ConfigDict(strict=True, extra="forbid")

    query: str
    row_count: int
    rows: list[IEDBEpitopeRow] = Field(default_factory=list)


class IEDBContainer(DataCite):
    """DataCite-compatible container for IEDB epitope export data."""

    iedb: IEDBFields

    @property
    def canonical_uri(self) -> str:
        query = urllib.parse.quote(self.iedb.query, safe="")
        return f"iedb:{query}"
