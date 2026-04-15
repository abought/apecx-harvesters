"""
PDB-specific schema extension.

Extends the base DataCite schema with fields specific to Protein Data Bank
structure entries retrieved from the RCSB PDB REST API.
"""

from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..base import DataCite
from ..base.registry import SchemaRegistry


class PolymerEntity(BaseModel):
    """A polymer entity within a PDB structure, with its source organism."""
    model_config = ConfigDict(strict=True, extra="forbid")

    entity_id: Annotated[str, Field(
        title="Entity ID",
        description="RCSB entity identifier, e.g. '6M0J_1'",
    )]
    scientific_name: Annotated[Optional[str], Field(
        title="Scientific Name",
        description="Source organism scientific name from rcsb_entity_source_organism. "
                    "None when the organism is not recorded (e.g. synthetic constructs).",
    )] = None
    polymer_type: Annotated[Optional[str], Field(
        title="Polymer Type",
        description="Broad polymer class from entity_poly.rcsb_entity_polymer_type, "
                    "e.g. 'Protein', 'DNA', 'RNA', 'NA-hybrid', 'Other'.",
    )] = None


class StructKeywords(BaseModel):
    """Raw ``_struct_keywords`` fields from the PDB API, preserved for harmonization."""
    model_config = ConfigDict(strict=True, extra="forbid")

    pdbx_keywords: Annotated[Optional[str], Field(
        title="Molecule Class",
        description="RCSB-assigned classification term (controlled vocabulary), e.g. 'TRANSFERASE'.",
    )] = None
    text: Annotated[Optional[str], Field(
        title="Depositor Keywords",
        description="Comma-separated keywords supplied by the depositor at submission.",
    )] = None


class PDBFields(BaseModel):
    """Domain-specific metadata for a PDB structure entry."""
    model_config = ConfigDict(strict=True, extra="forbid")

    pdb_id: Annotated[str, Field(
        title="PDB ID",
        description="RCSB PDB accession code, e.g. '1OMW'",
    )]
    method: Annotated[Optional[str], Field(
        title="Experimental Method",
        description="Experimental technique used to determine the structure, e.g. 'X-RAY DIFFRACTION'",
    )] = None
    resolution_angstrom: Annotated[Optional[float], Field(
        title="Resolution (Å)",
        description="Diffraction resolution in ångströms; absent for NMR and other non-diffraction methods",
    )] = None
    polymer_entities: Annotated[list[PolymerEntity], Field(
        title="Polymer Entities",
        description="Source organisms per polymer entity, preserving the entity-organism association. "
                    "# TODO: this is a more complex model and domain experts should evaluate full representation",
    )] = Field(default_factory=list)
    struct_keywords: Annotated[Optional[StructKeywords], Field(
        title="Structure Keywords",
        description="Raw keyword fields from the PDB API (mmCIF _struct_keywords).",
    )] = None



@SchemaRegistry.register
class PDBContainer(DataCite):
    """
    Represents a single PDB entry from the RCSB API (`/core/entry/`).
    """

    _schema_title: ClassVar[str] = "PDB Structure metadata"
    _schema_description: ClassVar[str] = "Extends the base DataCite schema with PDB-specific fields."

    pdb: Annotated[PDBFields, Field(
        title="PDB",
        description="PDB-specific metadata fields",
    )]

    @property
    def canonical_uri(self) -> str:
        return f"pdb:{self.pdb.pdb_id}"
