"""
EMDB-specific schema extension.

Extends the base DataCite schema with fields specific to Electron Microscopy
Data Bank entries retrieved from the EMDB REST API
(https://www.ebi.ac.uk/emdb/api/entry/{id}).
"""

from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..base import DataCite
from ..base.registry import SchemaRegistry


class EMDBFields(BaseModel):
    """Domain-specific metadata for an EMDB structure entry."""
    model_config = ConfigDict(strict=True, extra="forbid")

    emdb_id: Annotated[str, Field(
        title="EMDB ID",
        description="EMDB accession code, e.g. 'EMD-74041'",
    )]
    method: Annotated[str, Field(
        title="Experimental Method",
        description=(
            "Structure determination method, e.g. 'singleParticle', "
            "'subtomogramAveraging', 'helical', 'tomography'"
        ),
    )]
    resolution_angstrom: Annotated[Optional[float], Field(
        title="Resolution (\u212b)",
        description="Map resolution in \u00e5ngstr\u00f6ms as reported by the authors",
    )] = None
    resolution_method: Annotated[Optional[str], Field(
        title="Resolution Method",
        description="Method used to estimate resolution, e.g. 'FSC 0.143 CUT-OFF'",
    )] = None


@SchemaRegistry.register
class EMDBContainer(DataCite):
    _schema_title: ClassVar[str] = "EMDB structure metadata"
    _schema_description: ClassVar[str] = (
        "Extends the base DataCite schema with cryo-EM-specific fields "
        "from the Electron Microscopy Data Bank REST API."
    )

    emdb: Annotated[EMDBFields, Field(
        title="EMDB",
        description="EMDB-specific structure metadata fields",
    )]

    @property
    def canonical_uri(self) -> str:
        return f"emdb:{self.emdb.emdb_id}"
