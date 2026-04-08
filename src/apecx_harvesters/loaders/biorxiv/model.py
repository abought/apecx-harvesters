"""
bioRxiv/medRxiv-specific schema extension.

Extends the base DataCite schema with fields specific to preprint records
retrieved from the bioRxiv/medRxiv API.
"""

from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..base import DataCite
from ..base.registry import SchemaRegistry


class BiorXivFields(BaseModel):
    """Domain-specific metadata for a bioRxiv or medRxiv preprint."""
    model_config = ConfigDict(strict=True, extra="forbid")

    server: Annotated[str, Field(
        title="Server",
        description="Preprint server: 'bioRxiv' or 'medRxiv'",
    )]
    publication_type: Annotated[str, Field(
        title="Publication Type",
        description="Publication status, e.g. 'PUBLISHAHEADOFPRINT'",
    )]
    jats_xml_url: Annotated[Optional[str], Field(
        title="JATS XML URL",
        description="URL to the JATS XML source for the latest version",
    )] = None


@SchemaRegistry.register
class BiorXivContainer(DataCite):
    """
    A bioRxiv or medRxiv preprint. Since these are captured via CrossRef APIs, this specialist
        type may not be used much in practice.
    """

    _schema_title: ClassVar[str] = "bioRxiv/medRxiv preprint metadata"
    _schema_description: ClassVar[str] = (
        "Extends the base DataCite schema with bioRxiv/medRxiv-specific fields."
    )

    biorxiv: Annotated[BiorXivFields, Field(
        title="bioRxiv",
        description="bioRxiv/medRxiv-specific metadata fields",
    )]
