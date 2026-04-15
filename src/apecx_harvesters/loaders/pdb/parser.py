"""RCSB PDB field parsers."""

from __future__ import annotations

from typing import Any

from ..base import AlternateIdentifier, Creator, Date, DateType, Publisher
from ..base import RelatedIdentifier, RelatedIdentifierType, RelatedItem, RelatedItemIdentifier, RelatedItemType, RelationType, ResourceType, ResourceTypeGeneral, Title
from ..base.parser import deduplicate_subjects, orcid_name_identifier
from .model import PDBContainer, PDBFields, PolymerEntity, StructKeywords


def _parse_entry(entry: dict[str, Any]) -> PDBContainer:
    """Parse a single GraphQL entry dict into a ``PDBContainer``."""
    primary = entry.get("rcsb_primary_citation") or {}
    citation_items, citation_ids = _build_citation_fields(
        doi=primary.get("pdbx_database_id_DOI"),
        pmid=primary.get("pdbx_database_id_PubMed"),
        title=primary.get("title"),
        year=primary.get("year"),
    )
    pdb_id = entry["rcsb_id"]
    info = entry.get("rcsb_accession_info", {})
    release_date = info.get("initial_release_date", "")
    return PDBContainer.new(
        creators=_parse_creators(entry),
        title=entry["struct"]["title"],
        description=_make_description(entry),
        publisher=Publisher(name="RCSB PDB"),
        publicationYear=release_date[:4] if release_date else None,
        resourceType=ResourceType(resourceTypeGeneral=ResourceTypeGeneral.Dataset),
        subjects=_parse_subjects(entry),
        dates=_parse_dates(entry),
        doi=_parse_entry_doi(entry),
        alternateIdentifiers=[
            AlternateIdentifier(alternateIdentifier=pdb_id, alternateIdentifierType="PDB"),
        ],
        relatedIdentifiers=citation_ids,
        relatedItems=citation_items,
        pdb=_parse_pdb_fields(entry),
    )


def _parse_creators(data: dict[str, Any]) -> list[Creator]:
    """
    Build `Creator` objects from ``audit_author`` entries.

    Author names are ``"FamilyName, Initials"`` strings.  ORCID is captured
    when present in ``identifier_ORCID``.
    """
    creators = []
    for author in data.get("audit_author", []):
        raw: str = author["name"]
        parts = raw.split(", ", 1)
        name_identifiers = []
        if orcid := author.get("identifier_ORCID"):
            name_identifiers.append(orcid_name_identifier(orcid))
        creators.append(Creator(
            name=raw,
            familyName=parts[0],
            givenName=parts[1] if len(parts) > 1 else None,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_dates(data: dict[str, Any]) -> list[Date]:
    """Map ``rcsb_accession_info`` timestamps to ``Date`` entries."""
    info = data.get("rcsb_accession_info", {})
    dates = []
    if deposit := info.get("deposit_date"):
        dates.append(Date(date=deposit, dateType=DateType.Submitted))
    if release := info.get("initial_release_date"):
        dates.append(Date(date=release, dateType=DateType.Created))
    if revision := info.get("revision_date"):
        dates.append(Date(date=revision, dateType=DateType.Updated))
    return dates


def _parse_subjects(data: dict[str, Any]):
    """Build ``Subject`` entries from ``struct_keywords``, merging ``pdbx_keywords`` and ``text``."""
    from itertools import chain
    kws = data.get("struct_keywords") or {}
    sources = (kws.get("pdbx_keywords", ""), kws.get("text", ""))
    return deduplicate_subjects(chain.from_iterable(s.split(",") for s in sources))


def _parse_entry_doi(data: dict[str, Any]) -> str | None:
    """Extract the PDB entry DOI from ``database_2``."""
    return next(
        (db.get("pdbx_DOI") for db in data.get("database_2", []) if db.get("database_id") == "PDB"),
        None,
    )


def _build_citation_fields(
    *,
    doi: str | None,
    pmid: int | None,
    title: str | None,
    year: int | None,
) -> tuple[list[RelatedItem], list[RelatedIdentifier]]:
    """Build parent-schema citation fields from primary citation metadata."""
    related_items: list[RelatedItem] = []
    related_identifiers: list[RelatedIdentifier] = []

    if doi or title or year:
        related_items.append(RelatedItem(
            relationType=RelationType.IsDocumentedBy,
            relatedItemType=RelatedItemType.JournalArticle,
            relatedItemIdentifier=RelatedItemIdentifier(
                relatedItemIdentifier=doi,
                relatedItemIdentifierType=RelatedIdentifierType.DOI,
            ) if doi else None,
            titles=[Title(title=title)] if title else [],
            publicationYear=str(year) if year else None,
        ))

    if pmid:
        related_identifiers.append(RelatedIdentifier(
            relatedIdentifier=str(pmid),
            relatedIdentifierType=RelatedIdentifierType.PMID,
            relationType=RelationType.IsDocumentedBy,
        ))

    return related_items, related_identifiers


def _parse_polymer_entities(data: dict[str, Any]) -> list[PolymerEntity]:
    """Extract per-entity source organism from ``polymer_entities``."""
    entities = []
    for entity in data.get("polymer_entities") or []:
        organisms = entity.get("rcsb_entity_source_organism") or []
        scientific_name = organisms[0].get("scientific_name") if organisms else None
        poly = entity.get("entity_poly") or {}
        entities.append(PolymerEntity(
            entity_id=entity["rcsb_id"],
            scientific_name=scientific_name,
            polymer_type=poly.get("rcsb_entity_polymer_type"),
        ))
    return entities


def _parse_pdb_fields(data: dict[str, Any]) -> PDBFields:
    """Extract PDB-specific metadata from a GraphQL entry."""
    resolution_list = (data.get("rcsb_entry_info") or {}).get("resolution_combined")
    raw_kw = data.get("struct_keywords")
    struct_kw = StructKeywords(
        pdbx_keywords=raw_kw.get("pdbx_keywords") if raw_kw else None,
        text=raw_kw.get("text") if raw_kw else None,
    ) if raw_kw else None
    exptl = data.get("exptl") or []
    return PDBFields(
        pdb_id=data["rcsb_id"],
        method=exptl[0].get("method") if exptl else None,
        resolution_angstrom=resolution_list[0] if resolution_list else None,
        polymer_entities=_parse_polymer_entities(data),
        struct_keywords=struct_kw,
    )


def _make_description(data: dict[str, Any]) -> str:
    """
    Synthesize a description from experimental method and resolution.

    The PDB API does not provide a free-text abstract; keywords are stored
    separately in ``subjects``.
    """
    method = (data.get("exptl") or [{}])[0].get("method", "").lower().capitalize()
    resolution_list = (data.get("rcsb_entry_info") or {}).get("resolution_combined")
    resolution = resolution_list[0] if resolution_list else None

    desc = f"Structure determined by {method}"
    if resolution:
        desc += f" at {resolution} Å resolution"
    return desc + "."