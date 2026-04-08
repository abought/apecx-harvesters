"""EMDB field parsers."""

from __future__ import annotations

from typing import Any

from ..base import (
    AlternateIdentifier,
    Creator,
    Date,
    DateType,
    FundingReference,
    Publisher,
    RelatedIdentifier,
    RelatedIdentifierType,
    RelationType,
    ResourceType,
    ResourceTypeGeneral,
)
from ..base.parser import deduplicate_subjects, orcid_name_identifier
from .model import EMDBContainer, EMDBFields

_PUBLISHER = Publisher(name="Electron Microscopy Data Bank")


def _parse_entry(payload: dict[str, Any]) -> EMDBContainer:
    """Parse an EMDB REST API response dict into an ``EMDBContainer``."""
    admin = payload.get("admin", {})
    sd = _first_structure_determination(payload)
    emdb_id = payload["emdb_id"]
    release_date = admin.get("key_dates", {}).get("header_release", "")
    return EMDBContainer.new(
        creators=_parse_creators(payload),
        title=admin.get("title", ""),
        description=_parse_description(payload),
        publisher=_PUBLISHER,
        publicationYear=release_date[:4] if release_date else None,
        resourceType=ResourceType(resourceTypeGeneral=ResourceTypeGeneral.Dataset),
        subjects=_parse_subjects(admin),
        dates=_parse_dates(admin),
        alternateIdentifiers=[
            AlternateIdentifier(alternateIdentifier=emdb_id, alternateIdentifierType="EMDB"),
        ],
        fundingReferences=_parse_funding(admin),
        relatedIdentifiers=_parse_related_identifiers(payload),
        emdb=EMDBFields(
            emdb_id=emdb_id,
            method=sd.get("method", "") if sd else "",
            resolution_angstrom=_parse_resolution(sd),
            resolution_method=_parse_resolution_method(sd),
        ),
    )


def _parse_creators(payload: dict[str, Any]) -> list[Creator]:
    """
    Build ``Creator`` objects from the primary citation author list.

    The citation authors are preferred over ``admin.authors_list`` because
    they carry ORCIDs.  Authors are sorted by their ``order`` field.
    Author names are in ``"Lastname Initials"`` format (e.g. ``"Choi KY"``)
    and cannot be reliably split into given/family parts, so only ``name``
    is populated.
    """
    citation = _get_citation(payload)
    if not citation:
        return []

    authors = citation.get("author", [])
    authors = sorted(authors, key=lambda a: a.get("order", 0))

    creators = []
    for author in authors:
        name = (author.get("valueOf_") or "").strip() or None
        orcid = author.get("ORCID")

        name_identifiers = []
        if orcid:
            name_identifiers.append(orcid_name_identifier(orcid))

        creators.append(Creator(
            name=name,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_description(payload: dict[str, Any]) -> str | None:
    """Return the sample name as a plain-text description; ``None`` when absent."""
    return payload.get("sample", {}).get("name", {}).get("valueOf_") or None


def _parse_subjects(admin: dict[str, Any]):
    """
    Split ``admin.keywords`` (a comma-separated string) into ``Subject`` entries.

    Duplicates and empty tokens are silently dropped.
    """
    return deduplicate_subjects(admin.get("keywords", "").split(","))


def _parse_dates(admin: dict[str, Any]) -> list[Date]:
    """
    Extract deposition, release, and update dates from ``admin.key_dates``.

    * ``deposition``    → `DateType.Submitted`
    * ``header_release`` → `DateType.Created`
    * ``update``        → `DateType.Updated`

    EMDB timestamps arrive without a timezone suffix (``"2026-03-25T00:00:00"``);
    a ``"Z"`` marker is appended to produce valid ISO 8601 UTC strings.
    """
    key_dates = admin.get("key_dates", {})
    dates = []

    for field, date_type in (
        ("deposition", DateType.Submitted),
        ("header_release", DateType.Created),
        ("update", DateType.Updated),
    ):
        value = key_dates.get(field)
        if value:
            dates.append(Date(date=_utc(value), dateType=date_type))

    return dates


def _parse_funding(admin: dict[str, Any]) -> list[FundingReference]:
    """
    Map ``admin.grant_support.grant_reference`` entries to ``FundingReference`` objects.

    ``funding_body`` → ``funderName``;  ``code`` → ``awardNumber`` when present.
    """
    grant_support = admin.get("grant_support", {})
    grant_refs = grant_support.get("grant_reference", [])
    # The API may return a single dict instead of a list for one-element arrays.
    if isinstance(grant_refs, dict):
        grant_refs = [grant_refs]

    refs = []
    for grant in grant_refs:
        name = grant.get("funding_body", "")
        if not name:
            continue
        refs.append(FundingReference(
            funderName=name,
            awardNumber=grant.get("code") or None,
        ))
    return refs


def _parse_related_identifiers(payload: dict[str, Any]) -> list[RelatedIdentifier]:
    """
    Build ``RelatedIdentifier`` entries for the citation DOI and PDB cross-references.

    * Citation DOI → type ``DOI``, relation ``IsDescribedBy`` (the paper describes
      this structure entry).
    * PDB entries  → type ``URL`` (RCSB landing page), relation ``IsSourceOf``
      (the EM density map is the source of the atomic model).
    """
    result = []

    citation = _get_citation(payload)
    if citation:
        for ref in citation.get("external_references", []):
            if ref.get("type_") == "DOI":
                raw = ref.get("valueOf_", "")
                # Strip "doi:" or "DOI:" prefix used by EMDB
                doi = raw.split("doi:", 1)[-1].split("DOI:", 1)[-1].strip()
                if doi:
                    result.append(RelatedIdentifier(
                        relatedIdentifier=doi,
                        relatedIdentifierType=RelatedIdentifierType.DOI,
                        relationType=RelationType.IsDescribedBy,
                    ))

    pdb_refs = (
        payload.get("crossreferences", {})
               .get("pdb_list", {})
               .get("pdb_reference", [])
    )
    if isinstance(pdb_refs, dict):
        pdb_refs = [pdb_refs]
    for pdb_ref in pdb_refs:
        pdb_id = (pdb_ref.get("pdb_id") or "").upper()
        if pdb_id:
            result.append(RelatedIdentifier(
                relatedIdentifier=f"https://www.rcsb.org/structure/{pdb_id}",
                relatedIdentifierType=RelatedIdentifierType.URL,
                relationType=RelationType.IsSourceOf,
            ))

    return result


def _get_citation(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Return the primary citation ``citation_type`` dict, or ``None``."""
    try:
        return payload["crossreferences"]["citation_list"]["primary_citation"]["citation_type"]
    except KeyError:
        return None


def _first_structure_determination(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Return the first structure determination entry, or ``None``."""
    sd_list = (
        payload.get("structure_determination_list", {})
               .get("structure_determination", [])
    )
    return sd_list[0] if sd_list else None


def _parse_resolution(sd: dict[str, Any] | None) -> float | None:
    """Extract the final-reconstruction resolution (Å) from a structure determination."""
    if not sd:
        return None
    ip_list = sd.get("image_processing", [])
    if not ip_list:
        return None
    val = ip_list[0].get("final_reconstruction", {}).get("resolution", {}).get("valueOf_")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_resolution_method(sd: dict[str, Any] | None) -> str | None:
    """Extract the resolution-estimation method from a structure determination."""
    if not sd:
        return None
    ip_list = sd.get("image_processing", [])
    if not ip_list:
        return None
    return ip_list[0].get("final_reconstruction", {}).get("resolution_method")


def _utc(timestamp: str) -> str:
    """Append a UTC marker to an EMDB timestamp that lacks one."""
    if timestamp.endswith("Z") or "+" in timestamp:
        return timestamp
    return timestamp + "Z"