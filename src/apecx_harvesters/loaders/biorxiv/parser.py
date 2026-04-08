"""bioRxiv/medRxiv field parsers."""

from __future__ import annotations

from typing import Any

from ..base import (
    Affiliation,
    Contributor,
    ContributorType,
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
    Rights,
    Subject,
)
from .model import BiorXivContainer, BiorXivFields


def _parse_preprint(data: dict[str, Any]) -> BiorXivContainer:
    """Parse a bioRxiv/medRxiv API response dict into a ``BiorXivContainer``."""
    collection: list[dict[str, Any]] = data["collection"]
    latest = collection[-1]
    return BiorXivContainer.new(
        creators=_parse_creators(latest),
        title=latest["title"],
        description=latest["abstract"],
        publisher=Publisher(name=latest["server"]),
        publicationYear=collection[0]["date"][:4],
        resourceType=ResourceType(resourceTypeGeneral=ResourceTypeGeneral.Preprint),
        contributors=_parse_contributors(latest),
        subjects=_parse_subjects(latest),
        version=latest["version"],
        dates=_parse_dates(collection),
        rightsList=_parse_rights(latest),
        fundingReferences=_parse_funding(latest),
        doi=latest["doi"],
        relatedIdentifiers=_parse_related_identifiers(latest),
        biorxiv=_parse_biorxiv_fields(latest),
    )


def _parse_creators(record: dict[str, Any]) -> list[Creator]:
    """
    Build `Creator` objects from the semicolon-separated `authors` string.

    Each entry is in ``"FamilyName, Initials"`` format.  The corresponding
    author is identified by matching family name and first initial against
    `author_corresponding`, and their institution is attached as an affiliation.
    """
    corr_full = record.get("author_corresponding", "")
    institution = record.get("author_corresponding_institution", "")
    corr_parts = corr_full.split()
    corr_family = corr_parts[-1] if corr_parts else ""
    corr_initial = corr_parts[0][0].upper() if len(corr_parts) >= 2 else ""

    creators = []
    for entry in record.get("authors", "").split("; "):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(", ", 1)
        family = parts[0].strip()
        given = parts[1].strip() if len(parts) > 1 else None

        affiliation = None
        if (institution and family == corr_family
                and given and corr_initial
                and given.upper().startswith(corr_initial)):
            affiliation = Affiliation(name=institution)

        creators.append(Creator(
            name=entry,
            familyName=family,
            givenName=given,
            affiliation=affiliation,
        ))
    return creators


def _parse_dates(collection: list[dict[str, Any]]) -> list[Date]:
    """
    Extract submission and (if multiple versions exist) update dates.

    The first version's date maps to `Submitted`; the latest version's date
    maps to `Updated`.  Dates are converted from ``YYYY-MM-DD`` to ISO 8601.
    """
    dates = [Date(date=_to_datetime(collection[0]["date"]), dateType=DateType.Submitted)]
    if len(collection) > 1:
        dates.append(Date(date=_to_datetime(collection[-1]["date"]), dateType=DateType.Updated))
    return dates


def _parse_subjects(record: dict[str, Any]) -> list[Subject]:
    """Map the `category` field to a single `Subject` entry."""
    category = record.get("category", "").strip()
    return [Subject(subject=category)] if category else []


def _parse_rights(record: dict[str, Any]) -> list[Rights]:
    """Map the `license` field to a `Rights` entry."""
    license_code = record.get("license", "").strip()
    return [Rights(rights=license_code)] if license_code else []


def _parse_funding(record: dict[str, Any]) -> list[FundingReference]:
    """Map the `funder` field to a `FundingReference` entry, omitting ``"NA"``."""
    funder = record.get("funder", "NA")
    if funder and funder != "NA":
        return [FundingReference(funderName=funder)]
    return []


def _parse_related_identifiers(record: dict[str, Any]) -> list[RelatedIdentifier]:
    """
    Build additional `RelatedIdentifier` entries beyond the preprint DOI.

    When the `published` field is set (and not ``"NA"``), the preprint record
    is a previous version of the published journal article.
    """
    published = record.get("published", "NA")
    if published and published != "NA":
        return [RelatedIdentifier(
            relatedIdentifier=published,
            relatedIdentifierType=RelatedIdentifierType.DOI,
            relationType=RelationType.IsPreviousVersionOf,
        )]
    return []


def _parse_contributors(record: dict[str, Any]) -> list[Contributor]:
    """Build a ContactPerson contributor from the corresponding author field."""
    corr = record.get("author_corresponding", "").strip()
    institution = record.get("author_corresponding_institution", "").strip()
    if not corr:
        return []
    return [Contributor(
        contributorType=ContributorType.ContactPerson,
        name=corr,
        affiliation=Affiliation(name=institution) if institution else None,
    )]


def _parse_biorxiv_fields(record: dict[str, Any]) -> BiorXivFields:
    """Extract bioRxiv-specific metadata from the latest version record."""
    return BiorXivFields(
        server=record["server"],
        publication_type=record["type"],
        jats_xml_url=record.get("jatsxml"),
    )


def _to_datetime(date_str: str) -> str:
    """Convert a ``YYYY-MM-DD`` date string to an ISO 8601 datetime string."""
    return f"{date_str}T00:00:00Z"