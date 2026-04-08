"""DataCite field parsers."""

from __future__ import annotations

import re

from ..base.parser import compose_creator_name

from ..base import (
    Affiliation,
    AlternateIdentifier,
    Contributor,
    ContributorType,
    Creator,
    DataCite,
    Date,
    DateType,
    Description,
    DescriptionType,
    FundingReference,
    NameIdentifier,
    Publisher,
    RelatedIdentifier,
    RelatedIdentifierType,
    RelatedItem,
    RelatedItemIdentifier,
    RelatedItemType,
    RelationType,
    ResourceType,
    ResourceTypeGeneral,
    Rights,
    Subject,
    Title,
    TitleType,
)

_RELATION_TYPE_MAP: dict[str, RelationType] = {rt.value: rt for rt in RelationType}


def _parse_work(attrs: dict) -> DataCite:
    """Parse a DataCite API record ``attributes`` dict into a ``DataCite`` object."""
    publisher = attrs.get("publisher")
    publisher_name = publisher["name"] if isinstance(publisher, dict) else (publisher or "")
    return DataCite.new(
        creators=_parse_creators(attrs.get("creators") or []),
        titles=_parse_titles(attrs.get("titles") or []),
        descriptions=_parse_descriptions(attrs.get("descriptions") or []),
        publisher=Publisher(name=publisher_name),
        publicationYear=str(attrs["publicationYear"]) if attrs.get("publicationYear") else None,
        resourceType=_parse_types(attrs.get("types")),
        contributors=_parse_contributors(attrs.get("contributors") or []),
        dates=_parse_dates(attrs.get("dates") or []),
        subjects=_parse_subjects(attrs.get("subjects") or []),
        rightsList=_parse_rights(attrs.get("rightsList") or []),
        fundingReferences=_parse_funding(attrs.get("fundingReferences") or []),
        alternateIdentifiers=[
            AlternateIdentifier(
                alternateIdentifier=a["alternateIdentifier"],
                alternateIdentifierType=a["alternateIdentifierType"],
            )
            for a in (attrs.get("alternateIdentifiers") or [])
            if a.get("alternateIdentifier") and a.get("alternateIdentifierType")
        ],
        relatedIdentifiers=_parse_related_identifiers(attrs.get("relatedIdentifiers") or []),
        relatedItems=_parse_related_items(attrs.get("relatedItems") or []),
        formats=attrs.get("formats") or [],
        version=attrs.get("version"),
        language=attrs.get("language"),
        doi=attrs.get("doi"),
    )


def _parse_creators(creators_data: list[dict]) -> list[Creator]:
    """
    Build `Creator` objects from DataCite creator entries.

    Both personal (given+family name) and organizational (name only) creators
    are handled.  ORCID and other name identifiers are preserved.
    """
    creators = []
    for c in creators_data:
        name_identifiers = []
        for ni in c.get("nameIdentifiers") or []:
            value = ni.get("nameIdentifier", "")
            scheme = ni.get("nameIdentifierScheme", "")
            if value and scheme:
                name_identifiers.append(NameIdentifier(
                    nameIdentifier=value,
                    nameIdentifierScheme=scheme,
                    schemeUri=ni.get("schemeUri"),
                ))

        affiliation = None
        affiliations = c.get("affiliation") or []
        if affiliations:
            first = affiliations[0]
            aff_name = first.get("name") if isinstance(first, dict) else str(first)
            if aff_name:
                affiliation = Affiliation(name=aff_name)

        given = c.get("givenName") or None
        family = c.get("familyName") or None
        name = c.get("name") or compose_creator_name(family, given)

        creators.append(Creator(
            givenName=given,
            familyName=family,
            name=name,
            affiliation=affiliation,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_titles(titles_data: list[dict]) -> list[Title]:
    titles = []
    for t in titles_data:
        text = t.get("title", "")
        if not text:
            continue
        title_type = None
        if t.get("titleType"):
            try:
                title_type = TitleType(t["titleType"])
            except ValueError:
                pass
        titles.append(Title(title=text, titleType=title_type))
    return titles


def _parse_descriptions(descriptions_data: list[dict]) -> list[Description]:
    descriptions = []
    for d in descriptions_data:
        text = d.get("description", "")
        if not text:
            continue
        desc_type_str = d.get("descriptionType", "")
        try:
            desc_type = DescriptionType(desc_type_str)
        except ValueError:
            desc_type = DescriptionType.Other
        # Strip residual JATS/HTML markup occasionally present in DataCite records
        text = re.sub(r"<[^>]+>", " ", text).strip()
        if text:
            descriptions.append(Description(description=text, descriptionType=desc_type))
    return descriptions


def _parse_dates(dates_data: list[dict]) -> list[Date]:
    dates = []
    for d in dates_data:
        date_str = d.get("date", "")
        if not date_str:
            continue
        date_type_str = d.get("dateType", "")
        try:
            date_type = DateType(date_type_str)
        except ValueError:
            raise ValueError(
                f"DataCite returned unrecognised dateType {date_type_str!r}. "
                "This may indicate a schema version change; manual review required."
            ) from None
        # Normalise "YYYY-MM-DD" to a full ISO 8601 datetime string
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            date_str = date_str + "T00:00:00Z"
        dates.append(Date(date=date_str, dateType=date_type))
    return dates


def _parse_subjects(subjects_data: list[dict]) -> list[Subject]:
    return [
        Subject(subject=s["subject"])
        for s in subjects_data
        if s.get("subject")
    ]


def _parse_rights(rights_data: list[dict]) -> list[Rights]:
    result = []
    for r in rights_data:
        text = r.get("rights", "")
        if not text:
            continue
        result.append(Rights(
            rights=text,
            rightsUri=r.get("rightsUri"),
            rightsIdentifier=r.get("rightsIdentifier"),
        ))
    return result


def _parse_funding(funding_data: list[dict]) -> list[FundingReference]:
    result = []
    for f in funding_data:
        name = f.get("funderName", "")
        if not name:
            continue
        result.append(FundingReference(
            funderName=name,
            awardNumber=f.get("awardNumber") or None,
            awardTitle=f.get("awardTitle") or None,
        ))
    return result


def _parse_related_identifiers(related_data: list[dict]) -> list[RelatedIdentifier]:
    result = []
    for r in related_data:
        identifier = r.get("relatedIdentifier", "")
        if not identifier:
            continue
        id_type_str = r.get("relatedIdentifierType", "")
        relation_str = r.get("relationType", "")
        try:
            id_type = RelatedIdentifierType(id_type_str)
        except ValueError:
            continue
        relation_type = _RELATION_TYPE_MAP.get(relation_str)
        if relation_type is None:
            continue
        result.append(RelatedIdentifier(
            relatedIdentifier=identifier,
            relatedIdentifierType=id_type,
            relationType=relation_type,
        ))
    return result


_CONTRIBUTOR_TYPE_MAP: dict[str, ContributorType] = {ct.value: ct for ct in ContributorType}
_DATACITE_TYPE_MAP: dict[str, ResourceTypeGeneral] = {rt.value: rt for rt in ResourceTypeGeneral}
_RELATED_ITEM_TYPE_MAP: dict[str, RelatedItemType] = {rt.value: rt for rt in RelatedItemType}


def _parse_contributors(contributors_data: list[dict]) -> list[Contributor]:
    """
    Build ``Contributor`` objects from DataCite contributor entries.

    Same name-field handling as ``_parse_creators``; additionally maps
    ``contributorType`` to the controlled vocabulary.  Entries with
    unrecognised contributor types are silently skipped.
    """
    contributors = []
    for c in contributors_data:
        contributor_type = _CONTRIBUTOR_TYPE_MAP.get(c.get("contributorType", ""))
        if contributor_type is None:
            continue

        name_identifiers = []
        for ni in c.get("nameIdentifiers") or []:
            value = ni.get("nameIdentifier", "")
            scheme = ni.get("nameIdentifierScheme", "")
            if value and scheme:
                name_identifiers.append(NameIdentifier(
                    nameIdentifier=value,
                    nameIdentifierScheme=scheme,
                    schemeUri=ni.get("schemeUri"),
                ))

        affiliation = None
        affiliations = c.get("affiliation") or []
        if affiliations:
            first = affiliations[0]
            aff_name = first.get("name") if isinstance(first, dict) else str(first)
            if aff_name:
                affiliation = Affiliation(name=aff_name)

        given = c.get("givenName") or None
        family = c.get("familyName") or None
        name = c.get("name") or compose_creator_name(family, given)

        contributors.append(Contributor(
            contributorType=contributor_type,
            givenName=given,
            familyName=family,
            name=name,
            affiliation=affiliation,
            nameIdentifiers=name_identifiers,
        ))
    return contributors


def _parse_related_items(related_items_data: list[dict]) -> list[RelatedItem]:
    """
    Parse DataCite API ``relatedItems`` entries into ``RelatedItem`` objects.

    The DataCite API already structures these as the schema expects, so
    this is primarily a type-checked transcription.  Entries with
    unrecognised ``relatedItemType`` or ``relationType`` are silently skipped.
    """
    result = []
    for item in related_items_data:
        item_type = _RELATED_ITEM_TYPE_MAP.get(item.get("relatedItemType", ""))
        relation_type = _RELATION_TYPE_MAP.get(item.get("relationType", ""))
        if item_type is None or relation_type is None:
            continue

        ri_id = None
        rid_data = item.get("relatedItemIdentifier")
        if rid_data:
            id_val = rid_data.get("relatedItemIdentifier", "")
            id_type_str = rid_data.get("relatedItemIdentifierType", "")
            if id_val and id_type_str:
                try:
                    ri_id = RelatedItemIdentifier(
                        relatedItemIdentifier=id_val,
                        relatedItemIdentifierType=RelatedIdentifierType(id_type_str),
                    )
                except ValueError:
                    pass

        titles = [
            Title(title=t["title"])
            for t in (item.get("titles") or [])
            if t.get("title")
        ]

        result.append(RelatedItem(
            relatedItemType=item_type,
            relationType=relation_type,
            relatedItemIdentifier=ri_id,
            titles=titles,
            volume=item.get("volume") or None,
            issue=item.get("issue") or None,
            firstPage=item.get("firstPage") or None,
            lastPage=item.get("lastPage") or None,
            publisher=item.get("publisher") or None,
            publicationYear=item.get("publicationYear") or None,
        ))
    return result


def _parse_types(types_data: dict | None) -> ResourceType | None:
    """Map the DataCite API ``types`` dict to a DataCite ResourceType."""
    if not types_data:
        return None
    general_str = types_data.get("resourceTypeGeneral", "")
    general = _DATACITE_TYPE_MAP.get(general_str)
    if general is None:
        return None
    return ResourceType(
        resourceTypeGeneral=general,
        resourceType=types_data.get("resourceType") or None,
    )