"""Crossref field parsers."""

from __future__ import annotations

import re
from itertools import chain
from typing import Any

from ..base import (
    Affiliation,
    Creator,
    DataCite,
    Date,
    DateType,
    FundingReference,
    Publisher,
    RelatedIdentifier,
    RelatedIdentifierType,
    RelatedItem,
    RelationType,
    ResourceType,
    ResourceTypeGeneral,
    Rights,
)
from ..base.parser import (
    build_journal_related_item,
    compose_creator_name,
    deduplicate_subjects,
    orcid_name_identifier,
    split_page,
)

_CROSSREF_TYPE_MAP: dict[str, ResourceTypeGeneral] = {
    "journal-article": ResourceTypeGeneral.JournalArticle,
    "proceedings-article": ResourceTypeGeneral.ConferencePaper,
    "book-chapter": ResourceTypeGeneral.BookChapter,
    "book": ResourceTypeGeneral.Book,
    "monograph": ResourceTypeGeneral.Book,
    "dissertation": ResourceTypeGeneral.Dissertation,
    "dataset": ResourceTypeGeneral.Dataset,
    "posted-content": ResourceTypeGeneral.Preprint,
    "report": ResourceTypeGeneral.Report,
    "standard": ResourceTypeGeneral.Standard,
}

# Relation types we know how to map; others are silently skipped.
_RELATION_MAP: dict[str, RelationType] = {
    "is-preprint-of": RelationType.IsPreviousVersionOf,
    "has-preprint": RelationType.IsNewVersionOf,
}


def _parse_work(msg: dict[str, Any]) -> DataCite:
    return DataCite.new(
        creators=_parse_creators(msg),
        title=msg["title"][0],
        description=_parse_abstract(msg),
        publisher=_parse_publisher(msg),
        publicationYear=_parse_publication_year(msg),
        resourceType=_parse_types(msg),
        subjects=_parse_subjects(msg),
        dates=_parse_dates(msg),
        rightsList=_parse_rights(msg),
        fundingReferences=_parse_funding(msg),
        doi=msg["DOI"],
        relatedIdentifiers=_parse_related_identifiers(msg),
        relatedItems=_build_journal_container_list(msg),
        language=msg.get("language"),
    )


def _parse_creators(msg: dict[str, Any]) -> list[Creator]:
    """
    Build `Creator` objects from the Crossref `author` array.

    Structured given/family names are used directly.  The first affiliation
    name (if any) is captured.  ORCID URLs are split into the bare identifier
    and stored as a `NameIdentifier` with scheme ``"ORCID"``.
    """
    creators = []
    for author in msg.get("author", []):
        family = author.get("family")
        given = author.get("given")

        affiliation = None
        affiliations = author.get("affiliation", [])
        if affiliations and affiliations[0].get("name"):
            affiliation = Affiliation(name=affiliations[0]["name"])

        name_identifiers = []
        orcid_url = author.get("ORCID")
        if orcid_url:
            name_identifiers.append(orcid_name_identifier(orcid_url))

        name = compose_creator_name(family, given)

        creators.append(Creator(
            familyName=family,
            givenName=given,
            name=name,
            affiliation=affiliation,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_abstract(msg: dict[str, Any]) -> str | None:
    """
    Extract plain text from a JATS XML abstract string.

    Strips all XML tags and collapses whitespace.  Returns `None` when the
    ``abstract`` field is absent (common for preprints and older records).
    """
    raw = msg.get("abstract")
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()


def _parse_publisher(msg: dict[str, Any]) -> Publisher:
    """
    Resolve the publisher name.

    For preprints, Crossref stores the generic platform name (e.g. 'openRxiv')
    in `publisher` and the actual server name in `institution[0].name`.  The
    institution name is preferred when present.
    """
    institution = msg.get("institution", [])
    if institution:
        return Publisher(name=institution[0]["name"])
    return Publisher(name=msg["publisher"])


def _parse_subjects(msg: dict[str, Any]):
    """
    Collect subjects from the `subject` array and, for preprints, `group-title`.

    The `subject` array in Crossref is often empty even for journal articles;
    `group-title` carries the discipline for preprint records.
    """
    group = msg.get("group-title", "").strip()
    return deduplicate_subjects(chain(msg.get("subject", []), [group] if group else []))


def _parse_dates(msg: dict[str, Any]) -> list[Date]:
    """
    Extract publication dates.

    * ``posted`` → `Submitted` (preprints only)
    * ``published-print``, ``published-online``, or ``issued`` → `Created`
      (first of these present wins)
    """
    dates = []
    has_posted = "posted" in msg
    if has_posted:
        dates.append(Date(date=_date_parts_to_iso(msg["posted"]), dateType=DateType.Submitted))
    # For preprints, 'issued' duplicates 'posted' — skip it and only use explicit
    # print/online dates.  For journal articles (no 'posted'), 'issued' is the
    # canonical publication date.
    candidates = ("published-print", "published-online") if has_posted else ("published-print", "published-online", "issued")
    for key in candidates:
        if key in msg:
            dates.append(Date(date=_date_parts_to_iso(msg[key]), dateType=DateType.Created))
            break
    return dates


def _parse_rights(msg: dict[str, Any]) -> list[Rights]:
    """
    Map the `license` array to `Rights` entries.

    The VOR (version of record) license is preferred; if absent the first
    entry is used.  Non-VOR licenses (e.g. TDM) are omitted — they describe
    machine-access rights, not the work's open-access status.
    """
    licenses = msg.get("license", [])
    vor = next((lic for lic in licenses if lic.get("content-version") == "vor"), None)
    chosen = vor or (licenses[0] if licenses else None)
    if chosen:
        url = chosen["URL"]
        return [Rights(rights=url, rightsUri=url)]
    return []


def _parse_funding(msg: dict[str, Any]) -> list[FundingReference]:
    """
    Expand the `funder` array into `FundingReference` entries.

    One entry is created per (funder, award) pair.  Funders with no awards
    produce a single entry with only `funderName` set.
    """
    refs = []
    for funder in msg.get("funder", []):
        name = funder["name"]
        awards = funder.get("award", [])
        if awards:
            for award in awards:
                refs.append(FundingReference(funderName=name, awardNumber=award))
        else:
            refs.append(FundingReference(funderName=name))
    return refs


def _parse_related_identifiers(msg: dict[str, Any]) -> list[RelatedIdentifier]:
    """
    Build `RelatedIdentifier` entries from the Crossref `relation` object.

    Only DOI-typed relations in `_RELATION_MAP` are captured; unknown relation
    types are silently skipped.
    """
    ris = []
    for rel_key, entries in msg.get("relation", {}).items():
        relation_type = _RELATION_MAP.get(rel_key)
        if relation_type is None:
            continue
        for entry in entries:
            if entry.get("id-type") == "doi":
                ris.append(RelatedIdentifier(
                    relatedIdentifier=entry["id"],
                    relatedIdentifierType=RelatedIdentifierType.DOI,
                    relationType=relation_type,
                ))
    return ris


def _date_parts_to_iso(date_obj: dict[str, Any]) -> str:
    """Convert a Crossref `date-parts` object to an ISO 8601 datetime string."""
    parts = date_obj["date-parts"][0]
    year = parts[0]
    month = parts[1] if len(parts) > 1 else 1
    day = parts[2] if len(parts) > 2 else 1
    return f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z"


def _parse_publication_year(msg: dict[str, Any]) -> str | None:
    """Extract the 4-digit publication year from the earliest available date field."""
    for key in ("published-print", "published-online", "issued", "posted"):
        if key in msg:
            parts = msg[key].get("date-parts", [[]])[0]
            if parts:
                return str(parts[0])
    return None


def _parse_types(msg: dict[str, Any]) -> ResourceType | None:
    """Map the Crossref `type` field to a DataCite ResourceType."""
    crossref_type = msg.get("type", "")
    general = _CROSSREF_TYPE_MAP.get(crossref_type)
    if general is None:
        return None
    return ResourceType(resourceTypeGeneral=general, resourceType=crossref_type)


def _build_journal_container_list(msg: dict[str, Any]) -> list[RelatedItem]:
    """
    Build a ``RelatedItem(type=Journal)`` from Crossref container fields.

    Returns an empty list when no substantive container information is available
    (e.g. preprints and records with no ISSN, volume, issue, or page).
    """
    container_titles = msg.get("container-title") or []
    title = container_titles[0] if container_titles else None
    issn_list = msg.get("ISSN") or []
    issn = issn_list[0] if issn_list else None
    volume = str(msg["volume"]) if msg.get("volume") is not None else None
    issue = str(msg["issue"]) if msg.get("issue") is not None else None
    first_page, last_page = split_page(msg.get("page"))
    ri = build_journal_related_item(
        title=title, issn=issn, volume=volume, issue=issue,
        first_page=first_page, last_page=last_page,
    )
    return [ri] if ri else []