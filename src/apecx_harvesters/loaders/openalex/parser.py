"""OpenAlex field parsers."""

from __future__ import annotations

from itertools import chain

from ..base import (
    Affiliation,
    AlternateIdentifier,
    Creator,
    DataCite,
    Date,
    DateType,
    FundingReference,
    Publisher,
    RelatedItem,
    ResourceType,
    ResourceTypeGeneral,
    Rights,
)
from ..base.parser import (
    build_journal_related_item,
    deduplicate_subjects,
    orcid_name_identifier,
)

_OPENALEX_TYPE_MAP: dict[str, ResourceTypeGeneral] = {
    "journal-article": ResourceTypeGeneral.JournalArticle,
    "preprint": ResourceTypeGeneral.Preprint,
    "book-chapter": ResourceTypeGeneral.BookChapter,
    "book": ResourceTypeGeneral.Book,
    "dissertation": ResourceTypeGeneral.Dissertation,
    "dataset": ResourceTypeGeneral.Dataset,
    "report": ResourceTypeGeneral.Report,
    "standard": ResourceTypeGeneral.Standard,
    "editorial": ResourceTypeGeneral.JournalArticle,
    "letter": ResourceTypeGeneral.JournalArticle,
    "review": ResourceTypeGeneral.JournalArticle,
    "paratext": ResourceTypeGeneral.Other,
    "libguides": ResourceTypeGeneral.Other,
    "other": ResourceTypeGeneral.Other,
}

# Fields used by _parse_work; listed explicitly because list endpoints omit
# abstract_inverted_index by default unless requested via select=.
_BATCH_SELECT = ",".join([
    "doi", "display_name", "title", "abstract_inverted_index",
    "authorships", "primary_location", "publication_date",
    "topics", "keywords", "funders", "ids", "language",
])

_LICENSE_LABELS: dict[str, str] = {
    "cc-by": "Creative Commons Attribution 4.0 International",
    "cc-by-sa": "Creative Commons Attribution-ShareAlike 4.0 International",
    "cc-by-nc": "Creative Commons Attribution-NonCommercial 4.0 International",
    "cc-by-nd": "Creative Commons Attribution-NoDerivatives 4.0 International",
    "cc-by-nc-sa": "Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International",
    "cc-by-nc-nd": "Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International",
    "cc0": "Creative Commons Zero v1.0 Universal",
}

_LICENSE_URIS: dict[str, str] = {
    "cc-by": "https://creativecommons.org/licenses/by/4.0/legalcode",
    "cc-by-sa": "https://creativecommons.org/licenses/by-sa/4.0/legalcode",
    "cc-by-nc": "https://creativecommons.org/licenses/by-nc/4.0/legalcode",
    "cc-by-nd": "https://creativecommons.org/licenses/by-nd/4.0/legalcode",
    "cc-by-nc-sa": "https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode",
    "cc-by-nc-nd": "https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode",
    "cc0": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
}


def _parse_work(payload: dict) -> DataCite:
    doi = _strip_doi(payload.get("doi"))
    title = payload.get("display_name") or payload.get("title") or ""
    description = _reconstruct_abstract(payload.get("abstract_inverted_index"))
    pub_date = payload.get("publication_date") or ""
    return DataCite.new(
        creators=_parse_creators(payload.get("authorships") or []),
        title=title,
        description=description,
        publisher=_parse_publisher(payload),
        publicationYear=pub_date[:4] if pub_date else None,
        resourceType=_parse_types(payload),
        dates=_parse_dates(payload),
        subjects=_parse_subjects(payload),
        rightsList=_parse_rights(payload),
        fundingReferences=_parse_funding(payload.get("funders") or []),
        alternateIdentifiers=_parse_alternate_identifiers(payload),
        relatedItems=_build_journal_container_list(payload),
        language=payload.get("language"),
        doi=doi,
    )


def _reconstruct_abstract(inverted_index: dict[str, list[int]] | None) -> str | None:
    """
    Reconstruct a plain-text abstract from an OpenAlex inverted index.

    The index maps each word to the list of positions at which it appears.
    Sorting by position and joining with spaces recovers the original text.
    """
    if not inverted_index:
        return None
    word_positions: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort()
    return " ".join(word for _, word in word_positions) or None


def _parse_creators(authorships: list[dict]) -> list[Creator]:
    """
    Build `Creator` objects from OpenAlex authorship entries.

    OpenAlex provides only a ``display_name`` (no reliable given/family split),
    so the ``name`` field is populated and ``givenName``/``familyName`` are
    left ``None``.  ORCIDs are stripped from their full URL form.
    """
    creators = []
    for authorship in authorships:
        author = authorship.get("author") or {}
        display_name = author.get("display_name") or None

        name_identifiers = []
        orcid_url = author.get("orcid")
        if orcid_url:
            name_identifiers.append(orcid_name_identifier(orcid_url))

        # First institution → primary affiliation
        institutions = authorship.get("institutions") or []
        affiliation = None
        if institutions:
            inst_name = institutions[0].get("display_name")
            if inst_name:
                affiliation = Affiliation(name=inst_name)

        creators.append(Creator(
            name=display_name,
            affiliation=affiliation,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_publisher(payload: dict) -> Publisher:
    """Return publisher from primary location source name, falling back to an empty string."""
    primary = payload.get("primary_location") or {}
    source = primary.get("source") or {}
    name = source.get("display_name") or ""
    return Publisher(name=name)


def _parse_dates(payload: dict) -> list[Date]:
    """Extract publication date as a Created date."""
    dates = []
    pub_date = payload.get("publication_date")
    if pub_date:
        dates.append(Date(date=f"{pub_date}T00:00:00Z", dateType=DateType.Created))
    return dates


def _parse_subjects(payload: dict):
    """
    Collect subjects from OpenAlex ``topics`` and ``keywords``.

    Topics come first (they are curated classifications); keywords follow.
    Duplicates are silently dropped.
    """
    topic_names = (t.get("display_name") or "" for t in (payload.get("topics") or []))
    keyword_names = (k.get("display_name") or "" for k in (payload.get("keywords") or []))
    return deduplicate_subjects(chain(topic_names, keyword_names))


def _parse_rights(payload: dict) -> list[Rights]:
    """Map the primary location license to a Rights entry."""
    primary = payload.get("primary_location") or {}
    license_id: str | None = primary.get("license")
    if not license_id:
        return []
    label = _LICENSE_LABELS.get(license_id) or license_id
    uri = _LICENSE_URIS.get(license_id)
    return [Rights(rights=label, rightsUri=uri, rightsIdentifier=license_id)]


def _parse_funding(funders: list[dict]) -> list[FundingReference]:
    """Map OpenAlex funder entries to FundingReference objects (no award numbers)."""
    result = []
    for funder in funders:
        name = funder.get("display_name", "")
        if name:
            result.append(FundingReference(funderName=name))
    return result


def _parse_alternate_identifiers(payload: dict) -> list[AlternateIdentifier]:
    """Extract PMID from ``ids.pmid`` as an alternate identifier."""
    result = []
    ids = payload.get("ids") or {}
    pmid_url = ids.get("pmid")
    if pmid_url:
        pmid = pmid_url.rstrip("/").split("/")[-1]
        result.append(AlternateIdentifier(alternateIdentifier=pmid, alternateIdentifierType="PMID"))
    return result


def _parse_types(payload: dict) -> ResourceType | None:
    """Map the OpenAlex ``type`` field to a DataCite ResourceType."""
    work_type = (payload.get("type") or "").lower()
    general = _OPENALEX_TYPE_MAP.get(work_type)
    if general is None:
        return None
    return ResourceType(resourceTypeGeneral=general, resourceType=work_type)


def _build_journal_container_list(payload: dict) -> list[RelatedItem]:
    """
    Build a ``RelatedItem(type=Journal)`` from OpenAlex primary location fields.

    Returns an empty list when no substantive container information is available.
    """
    primary = payload.get("primary_location") or {}
    source = primary.get("source") or {}
    biblio = payload.get("biblio") or {}

    title = source.get("display_name") or None
    issn = source.get("issn_l") or None
    volume = biblio.get("volume") or None
    issue = biblio.get("issue") or None
    first_page = biblio.get("first_page") or None
    last_page = biblio.get("last_page") or None
    ri = build_journal_related_item(
        title=title, issn=issn, volume=volume, issue=issue,
        first_page=first_page, last_page=last_page,
    )
    return [ri] if ri else []


def _strip_doi(doi_url: str | None) -> str | None:
    """Strip 'https://doi.org/' prefix from a DOI URL, returning the bare DOI."""
    if not doi_url:
        return None
    return doi_url.replace("https://doi.org/", "").replace("http://doi.org/", "")