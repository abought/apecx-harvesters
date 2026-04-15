"""PubMed field parsers."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from itertools import chain

from ..base import (
    Affiliation,
    AlternateIdentifier,
    Creator,
    Date,
    DateType,
    FundingReference,
    Publisher,
    RelatedItem,
    RelatedIdentifierType,
    RelatedItemIdentifier,
    RelatedItemType,
    RelationType,
    ResourceType,
    ResourceTypeGeneral,
)
from ..base.parser import (
    build_journal_related_item,
    compose_creator_name,
    deduplicate_subjects,
    orcid_name_identifier,
    split_page,
)
from .model import PubMedContainer, PubMedFields

_MONTH_ABBR: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_article(article_elem: ET.Element) -> PubMedContainer:
    """Parse one ``<PubmedArticle>`` element into a ``PubMedContainer``."""
    medline = article_elem.find("MedlineCitation")
    pubmed_data = article_elem.find("PubmedData")
    if medline is None:
        raise ValueError("Missing <MedlineCitation>")
    if pubmed_data is None:
        raise ValueError("Missing <PubmedData>")
    article = medline.find("Article")
    if article is None:
        raise ValueError("Missing <Article>")

    pmid = medline.findtext("PMID")
    if pmid is None:
        raise ValueError("Missing PMID")
    doi = _find_article_id(pubmed_data, "doi")
    pmc_id = _find_article_id(pubmed_data, "pmc")

    alternate_ids: list[AlternateIdentifier] = [
        AlternateIdentifier(alternateIdentifier=pmid, alternateIdentifierType="PMID")
    ]
    if pmc_id:
        alternate_ids.append(
            AlternateIdentifier(alternateIdentifier=pmc_id, alternateIdentifierType="PMCID")
        )

    journal_ri = _build_journal_container(article)
    return PubMedContainer.new(
        creators=_parse_creators(article.findall("AuthorList/Author")),
        title=_parse_title(article),
        description=_parse_abstract(article),
        publisher=Publisher(name=article.findtext("Journal/Title") or ""),
        publicationYear=_parse_publication_year(article),
        resourceType=ResourceType(resourceTypeGeneral=ResourceTypeGeneral.JournalArticle),
        subjects=_parse_subjects(medline),
        dates=_parse_dates(article, pubmed_data),
        fundingReferences=_parse_funding(article),
        doi=doi,
        alternateIdentifiers=alternate_ids,
        relatedItems=[journal_ri] if journal_ri else [],
        pubmed=_parse_pubmed_fields(medline, article),
        language=article.findtext("Language"),
    )


def _parse_book_article(elem: ET.Element) -> PubMedContainer:
    """Parse one ``<PubmedBookArticle>`` element into a ``PubMedContainer``."""
    book_doc = elem.find("BookDocument")
    pubmed_book_data = elem.find("PubmedBookData")
    if book_doc is None:
        raise ValueError("Missing <BookDocument>")
    if pubmed_book_data is None:
        raise ValueError("Missing <PubmedBookData>")

    pmid = book_doc.findtext("PMID")
    if pmid is None:
        raise ValueError("Missing PMID")

    book = book_doc.find("Book")

    # Chapter title; fall back to book title when absent.
    title_elem = book_doc.find("ArticleTitle")
    title = "".join(title_elem.itertext()).strip() if title_elem is not None else ""
    if not title and book is not None:
        title = book.findtext("BookTitle") or ""

    # Chapter authors only — exclude the editor list.
    chapter_authors = book_doc.findall("AuthorList[@Type='authors']/Author")
    if not chapter_authors:
        chapter_authors = book_doc.findall("AuthorList/Author")

    publisher_name = ""
    pub_year = None
    book_title = None
    isbn = None
    if book is not None:
        publisher_name = book.findtext("Publisher/PublisherName") or ""
        pub_year = book.findtext("PubDate/Year")
        book_title = book.findtext("BookTitle")
        isbn = book.findtext("Isbn")

    alternate_ids: list[AlternateIdentifier] = [
        AlternateIdentifier(alternateIdentifier=pmid, alternateIdentifierType="PMID")
    ]
    for aid in book_doc.findall("ArticleIdList/ArticleId"):
        if aid.get("IdType") == "bookaccession" and aid.text:
            alternate_ids.append(
                AlternateIdentifier(alternateIdentifier=aid.text, alternateIdentifierType="NCBI Bookshelf")
            )

    doi = _find_article_id(pubmed_book_data, "doi")

    pub_date: list[Date] = []
    if pub_year:
        pub_date.append(Date(date=f"{pub_year}-01-01T00:00:00Z", dateType=DateType.Created))

    book_ri: RelatedItem | None = None
    if book_title:
        book_ri = RelatedItem(
            relatedItemType=RelatedItemType.Book,
            relationType=RelationType.IsPublishedIn,
            relatedItemIdentifier=RelatedItemIdentifier(
                relatedItemIdentifier=isbn,
                relatedItemIdentifierType=RelatedIdentifierType.ISBN,
            ) if isbn else None,
            titles=[],
        )

    pub_types = [pt.text for pt in book_doc.findall("PublicationType") if pt.text]

    return PubMedContainer.new(
        creators=_parse_creators(chapter_authors),
        title=title,
        description=_parse_abstract(book_doc),
        publisher=Publisher(name=publisher_name),
        publicationYear=pub_year,
        resourceType=ResourceType(resourceTypeGeneral=ResourceTypeGeneral.BookChapter),
        subjects=[],
        dates=pub_date,
        fundingReferences=[],
        doi=doi,
        alternateIdentifiers=alternate_ids,
        relatedItems=[book_ri] if book_ri else [],
        pubmed=PubMedFields(publication_types=pub_types),
        language=book_doc.findtext("Language"),
    )


def _parse_creators(author_elems: list[ET.Element]) -> list[Creator]:
    """
    Build `Creator` objects from a list of ``<Author>`` elements.

    * The first ``<AffiliationInfo><Affiliation>`` is captured per author.
    * ``<Identifier Source="ORCID">`` values are normalised to bare IDs
      (stripping any ``http(s)://orcid.org/`` prefix).
    * Multiple affiliations beyond the first are ignored — PubMed frequently
      lists secondary addresses that cannot reliably be distinguished from
      the primary one.
    """
    creators = []
    for author in author_elems:
        family = author.findtext("LastName")
        given = author.findtext("ForeName")

        affiliation = None
        aff_elem = author.find("AffiliationInfo/Affiliation")
        if aff_elem is not None and aff_elem.text:
            affiliation = Affiliation(name=aff_elem.text.strip())

        name_identifiers = []
        for ident in author.findall("Identifier"):
            if ident.get("Source") == "ORCID" and ident.text:
                name_identifiers.append(orcid_name_identifier(ident.text.strip()))

        name = compose_creator_name(family, given)

        creators.append(Creator(
            familyName=family,
            givenName=given,
            name=name,
            affiliation=affiliation,
            nameIdentifiers=name_identifiers,
        ))
    return creators


def _parse_title(article: ET.Element) -> str:
    """
    Extract the article title, collapsing any inline XML markup to plain text.

    PubMed titles occasionally contain sub/superscript elements
    (``<i>``, ``<sup>``, etc.); `ET.tostring` with `method="text"` recovers
    all text nodes including tail text on child elements.
    """
    title_elem = article.find("ArticleTitle")
    if title_elem is None:
        return ""
    return "".join(title_elem.itertext()).strip()


def _parse_abstract(article: ET.Element) -> str | None:
    """
    Build a plain-text abstract from ``<AbstractText>`` elements.

    When multiple sections are present (structured abstract), each section is
    prefixed with its ``Label`` attribute and joined with double newlines::

        BACKGROUND: ...

        METHODS: ...

    A single unlabelled ``<AbstractText>`` is returned as-is.
    Returns ``None`` when no ``<Abstract>`` element is present.
    """
    abstract_elem = article.find("Abstract")
    if abstract_elem is None:
        return None

    sections = abstract_elem.findall("AbstractText")
    if not sections:
        return None

    parts = []
    for section in sections:
        text = "".join(section.itertext()).strip()
        if not text:
            continue
        label = section.get("Label")
        parts.append(f"{label}: {text}" if label else text)

    return "\n\n".join(parts) if parts else None


def _parse_subjects(medline: ET.Element):
    """
    Collect subjects from MeSH descriptors and author keywords.

    MeSH terms come first (they are controlled vocabulary); free-text keywords
    from ``<KeywordList>`` follow.  Duplicates are silently dropped.
    """
    mesh_terms = (d.text or "" for d in medline.findall("MeshHeadingList/MeshHeading/DescriptorName"))
    keywords = (k.text or "" for k in medline.findall("KeywordList/Keyword"))
    return deduplicate_subjects(chain(mesh_terms, keywords))


def _parse_dates(article: ET.Element, pubmed_data: ET.Element) -> list[Date]:
    """
    Extract submission and publication dates.

    * ``PubStatus="received"`` → `Submitted`
    * ``<ArticleDate DateType="Electronic">`` (preferred) or
      ``<JournalIssue/PubDate>`` → `Created`
    """
    dates = []

    received = pubmed_data.find("History/PubMedPubDate[@PubStatus='received']")
    if received is not None:
        iso = _pubmed_date_to_iso(received)
        if iso:
            dates.append(Date(date=iso, dateType=DateType.Submitted))

    accepted = pubmed_data.find("History/PubMedPubDate[@PubStatus='accepted']")
    if accepted is not None:
        iso = _pubmed_date_to_iso(accepted)
        if iso:
            dates.append(Date(date=iso, dateType=DateType.Accepted))

    revised = pubmed_data.find("History/PubMedPubDate[@PubStatus='revised']")
    if revised is not None:
        iso = _pubmed_date_to_iso(revised)
        if iso:
            dates.append(Date(date=iso, dateType=DateType.Updated))

    # Prefer the explicit electronic publication date; fall back to journal issue date.
    article_date = article.find("ArticleDate[@DateType='Electronic']")
    pub_date_elem = article_date if article_date is not None else article.find("Journal/JournalIssue/PubDate")
    if pub_date_elem is not None:
        iso = _pubmed_date_to_iso(pub_date_elem)
        if iso:
            dates.append(Date(date=iso, dateType=DateType.Created))

    return dates


def _parse_funding(article: ET.Element) -> list[FundingReference]:
    """
    Map ``<GrantList><Grant>`` elements to `FundingReference` entries.

    Each grant produces one entry: ``Agency`` → ``funderName``,
    ``GrantID`` → ``awardNumber`` (when present).
    """
    refs = []
    for grant in article.findall("GrantList/Grant"):
        agency = grant.findtext("Agency")
        if not agency:
            continue
        grant_id = grant.findtext("GrantID")
        refs.append(FundingReference(
            funderName=agency,
            awardNumber=grant_id if grant_id else None,
        ))
    return refs


def _parse_pubmed_fields(medline: ET.Element, article: ET.Element) -> PubMedFields:
    """Extract PubMed-specific bibliographic metadata."""
    pub_types = [
        pt.text for pt in article.findall("PublicationTypeList/PublicationType")
        if pt.text
    ]
    return PubMedFields(publication_types=pub_types)


def _build_journal_container(article: ET.Element) -> RelatedItem | None:
    """
    Build a ``RelatedItem`` describing the journal container of this article.

    Returns ``None`` when no substantive container information is available.
    ISSN, title, volume, issue, and page are collected from the ``<Journal>``
    and ``<Pagination>`` elements.
    """
    journal = article.find("Journal")
    if journal is None:
        return None
    issn = journal.findtext("ISSN")
    title = journal.findtext("Title")
    volume = journal.findtext("JournalIssue/Volume")
    issue = journal.findtext("JournalIssue/Issue")
    first_page, last_page = split_page(article.findtext("Pagination/MedlinePgn"))
    return build_journal_related_item(
        title=title, issn=issn, volume=volume, issue=issue,
        first_page=first_page, last_page=last_page,
    )


def _parse_publication_year(article: ET.Element) -> str | None:
    """Extract 4-digit publication year from electronic article date or journal issue date."""
    article_date = article.find("ArticleDate[@DateType='Electronic']")
    if article_date is not None:
        year = article_date.findtext("Year")
        if year:
            return year
    pub_date = article.find("Journal/JournalIssue/PubDate")
    if pub_date is not None:
        year = pub_date.findtext("Year")
        if year:
            return year
        medline_date = pub_date.findtext("MedlineDate")
        if medline_date:
            m = re.search(r"\b(\d{4})\b", medline_date)
            if m:
                return m.group(1)
    return None


def _find_article_id(pubmed_data: ET.Element, id_type: str) -> str | None:
    """Return the text of ``<ArticleId IdType='{id_type}'>`` or ``None``."""
    if pubmed_data is None:
        return None
    for aid in pubmed_data.findall("ArticleIdList/ArticleId"):
        if aid.get("IdType") == id_type:
            return aid.text
    return None


def _pubmed_date_to_iso(date_elem: ET.Element) -> str | None:
    """
    Convert a PubMed date element to an ISO 8601 datetime string.

    Handles the three common formats found in PubMed XML:

    * ``<Year>/<Month>/<Day>`` — Month may be a number or three-letter
      abbreviation (``Jan``, ``Feb``, …).
    * ``<Year>/<Month>`` — Day defaults to 1.
    * ``<MedlineDate>`` — e.g. ``"2020 Jan-Feb"``; only the year is extracted.
    """
    year_text = date_elem.findtext("Year")
    month_text = date_elem.findtext("Month")
    day_text = date_elem.findtext("Day")

    if year_text:
        try:
            year = int(year_text)
        except ValueError:
            return None

        month = 1
        if month_text:
            try:
                month = int(month_text)
            except ValueError:
                month = _MONTH_ABBR.get(month_text.lower()[:3], 1)

        day = 1
        if day_text:
            try:
                day = int(day_text)
            except ValueError:
                pass

        return f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z"

    # Fall back to MedlineDate — extract four-digit year only.
    medline_date = date_elem.findtext("MedlineDate")
    if medline_date:
        m = re.search(r"\b(\d{4})\b", medline_date)
        if m:
            return f"{m.group(1)}-01-01T00:00:00Z"

    return None