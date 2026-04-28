"""
Unit tests for the PubMed harvester and schema.

Two fixture payloads are exercised:
- ``pubmed_33594067.xml`` — Nature Communications article with ORCIDs, MeSH
  terms, PMC ID, and a single-paragraph abstract.
- ``pubmed_32672655.xml`` — Orthodontics article with a structured abstract
  (BACKGROUND/METHODS/RESULTS/CONCLUSIONS) and author keywords.
"""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import jsonschema
import pytest

from apecx_harvesters.loaders.pubmed import PubMedHarvester
from apecx_harvesters.loaders.base import DateType, RelatedIdentifierType, RelatedItemType, RelationType, ResourceTypeGeneral
from apecx_harvesters.loaders.pubmed import PubMedContainer


def _parse(xml: str) -> PubMedContainer:
    harvester = PubMedHarvester()
    raw_items = asyncio.run(harvester._split_batch(xml, []))
    raw = next(iter(raw_items.values()))
    return asyncio.run(harvester._parse_item(raw))


FIXTURE_DIR = Path(__file__).parent / "fixtures"
NATCOMM_FIXTURE = FIXTURE_DIR / "pubmed_33594067.xml"
ORTHODONTICS_FIXTURE = FIXTURE_DIR / "pubmed_32672655.xml"
BATCH_FIXTURE = FIXTURE_DIR / "pubmed_batch_33594067_32672655.xml"
BOOK_ARTICLE_FIXTURE = FIXTURE_DIR / "pubmed_efetch_21413253.xml"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def natcomm_xml() -> str:
    return NATCOMM_FIXTURE.read_text()


@pytest.fixture(scope="module")
def natcomm(natcomm_xml) -> PubMedContainer:
    return _parse(natcomm_xml)


@pytest.fixture(scope="module")
def orthodontics_xml() -> str:
    return ORTHODONTICS_FIXTURE.read_text()


@pytest.fixture(scope="module")
def orthodontics(orthodontics_xml) -> PubMedContainer:
    return _parse(orthodontics_xml)


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

class TestContainer:
    def test_title(self, natcomm):
        assert "LIGHT" in natcomm.titles[0].title
        assert "hematopoietic" in natcomm.titles[0].title

    def test_publisher_is_journal_name(self, natcomm):
        assert natcomm.publisher.name == "Nature communications"

    def test_language(self, natcomm):
        assert natcomm.language == "eng"

    def test_orthodontics_language(self, orthodontics):
        assert orthodontics.language == "eng"


# ---------------------------------------------------------------------------
# Creators — Nature Communications (ORCIDs, multiple affiliations)
# ---------------------------------------------------------------------------

class TestCreatorsNatComm:
    def test_creator_count(self, natcomm):
        assert len(natcomm.creators) == 8

    def test_first_creator_family_name(self, natcomm):
        assert natcomm.creators[0].familyName == "Höpner"

    def test_first_creator_given_name(self, natcomm):
        assert natcomm.creators[0].givenName == "S S"

    def test_name_field_formatted(self, natcomm):
        assert natcomm.creators[0].name == "Höpner, S S"

    def test_affiliation_captured(self, natcomm):
        assert natcomm.creators[0].affiliation is not None
        assert "Bern" in natcomm.creators[0].affiliation.name

    def test_orcid_captured(self, natcomm):
        radpour = next(c for c in natcomm.creators if c.familyName == "Radpour")
        assert len(radpour.nameIdentifiers) == 1
        assert radpour.nameIdentifiers[0].nameIdentifier == "0000-0002-5632-7833"

    def test_author_without_orcid_has_empty_identifiers(self, natcomm):
        hopner = natcomm.creators[0]
        assert hopner.nameIdentifiers == []

    def test_multiple_orcid_authors(self, natcomm):
        orcid_authors = [c for c in natcomm.creators if c.nameIdentifiers]
        assert len(orcid_authors) == 4  # Radpour, Amrein, Riether, Ochsenbein


# ---------------------------------------------------------------------------
# Affiliation — mega-concatenated list dropped
# ---------------------------------------------------------------------------

_MINIMAL_ARTICLE_XML = """\
<PubmedArticleSet>
<PubmedArticle>
  <MedlineCitation Status="MEDLINE" Owner="NLM">
    <PMID Version="1">99999999</PMID>
    <Article PubModel="Print">
      <Journal>
        <JournalIssue CitedMedium="Internet">
          <PubDate><Year>2024</Year></PubDate>
        </JournalIssue>
        <Title>Test Journal</Title>
      </Journal>
      <ArticleTitle>Test title</ArticleTitle>
      <AuthorList CompleteYN="Y">
        <Author ValidYN="Y">
          <LastName>Smith</LastName>
          <ForeName>John</ForeName>
          <AffiliationInfo>
            <Affiliation>{affiliation}</Affiliation>
          </AffiliationInfo>
        </Author>
      </AuthorList>
      <Language>eng</Language>
    </Article>
  </MedlineCitation>
  <PubmedData/>
</PubmedArticle>
</PubmedArticleSet>"""


class TestAffiliationLength:
    def test_short_affiliation_is_captured(self):
        xml = _MINIMAL_ARTICLE_XML.format(affiliation="Department of X, University Y, City.")
        record = _parse(xml)
        assert record.creators[0].affiliation is not None
        assert "University Y" in record.creators[0].affiliation.name

    def test_mega_affiliation_is_dropped(self, caplog):
        mega = ("Inst A, Univ B; " * 100)  # ~1600 chars, clearly a concatenated list
        xml = _MINIMAL_ARTICLE_XML.format(affiliation=mega)
        with caplog.at_level("WARNING"):
            record = _parse(xml)
        assert record.creators[0].affiliation is None
        assert any("99999999" in r.message and "concatenated" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Abstract — single paragraph
# ---------------------------------------------------------------------------

class TestAbstractNatComm:
    def test_abstract_present(self, natcomm):
        assert natcomm.descriptions[0].description is not None

    def test_abstract_content(self, natcomm):
        desc = natcomm.descriptions[0].description
        assert "hematopoietic stem cell" in desc.lower()

    def test_no_xml_tags(self, natcomm):
        assert "<" not in natcomm.descriptions[0].description


# ---------------------------------------------------------------------------
# Abstract — structured (BACKGROUND/METHODS/RESULTS/CONCLUSIONS)
# ---------------------------------------------------------------------------

class TestAbstractStructured:
    def test_sections_joined(self, orthodontics):
        desc = orthodontics.descriptions[0].description
        assert "BACKGROUND:" in desc
        assert "METHODS:" in desc
        assert "RESULTS:" in desc
        assert "CONCLUSIONS:" in desc

    def test_sections_separated_by_blank_line(self, orthodontics):
        desc = orthodontics.descriptions[0].description
        assert "\n\n" in desc

    def test_content_present(self, orthodontics):
        desc = orthodontics.descriptions[0].description
        assert "jiggling" in desc.lower()


# ---------------------------------------------------------------------------
# Identifiers (PMID + DOI)
# ---------------------------------------------------------------------------

class TestIdentifiers:
    def test_pmid_in_alternate_identifiers(self, natcomm):
        pmid_ai = next(
            a for a in natcomm.alternateIdentifiers
            if a.alternateIdentifierType == "PMID"
        )
        assert pmid_ai.alternateIdentifier == "33594067"

    def test_doi_in_identifiers(self, natcomm):
        assert natcomm.identifier is not None
        assert natcomm.identifier.identifier == "10.1038/s41467-021-21317-x"
        assert natcomm.identifier.identifierType == "DOI"

    def test_pmc_in_alternate_identifiers(self, natcomm):
        pmc_ai = next(
            a for a in natcomm.alternateIdentifiers
            if a.alternateIdentifierType == "PMCID"
        )
        assert pmc_ai.alternateIdentifier == "PMC7887212"

    def test_no_doi_when_absent(self, natcomm_xml):
        xml_no_doi = re.sub(
            r'<ArticleId IdType="doi">.*?</ArticleId>', "", natcomm_xml
        )
        record = _parse(xml_no_doi)
        assert record.identifier is None

    def test_canonical_uri_prefers_pmid(self, natcomm):
        assert natcomm.canonical_uri == "pubmed:33594067"

    def test_publication_year(self, natcomm):
        assert natcomm.publicationYear == "2021"

    def test_resource_type(self, natcomm):
        assert natcomm.resourceType is not None
        assert natcomm.resourceType.resourceTypeGeneral == ResourceTypeGeneral.JournalArticle


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

class TestDatesNatComm:
    def test_submitted_from_received(self, natcomm):
        submitted = next(d for d in natcomm.dates if d.dateType == DateType.Submitted)
        assert submitted.date == "2018-12-03T00:00:00+00:00"

    def test_accepted_date(self, natcomm):
        accepted = next(d for d in natcomm.dates if d.dateType == DateType.Accepted)
        assert accepted.date == "2021-01-17T00:00:00+00:00"

    def test_created_from_article_date(self, natcomm):
        created = next(d for d in natcomm.dates if d.dateType == DateType.Created)
        assert created.date == "2021-02-16T00:00:00+00:00"

    def test_orthodontics_month_abbreviation(self, orthodontics):
        # PubDate has <Month>Jun</Month> with no Day — should default to day 1
        created = next(d for d in orthodontics.dates if d.dateType == DateType.Created)
        assert created.date == "2020-05-27T00:00:00+00:00"  # ArticleDate wins over JournalIssue/PubDate

    def test_orthodontics_accepted_date(self, orthodontics):
        accepted = next(d for d in orthodontics.dates if d.dateType == DateType.Accepted)
        assert accepted.date == "2020-04-27T00:00:00+00:00"

    def test_no_revised_date_when_absent(self, natcomm):
        assert not any(d.dateType == DateType.Updated for d in natcomm.dates)


# ---------------------------------------------------------------------------
# Subjects (MeSH + Keywords)
# ---------------------------------------------------------------------------

class TestSubjectsNatComm:
    def test_mesh_terms_captured(self, natcomm):
        terms = {s.subject for s in natcomm.subjects}
        assert "Cell Differentiation" in terms
        assert "Hematopoietic Stem Cells" in terms

    def test_no_keyword_list_when_absent(self, natcomm):
        # This fixture has no <KeywordList>; subjects come entirely from MeSH
        assert len(natcomm.subjects) == 6  # 6 MeSH terms in trimmed fixture


class TestSubjectsOrthodontics:
    def test_keywords_captured(self, orthodontics):
        terms = {s.subject for s in orthodontics.subjects}
        assert "Jiggling force" in terms
        assert "Root resorption" in terms

    def test_mesh_plus_keywords(self, orthodontics):
        terms = {s.subject for s in orthodontics.subjects}
        assert "Root Resorption" in terms  # MeSH
        assert "IL-17" in terms            # keyword


# ---------------------------------------------------------------------------
# PubMedFields
# ---------------------------------------------------------------------------

class TestPubMedFieldsNatComm:
    def test_publication_types(self, natcomm):
        assert "Journal Article" in natcomm.pubmed.publication_types

    def test_journal(self, natcomm):
        assert natcomm.publisher.name == "Nature communications"


# ---------------------------------------------------------------------------
# Journal container (RelatedItem)
# ---------------------------------------------------------------------------

class TestJournalContainerNatComm:
    @staticmethod
    def _journal_ri(record):
        return next(r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Journal)

    def test_journal_related_item_present(self, natcomm):
        assert any(r.relatedItemType == RelatedItemType.Journal for r in natcomm.relatedItems)

    def test_relation_is_published_in(self, natcomm):
        assert self._journal_ri(natcomm).relationType == RelationType.IsPublishedIn

    def test_issn_as_related_item_identifier(self, natcomm):
        ident = self._journal_ri(natcomm).relatedItemIdentifier
        assert ident is not None
        assert ident.relatedItemIdentifier == "2041-1723"
        assert ident.relatedItemIdentifierType == RelatedIdentifierType.ISSN

    def test_journal_title(self, natcomm):
        ri = self._journal_ri(natcomm)
        assert ri.titles[0].title == "Nature communications"

    def test_volume(self, natcomm):
        assert self._journal_ri(natcomm).volume == "12"

    def test_issue(self, natcomm):
        assert self._journal_ri(natcomm).issue == "1"

    def test_single_page_not_split(self, natcomm):
        ri = self._journal_ri(natcomm)
        assert ri.firstPage == "1065"
        assert ri.lastPage is None

    def test_issn_not_in_related_identifiers(self, natcomm):
        issn_ris = [r for r in natcomm.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.ISSN]
        assert issn_ris == []


class TestJournalContainerOrthodontics:
    @staticmethod
    def _journal_ri(record):
        return next(r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Journal)

    def test_page_range_split(self, orthodontics):
        ri = self._journal_ri(orthodontics)
        assert ri.firstPage == "47"
        assert ri.lastPage == "55"


class TestPubMedFieldsOrthodontics:
    def test_no_pmc_in_alternate_identifiers(self, orthodontics):
        pmc_ids = [a for a in orthodontics.alternateIdentifiers if a.alternateIdentifierType == "PMCID"]
        assert pmc_ids == []


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_natcomm_validates_against_schema(self, natcomm):
        jsonschema.validate(
            instance=natcomm.to_dict(),
            schema=PubMedContainer.json_schema(),
        )

    def test_orthodontics_validates_against_schema(self, orthodontics):
        jsonschema.validate(
            instance=orthodontics.to_dict(),
            schema=PubMedContainer.json_schema(),
        )



# ---------------------------------------------------------------------------
# Batch parsing
# ---------------------------------------------------------------------------

class TestBatchParsing:
    @pytest.fixture(scope="class")
    def batch_records(self):
        xml = BATCH_FIXTURE.read_text()
        return asyncio.run(PubMedHarvester()._parse_many(xml))

    def test_both_pmids_present(self, batch_records):
        assert "33594067" in batch_records
        assert "32672655" in batch_records

    def test_natcomm_title(self, batch_records):
        record = batch_records["33594067"]
        assert "LIGHT" in record.titles[0].title

    def test_orthodontics_structured_abstract(self, batch_records):
        record = batch_records["32672655"]
        assert "BACKGROUND:" in record.descriptions[0].description

    def test_parse_item_round_trip(self, natcomm):
        """Raw XML items split from a batch response can be re-parsed from cache."""
        harvester = PubMedHarvester()
        raw_items = asyncio.run(harvester._split_batch(NATCOMM_FIXTURE.read_text(), []))
        raw = raw_items["33594067"]
        restored = asyncio.run(harvester._parse_item(raw))
        assert restored.titles[0].title == natcomm.titles[0].title


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# PubmedBookArticle — PMID 21413253 (book chapter, Medical Microbiology)
# ---------------------------------------------------------------------------

class TestBookArticle:
    @pytest.fixture(scope="class")
    def record(self) -> PubMedContainer:
        return _parse(BOOK_ARTICLE_FIXTURE.read_text())

    def test_split_batch_includes_book_article(self):
        harvester = PubMedHarvester()
        result = asyncio.run(harvester._split_batch(BOOK_ARTICLE_FIXTURE.read_text(), []))
        assert "21413253" in result

    def test_resource_type_is_book_chapter(self, record):
        assert record.resourceType is not None
        assert record.resourceType.resourceTypeGeneral == ResourceTypeGeneral.BookChapter

    def test_title_is_chapter_title(self, record):
        assert "Alphaviruses" in record.titles[0].title
        assert "Flaviviruses" in record.titles[0].title

    def test_chapter_authors_not_editors(self, record):
        names = [c.name for c in record.creators]
        assert "Schmaljohn, Alan L." in names
        assert "McClain, David" in names
        # Baron is the book editor, not a chapter author
        assert not any("Baron" in n for n in names)

    def test_creator_count(self, record):
        assert len(record.creators) == 2

    def test_publisher_from_book(self, record):
        assert "Texas" in record.publisher.name

    def test_publication_year(self, record):
        assert record.publicationYear == "1996"

    def test_pmid_in_alternate_identifiers(self, record):
        pmids = [a.alternateIdentifier for a in record.alternateIdentifiers
                 if a.alternateIdentifierType == "PMID"]
        assert "21413253" in pmids

    def test_bookaccession_in_alternate_identifiers(self, record):
        accs = [a.alternateIdentifier for a in record.alternateIdentifiers
                if a.alternateIdentifierType == "NCBI Bookshelf"]
        assert "NBK7633" in accs

    def test_book_container_related_item(self, record):
        books = [r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Book]
        assert len(books) == 1
        assert books[0].relationType == RelationType.IsPublishedIn

    def test_isbn_in_book_container(self, record):
        books = [r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Book]
        assert books[0].relatedItemIdentifier is not None
        assert books[0].relatedItemIdentifier.relatedItemIdentifierType == RelatedIdentifierType.ISBN

    def test_abstract_present(self, record):
        assert record.descriptions[0].description is not None
        assert "alphavirus" in record.descriptions[0].description.lower()

    def test_validates_against_schema(self, record):
        import jsonschema
        jsonschema.validate(
            instance=record.to_dict(),
            schema=PubMedContainer.json_schema(),
        )


class TestErrorHandling:
    def test_no_pubmed_article_returns_empty(self):
        harvester = PubMedHarvester()
        result = asyncio.run(harvester._split_batch("<PubmedArticleSet></PubmedArticleSet>", []))
        assert result == {}

    def test_malformed_xml_raises(self):
        harvester = PubMedHarvester()
        with pytest.raises(ET.ParseError):
            asyncio.run(harvester._split_batch("not xml at all", []))
