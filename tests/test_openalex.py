"""
Unit tests for the OpenAlex harvester.

Fixture: ``openalex_10.1038_s41467-021-21317-x.json`` — Nature Communications
article on hematopoietic stem cells.  Key characteristics:
- 8 authors (5 with ORCIDs), all Swiss institutions
- Abstract reconstructed from inverted index
- CC-BY license in primary_location
- PMID in ids.pmid
- 3 topics + 3 keywords → subjects
- 2 funders (no award numbers)
- ``language: "en"``
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest

from apecx_harvesters.loaders.openalex import OpenAlexHarvester
from apecx_harvesters.loaders.openalex.parser import _parse_work
from apecx_harvesters.loaders.base import DataCite, DateType, RelatedIdentifierType, RelatedItemType


def _parse(data: dict) -> DataCite:
    return _parse_work(data)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURE_DIR / "openalex_10.1038_s41467-021-21317-x.json"
BATCH_FIXTURE = FIXTURE_DIR / "openalex_batch.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def record(payload) -> DataCite:
    return _parse_work(payload)


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

class TestContainer:
    def test_title(self, record):
        assert "LIGHT" in record.titles[0].title
        assert "hematopoietic" in record.titles[0].title

    def test_publisher_from_source(self, record):
        assert record.publisher.name == "Nature Communications"

    def test_language(self, record):
        assert record.language == "en"


# ---------------------------------------------------------------------------
# Creators — display_name only, no given/family split
# ---------------------------------------------------------------------------

class TestCreators:
    def test_creator_count(self, record):
        assert len(record.creators) == 8

    def test_first_creator_name(self, record):
        assert record.creators[0].name == "Sabine Höpner"

    def test_no_given_name(self, record):
        # OpenAlex does not split given/family
        assert record.creators[0].givenName is None
        assert record.creators[0].familyName is None

    def test_affiliation_from_first_institution(self, record):
        assert record.creators[0].affiliation is not None
        assert "Bern" in record.creators[0].affiliation.name

    def test_orcid_captured(self, record):
        radpour = next(c for c in record.creators if c.name == "Ramin Radpour")
        assert len(radpour.nameIdentifiers) == 1
        assert radpour.nameIdentifiers[0].nameIdentifier == "0000-0002-5632-7833"

    def test_creator_without_orcid(self, record):
        hopner = record.creators[0]
        assert hopner.nameIdentifiers == []

    def test_orcid_authors_count(self, record):
        orcid_authors = [c for c in record.creators if c.nameIdentifiers]
        assert len(orcid_authors) == 6


# ---------------------------------------------------------------------------
# Abstract — reconstructed from inverted index
# ---------------------------------------------------------------------------

class TestAbstract:
    def test_abstract_present(self, record):
        assert record.descriptions[0].description is not None

    def test_abstract_content(self, record):
        desc = record.descriptions[0].description
        assert "hematopoietic" in desc.lower()
        assert "stem" in desc.lower()

    def test_abstract_starts_with_the(self, record):
        # Position 0 in the inverted index is "The"
        assert record.descriptions[0].description.startswith("The ")

    def test_no_xml_or_markup(self, record):
        assert "<" not in record.descriptions[0].description


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

class TestDates:
    def test_created_from_publication_date(self, record):
        created = next(d for d in record.dates if d.dateType == DateType.Created)
        assert created.date == "2021-02-16T00:00:00Z"

    def test_no_submitted_date(self, record):
        assert [d for d in record.dates if d.dateType == DateType.Submitted] == []


# ---------------------------------------------------------------------------
# Rights
# ---------------------------------------------------------------------------

class TestRights:
    def test_cc_by_license_captured(self, record):
        assert len(record.rightsList) == 1
        assert record.rightsList[0].rightsIdentifier == "cc-by"

    def test_rights_label(self, record):
        assert "Creative Commons Attribution" in record.rightsList[0].rights

    def test_rights_uri(self, record):
        assert "creativecommons.org" in record.rightsList[0].rightsUri


# ---------------------------------------------------------------------------
# Related identifiers — DOI (self) + PMID
# ---------------------------------------------------------------------------

class TestIdentifiers:
    def test_doi_in_identifiers(self, record):
        assert record.identifier is not None
        assert record.identifier.identifier == "10.1038/s41467-021-21317-x"
        assert record.identifier.identifierType == "DOI"

    def test_pmid_in_alternate_identifiers(self, record):
        pmid_ai = next(a for a in record.alternateIdentifiers if a.alternateIdentifierType == "PMID")
        assert pmid_ai.alternateIdentifier == "33594067"

    def test_no_self_doi_in_related_identifiers(self, record):
        doi_ris = [r for r in record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.DOI]
        assert doi_ris == []


# ---------------------------------------------------------------------------
# Subjects — topics then keywords, deduplicated
# ---------------------------------------------------------------------------

class TestSubjects:
    def test_topics_captured(self, record):
        terms = {s.subject for s in record.subjects}
        assert "Hematopoietic Stem Cell Transplantation" in terms

    def test_keywords_captured(self, record):
        terms = {s.subject for s in record.subjects}
        assert "Stem cell" in terms

    def test_total_subject_count(self, record):
        # 3 topics + 3 keywords (no overlap) = 6
        assert len(record.subjects) == 6


# ---------------------------------------------------------------------------
# Funding
# ---------------------------------------------------------------------------

class TestFunding:
    def test_funders_captured(self, record):
        names = [f.funderName for f in record.fundingReferences]
        assert any("Nationalfonds" in n for n in names)
        assert any("Krebsliga" in n for n in names)

    def test_no_award_numbers(self, record):
        assert all(f.awardNumber is None for f in record.fundingReferences)

    def test_funder_count(self, record):
        assert len(record.fundingReferences) == 2


# ---------------------------------------------------------------------------
# Journal container (RelatedItem)
# ---------------------------------------------------------------------------

class TestJournalContainer:
    @staticmethod
    def _journal_ri(record):
        return next(r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Journal)

    def test_journal_related_item_present(self, record):
        assert any(r.relatedItemType == RelatedItemType.Journal for r in record.relatedItems)

    def test_journal_title(self, record):
        ri = self._journal_ri(record)
        assert ri.titles[0].title == "Nature Communications"

    def test_issn(self, record):
        ident = self._journal_ri(record).relatedItemIdentifier
        assert ident is not None
        assert ident.relatedItemIdentifier == "2041-1723"
        assert ident.relatedItemIdentifierType == RelatedIdentifierType.ISSN

    def test_volume(self, record):
        assert self._journal_ri(record).volume == "12"

    def test_issue(self, record):
        assert self._journal_ri(record).issue == "1"

    def test_first_page(self, record):
        assert self._journal_ri(record).firstPage == "1065"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_validates_against_schema(self, record):
        jsonschema.validate(
            instance=record.to_dict(),
            schema=DataCite.json_schema(),
        )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_no_abstract_inverted_index(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["abstract_inverted_index"]
        record = _parse(p)
        assert record.descriptions == []

    def test_no_pmid(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["ids"]["pmid"]
        record = _parse(p)
        pmid_ris = [r for r in record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.PMID]
        assert pmid_ris == []

    def test_unknown_license_id_falls_back_to_raw_string(self, payload):
        import copy
        p = copy.deepcopy(payload)
        p["primary_location"]["license"] = "custom-license"
        record = _parse(p)
        assert record.rightsList[0].rights == "custom-license"
        assert record.rightsList[0].rightsUri is None


# ---------------------------------------------------------------------------
# Batch parsing
# ---------------------------------------------------------------------------

class TestBatchParsing:
    @pytest.fixture(scope="class")
    def batch_records(self):
        content = BATCH_FIXTURE.read_text()
        return asyncio.run(OpenAlexHarvester()._parse_many(content))

    def test_doi_present(self, batch_records):
        assert "10.1038/s41467-021-21317-x" in batch_records

    def test_title(self, batch_records):
        record = batch_records["10.1038/s41467-021-21317-x"]
        assert "LIGHT" in record.titles[0].title

    def test_abstract_reconstructed(self, batch_records):
        record = batch_records["10.1038/s41467-021-21317-x"]
        assert record.descriptions != []
        assert "hematopoietic" in record.descriptions[0].description.lower()

    def test_parse_item_round_trip(self, record):
        """Raw items split from a batch response can be re-parsed from cache."""
        harvester = OpenAlexHarvester()
        raw_items = asyncio.run(harvester._split_batch(BATCH_FIXTURE.read_text(), []))
        raw = raw_items["10.1038/s41467-021-21317-x"]
        restored = asyncio.run(harvester._parse_item(raw))
        assert restored.titles[0].title == record.titles[0].title
