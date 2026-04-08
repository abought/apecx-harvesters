"""
Unit tests for the Crossref harvester.

Two fixture payloads are exercised:
- ``crossref_10.1101_2020.09.09.20191205.json`` — a preprint (posted-content)
- ``crossref_10.1128_mbio.01735-25.json``        — a journal article

Tests exercise `_parse()` without making network requests.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest

from apecx_harvesters.loaders.crossref import CrossrefHarvester
from apecx_harvesters.loaders.crossref.parser import _parse_work
from apecx_harvesters.loaders.base import DataCite, DateType, RelatedIdentifierType, RelatedItemType, RelationType


def _parse(data: dict) -> DataCite:
    return _parse_work(data["message"])

FIXTURE_DIR = Path(__file__).parent / "fixtures"
PREPRINT_FIXTURE = FIXTURE_DIR / "crossref_10.1101_2020.09.09.20191205.json"
ARTICLE_FIXTURE = FIXTURE_DIR / "crossref_10.1128_mbio.01735-25.json"
BATCH_FIXTURE = FIXTURE_DIR / "crossref_batch.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def preprint_data() -> dict:
    return json.loads(PREPRINT_FIXTURE.read_text())


@pytest.fixture(scope="module")
def preprint(preprint_data) -> DataCite:
    return _parse_work(preprint_data["message"])


@pytest.fixture(scope="module")
def article_data() -> dict:
    return json.loads(ARTICLE_FIXTURE.read_text())


@pytest.fixture(scope="module")
def article(article_data) -> DataCite:
    return _parse_work(article_data["message"])


# ---------------------------------------------------------------------------
# Preprint: top-level fields
# ---------------------------------------------------------------------------

class TestPreprintContainer:
    def test_title(self, preprint):
        assert preprint.titles[0].title == "Evolution of immunity to SARS-CoV-2"

    def test_publisher_uses_institution_name(self, preprint):
        # 'openRxiv' is the generic publisher; 'medRxiv' comes from institution[]
        assert preprint.publisher.name == "medRxiv"

    def test_doi_in_identifiers(self, preprint):
        assert preprint.identifier is not None
        assert preprint.identifier.identifier == "10.1101/2020.09.09.20191205"
        assert preprint.identifier.identifierType == "DOI"


# ---------------------------------------------------------------------------
# Preprint: abstract (JATS stripping)
# ---------------------------------------------------------------------------

class TestPreprintAbstract:
    def test_abstract_is_plain_text(self, preprint):
        desc = preprint.descriptions[0].description
        assert "<jats:" not in desc
        assert "</" not in desc

    def test_abstract_content(self, preprint):
        desc = preprint.descriptions[0].description
        assert "durability of infection-induced SARS-CoV-2 immunity" in desc

    def test_abstract_no_leading_trailing_whitespace(self, preprint):
        assert preprint.descriptions[0].description == preprint.descriptions[0].description.strip()


# ---------------------------------------------------------------------------
# Preprint: creators
# ---------------------------------------------------------------------------

class TestPreprintCreators:
    def test_creator_count(self, preprint):
        assert len(preprint.creators) == 4

    def test_first_creator_name(self, preprint):
        first = preprint.creators[0]
        assert first.familyName == "Wheatley"
        assert first.givenName == "Adam K."

    def test_name_field_formatted(self, preprint):
        assert preprint.creators[0].name == "Wheatley, Adam K."

    def test_orcid_captured(self, preprint):
        juno = next(c for c in preprint.creators if c.familyName == "Juno")
        assert len(juno.nameIdentifiers) == 1
        assert juno.nameIdentifiers[0].nameIdentifier == "0000-0002-9072-1017"

    def test_creator_without_orcid_has_empty_identifiers(self, preprint):
        wheatley = preprint.creators[0]
        assert wheatley.nameIdentifiers == []

    def test_no_affiliation_when_empty_array(self, preprint):
        assert preprint.creators[0].affiliation is None


# ---------------------------------------------------------------------------
# Preprint: dates
# ---------------------------------------------------------------------------

class TestPreprintDates:
    def test_submitted_date_from_posted(self, preprint):
        submitted = next(d for d in preprint.dates if d.dateType == DateType.Submitted)
        assert submitted.date == "2020-09-10T00:00:00Z"

    def test_no_created_date_for_preprint(self, preprint):
        created = [d for d in preprint.dates if d.dateType == DateType.Created]
        assert created == []


# ---------------------------------------------------------------------------
# Preprint: subjects
# ---------------------------------------------------------------------------

class TestPreprintSubjects:
    def test_group_title_captured_as_subject(self, preprint):
        subjects = [s.subject for s in preprint.subjects]
        assert "Infectious Diseases (except HIV/AIDS)" in subjects


# ---------------------------------------------------------------------------
# Preprint: related identifiers
# ---------------------------------------------------------------------------

class TestPreprintRelatedIdentifiers:
    def test_is_previous_version_of_published(self, preprint):
        ri = next(
            r for r in preprint.relatedIdentifiers
            if r.relationType == RelationType.IsPreviousVersionOf
        )
        assert ri.relatedIdentifier == "10.1038/s41467-021-21444-5"
        assert ri.relatedIdentifierType == RelatedIdentifierType.DOI


# ---------------------------------------------------------------------------
# Journal article: top-level fields
# ---------------------------------------------------------------------------

class TestArticleContainer:
    def test_title(self, article):
        assert "ACE-2-like enzymatic activity" in article.titles[0].title

    def test_publisher_from_publisher_field(self, article):
        assert article.publisher.name == "American Society for Microbiology"

    def test_doi(self, article):
        assert article.identifier is not None
        assert article.identifier.identifier == "10.1128/mbio.01735-25"


# ---------------------------------------------------------------------------
# Journal article: creators and ORCIDs
# ---------------------------------------------------------------------------

class TestArticleCreators:
    def test_creator_count(self, article):
        assert len(article.creators) == 4

    def test_affiliation_captured(self, article):
        song = article.creators[0]
        assert song.affiliation is not None
        assert "University of Virginia" in song.affiliation.name

    def test_orcid_on_first_author(self, article):
        assert article.creators[0].nameIdentifiers[0].nameIdentifier == "0009-0001-6788-1717"

    def test_author_without_orcid(self, article):
        zeichner = next(c for c in article.creators if c.familyName == "Zeichner")
        assert zeichner.nameIdentifiers == []


# ---------------------------------------------------------------------------
# Journal article: abstract
# ---------------------------------------------------------------------------

class TestArticleAbstract:
    def test_jats_tags_stripped(self, article):
        desc = article.descriptions[0].description
        assert "<jats:" not in desc
        assert "ABSTRACT" in desc or "COVID-19" in desc

    def test_multiple_sections_merged(self, article):
        desc = article.descriptions[0].description
        assert "IMPORTANCE" in desc
        assert "long COVID" in desc.lower() or "LC" in desc


# ---------------------------------------------------------------------------
# Journal article: dates
# ---------------------------------------------------------------------------

class TestArticleDates:
    def test_created_date_from_published_print(self, article):
        created = next(d for d in article.dates if d.dateType == DateType.Created)
        assert created.date == "2025-08-13T00:00:00Z"

    def test_no_submitted_date_for_journal_article(self, article):
        submitted = [d for d in article.dates if d.dateType == DateType.Submitted]
        assert submitted == []


# ---------------------------------------------------------------------------
# Journal article: license
# ---------------------------------------------------------------------------

class TestArticleRights:
    def test_vor_license_selected(self, article):
        assert len(article.rightsList) == 1
        assert article.rightsList[0].rightsUri == "https://creativecommons.org/licenses/by/4.0/"

    def test_tdm_license_excluded(self, article):
        uris = [r.rightsUri for r in article.rightsList]
        assert "https://journals.asm.org/non-commercial-tdm-license" not in uris


# ---------------------------------------------------------------------------
# Journal article: funding
# ---------------------------------------------------------------------------

class TestArticleFunding:
    def test_funder_count(self, article):
        # 3 funders without awards + 3 funder/award pairs = 6 entries
        assert len(article.fundingReferences) == 6

    def test_funder_name(self, article):
        names = [f.funderName for f in article.fundingReferences]
        assert "National Institute of Allergy and Infectious Diseases" in names

    def test_award_number_captured(self, article):
        awards = {f.awardNumber for f in article.fundingReferences if f.awardNumber}
        assert "AI176515" in awards
        assert "AI160334" in awards
        assert "AI178669" in awards

    def test_funder_without_award_has_no_award_number(self, article):
        manning = next(f for f in article.fundingReferences if "Manning" in f.funderName)
        assert manning.awardNumber is None


# ---------------------------------------------------------------------------
# Journal article: related identifiers
# ---------------------------------------------------------------------------

class TestArticleRelatedIdentifiers:
    def test_has_preprint_is_new_version_of(self, article):
        ri = next(
            r for r in article.relatedIdentifiers
            if r.relationType == RelationType.IsNewVersionOf
        )
        assert ri.relatedIdentifier == "10.1101/2025.02.12.25322167"
        assert ri.relatedIdentifierType == RelatedIdentifierType.DOI


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_preprint_validates_against_schema(self, preprint):
        jsonschema.validate(
            instance=preprint.to_dict(),
            schema=DataCite.json_schema(),
        )

    def test_article_validates_against_schema(self, article):
        jsonschema.validate(
            instance=article.to_dict(),
            schema=DataCite.json_schema(),
        )


# ---------------------------------------------------------------------------
# Journal article: journal container (RelatedItem)
# ---------------------------------------------------------------------------

class TestArticleJournalContainer:
    @staticmethod
    def _journal_ri(record):
        return next(r for r in record.relatedItems if r.relatedItemType == RelatedItemType.Journal)

    def test_journal_related_item_present(self, article):
        assert any(r.relatedItemType == RelatedItemType.Journal for r in article.relatedItems)

    def test_relation_is_published_in(self, article):
        assert self._journal_ri(article).relationType.value == "IsPublishedIn"

    def test_journal_title(self, article):
        ri = self._journal_ri(article)
        assert ri.titles[0].title == "mBio"

    def test_issn(self, article):
        ident = self._journal_ri(article).relatedItemIdentifier
        assert ident is not None
        assert ident.relatedItemIdentifier == "2150-7511"
        assert ident.relatedItemIdentifierType == RelatedIdentifierType.ISSN

    def test_volume(self, article):
        assert self._journal_ri(article).volume == "16"

    def test_issue(self, article):
        assert self._journal_ri(article).issue == "8"

    def test_preprint_has_no_journal_container(self, preprint):
        journal_items = [r for r in preprint.relatedItems if r.relatedItemType == RelatedItemType.Journal]
        assert journal_items == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_missing_message_key_raises(self):
        with pytest.raises(KeyError):
            _parse({"status": "ok"})

    def test_no_abstract_produces_no_description(self, preprint_data):
        import copy
        data = copy.deepcopy(preprint_data)
        del data["message"]["abstract"]
        record = _parse(data)
        assert record.titles[0].title == "Evolution of immunity to SARS-CoV-2"
        assert record.descriptions == []

    def test_unknown_relation_type_skipped(self, article_data):
        import copy
        data = copy.deepcopy(article_data)
        data["message"]["relation"]["is-supplemented-by"] = [
            {"id-type": "doi", "id": "10.9999/supplement", "asserted-by": "subject"}
        ]
        record = _parse(data)
        ris = [r.relatedIdentifier for r in record.relatedIdentifiers]
        assert "10.9999/supplement" not in ris

    def test_no_license_produces_empty_rights(self, preprint_data):
        import copy
        data = copy.deepcopy(preprint_data)
        data["message"].pop("license", None)
        record = _parse(data)
        assert record.rightsList == []


# ---------------------------------------------------------------------------
# Batch parsing
# ---------------------------------------------------------------------------

class TestBatchParsing:
    @pytest.fixture(scope="class")
    def batch_records(self):
        content = BATCH_FIXTURE.read_text()
        return asyncio.run(CrossrefHarvester()._parse_many(content))

    def test_both_dois_present(self, batch_records):
        assert "10.1101/2020.09.09.20191205" in batch_records
        assert "10.1128/mbio.01735-25" in batch_records

    def test_preprint_title(self, batch_records):
        record = batch_records["10.1101/2020.09.09.20191205"]
        assert "SARS-CoV-2" in record.titles[0].title

    def test_article_title(self, batch_records):
        record = batch_records["10.1128/mbio.01735-25"]
        assert record.titles[0].title != ""

    def test_parse_item_round_trip(self, preprint):
        """Raw items split from a batch response can be re-parsed from cache."""
        harvester = CrossrefHarvester()
        raw_items = asyncio.run(harvester._split_batch(BATCH_FIXTURE.read_text(), []))
        raw = raw_items["10.1101/2020.09.09.20191205"]
        restored = asyncio.run(harvester._parse_item(raw))
        assert restored.titles[0].title == preprint.titles[0].title
