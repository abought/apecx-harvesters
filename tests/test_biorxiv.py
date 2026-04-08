"""
Unit tests for the bioRxiv/medRxiv harvester and schema.

Tests exercise `_parse()` against a captured fixture payload without making
network requests.  The fixture (medrxiv_2020.09.09.20191205.json) is a real
two-version medRxiv response for doi 10.1101/2020.09.09.20191205.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from apecx_harvesters.loaders.biorxiv import BiorxivHarvester
from apecx_harvesters.loaders.base import DateType, RelatedIdentifierType, RelationType
from apecx_harvesters.loaders.biorxiv import BiorXivContainer


def _parse(data: dict) -> BiorXivContainer:
    return asyncio.run(BiorxivHarvester()._parse_item(json.dumps(data)))

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "medrxiv_2020.09.09.20191205.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def raw_data() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture(scope="module")
def record(raw_data) -> BiorXivContainer:
    return asyncio.run(BiorxivHarvester()._parse_item(json.dumps(raw_data)))


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

class TestContainer:
    def test_title(self, record):
        assert record.titles[0].title == "Evolution of immunity to SARS-CoV-2"

    def test_abstract(self, record):
        assert record.descriptions[0].description.startswith(
            "The durability of infection-induced SARS-CoV-2 immunity"
        )

    def test_publisher(self, record):
        assert record.publisher.name == "medRxiv"

    def test_version(self, record):
        assert record.version == "2"


# ---------------------------------------------------------------------------
# Creators
# ---------------------------------------------------------------------------

class TestCreators:
    def test_creator_count(self, record):
        assert len(record.creators) == 22

    def test_first_creator(self, record):
        first = record.creators[0]
        assert first.familyName == "Wheatley"
        assert first.givenName == "A. K."

    def test_last_creator(self, record):
        last = record.creators[-1]
        assert last.familyName == "Kent"
        assert last.givenName == "S. J."

    def test_corresponding_author_has_affiliation(self, record):
        # Stephen J Kent -> familyName="Kent", givenName="S. J."
        kent_sj = next(
            c for c in record.creators
            if c.familyName == "Kent" and c.givenName is not None
            and c.givenName.upper().startswith("S")
        )
        assert kent_sj.affiliation is not None
        assert kent_sj.affiliation.name == "University of Melbourne"

    def test_non_corresponding_kent_has_no_affiliation(self, record):
        # H. E. Kent is a different author and should have no affiliation
        kent_he = next(
            c for c in record.creators
            if c.familyName == "Kent" and c.givenName is not None
            and c.givenName.upper().startswith("H")
        )
        assert kent_he.affiliation is None

    def test_creator_name_field(self, record):
        first = record.creators[0]
        assert first.name == "Wheatley, A. K."


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

class TestDates:
    def test_two_dates(self, record):
        assert len(record.dates) == 2

    def test_submitted_date(self, record):
        submitted = next(d for d in record.dates if d.dateType == DateType.Submitted)
        assert submitted.date == "2020-09-10T00:00:00Z"

    def test_updated_date(self, record):
        updated = next(d for d in record.dates if d.dateType == DateType.Updated)
        assert updated.date == "2020-09-11T00:00:00Z"


# ---------------------------------------------------------------------------
# Subjects
# ---------------------------------------------------------------------------

class TestSubjects:
    def test_one_subject(self, record):
        assert len(record.subjects) == 1

    def test_subject_value(self, record):
        assert record.subjects[0].subject == "infectious diseases"


# ---------------------------------------------------------------------------
# Rights (license)
# ---------------------------------------------------------------------------

class TestRights:
    def test_one_rights_entry(self, record):
        assert len(record.rightsList) == 1

    def test_rights_value(self, record):
        assert record.rightsList[0].rights == "cc_no"


# ---------------------------------------------------------------------------
# Funding (funder = "NA" in fixture -> empty list)
# ---------------------------------------------------------------------------

class TestFunding:
    def test_no_funding_when_na(self, record):
        assert record.fundingReferences == []


# ---------------------------------------------------------------------------
# Related identifiers
# ---------------------------------------------------------------------------

class TestRelatedIdentifiers:
    def test_preprint_doi_in_identifiers(self, record):
        assert record.identifier is not None
        assert record.identifier.identifier == "10.1101/2020.09.09.20191205"
        assert record.identifier.identifierType == "DOI"

    def test_one_related_identifier(self, record):
        # only IsPreviousVersionOf for the published version; preprint DOI is now in identifiers
        assert len(record.relatedIdentifiers) == 1

    def test_published_doi_is_previous_version_of(self, record):
        ri = next(
            r for r in record.relatedIdentifiers
            if r.relationType == RelationType.IsPreviousVersionOf
        )
        assert ri.relatedIdentifier == "10.1038/s41467-021-21444-5"
        assert ri.relatedIdentifierType == RelatedIdentifierType.DOI


# ---------------------------------------------------------------------------
# BiorXivFields (domain-specific nested fields)
# ---------------------------------------------------------------------------

class TestBiorXivFields:
    def test_server(self, record):
        assert record.biorxiv.server == "medRxiv"

    def test_publication_type(self, record):
        assert record.biorxiv.publication_type == "PUBLISHAHEADOFPRINT"

    def test_corresponding_author_in_contributors(self, record):
        assert len(record.contributors) == 1
        assert record.contributors[0].name == "Stephen J Kent"

    def test_jats_xml_url(self, record):
        assert record.biorxiv.jats_xml_url == (
            "https://www.medrxiv.org/content/early/2020/09/11/"
            "2020.09.09.20191205.source.xml"
        )


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_to_dict_validates_against_biorxiv_schema(self, record):
        jsonschema.validate(
            instance=record.to_dict(),
            schema=BiorXivContainer.json_schema(),
        )



# ---------------------------------------------------------------------------
# Single-version record (only Submitted date, no Updated)
# ---------------------------------------------------------------------------

class TestSingleVersion:
    def test_only_submitted_date(self, raw_data):
        single_version = {"messages": raw_data["messages"], "collection": [raw_data["collection"][0]]}
        rec = _parse(single_version)
        assert len(rec.dates) == 1
        assert rec.dates[0].dateType == DateType.Submitted


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_missing_collection_key_raises(self):
        with pytest.raises(KeyError):
            _parse({"messages": [{"status": "ok"}]})

    def test_invalid_biorxiv_fields_raises(self, raw_data):
        import copy
        bad = copy.deepcopy(raw_data)
        del bad["collection"][-1]["server"]
        with pytest.raises((KeyError, ValidationError)):
            _parse(bad)
