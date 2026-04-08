"""
Unit tests for the DataCite DOI harvester.

Fixture: ``datacite_10.14454_qdd3-ps68.json`` — DataCite-registered DOI for
the DataCite Metadata Schema v4.7 documentation.  Key characteristics:
- Single organizational creator (no given/family name split)
- ``dateType: "Issued"`` (mapped to ``DateType.Created``)
- CC-BY 4.0 license in ``rightsList``
- Two related identifiers (Documents, IsNewVersionOf)
- Empty fundingReferences and subjects
- ``language: "en"``
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from apecx_harvesters.loaders.datacite import DataCiteHarvester
from apecx_harvesters.loaders.base import (
    ContributorType,
    DataCite,
    DateType,
    DescriptionType,
    RelatedIdentifierType,
    RelatedItemType,
    RelationType,
    ResourceTypeGeneral,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURE_DIR / "datacite_10.14454_qdd3-ps68.json"
DATASET_FIXTURE = FIXTURE_DIR / "datacite_10.82433_9184-dy35.json"
ARTICLE_FIXTURE = FIXTURE_DIR / "datacite_10.82433_q54d-pf76.json"
SOFTWARE_FIXTURE = FIXTURE_DIR / "datacite_10.5281_zenodo.7635478.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def record(payload) -> DataCite:
    return asyncio.run(DataCiteHarvester()._parse_item(json.dumps(payload)))


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

class TestContainer:
    def test_title(self, record):
        assert "DataCite Metadata Schema" in record.titles[0].title
        assert "4.7" in record.titles[0].title

    def test_publisher(self, record):
        assert record.publisher.name == "DataCite"

    def test_version(self, record):
        assert record.version == "4.7"

    def test_language(self, record):
        assert record.language == "en"

    def test_resource_type(self, record):
        assert record.resourceType is not None
        assert record.resourceType.resourceTypeGeneral == ResourceTypeGeneral.Text
        assert record.resourceType.resourceType == "Documentation"


# ---------------------------------------------------------------------------
# Creators — organizational (no given/family)
# ---------------------------------------------------------------------------

class TestCreators:
    def test_creator_count(self, record):
        assert len(record.creators) == 1

    def test_organizational_name(self, record):
        assert record.creators[0].name == "DataCite Metadata Working Group"

    def test_no_given_name(self, record):
        assert record.creators[0].givenName is None

    def test_no_family_name(self, record):
        assert record.creators[0].familyName is None

    def test_no_name_identifiers(self, record):
        assert record.creators[0].nameIdentifiers == []

    def test_no_affiliation(self, record):
        assert record.creators[0].affiliation is None


# ---------------------------------------------------------------------------
# Dates — "Issued" → DateType.Created
# ---------------------------------------------------------------------------

class TestDates:
    def test_issued_date_preserved(self, record):
        issued = next(d for d in record.dates if d.dateType == DateType.Issued)
        assert issued is not None

    def test_date_value(self, record):
        issued = next(d for d in record.dates if d.dateType == DateType.Issued)
        assert issued.date == "2026-03-03T00:00:00Z"

    def test_no_submitted_date(self, record):
        assert [d for d in record.dates if d.dateType == DateType.Submitted] == []


# ---------------------------------------------------------------------------
# Rights
# ---------------------------------------------------------------------------

class TestRights:
    def test_rights_captured(self, record):
        assert len(record.rightsList) == 1

    def test_rights_label(self, record):
        assert record.rightsList[0].rights == "Creative Commons Attribution 4.0 International"

    def test_rights_uri(self, record):
        assert "creativecommons.org" in record.rightsList[0].rightsUri

    def test_rights_identifier(self, record):
        assert record.rightsList[0].rightsIdentifier == "cc-by-4.0"


# ---------------------------------------------------------------------------
# Related identifiers
# ---------------------------------------------------------------------------

class TestRelatedIdentifiers:
    def test_doi_in_identifiers(self, record):
        assert record.identifier is not None
        assert record.identifier.identifier == "10.14454/qdd3-ps68"
        assert record.identifier.identifierType == "DOI"

    def test_documents_relation(self, record):
        docs = next(r for r in record.relatedIdentifiers if r.relationType == RelationType.Documents)
        assert docs.relatedIdentifier == "10.14454/28a4-kd32"
        assert docs.relatedIdentifierType == RelatedIdentifierType.DOI

    def test_is_new_version_of_relation(self, record):
        new_ver = next(r for r in record.relatedIdentifiers if r.relationType == RelationType.IsNewVersionOf)
        assert new_ver.relatedIdentifier == "10.14454/mzv1-5b55"

    def test_total_related_identifier_count(self, record):
        # Documents + IsNewVersionOf = 2 (DOI self-identifier is now in identifiers)
        assert len(record.relatedIdentifiers) == 2


# ---------------------------------------------------------------------------
# Subjects and funding
# ---------------------------------------------------------------------------

class TestSubjectsAndFunding:
    def test_no_subjects(self, record):
        assert record.subjects == []

    def test_no_funding(self, record):
        assert record.fundingReferences == []


# ---------------------------------------------------------------------------
# Descriptions — TableOfContents type preserved
# ---------------------------------------------------------------------------

class TestDescriptions:
    def test_description_present(self, record):
        assert len(record.descriptions) == 1

    def test_description_content(self, record):
        assert "DataCite" in record.descriptions[0].description

    def test_description_type(self, record):
        assert record.descriptions[0].descriptionType == DescriptionType.TableOfContents


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
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_missing_data_key_raises(self):
        with pytest.raises((KeyError, ValidationError)):
            asyncio.run(DataCiteHarvester()._parse_item("{}"))

    def test_alternate_identifiers_parsed(self, payload):
        import copy
        p = copy.deepcopy(payload)
        p["data"]["attributes"]["alternateIdentifiers"] = [
            {"alternateIdentifier": "arXiv:2104.00001", "alternateIdentifierType": "arXiv"}
        ]
        record = asyncio.run(DataCiteHarvester()._parse_item(json.dumps(p)))
        assert len(record.alternateIdentifiers) == 1
        assert record.alternateIdentifiers[0].alternateIdentifier == "arXiv:2104.00001"
        assert record.alternateIdentifiers[0].alternateIdentifierType == "arXiv"

    def test_unknown_date_type_raises(self, payload):
        import copy
        p = copy.deepcopy(payload)
        p["data"]["attributes"]["dates"] = [
            {"date": "2026-01-01", "dateType": "UnknownFutureType", "dateInformation": None}
        ]
        with pytest.raises(ValueError, match="UnknownFutureType"):
            asyncio.run(DataCiteHarvester()._parse_item(json.dumps(p)))


# ===========================================================================
# Dataset fixture — 10.82433/9184-DY35
# National Gallery environmental sensor data
# ===========================================================================

@pytest.fixture(scope="module")
def dataset_record() -> DataCite:
    return asyncio.run(DataCiteHarvester()._parse_item(DATASET_FIXTURE.read_text()))


class TestDataset:
    def test_resource_type(self, dataset_record):
        assert dataset_record.resourceType is not None
        assert dataset_record.resourceType.resourceTypeGeneral == ResourceTypeGeneral.Dataset
        assert dataset_record.resourceType.resourceType == "Environmental data"

    def test_publication_year(self, dataset_record):
        assert dataset_record.publicationYear == "2022"

    def test_doi_in_identifier(self, dataset_record):
        assert dataset_record.identifier is not None
        assert dataset_record.identifier.identifier == "10.82433/9184-dy35"

    def test_organizational_creator(self, dataset_record):
        assert dataset_record.creators[0].name == "National Gallery"
        assert dataset_record.creators[0].familyName is None

    def test_formats(self, dataset_record):
        assert dataset_record.formats == ["application/json"]

    def test_version(self, dataset_record):
        assert dataset_record.version == "1.0"

    def test_subjects_captured(self, dataset_record):
        terms = {s.subject for s in dataset_record.subjects}
        assert "temperature" in terms
        assert "relative humidity" in terms

    def test_funding_captured(self, dataset_record):
        ref = dataset_record.fundingReferences[0]
        assert ref.funderName == "H2020 Excellent Science"
        assert ref.awardNumber == "871034"

    def test_contributors_parsed(self, dataset_record):
        assert len(dataset_record.contributors) == 2

    def test_contact_person_contributor(self, dataset_record):
        contact = next(c for c in dataset_record.contributors if c.contributorType == ContributorType.ContactPerson)
        assert contact.familyName == "Padfield"
        assert contact.givenName == "Joseph"

    def test_data_collector_contributor(self, dataset_record):
        collector = next(c for c in dataset_record.contributors if c.contributorType == ContributorType.DataCollector)
        assert collector.name == "Building Facilities Department"

    def test_doi_related_identifiers_parsed(self, dataset_record):
        doi_ris = [r for r in dataset_record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.DOI]
        assert len(doi_ris) == 2  # IsSupplementedBy + IsDocumentedBy

    def test_url_related_identifiers_parsed(self, dataset_record):
        url_ris = [r for r in dataset_record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.URL]
        assert len(url_ris) == 2  # IsSupplementTo + IsSourceOf

    def test_validates_against_schema(self, dataset_record):
        import jsonschema
        jsonschema.validate(instance=dataset_record.to_dict(), schema=DataCite.json_schema())


# ===========================================================================
# Journal article fixture — 10.82433/q54d-pf76
# Example article with relatedItems journal container from API
# ===========================================================================

@pytest.fixture(scope="module")
def article_record() -> DataCite:
    return asyncio.run(DataCiteHarvester()._parse_item(ARTICLE_FIXTURE.read_text()))


class TestArticle:
    def test_resource_type(self, article_record):
        assert article_record.resourceType is not None
        assert article_record.resourceType.resourceTypeGeneral == ResourceTypeGeneral.JournalArticle

    def test_publication_year(self, article_record):
        assert article_record.publicationYear == "2022"

    def test_creator_with_orcid(self, article_record):
        creator = article_record.creators[0]
        assert creator.familyName == "Garcia"
        assert creator.givenName == "Sofia"
        assert len(creator.nameIdentifiers) == 1
        assert creator.nameIdentifiers[0].nameIdentifierScheme == "ORCID"

    def test_journal_related_item_from_api(self, article_record):
        journal_ri = next(
            r for r in article_record.relatedItems
            if r.relatedItemType == RelatedItemType.Journal
        )
        assert journal_ri.titles[0].title == "Journal of Metadata Examples"
        assert journal_ri.volume == "3"
        assert journal_ri.issue == "4"
        assert journal_ri.firstPage == "20"
        assert journal_ri.lastPage == "35"
        assert journal_ri.relatedItemIdentifier.relatedItemIdentifier == "1234-5678"
        assert journal_ri.relatedItemIdentifier.relatedItemIdentifierType == RelatedIdentifierType.ISSN

    def test_validates_against_schema(self, article_record):
        import jsonschema
        jsonschema.validate(instance=article_record.to_dict(), schema=DataCite.json_schema())


# ===========================================================================
# Software fixture — 10.5281/zenodo.7635478
# Zenodo software deposit with multiple descriptions and contributors
# ===========================================================================

@pytest.fixture(scope="module")
def software_record() -> DataCite:
    return asyncio.run(DataCiteHarvester()._parse_item(SOFTWARE_FIXTURE.read_text()))


class TestSoftware:
    def test_resource_type(self, software_record):
        assert software_record.resourceType is not None
        assert software_record.resourceType.resourceTypeGeneral == ResourceTypeGeneral.Software
        assert software_record.resourceType.resourceType is None  # not set in this fixture

    def test_version(self, software_record):
        assert software_record.version == "v1.4"

    def test_publication_year(self, software_record):
        assert software_record.publicationYear == "2023"

    def test_multiple_descriptions(self, software_record):
        types = {d.descriptionType for d in software_record.descriptions}
        assert DescriptionType.Abstract in types
        assert DescriptionType.Other in types

    def test_abstract_content(self, software_record):
        abstract = next(d for d in software_record.descriptions if d.descriptionType == DescriptionType.Abstract)
        assert "mermaid" in abstract.description

    def test_contributors_parsed(self, software_record):
        assert len(software_record.contributors) == 2
        assert all(c.contributorType == ContributorType.Other for c in software_record.contributors)

    def test_contributor_orcid(self, software_record):
        fremout = next(c for c in software_record.contributors if c.familyName == "Fremout")
        assert fremout.nameIdentifiers[0].nameIdentifierScheme == "ORCID"

    def test_funding_references(self, software_record):
        assert len(software_record.fundingReferences) == 3
        names = {f.funderName for f in software_record.fundingReferences}
        assert "European Commission" in names

    def test_funding_award_numbers(self, software_record):
        award_numbers = {f.awardNumber for f in software_record.fundingReferences}
        assert "654028" in award_numbers
        assert "871034" in award_numbers

    def test_is_version_of_relation(self, software_record):
        ver_of = next(
            r for r in software_record.relatedIdentifiers
            if r.relationType == RelationType.IsVersionOf
        )
        assert ver_of.relatedIdentifier == "10.5281/zenodo.4724103"
        assert ver_of.relatedIdentifierType == RelatedIdentifierType.DOI

    def test_multiple_rights(self, software_record):
        assert len(software_record.rightsList) == 2

    def test_gpl_license(self, software_record):
        gpl = next(r for r in software_record.rightsList if r.rightsIdentifier == "gpl-3.0+")
        assert "GNU General Public License" in gpl.rights

    def test_validates_against_schema(self, software_record):
        import jsonschema
        jsonschema.validate(instance=software_record.to_dict(), schema=DataCite.json_schema())
