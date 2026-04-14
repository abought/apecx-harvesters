"""
Unit tests for the EMDB harvester and schema.

Fixture: ``emdb_EMD-74041.json`` — cryo-EM structure of human TRPM8 ion channel.
Key characteristics:
- 4 authors from the primary citation, all with ORCIDs
- No author affiliation available (EMDB citation records lack affiliations)
- 3 comma-separated keywords as subjects
- 3 funding sources (1 without award number, 2 with)
- Citation DOI → IsDescribedBy relation
- PDB cross-reference (9zcu) → IsSourceOf relation (URL type)
- Resolution 2.86 Å by FSC 0.143 CUT-OFF
- Method: singleParticle
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from apecx_harvesters.loaders.emdb import EMDBHarvester
from apecx_harvesters.loaders.base import DateType, RelatedIdentifierType, RelationType
from apecx_harvesters.loaders.emdb import EMDBContainer


def _parse(data: dict) -> EMDBContainer:
    return asyncio.run(EMDBHarvester()._parse_item(json.dumps(data)))

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURE_DIR / "emdb_EMD-74041.json"
BATCH_FIXTURE = FIXTURE_DIR / "emdb_batch_EMD-1000_EMD-74041.json"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def record(payload) -> EMDBContainer:
    return asyncio.run(EMDBHarvester()._parse_item(json.dumps(payload)))


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

class TestContainer:
    def test_title(self, record):
        assert "TRPM8" in record.titles[0].title
        assert "GDN" in record.titles[0].title

    def test_publisher_is_emdb(self, record):
        assert record.publisher.name == "Electron Microscopy Data Bank"


# ---------------------------------------------------------------------------
# Creators — from citation, not admin.authors_list
# ---------------------------------------------------------------------------

class TestCreators:
    def test_creator_count(self, record):
        assert len(record.creators) == 4

    def test_first_creator_name(self, record):
        assert record.creators[0].name == "Choi KY"

    def test_creator_order(self, record):
        names = [c.name for c in record.creators]
        assert names == ["Choi KY", "Lin X", "Cheng Y", "Julius D"]

    def test_no_given_family_split(self, record):
        # EMDB author names cannot be reliably split
        assert record.creators[0].givenName is None
        assert record.creators[0].familyName is None

    def test_no_affiliation(self, record):
        # Citation records in EMDB do not carry affiliation data
        assert all(c.affiliation is None for c in record.creators)

    def test_orcid_captured(self, record):
        choi = record.creators[0]
        assert len(choi.nameIdentifiers) == 1
        ni = choi.nameIdentifiers[0]
        assert ni.nameIdentifier == "0000-0001-9299-3924"
        assert ni.nameIdentifierScheme == "ORCID"
        assert ni.schemeUri == "https://orcid.org"

    def test_all_authors_have_orcid(self, record):
        assert all(len(c.nameIdentifiers) == 1 for c in record.creators)


# ---------------------------------------------------------------------------
# Description — from sample.name
# ---------------------------------------------------------------------------

class TestDescription:
    def test_description_present(self, record):
        assert record.descriptions[0].description is not None

    def test_description_mentions_trpm8(self, record):
        assert "TRPM8" in record.descriptions[0].description


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

class TestDates:
    def test_submitted_from_deposition(self, record):
        submitted = next(d for d in record.dates if d.dateType == DateType.Submitted)
        assert submitted.date == "2025-11-24T00:00:00Z"

    def test_created_from_header_release(self, record):
        created = next(d for d in record.dates if d.dateType == DateType.Created)
        assert created.date == "2026-03-25T00:00:00Z"

    def test_updated_date_present(self, record):
        updated = next(d for d in record.dates if d.dateType == DateType.Updated)
        assert updated is not None

    def test_dates_are_utc(self, record):
        assert all(d.date.endswith("Z") for d in record.dates)


# ---------------------------------------------------------------------------
# Subjects — from admin.keywords
# ---------------------------------------------------------------------------

class TestSubjects:
    def test_keywords_captured(self, record):
        terms = {s.subject for s in record.subjects}
        assert "TRPM8" in terms
        assert "MEMBRANE PROTEIN" in terms

    def test_keyword_count(self, record):
        assert len(record.subjects) == 3

    def test_no_duplicates(self, record):
        terms = [s.subject for s in record.subjects]
        assert len(terms) == len(set(terms))


# ---------------------------------------------------------------------------
# Funding
# ---------------------------------------------------------------------------

class TestFunding:
    def test_funder_count(self, record):
        assert len(record.fundingReferences) == 3

    def test_hhmi_present(self, record):
        names = [f.funderName for f in record.fundingReferences]
        assert any("Howard Hughes" in n for n in names)

    def test_award_number_captured(self, record):
        awards = {f.awardNumber for f in record.fundingReferences if f.awardNumber}
        assert "R35NS105038" in awards
        assert "R35GM140847" in awards

    def test_hhmi_has_no_award_number(self, record):
        hhmi = next(f for f in record.fundingReferences if "Hughes" in f.funderName)
        assert hhmi.awardNumber is None


# ---------------------------------------------------------------------------
# Related identifiers — citation DOI + PDB
# ---------------------------------------------------------------------------

class TestRelatedIdentifiers:
    def test_citation_doi_is_described_by(self, record):
        doi_ri = next(
            r for r in record.relatedIdentifiers
            if r.relatedIdentifierType == RelatedIdentifierType.DOI
        )
        assert doi_ri.relatedIdentifier == "10.1038/s41586-026-10276-2"
        assert doi_ri.relationType == RelationType.IsDescribedBy

    def test_doi_prefix_stripped(self, record):
        doi_ri = next(
            r for r in record.relatedIdentifiers
            if r.relatedIdentifierType == RelatedIdentifierType.DOI
        )
        assert not doi_ri.relatedIdentifier.startswith("doi:")

    def test_pdb_cross_reference_is_source_of(self, record):
        pdb_ri = next(
            r for r in record.relatedIdentifiers
            if r.relatedIdentifierType == RelatedIdentifierType.URL
        )
        assert "9ZCU" in pdb_ri.relatedIdentifier
        assert "rcsb.org" in pdb_ri.relatedIdentifier
        assert pdb_ri.relationType == RelationType.IsSourceOf

    def test_total_related_identifier_count(self, record):
        assert len(record.relatedIdentifiers) == 2


# ---------------------------------------------------------------------------
# EMDBFields
# ---------------------------------------------------------------------------

class TestEMDBFields:
    def test_emdb_id(self, record):
        assert record.emdb.emdb_id == "EMD-74041"

    def test_method(self, record):
        assert record.emdb.method == "singleParticle"

    def test_resolution(self, record):
        assert record.emdb.resolution_angstrom == pytest.approx(2.86)

    def test_resolution_method(self, record):
        assert record.emdb.resolution_method == "FSC 0.143 CUT-OFF"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_validates_against_schema(self, record):
        jsonschema.validate(
            instance=record.to_dict(),
            schema=EMDBContainer.json_schema(),
        )



# ---------------------------------------------------------------------------
# Batch retrieval — _split_batch
# ---------------------------------------------------------------------------

class TestSplitBatch:
    @pytest.fixture(scope="class")
    def batch_raw(self) -> str:
        return BATCH_FIXTURE.read_text()

    @pytest.fixture(scope="class")
    def split(self, batch_raw) -> dict:
        return asyncio.run(EMDBHarvester()._split_batch(batch_raw, []))

    def test_both_ids_present(self, split):
        assert set(split.keys()) == {"EMD-1000", "EMD-74041"}

    def test_each_value_is_json_string(self, split):
        for raw in split.values():
            parsed = json.loads(raw)
            assert isinstance(parsed, dict)

    def test_id_matches_content(self, split):
        for emdb_id, raw in split.items():
            assert json.loads(raw)["emdb_id"] == emdb_id

    def test_entries_parse_successfully(self, split):
        """Confirm the parser accepts the data format returned by the search endpoint."""
        for raw in split.values():
            record = asyncio.run(EMDBHarvester()._parse_item(raw))
            assert isinstance(record, EMDBContainer)

    def test_emdb_74041_id_field(self, split):
        record: EMDBContainer = asyncio.run(EMDBHarvester()._parse_item(split["EMD-74041"]))
        assert record.emdb.emdb_id == "EMD-74041"

    def test_emdb_1000_id_field(self, split):
        record: EMDBContainer = asyncio.run(EMDBHarvester()._parse_item(split["EMD-1000"]))
        assert record.emdb.emdb_id == "EMD-1000"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_missing_emdb_id_raises(self):
        with pytest.raises((KeyError, ValidationError)):
            _parse({})

    def test_no_citation_yields_no_creators(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["crossreferences"]["citation_list"]
        record = _parse(p)
        assert record.creators == []

    def test_no_citation_yields_no_doi(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["crossreferences"]["citation_list"]
        record = _parse(p)
        doi_ris = [r for r in record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.DOI]
        assert doi_ris == []

    def test_no_pdb_list_yields_no_pdb_related(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["crossreferences"]["pdb_list"]
        record = _parse(p)
        url_ris = [r for r in record.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.URL]
        assert url_ris == []

    def test_no_sample_name_yields_no_description(self, payload):
        import copy
        p = copy.deepcopy(payload)
        del p["sample"]["name"]
        record = _parse(p)
        assert record.descriptions == []

    def test_single_grant_ref_as_dict_not_list(self, payload):
        import copy
        p = copy.deepcopy(payload)
        # Simulate the API returning a bare dict instead of a one-element list
        p["admin"]["grant_support"]["grant_reference"] = {
            "funding_body": "Lone Funder",
            "instance_type": "grant_reference"
        }
        record = _parse(p)
        assert len(record.fundingReferences) == 1
        assert record.fundingReferences[0].funderName == "Lone Funder"
