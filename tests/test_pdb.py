"""
Unit tests for the PDB harvester.

Three GraphQL fixtures are used:
- ``pdb_graphql_1omw.json`` — GRK2 + Gβγ complex (Bos taurus, all-protein, single organism)
- ``pdb_graphql_6m0j.json`` — SARS-CoV-2 spike RBD + human ACE2 (all-protein, two-organism)
- ``pdb_graphql_4zt0.json`` — SpCas9 + sgRNA (single organism, mixed Protein + RNA)
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from apecx_harvesters.loaders.pdb import PDBHarvester
from apecx_harvesters.loaders.pdb.parser import _parse_entry
from apecx_harvesters.loaders.base import DateType, RelatedIdentifierType, RelatedItemType, RelationType, ResourceTypeGeneral, Subject
from apecx_harvesters.loaders.pdb import PDBContainer


FIXTURE_DIR = Path(__file__).parent / "fixtures"
GRAPHQL_FIXTURE_PATH = FIXTURE_DIR / "pdb_graphql_1omw.json"
GRAPHQL_6M0J_PATH = FIXTURE_DIR / "pdb_graphql_6m0j.json"
GRAPHQL_4ZT0_PATH = FIXTURE_DIR / "pdb_graphql_4zt0.json"


def _graphql_entry(path: Path) -> dict:
    data = json.loads(path.read_text())
    return data["data"]["entries"][0]


def _parse(data: dict) -> PDBContainer:
    return _parse_entry(data)


@pytest.fixture(scope="module")
def raw_1omw() -> dict:
    return _graphql_entry(GRAPHQL_FIXTURE_PATH)


@pytest.fixture(scope="module")
def record_1omw(raw_1omw) -> PDBContainer:
    return _parse_entry(raw_1omw)


@pytest.fixture(scope="module")
def raw_6m0j() -> dict:
    return _graphql_entry(GRAPHQL_6M0J_PATH)


@pytest.fixture(scope="module")
def record_6m0j(raw_6m0j) -> PDBContainer:
    return _parse_entry(raw_6m0j)


@pytest.fixture(scope="module")
def raw_4zt0() -> dict:
    return _graphql_entry(GRAPHQL_4ZT0_PATH)


@pytest.fixture(scope="module")
def record_4zt0(raw_4zt0) -> PDBContainer:
    return _parse_entry(raw_4zt0)


# ---------------------------------------------------------------------------
# Creator / author parsing
# ---------------------------------------------------------------------------

class TestCreators:
    def test_count(self, record_1omw):
        assert len(record_1omw.creators) == 5

    def test_name_preserved(self, record_1omw):
        assert record_1omw.creators[0].name == "Lodowski, D.T."

    def test_family_name_split(self, record_1omw):
        assert record_1omw.creators[0].familyName == "Lodowski"

    def test_given_name_split(self, record_1omw):
        assert record_1omw.creators[0].givenName == "D.T."

    def test_last_author(self, record_1omw):
        last = record_1omw.creators[-1]
        assert last.familyName == "Tesmer"
        assert last.givenName == "J.J.G."

    def test_no_orcids_in_1omw(self, record_1omw):
        # 1OMW predates widespread ORCID adoption; live API returns no identifiers
        for creator in record_1omw.creators:
            assert creator.nameIdentifiers == []


# ---------------------------------------------------------------------------
# Core DataCite fields
# ---------------------------------------------------------------------------

class TestIdentifiers:
    def test_entry_doi_in_identifiers(self, record_1omw):
        assert record_1omw.identifier is not None
        assert record_1omw.identifier.identifier == "10.2210/pdb1omw/pdb"
        assert record_1omw.identifier.identifierType == "DOI"

    def test_pdb_id_in_alternate_identifiers(self, record_1omw):
        pdb_ai = next(a for a in record_1omw.alternateIdentifiers if a.alternateIdentifierType == "PDB")
        assert pdb_ai.alternateIdentifier == "1OMW"

    def test_citation_pmid_in_related_identifiers(self, record_1omw):
        pmids = [r for r in record_1omw.relatedIdentifiers if r.relatedIdentifierType == RelatedIdentifierType.PMID]
        assert len(pmids) == 1
        assert pmids[0].relatedIdentifier == "12764189"
        assert pmids[0].relationType == RelationType.IsDocumentedBy

    def test_missing_database2_yields_no_entry_doi(self, raw_1omw):
        data = {k: v for k, v in raw_1omw.items() if k != "database_2"}
        record = _parse(data)
        assert record.identifier is None

    def test_canonical_uri_uses_pdb_id(self, record_1omw):
        assert record_1omw.canonical_uri == "pdb:1OMW"

    def test_publication_year(self, record_1omw):
        assert record_1omw.publicationYear == "2003"

    def test_resource_type(self, record_1omw):
        assert record_1omw.resourceType is not None
        assert record_1omw.resourceType.resourceTypeGeneral == ResourceTypeGeneral.Dataset


class TestSubjects:
    def test_subjects_populated(self, record_1omw):
        assert len(record_1omw.subjects) > 0

    def test_subjects_are_subject_instances(self, record_1omw):
        assert all(isinstance(s, Subject) for s in record_1omw.subjects)

    def test_known_keyword_present(self, record_1omw):
        terms = {s.subject for s in record_1omw.subjects}
        assert "TRANSFERASE" in terms
        assert "WD-40 repeat" in terms

    def test_pdbx_keywords_included(self, record_1omw):
        # pdbx_keywords "TRANSFERASE" also appears in text, so deduplication applies
        terms = {s.subject for s in record_1omw.subjects}
        assert "TRANSFERASE" in terms

    def test_no_duplicates_across_fields(self, record_1omw):
        # "TRANSFERASE" appears in both pdbx_keywords and text — must appear only once
        all_subjects = [s.subject for s in record_1omw.subjects]
        assert len(all_subjects) == len(set(all_subjects))

    def test_missing_struct_keywords_yields_empty(self, raw_1omw):
        data = {k: v for k, v in raw_1omw.items() if k != "struct_keywords"}
        record = _parse(data)
        assert record.subjects == []


class TestCoreFields:
    def test_title(self, record_1omw):
        assert record_1omw.titles[0].title == (
            "Crystal Structure of the complex between G Protein-Coupled Receptor "
            "Kinase 2 and Heterotrimeric G Protein beta 1 and gamma 2 subunits"
        )

    def test_publisher(self, record_1omw):
        assert record_1omw.publisher.name == "RCSB PDB"

    def test_description_contains_method(self, record_1omw):
        assert "x-ray diffraction" in record_1omw.descriptions[0].description.lower()

    def test_description_contains_resolution(self, record_1omw):
        assert "2.5" in record_1omw.descriptions[0].description

    def test_description_does_not_contain_keywords(self, record_1omw):
        """Keywords are stored in subjects, not duplicated in the description."""
        assert "TRANSFERASE" not in record_1omw.descriptions[0].description


# ---------------------------------------------------------------------------
# Date mapping
# ---------------------------------------------------------------------------

class TestDates:
    def test_submitted_date(self, record_1omw):
        submitted = next(d for d in record_1omw.dates if d.dateType == DateType.Submitted)
        assert submitted.date == "2003-02-26T00:00:00Z"

    def test_created_date(self, record_1omw):
        created = next(d for d in record_1omw.dates if d.dateType == DateType.Created)
        assert created.date == "2003-06-03T00:00:00Z"

    def test_updated_date(self, record_1omw):
        updated = next(d for d in record_1omw.dates if d.dateType == DateType.Updated)
        assert updated.date.startswith("2023-08-16")


# ---------------------------------------------------------------------------
# PDB-specific fields
# ---------------------------------------------------------------------------

class TestPDBFields:
    def test_pdb_id(self, record_1omw):
        assert record_1omw.pdb.pdb_id == "1OMW"

    def test_method(self, record_1omw):
        assert record_1omw.pdb.method == "X-RAY DIFFRACTION"

    def test_resolution(self, record_1omw):
        assert record_1omw.pdb.resolution_angstrom == pytest.approx(2.5)

    def test_struct_keywords_pdbx_keywords(self, record_1omw):
        assert record_1omw.pdb.struct_keywords is not None
        assert record_1omw.pdb.struct_keywords.pdbx_keywords == "TRANSFERASE"

    def test_struct_keywords_text(self, record_1omw):
        assert record_1omw.pdb.struct_keywords is not None
        assert record_1omw.pdb.struct_keywords.text == "WD-40 repeat, TRANSFERASE"

    def test_struct_keywords_none_when_absent(self, raw_1omw):
        data = {k: v for k, v in raw_1omw.items() if k != "struct_keywords"}
        record = _parse(data)
        assert record.pdb.struct_keywords is None

    def test_polymer_entities_count(self, record_1omw):
        assert len(record_1omw.pdb.polymer_entities) == 3

    def test_polymer_entity_ids(self, record_1omw):
        ids = {e.entity_id for e in record_1omw.pdb.polymer_entities}
        assert ids == {"1OMW_1", "1OMW_2", "1OMW_3"}

    def test_polymer_entity_single_organism(self, record_1omw):
        # All three entities in 1OMW are Bos taurus
        organisms = {e.scientific_name for e in record_1omw.pdb.polymer_entities}
        assert organisms == {"Bos taurus"}

    def test_polymer_entity_types(self, record_1omw):
        types = {e.polymer_type for e in record_1omw.pdb.polymer_entities}
        assert types == {"Protein"}

    def test_polymer_entities_empty_when_absent(self, raw_1omw):
        data = {k: v for k, v in raw_1omw.items() if k != "polymer_entities"}
        assert _parse(data).pdb.polymer_entities == []


class TestPolymerEntitiesMultiOrganism:
    """6M0J: SARS-CoV-2 spike RBD bound to human ACE2 — two distinct source organisms."""

    def test_entity_count(self, record_6m0j):
        assert len(record_6m0j.pdb.polymer_entities) == 2

    def test_organism_names(self, record_6m0j):
        names = {e.scientific_name for e in record_6m0j.pdb.polymer_entities}
        assert "Homo sapiens" in names
        assert "Severe acute respiratory syndrome coronavirus 2" in names

    def test_entity_organism_association(self, record_6m0j):
        by_id = {e.entity_id: e.scientific_name for e in record_6m0j.pdb.polymer_entities}
        assert by_id["6M0J_1"] == "Homo sapiens"
        assert by_id["6M0J_2"] == "Severe acute respiratory syndrome coronavirus 2"

    def test_polymer_types(self, record_6m0j):
        types = {e.polymer_type for e in record_6m0j.pdb.polymer_entities}
        assert types == {"Protein"}


class TestPolymerEntitiesMixedTypes:
    """4ZT0: SpCas9 + sgRNA — single organism, mixed Protein and RNA entities."""

    def test_entity_count(self, record_4zt0):
        assert len(record_4zt0.pdb.polymer_entities) == 2

    def test_contains_protein_and_rna(self, record_4zt0):
        types = {e.polymer_type for e in record_4zt0.pdb.polymer_entities}
        assert types == {"Protein", "RNA"}

    def test_single_organism(self, record_4zt0):
        organisms = {e.scientific_name for e in record_4zt0.pdb.polymer_entities}
        assert organisms == {"Streptococcus pyogenes"}

    def test_type_entity_association(self, record_4zt0):
        by_id = {e.entity_id: e.polymer_type for e in record_4zt0.pdb.polymer_entities}
        assert by_id["4ZT0_1"] == "Protein"
        assert by_id["4ZT0_2"] == "RNA"


class TestCitationRelatedItem:
    def test_citation_related_item_present(self, record_1omw):
        assert len(record_1omw.relatedItems) == 1

    def test_citation_relation_type(self, record_1omw):
        assert record_1omw.relatedItems[0].relationType == RelationType.IsDocumentedBy

    def test_citation_item_type(self, record_1omw):
        assert record_1omw.relatedItems[0].relatedItemType == RelatedItemType.JournalArticle

    def test_citation_doi(self, record_1omw):
        identifier = record_1omw.relatedItems[0].relatedItemIdentifier
        assert identifier is not None
        assert identifier.relatedItemIdentifier == "10.1126/science.1082348"
        assert identifier.relatedItemIdentifierType == RelatedIdentifierType.DOI

    def test_citation_title(self, record_1omw):
        assert "G Protein" in record_1omw.relatedItems[0].titles[0].title

    def test_citation_year(self, record_1omw):
        assert record_1omw.relatedItems[0].publicationYear == "2003"


# ---------------------------------------------------------------------------
# JSON Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_validates_against_pdb_schema(self, record_1omw):
        """Serialised record must satisfy the extended PDB schema."""
        jsonschema.validate(
            instance=record_1omw.to_dict(),
            schema=PDBContainer.json_schema(),
        )



# ---------------------------------------------------------------------------
# Invalid / incomplete payloads
# ---------------------------------------------------------------------------

class TestInvalidPayloads:
    """
    Verify harvester behaviour when the API response is malformed or incomplete.

    With Pydantic models, type errors are caught at *construction time*: a
    `ValidationError` is raised before any invalid object can be created.
    Structural errors (missing keys in the raw dict) still surface as plain
    Python `KeyError` / `IndexError` from the harvester's parsing code.
    """

    def test_missing_struct_raises_at_parse(self, raw_1omw):
        """struct.title is accessed unconditionally; absent key → KeyError."""
        data = {k: v for k, v in raw_1omw.items() if k != "struct"}
        with pytest.raises(KeyError):
            _parse(data)

    def test_missing_rcsb_id_raises_at_parse(self, raw_1omw):
        """rcsb_id is accessed unconditionally; absent key → KeyError."""
        data = {k: v for k, v in raw_1omw.items() if k != "rcsb_id"}
        with pytest.raises(KeyError):
            _parse(data)

    def test_empty_exptl_list_yields_unknown_method(self, raw_1omw):
        """exptl present but empty → falls back to UNKNOWN method gracefully."""
        data = {**raw_1omw, "exptl": []}
        record = _parse(data)
        assert record.pdb.method == "UNKNOWN"

    def test_integer_rcsb_id_raises_at_construction(self, raw_1omw):
        """With strict=True, passing an integer where rcsb_id expects a string
        raises ValidationError at object construction time (caught by PDBFields or AlternateIdentifier)."""
        data = {**raw_1omw, "rcsb_id": 10000}
        with pytest.raises(ValidationError):
            _parse(data)

    def test_integer_date_raises_at_construction(self, raw_1omw):
        """A non-string deposit_date raises ValidationError when building Date."""
        data = {
            **raw_1omw,
            "rcsb_accession_info": {
                **raw_1omw["rcsb_accession_info"],
                "deposit_date": 20030226,
            },
        }
        with pytest.raises(ValidationError) as exc_info:
            _parse(data)
        assert "date" in str(exc_info.value)

    def test_missing_audit_author_yields_empty_creators(self, raw_1omw):
        """Absent audit_author produces an empty creators list without raising."""
        data = {k: v for k, v in raw_1omw.items() if k != "audit_author"}
        record = _parse(data)
        assert record.creators == []
        jsonschema.validate(instance=record.to_dict(), schema=PDBContainer.json_schema())

    def test_missing_accession_info_yields_empty_dates(self, raw_1omw):
        """Absent rcsb_accession_info produces an empty dates list without raising."""
        data = {k: v for k, v in raw_1omw.items() if k != "rcsb_accession_info"}
        record = _parse(data)
        assert record.dates == []
        jsonschema.validate(instance=record.to_dict(), schema=PDBContainer.json_schema())


# ---------------------------------------------------------------------------
# Batch parsing (GraphQL)
# ---------------------------------------------------------------------------

class TestBatchParsing:
    @pytest.fixture(scope="class")
    def batch_records(self):
        content = GRAPHQL_FIXTURE_PATH.read_text()
        return asyncio.run(PDBHarvester()._parse_many(content))

    def test_entry_id_present(self, batch_records):
        assert "1OMW" in batch_records

    def test_title(self, batch_records):
        record = batch_records["1OMW"]
        assert "G Protein" in record.titles[0].title

    def test_no_orcids_in_1omw(self, batch_records):
        record = batch_records["1OMW"]
        for creator in record.creators:
            assert creator.nameIdentifiers == []

    def test_pdb_fields_populated(self, batch_records):
        record = batch_records["1OMW"]
        assert record.pdb.pdb_id == "1OMW"
        assert record.pdb.method == "X-RAY DIFFRACTION"

    def test_citation_related_item_populated(self, batch_records):
        record = batch_records["1OMW"]
        assert len(record.relatedItems) == 1
        identifier = record.relatedItems[0].relatedItemIdentifier
        assert identifier is not None
        assert identifier.relatedItemIdentifier == "10.1126/science.1082348"

    def test_parse_item_round_trip(self, record_1omw):
        """Raw items split from a batch response can be re-parsed from cache."""
        harvester = PDBHarvester()
        raw_items = asyncio.run(harvester._split_batch(GRAPHQL_FIXTURE_PATH.read_text(), []))
        raw = raw_items["1OMW"]
        restored = asyncio.run(harvester._parse_item(raw))
        assert restored.titles[0].title == record_1omw.titles[0].title