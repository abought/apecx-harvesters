import pytest
from gsearch.parse import filter_fields, get_entry


RECORD = {
    "publicationYear": 2021,
    "identifier": {"identifier": "10.2210/pdb1TUC/pdb", "identifierType": "DOI"},
    "titles": [
        {"title": "Structure of SOD1", "titleType": None},
        {"title": "Superoxide dismutase 1", "titleType": "AlternativeTitle"},
    ],
    "creators": [
        {"name": "Smith, J.", "givenName": "J.", "familyName": "Smith"},
        {"name": "Doe, A.", "givenName": "A.", "familyName": "Doe"},
    ],
    "pdb": {"method": "X-RAY DIFFRACTION", "resolution_angstrom": 1.5},
}


class TestGetEntry:
    def test_returns_first_entry_content(self):
        gmeta = {"entries": [{"entry_id": "a", "content": {"x": 1}}, {"entry_id": "b", "content": {"x": 2}}]}
        assert get_entry(gmeta) == {"x": 1}

    def test_returns_none_when_no_entries(self):
        assert get_entry({"entries": []}) is None
        assert get_entry({}) is None


class TestFilterFields:
    def test_top_level(self):
        result = filter_fields(RECORD, ["publicationYear"])
        assert result == {"publicationYear": 2021}

    def test_nested_dict(self):
        result = filter_fields(RECORD, ["pdb.method"])
        assert result == {"pdb": {"method": "X-RAY DIFFRACTION"}}

    def test_nested_dict_multiple_subfields(self):
        result = filter_fields(RECORD, ["pdb.method", "pdb.resolution_angstrom"])
        assert result == {"pdb": {"method": "X-RAY DIFFRACTION", "resolution_angstrom": 1.5}}

    def test_list_of_dicts(self):
        result = filter_fields(RECORD, ["titles.title"])
        assert result == {"titles": [{"title": "Structure of SOD1"}, {"title": "Superoxide dismutase 1"}]}

    def test_list_of_dicts_multiple_subfields(self):
        result = filter_fields(RECORD, ["creators.name", "creators.familyName"])
        assert result == {
            "creators": [
                {"name": "Smith, J.", "familyName": "Smith"},
                {"name": "Doe, A.", "familyName": "Doe"},
            ]
        }

    def test_mixed_paths(self):
        result = filter_fields(RECORD, ["publicationYear", "identifier.identifier", "titles.title", "pdb.method"])
        assert result == {
            "publicationYear": 2021,
            "identifier": {"identifier": "10.2210/pdb1TUC/pdb"},
            "titles": [{"title": "Structure of SOD1"}, {"title": "Superoxide dismutase 1"}],
            "pdb": {"method": "X-RAY DIFFRACTION"},
        }

    def test_missing_key_silently_skipped(self):
        result = filter_fields(RECORD, ["emdb.emdb_id"])
        assert result == {}

    def test_path_into_primitive_silently_skipped(self):
        result = filter_fields(RECORD, ["publicationYear.foo"])
        assert result == {}

    def test_empty_fields_returns_empty(self):
        result = filter_fields(RECORD, [])
        assert result == {}
