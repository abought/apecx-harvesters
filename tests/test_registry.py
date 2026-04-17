"""
Tests for the schema registry and combined JSON schema generation.
"""

from __future__ import annotations

import pytest

from apecx_harvesters.loaders.base import DataCite
from apecx_harvesters.loaders.pdb import PDBContainer
from apecx_harvesters.loaders.base.registry import (
    SchemaRegistry,
    _BASE_PROPERTY_NAMES,
    _inline_single_use_defs,
    _collect_refs,
)


class TestRegistration:
    def test_pdb_container_is_registered(self):
        assert "PDBContainer" in SchemaRegistry.registered()

    def test_registered_returns_snapshot(self):
        snapshot = SchemaRegistry.registered()
        snapshot["Fake"] = DataCite  # mutating the snapshot must not affect the registry
        assert "Fake" not in SchemaRegistry.registered()

    def test_register_decorator_returns_class_unchanged(self):
        # The decorator must be transparent so the class is still usable normally.
        assert SchemaRegistry.registered()["PDBContainer"] is PDBContainer

    def test_duplicate_registration_is_idempotent(self):
        # Re-registering the same class should not raise and should not duplicate it.
        SchemaRegistry.register(PDBContainer)
        count = sum(1 for v in SchemaRegistry.registered().values() if v is PDBContainer)
        assert count == 1


class TestCombinedJsonSchema:
    @pytest.fixture(scope="class")
    def schema(self) -> dict:
        return SchemaRegistry.combined_json_schema()

    def test_schema_uri(self, schema):
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"

    def test_schema_title(self, schema):
        assert schema["title"] == "Combined harvester schema"

    def test_base_required_fields_preserved(self, schema):
        base_required = set(DataCite.model_json_schema().get("required", []))
        assert base_required.issubset(set(schema["required"]))

    def test_base_properties_present(self, schema):
        for prop in _BASE_PROPERTY_NAMES:
            assert prop in schema["properties"], f"Base property '{prop}' missing"

    def test_pdb_domain_field_present_and_optional(self, schema):
        # 'pdb' is a required field on PDBContainer itself, but must be optional
        # in the combined schema because non-PDB records won't have it.
        assert "pdb" in schema["properties"]
        assert "pdb" not in schema["required"]

    def test_additional_properties_false(self, schema):
        assert schema["additionalProperties"] is False

    def test_pdb_defs_included(self, schema):
        assert "PDBFields" in schema.get("$defs", {})

    def test_base_defs_included(self, schema):
        # Base sub-models must still be reachable for $ref resolution.
        defs = schema.get("$defs", {})
        for name in ("Creator", "Publisher", "Date", "DateType"):
            assert name in defs, f"Expected base $def '{name}' in combined schema"

    def test_combined_schema_is_independent_copy(self):
        # Mutating the returned dict must not affect subsequent calls.
        s1 = SchemaRegistry.combined_json_schema()
        s1["properties"]["injected"] = {}
        s2 = SchemaRegistry.combined_json_schema()
        assert "injected" not in s2["properties"]


class TestInlineSingleUseDefs:
    def test_single_use_def_is_inlined(self):
        schema = {
            "properties": {"a": {"$ref": "#/$defs/A"}},
            "$defs": {"A": {"type": "string", "description": "leaf"}},
        }
        result = _inline_single_use_defs(schema)
        assert "$defs" not in result
        assert result["properties"]["a"] == {"type": "string", "description": "leaf"}

    def test_chained_single_use_defs_are_fully_inlined(self):
        # A → B → C, all single-use: no dangling refs or leftover $defs.
        schema = {
            "properties": {"a": {"$ref": "#/$defs/A"}},
            "$defs": {
                "A": {"properties": {"b": {"$ref": "#/$defs/B"}}, "type": "object"},
                "B": {"properties": {"c": {"$ref": "#/$defs/C"}}, "type": "object"},
                "C": {"type": "string"},
            },
        }
        result = _inline_single_use_defs(schema)
        assert "$defs" not in result
        assert not _collect_refs(result), "no $refs should remain"

    def test_multi_use_def_is_preserved(self):
        # Shared is referenced by both A and B — must stay in $defs.
        schema = {
            "properties": {
                "a": {"$ref": "#/$defs/A"},
                "b": {"$ref": "#/$defs/B"},
            },
            "$defs": {
                "A": {"properties": {"s": {"$ref": "#/$defs/Shared"}}, "type": "object"},
                "B": {"properties": {"s": {"$ref": "#/$defs/Shared"}}, "type": "object"},
                "Shared": {"type": "string"},
            },
        }
        result = _inline_single_use_defs(schema)
        # A and B are single-use and get inlined; Shared remains.
        assert "$defs" in result
        assert "Shared" in result["$defs"]
        assert "A" not in result.get("$defs", {})
        assert "B" not in result.get("$defs", {})

    def test_no_dangling_refs_in_query_schema(self):
        schema = SchemaRegistry.query_json_schema()
        defs = set(schema.get("$defs", {}).keys())
        referenced = set(_collect_refs(schema).keys())
        dangling = referenced - defs
        assert not dangling, f"Dangling $refs in query schema: {dangling}"


class TestSchemaConflictDetection:
    def test_conflicting_defs_raise(self):
        """Two loaders that define the same $defs key differently should raise."""
        from pydantic import BaseModel, ConfigDict

        # Temporarily register a harvester whose $defs collide with an existing one.
        # We fabricate a minimal subclass that reuses a def name with different content.
        class _FakeFields(BaseModel):
            model_config = ConfigDict(strict=True, extra="forbid")
            x: int

        class _FakeContainer(DataCite):
            # Name the field "pdb" so its $defs entry is "PDBFields"-like,
            # but point it at our _FakeFields which has a different schema body.
            # We'll use a different field name to avoid overwriting "pdb" property;
            # instead we directly manipulate model_json_schema output via monkeypatching.
            fake: _FakeFields

        # Patch model_json_schema to produce a conflicting "PDBFields" def.
        original = _FakeContainer.model_json_schema

        def _patched_schema():
            s = original()
            s.setdefault("$defs", {})["PDBFields"] = {"type": "string"}  # intentionally wrong
            return s

        _FakeContainer.model_json_schema = classmethod(lambda cls: _patched_schema())  # type: ignore[assignment]

        SchemaRegistry._registry["_FakeContainer"] = _FakeContainer
        try:
            with pytest.raises(ValueError, match="Schema conflict"):
                SchemaRegistry.combined_json_schema()
        finally:
            SchemaRegistry._registry.pop("_FakeContainer", None)
            _FakeContainer.model_json_schema = original  # type: ignore[method-assign]
