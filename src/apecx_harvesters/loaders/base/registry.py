"""
Schema registry for all harvester data containers.

This aggregates all harvester schemas, making it possible to create a single global
    JSONSchema document with all known harvester fields.
"""

from __future__ import annotations

import copy
import re
from typing import Any, ClassVar, Type

from apecx_harvesters.loaders.base.model import DataCite

SCHEMA_URI = "https://json-schema.org/draft/2020-12/schema"

# Computed once at import time; used to identify which properties are
# domain-specific additions vs. inherited base fields.
_BASE_PROPERTY_NAMES: frozenset[str] = frozenset(
    DataCite.model_json_schema().get("properties", {}).keys()
)

# Base-schema fields excluded from the query schema. These "hidden fields" represent data that is stored, 
#   but rarely queried by consumers. Simplifying the jsonschema can make AI clients more efficient. 
_QUERY_DROP_BASE_FIELDS: frozenset[str] = frozenset({
    "formats",           # file-format metadata
    "fundingReferences", # grant provenance; rarely searched
    "relatedItems",      # full bibliographic records; use relatedIdentifiers for PID-to-PID joins
    "rightsList",        # license info; may matter for rights-aware search later
    "version",           # dataset version string; not a useful search axis
})

# Per-definition fields excluded from the query schema.
_QUERY_DROP_DEF_FIELDS: dict[str, frozenset[str]] = {
    # jats_xml_url is a retrieval URL, not a query field
    "BiorXivFields": frozenset({"jats_xml_url"}),
}


def _collect_refs(node: Any) -> set[str]:
    """Return all $defs names referenced anywhere in *node*."""
    refs: set[str] = set()
    if isinstance(node, dict):
        ref = node.get("$ref", "")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            refs.add(ref[len("#/$defs/"):])
        for v in node.values():
            refs |= _collect_refs(v)
    elif isinstance(node, list):
        for item in node:
            refs |= _collect_refs(item)
    return refs


def _prune_defs(schema: dict[str, Any]) -> dict[str, Any]:
    """Remove $defs entries unreachable from properties (transitively)."""
    defs: dict[str, Any] = schema.get("$defs", {})
    if not defs:
        return schema
    reachable: set[str] = set()
    frontier = _collect_refs({k: v for k, v in schema.items() if k != "$defs"})
    while frontier - reachable:
        newly_found = frontier - reachable
        reachable |= newly_found
        for name in newly_found:
            if name in defs:
                frontier |= _collect_refs(defs[name])
    result = {k: v for k, v in schema.items() if k != "$defs"}
    pruned = {k: v for k, v in defs.items() if k in reachable}
    if pruned:
        result["$defs"] = pruned
    return result


def _normalize_key(s: str) -> str:
    """Lowercase and strip all non-alphanumeric characters for title comparison."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _simplify_node(node: Any, _key: str | None = None) -> Any:
    """
    Recursively simplify a schema node for query use:
    - Collapse Pydantic's nullable anyOf pattern: anyOf [{T}, {type: null}] → T
    - Remove 'additionalProperties' (validation-only, noise for query construction)
    - Remove 'default' values (not relevant for query construction)
    - Remove 'title' when it is redundant with the property/def key name
      (both normalized to lowercase alphanumeric); keep when it adds meaning
    """
    if isinstance(node, list):
        return [_simplify_node(item) for item in node]
    if not isinstance(node, dict):
        return node

    # Collapse anyOf [{type: T}, {type: null}] — Pydantic's optional-field encoding.
    # Optionality is already conveyed by absence from the `required` array (JSON Schema
    # 2020-12 has no `optional` keyword; non-required properties are implicitly optional).
    if "anyOf" in node:
        variants: list[Any] = node["anyOf"]
        non_null = [v for v in variants if v != {"type": "null"}]
        if len(non_null) == 1 and len(variants) == 2:
            result = _simplify_node(non_null[0], _key)
            if isinstance(result, dict) and "description" in node:
                result = {**result, "description": node["description"]}
            return result

    _STRIP_KEYS = {"additionalProperties", "default", "required"}
    result: dict[str, Any] = {}
    for k, v in node.items():
        if k in _STRIP_KEYS:
            continue
        if k == "title" and _key is not None:
            assert isinstance(v, str)
            if _normalize_key(v) == _normalize_key(_key):
                continue  # redundant with the key name
        # Recurse, passing the child key name for properties and $defs dicts so
        # that title-stripping works one level deeper.
        if k in ("properties", "$defs") and isinstance(v, dict):
            result[k] = {ck: _simplify_node(cv, ck) for ck, cv in v.items()}
        else:
            result[k] = _simplify_node(v, None)
    return result


class SchemaRegistry:
    """
    Registry of various data types. Used to create one catch-all schema document for everything

        @SchemaRegistry.register
        class PDBContainer(DataCite):
            pdb: PDBFields
    """

    _registry: ClassVar[dict[str, Type[DataCite]]] = {}

    @classmethod
    def register(cls, schema_class: Type[DataCite]) -> Type[DataCite]:
        """Register a harvester schema class.  Returns the class unchanged (decorator-safe)."""
        cls._registry[schema_class.__name__] = schema_class
        return schema_class

    @classmethod
    def registered(cls) -> dict[str, Type[DataCite]]:
        """Return a snapshot of all registered schema classes, keyed by class name."""
        return dict(cls._registry)

    @classmethod
    def combined_json_schema(cls) -> dict[str, Any]:
        """
        Build a "universal" JSONSchema that includes domain-specific fields from all subtypes.

        Base fields remain required.  Each harvester's domain-specific nested
        field (e.g. `pdb`) is added as an optional property so the combined
        schema can validate records from any harvester.
        """
        base_raw = DataCite.model_json_schema()

        combined: dict[str, Any] = {
            "$schema": SCHEMA_URI,
            "type": "object",
            "title": "Combined harvester schema",
            "description": (
                "Base DataCite fields plus optional domain-specific fields "
                "from all registered loaders."
            ),
            "properties": copy.deepcopy(base_raw.get("properties", {})),
            "required": list(base_raw.get("required", [])),
            "additionalProperties": False,
        }

        all_defs: dict[str, Any] = copy.deepcopy(base_raw.get("$defs", {}))

        for schema_class in cls._registry.values():
            harvester_raw = schema_class.model_json_schema()

            # Merge $defs.  Shared base-model defs (Creator, Publisher, etc.) appear
            # in every harvester schema and are identical — dedup silently.  Raise
            # if two loaders define the same name with genuinely different bodies.
            for def_name, def_body in harvester_raw.get("$defs", {}).items():
                if def_name in all_defs and all_defs[def_name] != def_body:
                    raise ValueError(
                        f"Schema conflict: '{def_name}' is defined differently by "
                        f"'{schema_class.__name__}' and a previously registered harvester."
                    )
                all_defs[def_name] = def_body

            # Add domain-specific properties (absent from the base) as optional.
            for prop_name, prop_body in harvester_raw.get("properties", {}).items():
                if prop_name not in _BASE_PROPERTY_NAMES:
                    combined["properties"][prop_name] = copy.deepcopy(prop_body)

        if all_defs:
            combined["$defs"] = all_defs

        return combined

    @classmethod
    def query_json_schema(cls) -> dict[str, Any]:
        """
        Build a more concise schema (~55% fewer tokens) for an LLM to use.

        Compared to combined_json_schema(): some fields are excluded per
        _QUERY_DROP_BASE_FIELDS and _QUERY_DROP_DEF_FIELDS. The full schema is
        authoritative for validation; this schema is intended for LLM query construction.
        """
        combined = cls.combined_json_schema()

        # Drop excluded top-level fields
        props = {
            k: v for k, v in combined["properties"].items()
            if k not in _QUERY_DROP_BASE_FIELDS
        }

        # Drop excluded per-definition fields
        all_defs = copy.deepcopy(combined.get("$defs", {}))
        for def_name, drop_fields in _QUERY_DROP_DEF_FIELDS.items():
            if def_name in all_defs and "properties" in all_defs[def_name]:
                all_defs[def_name]["properties"] = {
                    k: v for k, v in all_defs[def_name]["properties"].items()
                    if k not in drop_fields
                }

        schema: dict[str, Any] = {
            "$schema": SCHEMA_URI,
            "type": "object",
            "title": "Query schema",
            "description": (
                "Searchable fields across all harvesters. "
                "Domain fields (pdb, pubmed, emdb, biorxiv) are present only on records "
                "from that source - use their presence to identify the record type. "
            ),
            "properties": props,
            "required": list(combined.get("required", [])),
            "additionalProperties": False,
        }
        if all_defs:
            schema["$defs"] = all_defs

        schema = _simplify_node(schema)

        # Collapse enum $defs that carry a description down to {type, description}.
        # The description (set on each enum class) identifies the DataCite property name;
        # the LLM is expected to have embedded knowledge of the allowed values.
        for def_name, def_body in schema.get("$defs", {}).items():
            if "enum" in def_body and "description" in def_body:
                schema["$defs"][def_name] = {
                    "type": def_body.get("type", "string"),
                    "description": def_body["description"],
                }

        return _prune_defs(schema)
