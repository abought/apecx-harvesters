"""IEDB field parsers."""

from __future__ import annotations

import re
from typing import Any

from apecx_harvesters.loaders.base import (
    Creator,
    Publisher,
    ResourceType,
    ResourceTypeGeneral,
    Subject,
    Title,
)

from .model import IEDBContainer, IEDBEpitopeRow, IEDBFields


def _clean_value(value: Any) -> Any:
    """Normalize simple string noise from IEDB export rows."""
    if not isinstance(value, str):
        return value

    value = re.sub(r"\n", " ", value)
    value = re.sub(r" +", " ", value)
    value = re.sub(r"[\'\"]", "", value)
    value = value.strip()
    return value or None


def _subjects(rows: list[IEDBEpitopeRow]) -> list[Subject]:
    values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for value in (
                row.epitope__source_organism,
                row.epitope__species,
                row.epitope__source_molecule,
                row.epitope__molecule_parent,
        ):
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return [Subject(subject=value) for value in values]


def _parse_export(data: dict[str, Any] | list[dict[str, Any]]) -> IEDBContainer:
    """
    Parse IEDB epitope_export data into an IEDBContainer.

    IEDBHarvester stores raw cache entries as {"query": ..., "rows": [...]}, but
    accepting a bare list keeps this parser easy to test with direct API output.
    """
    if isinstance(data, list):
        query = "iedb_epitope_export"
        raw_rows = data
    else:
        query = str(data.get("query") or "iedb_epitope_export")
        raw_rows = data.get("rows") or []

    rows: list[IEDBEpitopeRow] = []

    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            continue

        cleaned = {
            key: value
            for key, raw_value in raw_row.items()
            if (value := _clean_value(raw_value)) is not None
        }
        if not cleaned:
            continue

        rows.append(IEDBEpitopeRow(**cleaned))

    return IEDBContainer.new(
        titles=[Title(title=f"IEDB epitope export for {query}")],
        creators=[Creator(name="Immune Epitope Database")],
        publisher=Publisher(name="Immune Epitope Database"),
        resourceType=ResourceType(
            resourceTypeGeneral=ResourceTypeGeneral.Dataset,
            resourceType="Epitope export",
        ),
        subjects=_subjects(rows),
        iedb=IEDBFields(
            query=query,
            row_count=len(rows),
            rows=rows,
        ),
    )
