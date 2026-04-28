"""Pipeline sinks: terminal steps that consume a RetrievalResult stream."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from apecx_harvesters.loaders.base import DataCite
from apecx_harvesters.loaders.base.retrieve import RetrievalResult

logger = logging.getLogger(__name__)

# Globus Search ingest limits. https://docs.globus.org/api/search/limits/
_GSEARCH_MAX_BYTES = 10_000_000          # GMetaList chunk ceiling
_GSEARCH_MAX_ENTRY_BYTES = 10_000_000    # per GMetaEntry
_GSEARCH_MAX_FIELD_BYTES = 32_000        # per field value
# Note: calculation must use stdlib json; faster libraries like orjson yield diff payload sizes. Really.
_GSEARCH_WRAPPER_OVERHEAD = len(json.dumps({"ingest_type": "GMetaList", "ingest_data": {"gmeta": []}}).encode())


@dataclass
class ReportResult:
    name: str
    n_success: int
    n_errors: int


def report(name: str = "") -> Callable[[AsyncIterator[RetrievalResult[Any]]], Awaitable[ReportResult]]:
    """
    Retrieve all results and report how many records were found.

    In conjunction with a search + retrieval step, this has the effect of fetching all results into a local cache,
        for subsequent aggregation or re-processing.
    """
    async def _sink(results: AsyncIterator[RetrievalResult[Any]]) -> ReportResult:
        fetched = errors = 0
        async for result in results:
            if result.ok:
                fetched += 1
            else:
                errors += 1
                logger.warning("[%s] fetch error %r: %s", name, result.id, result.error)
        logger.info("[%s] %d fetched, %d error(s)", name, fetched, errors)
        return ReportResult(name=name, n_success=fetched, n_errors=errors)

    return _sink


def _truncate_fields(obj: Any, subject: str, path: str = "") -> Any:
    """Recursively truncate string leaves exceeding the Globus 32 KB field limit."""
    if isinstance(obj, str):
        encoded = obj.encode()
        if len(encoded) > _GSEARCH_MAX_FIELD_BYTES:
            logger.warning(
                "%s field %r: %d bytes truncated to %d (Globus 32 KB field limit)",
                subject, path, len(encoded), _GSEARCH_MAX_FIELD_BYTES,
            )
            return encoded[:_GSEARCH_MAX_FIELD_BYTES].decode("utf-8", errors="ignore")
        return obj
    if isinstance(obj, dict):
        return {k: _truncate_fields(v, subject, f"{path}.{k}" if path else k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_truncate_fields(v, subject, f"{path}[{i}]") for i, v in enumerate(obj)]
    return obj


def _to_gmetaentry(
    record: DataCite,
    *,
    visible_to: list[str] | None = None,
) -> dict[str, Any]:
    """Convert a harvested record to a Globus Search GMetaEntry document."""
    subject = record.canonical_uri
    content = _truncate_fields(record.to_dict(), subject)
    entry: dict[str, Any] = {
        "subject": subject,
        "visible_to": visible_to or ["public"],
        "content": content,
    }
    entry_bytes = len(json.dumps(entry).encode())
    if entry_bytes > _GSEARCH_MAX_ENTRY_BYTES:
        logger.warning(
            "%s: entry is %d bytes after field truncation, exceeds 10 MB GMetaEntry limit",
            subject, entry_bytes,
        )
    return entry


async def to_gmetalist(
    results: AsyncIterator[RetrievalResult[Any]],
    *,
    visible_to: list[str] | None = None,
    max_bytes: int = _GSEARCH_MAX_BYTES,
) -> AsyncIterator[dict[str, Any]]:
    """
    Yield batched Globus Search GMetaList ingest documents from *results*.

    Filters to successful results only. Each yielded document fits within
    *max_bytes* when serialised by the Globus SDK. Callers should POST each
    document to the ingest API separately.
    """
    batch: list[dict[str, Any]] = []
    batch_size = 0

    async for result in results:
        if not result.ok:
            # If record failed retrieval or transform steps, don't put it in search index!
            logger.warning("skipping failed result %r: %s", result.id, result.error)
            continue
        assert result.record is not None
        entry = _to_gmetaentry(result.record, visible_to=visible_to)
        entry_size = len(json.dumps(entry).encode())
        projected = _GSEARCH_WRAPPER_OVERHEAD + batch_size + len(batch) + entry_size

        if batch and projected > max_bytes:
            yield {"ingest_type": "GMetaList", "ingest_data": {"gmeta": batch}}
            batch = []
            batch_size = 0

        batch.append(entry)
        batch_size += entry_size

    if batch:
        yield {"ingest_type": "GMetaList", "ingest_data": {"gmeta": batch}}
