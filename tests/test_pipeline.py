"""Unit tests for the pipeline package (run, sinks, sources)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from apecx_harvesters.loaders.base import DataCite, RetrievalResult
from apecx_harvesters.loaders.base.model import Identifier, Publisher
from apecx_harvesters.pipeline import (
    PipelineSpec,
    ReportResult,
    report,
    run,
    run_parallel,
    to_gmetalist,
)
from apecx_harvesters.pipeline.sources import csv_ids


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(title: str = "Test", identifier: str = "test:1") -> DataCite:
    return DataCite.new(
        title=title,
        creators=[],
        publisher=Publisher(name="Test"),
        identifier=Identifier(identifier=identifier, identifierType="URL"),
    )


def _ok(title: str = "Test", id_: str = "1", identifier: str = "1") -> RetrievalResult:
    return RetrievalResult(id=id_, record=_record(title, identifier=identifier))


def _err(id_: str = "1", msg: str = "boom") -> RetrievalResult:
    return RetrievalResult(id=id_, error=msg)


async def _stream(*results: RetrievalResult) -> AsyncIterator[RetrievalResult]:
    for r in results:
        yield r


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# run() — transforms and error handling
# ---------------------------------------------------------------------------

class TestRun:
    def test_no_transforms_passes_results_unchanged(self):
        sink_received = []

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for r in stream:
                sink_received.append(r)

        r = _ok("Alpha")
        _run(run(_stream(r), _sink))
        assert sink_received == [r]

    def test_failed_result_bypasses_transforms(self):
        called = False

        async def _transform(record: DataCite) -> DataCite:
            nonlocal called
            called = True
            return record

        sink_received = []

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for r in stream:
                sink_received.append(r)

        err = _err()
        _run(run(_stream(err), _sink, transforms=[_transform]))
        assert not called
        assert sink_received[0].ok is False

    def test_transform_applied_to_successful_result(self):
        async def _upper(record: DataCite) -> DataCite:
            from apecx_harvesters.loaders.base.model import Title
            record.titles[0] = Title(title=record.titles[0].title.upper())
            return record

        sink_received = []

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for r in stream:
                sink_received.append(r)

        _run(run(_stream(_ok("hello")), _sink, transforms=[_upper]))
        assert sink_received[0].record.titles[0].title == "HELLO"

    def test_multiple_transforms_chained_in_order(self):
        log = []

        async def _tag_a(record: DataCite) -> DataCite:
            log.append("a")
            return record

        async def _tag_b(record: DataCite) -> DataCite:
            log.append("b")
            return record

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for _ in stream:
                pass

        _run(run(_stream(_ok()), _sink, transforms=[_tag_a, _tag_b]))
        assert log == ["a", "b"]

    def test_transform_exception_yields_error_result(self):
        async def _bad(record: DataCite) -> DataCite:
            raise ValueError("transform failed")

        sink_received = []

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for r in stream:
                sink_received.append(r)

        _run(run(_stream(_ok()), _sink, transforms=[_bad]))
        assert len(sink_received) == 1
        assert sink_received[0].ok is False
        assert "ValueError" in sink_received[0].error
        assert "transform failed" in sink_received[0].error

    def test_transform_error_does_not_abort_remaining_items(self):
        async def _bad_on_first(record: DataCite) -> DataCite:
            if record.titles[0].title == "first":
                raise RuntimeError("nope")
            return record

        sink_received = []

        async def _sink(stream: AsyncIterator[RetrievalResult]) -> None:
            async for r in stream:
                sink_received.append(r)

        _run(run(_stream(_ok("first"), _ok("second")), _sink, transforms=[_bad_on_first]))
        assert sink_received[0].ok is False
        assert sink_received[1].ok is True

    def test_returns_sink_result(self):
        async def _sink(stream: AsyncIterator[RetrievalResult]) -> int:
            count = 0
            async for _ in stream:
                count += 1
            return count

        result = _run(run(_stream(_ok(), _ok()), _sink))
        assert result == 2


# ---------------------------------------------------------------------------
# run_parallel()
# ---------------------------------------------------------------------------

class TestRunParallel:
    def test_results_returned_in_spec_order(self):
        async def _count_sink(name: str):
            async def _sink(stream: AsyncIterator[RetrievalResult]) -> str:
                async for _ in stream:
                    pass
                return name
            return _sink

        async def _go():
            s1 = await _count_sink("first")
            s2 = await _count_sink("second")
            return await run_parallel(
                PipelineSpec(source=_stream(_ok()), sink=s1, name="first"),
                PipelineSpec(source=_stream(_ok(), _ok()), sink=s2, name="second"),
            )

        results = _run(_go())
        assert results == ["first", "second"]


# ---------------------------------------------------------------------------
# report() sink
# ---------------------------------------------------------------------------

class TestReport:
    def test_counts_successes_and_errors(self):
        result: ReportResult = _run(report("x")(_stream(_ok(), _ok(), _err())))
        assert result.name == "x"
        assert result.n_success == 2
        assert result.n_errors == 1

    def test_empty_stream_returns_zero_counts(self):
        result: ReportResult = _run(report("x")(_stream()))
        assert result.n_success == 0
        assert result.n_errors == 0


# ---------------------------------------------------------------------------
# to_gmetalist() sink
# ---------------------------------------------------------------------------

class TestToGmetalist:
    async def _collect(self, stream, **kwargs) -> list[dict]:
        return [chunk async for chunk in to_gmetalist(stream, **kwargs)]

    def test_yields_gmetalist_structure(self):
        chunks = _run(self._collect(_stream(_ok(identifier="1"))))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["ingest_type"] == "GMetaList"
        entries = chunk["ingest_data"]["gmeta"]
        assert len(entries) == 1
        assert entries[0]["subject"] == "datacite:1"
        assert entries[0]["visible_to"] == ["public"]
        assert "content" in entries[0]

    def test_skips_failed_results(self):
        chunks = _run(self._collect(_stream(_err(), _ok(identifier="2"), _err())))
        entries = chunks[0]["ingest_data"]["gmeta"]
        assert len(entries) == 1
        assert entries[0]["subject"] == "datacite:2"

    def test_empty_stream_yields_no_chunks(self):
        chunks = _run(self._collect(_stream()))
        assert chunks == []

    def test_splits_into_multiple_chunks_when_over_max_bytes(self):
        # Use a very small max_bytes to force a split across two records.
        r1 = _ok("First", id_="1", identifier="1")
        r2 = _ok("Second", id_="2", identifier="2")
        chunks = _run(self._collect(_stream(r1, r2), max_bytes=1))
        assert len(chunks) == 2
        assert chunks[0]["ingest_data"]["gmeta"][0]["subject"] == "datacite:1"
        assert chunks[1]["ingest_data"]["gmeta"][0]["subject"] == "datacite:2"

    def test_all_records_in_single_chunk_when_under_limit(self):
        records = [_ok(f"R{i}", id_=str(i), identifier=f"test:{i}") for i in range(5)]
        chunks = _run(self._collect(_stream(*records), max_bytes=10_000_000))
        assert len(chunks) == 1
        assert len(chunks[0]["ingest_data"]["gmeta"]) == 5

    def test_chunk_json_is_valid_and_within_max_bytes(self):
        max_bytes = 500
        records = [_ok(f"R{i}", id_=str(i), identifier=f"test:{i}") for i in range(10)]
        chunks = _run(self._collect(_stream(*records), max_bytes=max_bytes))
        for chunk in chunks:
            size = len(json.dumps(chunk).encode())
            assert size <= max_bytes


# ---------------------------------------------------------------------------
# csv_ids() source
# ---------------------------------------------------------------------------

class TestCsvIds:
    def test_yields_ids_from_named_column(self, tmp_path):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text("id,name\nABC,foo\nDEF,bar\n")

        async def _collect() -> list[str]:
            return [x async for x in csv_ids(csv_file)]

        assert _run(_collect()) == ["ABC", "DEF"]

    def test_custom_column_name(self, tmp_path):
        csv_file = tmp_path / "ids.csv"
        csv_file.write_text("pmid,title\n12345,hello\n67890,world\n")

        async def _collect() -> list[str]:
            return [x async for x in csv_ids(csv_file, col="pmid")]

        assert _run(_collect()) == ["12345", "67890"]