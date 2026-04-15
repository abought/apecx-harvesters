"""
Unit tests for BaseHarvester.retrieve / iter_results behavior.

Tests use minimal in-process stubs rather than any real harvester or network
calls.  Two stubs cover the two code paths:

- ``_SequentialStub`` — ``_BATCH_SIZE = 0``, per-item fetch+parse
- ``_BatchStub``      — ``_BATCH_SIZE = 2``, chunked batch fetch+parse
"""

from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

import pytest

from apecx_harvesters.loaders.base import BaseHarvester, DataCite, RetrievalResult
from apecx_harvesters.loaders.base.model import Publisher


# ---------------------------------------------------------------------------
# Minimal record factory and async collection helper
# ---------------------------------------------------------------------------

def _record(title: str = "Test") -> DataCite:
    return DataCite.new(title=title, creators=[], publisher=Publisher(name="Test"))


def _collect(harvester: BaseHarvester, ids: list[str]) -> list[RetrievalResult]:
    async def _run() -> list[RetrievalResult]:
        return [r async for r in harvester.iter_results(ids)]
    return asyncio.run(_run())


# ---------------------------------------------------------------------------
# Stub harvesters
# ---------------------------------------------------------------------------

class _SequentialStub(BaseHarvester):
    """Sequential harvester (``_BATCH_SIZE = 1``) with configurable responses."""

    _BATCH_SIZE = 1

    def __init__(
        self,
        responses: dict[str, DataCite],
        *,
        fail_ids: frozenset[str] | set[str] = frozenset(),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._responses = responses
        self._fail_ids = fail_ids
        self.fetch_count = 0

    async def _cache_path(self, id_: str) -> Path:
        return self._cache_root / f"{id_}.json.gz"

    async def _build_request(self, ids: list[str]):
        return f"http://fake/{ids[0]}", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        return {ids[0]: content}

    async def _parse_item(self, content: str) -> DataCite:
        return DataCite.model_validate_json(content)

    async def _fetch(self, url: str, body, headers) -> str:
        self.fetch_count += 1
        id_ = url.split("/")[-1]
        if id_ in self._fail_ids:
            raise RuntimeError(f"simulated failure for {id_!r}")
        return self._responses[id_].model_dump_json()


class _BatchStub(BaseHarvester):
    """Batch harvester (``_BATCH_SIZE = 2``) with configurable batch response.

    ``_split_batch`` serialises each record to JSON and ``_parse_item``
    deserialises it, so raw item strings stored in cache are model JSON.
    """

    _BATCH_SIZE = 2

    def __init__(
        self,
        batch_response: dict[str, DataCite],
        *,
        fail_chunks: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._batch_response = batch_response
        self._fail_chunks = fail_chunks
        self.fetch_count = 0

    async def _cache_path(self, id_: str) -> Path:
        return self._cache_root / f"{id_}.json.gz"

    async def _build_request(self, ids: list[str]):
        return "http://fake/batch", None, None

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        return {id_: record.model_dump_json() for id_, record in self._batch_response.items()}

    async def _parse_item(self, content: str) -> DataCite:
        return DataCite.model_validate_json(content)

    async def _fetch(self, url: str, body, headers) -> str:
        self.fetch_count += 1
        if self._fail_chunks:
            raise RuntimeError("simulated chunk failure")
        return "{}"


# ---------------------------------------------------------------------------
# RetrievalResult — unit tests
# ---------------------------------------------------------------------------

class TestRetrievalResult:
    def test_ok_true_when_record_present(self):
        r = RetrievalResult(id="x", record=_record())
        assert r.ok is True

    def test_ok_false_when_error_set(self):
        r = RetrievalResult(id="x", error="something went wrong")
        assert r.ok is False

    def test_ok_false_when_both_unset(self):
        r = RetrievalResult(id="x")
        assert r.ok is False


# ---------------------------------------------------------------------------
# retrieve (single-item wrapper)
# ---------------------------------------------------------------------------

class TestRetrieve:
    def test_returns_record_on_success(self, tmp_path):
        rec = _record("Success")
        harvester = _SequentialStub({"A": rec}, use_cache=False, cache_root=tmp_path)
        result = asyncio.run(harvester.retrieve("A"))
        assert isinstance(result, DataCite)
        assert result.titles[0].title == "Success"

    def test_raises_on_error(self, tmp_path):
        harvester = _SequentialStub({}, fail_ids={"A"}, use_cache=False, cache_root=tmp_path)
        with pytest.raises(ValueError, match="simulated failure"):
            asyncio.run(harvester.retrieve("A"))


# ---------------------------------------------------------------------------
# Sequential mode (BATCH_SIZE = 0)
# ---------------------------------------------------------------------------

class TestSequentialIterResults:
    def test_successful_retrieval(self, tmp_path):
        recs = {"A": _record("Alpha"), "B": _record("Beta")}
        harvester = _SequentialStub(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B"])
        assert all(r.ok for r in results)
        assert results[0].record is not None and results[0].record.titles[0].title == "Alpha"
        assert results[1].record is not None and results[1].record.titles[0].title == "Beta"

    def test_per_item_failure_reported_as_error(self, tmp_path):
        recs = {"A": _record("Alpha"), "B": _record("Beta")}
        harvester = _SequentialStub(recs, fail_ids={"A"}, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B"])
        assert results[0].ok is False
        assert results[0].error is not None and "simulated failure" in results[0].error
        assert results[1].ok is True

    def test_failure_does_not_abort_remaining_items(self, tmp_path):
        recs = {"A": _record("Alpha"), "B": _record("Beta"), "C": _record("Gamma")}
        harvester = _SequentialStub(recs, fail_ids={"B"}, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B", "C"])
        assert results[0].ok is True
        assert results[1].ok is False
        assert results[2].ok is True

    def test_results_in_input_order(self, tmp_path):
        recs = {str(i): _record(str(i)) for i in range(5)}
        harvester = _SequentialStub(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["4", "2", "0", "3", "1"])
        assert [r.id for r in results] == ["4", "2", "0", "3", "1"]

    def test_cache_hit_skips_fetch(self, tmp_path):
        rec = _record("Cached")
        harvester = _SequentialStub({"A": rec}, use_cache=True, cache_root=tmp_path)
        # Warm the cache manually
        cache_path = tmp_path / "A.json.gz"
        with gzip.open(cache_path, "wt", encoding="utf-8") as f:
            f.write(rec.model_dump_json())

        results = _collect(harvester, ["A"])
        assert results[0].ok is True
        assert harvester.fetch_count == 0

    def test_corrupt_cache_triggers_refetch(self, tmp_path):
        rec = _record("Fresh")
        harvester = _SequentialStub({"A": rec}, use_cache=True, cache_root=tmp_path)
        # Write invalid JSON to the cache
        (tmp_path / "A.json.gz").write_bytes(b"not valid gzip content")

        results = _collect(harvester, ["A"])
        assert results[0].ok is True
        assert harvester.fetch_count == 1  # re-fetched despite cache file existing

    def test_accepts_async_iterable(self, tmp_path):
        rec = _record("Async")
        harvester = _SequentialStub({"A": rec}, use_cache=False, cache_root=tmp_path)

        async def _gen():
            yield "A"

        async def _run():
            return [r async for r in harvester.iter_results(_gen())]

        results = asyncio.run(_run())
        assert results[0].ok is True


# ---------------------------------------------------------------------------
# Batch mode (BATCH_SIZE = 2)
# ---------------------------------------------------------------------------

class TestBatchIterResults:
    def test_successful_batch(self, tmp_path):
        recs = {"A": _record("Alpha"), "B": _record("Beta"), "C": _record("Gamma")}
        harvester = _BatchStub(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B", "C"])
        assert all(r.ok for r in results)

    def test_missing_id_reported_as_error(self, tmp_path):
        # B is not returned by the batch API
        recs = {"A": _record("Alpha")}
        harvester = _BatchStub(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B"])
        a, b = results
        assert a.ok is True
        assert b.ok is False
        assert b.error == "not returned by API"

    def test_chunk_failure_reported_for_all_chunk_members(self, tmp_path):
        # _BATCH_SIZE=2, so ["A","B","C"] → chunks ["A","B"] and ["C"]
        # chunk failure affects all members of the failing chunk
        harvester = _BatchStub({}, fail_chunks=True, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B", "C"])
        assert all(not r.ok for r in results)
        assert all(r.error is not None and "simulated chunk failure" in r.error for r in results)

    def test_results_in_input_order(self, tmp_path):
        recs = {str(i): _record(str(i)) for i in range(5)}
        harvester = _BatchStub(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["4", "2", "0", "3", "1"])
        assert [r.id for r in results] == ["4", "2", "0", "3", "1"]

    def test_successful_records_written_to_cache(self, tmp_path):
        rec = _record("Cached")
        harvester = _BatchStub({"A": rec}, use_cache=True, cache_root=tmp_path)
        _collect(harvester, ["A"])
        cache_file = tmp_path / "A.json.gz"
        assert cache_file.exists()
        with gzip.open(cache_file, "rt", encoding="utf-8") as f:
            restored = DataCite.model_validate_json(f.read())
        assert restored.titles[0].title == "Cached"

    def test_cache_hit_skips_fetch(self, tmp_path):
        rec = _record("Cached")
        harvester = _BatchStub({"A": rec}, use_cache=True, cache_root=tmp_path)
        with gzip.open(tmp_path / "A.json.gz", "wt", encoding="utf-8") as f:
            f.write(rec.model_dump_json())

        results = _collect(harvester, ["A"])
        assert results[0].ok is True
        assert harvester.fetch_count == 0

    def test_corrupt_cache_triggers_refetch(self, tmp_path):
        rec = _record("Fresh")
        harvester = _BatchStub({"A": rec}, use_cache=True, cache_root=tmp_path)
        (tmp_path / "A.json.gz").write_bytes(b"not valid gzip content")

        results = _collect(harvester, ["A"])
        assert results[0].ok is True
        assert harvester.fetch_count == 1

    def test_partial_chunk_failure_leaves_other_chunks_intact(self, tmp_path):
        # With _BATCH_SIZE=2 and 4 IDs: chunks are ["A","B"] and ["C","D"].
        # Make only the first chunk fail.
        call_count = 0

        class _SelectiveFail(_BatchStub):
            async def _fetch(self, url, body, headers):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("first chunk fails")
                return "{}"

        recs = {"C": _record("C"), "D": _record("D")}
        harvester = _SelectiveFail(recs, use_cache=False, cache_root=tmp_path)
        results = _collect(harvester, ["A", "B", "C", "D"])

        a, b, c, d = results
        assert a.ok is False and a.error is not None and "first chunk fails" in a.error
        assert b.ok is False and b.error is not None and "first chunk fails" in b.error
        assert c.ok is True
        assert d.ok is True
