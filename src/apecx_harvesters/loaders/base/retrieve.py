from __future__ import annotations

import gzip
import hashlib
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, Generic, TypeVar

import httpx

from .http_retry import http_request
from .model import DataCite
from .rate_limit import RateLimiter

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_ROOT = Path(".cache")

T = TypeVar("T", bound=DataCite)


@dataclass
class RetrievalResult(Generic[T]):
    """Outcome of a single record retrieval attempt."""
    id: str
    record: T | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.record is not None


class BaseHarvester(ABC, Generic[T]):
    """
    Retrieve record(s) from an API by ID, then parse them into a structured format

    Future fetches are cached to support incremental evolution of the data model
    """
    # Chunk size for batching; should respect API limits. 1 = one request per ID
    _BATCH_SIZE: ClassVar[int] = 1
    # Subdirectory under cache_root for this harvester's files.
    _CACHE_DIR: ClassVar[str] = ""
    # Default rate limit for this harvester's API. Subclasses should set this.
    _DEFAULT_REQUESTS_PER_SECOND: ClassVar[float | None] = None

    def __init__(
        self,
        *,
        use_cache: bool = True,
        cache_root: Path | str | None = None,
        client: httpx.AsyncClient | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self._use_cache = use_cache
        self._cache_root = Path(cache_root) if cache_root is not None else _DEFAULT_CACHE_ROOT
        self._client = client
        # If no limiter is provided, create one from the class default (if set).
        # Pass an explicit RateLimiter to share the budget with concurrent search calls.
        if rate_limiter is not None:
            self._rate_limiter: RateLimiter | None = rate_limiter
        elif self._DEFAULT_REQUESTS_PER_SECOND is not None:
            self._rate_limiter = RateLimiter(self._DEFAULT_REQUESTS_PER_SECOND)
        else:
            self._rate_limiter = None

    async def _cache_path(self, id_: str) -> Path:
        """Return the cache file path for a single item ID."""
        if not self._CACHE_DIR:
            raise NotImplementedError(
                f"{type(self).__name__} must either set _CACHE_DIR or override _cache_path"
            )
        safe = re.sub(r'[/\\:*?"<>|\s]', "_", id_)
        prefix = hashlib.md5(safe.encode()).hexdigest()[:2]
        return self._cache_root / self._CACHE_DIR / prefix / f"{safe}.json.gz"

    @abstractmethod
    async def _parse_item(self, content: str) -> T:
        """Parse a single raw item string into a DataCite record.

        For harvesters with ``_BATCH_SIZE == 1``: *content* is the full
        single-item API response.  For batch harvesters: *content* is one
        entry extracted by ``_split_batch``.
        """

    def _normalize_id(self, id_: str) -> str:
        """Normalize an ID before cache lookup and batching (e.g. uppercase for PDB)."""
        return id_

    async def _build_request(self, ids: list[str]) -> tuple[str, str | None, dict | None]:
        """
        Return (url, body, headers) for a request.

        For harvesters with ``_BATCH_SIZE == 1``, *ids* always contains 1 item
        """
        raise NotImplementedError

    async def _split_batch(self, content: str, ids: list[str]) -> dict[str, str]:
        """
        Most API calls request a batch of items, but we cache items singly.
        Split an API response into ``{normalized_id: raw_item_str}`` pairs.
        """
        raise NotImplementedError

    async def _parse_many(self, content: str) -> dict[str, T]:
        """
        Parse a batch response into a ``{normalized_id: record}`` mapping.
        """
        raw_items = await self._split_batch(content, [])
        return {id_: await self._parse_item(raw) for id_, raw in raw_items.items()}

    async def _fetch(self, url: str, body: str | None, headers: dict | None) -> str:
        """Run the actual request. GET when body is None, POST otherwise."""
        assert self._client is not None
        method = "GET" if body is None else "POST"
        kwargs: dict[str, Any] = {}
        if headers:
            kwargs["headers"] = headers
        if body is not None:
            kwargs["content"] = body
        response = await http_request(
            self._client, method, url, rate_limiter=self._rate_limiter, **kwargs
        )
        response.raise_for_status()
        return response.text

    async def _cache_save(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(content)

    async def _load_from_cache(self, id_: str) -> RetrievalResult[T] | None:
        """Return a cached ``RetrievalResult``, or ``None`` on miss or corrupt entry."""
        if not self._use_cache:
            return None
        path = await self._cache_path(id_)
        if path.exists():
            try:
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    return RetrievalResult(id=id_, record=await self._parse_item(f.read()))
            except Exception:
                pass  # corrupt or unreadable → treat as miss
        return None

    async def _iter_chunk(self, chunk: list[str]) -> AsyncIterator[RetrievalResult[T]]:
        """A large batch will be chunked into groups of requests. Fetch items from cache or remote as needed."""
        cached: dict[str, RetrievalResult[T]] = {}
        uncached: list[str] = []

        for id_ in chunk:
            hit = await self._load_from_cache(id_)
            if hit is not None:
                cached[id_] = hit
            else:
                uncached.append(id_)

        fetched: dict[str, RetrievalResult[T]] = {}
        if uncached:
            try:
                url, body, headers = await self._build_request(uncached)
                content = await self._fetch(url, body, headers)
                raw_items = await self._split_batch(content, uncached)
            except Exception as exc:
                for id_ in uncached:
                    fetched[id_] = RetrievalResult(id=id_, error=str(exc))
            else:
                for id_ in uncached:
                    raw = raw_items.get(id_)
                    if raw is not None:
                        try:
                            record = await self._parse_item(raw)
                        except Exception as exc:
                            fetched[id_] = RetrievalResult(id=id_, error=str(exc))
                        else:
                            fetched[id_] = RetrievalResult(id=id_, record=record)
                            if self._use_cache:
                                path = await self._cache_path(id_)
                                await self._cache_save(path, raw)
                    else:
                        fetched[id_] = RetrievalResult(id=id_, error="not returned by API")

        for id_ in chunk:
            yield cached.get(id_) or fetched[id_]

    async def iter_results(
        self, ids: AsyncIterable[str] | Iterable[str]
    ) -> AsyncIterator[RetrievalResult[T]]:
        """
        Retrieve all results, handling pagination where necessary. Consumed via an iterable to allow
            backpressure when harvesting at scale. Uses cache where possible.
        """
        if isinstance(ids, AsyncIterable):
            aids: AsyncIterable[str] = ids
        else:
            async def _wrap(it: Iterable[str]) -> AsyncIterator[str]:
                for item in it:
                    yield item
            aids = _wrap(ids)

        owned = self._client is None
        if owned:
            self._client = httpx.AsyncClient()
        try:
            chunk: list[str] = []
            async for id_ in aids:
                chunk.append(self._normalize_id(id_))
                if len(chunk) >= self._BATCH_SIZE:
                    async for result in self._iter_chunk(chunk):
                        yield result
                    chunk = []
            if chunk:
                async for result in self._iter_chunk(chunk):
                    yield result
        finally:
            if owned:
                assert self._client is not None
                await self._client.aclose()
                self._client = None

    async def iter_cached(
        self, *, since: datetime | None = None
    ) -> AsyncIterator[RetrievalResult[T]]:
        """Yield parsed records from the local cache.

        :param since: If provided, only yield records whose cache file is newer than this datetime.
        """
        if not self._CACHE_DIR:
            raise NotImplementedError(
                f"{type(self).__name__} must either set _CACHE_DIR or override iter_cached"
            )
        cache_dir = self._cache_root / self._CACHE_DIR
        if not cache_dir.exists():
            return
        cutoff = since.timestamp() if since is not None else None
        paths = (
            path
            for subdir in sorted(cache_dir.iterdir())
            if subdir.is_dir()
            for path in sorted(subdir.glob("*.json.gz"))
        )
        for path in paths:
            if cutoff is not None and path.stat().st_mtime <= cutoff:
                continue
            try:
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    record = await self._parse_item(f.read())
                yield RetrievalResult(id=record.canonical_uri, record=record)
            except Exception as exc:
                yield RetrievalResult(id=path.stem, error=str(exc))

    async def retrieve(self, id_: str) -> T:
        """
        Convenience helper to fetch a single record by ID.

        Raises ``ValueError`` if not found or on error.
        """
        async for result in self.iter_results([id_]):
            if not result.ok:
                raise ValueError(result.error or f"Entry not found: {id_!r}")
            record = result.record
            assert record is not None
            return record
        raise ValueError(f"Entry not found: {id_!r}")