"""
Globus Search API client with per-IP and global rate limiting.
Reference: https://docs.globus.org/api/search/reference/post_query/
"""
import asyncio
import contextvars
import logging
import os
import random
import time
import uuid
from collections import OrderedDict

import httpx

log = logging.getLogger(__name__)

_GSEARCH_BASE = "https://search.api.globus.org/v1"

# Global ceiling: 10 req/s is the shared unauthenticated Globus Search limit; leave headroom.
_GSEARCH_RATE_GLOBAL = float(os.environ.get("GSEARCH_RATE_LIMIT_GLOBAL", "5"))
# Per-IP limit: prevents any one client from monopolizing the global budget.
_GSEARCH_RATE_PER_IP = float(os.environ.get("GSEARCH_RATE_LIMIT_PER_IP", "1"))
_GSEARCH_MAX_RETRIES = 3
_IP_LIMITER_MAX = 10_000  # max tracked IPs; oldest evicted when full

# Set once per HTTP request by the IP middleware; read when issuing search requests.
client_ip_var: contextvars.ContextVar[str] = contextvars.ContextVar("client_ip", default="unknown")


class _RateLimiter:
    """Minimum-interval rate limiter that serialises callers through a single async lock."""

    def __init__(self, rate: float) -> None:
        self._interval = 1.0 / rate
        self._last: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


_global_limiter = _RateLimiter(_GSEARCH_RATE_GLOBAL)
_ip_limiters: OrderedDict[str, _RateLimiter] = OrderedDict()
_ip_limiters_lock = asyncio.Lock()


async def _get_ip_limiter(ip: str) -> _RateLimiter:
    async with _ip_limiters_lock:
        if ip in _ip_limiters:
            _ip_limiters.move_to_end(ip)
        else:
            if len(_ip_limiters) >= _IP_LIMITER_MAX:
                _ip_limiters.popitem(last=False)
            _ip_limiters[ip] = _RateLimiter(_GSEARCH_RATE_PER_IP)
        return _ip_limiters[ip]


def validate_uuid(index_id: str) -> None:
    """Raise ValueError if index_id is not a valid UUID (prevents path manipulation)."""
    try:
        uuid.UUID(index_id)
    except ValueError:
        raise ValueError(f"Invalid index_id (expected UUID): {index_id!r}")


async def search(index_id: str, payload: dict) -> dict:
    """POST a Globus Search query with per-IP + global rate limiting and 429 retry-with-jitter.

    Raises httpx.HTTPStatusError on non-2xx responses after retries are exhausted.
    """
    validate_uuid(index_id)
    ip = client_ip_var.get()
    ip_limiter = await _get_ip_limiter(ip)
    attempt = 0
    while True:
        await ip_limiter.acquire()
        await _global_limiter.acquire()
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{_GSEARCH_BASE}/index/{index_id}/search",
                json=payload,
                timeout=30.0,
            )

        if response.status_code != 429:
            break

        if attempt >= _GSEARCH_MAX_RETRIES:
            log.error("rate-limited index=%s ip=%s exhausted %d retries", index_id, ip, _GSEARCH_MAX_RETRIES)
            break

        retry_after = float(response.headers.get("Retry-After", 1.0))
        jitter = random.uniform(0.0, 1.0)
        log.warning(
            "rate-limited index=%s ip=%s attempt=%d/%d retrying in %.1fs",
            index_id, ip, attempt + 1, _GSEARCH_MAX_RETRIES, retry_after + jitter,
        )
        await asyncio.sleep(retry_after + jitter)
        attempt += 1

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        log.warning("search failed index=%s ip=%s status=%d body=%.200s",
                    index_id, ip, exc.response.status_code, exc.response.text)
        raise

    return response.json()
