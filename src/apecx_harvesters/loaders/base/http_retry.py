from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .rate_limit import RateLimiter

_log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_RATE_LIMIT_SAFETY_FACTOR = 0.9


def _maybe_adjust_rate(response: httpx.Response, rate_limiter: RateLimiter | None) -> None:
    """Lower the rate limiter if the server advertises a stricter limit."""
    if rate_limiter is None:
        return
    raw = response.headers.get("x-ratelimit-limit")
    if raw is None:
        return
    try:
        published = float(raw)
    except ValueError:
        return
    adjusted = published * _RATE_LIMIT_SAFETY_FACTOR
    if adjusted < rate_limiter.rate:
        rate_limiter.set_rate(adjusted)


async def http_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    rate_limiter: RateLimiter | None = None,
    max_retries: int = _MAX_RETRIES,
    **kwargs: Any,
) -> httpx.Response:
    """
    Makes an HTTP request with retry-with-backoff on transient failures.

    - **429**: waits for ``Retry-After`` seconds if the header is present,
      otherwise falls back to a 2-second fixed wait.  If ``x-ratelimit-limit``
      is present, the rate limiter should respect the advertised value going forward.
    - **5xx / transport errors**: exponential backoff capped at 60 seconds.

    Retries are logged at WARNING level.  Raises on final failure.
    """
    for attempt in range(max_retries + 1):
        if rate_limiter is not None:
            await rate_limiter.acquire()
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            if attempt == max_retries:
                raise
            wait = min(2.0 ** attempt, 60.0)
            _log.warning(
                "%s for %s (attempt %d/%d); retrying in %.1fs",
                type(exc).__name__, url, attempt + 1, max_retries, wait,
            )
            await asyncio.sleep(wait)
            continue

        if response.status_code not in _RETRYABLE_STATUSES:
            return response

        if attempt == max_retries:
            response.raise_for_status()  # always raises; status is retryable
            return response  # unreachable

        if response.status_code == 429:
            _maybe_adjust_rate(response, rate_limiter)
            retry_after = response.headers.get("retry-after")
            try:
                wait = float(retry_after) if retry_after is not None else 2.0
            except ValueError:
                wait = 2.0
        else:
            wait = min(2.0 ** attempt, 60.0)

        _log.warning(
            "HTTP %d for %s (attempt %d/%d); retrying in %.1fs — headers: %s",
            response.status_code, url, attempt + 1, max_retries, wait,
            dict(response.headers),
        )
        await asyncio.sleep(wait)

    raise RuntimeError("unreachable")