from __future__ import annotations

import asyncio
import logging
import time

_log = logging.getLogger(__name__)


class RateLimiter:
    """
    Token-bucket rate limiter for async callers.

    A shared instance coordinates budget across concurrent callers (e.g. search
    and retrieval against the same API host).  Each call to ``acquire()`` blocks
    until a token is available, then consumes one.
    """

    def __init__(self, rate: float, *, name: str = "") -> None:
        self._rate = rate
        self._name = name or "limiter"
        self._tokens: float = 1.0  # one free token so the first request isn't delayed; avoids burst
        self._last: float = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a request token is available, then consume one."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(1.0, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                _log.debug("[%s] immediate (elapsed since last: %.3fs)", self._name, elapsed)
            else:
                wait = (1 - self._tokens) / self._rate
                _log.debug(
                    "[%s] sleeping %.3fs (tokens=%.3f, elapsed since last: %.3fs)",
                    self._name, wait, self._tokens, elapsed,
                )
                await asyncio.sleep(wait)
                self._tokens = 0
                self._last = time.monotonic()