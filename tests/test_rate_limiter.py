"""Tests for the token-bucket RateLimiter."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from apecx_harvesters.loaders.base.rate_limit import RateLimiter

_MOD = "apecx_harvesters.loaders.base.rate_limit"


class TestRateLimiter:
    def test_first_acquire_is_immediate(self):
        """Bucket starts with one token; first acquire never sleeps."""
        limiter = RateLimiter(5.0)

        async def _run():
            with patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await limiter.acquire()
                mock_sleep.assert_not_called()

        asyncio.run(_run())

    def test_no_burst_after_first_acquire(self):
        """Only one free acquire at startup — no multi-token burst regardless of rate."""
        # rate=3: bucket capacity=3, but starts at 1. Second immediate acquire must sleep.
        # time.monotonic: __init__(→0.0), acq1(→0.0), acq2(→0.0), acq2_post_sleep(→0.0)
        async def _run():
            with patch(f"{_MOD}.time") as mock_time, \
                 patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_time.monotonic.side_effect = [0.0, 0.0, 0.0, 0.0]
                limiter = RateLimiter(3.0)
                await limiter.acquire()
                mock_sleep.assert_not_called()
                await limiter.acquire()
                mock_sleep.assert_called_once()

        asyncio.run(_run())

    def test_depleted_bucket_sleeps_correct_duration(self):
        """After exhausting the token, the next acquire sleeps for 1/rate seconds."""
        # rate=1: one token, used immediately. Second acquire must sleep 1.0s.
        # time.monotonic: __init__(→0.0), acquire1(→0.0), acquire2(→0.0), acquire2_post_sleep(→0.0)
        async def _run():
            with patch(f"{_MOD}.time") as mock_time, \
                 patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_time.monotonic.side_effect = [0.0, 0.0, 0.0, 0.0]
                limiter = RateLimiter(1.0)
                await limiter.acquire()
                mock_sleep.assert_not_called()
                await limiter.acquire()
                mock_sleep.assert_called_once_with(pytest.approx(1.0))

        asyncio.run(_run())

    def test_elapsed_time_reduces_sleep_duration(self):
        """Time elapsed between acquires refills the bucket, reducing the wait."""
        # rate=2, start=1 token. First acquire is free. Second at t=0.25s:
        # tokens = 0 + 0.25*2 = 0.5 → sleep = (1-0.5)/2 = 0.25s (not 0.5s).
        # time.monotonic: __init__(→0.0), acq1(→0.0), acq2(→0.25), acq2_post_sleep(→0.25)
        async def _run():
            with patch(f"{_MOD}.time") as mock_time, \
                 patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_time.monotonic.side_effect = [0.0, 0.0, 0.25, 0.25]
                limiter = RateLimiter(2.0)
                await limiter.acquire()
                mock_sleep.assert_not_called()
                await limiter.acquire()
                mock_sleep.assert_called_once_with(pytest.approx(0.25))

        asyncio.run(_run())

    def test_full_refill_allows_immediate_acquire(self):
        """After a full refill period, acquires are immediate again."""
        # rate=2: first acquire depletes to 0 at t=0. At t=1.0s, tokens = min(2, 0+2) = 2 → no sleep.
        # time.monotonic: __init__(→0.0), acq1(→0.0), acq2(→1.0)
        async def _run():
            with patch(f"{_MOD}.time") as mock_time, \
                 patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_time.monotonic.side_effect = [0.0, 0.0, 1.0]
                limiter = RateLimiter(2.0)
                await limiter.acquire()
                await limiter.acquire()  # 1s later — refilled, no sleep
                mock_sleep.assert_not_called()

        asyncio.run(_run())

    def test_token_count_capped_at_one(self):
        """Long idle periods never accumulate more than 1 token, preventing burst."""
        # rate=2: after 100s idle, tokens = min(1.0, 1+100*2) = 1.0, not 201.
        # Only 1 free acquire, then sleep on the second.
        # time.monotonic: __init__(→0.0), acq1(→100.0), acq2(→100.0), acq2_post_sleep(→100.0)
        async def _run():
            with patch(f"{_MOD}.time") as mock_time, \
                 patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_time.monotonic.side_effect = [0.0, 100.0, 100.0, 100.0]
                limiter = RateLimiter(2.0)
                await limiter.acquire()
                mock_sleep.assert_not_called()
                await limiter.acquire()
                mock_sleep.assert_called_once_with(pytest.approx(0.5))

        asyncio.run(_run())