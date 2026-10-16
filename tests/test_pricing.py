"""Tests for the async price feed.

We monkey-patch the aiohttp session with a fake that records URLs +
returns canned JSON, then exercise:

* stables short-circuit (no network call at all),
* basic fetch + cache hit,
* stale-while-error fallback,
* batched `get_usd_many` with a mix of stables + cache hits + misses.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from pricing import PriceFeed


SOL_MINT = "So11111111111111111111111111111111111111112"
JTO_MINT = "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class _FakeResponse:
    def __init__(self, status: int, body):
        self.status = status
        self._body = body

    async def __aenter__(self): return self

    async def __aexit__(self, *exc): return False

    async def json(self): return self._body


class _FakeSession:
    """Records every GET URL + yields a programmed sequence of responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        if not self.responses:
            return _FakeResponse(500, {})
        return self.responses.pop(0)

    async def close(self): pass


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.mark.asyncio
async def test_stable_shortcircuits_without_network():
    pf = PriceFeed(coalesce_window_ms=0)
    pf._session = _FakeSession([])
    assert await pf.get_usd(USDC_MINT) == 1.0
    assert pf._session.calls == []


@pytest.mark.asyncio
async def test_fetch_then_cache_hit():
    body = {"data": {SOL_MINT: {"price": "150.25"}}}
    pf = PriceFeed(coalesce_window_ms=0)
    pf._session = _FakeSession([_FakeResponse(200, body)])
    p = await pf.get_usd(SOL_MINT)
    assert p == pytest.approx(150.25)
    # Cache hit on second call: no further session calls.
    p2 = await pf.get_usd(SOL_MINT)
    assert p2 == pytest.approx(150.25)
    assert len(pf._session.calls) == 1


@pytest.mark.asyncio
async def test_unknown_mint_returns_none():
    body = {"data": {}}
    pf = PriceFeed(coalesce_window_ms=0)
    pf._session = _FakeSession([_FakeResponse(200, body)])
    p = await pf.get_usd("NotARealMint11111111111111111111111111111111")
    assert p is None


@pytest.mark.asyncio
async def test_get_usd_many_mixes_stables_cache_and_fetch():
    # Pre-populate the cache for SOL so it should not hit the network.
    import time
    pf = PriceFeed(ttl_sec=60, coalesce_window_ms=0)
    pf._cache[SOL_MINT] = (123.45, time.monotonic())
    body = {"data": {JTO_MINT: {"price": "2.10"}}}
    pf._session = _FakeSession([_FakeResponse(200, body)])

    out = await pf.get_usd_many([USDC_MINT, SOL_MINT, JTO_MINT])
    assert out[USDC_MINT] == 1.0
    assert out[SOL_MINT] == pytest.approx(123.45)
    assert out[JTO_MINT] == pytest.approx(2.10)
    # Only one HTTP call (just for the missing JTO).
    assert len(pf._session.calls) == 1
    assert "ids" in pf._session.calls[0][1]
    assert pf._session.calls[0][1]["ids"] == JTO_MINT


@pytest.mark.asyncio
async def test_stale_grace_serves_old_value_on_error():
    import time
    pf = PriceFeed(ttl_sec=0, stale_grace_sec=600, coalesce_window_ms=0)
    pf._cache[SOL_MINT] = (99.0, time.monotonic() - 30)
    pf._session = _FakeSession([_FakeResponse(500, {})])
    # First call now returns the stale value (since the upstream errored
    # and we have a still-fresh-enough cache entry).
    p = await pf.get_usd(SOL_MINT)
    assert p == 99.0
