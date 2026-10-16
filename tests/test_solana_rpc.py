"""Unit tests for the small Solana RPC helper.

We don't hit the network — the aiohttp session is monkey-patched with a tiny
fake that records calls and returns canned responses. This keeps the test
deterministic and ~100x faster.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trackers.solana_rpc import SolanaRpc


# ── Fake aiohttp ─────────────────────────────────────────────────────────────
class _FakeResponse:
    def __init__(self, status: int, body):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._body

    async def text(self):
        return str(self._body)


class _FakeSession:
    """Return a programmable sequence of responses — pop one per .post call."""

    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.calls: list[dict] = []
        self.closed = False

    def post(self, url, *, json, timeout=None):
        self.calls.append({"url": url, "payload": json})
        if not self.sequence:
            raise RuntimeError("test exhausted fake response sequence")
        return self.sequence.pop(0)

    async def close(self):
        self.closed = True


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ── Tests ────────────────────────────────────────────────────────────────────
def test_call_returns_result_on_first_success():
    rpc = SolanaRpc(http_url="x", base_backoff_sec=0.001, max_retries=2)
    fake = _FakeSession([
        _FakeResponse(200, {"jsonrpc": "2.0", "id": 1, "result": {"hello": "world"}}),
    ])
    rpc._session = fake
    out = _run(rpc.call("dummy", []))
    assert out == {"hello": "world"}
    assert rpc.stats["ok"] == 1
    assert len(fake.calls) == 1


def test_call_retries_on_429_then_succeeds():
    rpc = SolanaRpc(http_url="x", base_backoff_sec=0.001, max_retries=2)
    fake = _FakeSession([
        _FakeResponse(429, "rate limit"),
        _FakeResponse(200, {"jsonrpc": "2.0", "result": "ok"}),
    ])
    rpc._session = fake
    out = _run(rpc.call("dummy", []))
    assert out == "ok"
    assert rpc.stats["rate_limited"] == 1
    assert rpc.stats["ok"] == 1


def test_call_returns_none_after_exhausting_retries():
    rpc = SolanaRpc(http_url="x", base_backoff_sec=0.001, max_retries=1)
    fake = _FakeSession([
        _FakeResponse(429, "x"),
        _FakeResponse(429, "x"),
    ])
    rpc._session = fake
    out = _run(rpc.call("dummy", []))
    assert out is None
    assert rpc.stats["rate_limited"] == 2


def test_call_treats_null_result_as_retriable():
    """getTransaction returns null when slot isn't finalized — we retry."""
    rpc = SolanaRpc(http_url="x", base_backoff_sec=0.001, max_retries=2)
    fake = _FakeSession([
        _FakeResponse(200, {"jsonrpc": "2.0", "result": None}),
        _FakeResponse(200, {"jsonrpc": "2.0", "result": {"slot": 1}}),
    ])
    rpc._session = fake
    out = _run(rpc.call("getTransaction", []))
    assert out == {"slot": 1}
    assert rpc.stats["null_results"] == 1
    assert rpc.stats["ok"] == 1


def test_all_account_keys_merges_lookup_addresses():
    tx = {
        "transaction": {"message": {"accountKeys": ["A", "B", "C"]}},
        "meta": {"loadedAddresses": {"writable": ["W1"], "readonly": ["R1", "R2"]}},
    }
    assert SolanaRpc.all_account_keys(tx) == ["A", "B", "C", "W1", "R1", "R2"]


def test_all_account_keys_handles_missing_loaded_addresses():
    tx = {"transaction": {"message": {"accountKeys": ["X"]}}, "meta": {}}
    assert SolanaRpc.all_account_keys(tx) == ["X"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK {name}")
