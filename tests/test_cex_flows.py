"""Tests for the CEX flow tracker's transaction decoder + bot.py glue.

We don't run the websocket loop — instead we hand-craft a `getTransaction`
response with `pre/postTokenBalances` deltas + a registered CEX wallet,
inject a stub price feed, and assert the decoder emits the right number
of `CexFlowEvent`s with the right direction and USD valuation.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from cex_wallets import CEX_WALLETS, WSOL_MINT
from trackers.cex_flows import CexFlowsTracker, CexFlowEvent


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
BINANCE_HOT = CEX_WALLETS[0].address    # first Binance entry


class _StubRpc:
    """Pretend `SolanaRpc` — only `get_transaction` is exercised here."""

    def __init__(self): pass

    async def get_transaction(self, sig): return None  # not used by _decode_tx


class _StubPriceFeed:
    def __init__(self, prices): self._prices = dict(prices)

    async def get_usd_many(self, mints):
        return {m: self._prices.get(m) for m in mints}


def _make_tracker(prices=None):
    return CexFlowsTracker(
        ws_url="wss://example.invalid",
        rpc=_StubRpc(),
        price_feed=_StubPriceFeed(prices or {USDC_MINT: 1.0, WSOL_MINT: 150.0}),
        wallets=list(CEX_WALLETS),
        min_flow_usd=0.0,            # don't filter so we can assert emission
    )


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_tx_with_token_transfer(
    *, from_owner: str, to_owner: str, mint: str, amount: float,
    decimals: int = 6, accountKeys: list[str] | None = None,
):
    """Synthesize a getTransaction response for a single SPL transfer."""
    keys = accountKeys or [from_owner, to_owner, "Token1111", "System111"]
    return {
        "transaction": {"message": {"accountKeys": keys}},
        "blockTime": 1700000000,
        "meta": {
            "err": None,
            "fee": 5000,
            "preBalances":  [1_000_000_000, 1_000_000_000, 0, 0],
            "postBalances": [1_000_000_000 - 5000, 1_000_000_000, 0, 0],
            "preTokenBalances": [
                {"accountIndex": 0, "mint": mint, "owner": from_owner,
                 "uiTokenAmount": {"uiAmount": amount, "decimals": decimals}},
                {"accountIndex": 1, "mint": mint, "owner": to_owner,
                 "uiTokenAmount": {"uiAmount": 0.0, "decimals": decimals}},
            ],
            "postTokenBalances": [
                {"accountIndex": 0, "mint": mint, "owner": from_owner,
                 "uiTokenAmount": {"uiAmount": 0.0, "decimals": decimals}},
                {"accountIndex": 1, "mint": mint, "owner": to_owner,
                 "uiTokenAmount": {"uiAmount": amount, "decimals": decimals}},
            ],
        },
    }


def test_decode_user_to_cex_emits_inflow():
    user = "USER1usw1tx5VntJ5gAh77GdSj5MDcCZSGyPv7g8Vmw9j"
    tx = _make_tx_with_token_transfer(
        from_owner=user, to_owner=BINANCE_HOT, mint=USDC_MINT, amount=10_000.0,
    )
    tr = _make_tracker()
    events = _run(tr._decode_tx(tx, signature="sigA", slot=42))
    assert len(events) == 1
    ev = events[0]
    assert ev.exchange == "Binance"
    assert ev.direction == "IN"
    assert ev.asset == "USDC"
    assert ev.amount == pytest.approx(10_000.0)
    assert ev.amount_usd == pytest.approx(10_000.0)
    assert ev.counterparty == user


def test_decode_cex_to_user_emits_outflow():
    user = "USER2usw1tx5VntJ5gAh77GdSj5MDcCZSGyPv7g8Vmw9k"
    tx = _make_tx_with_token_transfer(
        from_owner=BINANCE_HOT, to_owner=user, mint=USDC_MINT, amount=4_321.0,
    )
    tr = _make_tracker()
    events = _run(tr._decode_tx(tx, signature="sigB", slot=43))
    assert len(events) == 1
    ev = events[0]
    assert ev.direction == "OUT"
    assert ev.asset == "USDC"
    assert ev.amount == pytest.approx(4_321.0)
    assert ev.counterparty == user


def test_decode_skips_non_cex_transfers():
    a = "USER3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    b = "USER4bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    tx = _make_tx_with_token_transfer(
        from_owner=a, to_owner=b, mint=USDC_MINT, amount=999.0,
    )
    tr = _make_tracker()
    events = _run(tr._decode_tx(tx, signature="sigC", slot=44))
    assert events == []


def test_decode_uses_price_feed_for_non_stables():
    user = "USER5sssssssssssssssssssssssssssssssssssssss"
    # Native SOL sweep into Binance hot wallet (1.5 SOL ≈ $225 at 150/SOL).
    tx = _make_tx_with_token_transfer(
        from_owner=user, to_owner=BINANCE_HOT, mint=WSOL_MINT,
        amount=1.5, decimals=9,
    )
    tr = _make_tracker(prices={WSOL_MINT: 150.0})
    events = _run(tr._decode_tx(tx, signature="sigD", slot=45))
    assert len(events) == 1
    ev = events[0]
    assert ev.asset == "SOL"
    assert ev.amount == pytest.approx(1.5)
    assert ev.amount_usd == pytest.approx(225.0)


def test_decode_skips_unknown_mint():
    user = "USER6uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu"
    fake_mint = "Garbage111111111111111111111111111111111111"
    tx = _make_tx_with_token_transfer(
        from_owner=user, to_owner=BINANCE_HOT, mint=fake_mint, amount=1000.0,
    )
    tr = _make_tracker()
    events = _run(tr._decode_tx(tx, signature="sigE", slot=46))
    assert events == []
