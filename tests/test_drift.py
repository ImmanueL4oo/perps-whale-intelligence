"""Unit tests for the Drift Protocol tracker.

Covers
------
* `_coin_from_symbol` strips Drift's `1M` / `1K` size-scale prefixes so we
  align with cross-venue analytics (kPEPE on HL → PEPE on both).
* `_side_from_direction` maps taker direction correctly, *flipping* on
  liquidation actions because the bot is the taker but the trader on the
  receiving end is what we care about.
* `_extract_events` honours the threshold, dedupe cursor, and coin filter.
* `_extract_events` returns events ordered oldest→newest so the cursor
  monotonically advances.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import time

from trackers import EventKind, Side
from trackers.drift import (
    DriftTracker,
    _TokenBucket,
    _coin_from_symbol,
    _kind_from_action,
    _parse_amount,
    _side_from_direction,
)


def test_coin_from_symbol_strips_size_prefix():
    assert _coin_from_symbol("SOL-PERP") == "SOL"
    assert _coin_from_symbol("1MPEPE-PERP") == "PEPE"
    assert _coin_from_symbol("1KBONK-PERP") == "BONK"
    # Extra defensiveness — already-uppercased / mixed-case input.
    assert _coin_from_symbol("hype-perp") == "HYPE"


def test_side_from_direction_basic():
    assert _side_from_direction("long", "fill") == Side.LONG
    assert _side_from_direction("short", "fill") == Side.SHORT
    # Liquidation: the LIQUIDATOR's direction is opposite to the liquidated
    # trader's. We attribute the position to the trader being closed out.
    assert _side_from_direction("long", "liquidatePerp") == Side.SHORT
    assert _side_from_direction("short", "liquidatePerp") == Side.LONG


def test_kind_from_action_detects_liquidation():
    assert _kind_from_action("fill") == EventKind.OPEN
    assert _kind_from_action("liquidatePerp") == EventKind.LIQUIDATION


def test_parse_amount_handles_strings_and_garbage():
    assert _parse_amount("1.5") == 1.5
    assert _parse_amount("0.000600000") == 0.000600000
    assert _parse_amount(None) == 0.0
    assert _parse_amount("not-a-number") == 0.0


# ── _extract_events ──────────────────────────────────────────────────────────
def _record(
    *,
    fill_id: int,
    notional: float,
    base: float = 1.0,
    side: str = "long",
    sym: str = "SOL-PERP",
    sig: str = "sig",
    action: str = "fill",
    taker: str = "Tkr",
    maker: str = "Mkr",
    ts: int = 1_700_000_000,
    market_index: int = 0,
) -> dict:
    return {
        "fillRecordId": str(fill_id),
        "txSig": sig,
        "ts": ts,
        "quoteAssetAmountFilled": f"{notional:.6f}",
        "baseAssetAmountFilled": f"{base:.9f}",
        "oraclePrice": "100.0",
        "takerOrderDirection": side,
        "taker": taker,
        "maker": maker,
        "action": action,
        "symbol": sym,
        "marketIndex": market_index,
    }


def _tracker(threshold: float = 1_000_000.0, coins=None) -> DriftTracker:
    return DriftTracker(
        threshold_usd=threshold,
        markets=["SOL-PERP", "BTC-PERP"],
        coins=coins,
        poll_interval_sec=3.0,
    )


def test_extract_events_filters_below_threshold():
    t = _tracker(threshold=1_000_000)
    events = t._extract_events("SOL-PERP", [
        _record(fill_id=1, notional=999_999),  # below
        _record(fill_id=2, notional=2_500_000, sig="big-tx"),  # whale
    ])
    assert len(events) == 1
    ev = events[0]
    assert ev.venue == "Drift"
    assert ev.coin == "SOL"
    assert ev.side == Side.LONG
    assert ev.notional_usd == 2_500_000.0
    assert ev.tx_ref == "big-tx"


def test_extract_events_advances_cursor_monotonically():
    t = _tracker(threshold=100)
    state = t._state["SOL-PERP"]
    out1 = t._extract_events("SOL-PERP", [
        _record(fill_id=10, notional=1_000_000, sig="a"),
        _record(fill_id=11, notional=2_000_000, sig="b"),
    ])
    assert [e.tx_ref for e in out1] == ["a", "b"]
    assert state.last_fill_record_id == 11

    # Replay the same records: must NOT re-emit.
    out2 = t._extract_events("SOL-PERP", [
        _record(fill_id=10, notional=1_000_000, sig="a"),
        _record(fill_id=11, notional=2_000_000, sig="b"),
    ])
    assert out2 == []

    # New records advance.
    out3 = t._extract_events("SOL-PERP", [
        _record(fill_id=12, notional=3_000_000, sig="c"),
    ])
    assert [e.tx_ref for e in out3] == ["c"]
    assert state.last_fill_record_id == 12


def test_extract_events_coin_filter():
    t = _tracker(threshold=100, coins=["BTC"])
    out = t._extract_events("SOL-PERP", [
        _record(fill_id=1, notional=2_000_000, sym="SOL-PERP"),
    ])
    assert out == []  # SOL filtered out


def test_extract_events_dedupes_across_markets():
    """If two markets ever share a `txSig+fillRecordId` (shouldn't happen but
    worth defending against), our second-line dedupe catches it.
    """
    t = _tracker(threshold=100)
    # Different markets, same sig+id -> only first emits.
    out_a = t._extract_events("SOL-PERP", [
        _record(fill_id=1, notional=2_000_000, sig="dup", sym="SOL-PERP"),
    ])
    out_b = t._extract_events("BTC-PERP", [
        _record(fill_id=1, notional=2_000_000, sig="dup", sym="BTC-PERP"),
    ])
    assert len(out_a) == 1
    assert len(out_b) == 0


def test_token_bucket_caps_rps():
    """At 5 RPS with burst=2, 4 calls in a tight loop must take roughly
    (4-2) / 5 = 0.4s. Generous slack accommodates CI variance."""
    bucket = _TokenBucket(rate_per_sec=5.0, burst=2)

    async def _go():
        start = time.monotonic()
        for _ in range(4):
            await bucket.acquire()
        return time.monotonic() - start

    elapsed = asyncio.new_event_loop().run_until_complete(_go())
    # First two should be free (burst), next two each ~0.2s. Allow [0.3, 1.5].
    assert 0.3 <= elapsed <= 1.5, f"unexpected elapsed: {elapsed:.3f}s"


def test_extract_events_liquidation_flips_side():
    t = _tracker(threshold=100)
    out = t._extract_events("SOL-PERP", [
        _record(
            fill_id=1, notional=5_000_000, side="long",
            action="liquidatePerpPosition", sig="liq",
        ),
    ])
    assert len(out) == 1
    assert out[0].kind == EventKind.LIQUIDATION
    # Taker direction was "long" but in a liquidation that's the liquidator.
    # The TRADER side is the opposite — SHORT.
    assert out[0].side == Side.SHORT


# ── Drift wallet track-record helper ──────────────────────────────────────────
def test_stats_from_drift_fills_aggregates_volume_and_coins():
    from trackrecord import _stats_from_drift_fills

    wallet = "WhaleW111111111111111111111111111111111111"
    fills = [
        {
            "ts": 1700000000, "txSig": "a", "fillRecordId": "1",
            "quoteAssetAmountFilled": "120.5", "baseAssetAmountFilled": "1.0",
            "oraclePrice": "120.5", "takerFee": "0.05",
            "taker": wallet, "maker": "Other", "takerOrderDirection": "long",
            "symbol": "SOL-PERP", "action": "fill",
        },
        {
            "ts": 1700001000, "txSig": "b", "fillRecordId": "2",
            "quoteAssetAmountFilled": "50.0", "baseAssetAmountFilled": "0.001",
            "oraclePrice": "50000.0", "makerFee": "-0.01",
            "taker": "Other", "maker": wallet, "makerOrderDirection": "short",
            "symbol": "BTC-PERP", "action": "fill",
        },
        {  # malformed row that must be ignored
            "ts": 0, "quoteAssetAmountFilled": "0",
            "taker": wallet, "symbol": "SOL-PERP",
        },
    ]
    stats = _stats_from_drift_fills(wallet, fills)

    assert stats.venue == "Drift"
    assert stats.wallet == wallet
    assert stats.closed_trades == 2  # malformed row dropped
    # taker fee + maker rebate (negative) = 0.04
    assert abs(stats.fee_paid_usd - 0.04) < 1e-9
    # PnL stays unknowable, so the wallet must NOT bucket above NEW.
    assert stats.realized_pnl_usd == 0.0
    assert stats.tier().label == "NEW"
    # Per-coin aggregation keeps both markets, kPEPE-style stripping not
    # applied here since these symbols have no 1M/1K prefix.
    assert set(stats.by_coin.keys()) == {"SOL", "BTC"}
    assert stats.by_coin["SOL"].trades == 1
    assert stats.by_coin["BTC"].trades == 1
    # Coverage note is honest about why PnL is missing.
    assert "doesn't expose realized PnL" in (stats.coverage_note or "")


def test_stats_from_drift_fills_strips_size_prefix():
    from trackrecord import _stats_from_drift_fills

    wallet = "Memew111111111111111111111111111111111111"
    fills = [
        {
            "ts": 1700000000, "fillRecordId": "1",
            "quoteAssetAmountFilled": "10000",
            "baseAssetAmountFilled": "1000000",
            "oraclePrice": "0.01",
            "taker": wallet, "takerOrderDirection": "long",
            "symbol": "1MPEPE-PERP",
        },
    ]
    stats = _stats_from_drift_fills(wallet, fills)
    assert "PEPE" in stats.by_coin


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK {name}")
