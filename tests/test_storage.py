"""Tests for the MongoStorage layer.

These tests are skipped (with a clear message) if MongoDB isn't reachable on
localhost:27017. The graceful-fallback test always runs.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_bus import serialize_event
from storage import MongoStorage, build_query, doc_id
from trackers import EventKind, Side, WhaleEvent
from trackrecord import _empty_stats


def _sample_payload(notional: float = 1_000_000, kind: EventKind = EventKind.OPEN, *,
                    coin: str = "BTC", trader: str = "0xabc", tx: str = "tid-1",
                    venue: str = "Hyperliquid", with_stats: bool = False) -> dict:
    extra = {}
    if with_stats:
        s = _empty_stats(venue, trader, None)
        s.closed_trades = 50
        s.win_count = 35
        s.realized_pnl_usd = 4_200_000
        extra["track_record"] = s
    ev = WhaleEvent(
        venue=venue, kind=kind, coin=coin, side=Side.LONG,
        notional_usd=notional, price_usd=60_000.0, size=notional / 60_000.0,
        trader=trader, tx_ref=tx, leverage=10.0, extra=extra,
    )
    return serialize_event(ev)


# ── Pure helpers ─────────────────────────────────────────────────────────────
def test_doc_id_is_stable():
    p1 = _sample_payload()
    p2 = _sample_payload()
    assert doc_id(p1) == doc_id(p2)
    assert doc_id(p1) == "Hyperliquid|tid-1|0xabc|BTC|OPEN"


def test_doc_id_distinct_by_kind():
    a = _sample_payload(kind=EventKind.OPEN)
    b = _sample_payload(kind=EventKind.CLOSE)
    assert doc_id(a) != doc_id(b)


def test_build_query_handles_filters():
    q = build_query({
        "venue": "Hyperliquid",
        "kind": "OPEN",
        "coin": "BTC",
        "trader": "0xABC",
        "min_notional": 1_000_000,
        "min_score": 70,
        "after_ts": 1700000000000,
        "before_ts": 1800000000000,
    })
    assert q["venue"] == "Hyperliquid"
    assert q["kind"] == "OPEN"
    assert q["coin"] == "BTC"
    assert q["trader"]["$options"] == "i"
    assert q["notional_usd"] == {"$gte": 1_000_000.0}
    assert q["track_record.score"] == {"$gte": 70.0}
    assert q["ts_ms"] == {"$gte": 1700000000000, "$lt": 1800000000000}


def test_build_query_empty():
    assert build_query({}) == {}
    assert build_query({"min_notional": 0, "min_score": 0, "trader": ""}) == {}


# ── Graceful fallback ────────────────────────────────────────────────────────
async def _exercise_unreachable_mongo():
    s = MongoStorage(uri="mongodb://127.0.0.1:1", db_name="t", collection_name="t")
    ok = await s.connect(ping_timeout_s=1.0)
    assert ok is False
    assert s.available is False
    # All ops are no-ops, no exceptions:
    assert await s.insert(_sample_payload()) is False
    assert await s.recent(limit=10) == []
    assert await s.history(limit=10, venue="Hyperliquid") == []
    assert await s.wallet_history("Hyperliquid", "0xabc") == []
    assert await s.total_count() == 0
    await s.close()


def test_unreachable_mongo_is_a_noop():
    asyncio.run(_exercise_unreachable_mongo())


# ── Real Mongo round-trip (auto-skipped if Mongo is down) ────────────────────
TEST_DB = "perps_whales_test"
TEST_COLL = f"events_{int(time.time())}"


async def _check_mongo() -> bool:
    s = MongoStorage(db_name=TEST_DB, collection_name=TEST_COLL)
    return await s.connect(ping_timeout_s=2.0)


async def _exercise_round_trip():
    s = MongoStorage(db_name=TEST_DB, collection_name=TEST_COLL)
    if not await s.connect(ping_timeout_s=2.0):
        return False  # tells the caller we skipped

    try:
        # Drop the collection so each run starts clean.
        await s._coll.drop()
        await s._ensure_indexes()

        # Insert three events: BTC open + ETH open + BTC liquidation.
        a = _sample_payload(notional=2_000_000, coin="BTC", tx="tid-A", with_stats=True)
        b = _sample_payload(notional=500_000, coin="ETH", tx="tid-B")
        c = _sample_payload(notional=8_000_000, coin="BTC", tx="tid-C", kind=EventKind.LIQUIDATION)
        # Adjust ts_ms so the order is deterministic.
        a["ts_ms"], b["ts_ms"], c["ts_ms"] = 1700000000000, 1700000010000, 1700000020000

        assert await s.insert(a) is True
        assert await s.insert(b) is True
        assert await s.insert(c) is True
        # Re-inserting any of them is a silent no-op (dedupe via _id).
        assert await s.insert(a) is False

        assert await s.total_count() >= 3

        recent = await s.recent(limit=10)
        # Returned oldest-first to match bus._recent semantics.
        assert [r["tx_ref"] for r in recent] == ["tid-A", "tid-B", "tid-C"]

        # /api/history filters: only big BTC events ≥ $1M
        big_btc = await s.history(limit=10, coin="BTC", min_notional=1_000_000)
        assert {r["tx_ref"] for r in big_btc} == {"tid-A", "tid-C"}
        # Newest first when querying history.
        assert big_btc[0]["tx_ref"] == "tid-C"

        liqs = await s.history(limit=10, kind="LIQ")
        assert [r["tx_ref"] for r in liqs] == ["tid-C"]

        partial_addr = await s.history(limit=10, trader="0xab")
        assert len(partial_addr) >= 3   # case-insensitive substring matches all

        wallet_hist = await s.wallet_history("Hyperliquid", "0xabc", limit=10)
        assert len(wallet_hist) >= 3

        # min_score on a tiered event keeps it; non-tiered events are filtered.
        elite_only = await s.history(limit=10, min_score=70)
        assert all(
            r.get("track_record") and (r["track_record"].get("score") or 0) >= 70
            for r in elite_only
        )
        assert any(r["tx_ref"] == "tid-A" for r in elite_only)

        return True
    finally:
        try:
            await s._coll.drop()
        except Exception:
            pass
        await s.close()


def test_real_mongo_round_trip():
    available = asyncio.run(_check_mongo())
    if not available:
        print("(skipped — MongoDB not reachable on localhost:27017)")
        return
    ok = asyncio.run(_exercise_round_trip())
    assert ok, "round-trip should have completed"


if __name__ == "__main__":
    test_doc_id_is_stable()
    test_doc_id_distinct_by_kind()
    test_build_query_handles_filters()
    test_build_query_empty()
    test_unreachable_mongo_is_a_noop()
    print("storage helpers OK")
    test_real_mongo_round_trip()
    print("storage round-trip OK")
