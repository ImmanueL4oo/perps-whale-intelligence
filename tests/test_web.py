"""Tests for the event bus, JSON serialization, and the HTTP/SSE endpoints."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiohttp
from aiohttp import web

from event_bus import EventBus, serialize_event
from trackers import EventKind, Side, WhaleEvent
from trackrecord import _empty_stats
import webserver


def _sample_event(notional: float = 6_000_000, with_stats: bool = False) -> WhaleEvent:
    extra: dict = {}
    if with_stats:
        s = _empty_stats("Hyperliquid", "0xabc", None)
        s.closed_trades = 50
        s.win_count = 35
        s.realized_pnl_usd = 4_200_000
        s.pnl_30d_usd = 800_000
        extra["track_record"] = s
    return WhaleEvent(
        venue="Hyperliquid",
        kind=EventKind.OPEN,
        coin="BTC",
        side=Side.LONG,
        notional_usd=notional,
        price_usd=60_000.0,
        size=notional / 60_000.0,
        trader="0xabc1230000000000000000000000000000000000",
        tx_ref=f"tid-{int(time.time()*1000)}",
        leverage=10.0,
        extra=extra,
    )


def test_serialize_event_basic():
    p = serialize_event(_sample_event())
    assert p["venue"] == "Hyperliquid"
    assert p["kind"] == "OPEN"
    assert p["side"] == "long"
    assert p["coin"] == "BTC"
    assert "track_record" not in p  # no stats attached


def test_serialize_event_with_stats():
    p = serialize_event(_sample_event(with_stats=True))
    tr = p["track_record"]
    assert tr["tier"] == "ELITE"
    assert tr["winrate"] == 35 / 50
    assert tr["realized_pnl_usd"] == 4_200_000
    json.dumps(p)  # must be JSON-clean


def test_bus_publish_and_recent():
    bus = EventBus(recent_size=3)
    bus.publish(_sample_event(notional=1_000_000))
    bus.publish(_sample_event(notional=2_000_000))
    bus.publish(_sample_event(notional=3_000_000))
    bus.publish(_sample_event(notional=4_000_000))  # evicts oldest
    rec = bus.recent()
    assert len(rec) == 3
    assert rec[0]["notional_usd"] == 2_000_000
    assert rec[-1]["notional_usd"] == 4_000_000


def test_bus_stats_aggregation():
    bus = EventBus()
    bus.publish(_sample_event(notional=5e6))
    short = _sample_event(notional=1e6)
    short.kind = EventKind.LIQUIDATION
    short.side = Side.SHORT
    bus.publish(short)
    s = bus.stats()
    assert s["total"] == 2
    assert s["notional_total_usd"] == 6e6
    assert s["liquidation_count"] == 1
    assert s["long_count"] == 1
    assert s["short_count"] == 1


async def _exercise_subscribe_fanout():
    bus = EventBus()
    q1: asyncio.Queue = asyncio.Queue(maxsize=10)
    q2: asyncio.Queue = asyncio.Queue(maxsize=10)
    bus.subscribe(q1)
    bus.subscribe(q2)
    bus.publish(_sample_event())
    p1 = q1.get_nowait()
    p2 = q2.get_nowait()
    assert p1["venue"] == "Hyperliquid"
    assert p2 == p1


def test_subscribe_fanout():
    asyncio.run(_exercise_subscribe_fanout())


async def _exercise_full_http():
    """Boot the actual aiohttp server, hit every endpoint, parse SSE."""
    bus = EventBus()
    bus.publish(_sample_event(notional=5e6, with_stats=True))

    cleanup = await webserver.serve(
        bus, host="127.0.0.1", port=0,
        threshold_usd=5_000_000, venues=["Hyperliquid"],
    )
    # web.TCPSite with port=0 picks an ephemeral port — discover it.
    # Re-create with a specific port to keep the test reproducible.
    await cleanup()

    port = 18790
    cleanup = await webserver.serve(
        bus, host="127.0.0.1", port=port,
        threshold_usd=5_000_000, venues=["Hyperliquid", "JupiterPerps"],
    )
    base = f"http://127.0.0.1:{port}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/api/config") as r:
                cfg = await r.json()
                assert cfg["threshold_usd"] == 5_000_000
                assert "JupiterPerps" in cfg["venues"]

            async with s.get(base + "/api/recent") as r:
                recent = await r.json()
                assert len(recent) == 1
                assert recent[0]["track_record"]["tier"] == "ELITE"

            async with s.get(base + "/api/stats") as r:
                stats = await r.json()
                assert stats["total"] == 1
                assert stats["notional_total_usd"] == 5e6

            async with s.get(base + "/") as r:
                assert r.status == 200
                body = await r.text()
                assert "Solana Perps" in body
                assert "/events" in body  # script wires SSE

            # SSE: connect, then publish and verify we receive it.
            async with s.get(base + "/events") as r:
                assert r.status == 200
                # Read until we have one full message (the replayed event).
                buf = b""
                first_seen = None
                while first_seen is None:
                    chunk = await r.content.read(1024)
                    if not chunk:
                        break
                    buf += chunk
                    if b"\n\n" in buf:
                        for line in buf.split(b"\n\n"):
                            if line.startswith(b"data:"):
                                first_seen = json.loads(line[5:].decode().strip())
                                break
                        buf = b""
                assert first_seen is not None
                assert first_seen["venue"] == "Hyperliquid"

                # Publish something new and read the next message.
                bus.publish(_sample_event(notional=7e6))
                buf = b""
                second = None
                while second is None:
                    chunk = await asyncio.wait_for(r.content.read(1024), timeout=5.0)
                    if not chunk:
                        break
                    buf += chunk
                    if b"\n\n" in buf:
                        for line in buf.split(b"\n\n"):
                            if line.startswith(b"data:"):
                                second = json.loads(line[5:].decode().strip())
                                break
                        buf = b""
                assert second["notional_usd"] == 7e6
    finally:
        await cleanup()


def test_full_http_and_sse():
    asyncio.run(_exercise_full_http())


async def _exercise_leaderboard_and_wallet():
    """Verify the /api/leaderboard ranking and /api/wallet/<…> handler.

    The /api/wallet handler delegates to TrackRecord; we stub its `.get` so
    no real network calls happen.
    """
    from trackrecord import TrackRecord, _empty_stats, ClosedTradeRecord, CoinPnL
    bus = EventBus()

    # Two whales: one ELITE high-score, one POOR low-score.
    elite_stats = _empty_stats("Hyperliquid", "0xelite", None)
    elite_stats.closed_trades = 200
    elite_stats.win_count = 140
    elite_stats.realized_pnl_usd = 8_000_000
    elite_stats.gross_wins_usd = 12_000_000
    elite_stats.gross_losses_usd = -4_000_000

    poor_stats = _empty_stats("Hyperliquid", "0xpoor", None)
    poor_stats.closed_trades = 100
    poor_stats.win_count = 25
    poor_stats.realized_pnl_usd = -1_000_000
    poor_stats.gross_wins_usd = 200_000
    poor_stats.gross_losses_usd = -1_200_000

    elite_ev = _sample_event()
    elite_ev.trader = "0xelite"
    elite_ev.tx_ref = "tide"
    elite_ev.extra["track_record"] = elite_stats
    bus.publish(elite_ev)

    poor_ev = _sample_event()
    poor_ev.trader = "0xpoor"
    poor_ev.tx_ref = "tidp"
    poor_ev.extra["track_record"] = poor_stats
    bus.publish(poor_ev)

    # And one event for the elite again to bump events_seen.
    elite_ev_2 = _sample_event(notional=12e6)
    elite_ev_2.trader = "0xelite"
    elite_ev_2.tx_ref = "tide2"
    elite_ev_2.extra["track_record"] = elite_stats
    bus.publish(elite_ev_2)

    # Stubbed TrackRecord — returns the canned stats with recent trades attached.
    class FakeTR:
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return None
        async def get(self, venue, wallet, *, timeout=5.0):
            if wallet == "0xelite":
                s = elite_stats
                s.recent_trades = [
                    ClosedTradeRecord(
                        ts_ms=1700000000000, coin="BTC", side="long",
                        size=0.5, price_usd=60000.0, pnl_usd=2500.0, fee_usd=20.0,
                    ),
                    ClosedTradeRecord(
                        ts_ms=1700000060000, coin="ETH", side="short",
                        size=10.0, price_usd=3000.0, pnl_usd=-500.0, fee_usd=15.0,
                    ),
                ]
                s.by_coin = {
                    "BTC": CoinPnL(coin="BTC", trades=120, win_count=80, pnl_usd=6_000_000),
                    "ETH": CoinPnL(coin="ETH", trades=80, win_count=60, pnl_usd=2_000_000),
                }
                return s
            if wallet == "0xpoor":
                return poor_stats
            return None

    fake_tr = FakeTR()
    port = 18791
    cleanup = await webserver.serve(
        bus, host="127.0.0.1", port=port,
        threshold_usd=1e6, venues=["Hyperliquid"], track_record=fake_tr,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/api/leaderboard?limit=10") as r:
                lb = await r.json()
                assert len(lb) == 2
                # Sorted by score desc — elite first.
                assert lb[0]["trader"] == "0xelite"
                assert lb[0]["events_seen"] == 2
                assert lb[0]["score"] > lb[1]["score"]
                assert lb[1]["trader"] == "0xpoor"

            async with s.get(base + "/api/wallet/Hyperliquid/0xelite") as r:
                assert r.status == 200
                data = await r.json()
                assert data["tier"] == "ELITE"
                assert data["score"] is not None and data["score"] > 70
                # full history fields are present in the modal payload
                assert "recent_trades" in data
                assert "by_coin" in data
                assert len(data["recent_trades"]) == 2
                assert "BTC" in data["by_coin"]
                assert data["by_coin"]["BTC"]["pnl_usd"] == 6_000_000

            # Non-existent wallet → 504 from the handler.
            async with s.get(base + "/api/wallet/Hyperliquid/0xunknown") as r:
                assert r.status == 504
    finally:
        await cleanup()


def test_leaderboard_and_wallet_endpoints():
    asyncio.run(_exercise_leaderboard_and_wallet())


async def _exercise_analytics_endpoints():
    """Boot the server with a populated bus and verify /api/signals,
    /api/coins, /api/coin/<coin>, /api/positions return the expected shapes
    and reflect the underlying analytics functions.
    """
    bus = EventBus()

    # Three ELITE longs on SOL within minutes → should generate a consensus signal.
    for i, addr in enumerate(["0xa", "0xb", "0xc"]):
        s = _empty_stats("Hyperliquid", addr, None)
        s.closed_trades = 200
        s.win_count = 140
        s.realized_pnl_usd = 8_000_000
        s.gross_wins_usd = 12_000_000
        s.gross_losses_usd = -4_000_000
        ev = WhaleEvent(
            venue="Hyperliquid", kind=EventKind.OPEN, coin="SOL", side=Side.LONG,
            notional_usd=2_000_000 + i * 1_000_000, price_usd=200.0,
            size=10000.0, trader=addr, tx_ref=f"tid-sol-{addr}",
            leverage=10.0, extra={"track_record": s},
        )
        bus.publish(ev)

    # And one CLOSE for the first wallet to test smart-positions clearing.
    close_stats = _empty_stats("Hyperliquid", "0xa", None)
    close_stats.closed_trades = 200
    close_stats.win_count = 140
    close_stats.realized_pnl_usd = 8_000_000
    close_stats.gross_wins_usd = 12_000_000
    close_stats.gross_losses_usd = -4_000_000
    close_ev = WhaleEvent(
        venue="Hyperliquid", kind=EventKind.CLOSE, coin="SOL", side=Side.LONG,
        notional_usd=2_000_000, price_usd=205.0, size=9756.0, trader="0xa",
        tx_ref="tid-sol-close-a", leverage=10.0, extra={"track_record": close_stats},
    )
    bus.publish(close_ev)

    port = 18792
    cleanup = await webserver.serve(
        bus, host="127.0.0.1", port=port,
        threshold_usd=1e6, venues=["Hyperliquid"],
    )
    base = f"http://127.0.0.1:{port}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/api/signals") as r:
                payload = await r.json()
                assert "consensus" in payload and "coins" in payload
                # 0xa was closed but 0xb and 0xc still have OPEN longs on SOL.
                consensus = payload["consensus"]
                # Consensus also looks at OPENs only — all 3 still count.
                assert any(s["coin"] == "SOL" and s["side"] == "long" for s in consensus)

            async with session.get(base + "/api/coins") as r:
                coins = (await r.json())["coins"]
                assert any(c["coin"] == "SOL" for c in coins)

            async with session.get(base + "/api/coin/SOL") as r:
                cd = await r.json()
                assert cd["coin"] == "SOL"
                assert cd["summary"] is not None
                assert cd["flow_series"] and isinstance(cd["flow_series"], list)
                # Top traders should include all three addresses.
                top = {t["trader"] for t in cd["top_traders"]}
                assert {"0xa", "0xb", "0xc"}.issubset(top)
                assert cd["recent_events"]

            async with session.get(base + "/api/positions?min_score=60") as r:
                pos = await r.json()
                # 0xa was closed; 0xb and 0xc remain.
                traders = {row["trader"] for row in pos["rows"]}
                assert traders == {"0xb", "0xc"}
                for row in pos["rows"]:
                    assert row["coin"] == "SOL" and row["side"] == "long"
    finally:
        await cleanup()


def test_analytics_endpoints():
    asyncio.run(_exercise_analytics_endpoints())


if __name__ == "__main__":
    test_serialize_event_basic()
    test_serialize_event_with_stats()
    test_bus_publish_and_recent()
    test_bus_stats_aggregation()
    test_subscribe_fanout()
    print("event bus + serialization OK")
    test_full_http_and_sse()
    print("HTTP + SSE round-trip OK")
    test_leaderboard_and_wallet_endpoints()
    print("leaderboard + /api/wallet OK")
    test_analytics_endpoints()
    print("/api/signals + /api/coin + /api/positions OK")
