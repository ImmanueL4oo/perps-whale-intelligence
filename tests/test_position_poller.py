"""Tests for position_poller.py.

We never hit a real Hyperliquid endpoint — `fetch_hl_snapshot` is exercised
indirectly through `diff_snapshots` with hand-crafted snapshots, and the
working-set logic is tested by stuffing the EventBus with synthetic events.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_bus import EventBus
from position_poller import (
    Position,
    PositionPoller,
    WalletSnapshot,
    diff_snapshots,
)


# ── helpers ───────────────────────────────────────────────────────────────────
def _pos(coin: str, side: str, size: float, notional: float, *, mark: float = 100.0) -> Position:
    return Position(
        venue="Hyperliquid", wallet="0xabc", coin=coin, side=side,
        size=size, entry_px=mark, mark_px=mark,
        notional_usd=notional, unrealized_pnl_usd=0.0,
        leverage=10.0, liq_px=None, margin_used_usd=notional / 10,
        fetched_at_ms=int(time.time() * 1000),
    )


def _snap(positions: list[Position]) -> WalletSnapshot:
    s = WalletSnapshot(
        venue="Hyperliquid", wallet="0xabc",
        fetched_at_ms=int(time.time() * 1000),
        account_value_usd=10_000.0, margin_used_usd=1_000.0,
    )
    for p in positions:
        s.positions[p.coin] = p
    return s


def _bus_event(*, ts_ms: int, trader: str, tier: str = "TRUSTED",
               score: float = 70, venue: str = "Hyperliquid") -> dict:
    return {
        "ts_ms": ts_ms,
        "venue": venue,
        "kind": "OPEN",
        "side": "long",
        "coin": "BTC",
        "trader": trader,
        "tx_ref": f"tx-{trader}-{ts_ms}",
        "notional_usd": 500_000,
        "size": 5.0,
        "price_usd": 100_000.0,
        "leverage": 10.0,
        "track_record": {"score": score, "tier": tier, "actor": "HUMAN"},
    }


# ── diff_snapshots ────────────────────────────────────────────────────────────
def test_diff_detects_brand_new_open():
    cur = _snap([_pos("BTC", "long", 1.0, 70_000)])
    changes = diff_snapshots(None, cur)
    assert len(changes) == 1
    c = changes[0]
    assert c.kind == "open"
    assert c.coin == "BTC"
    assert c.side_now == "long"
    assert c.delta_notional_usd == 70_000


def test_diff_detects_close():
    prev = _snap([_pos("BTC", "long", 1.0, 70_000)])
    cur = _snap([])
    changes = diff_snapshots(prev, cur)
    assert len(changes) == 1
    c = changes[0]
    assert c.kind == "close"
    assert c.side_prev == "long"
    assert c.delta_notional_usd == -70_000


def test_diff_detects_increase():
    prev = _snap([_pos("BTC", "long", 1.0, 70_000)])
    cur = _snap([_pos("BTC", "long", 1.5, 105_000)])
    changes = diff_snapshots(prev, cur)
    assert len(changes) == 1
    c = changes[0]
    assert c.kind == "increase"
    assert c.delta_notional_usd == 35_000


def test_diff_detects_decrease():
    prev = _snap([_pos("BTC", "long", 2.0, 140_000)])
    cur = _snap([_pos("BTC", "long", 1.0, 70_000)])
    changes = diff_snapshots(prev, cur)
    assert len(changes) == 1
    c = changes[0]
    assert c.kind == "decrease"
    assert c.delta_notional_usd == -70_000


def test_diff_detects_flip():
    prev = _snap([_pos("BTC", "long", 2.0, 140_000)])
    cur = _snap([_pos("BTC", "short", 1.0, 70_000)])
    changes = diff_snapshots(prev, cur)
    assert len(changes) == 1
    c = changes[0]
    assert c.kind == "flip"
    assert c.side_prev == "long"
    assert c.side_now == "short"


def test_diff_ignores_tiny_size_changes():
    prev = _snap([_pos("BTC", "long", 1.0, 70_000)])
    # $500 jitter — under the default $1000 threshold.
    cur = _snap([_pos("BTC", "long", 1.007, 70_500)])
    changes = diff_snapshots(prev, cur)
    assert changes == [], f"expected no changes for $500 jitter, got {changes}"


def test_diff_handles_multiple_coins():
    prev = _snap([
        _pos("BTC", "long", 1.0, 70_000),
        _pos("ETH", "long", 10.0, 30_000),
    ])
    cur = _snap([
        _pos("BTC", "long", 1.5, 105_000),  # increase
        # ETH closed
        _pos("SOL", "long", 100.0, 20_000),  # new open
    ])
    changes = diff_snapshots(prev, cur)
    by_coin = {c.coin: c.kind for c in changes}
    assert by_coin == {"BTC": "increase", "ETH": "close", "SOL": "open"}


# ── PositionPoller.working set ────────────────────────────────────────────────
def test_working_set_includes_watchlist_first():
    bus = EventBus(recent_size=100)
    now = int(time.time() * 1000)
    # Add a non-watchlist trusted trader — watchlist should still come ahead.
    bus._recent.append(_bus_event(ts_ms=now, trader="0xpro", tier="ELITE", score=99))
    poller = PositionPoller(
        bus, watchlist=["0xWATCH1", "0xWATCH2"],
        max_wallets=5,
    )
    addrs = poller._build_working_set()
    assert set(addrs[:2]) == {"0xwatch1", "0xwatch2"}, addrs
    assert "0xpro" in addrs


def test_working_set_picks_up_trusted_event_traders():
    bus = EventBus(recent_size=100)
    now = int(time.time() * 1000)
    bus._recent.append(_bus_event(ts_ms=now, trader="0xpro1", tier="TRUSTED", score=80))
    bus._recent.append(_bus_event(ts_ms=now, trader="0xpro2", tier="ELITE", score=92))
    bus._recent.append(_bus_event(ts_ms=now, trader="0xnoob", tier="POOR", score=10))
    bus._recent.append(_bus_event(ts_ms=now, trader="0xnewbie", tier="NEW", score=0))

    poller = PositionPoller(bus, max_wallets=10)
    addrs = poller._build_working_set()
    assert "0xpro1" in addrs
    assert "0xpro2" in addrs
    assert "0xnoob" not in addrs
    assert "0xnewbie" not in addrs


def test_working_set_dedupes_watchlist_and_events():
    bus = EventBus(recent_size=100)
    now = int(time.time() * 1000)
    # Same address appears in both watchlist and as a TRUSTED event trader.
    bus._recent.append(_bus_event(ts_ms=now, trader="0xboth", tier="ELITE", score=95))
    poller = PositionPoller(
        bus, watchlist=["0xboth"], max_wallets=5,
    )
    addrs = poller._build_working_set()
    assert addrs.count("0xboth") == 1


def test_working_set_caps_at_max_wallets():
    bus = EventBus(recent_size=200)
    now = int(time.time() * 1000)
    for i in range(50):
        bus._recent.append(_bus_event(
            ts_ms=now, trader=f"0xpro{i:02d}", tier="TRUSTED", score=70 + i * 0.1,
        ))
    poller = PositionPoller(bus, max_wallets=10)
    addrs = poller._build_working_set()
    assert len(addrs) == 10


def test_working_set_skips_jupiter_events():
    """HL-only polling: Jupiter Perps wallets don't make the cut."""
    bus = EventBus(recent_size=100)
    now = int(time.time() * 1000)
    bus._recent.append(_bus_event(
        ts_ms=now, trader="JupABC", tier="ELITE", score=90, venue="JupiterPerps",
    ))
    bus._recent.append(_bus_event(
        ts_ms=now, trader="0xhl", tier="ELITE", score=90, venue="Hyperliquid",
    ))
    poller = PositionPoller(bus, max_wallets=10)
    addrs = poller._build_working_set()
    assert "0xhl" in addrs
    assert "JupABC" not in addrs and "jupabc" not in addrs


def test_working_set_drops_stale_events():
    bus = EventBus(recent_size=100)
    now = int(time.time() * 1000)
    # 2-day-old event — outside the default 24h recent_event_window_ms.
    bus._recent.append(_bus_event(
        ts_ms=now - 2 * 24 * 60 * 60_000, trader="0xstale", tier="ELITE", score=95,
    ))
    poller = PositionPoller(bus, max_wallets=10)
    addrs = poller._build_working_set()
    assert "0xstale" not in addrs


# ── Run ───────────────────────────────────────────────────────────────────────
test_diff_detects_brand_new_open()
test_diff_detects_close()
test_diff_detects_increase()
test_diff_detects_decrease()
test_diff_detects_flip()
test_diff_ignores_tiny_size_changes()
test_diff_handles_multiple_coins()
test_working_set_includes_watchlist_first()
test_working_set_picks_up_trusted_event_traders()
test_working_set_dedupes_watchlist_and_events()
test_working_set_caps_at_max_wallets()
test_working_set_skips_jupiter_events()
test_working_set_drops_stale_events()
print("position-poller tests OK (13)")
