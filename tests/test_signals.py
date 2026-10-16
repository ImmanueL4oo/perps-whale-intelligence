"""Tests for the trading-signal helpers (signals.py) and the smart-money
positioning analytic.

We don't actually hit Telegram — we substitute a stub coroutine that captures
each push so we can assert on count/content/order/cooldown.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analytics
from event_bus import EventBus
from signals import ConvictionWatcher, LifecycleTracker


# ── helpers ───────────────────────────────────────────────────────────────────
def _ev(
    *, ts_ms: int, venue: str = "Hyperliquid", trader: str, coin: str,
    side: str, kind: str = "OPEN", notional: float = 500_000,
    score: float = 70, tier: str = "TRUSTED", actor: str = "HUMAN",
    pnl: float | None = None, leverage: float | None = 10.0,
    price: float = 100.0,
) -> dict:
    return {
        "ts_ms": ts_ms,
        "venue": venue,
        "kind": kind,
        "side": side,
        "coin": coin,
        "trader": trader,
        "tx_ref": f"tx-{trader}-{ts_ms}",
        "notional_usd": notional,
        "size": notional / price,
        "price_usd": price,
        "leverage": leverage,
        "pnl_usd": pnl,
        "track_record": {
            "score": score, "tier": tier, "actor": actor, "tier_emoji": "✅",
        },
    }


def _push_collector():
    pushes: list[str] = []

    async def push(text: str):
        pushes.append(text)

    return push, pushes


# ── ConvictionWatcher ─────────────────────────────────────────────────────────
def test_conviction_fires_when_three_traders_agree():
    bus = EventBus(recent_size=100)
    push, pushes = _push_collector()
    watcher = ConvictionWatcher(
        bus, push, min_traders=3,
        window_ms=30 * 60_000, cooldown_ms=30 * 60_000,
    )

    base = int(time.time() * 1000)
    asyncio.run(_run_three_opens(watcher, bus, base))

    assert len(pushes) == 1, f"expected 1 conviction push, got {len(pushes)}"
    msg = pushes[0]
    assert "CONVICTION SIGNAL" in msg
    assert "BTC" in msg
    assert "LONG" in msg
    assert "3</b> trusted traders" in msg


async def _run_three_opens(watcher: ConvictionWatcher, bus: EventBus, base: int):
    for i, addr in enumerate(["0xa", "0xb", "0xc"]):
        ev = _ev(ts_ms=base + i * 1000, trader=addr, coin="BTC", side="long")
        bus._recent.append(ev)
        await watcher.check(ev)


def test_conviction_skips_below_threshold_count():
    bus = EventBus(recent_size=100)
    push, pushes = _push_collector()
    watcher = ConvictionWatcher(bus, push, min_traders=3)

    base = int(time.time() * 1000)
    asyncio.run(_run_two_opens(watcher, bus, base))
    assert pushes == []


async def _run_two_opens(watcher: ConvictionWatcher, bus: EventBus, base: int):
    for i, addr in enumerate(["0xa", "0xb"]):
        ev = _ev(ts_ms=base + i * 1000, trader=addr, coin="BTC", side="long")
        bus._recent.append(ev)
        await watcher.check(ev)


def test_conviction_respects_cooldown():
    bus = EventBus(recent_size=100)
    push, pushes = _push_collector()
    watcher = ConvictionWatcher(
        bus, push, min_traders=3,
        window_ms=30 * 60_000, cooldown_ms=30 * 60_000,
    )

    base = int(time.time() * 1000)
    asyncio.run(_cooldown_sequence(watcher, bus, base))

    # 1st burst fires once. 4th trader, still inside cooldown → NO new push.
    assert len(pushes) == 1, f"expected 1 push (cooldown), got {len(pushes)}"


async def _cooldown_sequence(watcher: ConvictionWatcher, bus: EventBus, base: int):
    for i, addr in enumerate(["0xa", "0xb", "0xc"]):
        ev = _ev(ts_ms=base + i * 1000, trader=addr, coin="BTC", side="long")
        bus._recent.append(ev)
        await watcher.check(ev)
    # Same coin, same side, just 5 minutes later → cooldown active.
    ev = _ev(ts_ms=base + 5 * 60_000, trader="0xd", coin="BTC", side="long")
    bus._recent.append(ev)
    await watcher.check(ev)


def test_conviction_low_tier_traders_ignored():
    bus = EventBus(recent_size=100)
    push, pushes = _push_collector()
    watcher = ConvictionWatcher(bus, push, min_traders=3)

    base = int(time.time() * 1000)

    async def run():
        # Three POOR-tier traders should NOT trigger conviction (filter).
        for i, addr in enumerate(["0xa", "0xb", "0xc"]):
            ev = _ev(
                ts_ms=base + i * 1000, trader=addr, coin="BTC", side="long",
                tier="POOR", score=15,
            )
            bus._recent.append(ev)
            await watcher.check(ev)

    asyncio.run(run())
    assert pushes == []


# ── LifecycleTracker ──────────────────────────────────────────────────────────
def test_lifecycle_pairs_open_with_close():
    push, pushes = _push_collector()
    tracker = LifecycleTracker(push)

    base = int(time.time() * 1000)
    open_ev = _ev(
        ts_ms=base, trader="0xabc1234567890def", coin="BTC", side="long",
        notional=500_000, price=100.0,
    )
    close_ev = _ev(
        ts_ms=base + 60 * 60_000, trader="0xabc1234567890def", coin="BTC",
        side="long", kind="CLOSE", notional=520_000, price=104.0, pnl=20_000,
    )

    asyncio.run(_run_lifecycle(tracker, [open_ev, close_ev]))
    assert len(pushes) == 1
    msg = pushes[0]
    assert "EXIT" in msg
    assert "BTC" in msg
    assert "LONG" in msg
    assert "60 min" in msg
    assert "$20.0K" in msg


async def _run_lifecycle(tracker: LifecycleTracker, events: list[dict]):
    for ev in events:
        await tracker.check(ev)


def test_lifecycle_ignores_low_tier_wallets():
    push, pushes = _push_collector()
    tracker = LifecycleTracker(push)
    base = int(time.time() * 1000)
    open_ev = _ev(
        ts_ms=base, trader="0xpoor", coin="BTC", side="long",
        tier="POOR", score=15,
    )
    close_ev = _ev(
        ts_ms=base + 60_000, trader="0xpoor", coin="BTC", side="long",
        kind="CLOSE", tier="POOR", score=15, pnl=-5_000,
    )
    asyncio.run(_run_lifecycle(tracker, [open_ev, close_ev]))
    assert pushes == []


def test_lifecycle_ignores_orphan_close():
    push, pushes = _push_collector()
    tracker = LifecycleTracker(push)
    base = int(time.time() * 1000)
    # Close for a position we never saw open — no push.
    close_ev = _ev(
        ts_ms=base, trader="0xabc", coin="BTC", side="long", kind="CLOSE",
        pnl=10_000,
    )
    asyncio.run(_run_lifecycle(tracker, [close_ev]))
    assert pushes == []


def test_lifecycle_warm_up_replays_history():
    push, pushes = _push_collector()
    tracker = LifecycleTracker(push)
    base = int(time.time() * 1000)
    # Replay an OPEN from history (no push), then a fresh CLOSE → push.
    open_ev = _ev(ts_ms=base, trader="0xabc", coin="ETH", side="long")
    tracker.warm_up([open_ev])
    assert pushes == []
    close_ev = _ev(
        ts_ms=base + 30 * 60_000, trader="0xabc", coin="ETH", side="long",
        kind="CLOSE", pnl=15_000,
    )
    asyncio.run(_run_lifecycle(tracker, [close_ev]))
    assert len(pushes) == 1
    assert "EXIT" in pushes[0]


# ── smart_money_positioning ───────────────────────────────────────────────────
def test_smart_money_returns_net_long_bias():
    base = int(time.time() * 1000)
    events = [
        _ev(ts_ms=base, trader="0xa", coin="BTC", side="long", notional=500_000),
        _ev(ts_ms=base + 1, trader="0xb", coin="BTC", side="long", notional=300_000),
        _ev(ts_ms=base + 2, trader="0xc", coin="BTC", side="short", notional=100_000),
    ]
    rows = analytics.smart_money_positioning(
        events, window_ms=60 * 60_000, min_traders=2, now_ms=base + 1000,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["coin"] == "BTC"
    assert r["label"] == "LONG"
    assert r["bias"] > 0.5  # 800K long vs 100K short
    assert r["long_traders"] == 2
    assert r["short_traders"] == 1


def test_smart_money_drops_low_tier_events():
    base = int(time.time() * 1000)
    events = [
        _ev(ts_ms=base, trader="0xa", coin="SOL", side="long", tier="POOR", score=15),
        _ev(ts_ms=base + 1, trader="0xb", coin="SOL", side="long", tier="POOR", score=15),
    ]
    rows = analytics.smart_money_positioning(events, now_ms=base + 1000)
    assert rows == []


def test_smart_money_filters_below_min_traders():
    base = int(time.time() * 1000)
    events = [
        _ev(ts_ms=base, trader="0xa", coin="DOGE", side="long"),
    ]
    rows = analytics.smart_money_positioning(
        events, min_traders=2, now_ms=base + 1000,
    )
    assert rows == []


def test_smart_money_marks_mixed_when_balanced():
    base = int(time.time() * 1000)
    events = [
        _ev(ts_ms=base, trader="0xa", coin="ETH", side="long", notional=500_000),
        _ev(ts_ms=base + 1, trader="0xb", coin="ETH", side="short", notional=500_000),
    ]
    rows = analytics.smart_money_positioning(events, now_ms=base + 1000)
    assert len(rows) == 1
    r = rows[0]
    assert r["label"] == "MIXED"
    assert abs(r["bias"]) < 0.15


test_conviction_fires_when_three_traders_agree()
test_conviction_skips_below_threshold_count()
test_conviction_respects_cooldown()
test_conviction_low_tier_traders_ignored()
test_lifecycle_pairs_open_with_close()
test_lifecycle_ignores_low_tier_wallets()
test_lifecycle_ignores_orphan_close()
test_lifecycle_warm_up_replays_history()
test_smart_money_returns_net_long_bias()
test_smart_money_drops_low_tier_events()
test_smart_money_filters_below_min_traders()
test_smart_money_marks_mixed_when_balanced()
print("trading-signal tests OK (12)")
