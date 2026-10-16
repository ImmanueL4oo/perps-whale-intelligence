"""Pure-function tests for the wallet track record service."""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifier import Notifier
from trackers import EventKind, Side, WhaleEvent
from trackrecord import (
    TIER_ELITE,
    TIER_MIXED,
    TIER_NEW,
    TIER_POOR,
    TIER_TRUSTED,
    ClosedTradeRecord,
    CoinPnL,
    TrackRecord,
    WalletStats,
    _empty_stats,
    _hl_side_from_dir,
    _stats_from_hl_fills,
)


def _hl_fill(
    closed_pnl: float, fee: float = 0.0, t_ms: int | None = None,
    coin: str = "BTC", direction: str = "Close Long",
) -> dict:
    return {
        "closedPnl": str(closed_pnl),
        "fee": str(fee),
        "time": t_ms if t_ms is not None else int(time.time() * 1000),
        "coin": coin, "side": "B", "px": "60000", "sz": "0.1",
        "dir": direction,
    }


def test_empty_fills():
    s = _stats_from_hl_fills("Hyperliquid", "0xabc", [])
    assert s.closed_trades == 0
    assert s.realized_pnl_usd == 0.0
    assert s.tier() == TIER_NEW


def test_hl_pnl_aggregation():
    fills = [
        _hl_fill(100.0, fee=1.0),
        _hl_fill(-50.0, fee=1.0),
        _hl_fill(200.0, fee=1.0),
        _hl_fill(0.0, fee=1.0),  # opening fill (no realized pnl)
    ]
    s = _stats_from_hl_fills("Hyperliquid", "0xabc", fills)
    assert s.closed_trades == 3        # zero-pnl fill excluded
    assert s.win_count == 2
    assert s.realized_pnl_usd == 250.0
    assert s.fee_paid_usd == 4.0
    assert s.largest_win_usd == 200.0
    assert s.largest_loss_usd == -50.0
    assert abs((s.winrate or 0) - 2/3) < 1e-9
    # New extended fields:
    assert s.gross_wins_usd == 300.0
    assert s.gross_losses_usd == -50.0
    assert abs(s.profit_factor - 6.0) < 1e-9
    assert abs(s.avg_win_usd - 150.0) < 1e-9
    assert s.avg_loss_usd == -50.0
    assert len(s.recent_trades) == 3       # opening fill excluded from records
    assert all(isinstance(t, ClosedTradeRecord) for t in s.recent_trades)
    # by_coin should aggregate per asset.
    assert "BTC" in s.by_coin
    btc = s.by_coin["BTC"]
    assert btc.trades == 3 and btc.win_count == 2 and btc.pnl_usd == 250.0


def test_hl_by_coin_split():
    fills = [
        _hl_fill(100.0, coin="BTC"),
        _hl_fill(50.0, coin="ETH"),
        _hl_fill(-30.0, coin="ETH"),
        _hl_fill(0.0, coin="SOL"),  # opening — ignored
    ]
    s = _stats_from_hl_fills("Hyperliquid", "0xabc", fills)
    assert sorted(s.by_coin.keys()) == ["BTC", "ETH"]
    assert s.by_coin["BTC"].pnl_usd == 100.0
    assert s.by_coin["ETH"].pnl_usd == 20.0
    assert s.by_coin["ETH"].trades == 2 and s.by_coin["ETH"].win_count == 1


def test_hl_side_from_dir():
    assert _hl_side_from_dir("Close Long") == "long"
    assert _hl_side_from_dir("Close Short") == "short"
    assert _hl_side_from_dir("Open Long") == "long"
    assert _hl_side_from_dir("Open Short") == "short"
    # On a flip "Long > Short", the *closed* position was the long.
    assert _hl_side_from_dir("Long > Short") == "long"
    assert _hl_side_from_dir(None) == "long"


def test_recent_trades_sorted_newest_first():
    now_ms = int(time.time() * 1000)
    fills = [
        _hl_fill(10.0, t_ms=now_ms - 3000),
        _hl_fill(20.0, t_ms=now_ms - 1000),
        _hl_fill(15.0, t_ms=now_ms - 2000),
    ]
    s = _stats_from_hl_fills("Hyperliquid", "0xabc", fills)
    assert [t.pnl_usd for t in s.recent_trades] == [20.0, 15.0, 10.0]


def test_30d_window():
    now_ms = int(time.time() * 1000)
    older = now_ms - 60 * 86_400_000
    recent = now_ms - 5 * 86_400_000
    fills = [
        _hl_fill(1000.0, t_ms=older),
        _hl_fill(-200.0, t_ms=recent),
        _hl_fill(500.0, t_ms=recent),
    ]
    s = _stats_from_hl_fills("Hyperliquid", "0xabc", fills)
    assert s.realized_pnl_usd == 1300.0
    assert s.pnl_30d_usd == 300.0   # only the two recent ones


def test_tier_elite():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 50
    s.win_count = 35   # 70% wr
    s.realized_pnl_usd = 5_000_000
    assert s.tier() == TIER_ELITE


def test_tier_trusted():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 15
    s.win_count = 9    # 60% wr
    s.realized_pnl_usd = 250_000
    assert s.tier() == TIER_TRUSTED


def test_tier_poor_negative_pnl():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 30
    s.win_count = 18  # 60% wr but heavy losses
    s.realized_pnl_usd = -500_000
    assert s.tier() == TIER_POOR


def test_tier_poor_low_winrate():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 30
    s.win_count = 9   # 30% wr
    s.realized_pnl_usd = 100_000
    assert s.tier() == TIER_POOR


def test_tier_mixed_default():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 12
    s.win_count = 6    # 50% wr
    s.realized_pnl_usd = 5_000
    assert s.tier() == TIER_MIXED


def test_tier_new_low_sample():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 3
    s.win_count = 3
    s.realized_pnl_usd = 50_000
    assert s.tier() == TIER_NEW  # not enough samples to trust


def test_score_below_min_samples_is_none():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 3
    s.win_count = 3
    s.realized_pnl_usd = 100_000
    assert s.score is None


def test_score_elite_high():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 200
    s.win_count = 140
    s.realized_pnl_usd = 8_000_000
    s.gross_wins_usd = 12_000_000
    s.gross_losses_usd = -4_000_000
    score = s.score
    assert score is not None and score > 80


def test_score_poor_trader():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 100
    s.win_count = 25
    s.realized_pnl_usd = -2_000_000
    s.gross_wins_usd = 500_000
    s.gross_losses_usd = -2_500_000
    score = s.score
    assert score is not None and score < 20


def test_score_breakeven_mid():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.closed_trades = 50
    s.win_count = 28
    s.realized_pnl_usd = 5_000
    s.gross_wins_usd = 50_000
    s.gross_losses_usd = -45_000
    score = s.score
    assert score is not None and 5 < score < 60


def test_profit_factor_nones_and_values():
    s = _empty_stats("Hyperliquid", "0xabc", None)
    s.gross_wins_usd = 1000.0
    s.gross_losses_usd = 0.0
    assert s.profit_factor is None        # no losses → undefined
    s.gross_losses_usd = -250.0
    assert s.profit_factor == 4.0         # 1000 / 250


async def _exercise_cache_and_singleflight():
    """Patch the fetcher and check we hit it once for concurrent same-wallet calls."""
    tr = TrackRecord()
    calls = {"n": 0}
    expected = _empty_stats("Hyperliquid", "0xabc", None)
    expected.closed_trades = 10
    expected.win_count = 6
    expected.realized_pnl_usd = 100_000

    async def fake(venue, wallet):
        calls["n"] += 1
        await asyncio.sleep(0.05)
        return expected

    tr._fetch = fake  # type: ignore[method-assign]

    a, b, c = await asyncio.gather(
        tr.get("Hyperliquid", "0xabc"),
        tr.get("Hyperliquid", "0xabc"),
        tr.get("Hyperliquid", "0xabc"),
    )
    assert a is expected and b is expected and c is expected
    assert calls["n"] == 1   # single-flight collapsed concurrent calls

    # And the second wave should hit the cache (no new fetch)
    d = await tr.get("Hyperliquid", "0xabc")
    assert d is expected
    assert calls["n"] == 1


def test_cache_and_singleflight():
    asyncio.run(_exercise_cache_and_singleflight())


async def _exercise_notifier_with_stats():
    async with Notifier() as n:
        n.banner(5_000_000, ["Hyperliquid"])
        elite = _empty_stats("Hyperliquid", "0xabc123def4567890abc123def4567890abc12345", None)
        elite.closed_trades = 80
        elite.win_count = 56
        elite.realized_pnl_usd = 12_300_000
        elite.pnl_30d_usd = 2_400_000
        elite.largest_win_usd = 850_000
        elite.largest_loss_usd = -200_000
        elite.first_trade_ts_ms = int(time.time() * 1000) - 220 * 86_400_000

        await n.emit(WhaleEvent(
            venue="Hyperliquid",
            kind=EventKind.OPEN,
            coin="BTC",
            side=Side.LONG,
            notional_usd=8_000_000,
            price_usd=60_000.0,
            size=133.33,
            trader="0xabc123def4567890abc123def4567890abc12345",
            tx_ref="t1",
            leverage=10.0,
            extra={"track_record": elite},
        ))

        poor = _empty_stats("JupiterPerps", "JupOwner1111111111111111111111111111111111", None)
        poor.closed_trades = 22
        poor.win_count = 6
        poor.realized_pnl_usd = -1_500_000
        poor.coverage_note = "(scanned 50/100 recent sigs)"
        await n.emit(WhaleEvent(
            venue="JupiterPerps",
            kind=EventKind.LIQUIDATION,
            coin="SOL",
            side=Side.LONG,
            notional_usd=6_500_000,
            price_usd=180.0,
            size=36111.11,
            trader="JupOwner1111111111111111111111111111111111",
            tx_ref="sig_xyz",
            pnl_usd=-1_200_000,
            extra={"track_record": poor},
        ))


def test_notifier_with_stats_renders():
    asyncio.run(_exercise_notifier_with_stats())


if __name__ == "__main__":
    test_empty_fills()
    test_hl_pnl_aggregation()
    test_hl_by_coin_split()
    test_hl_side_from_dir()
    test_recent_trades_sorted_newest_first()
    test_30d_window()
    test_tier_elite()
    test_tier_trusted()
    test_tier_poor_negative_pnl()
    test_tier_poor_low_winrate()
    test_tier_mixed_default()
    test_tier_new_low_sample()
    test_score_below_min_samples_is_none()
    test_score_elite_high()
    test_score_poor_trader()
    test_score_breakeven_mid()
    test_profit_factor_nones_and_values()
    test_cache_and_singleflight()
    print("trackrecord stats + tier + cache OK")
    test_notifier_with_stats_renders()
    print("trackrecord rendering OK")
