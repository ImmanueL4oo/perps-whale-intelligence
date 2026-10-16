"""Notifier Telegram filter tests.

We never actually hit Telegram — we just check that ._passes_tg_filter()
returns the right verdict for representative WalletStats / WhaleEvent
combinations so the user's "ping me only on real signal" preference is
preserved.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifier import Notifier
from trackers import EventKind, Side, WhaleEvent
from trackrecord import WalletStats


def _ev(*, notional: float, stats: WalletStats | None = None) -> WhaleEvent:
    extra = {"track_record": stats} if stats is not None else {}
    return WhaleEvent(
        ts_ms=int(time.time() * 1000),
        venue="Hyperliquid",
        kind=EventKind.OPEN,
        side=Side.LONG,
        coin="BTC",
        size=1.0,
        price_usd=70_000.0,
        notional_usd=notional,
        trader="0xabc",
        tx_ref="hl-1",
        extra=extra,
    )


def _stats(*, n: int, wr: float, pnl: float) -> WalletStats:
    win_count = int(round(wr * n))
    avg = pnl / n if n else 0.0
    return WalletStats(
        venue="Hyperliquid",
        wallet="0xabc",
        closed_trades=n,
        win_count=win_count,
        realized_pnl_usd=pnl,
        pnl_30d_usd=pnl,
        largest_win_usd=max(pnl, 0),
        largest_loss_usd=min(pnl, 0),
        avg_trade_pnl_usd=avg,
        first_trade_ts_ms=int(time.time() * 1000) - 120 * 86_400_000,
        fee_paid_usd=0.0,
        coverage_note=None,
        gross_wins_usd=max(pnl, 0),
        gross_losses_usd=min(pnl, 0),
    )


def _strict_notifier() -> Notifier:
    """Default Telegram filter: ≥$400K, ≥61% wr, TRUSTED/ELITE tier."""
    return Notifier(
        telegram_token="x", telegram_chat_id="y",
        tg_min_notional_usd=400_000,
        tg_min_winrate=0.61,
        tg_allowed_tiers=["TRUSTED", "ELITE"],
    )


def test_passes_when_all_conditions_met():
    n = _strict_notifier()
    s = _stats(n=15, wr=0.65, pnl=500_000)  # → TRUSTED tier
    ok, reason = n._passes_tg_filter(_ev(notional=500_000, stats=s))
    assert ok and reason == "", f"expected pass, got reason={reason!r}"


def test_rejected_below_notional():
    n = _strict_notifier()
    s = _stats(n=15, wr=0.65, pnl=500_000)
    ok, reason = n._passes_tg_filter(_ev(notional=399_999, stats=s))
    assert not ok and reason == "notional"


def test_rejected_below_winrate():
    n = _strict_notifier()
    # 60% wr is high enough for TRUSTED tier (≥55%) but below the 61% rule.
    s = _stats(n=15, wr=0.60, pnl=500_000)
    ok, reason = n._passes_tg_filter(_ev(notional=500_000, stats=s))
    assert not ok and reason == "winrate"


def test_rejected_when_tier_is_mixed():
    n = _strict_notifier()
    # 50% wr + positive PnL → MIXED, not TRUSTED.
    s = _stats(n=15, wr=0.50, pnl=10_000)
    ok, reason = n._passes_tg_filter(_ev(notional=500_000, stats=s))
    # winrate filter triggers first; either reason is correct, but it must be rejected.
    assert not ok
    assert reason in {"winrate", "tier"}


def test_rejected_when_no_track_record():
    n = _strict_notifier()
    ok, reason = n._passes_tg_filter(_ev(notional=500_000, stats=None))
    assert not ok and reason == "no_track_record"


def test_elite_passes_too():
    n = _strict_notifier()
    s = _stats(n=200, wr=0.70, pnl=5_000_000)  # → ELITE
    ok, reason = n._passes_tg_filter(_ev(notional=1_000_000, stats=s))
    assert ok, f"expected ELITE to pass, got reason={reason!r}"


def test_disabled_filter_lets_everything_through():
    # All thresholds zero / no allowed tiers → no filter at all.
    n = Notifier(
        telegram_token="x", telegram_chat_id="y",
        tg_min_notional_usd=0, tg_min_winrate=0, tg_allowed_tiers=None,
    )
    ok, _ = n._passes_tg_filter(_ev(notional=10_000))
    assert ok
    ok, _ = n._passes_tg_filter(_ev(notional=10_000, stats=_stats(n=1, wr=0.0, pnl=-1)))
    assert ok


def test_filter_active_flag():
    assert _strict_notifier()._tg_filter_active() is True
    n = Notifier(
        telegram_token="x", telegram_chat_id="y",
        tg_min_notional_usd=0, tg_min_winrate=0, tg_allowed_tiers=None,
    )
    assert n._tg_filter_active() is False


def test_watchlist_bypasses_filter():
    n = Notifier(
        telegram_token="x", telegram_chat_id="y",
        tg_min_notional_usd=400_000,
        tg_min_winrate=0.61,
        tg_allowed_tiers=["TRUSTED", "ELITE"],
        tg_watch_wallets=["0xWATCHED"],  # mixed-case input → matched lower-case
    )
    assert n.is_watched("0xwatched")
    assert n.is_watched("0xWATCHED")
    assert not n.is_watched("0xother")


def test_watchlist_empty_means_no_match():
    n = Notifier(telegram_token="x", telegram_chat_id="y")
    assert not n.is_watched("0xanything")


test_passes_when_all_conditions_met()
test_rejected_below_notional()
test_rejected_below_winrate()
test_rejected_when_tier_is_mixed()
test_rejected_when_no_track_record()
test_elite_passes_too()
test_disabled_filter_lets_everything_through()
test_filter_active_flag()
test_watchlist_bypasses_filter()
test_watchlist_empty_means_no_match()
print("telegram filter tests OK (10)")
