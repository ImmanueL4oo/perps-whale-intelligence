"""Tests for the trade-analytics module.

These exercise the pure functions in `analytics.py` against synthetic event
streams to lock down the directional / scoring conventions that the dashboard
depends on.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analytics


T0 = 1_700_000_000_000  # arbitrary anchor in ms
MIN = 60_000
HR = 60 * MIN
DAY = 24 * HR


def _ev(
    *,
    ts: int = T0,
    venue: str = "Hyperliquid",
    coin: str = "BTC",
    kind: str = "OPEN",
    side: str = "long",
    notional: float = 1_000_000,
    price: float = 60_000,
    trader: str = "0xa",
    tx: str = "tid-x",
    score: float | None = None,
    tier: str | None = None,
    leverage: float | None = 10.0,
) -> dict:
    out: dict = {
        "venue": venue, "coin": coin, "kind": kind, "side": side,
        "notional_usd": notional, "price_usd": price, "size": notional / price,
        "trader": trader, "tx_ref": tx, "ts_ms": ts,
        "leverage": leverage, "pnl_usd": None,
    }
    if score is not None or tier is not None:
        out["track_record"] = {
            "score": score,
            "tier": tier or "MIXED",
            "tier_emoji": {"ELITE": "🏆", "TRUSTED": "✅"}.get(tier or "MIXED", "⚖️"),
        }
    return out


# ── signed_flow conventions ──────────────────────────────────────────────────
def test_signed_flow_open_long_is_buy():
    assert analytics.signed_flow(_ev(kind="OPEN", side="long", notional=1e6)) == 1e6


def test_signed_flow_open_short_is_sell():
    assert analytics.signed_flow(_ev(kind="OPEN", side="short", notional=1e6)) == -1e6


def test_signed_flow_close_long_is_sell():
    assert analytics.signed_flow(_ev(kind="CLOSE", side="long", notional=1e6)) == -1e6


def test_signed_flow_close_short_is_buy():
    assert analytics.signed_flow(_ev(kind="CLOSE", side="short", notional=1e6)) == 1e6


def test_signed_flow_liq_long_is_forced_sell():
    assert analytics.signed_flow(_ev(kind="LIQ", side="long", notional=2e6)) == -2e6


def test_signed_flow_liq_short_is_forced_buy():
    assert analytics.signed_flow(_ev(kind="LIQ", side="short", notional=2e6)) == 2e6


# ── coin scoreboard ─────────────────────────────────────────────────────────
def test_coin_scoreboard_aggregates_per_window():
    events = [
        _ev(coin="BTC", kind="OPEN", side="long", notional=2e6, ts=T0 - 10 * MIN, trader="A"),
        _ev(coin="BTC", kind="OPEN", side="short", notional=1e6, ts=T0 - 30 * MIN, trader="B"),
        _ev(coin="BTC", kind="LIQ", side="long", notional=500_000, ts=T0 - 5 * MIN, trader="C"),
        _ev(coin="ETH", kind="OPEN", side="long", notional=3e6, ts=T0 - 90 * MIN, trader="D"),
        _ev(coin="ETH", kind="CLOSE", side="long", notional=2e6, ts=T0 - 25 * HR, trader="D"),  # outside 24h window
    ]
    rows = analytics.coin_scoreboard(events, now_ms=T0)
    assert {r["coin"] for r in rows} == {"BTC", "ETH"}

    btc = next(r for r in rows if r["coin"] == "BTC")
    w_15m = btc["windows"][str(15 * MIN)]
    # 15 m: only the long-2M open and the long-500K liq → 2M − 500K = +1.5M flow
    assert w_15m["flow_usd"] == 2e6 - 500_000
    assert w_15m["liq_count"] == 1
    assert w_15m["trade_count"] == 2

    w_1h = btc["windows"][str(HR)]
    # 1 h: long-2M open + short-1M open + long-500K liq → 2M − 1M − 500K = +500K
    assert w_1h["flow_usd"] == 2e6 - 1e6 - 500_000
    assert w_1h["volume_usd"] == 2e6 + 1e6 + 500_000

    eth = next(r for r in rows if r["coin"] == "ETH")
    w_24h_eth = eth["windows"][str(DAY)]
    assert w_24h_eth["trade_count"] == 1  # the close at -25h is outside the 24h window
    assert w_24h_eth["flow_usd"] == 3e6


def test_coin_scoreboard_smart_flow_uses_score_weight():
    events = [
        _ev(kind="OPEN", side="long", notional=1e6, score=80, tier="ELITE", trader="A"),
        _ev(kind="OPEN", side="long", notional=2e6, score=None, trader="B"),     # no score → ignored in smart flow
        _ev(kind="OPEN", side="short", notional=1e6, score=40, tier="MIXED", trader="C"),
    ]
    rows = analytics.coin_scoreboard(events, now_ms=T0)
    btc = rows[0]
    w_1h = btc["windows"][str(HR)]
    # Raw flow: +1M + 2M − 1M = +2M
    assert w_1h["flow_usd"] == 2e6
    # Smart flow: 1M*0.8 + 0 + (−1M)*0.4 = +800K − 400K = +400K
    assert abs(w_1h["smart_flow_usd"] - 400_000) < 1e-6


def test_coin_scoreboard_sorts_by_1h_volume():
    events = [
        _ev(coin="ETH", notional=1e6, ts=T0 - 5 * MIN, trader="A"),
        _ev(coin="BTC", notional=10e6, ts=T0 - 5 * MIN, trader="B"),
        _ev(coin="SOL", notional=5e6, ts=T0 - 5 * MIN, trader="C"),
    ]
    rows = analytics.coin_scoreboard(events, now_ms=T0)
    assert [r["coin"] for r in rows] == ["BTC", "SOL", "ETH"]


# ── consensus signals ────────────────────────────────────────────────────────
def test_consensus_fires_when_three_elite_traders_align():
    events = [
        _ev(coin="SOL", side="long", notional=5e6, trader="A", tx="a", ts=T0 - 10 * MIN, score=85, tier="ELITE"),
        _ev(coin="SOL", side="long", notional=3e6, trader="B", tx="b", ts=T0 - 8 * MIN, score=72, tier="TRUSTED"),
        _ev(coin="SOL", side="long", notional=2e6, trader="C", tx="c", ts=T0 - 2 * MIN, score=80, tier="ELITE"),
        # Distractor: opposite side → not part of the long cluster
        _ev(coin="SOL", side="short", notional=1e6, trader="D", tx="d", ts=T0 - 5 * MIN, score=70, tier="TRUSTED"),
    ]
    sigs = analytics.consensus_signals(events, now_ms=T0, min_traders=3)
    assert len(sigs) == 1
    s = sigs[0]
    assert s["coin"] == "SOL" and s["side"] == "long"
    assert s["trader_count"] == 3
    assert s["total_notional_usd"] == 1e7
    assert abs(s["avg_score"] - (85 + 72 + 80) / 3) < 1e-6
    assert {t["trader"] for t in s["traders"]} == {"A", "B", "C"}


def test_consensus_ignores_low_tier_traders():
    events = [
        _ev(coin="BTC", side="long", trader="A", tx="a", ts=T0, score=50, tier="MIXED"),
        _ev(coin="BTC", side="long", trader="B", tx="b", ts=T0, score=20, tier="POOR"),
    ]
    sigs = analytics.consensus_signals(events, now_ms=T0)
    assert sigs == []


def test_consensus_dedupes_same_trader_keeps_largest():
    events = [
        _ev(coin="ETH", side="short", trader="A", tx="a1", notional=1e6, ts=T0 - 10 * MIN, score=80, tier="ELITE"),
        _ev(coin="ETH", side="short", trader="A", tx="a2", notional=4e6, ts=T0 - 5 * MIN, score=80, tier="ELITE"),
        _ev(coin="ETH", side="short", trader="B", tx="b", notional=2e6, ts=T0 - 2 * MIN, score=75, tier="TRUSTED"),
    ]
    sigs = analytics.consensus_signals(events, now_ms=T0, min_traders=2)
    assert len(sigs) == 1
    assert sigs[0]["trader_count"] == 2
    # A's bigger event ($4M) should have been kept, totaling $6M not $7M.
    assert sigs[0]["total_notional_usd"] == 6e6


def test_consensus_close_events_are_ignored():
    events = [
        _ev(coin="HYPE", kind="CLOSE", side="long", trader="A", tx="a", ts=T0, score=80, tier="ELITE"),
        _ev(coin="HYPE", kind="CLOSE", side="long", trader="B", tx="b", ts=T0, score=80, tier="ELITE"),
    ]
    sigs = analytics.consensus_signals(events, now_ms=T0)
    assert sigs == []  # closes don't generate copy-trade signals


# ── smart positions ──────────────────────────────────────────────────────────
def test_smart_positions_close_clears_open():
    events = [
        _ev(coin="BTC", kind="OPEN", side="long", trader="A", tx="o", ts=T0 - 60 * MIN, score=80, tier="ELITE"),
        _ev(coin="BTC", kind="CLOSE", side="long", trader="A", tx="c", ts=T0 - 30 * MIN, score=80, tier="ELITE"),
        _ev(coin="BTC", kind="OPEN", side="short", trader="A", tx="o2", ts=T0 - 10 * MIN, notional=2e6, score=80, tier="ELITE"),
    ]
    rows = analytics.smart_positions(events, now_ms=T0)
    assert len(rows) == 1
    assert rows[0]["side"] == "short"
    assert rows[0]["notional_usd"] == 2e6


def test_smart_positions_filters_by_score():
    events = [
        _ev(coin="BTC", kind="OPEN", trader="A", tx="a", ts=T0, score=80, tier="ELITE"),
        _ev(coin="ETH", kind="OPEN", trader="B", tx="b", ts=T0, score=30, tier="POOR"),
    ]
    rows = analytics.smart_positions(events, now_ms=T0, min_score=60)
    assert {r["trader"] for r in rows} == {"A"}


def test_smart_positions_liq_clears_open():
    events = [
        _ev(coin="BTC", kind="OPEN", side="long", trader="A", tx="o", ts=T0 - 30 * MIN, score=80, tier="ELITE"),
        _ev(coin="BTC", kind="LIQ", side="long", trader="A", tx="l", ts=T0 - 5 * MIN, score=80, tier="ELITE"),
    ]
    rows = analytics.smart_positions(events, now_ms=T0)
    assert rows == []


def test_smart_positions_orders_by_score_then_notional():
    events = [
        _ev(coin="A", kind="OPEN", trader="t1", tx="1", ts=T0, notional=1e6, score=70, tier="TRUSTED"),
        _ev(coin="B", kind="OPEN", trader="t2", tx="2", ts=T0, notional=3e6, score=90, tier="ELITE"),
        _ev(coin="C", kind="OPEN", trader="t3", tx="3", ts=T0, notional=5e6, score=70, tier="TRUSTED"),
    ]
    rows = analytics.smart_positions(events, now_ms=T0)
    assert [r["trader"] for r in rows] == ["t2", "t3", "t1"]


def test_smart_positions_max_age_window():
    events = [
        _ev(coin="A", kind="OPEN", trader="A", tx="a", ts=T0 - 30 * HR, score=80, tier="ELITE"),
        _ev(coin="B", kind="OPEN", trader="B", tx="b", ts=T0 - 1 * HR, score=80, tier="ELITE"),
    ]
    rows = analytics.smart_positions(events, now_ms=T0, max_age_ms=24 * HR)
    assert {r["coin"] for r in rows} == {"B"}


# ── flow_timeseries ──────────────────────────────────────────────────────────
def test_flow_timeseries_bins_correctly():
    # Use a now_ms aligned exactly on a 5-minute boundary so the bin math is
    # easy to reason about — the two BTC longs land in the same bin, the ETH
    # event is filtered out, and the older BTC short ends up in an earlier bin.
    aligned_now = (T0 // (5 * MIN)) * (5 * MIN)
    events = [
        _ev(coin="BTC", kind="OPEN", side="long", notional=1e6, ts=aligned_now - 4 * MIN),
        _ev(coin="BTC", kind="OPEN", side="long", notional=2e6, ts=aligned_now - 2 * MIN),
        _ev(coin="BTC", kind="OPEN", side="short", notional=500_000, ts=aligned_now - 35 * MIN),
        _ev(coin="ETH", kind="OPEN", side="long", notional=10e6, ts=aligned_now - 4 * MIN),
    ]
    series = analytics.flow_timeseries(
        events, coin="BTC", bin_ms=5 * MIN, window_ms=HR, now_ms=aligned_now,
    )
    # All BTC events together: +1M + 2M − 500K = +2.5M
    total = sum(p["flow_usd"] for p in series)
    assert total == 2.5e6
    # The last BTC bin (covering -5..0 min) holds both longs = 3M.
    last_btc_bin = next(p for p in reversed(series) if p["flow_usd"] != 0)
    assert last_btc_bin["flow_usd"] == 3e6


# ── coin_top_traders ─────────────────────────────────────────────────────────
def test_coin_top_traders_ranks_by_score_then_size():
    events = [
        _ev(coin="BTC", trader="A", tx="a", ts=T0, notional=1e6, score=85, tier="ELITE"),
        _ev(coin="BTC", trader="B", tx="b", ts=T0, notional=5e6, score=70, tier="TRUSTED"),
        _ev(coin="BTC", trader="C", tx="c", ts=T0, notional=2e6, side="short", score=60, tier="MIXED"),
    ]
    rows = analytics.coin_top_traders(events, coin="BTC", now_ms=T0)
    assert [r["trader"] for r in rows] == ["A", "B", "C"]
    assert rows[2]["bias"] == "short"


# ── Actor classifier ─────────────────────────────────────────────────────────
def test_classify_actor_unknown_when_no_stats():
    label, tpd = analytics.classify_actor(None)
    assert label == "UNKNOWN" and tpd == 0.0


def test_classify_actor_unknown_when_below_min_samples():
    label, _ = analytics.classify_actor({"closed_trades": 3, "days_active": 10})
    assert label == "UNKNOWN"


def test_classify_actor_human_low_frequency():
    label, tpd = analytics.classify_actor({"closed_trades": 30, "days_active": 30})
    assert label == "HUMAN"
    assert tpd == 1.0


def test_classify_actor_hybrid_mid_frequency():
    label, tpd = analytics.classify_actor({"closed_trades": 200, "days_active": 20})
    assert label == "HYBRID"
    assert tpd == 10.0


def test_classify_actor_bot_high_frequency():
    label, tpd = analytics.classify_actor({"closed_trades": 5000, "days_active": 30})
    assert label == "BOT"
    assert abs(tpd - 5000 / 30) < 1e-6


def test_classify_actor_thresholds_are_tunable():
    # Re-classify the same wallet with stricter thresholds.
    stats = {"closed_trades": 200, "days_active": 20}  # 10 tpd
    label, _ = analytics.classify_actor(stats, hybrid_tpd=15)
    assert label == "HUMAN"
    label, _ = analytics.classify_actor(stats, bot_tpd=5)
    assert label == "BOT"


def test_consensus_filters_by_actor():
    """When `allowed_actors=['HUMAN','HYBRID']`, BOT-classified wallets are excluded."""
    bot_tr = {
        "score": 80, "tier": "ELITE",
        "closed_trades": 5000, "days_active": 30,  # → BOT
    }
    human_tr = {
        "score": 75, "tier": "TRUSTED",
        "closed_trades": 60, "days_active": 30,  # 2 tpd → HUMAN
    }
    events = [
        {**_ev(coin="ARB", side="long", trader="A", tx="a", ts=T0, score=80, tier="ELITE")},
        {**_ev(coin="ARB", side="long", trader="B", tx="b", ts=T0, score=75, tier="TRUSTED")},
    ]
    events[0]["track_record"] = {**events[0]["track_record"], **bot_tr}
    events[1]["track_record"] = {**events[1]["track_record"], **human_tr}

    sigs_all = analytics.consensus_signals(events, now_ms=T0, min_traders=2)
    assert len(sigs_all) == 1

    sigs_humans_only = analytics.consensus_signals(
        events, now_ms=T0, min_traders=2, allowed_actors={"HUMAN", "HYBRID"},
    )
    assert sigs_humans_only == []  # only one human, below min_traders


def test_smart_positions_filters_by_actor():
    bot_tr = {"score": 80, "tier": "ELITE", "closed_trades": 5000, "days_active": 30}
    human_tr = {"score": 75, "tier": "TRUSTED", "closed_trades": 60, "days_active": 30}
    e_bot = _ev(coin="X", trader="bot", tx="b", ts=T0, score=80, tier="ELITE")
    e_bot["track_record"] = {**e_bot["track_record"], **bot_tr}
    e_human = _ev(coin="Y", trader="hum", tx="h", ts=T0, score=75, tier="TRUSTED")
    e_human["track_record"] = {**e_human["track_record"], **human_tr}

    rows = analytics.smart_positions([e_bot, e_human], now_ms=T0, min_score=60)
    assert {r["trader"] for r in rows} == {"bot", "hum"}
    assert all(r["actor"] in {"BOT", "HUMAN"} for r in rows)

    no_bots = analytics.smart_positions(
        [e_bot, e_human], now_ms=T0, min_score=60,
        allowed_actors={"HUMAN", "HYBRID"},
    )
    assert {r["trader"] for r in no_bots} == {"hum"}


if __name__ == "__main__":
    test_classify_actor_unknown_when_no_stats()
    test_classify_actor_unknown_when_below_min_samples()
    test_classify_actor_human_low_frequency()
    test_classify_actor_hybrid_mid_frequency()
    test_classify_actor_bot_high_frequency()
    test_classify_actor_thresholds_are_tunable()
    test_consensus_filters_by_actor()
    test_smart_positions_filters_by_actor()
    test_signed_flow_open_long_is_buy()
    test_signed_flow_open_short_is_sell()
    test_signed_flow_close_long_is_sell()
    test_signed_flow_close_short_is_buy()
    test_signed_flow_liq_long_is_forced_sell()
    test_signed_flow_liq_short_is_forced_buy()
    test_coin_scoreboard_aggregates_per_window()
    test_coin_scoreboard_smart_flow_uses_score_weight()
    test_coin_scoreboard_sorts_by_1h_volume()
    test_consensus_fires_when_three_elite_traders_align()
    test_consensus_ignores_low_tier_traders()
    test_consensus_dedupes_same_trader_keeps_largest()
    test_consensus_close_events_are_ignored()
    test_smart_positions_close_clears_open()
    test_smart_positions_filters_by_score()
    test_smart_positions_liq_clears_open()
    test_smart_positions_orders_by_score_then_notional()
    test_smart_positions_max_age_window()
    test_flow_timeseries_bins_correctly()
    test_coin_top_traders_ranks_by_score_then_size()
    print("analytics tests OK (28)")
