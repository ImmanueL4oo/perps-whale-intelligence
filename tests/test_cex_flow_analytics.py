"""Tests for the windowed CEX flow analyzer.

We seed the analyzer with a mix of in-window and out-of-window events
covering multiple exchanges + assets, then assert the summary numbers
match what we'd compute by hand. No tracker code involved — analyzer
is standalone.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from cex_flow_analytics import CexFlowAnalyzer, WINDOWS_MIN


def _ev(*, ts_ms, exchange, direction, asset, usd):
    return {
        "ts_ms": ts_ms, "exchange": exchange, "direction": direction,
        "asset": asset, "amount_usd": usd,
        "signature": f"sig-{ts_ms}-{exchange}", "amount": 1.0,
    }


def _now_ms(): return int(time.time() * 1000)


def _seed_basic(analyzer):
    now = _now_ms()
    # 1 minute ago
    analyzer.record(_ev(ts_ms=now - 60_000,  exchange="Binance",  direction="IN",  asset="USDC", usd=100_000))
    analyzer.record(_ev(ts_ms=now - 120_000, exchange="Binance",  direction="OUT", asset="SOL",  usd=50_000))
    analyzer.record(_ev(ts_ms=now - 5*60_000, exchange="Coinbase", direction="OUT", asset="USDC", usd=200_000))
    # Out-of-window (2 hours ago)
    analyzer.record(_ev(ts_ms=now - 2*3600_000, exchange="OKX", direction="IN", asset="USDC", usd=999_999_999))


def test_summary_basic_aggregation():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    s = a.summary(window_min=15)
    tot = s["totals"]
    assert tot["events"] == 3
    assert tot["in_usd"]  == pytest.approx(100_000)
    assert tot["out_usd"] == pytest.approx(250_000)
    assert tot["net_usd"] == pytest.approx(150_000)


def test_summary_excludes_out_of_window():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    # 4-minute window — only the first two events qualify.
    s = a.summary(window_min=4)
    assert s["totals"]["events"] == 2


def test_by_exchange_and_asset_breakdown():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    s = a.summary(window_min=15)
    by_ex = {r["exchange"]: r for r in s["by_exchange"]}
    by_asset = {r["asset"]: r for r in s["by_asset"]}

    assert by_ex["Binance"]["in_usd"]  == pytest.approx(100_000)
    assert by_ex["Binance"]["out_usd"] == pytest.approx(50_000)
    assert by_ex["Binance"]["net_usd"] == pytest.approx(-50_000)
    assert by_ex["Coinbase"]["out_usd"] == pytest.approx(200_000)
    assert by_ex["Coinbase"]["net_usd"] == pytest.approx(200_000)

    assert by_asset["USDC"]["in_usd"]  == pytest.approx(100_000)
    assert by_asset["USDC"]["out_usd"] == pytest.approx(200_000)
    assert by_asset["SOL"]["out_usd"]  == pytest.approx(50_000)


def test_top_transfers_sorted_desc():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    s = a.summary(window_min=15, top_transfers=3)
    amounts = [t["amount_usd"] for t in s["top_transfers"]]
    assert amounts == sorted(amounts, reverse=True)
    assert amounts[0] == pytest.approx(200_000)


def test_bias_score_signs_correctly():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    # gross = 350K, net = +150K → bias ≈ +42.86
    s = a.summary(window_min=15)
    assert s["bias"] == pytest.approx(150_000 / 350_000 * 100, rel=1e-3)


def test_empty_summary_is_safe():
    a = CexFlowAnalyzer()
    s = a.summary(window_min=60)
    assert s["totals"]["events"] == 0
    assert s["totals"]["in_usd"] == 0.0
    assert s["bias"] == 0.0
    assert s["by_exchange"] == []
    assert s["by_asset"] == []
    assert s["top_transfers"] == []


def test_windows_strip_present_for_each_label():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    s = a.summary(window_min=60)
    for label in WINDOWS_MIN.keys():
        assert label in s["windows"]
        assert "net_usd" in s["windows"][label]


def test_buckets_returns_correct_shape():
    a = CexFlowAnalyzer()
    _seed_basic(a)
    rows = a.buckets(window_min=60, bucket_min=15)
    assert len(rows) == 4
    for r in rows:
        assert {"ts_ms", "in_usd", "out_usd", "net_usd", "events"} <= set(r.keys())
