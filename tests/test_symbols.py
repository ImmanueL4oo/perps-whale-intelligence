"""Tests for the symbol-selection plumbing.

Covers:
  • Config.coins property = union of HL + JUP coin lists.
  • JupiterPerpsTracker honours the optional `coins` whitelist when emitting events.
  • The actor classifier slot in /api/config.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from trackers.jupiter_perps import (
    CUSTODY_TO_COIN,
    JupiterPerpsTracker,
    _DecreaseEvent,
    _IncreaseEvent,
)


def _cfg(**overrides) -> Config:
    base = dict(
        threshold_usd=5_000_000.0,
        drop_unpriced=True,
        telegram_token=None,
        telegram_chat_id=None,
        hl_enabled=True,
        hl_coins=["BTC", "ETH"],
        hl_ws_url="x", hl_info_url="x",
        jup_enabled=True,
        jup_coins=["SOL", "BTC"],
        jup_enrich_instant=False,
        jup_enrich_concurrency=4,
        solana_rpc_http="x", solana_rpc_ws="x",
        drift_enabled=False,
        drift_markets=[],
        drift_coins=[],
        drift_poll_sec=10.0,
        drift_page_size=50,
        drift_data_api="https://data.api.drift.trade",
        drift_max_rps=1.0,
        cex_enabled=False,
        cex_min_flow_usd=25000.0,
        cex_max_concurrent_enrich=6,
        cex_alert_min_usd=0.0,
        web_enabled=False, web_host="x", web_port=0,
        mongo_enabled=False, mongo_uri="x", mongo_db="x", mongo_collection="x",
        bot_tpd_threshold=30.0,
        hybrid_tpd_threshold=5.0,
        actor_min_samples=10,
        tg_min_notional_usd=0.0,
        tg_min_winrate=0.0,
        tg_allowed_tiers=[],
        tg_watch_wallets=[],
        conviction_min_traders=3,
        conviction_window_min=30.0,
        conviction_cooldown_min=30.0,
        lifecycle_alerts_enabled=True,
        poll_enabled=False,
        poll_interval_sec=30.0,
        poll_max_wallets=40,
        poll_min_change_usd=1000.0,
        tg_command_bot_enabled=False,
        tg_authorized_chats=[],
        tg_dashboard_url="http://127.0.0.1:8787/",
        tg_snapshot_enabled=False,
        tg_auto_snap_min=0.0,
        snap_settle_ms=250,
        snap_cache_max_age_sec=2.0,
        snap_auto_refresh_sec=5.0,
        snap_reload_every_sec=300.0,
    )
    base.update(overrides)
    return Config(**base)


def test_config_coins_is_union_sorted():
    cfg = _cfg(hl_coins=["ETH", "BTC", "SOL"], jup_coins=["BTC"])
    assert cfg.coins == ["BTC", "ETH", "SOL"]


def test_config_coins_dedupes():
    cfg = _cfg(hl_coins=["BTC", "BTC"], jup_coins=["BTC"])
    assert cfg.coins == ["BTC"]


# ── Jupiter tracker symbol filter ─────────────────────────────────────────────
SOL_CUSTODY = next(c for c, name in CUSTODY_TO_COIN.items() if name == "SOL")
ETH_CUSTODY = next(c for c, name in CUSTODY_TO_COIN.items() if name == "ETH")


def _fake_increase(custody: str, atomic_size: int = 10_000_000_000) -> _IncreaseEvent:
    return _IncreaseEvent(
        position_key="pk",
        side=1,  # long
        position_custody=custody,
        position_size_usd=atomic_size,
        owner="owner1",
        size_usd_delta=atomic_size,
        price=200_000_000,
        fee_usd=0,
    )


def test_jupiter_tracker_passes_through_when_no_filter():
    tracker = JupiterPerpsTracker(threshold_usd=1_000.0)
    ev_sol = tracker._to_whale_event(_fake_increase(SOL_CUSTODY), "sig-sol")
    ev_eth = tracker._to_whale_event(_fake_increase(ETH_CUSTODY), "sig-eth")
    assert ev_sol is not None and ev_sol.coin == "SOL"
    assert ev_eth is not None and ev_eth.coin == "ETH"


def test_jupiter_tracker_drops_coins_outside_whitelist():
    tracker = JupiterPerpsTracker(threshold_usd=1_000.0, coins=["SOL"])
    sol_ev = tracker._to_whale_event(_fake_increase(SOL_CUSTODY), "sig-sol")
    eth_ev = tracker._to_whale_event(_fake_increase(ETH_CUSTODY), "sig-eth")
    assert sol_ev is not None and sol_ev.coin == "SOL"
    assert eth_ev is None  # filtered out


def test_jupiter_tracker_whitelist_is_case_insensitive():
    tracker = JupiterPerpsTracker(threshold_usd=1_000.0, coins=["sol"])
    sol_ev = tracker._to_whale_event(_fake_increase(SOL_CUSTODY), "sig-sol")
    assert sol_ev is not None and sol_ev.coin == "SOL"


if __name__ == "__main__":
    test_config_coins_is_union_sorted()
    test_config_coins_dedupes()
    test_jupiter_tracker_passes_through_when_no_filter()
    test_jupiter_tracker_drops_coins_outside_whitelist()
    test_jupiter_tracker_whitelist_is_case_insensitive()
    print("symbol selection tests OK (5)")
