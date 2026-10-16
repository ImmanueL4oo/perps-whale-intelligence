"""Solana Perps Whale Tracker — entrypoint.

Runs Hyperliquid + Jupiter Perps trackers concurrently and routes whale events
through a single notifier (rich console + optional Telegram).

Usage:
    python bot.py
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import AsyncIterator, Optional

from cex_flow_analytics import CexFlowAnalyzer
from config import Config
from dashboard_snapshot import SnapshotService
from event_bus import EventBus, serialize_event
from notifier import Notifier
from position_poller import PositionPoller
from pricing import PriceFeed
from signals import ConvictionWatcher, LifecycleTracker
from storage import MongoStorage, FileStorage
from telegram_bot import TelegramCommandBot
from trackers import WhaleEvent
from trackers.cex_flows import CexFlowEvent, CexFlowsTracker
from trackers.drift import DriftTracker, DEFAULT_MARKETS as DRIFT_DEFAULT_MARKETS
from trackers.hyperliquid import HyperliquidTracker
from trackers.jupiter_perps import JupiterPerpsTracker
from trackers.solana_rpc import SolanaRpc
from trackrecord import TrackRecord
import webserver


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


async def _drain(stream: AsyncIterator[WhaleEvent], queue: asyncio.Queue, label: str) -> None:
    try:
        async for ev in stream:
            await queue.put(ev)
    except asyncio.CancelledError:
        raise
    except Exception:
        logging.getLogger(label).exception("tracker died — restart by re-running the bot")
        raise


def _cex_event_to_payload(ev: CexFlowEvent) -> dict:
    """Render a CEX flow event into the same flat dict shape the dashboard /
    Mongo layer already understands. We map:

      direction  IN/OUT                     -> kind="CEX_IN"/"CEX_OUT"
      asset      "USDC"/"SOL"/...           -> coin
      cex_wallet (the CEX-side address)     -> trader
      signature                             -> tx_ref
      amount_usd                            -> notional_usd

    The extra `cex_*` fields ride along under the `cex` namespace so the
    frontend can render exchange/counterparty/etc. without reaching into
    a new collection.
    """
    side = "long" if ev.direction == "OUT" else "short"   # OUT==buy pressure
    return {
        "venue": "CEX Flows",
        "kind": f"CEX_{ev.direction}",
        "coin": ev.asset,
        "side": side,
        "notional_usd": float(ev.amount_usd or 0.0),
        "price_usd": float((ev.extra or {}).get("price_usd") or 0.0),
        "size": float(ev.amount or 0.0),
        "trader": ev.cex_wallet,
        "tx_ref": ev.signature,
        "ts_ms": int(ev.ts_ms),
        "pnl_usd": None,
        "leverage": None,
        "is_cex_flow": True,
        "cex": {
            "exchange": ev.exchange,
            "label": ev.cex_label,
            "wallet": ev.cex_wallet,
            "direction": ev.direction,
            "asset": ev.asset,
            "mint": ev.mint,
            "amount": float(ev.amount or 0.0),
            "amount_usd": float(ev.amount_usd or 0.0),
            "counterparty": ev.counterparty,
            "signature": ev.signature,
            "slot": ev.slot,
            "block_time": ev.block_time,
        },
    }


def _cex_alert_text(ev: CexFlowEvent) -> str:
    """HTML-formatted Telegram alert for a single big CEX flow."""
    arrow = "🔺 INFLOW" if ev.direction == "IN" else "🔻 OUTFLOW"
    return (
        f"<b>{arrow}</b>  <code>{ev.exchange}</code>\n"
        f"<b>{ev.amount:,.2f} {ev.asset}</b>  ≈ ${ev.amount_usd:,.0f}\n"
        f"sig: <code>{ev.signature[:16]}…</code>"
    )


async def _run(config: Config) -> None:
    venues: list[str] = []
    if config.hl_enabled:
        venues.append("Hyperliquid")
    if config.jup_enabled:
        venues.append("Jupiter Perps")
    if config.drift_enabled:
        venues.append("Drift")
    if config.cex_enabled:
        venues.append("CEX Flows")

    if not venues:
        print(
            "No venues enabled. Set HL_ENABLED, JUP_ENABLED, or DRIFT_ENABLED to true.",
            file=sys.stderr,
        )
        sys.exit(2)

    queue: asyncio.Queue[WhaleEvent] = asyncio.Queue(maxsize=1000)

    track_record = TrackRecord(
        hl_info_url=config.hl_info_url,
        sol_rpc_http=config.solana_rpc_http,
        drift_data_api=config.drift_data_api,
    )
    bus = EventBus(recent_size=500)

    # Created up-front so the webserver receives a stable reference even if
    # the CEX tracker is disabled (the analyzer is then never written to).
    cex_analyzer = CexFlowAnalyzer(recent_size=5000)

    storage = MongoStorage(
        uri=config.mongo_uri,
        db_name=config.mongo_db,
        collection_name=config.mongo_collection,
    )
    if config.mongo_enabled:
        ok = await storage.connect()
        if ok and storage.available:
            seeded = await storage.recent(limit=500)
            for doc in seeded:
                bus._recent.append(doc)
            count = await storage.total_count()
            print(f"  mongo: {storage.db_name}/{storage.collection_name} "
                  f"(seeded {len(seeded)} of ~{count:,} stored events)")
        else:
            # Try a local file-backed fallback so users without MongoDB still
            # get persistence across restarts.
            fallback = FileStorage(path=os.path.join(os.getcwd(), "data", "local_events.json"), db_name=config.mongo_db, collection_name=config.mongo_collection)
            ok2 = await fallback.connect()
            if ok2 and fallback.available:
                storage = fallback
                seeded = await storage.recent(limit=500)
                for doc in seeded:
                    bus._recent.append(doc)
                count = await storage.total_count()
                print(f"  local storage: {fallback.path} (seeded {len(seeded)} of ~{count:,} stored events)")

    async with Notifier(
        telegram_token=config.telegram_token,
        telegram_chat_id=config.telegram_chat_id,
        tg_min_notional_usd=config.tg_min_notional_usd,
        tg_min_winrate=config.tg_min_winrate,
        tg_allowed_tiers=config.tg_allowed_tiers,
        tg_watch_wallets=config.tg_watch_wallets,
    ) as notifier, track_record as tr:
        notifier.banner(config.threshold_usd, venues)

        # Trading-signal watchers — only useful when Telegram is configured.
        send_tg = notifier.send_html if config.telegram_configured else None
        conviction = ConvictionWatcher(
            bus,
            send_tg,
            min_traders=config.conviction_min_traders,
            window_ms=int(config.conviction_window_min * 60_000),
            cooldown_ms=int(config.conviction_cooldown_min * 60_000),
        )
        conviction.warm_up()
        lifecycle: Optional[LifecycleTracker] = None
        if config.lifecycle_alerts_enabled:
            lifecycle = LifecycleTracker(send_tg)
            lifecycle.warm_up(list(bus._recent))

        poller: Optional[PositionPoller] = None
        if config.poll_enabled and config.hl_enabled:
            poller = PositionPoller(
                bus,
                hl_info_url=config.hl_info_url,
                watchlist=config.tg_watch_wallets,
                interval_sec=config.poll_interval_sec,
                max_wallets=config.poll_max_wallets,
                min_change_usd=config.poll_min_change_usd,
            )
            await poller.start()

        web_cleanup = None
        if config.web_enabled:
            try:
                web_cleanup = await webserver.serve(
                    bus,
                    host=config.web_host,
                    port=config.web_port,
                    threshold_usd=config.threshold_usd,
                    venues=venues,
                    coins=config.coins,
                    track_record=tr,
                    storage=storage,
                    poller=poller,
                    cex_analyzer=cex_analyzer if config.cex_enabled else None,
                )
                print(f"  dashboard: http://{config.web_host if config.web_host != '0.0.0.0' else 'localhost'}:{config.web_port}")
            except OSError as exc:
                logging.getLogger("web").warning(
                    "could not bind %s:%d (%s) — dashboard disabled",
                    config.web_host, config.web_port, exc,
                )

        # ── Telegram command bot + dashboard snapshot service ──────────────
        snapshot: Optional[SnapshotService] = None
        tg_bot: Optional[TelegramCommandBot] = None
        if (
            config.telegram_configured
            and config.tg_command_bot_enabled
            and config.tg_authorized_chats
        ):
            if config.tg_snapshot_enabled and config.web_enabled:
                snapshot = SnapshotService(
                    default_url=config.tg_dashboard_url,
                    settle_ms=config.snap_settle_ms,
                    cache_max_age_sec=config.snap_cache_max_age_sec,
                    auto_refresh_sec=config.snap_auto_refresh_sec,
                    reload_every_sec=config.snap_reload_every_sec,
                )
                # Best-effort start; failures (no Playwright installed, no
                # Chromium binary) leave the service in an unavailable state
                # and the command bot reports the reason via /snap.
                await snapshot.start()
                if snapshot.available:
                    await snapshot.start_auto_refresh()
            tg_bot = TelegramCommandBot(
                notifier=notifier,
                bus=bus,
                poller=poller,
                snapshot=snapshot,
                dashboard_url=config.tg_dashboard_url,
                token=config.telegram_token,
                authorized_chats=config.tg_authorized_chats,
                cfg_threshold_usd=config.threshold_usd,
                venues=venues,
            )
            await tg_bot.start()
            # Optionally fire up scheduled snapshots straight from .env so
            # the user doesn't have to type /auto every restart.
            if config.tg_auto_snap_min > 0:
                tg_bot._auto.enabled = True
                tg_bot._auto.interval_sec = config.tg_auto_snap_min * 60.0
                tg_bot._auto.target_chats = set(config.tg_authorized_chats)
                tg_bot._auto.last_fired_at = 0.0
                print(
                    f"  telegram: auto-snap every {config.tg_auto_snap_min:g} min → "
                    f"{len(config.tg_authorized_chats)} chat(s)"
                )
            print(
                f"  telegram bot: command listener active for chat(s) "
                f"{config.tg_authorized_chats}"
            )

        tasks: list[asyncio.Task] = []
        if config.hl_enabled:
            hl = HyperliquidTracker(
                threshold_usd=config.threshold_usd,
                coins=config.hl_coins or None,
                ws_url=config.hl_ws_url,
                info_url=config.hl_info_url,
                drop_unpriced=config.drop_unpriced,
            )
            tasks.append(asyncio.create_task(_drain(hl.stream(), queue, "hyperliquid"), name="hl"))

        # One shared Solana RPC client for any on-chain enrichment we need:
        # - Jupiter Perps Instant* tx decoding (positional fetch via getTransaction).
        # - CEX flows: every logsNotification triggers a getTransaction.
        # - Future trackers can reuse it without reinitializing.
        sol_rpc: Optional[SolanaRpc] = None
        needs_rpc = (config.jup_enabled and config.jup_enrich_instant) or config.cex_enabled
        if needs_rpc:
            sol_rpc = SolanaRpc(http_url=config.solana_rpc_http)
            await sol_rpc.start()

        if config.jup_enabled:
            jup = JupiterPerpsTracker(
                threshold_usd=config.threshold_usd,
                rpc_ws_url=config.solana_rpc_ws,
                coins=config.jup_coins or None,
                rpc=sol_rpc,
                max_concurrent_enrich=config.jup_enrich_concurrency,
            )
            tasks.append(asyncio.create_task(_drain(jup.stream(), queue, "jupiter_perps"), name="jup"))

        drift_tracker: Optional[DriftTracker] = None
        if config.drift_enabled:
            # Markets default: Drift's deepest perps. Coin filter further
            # narrows; if both empty we fall back to the curated DEFAULT_MARKETS.
            markets = config.drift_markets or list(DRIFT_DEFAULT_MARKETS)
            coin_filter = config.drift_coins or config.coins or None
            drift_tracker = DriftTracker(
                threshold_usd=config.threshold_usd,
                markets=markets,
                coins=coin_filter,
                poll_interval_sec=config.drift_poll_sec,
                page_size=config.drift_page_size,
                data_api_url=config.drift_data_api,
                max_rps=config.drift_max_rps,
            )
            tasks.append(asyncio.create_task(
                _drain(drift_tracker.stream(), queue, "drift"), name="drift",
            ))

        # ── CEX in/out flow tracker ──────────────────────────────────────────
        # Independent pipeline because CEX flow events have a different shape
        # than `WhaleEvent` (no perp side / leverage / track-record). They get
        # serialized as flat dicts and published directly to the bus, mirroring
        # how `position_poller` handles its synthetic events.
        cex_tracker: Optional[CexFlowsTracker] = None
        price_feed: Optional[PriceFeed] = None
        if config.cex_enabled:
            assert sol_rpc is not None  # set above when cex_enabled is True
            price_feed = PriceFeed()
            await price_feed.start()
            cex_tracker = CexFlowsTracker(
                ws_url=config.solana_rpc_ws,
                rpc=sol_rpc,
                price_feed=price_feed,
                min_flow_usd=config.cex_min_flow_usd,
                max_concurrent_enrich=config.cex_max_concurrent_enrich,
            )

            cex_alert_min = config.cex_alert_min_usd

            async def cex_consumer() -> None:
                """Drain the CEX tracker, store + publish each event."""
                async for ev in cex_tracker.stream():
                    payload = _cex_event_to_payload(ev)
                    cex_analyzer.record(payload)
                    try:
                        bus._recent.append(payload)
                        for q in list(bus._subs):
                            try:
                                q.put_nowait(payload)
                            except asyncio.QueueFull:
                                pass
                    except Exception:
                        logging.getLogger("cex_flows").exception("bus broadcast failed")
                    if storage.available:
                        try:
                            await asyncio.wait_for(
                                storage.insert(payload), timeout=2.0,
                            )
                        except Exception:
                            logging.getLogger("storage").debug(
                                "cex flow persist failed", exc_info=True,
                            )
                    if cex_alert_min > 0 and ev.amount_usd >= cex_alert_min:
                        try:
                            await notifier.send_alert_html(_cex_alert_text(ev))
                        except Exception:
                            logging.getLogger("notifier").debug(
                                "cex alert failed", exc_info=True,
                            )

            tasks.append(asyncio.create_task(cex_consumer(), name="cex_flows"))

        # Enrichment timeouts: HL is one cheap call, Jupiter walks Solana RPC,
        # Drift is one REST call (gated by the same 1 RPS budget as the live
        # tracker but still typically <1s).
        hl_timeout = 4.0
        jup_timeout = 8.0
        drift_timeout = 4.0

        async def enrich_and_emit(ev: WhaleEvent) -> None:
            if ev.venue == "JupiterPerps":
                timeout = jup_timeout
            elif ev.venue == "Drift":
                timeout = drift_timeout
            else:
                timeout = hl_timeout
            try:
                stats = await tr.get(ev.venue, ev.trader, timeout=timeout)
            except Exception:
                stats = None
            if stats is not None:
                ev.extra["track_record"] = stats
            try:
                await notifier.emit(ev)
            except Exception:
                logging.getLogger("notifier").exception("notifier failed")
            try:
                bus.publish(ev)
            except Exception:
                logging.getLogger("web").exception("bus.publish failed")
            if storage.available:
                try:
                    await asyncio.wait_for(
                        storage.insert(serialize_event(ev)), timeout=2.0,
                    )
                except Exception:
                    logging.getLogger("storage").debug("persist failed", exc_info=True)
            # Run trading-signal watchers AFTER bus.publish so the new event
            # is part of the rolling window that consensus_signals scans.
            ev_dict = serialize_event(ev)
            try:
                await conviction.check(ev_dict)
            except Exception:
                logging.getLogger("conviction").exception("conviction check failed")
            if lifecycle is not None:
                try:
                    await lifecycle.check(ev_dict)
                except Exception:
                    logging.getLogger("lifecycle").exception("lifecycle check failed")

        async def consumer(worker_id: int) -> None:
            while True:
                ev = await queue.get()
                await enrich_and_emit(ev)

        # 4 concurrent workers — keeps the alert pipeline flowing when several
        # whales fire at once (each enrichment may sit on the network for ~seconds).
        for i in range(4):
            tasks.append(asyncio.create_task(consumer(i), name=f"consumer-{i}"))

        stop = asyncio.Event()
        sigint_count = {"n": 0}

        def _stop(*_: object) -> None:
            sigint_count["n"] += 1
            if sigint_count["n"] == 1:
                # First Ctrl+C: graceful shutdown with hard timeout below.
                print("\n  shutting down… (press Ctrl+C again to force-quit)", flush=True)
                stop.set()
            else:
                print("\n  force quit.", flush=True)
                os._exit(130)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                # Signals aren't supported on Windows event loops.
                pass

        # Wait for either a tracker to crash unrecoverably, or a stop signal.
        done_task = asyncio.create_task(stop.wait())
        all_tasks = tasks + [done_task]
        finished, _ = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

        for t in tasks:
            t.cancel()

        async def _drain_with_timeout() -> None:
            await asyncio.gather(*tasks, return_exceptions=True)
            if tg_bot is not None:
                try:
                    await tg_bot.stop()
                except Exception:
                    pass
            if snapshot is not None:
                try:
                    await snapshot.stop()
                except Exception:
                    pass
            if poller is not None:
                try:
                    await poller.stop()
                except Exception:
                    pass
            if drift_tracker is not None:
                try:
                    await drift_tracker.close()
                except Exception:
                    pass
            if cex_tracker is not None:
                try:
                    await cex_tracker.close()
                except Exception:
                    pass
            if price_feed is not None:
                try:
                    await price_feed.close()
                except Exception:
                    pass
            if sol_rpc is not None:
                try:
                    await sol_rpc.close()
                except Exception:
                    pass
            if web_cleanup is not None:
                try:
                    await web_cleanup()
                except Exception:
                    pass
            try:
                await storage.close()
            except Exception:
                pass

        try:
            await asyncio.wait_for(_drain_with_timeout(), timeout=3.0)
        except asyncio.TimeoutError:
            # WebSocket close handshakes / aiohttp finalizers can stall — never
            # block the user on cleanup. Mongo writes are crash-safe; the OS
            # will reap the rest.
            print("  cleanup timed out — exiting anyway.", flush=True)
            os._exit(0)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Solana Perps whale tracker")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        help="override WHALE_THRESHOLD_USD from .env (USD notional)",
    )
    parser.add_argument(
        "--coins",
        type=str,
        help=(
            "comma-separated list of symbols to track for this run. "
            "Overrides both HL_COINS and JUP_COINS — each tracker subscribes "
            "to the intersection with its own supported universe."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _setup_logging(args.log_level)
    cfg = Config.from_env()
    if args.threshold is not None:
        from dataclasses import replace
        cfg = replace(cfg, threshold_usd=args.threshold)
    if args.coins:
        from dataclasses import replace
        from trackers.jupiter_perps import CUSTODY_TO_COIN
        wanted = [c.strip() for c in args.coins.split(",") if c.strip()]
        # HL passes through whatever the user types — they know their tickers.
        # Jupiter is restricted to its three available custodies.
        jup_universe = {v.upper() for v in CUSTODY_TO_COIN.values() if v not in {"USDC", "USDT"}}
        jup = [c for c in wanted if c.upper() in jup_universe]
        # Drift covers everything HL does plus more — pass the wanted list
        # through as a coin filter (DriftTracker takes the intersection
        # against its subscribed markets).
        cfg = replace(cfg, hl_coins=wanted, jup_coins=jup, drift_coins=wanted)
    if cfg.coins:
        print(f"  tracking: {', '.join(cfg.coins)}")
    try:
        asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
