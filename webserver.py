"""Tiny aiohttp server that exposes the live event feed to the browser.

Routes
------
GET /                — the dashboard HTML (single static file)
GET /api/recent      — JSON list of recent events (initial page load)
GET /api/stats       — JSON aggregate counts / totals
GET /events          — Server-Sent Events stream of live whale events

Designed to run inside the bot's event loop alongside the trackers.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Awaitable, Callable, Optional

from aiohttp import web

import analytics
from cex_flow_analytics import CexFlowAnalyzer, WINDOWS_MIN
from event_bus import EventBus, serialize_stats
from storage import MongoStorage
from trackrecord import TrackRecord

log = logging.getLogger("web")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
INDEX_FILE = os.path.join(STATIC_DIR, "index.html")


def build_app(
    bus: EventBus,
    *,
    threshold_usd: float,
    venues: list[str],
    coins: Optional[list[str]] = None,
    track_record: Optional[TrackRecord] = None,
    storage: Optional[MongoStorage] = None,
    poller: Optional[object] = None,
    cex_analyzer: Optional[CexFlowAnalyzer] = None,
) -> web.Application:
    app = web.Application()
    app["bus"] = bus
    app["threshold_usd"] = threshold_usd
    app["venues"] = venues
    app["coins"] = coins or []
    app["track_record"] = track_record
    app["storage"] = storage
    app["poller"] = poller
    app["cex_analyzer"] = cex_analyzer

    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/recent", _handle_recent)
    app.router.add_get("/api/stats", _handle_stats)
    app.router.add_get("/api/config", _handle_config)
    app.router.add_get("/api/leaderboard", _handle_leaderboard)
    app.router.add_get("/api/wallet/{venue}/{address}", _handle_wallet)
    app.router.add_get("/api/wallet/{venue}/{address}/events", _handle_wallet_events)
    app.router.add_get("/api/history", _handle_history)
    app.router.add_get("/api/signals", _handle_signals)
    app.router.add_get("/api/coins", _handle_coins)
    app.router.add_get("/api/coin/{coin}", _handle_coin)
    app.router.add_get("/api/positions", _handle_positions)
    app.router.add_get("/api/smart-money", _handle_smart_money)
    app.router.add_get("/api/live-positions", _handle_live_positions)
    app.router.add_get("/api/health", _handle_health)
    app.router.add_get("/api/cex-flows", _handle_cex_flows)
    app.router.add_get("/api/cex-flows/recent", _handle_cex_flows_recent)
    app.router.add_get("/api/cex-flows/series", _handle_cex_flows_series)
    app.router.add_get("/events", _handle_sse)
    return app


# ── Source helper used by every analytics endpoint ───────────────────────────
def _backfill_actor(events: list[dict]) -> list[dict]:
    """Older Mongo documents pre-date the actor classifier — compute it lazily."""
    for e in events:
        tr = e.get("track_record")
        if isinstance(tr, dict) and "actor" not in tr:
            label, tpd = analytics.classify_actor(tr)
            tr["actor"] = label
            tr["trades_per_day"] = tpd
    return events


async def _fetch_events(request: web.Request, *, limit: int = 5000) -> list[dict]:
    """Prefer Mongo (richer history) and fall back to the in-memory bus."""
    storage: Optional[MongoStorage] = request.app.get("storage")
    if storage is not None and storage.available:
        rows = await storage.history(limit=limit)
        return _backfill_actor(rows)
    bus: EventBus = request.app["bus"]
    return _backfill_actor(list(bus.recent()))


def _parse_actors(raw: Optional[str]) -> Optional[set[str]]:
    """Parse a comma-separated actor list. None / empty → no filter."""
    if not raw:
        return None
    parts = {p.strip().upper() for p in raw.split(",") if p.strip()}
    valid = parts & set(analytics.ACTOR_LABELS)
    return valid or None


async def _handle_index(request: web.Request) -> web.Response:
    if not os.path.exists(INDEX_FILE):
        return web.Response(text="index.html missing", status=500)
    return web.FileResponse(INDEX_FILE)


async def _handle_recent(request: web.Request) -> web.Response:
    bus: EventBus = request.app["bus"]
    return web.json_response(_backfill_actor(list(bus.recent())))


async def _handle_health(request: web.Request) -> web.Response:
    """Lightweight health check — useful for `curl`, alerting, and the
    dashboard to self-diagnose when something feels off.

    Reports event throughput by walking only the recent buffer (capped at
    1000 events by the bus). Cheap enough to call every second.
    """
    bus: EventBus = request.app["bus"]
    poller = request.app.get("poller")
    storage: Optional[MongoStorage] = request.app.get("storage")
    recent = list(bus.recent())
    now_ms = int(time.time() * 1000)
    last_ts = max((e.get("ts_ms") or 0 for e in recent), default=0)
    age_s = (now_ms - last_ts) / 1000.0 if last_ts else None
    # Per-venue last-seen + counts in the last 5/15m.
    by_venue: dict[str, dict] = {}
    cut5 = now_ms - 5 * 60_000
    cut15 = now_ms - 15 * 60_000
    for e in recent:
        v = e.get("venue") or "?"
        b = by_venue.setdefault(v, {"last_ts_ms": 0, "n_5m": 0, "n_15m": 0, "total": 0})
        b["total"] += 1
        ts = e.get("ts_ms") or 0
        if ts > b["last_ts_ms"]:
            b["last_ts_ms"] = ts
        if ts >= cut5:
            b["n_5m"] += 1
        if ts >= cut15:
            b["n_15m"] += 1
    for v, b in by_venue.items():
        b["last_age_s"] = (now_ms - b["last_ts_ms"]) / 1000.0 if b["last_ts_ms"] else None

    poller_info: dict = {"enabled": False}
    if poller is not None:
        try:
            poller_info = {
                "enabled": True,
                "interval_sec": getattr(poller, "interval_sec", None),
                "snapshots": len(getattr(poller, "snapshots", {}) or {}),
                "watchlist_size": len(getattr(poller, "watchlist", set()) or set()),
                "max_wallets": getattr(poller, "max_wallets", None),
            }
        except Exception:
            poller_info = {"enabled": True, "error": "introspection-failed"}

    started_ms = getattr(bus, "_started_at_ms", None)
    uptime_s = (now_ms - started_ms) / 1000.0 if started_ms else None
    healthy = bool(recent) and (age_s is not None and age_s < 300)
    return web.json_response({
        "ok": healthy,
        "now_ms": now_ms,
        "uptime_s": uptime_s,
        "events_in_buffer": len(recent),
        "last_event_age_s": age_s,
        "by_venue": by_venue,
        "poller": poller_info,
        "mongo_available": bool(storage and storage.available),
    })


async def _handle_stats(request: web.Request) -> web.Response:
    bus: EventBus = request.app["bus"]
    return web.json_response(bus.stats())


async def _handle_config(request: web.Request) -> web.Response:
    storage: Optional[MongoStorage] = request.app.get("storage")
    return web.json_response({
        "threshold_usd": request.app["threshold_usd"],
        "venues": request.app["venues"],
        "coins": request.app.get("coins") or [],
        "mongo_available": bool(storage and storage.available),
        "actor_labels": list(analytics.ACTOR_LABELS),
    })


def _opt_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _opt_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError:
        return None


async def _handle_history(request: web.Request) -> web.Response:
    storage: Optional[MongoStorage] = request.app.get("storage")
    if storage is None or not storage.available:
        return web.json_response(
            {"error": "MongoDB persistence is not enabled or unreachable"},
            status=503,
        )
    q = request.query
    limit = max(1, min(1000, _opt_int(q.get("limit")) or 200))
    filters = {
        "venue": q.get("venue") or None,
        "kind": q.get("kind") or None,
        "coin": q.get("coin") or None,
        "side": q.get("side") or None,
        "tier": q.get("tier") or None,
        "trader": q.get("trader") or None,
        "min_notional": _opt_float(q.get("min_notional")),
        "min_score": _opt_float(q.get("min_score")),
        "before_ts": _opt_int(q.get("before_ts")),
        "after_ts": _opt_int(q.get("after_ts")),
    }
    rows = await storage.history(limit=limit, **filters)
    return web.json_response({
        "count": len(rows),
        "limit": limit,
        "events": rows,
    })


async def _handle_wallet_events(request: web.Request) -> web.Response:
    storage: Optional[MongoStorage] = request.app.get("storage")
    if storage is None or not storage.available:
        return web.json_response(
            {"error": "MongoDB persistence is not enabled or unreachable"},
            status=503,
        )
    venue = request.match_info["venue"]
    address = request.match_info["address"]
    try:
        limit = int(request.query.get("limit", "200"))
    except ValueError:
        limit = 200
    rows = await storage.wallet_history(venue, address, limit=limit)
    return web.json_response({
        "venue": venue,
        "trader": address,
        "count": len(rows),
        "events": rows,
    })


async def _handle_signals(request: web.Request) -> web.Response:
    """Aggregated trade signals: consensus clusters + per-coin scoreboard."""
    q = request.query
    window_ms = max(60_000, _opt_int(q.get("window_ms")) or 30 * 60_000)
    min_traders = max(2, _opt_int(q.get("min_traders")) or 2)
    min_tier_rank = _opt_int(q.get("min_tier_rank"))
    if min_tier_rank is None:
        min_tier_rank = analytics.TIER_RANK["TRUSTED"]
    actors = _parse_actors(q.get("actor"))
    events = await _fetch_events(request, limit=5000)
    consensus = analytics.consensus_signals(
        events,
        window_ms=window_ms,
        min_traders=min_traders,
        min_tier_rank=min_tier_rank,
        allowed_actors=actors,
    )
    coins = analytics.coin_scoreboard(events)
    return web.json_response({
        "consensus": consensus,
        "coins": coins,
        "windows_ms": list(analytics.DEFAULT_COIN_WINDOWS_MS),
        "params": {
            "window_ms": window_ms,
            "min_traders": min_traders,
            "min_tier_rank": min_tier_rank,
            "actor": sorted(actors) if actors else None,
        },
        "event_count": len(events),
    })


async def _handle_coins(request: web.Request) -> web.Response:
    """Just the coin scoreboard (faster than /api/signals when you only need the table)."""
    events = await _fetch_events(request, limit=5000)
    return web.json_response({
        "coins": analytics.coin_scoreboard(events),
        "windows_ms": list(analytics.DEFAULT_COIN_WINDOWS_MS),
    })


async def _handle_coin(request: web.Request) -> web.Response:
    """Per-coin deep view: time-series + top traders + recent events."""
    coin = request.match_info["coin"]
    q = request.query
    bin_ms = max(60_000, _opt_int(q.get("bin_ms")) or 5 * 60_000)
    window_ms = max(bin_ms, _opt_int(q.get("window_ms")) or 6 * 60 * 60_000)
    events = await _fetch_events(request, limit=5000)
    series = analytics.flow_timeseries(
        events, coin=coin, bin_ms=bin_ms, window_ms=window_ms,
    )
    top = analytics.coin_top_traders(events, coin=coin, window_ms=window_ms)
    coin_events = sorted(
        (e for e in events if e.get("coin") == coin),
        key=lambda e: -e.get("ts_ms", 0),
    )
    scoreboard = analytics.coin_scoreboard(events)
    coin_summary = next((c for c in scoreboard if c["coin"] == coin), None)
    return web.json_response({
        "coin": coin,
        "summary": coin_summary,
        "flow_series": series,
        "top_traders": top,
        "recent_events": coin_events[:50],
        "window_ms": window_ms,
        "bin_ms": bin_ms,
    })


async def _handle_live_positions(request: web.Request) -> web.Response:
    """Real-time wallet positions from the active poller (HL only for now).

    Unlike `/api/positions` (event-history reconstruction), these are the
    actual positions reported by Hyperliquid as of the last poll cycle —
    so they include sub-threshold rebalances, pre-startup positions, etc.
    """
    poller = request.app.get("poller")
    if poller is None:
        return web.json_response({
            "rows": [], "wallets": [], "changes": [], "enabled": False,
            "note": "Active polling is disabled (POLL_ENABLED=false).",
        })
    q = request.query
    min_notional = _opt_float(q.get("min_notional_usd")) or 0.0
    positions = poller.latest_positions(min_notional_usd=min_notional)

    rows = [
        {
            "venue": p.venue, "wallet": p.wallet, "coin": p.coin,
            "side": p.side, "size": p.size,
            "entry_px": p.entry_px, "mark_px": p.mark_px,
            "notional_usd": p.notional_usd,
            "unrealized_pnl_usd": p.unrealized_pnl_usd,
            "leverage": p.leverage, "liq_px": p.liq_px,
            "margin_used_usd": p.margin_used_usd,
            "fetched_at_ms": p.fetched_at_ms,
        }
        for p in positions
    ]
    wallets = [
        {
            "venue": s.venue, "wallet": s.wallet,
            "fetched_at_ms": s.fetched_at_ms,
            "account_value_usd": s.account_value_usd,
            "margin_used_usd": s.margin_used_usd,
            "n_positions": len(s.positions),
        }
        for s in poller.latest_snapshots()
    ]
    return web.json_response({
        "rows": rows,
        "wallets": wallets,
        "changes": poller.recent_changes(limit=50),
        "interval_sec": poller.interval_sec,
        "watchlist_size": len(poller.watchlist),
        "max_wallets": poller.max_wallets,
        "enabled": True,
    })


async def _handle_smart_money(request: web.Request) -> web.Response:
    """Per-coin consensus among ≥TRUSTED wallets — 'where are pros positioned now?'"""
    q = request.query
    window_min = _opt_float(q.get("window_min"))
    if window_min is None or window_min <= 0:
        window_min = 240.0  # 4 hours
    window_ms = int(window_min * 60_000)
    min_traders = max(2, _opt_int(q.get("min_traders")) or 2)
    min_tier_rank = _opt_int(q.get("min_tier_rank"))
    if min_tier_rank is None:
        min_tier_rank = analytics.TIER_RANK["TRUSTED"]
    actors = _parse_actors(q.get("actor"))
    events = await _fetch_events(request, limit=5000)
    rows = analytics.smart_money_positioning(
        events,
        window_ms=window_ms,
        min_tier_rank=min_tier_rank,
        min_traders=min_traders,
        allowed_actors=actors,
    )
    return web.json_response({
        "rows": rows,
        "window_min": window_min,
        "min_traders": min_traders,
        "min_tier_rank": min_tier_rank,
    })


async def _handle_positions(request: web.Request) -> web.Response:
    """Best-effort 'currently open' positions across high-score wallets."""
    q = request.query
    min_score = _opt_float(q.get("min_score"))
    if min_score is None:
        min_score = 60.0
    max_age_ms = _opt_int(q.get("max_age_ms")) or 24 * 60 * 60_000
    limit = max(1, min(200, _opt_int(q.get("limit")) or 50))
    actors = _parse_actors(q.get("actor"))
    events = await _fetch_events(request, limit=5000)
    rows = analytics.smart_positions(
        events,
        min_score=min_score, max_age_ms=max_age_ms, limit=limit,
        allowed_actors=actors,
    )
    return web.json_response({
        "rows": rows,
        "min_score": min_score,
        "max_age_ms": max_age_ms,
        "actor": sorted(actors) if actors else None,
        "event_count": len(events),
    })


async def _handle_leaderboard(request: web.Request) -> web.Response:
    bus: EventBus = request.app["bus"]
    try:
        limit = int(request.query.get("limit", "25"))
    except ValueError:
        limit = 25
    limit = max(1, min(100, limit))
    return web.json_response(bus.leaderboard(limit=limit))


async def _handle_wallet(request: web.Request) -> web.Response:
    venue = request.match_info["venue"]
    address = request.match_info["address"]
    tr: Optional[TrackRecord] = request.app.get("track_record")
    if tr is None:
        return web.json_response({"error": "track record service not configured"}, status=503)

    timeout = 12.0 if venue == "JupiterPerps" else 8.0
    stats = await tr.get(venue, address, timeout=timeout)
    if stats is None:
        return web.json_response(
            {"error": "could not fetch track record (timeout or upstream error)"},
            status=504,
        )
    return web.json_response(serialize_stats(stats, include_history=True))


async def _handle_cex_flows(request: web.Request) -> web.Response:
    """Windowed CEX flow summary used by the dashboard's CEX panel.

    Query:
      window_min  rolling window in minutes (default 60).
      top_assets  cap on per-asset rows (default 8).
      top_exchanges cap on per-exchange rows (default 8).
      top_transfers cap on largest single transfers (default 10).
    """
    analyzer: Optional[CexFlowAnalyzer] = request.app.get("cex_analyzer")
    if analyzer is None:
        return web.json_response(
            {"error": "CEX flow tracker disabled (set CEX_FLOWS_ENABLED=true)"},
            status=503,
        )
    q = request.query
    try:
        window_min = max(1, min(1440, int(q.get("window_min", "60"))))
    except (TypeError, ValueError):
        window_min = 60
    try:
        top_assets = max(1, min(50, int(q.get("top_assets", "8"))))
    except (TypeError, ValueError):
        top_assets = 8
    try:
        top_exchanges = max(1, min(50, int(q.get("top_exchanges", "8"))))
    except (TypeError, ValueError):
        top_exchanges = 8
    try:
        top_transfers = max(1, min(100, int(q.get("top_transfers", "10"))))
    except (TypeError, ValueError):
        top_transfers = 10
    return web.json_response(analyzer.summary(
        window_min=window_min,
        top_assets=top_assets,
        top_exchanges=top_exchanges,
        top_transfers=top_transfers,
    ))


async def _handle_cex_flows_recent(request: web.Request) -> web.Response:
    """Most recent flow events (for the live tape)."""
    analyzer: Optional[CexFlowAnalyzer] = request.app.get("cex_analyzer")
    if analyzer is None:
        return web.json_response({"events": [], "available": False})
    try:
        limit = max(1, min(500, int(request.query.get("limit", "100"))))
    except (TypeError, ValueError):
        limit = 100
    return web.json_response({
        "events": analyzer.recent(limit=limit),
        "available": True,
        "started_at_ms": analyzer.started_at_ms,
    })


async def _handle_cex_flows_series(request: web.Request) -> web.Response:
    """Time-bucketed series for sparklines."""
    analyzer: Optional[CexFlowAnalyzer] = request.app.get("cex_analyzer")
    if analyzer is None:
        return web.json_response({"buckets": [], "available": False})
    q = request.query
    try:
        window_min = max(5, min(1440, int(q.get("window_min", "60"))))
    except (TypeError, ValueError):
        window_min = 60
    try:
        bucket_min = max(1, min(60, int(q.get("bucket_min", "5"))))
    except (TypeError, ValueError):
        bucket_min = 5
    return web.json_response({
        "buckets": analyzer.buckets(
            window_min=window_min,
            bucket_min=bucket_min,
        ),
        "available": True,
        "window_min": window_min,
        "bucket_min": bucket_min,
        "valid_windows": list(WINDOWS_MIN.keys()),
    })


async def _handle_sse(request: web.Request) -> web.StreamResponse:
    bus: EventBus = request.app["bus"]
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )
    await response.prepare(request)

    queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=512)
    bus.subscribe(queue)
    log.info("SSE client connected (%s subs)", len(bus._subs))

    try:
        # Replay the ring buffer so a fresh tab isn't empty.
        for ev in bus.recent():
            await response.write(_sse_payload(ev))

        while True:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=15.0)
                await response.write(_sse_payload(ev))
            except asyncio.TimeoutError:
                # Heartbeat keeps proxies + browsers from closing idle connections.
                await response.write(b": heartbeat\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception:
        log.exception("SSE stream error")
    finally:
        bus.unsubscribe(queue)
        log.info("SSE client disconnected")
    return response


def _sse_payload(obj: dict) -> bytes:
    return f"data: {json.dumps(obj, separators=(',', ':'))}\n\n".encode()


async def serve(
    bus: EventBus,
    *,
    host: str,
    port: int,
    threshold_usd: float,
    venues: list[str],
    coins: Optional[list[str]] = None,
    track_record: Optional[TrackRecord] = None,
    storage: Optional[MongoStorage] = None,
    poller: Optional[object] = None,
    cex_analyzer: Optional[CexFlowAnalyzer] = None,
) -> Callable[[], Awaitable[None]]:
    """Start the server. Returns an async cleanup callable."""
    app = build_app(
        bus,
        threshold_usd=threshold_usd, venues=venues, coins=coins,
        track_record=track_record, storage=storage,
        poller=poller, cex_analyzer=cex_analyzer,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("dashboard live at http://%s:%d", host if host != "0.0.0.0" else "localhost", port)

    async def cleanup() -> None:
        await runner.cleanup()

    return cleanup
