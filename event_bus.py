"""In-process pub/sub for whale events.

The bot publishes every alert here. Any number of subscribers (notifier,
web server SSE handlers) can attach. New subscribers get the recent ring
buffer immediately so a freshly-opened browser tab doesn't show an empty
feed.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable, Optional

from analytics import classify_actor
from trackers import EventKind, Side, WhaleEvent


def serialize_stats(stats: Any, *, include_history: bool = False) -> dict[str, Any]:
    """Convert a WalletStats dataclass into a JSON-safe dict.

    `include_history=False` returns the lightweight view used in the live feed
    (no recent_trades / by_coin lists). The full view (modal / leaderboard
    detail) sets it to True.
    """
    s = asdict(stats)
    s["winrate"] = stats.winrate
    s["days_active"] = stats.days_active
    s["profit_factor"] = stats.profit_factor
    s["avg_win_usd"] = stats.avg_win_usd
    s["avg_loss_usd"] = stats.avg_loss_usd
    s["score"] = stats.score
    tier = stats.tier()
    s["tier"] = tier.label
    s["tier_emoji"] = tier.emoji
    actor, tpd = classify_actor(stats)
    s["actor"] = actor
    s["trades_per_day"] = tpd
    if not include_history:
        s.pop("recent_trades", None)
        s.pop("by_coin", None)
    return s


def serialize_event(ev: WhaleEvent) -> dict[str, Any]:
    """Convert a WhaleEvent (and any embedded WalletStats) into JSON-safe dict."""
    stats = ev.extra.get("track_record") if isinstance(ev.extra, dict) else None
    out: dict[str, Any] = {
        "venue": ev.venue,
        "kind": ev.kind.value,
        "coin": ev.coin,
        "side": ev.side.value,
        "notional_usd": ev.notional_usd,
        "price_usd": ev.price_usd,
        "size": ev.size,
        "trader": ev.trader,
        "tx_ref": ev.tx_ref,
        "ts_ms": ev.ts_ms,
        "pnl_usd": ev.pnl_usd,
        "leverage": ev.leverage,
    }
    if stats is not None and is_dataclass(stats):
        out["track_record"] = serialize_stats(stats, include_history=False)
    return out


class EventBus:
    def __init__(self, recent_size: int = 500) -> None:
        self._recent: deque[dict[str, Any]] = deque(maxlen=recent_size)
        self._subs: set[asyncio.Queue[dict[str, Any]]] = set()
        self._started_at_ms: int = int(time.time() * 1000)

    @property
    def started_at_ms(self) -> int:
        return self._started_at_ms

    def recent(self) -> list[dict[str, Any]]:
        return list(self._recent)

    def subscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subs.add(queue)

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subs.discard(queue)

    def publish(self, ev: WhaleEvent) -> None:
        payload = serialize_event(ev)
        self._recent.append(payload)
        for q in list(self._subs):
            # Non-blocking: drop messages on slow consumers rather than stall.
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    # ── Aggregates used by the stat cards ────────────────────────────────────
    def stats(self) -> dict[str, Any]:
        events = self.recent()
        total = len(events)
        notional_total = sum(e["notional_usd"] for e in events)
        longs = sum(1 for e in events if e["side"] == Side.LONG.value)
        shorts = sum(1 for e in events if e["side"] == Side.SHORT.value)
        liqs = sum(1 for e in events if e["kind"] == EventKind.LIQUIDATION.value)
        opens = sum(1 for e in events if e["kind"] == EventKind.OPEN.value)
        closes = sum(1 for e in events if e["kind"] == EventKind.CLOSE.value)
        by_venue: dict[str, int] = {}
        for e in events:
            by_venue[e["venue"]] = by_venue.get(e["venue"], 0) + 1
        return {
            "started_at_ms": self._started_at_ms,
            "total": total,
            "notional_total_usd": notional_total,
            "long_count": longs,
            "short_count": shorts,
            "liquidation_count": liqs,
            "open_count": opens,
            "close_count": closes,
            "by_venue": by_venue,
        }

    def leaderboard(self, limit: int = 25) -> list[dict[str, Any]]:
        """Top traders seen in this session, ranked by their composite score.

        We dedupe by (venue, trader) and keep the most recent enrichment we
        have for each. Wallets without a track_record (or without enough
        samples to score) are excluded.
        """
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        agg: dict[tuple[str, str], dict[str, Any]] = {}
        for e in self.recent():
            key = (e["venue"], e["trader"])
            tr = e.get("track_record")
            if not tr or tr.get("score") is None:
                continue
            latest[key] = e
            agg_entry = agg.get(key) or {
                "venue": e["venue"],
                "trader": e["trader"],
                "events_seen": 0,
                "notional_seen_usd": 0.0,
                "last_ts_ms": 0,
            }
            agg_entry["events_seen"] += 1
            agg_entry["notional_seen_usd"] += e["notional_usd"] or 0.0
            agg_entry["last_ts_ms"] = max(agg_entry["last_ts_ms"], e["ts_ms"])
            agg[key] = agg_entry

        rows: list[dict[str, Any]] = []
        for key, e in latest.items():
            tr = e["track_record"]
            row = {
                "venue": e["venue"],
                "trader": e["trader"],
                "score": tr.get("score") or 0.0,
                "tier": tr.get("tier"),
                "tier_emoji": tr.get("tier_emoji"),
                "winrate": tr.get("winrate"),
                "closed_trades": tr.get("closed_trades"),
                "realized_pnl_usd": tr.get("realized_pnl_usd"),
                "pnl_30d_usd": tr.get("pnl_30d_usd"),
                "events_seen": agg[key]["events_seen"],
                "notional_seen_usd": agg[key]["notional_seen_usd"],
                "last_ts_ms": agg[key]["last_ts_ms"],
            }
            rows.append(row)
        rows.sort(key=lambda r: r["score"], reverse=True)
        return rows[:limit]
