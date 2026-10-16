"""Stateful trading-signal helpers that sit between the EventBus and the
Notifier.

Two pieces:

* `ConvictionWatcher` — every time a new OPEN comes in, it scans the recent
  window for a `consensus_signals`-style burst. When a fresh burst is detected
  (subject to a per-(coin, side) cooldown), it pushes a single "🔥 CONVICTION"
  Telegram alert summarising who's in.

* `LifecycleTracker` — records every TRUSTED+ wallet's OPEN. When the same
  wallet later closes the same coin/side, it pushes an "🏁 EXIT" Telegram
  alert with realized PnL and time-to-close so you know to exit too if you
  copy-traded the entry.

Both are deliberately in-memory. They warm up from the bus's seeded history
on startup so a restart doesn't re-fire alerts you've already seen.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import analytics
from event_bus import EventBus

log = logging.getLogger(__name__)


# ── Conviction burst alerts ───────────────────────────────────────────────────
class ConvictionWatcher:
    """Detects bursts of agreement among ≥TRUSTED traders and pushes an alert.

    A "burst" = `min_traders` distinct ≥TRUSTED wallets all opening the same
    side on the same coin inside `window_ms`. After firing for a (coin, side),
    the watcher refuses to refire that pair for `cooldown_ms` to avoid spamming
    when traders pile in over a long stretch.
    """

    def __init__(
        self,
        bus: EventBus,
        send_telegram: Optional[Callable[[str], "asyncio.Future"]],
        *,
        min_traders: int = 3,
        window_ms: int = 30 * 60_000,
        cooldown_ms: int = 30 * 60_000,
    ) -> None:
        self.bus = bus
        self.send_telegram = send_telegram
        self.min_traders = min_traders
        self.window_ms = window_ms
        self.cooldown_ms = cooldown_ms
        # (coin, side) → ts_ms of last alert fired.
        self._last_fired: dict[tuple[str, str], int] = {}

    def warm_up(self) -> None:
        """Seed the cooldown map from any bursts already present in history,
        so a restart doesn't immediately re-alert on yesterday's clusters."""
        events = list(self.bus._recent)  # history snapshot
        if not events:
            return
        sigs = analytics.consensus_signals(
            events,
            window_ms=self.window_ms,
            min_traders=self.min_traders,
        )
        now = int(time.time() * 1000)
        for s in sigs:
            self._last_fired[(s["coin"], s["side"])] = now
        if sigs:
            log.info(
                "conviction watcher warmed up — %d historical burst(s) seeded",
                len(sigs),
            )

    async def check(self, ev: dict) -> None:
        """Called per new event. Cheap — only does work on OPEN events."""
        if ev.get("kind") != "OPEN":
            return
        if self.send_telegram is None:
            return
        events = list(self.bus._recent) + [ev]
        sigs = analytics.consensus_signals(
            events,
            window_ms=self.window_ms,
            min_traders=self.min_traders,
        )
        if not sigs:
            return
        now = ev.get("ts_ms") or int(time.time() * 1000)
        for s in sigs:
            key = (s["coin"], s["side"])
            last = self._last_fired.get(key, 0)
            if now - last < self.cooldown_ms:
                continue
            # Only fire if the latest event in the burst is the one we just
            # received — otherwise we'd refire on every subsequent OPEN that
            # doesn't change the cluster.
            if s["last_ts_ms"] != now:
                continue
            self._last_fired[key] = now
            try:
                await self.send_telegram(_format_conviction(s))
            except Exception as exc:
                log.warning("conviction telegram push failed: %s", exc)


def _fmt_usd(x: float) -> str:
    sign = "-" if x < 0 else ""
    a = abs(x)
    if a >= 1_000_000:
        return f"{sign}${a/1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a/1_000:.1f}K"
    return f"{sign}${a:,.0f}"


def _format_conviction(sig: dict) -> str:
    side_emoji = "📈" if sig["side"] == "long" else "📉"
    minutes = (sig["last_ts_ms"] - sig["first_ts_ms"]) / 60_000.0
    lines = [
        f"🔥 <b>CONVICTION SIGNAL</b>  {side_emoji} <b>{sig['side'].upper()}</b> <b>{sig['coin']}</b>",
        f"<b>{sig['trader_count']}</b> trusted traders agree  ·  "
        f"total <b>{_fmt_usd(sig['total_notional_usd'])}</b>  ·  "
        f"avg score <b>{sig['avg_score']:.0f}</b>  ·  spanned {minutes:.0f}m",
    ]
    for t in sig["traders"][:6]:
        emoji = t.get("tier_emoji") or ""
        addr = t["trader"]
        short = (addr[:6] + "…" + addr[-4:]) if len(addr) > 12 else addr
        lev = f"  ·  {t['leverage']:.0f}x" if t.get("leverage") else ""
        lines.append(
            f"   {emoji} <code>{short}</code>  "
            f"{_fmt_usd(t['notional_usd'])}  "
            f"<i>(score {t['score']:.0f})</i>{lev}"
        )
    if len(sig["traders"]) > 6:
        lines.append(f"   <i>… and {len(sig['traders']) - 6} more</i>")
    return "\n".join(lines)


# ── Position lifecycle (entry → exit pairing) ─────────────────────────────────
@dataclass
class _OpenPosition:
    venue: str
    trader: str
    coin: str
    side: str
    notional_usd: float
    price_usd: float
    leverage: Optional[float]
    score: float
    tier: str
    opened_at_ms: int
    notif_short: str  # short trader label, cached for the exit message


class LifecycleTracker:
    """Pairs OPENs and CLOSE/LIQs from the same wallet+coin so you get
    notified when your copy-trade should exit.

    Only TRUSTED+ wallets are tracked — the goal is "I followed this entry,
    tell me when they leave", not "log every position the bot ever sees".
    """

    def __init__(
        self,
        send_telegram: Optional[Callable[[str], "asyncio.Future"]],
        *,
        min_tier_rank: int = analytics.TIER_RANK["TRUSTED"],
        max_age_ms: int = 7 * 24 * 60 * 60_000,
    ) -> None:
        self.send_telegram = send_telegram
        self.min_tier_rank = min_tier_rank
        self.max_age_ms = max_age_ms
        # Each (venue, trader, coin, side) maps to a stack of opens.
        self._opens: dict[tuple[str, str, str, str], list[_OpenPosition]] = {}

    def warm_up(self, events: list[dict]) -> None:
        """Replay history so positions opened before restart are still tracked
        (and get an EXIT alert when they eventually close)."""
        for ev in sorted(events, key=lambda e: e.get("ts_ms", 0)):
            self._ingest(ev, push=False)

    async def check(self, ev: dict) -> None:
        msg = self._ingest(ev, push=True)
        if msg and self.send_telegram is not None:
            try:
                await self.send_telegram(msg)
            except Exception as exc:
                log.warning("lifecycle telegram push failed: %s", exc)

    # ── internals ────────────────────────────────────────────────────────────
    def _trader_short(self, addr: str) -> str:
        if not addr or len(addr) <= 12:
            return addr
        return f"{addr[:6]}…{addr[-4:]}"

    def _ingest(self, ev: dict, *, push: bool) -> Optional[str]:
        kind = ev.get("kind")
        if kind not in ("OPEN", "CLOSE", "LIQ"):
            return None
        tier = (ev.get("track_record") or {}).get("tier") or "NEW"
        if analytics.TIER_RANK.get(tier, 0) < self.min_tier_rank:
            return None
        venue = ev.get("venue", "")
        trader = ev.get("trader", "")
        coin = ev.get("coin", "")
        side = ev.get("side", "")
        if not (venue and trader and coin and side):
            return None
        key = (venue, trader, coin, side)
        ts = ev.get("ts_ms") or int(time.time() * 1000)

        if kind == "OPEN":
            pos = _OpenPosition(
                venue=venue, trader=trader, coin=coin, side=side,
                notional_usd=float(ev.get("notional_usd") or 0.0),
                price_usd=float(ev.get("price_usd") or 0.0),
                leverage=ev.get("leverage"),
                score=float((ev.get("track_record") or {}).get("score") or 0.0),
                tier=tier,
                opened_at_ms=ts,
                notif_short=self._trader_short(trader),
            )
            self._opens.setdefault(key, []).append(pos)
            return None

        # CLOSE / LIQ — flush all matching opens, oldest first.
        opens = self._opens.get(key) or []
        if not opens:
            return None
        # Drop stale opens older than max_age_ms before pairing.
        cutoff = ts - self.max_age_ms
        opens = [o for o in opens if o.opened_at_ms >= cutoff]
        if not opens:
            self._opens[key] = []
            return None

        pos = opens[0]
        # On HL one position per (coin, side) — clearing all is correct.
        # On Jupiter / multi-lot venues we may over-clear, but the dashboard
        # already documents this caveat in `smart_positions`.
        self._opens[key] = []

        held_min = (ts - pos.opened_at_ms) / 60_000.0
        pnl = ev.get("pnl_usd")
        emoji_kind = "🏁" if kind == "CLOSE" else "💥"
        side_emoji = "📈" if side == "long" else "📉"
        lines = [
            f"{emoji_kind} <b>EXIT — {kind}</b>  {side_emoji} <b>{side.upper()}</b> <b>{coin}</b>",
            f"👤 <code>{pos.notif_short}</code>  ·  tier <b>{pos.tier}</b>  ·  score <b>{pos.score:.0f}</b>",
            f"⏱️ Held: <b>{held_min:.0f} min</b>  ·  Entry: <code>${pos.price_usd:,.4f}</code>"
            f"  ·  Exit: <code>${float(ev.get('price_usd') or 0):,.4f}</code>",
        ]
        if pnl is not None:
            sign = "✅" if pnl >= 0 else "❌"
            lines.append(f"{sign} Realized PnL: <b>{_fmt_usd(float(pnl))}</b>")
        lines.append(
            f"💵 Open notional: <b>{_fmt_usd(pos.notional_usd)}</b>  ·  "
            f"Close notional: <b>{_fmt_usd(float(ev.get('notional_usd') or 0))}</b>"
        )
        return "\n".join(lines) if push else None
