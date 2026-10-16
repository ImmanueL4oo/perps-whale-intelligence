"""Active position polling for Hyperliquid wallets.

The event-driven side of the bot tells us *when* a whale-sized trade fires.
This module fills in the gaps between trades by periodically asking
Hyperliquid "what is wallet X actually holding right now?".

What it gives you that the event stream can't:

  * Positions opened **before** the bot started running.
  * Sub-threshold rebalances (a TRUSTED trader scaling into a position
    with a series of $40K trades that individually never trip our whale
    filter, but cumulatively shift their book).
  * The wallet's actual current entry / unrealized PnL / liquidation
    price / margin — none of which is available from the trade tape.
  * A "this wallet just went FLAT" signal we can act on, even when the
    closing trade itself was below the whale threshold.

Scope:
  * Hyperliquid only for now. Jupiter requires deriving 2N position-PDAs
    per wallet and a `getMultipleAccounts` call — TODO.
  * Polls a working set = `watchlist` ∪ `traders seen in recent events
    with tier ≥ POLL_MIN_TIER_RANK`, capped at `max_wallets` to keep
    request volume bounded.
  * Diffs successive snapshots and publishes a `POSITION_UPDATE` event
    on the bus for each detected change (new lot / size delta / flat).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import aiohttp

import analytics
from event_bus import EventBus
from ssl_ctx import get_ssl_context

log = logging.getLogger(__name__)


# ── Snapshot model ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Position:
    """One open position for a wallet on a single coin."""
    venue: str
    wallet: str
    coin: str
    side: str            # "long" / "short"
    size: float          # base-asset size (always positive)
    entry_px: float
    mark_px: float
    notional_usd: float
    unrealized_pnl_usd: float
    leverage: Optional[float]
    liq_px: Optional[float]
    margin_used_usd: Optional[float]
    fetched_at_ms: int

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.venue, self.wallet, self.coin)


@dataclass
class WalletSnapshot:
    """Most recent state for a single wallet."""
    venue: str
    wallet: str
    fetched_at_ms: int
    positions: dict[str, Position] = field(default_factory=dict)  # by coin
    account_value_usd: float = 0.0
    margin_used_usd: float = 0.0


# ── HL fetch ──────────────────────────────────────────────────────────────────
async def fetch_hl_snapshot(
    session: aiohttp.ClientSession,
    info_url: str,
    wallet: str,
    *,
    timeout: float = 6.0,
) -> Optional[WalletSnapshot]:
    """One `clearinghouseState` call → WalletSnapshot. None on error."""
    try:
        async with session.post(
            info_url,
            json={"type": "clearinghouseState", "user": wallet},
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            data = await r.json()
    except Exception as exc:
        log.debug("HL clearinghouseState failed for %s: %s", wallet, exc)
        return None

    now = int(time.time() * 1000)
    snap = WalletSnapshot(venue="Hyperliquid", wallet=wallet, fetched_at_ms=now)
    summary = data.get("marginSummary") or {}
    try:
        snap.account_value_usd = float(summary.get("accountValue") or 0.0)
        snap.margin_used_usd = float(summary.get("totalMarginUsed") or 0.0)
    except (TypeError, ValueError):
        pass

    for ap in data.get("assetPositions") or []:
        pos = ap.get("position") or {}
        try:
            szi = float(pos.get("szi") or 0.0)  # signed
        except (TypeError, ValueError):
            continue
        if abs(szi) < 1e-9:
            continue
        coin = pos.get("coin") or ""
        if not coin:
            continue
        side = "long" if szi > 0 else "short"
        size = abs(szi)
        try:
            entry = float(pos.get("entryPx") or 0.0)
        except (TypeError, ValueError):
            entry = 0.0
        try:
            notional = float(pos.get("positionValue") or 0.0)
        except (TypeError, ValueError):
            notional = entry * size
        # mark price = positionValue / size (HL doesn't expose mark separately
        # in clearinghouseState — derive it).
        mark = notional / size if size > 0 else entry
        try:
            upnl = float(pos.get("unrealizedPnl") or 0.0)
        except (TypeError, ValueError):
            upnl = 0.0
        lev_obj = pos.get("leverage") or {}
        try:
            lev = float(lev_obj.get("value")) if lev_obj.get("value") is not None else None
        except (TypeError, ValueError):
            lev = None
        try:
            liq = float(pos.get("liquidationPx")) if pos.get("liquidationPx") else None
        except (TypeError, ValueError):
            liq = None
        try:
            margin = float(pos.get("marginUsed") or 0.0)
        except (TypeError, ValueError):
            margin = None

        snap.positions[coin] = Position(
            venue="Hyperliquid", wallet=wallet, coin=coin, side=side,
            size=size, entry_px=entry, mark_px=mark,
            notional_usd=notional, unrealized_pnl_usd=upnl,
            leverage=lev, liq_px=liq, margin_used_usd=margin,
            fetched_at_ms=now,
        )
    return snap


# ── Diff ──────────────────────────────────────────────────────────────────────
@dataclass
class PositionChange:
    kind: str            # "open" / "close" / "increase" / "decrease" / "flip"
    venue: str
    wallet: str
    coin: str
    side_now: Optional[str]
    side_prev: Optional[str]
    size_now: float
    size_prev: float
    notional_now_usd: float
    notional_prev_usd: float
    delta_notional_usd: float
    mark_px: float
    leverage: Optional[float]
    ts_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "venue": self.venue,
            "wallet": self.wallet,
            "coin": self.coin,
            "side_now": self.side_now,
            "side_prev": self.side_prev,
            "size_now": self.size_now,
            "size_prev": self.size_prev,
            "notional_now_usd": self.notional_now_usd,
            "notional_prev_usd": self.notional_prev_usd,
            "delta_notional_usd": self.delta_notional_usd,
            "mark_px": self.mark_px,
            "leverage": self.leverage,
            "ts_ms": self.ts_ms,
        }


def diff_snapshots(
    prev: Optional[WalletSnapshot],
    cur: WalletSnapshot,
    *,
    min_delta_usd: float = 1_000.0,
) -> list[PositionChange]:
    """Compare two snapshots of the same wallet, return material changes."""
    changes: list[PositionChange] = []
    prev_positions = (prev.positions if prev else {}) or {}
    cur_positions = cur.positions or {}
    coins = set(prev_positions) | set(cur_positions)

    for coin in coins:
        p = prev_positions.get(coin)
        c = cur_positions.get(coin)

        if p is None and c is not None:
            changes.append(PositionChange(
                kind="open",
                venue=cur.venue, wallet=cur.wallet, coin=coin,
                side_now=c.side, side_prev=None,
                size_now=c.size, size_prev=0.0,
                notional_now_usd=c.notional_usd, notional_prev_usd=0.0,
                delta_notional_usd=c.notional_usd,
                mark_px=c.mark_px, leverage=c.leverage,
                ts_ms=cur.fetched_at_ms,
            ))
            continue
        if p is not None and c is None:
            changes.append(PositionChange(
                kind="close",
                venue=cur.venue, wallet=cur.wallet, coin=coin,
                side_now=None, side_prev=p.side,
                size_now=0.0, size_prev=p.size,
                notional_now_usd=0.0, notional_prev_usd=p.notional_usd,
                delta_notional_usd=-p.notional_usd,
                mark_px=p.mark_px, leverage=p.leverage,
                ts_ms=cur.fetched_at_ms,
            ))
            continue
        # both sides present
        if p.side != c.side:
            changes.append(PositionChange(
                kind="flip",
                venue=cur.venue, wallet=cur.wallet, coin=coin,
                side_now=c.side, side_prev=p.side,
                size_now=c.size, size_prev=p.size,
                notional_now_usd=c.notional_usd, notional_prev_usd=p.notional_usd,
                delta_notional_usd=c.notional_usd + p.notional_usd,
                mark_px=c.mark_px, leverage=c.leverage,
                ts_ms=cur.fetched_at_ms,
            ))
            continue
        delta_n = c.notional_usd - p.notional_usd
        if abs(delta_n) < min_delta_usd:
            continue
        changes.append(PositionChange(
            kind="increase" if delta_n > 0 else "decrease",
            venue=cur.venue, wallet=cur.wallet, coin=coin,
            side_now=c.side, side_prev=p.side,
            size_now=c.size, size_prev=p.size,
            notional_now_usd=c.notional_usd, notional_prev_usd=p.notional_usd,
            delta_notional_usd=delta_n,
            mark_px=c.mark_px, leverage=c.leverage,
            ts_ms=cur.fetched_at_ms,
        ))
    return changes


# ── Poller ────────────────────────────────────────────────────────────────────
class PositionPoller:
    """Periodically polls clearinghouseState for a working set of wallets and
    publishes diffs to the EventBus.

    The working set is rebuilt before every cycle as:
        watchlist  ∪  (TRUSTED+ wallets seen in recent events)
    capped at `max_wallets`.

    Snapshots are kept in-memory (per (venue, wallet)). Public read APIs:
        .latest_snapshots()          — list[WalletSnapshot]
        .latest_positions(min_notional=...)  — flat list[Position]
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        hl_info_url: str = "https://api.hyperliquid.xyz/info",
        watchlist: Iterable[str] = (),
        interval_sec: float = 30.0,
        min_tier_rank: int = analytics.TIER_RANK["TRUSTED"],
        max_wallets: int = 40,
        min_change_usd: float = 1_000.0,
        request_concurrency: int = 6,
        recent_event_window_ms: int = 24 * 60 * 60_000,
    ) -> None:
        self.bus = bus
        self.hl_info_url = hl_info_url
        self.watchlist = {w.lower() for w in watchlist if w}
        self.interval_sec = max(5.0, float(interval_sec))
        self.min_tier_rank = min_tier_rank
        self.max_wallets = max(1, int(max_wallets))
        self.min_change_usd = float(min_change_usd)
        self.request_concurrency = max(1, int(request_concurrency))
        self.recent_event_window_ms = int(recent_event_window_ms)

        # (venue, wallet) → snapshot
        self.snapshots: dict[tuple[str, str], WalletSnapshot] = {}
        # (venue, wallet) → unix ts_ms of last successful fetch
        self.last_fetch_ms: dict[tuple[str, str], int] = {}
        # rolling list of changes for /api/live-positions feed
        self._recent_changes: list[dict[str, Any]] = []

        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._sem = asyncio.Semaphore(self.request_concurrency)

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def start(self) -> None:
        if self._task is not None:
            return
        connector = aiohttp.TCPConnector(ssl=get_ssl_context(), limit=self.request_concurrency * 2)
        self._session = aiohttp.ClientSession(connector=connector)
        self._task = asyncio.create_task(self._loop(), name="position-poller")
        log.info(
            "position poller started — interval=%.0fs, watchlist=%d, "
            "auto-promote ≥%s",
            self.interval_sec, len(self.watchlist),
            next((k for k, v in analytics.TIER_RANK.items() if v == self.min_tier_rank), "?"),
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ── snapshot accessors ───────────────────────────────────────────────────
    def latest_snapshots(self) -> list[WalletSnapshot]:
        return list(self.snapshots.values())

    def latest_positions(self, *, min_notional_usd: float = 0.0) -> list[Position]:
        out: list[Position] = []
        for snap in self.snapshots.values():
            for pos in snap.positions.values():
                if pos.notional_usd >= min_notional_usd:
                    out.append(pos)
        out.sort(key=lambda p: -p.notional_usd)
        return out

    def recent_changes(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self._recent_changes[-limit:][::-1]

    # ── working set ──────────────────────────────────────────────────────────
    def _build_working_set(self) -> list[str]:
        """Watchlist + TRUSTED+ wallets seen in recent events, deduplicated."""
        seen: dict[str, int] = {}  # lowered wallet → -last_score (for sorting)
        # Watchlist first — they always poll.
        for w in self.watchlist:
            seen[w] = -10_000

        cutoff = int(time.time() * 1000) - self.recent_event_window_ms
        for ev in list(self.bus._recent):
            if ev.get("ts_ms", 0) < cutoff:
                continue
            if ev.get("venue") != "Hyperliquid":
                continue  # HL only for now
            tier = (ev.get("track_record") or {}).get("tier") or "NEW"
            if analytics.TIER_RANK.get(tier, 0) < self.min_tier_rank:
                continue
            addr = (ev.get("trader") or "").lower()
            if not addr:
                continue
            score = float((ev.get("track_record") or {}).get("score") or 0.0)
            # Use the highest score we've seen for this address to prioritise.
            if addr not in seen or seen[addr] > -score:
                seen[addr] = -score

        # Sort: watchlist (-10000) first, then by score desc, cap to max_wallets.
        addrs = sorted(seen.keys(), key=lambda a: seen[a])
        return addrs[: self.max_wallets]

    # ── main loop ────────────────────────────────────────────────────────────
    async def _loop(self) -> None:
        # Small initial delay so the bus has had a chance to load seed events.
        await asyncio.sleep(2.0)
        while True:
            try:
                wallets = self._build_working_set()
                if wallets:
                    await self._poll_cycle(wallets)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("position poller cycle failed")
            try:
                await asyncio.sleep(self.interval_sec)
            except asyncio.CancelledError:
                raise

    async def _poll_cycle(self, wallets: list[str]) -> None:
        if self._session is None:
            return

        async def one(addr: str) -> tuple[str, Optional[WalletSnapshot]]:
            async with self._sem:
                snap = await fetch_hl_snapshot(
                    self._session, self.hl_info_url, addr,
                )
            return addr, snap

        cycle_start = time.time()
        results = await asyncio.gather(*[one(w) for w in wallets], return_exceptions=False)

        new_changes: list[PositionChange] = []
        for addr, snap in results:
            if snap is None:
                continue
            key = ("Hyperliquid", addr)
            prev = self.snapshots.get(key)
            self.snapshots[key] = snap
            self.last_fetch_ms[key] = snap.fetched_at_ms
            if prev is None:
                # First time seeing this wallet — no diff to publish, just store.
                continue
            new_changes.extend(
                diff_snapshots(prev, snap, min_delta_usd=self.min_change_usd)
            )

        for ch in new_changes:
            payload = self._publish(ch)
            self._recent_changes.append(payload)
        # Trim the in-memory change feed.
        if len(self._recent_changes) > 1000:
            self._recent_changes = self._recent_changes[-1000:]

        elapsed = time.time() - cycle_start
        if new_changes:
            log.info(
                "poll cycle: %d wallets, %d changes (%.0fms)",
                len(results), len(new_changes), elapsed * 1000,
            )
        else:
            log.debug(
                "poll cycle: %d wallets, no changes (%.0fms)",
                len(results), elapsed * 1000,
            )

    def _publish(self, ch: PositionChange) -> dict[str, Any]:
        """Push a synthetic event onto the bus so it appears in the SSE stream
        and the dashboard can react in real time. Marked with kind=POSITION_UPDATE
        so the existing whale-feed renderer can filter / style them."""
        payload = ch.to_dict()
        payload.update({
            "ts_ms": ch.ts_ms,
            "venue": ch.venue,
            "kind": "POSITION_UPDATE",
            "side": ch.side_now or ch.side_prev or "long",
            "coin": ch.coin,
            "trader": ch.wallet,
            "tx_ref": f"poll-{ch.wallet}-{ch.coin}-{ch.ts_ms}",
            "notional_usd": abs(ch.delta_notional_usd),
            "price_usd": ch.mark_px,
            "size": abs(ch.size_now - ch.size_prev),
            "leverage": ch.leverage,
            "is_position_update": True,
            "change_kind": ch.kind,
        })
        # Mirror what publish() does without requiring a WhaleEvent dataclass.
        try:
            self.bus._recent.append(payload)
            for q in list(self.bus._subs):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    pass
        except Exception:
            log.debug("bus broadcast failed", exc_info=True)
        return payload
