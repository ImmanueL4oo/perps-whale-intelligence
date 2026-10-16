"""Hyperliquid whale tracker.

Strategy
--------
1. Pull `meta` once for the universe so we know which markets exist.
2. Subscribe to the public `trades` WS feed for a configurable list of coins
   (defaults: BTC, ETH, SOL plus a few high-volume HIP-2 coins).
3. For every fill where px * sz >= threshold, classify open vs close vs liq:
    - Inspect the *taker* (trade.users[0]). The taker drove the print, so its
      `clearinghouseState` after the fill reflects the new reality.
    - For the same coin, if |new_size| >= |size_traded| - 1e-9 and the sign
      matches the trade's direction, the taker just OPENED / INCREASED.
    - Otherwise it CLOSED / REDUCED (or flipped) — emit CLOSE.
4. Liquidation detection: query `userFills` for the taker, find the matching
   `tid`, and check `liquidation` field. (Hyperliquid public trades feed does
   not flag liquidations directly.)

The classification is best-effort by design. The notional and side are always
correct; "OPEN vs CLOSE" is the only heuristic and is documented as such.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import AsyncIterator, Optional

import aiohttp
import websockets
from websockets.client import WebSocketClientProtocol

from ssl_ctx import get_ssl_context
from trackers import EventKind, Side, WhaleEvent

log = logging.getLogger("hyperliquid")

DEFAULT_COINS = ["BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE", "SUI", "kPEPE"]
PING_INTERVAL_S = 20.0


class HyperliquidTracker:
    def __init__(
        self,
        *,
        threshold_usd: float,
        coins: Optional[list[str]] = None,
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
        info_url: str = "https://api.hyperliquid.xyz/info",
        drop_unpriced: bool = True,
    ) -> None:
        self.threshold_usd = threshold_usd
        self.coins = coins or DEFAULT_COINS
        self.ws_url = ws_url
        self.info_url = info_url
        self.drop_unpriced = drop_unpriced

        self._session: Optional[aiohttp.ClientSession] = None
        # Cache of recent userFills per address to detect liquidations without
        # hammering /info on every whale.
        self._fills_cache: dict[str, tuple[float, list[dict]]] = {}
        # Per-(taker, coin) clearinghouseState cache. When the same wallet
        # prints multiple sub-trades on the same coin within ~1.5 seconds
        # (very common during burst execution), we'd otherwise fire a
        # `clearinghouseState` request *per print*. The HL info endpoint
        # rate-limits us aggressively; this collapses the burst into one.
        self._classify_cache: dict[tuple[str, str], tuple[float, dict]] = {}
        self._classify_cache_ttl_s: float = 1.5
        # Diagnostic counters surfaced by the heartbeat.
        self._classify_calls_saved: int = 0

    # ── public API ────────────────────────────────────────────────────────────
    async def stream(self) -> AsyncIterator[WhaleEvent]:
        connector = aiohttp.TCPConnector(ssl=get_ssl_context())
        async with aiohttp.ClientSession(connector=connector) as session:
            self._session = session
            await self._validate_coins()

            backoff = 1.0
            while True:
                try:
                    async for ev in self._run_once():
                        yield ev
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("WS loop crashed: %s — reconnecting in %.1fs", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)

    # ── ws plumbing ───────────────────────────────────────────────────────────
    async def _run_once(self) -> AsyncIterator[WhaleEvent]:
        log.info("connecting %s (coins=%s)", self.ws_url, ",".join(self.coins))
        async with websockets.connect(
            self.ws_url,
            ping_interval=None,  # we send our own app-level ping
            max_size=2**22,
            close_timeout=5,
            ssl=get_ssl_context(),
        ) as ws:
            for coin in self.coins:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": coin},
                }))

            ping_task = asyncio.create_task(self._pinger(ws))
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    channel = msg.get("channel")
                    if channel != "trades":
                        # subscriptionResponse / pong / etc.
                        continue

                    trades = msg.get("data") or []
                    for t in trades:
                        ev = await self._maybe_event(t)
                        if ev is not None:
                            yield ev
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _pinger(self, ws: WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                await ws.send(json.dumps({"method": "ping"}))
            except Exception:
                return

    # ── per-trade pipeline ────────────────────────────────────────────────────
    async def _maybe_event(self, t: dict) -> Optional[WhaleEvent]:
        try:
            coin = t["coin"]
            px = float(t["px"])
            sz = float(t["sz"])
            side_letter = t["side"]            # "B" (taker bought) / "A" (taker sold)
            users = t.get("users") or []
            tid = t.get("tid")
            time_ms = int(t.get("time") or time.time() * 1000)
        except (KeyError, ValueError, TypeError):
            return None

        if px <= 0 or sz <= 0:
            return None
        notional = px * sz
        if notional < self.threshold_usd:
            return None

        # `users` is [buyer, seller]. The taker side ("B" = bought, "A" = sold)
        # tells us which one initiated the print.
        side = Side.LONG if side_letter == "B" else Side.SHORT
        taker_idx = 0 if side_letter == "B" else 1
        if len(users) <= taker_idx or not users[taker_idx]:
            if self.drop_unpriced:
                return None
            taker = "?"
        else:
            taker = users[taker_idx]
        kind = EventKind.OPEN  # default; refine below

        leverage: Optional[float] = None
        try:
            kind, leverage = await self._classify(taker, coin, side, sz)
        except Exception as exc:
            log.debug("classify failed for %s: %s", taker, exc)

        # Liquidation override
        if taker != "?":
            try:
                if await self._is_liquidation(taker, tid):
                    kind = EventKind.LIQUIDATION
            except Exception as exc:
                log.debug("liq check failed for %s: %s", taker, exc)

        return WhaleEvent(
            venue="Hyperliquid",
            kind=kind,
            coin=coin,
            side=side,
            notional_usd=notional,
            price_usd=px,
            size=sz,
            trader=taker,
            tx_ref=str(tid),
            ts_ms=time_ms,
            leverage=leverage,
            extra={"hash": t.get("hash")},
        )

    async def _classify(
        self, taker: str, coin: str, taker_side: Side, traded_size: float,
    ) -> tuple[EventKind, Optional[float]]:
        """Classify by inspecting the taker's clearinghouse state post-fill.

        Burst-aware: if we've fetched this wallet's state in the last
        `_classify_cache_ttl_s` seconds we reuse it. This dedupes the very
        common case of a single execution algorithm slicing one $5M order
        into 10 sub-prints, all in the same second. Trade-off: classification
        of the *very* first sub-print after a flip lags by up to TTL — a
        worthwhile concession to not get rate-limited by HL.
        """
        if taker == "?":
            return EventKind.OPEN, None
        ck = (taker, coin)
        now_s = time.time()
        cached = self._classify_cache.get(ck)
        if cached and (now_s - cached[0]) <= self._classify_cache_ttl_s:
            self._classify_calls_saved += 1
            state = cached[1]
        else:
            state = await self._info({"type": "clearinghouseState", "user": taker})
            self._classify_cache[ck] = (now_s, state)
            # Bound the cache so an event-storm wallet doesn't grow it forever.
            if len(self._classify_cache) > 4096:
                # Drop the oldest 25% in one sweep — cheap, infrequent.
                cutoff = now_s - self._classify_cache_ttl_s
                self._classify_cache = {
                    k: v for k, v in self._classify_cache.items() if v[0] >= cutoff
                }
        positions = state.get("assetPositions") or []
        # pick this coin's position
        for ap in positions:
            pos = ap.get("position") or {}
            if pos.get("coin") != coin:
                continue
            try:
                szi = float(pos.get("szi") or 0.0)         # signed size, base units
            except ValueError:
                szi = 0.0
            try:
                lev_obj = pos.get("leverage") or {}
                lev = float(lev_obj.get("value")) if lev_obj.get("value") is not None else None
            except (TypeError, ValueError):
                lev = None

            curr_side = Side.LONG if szi > 0 else Side.SHORT if szi < 0 else None
            abs_size = abs(szi)
            # If their net position is in the same direction as the taker side
            # and is at least as large as the trade, this trade INCREASED them.
            if curr_side == taker_side and abs_size + 1e-9 >= traded_size:
                return EventKind.OPEN, lev
            # Otherwise this trade reduced/flipped → CLOSE.
            return EventKind.CLOSE, lev

        # No open position in this coin → they fully closed (or never had one).
        # If they had no position before, they wouldn't have generated a print
        # of this size from the close side. So treat as CLOSE.
        return EventKind.CLOSE, None

    async def _is_liquidation(self, user: str, tid) -> bool:
        if tid is None:
            return False
        cache = self._fills_cache.get(user)
        now = time.time()
        if not cache or now - cache[0] > 10.0:
            fills = await self._info({"type": "userFills", "user": user})
            self._fills_cache[user] = (now, fills if isinstance(fills, list) else [])
            cache = self._fills_cache[user]
        for f in cache[1]:
            if f.get("tid") == tid:
                return bool(f.get("liquidation"))
        return False

    # ── info endpoint helpers ─────────────────────────────────────────────────
    async def _info(self, payload: dict, timeout: float = 8.0):
        assert self._session is not None
        async with self._session.post(self.info_url, json=payload, timeout=timeout) as r:
            r.raise_for_status()
            return await r.json()

    async def _validate_coins(self) -> None:
        try:
            meta = await self._info({"type": "meta"})
            universe = {u["name"] for u in meta.get("universe", []) if "name" in u}
            missing = [c for c in self.coins if c not in universe]
            if missing:
                log.warning("dropping unknown HL coins: %s", missing)
                self.coins = [c for c in self.coins if c in universe]
            if not self.coins:
                # Fall back to top default that exists.
                self.coins = [c for c in ["BTC", "ETH", "SOL"] if c in universe]
            log.info("HL subscribing to %d coins", len(self.coins))
        except Exception as exc:
            log.warning("HL meta fetch failed (%s) — continuing with configured coins", exc)
