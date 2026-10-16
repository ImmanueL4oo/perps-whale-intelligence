"""Drift Protocol whale tracker (Solana's largest perps DEX by volume).

Strategy
--------
Drift's `dlob.drift.trade` WebSocket *does* exist but in practice fires
intermittently for whale-sized fills (heartbeats dominate). The protocol's
public **data REST API** (`data.api.drift.trade/market/<SYMBOL>/trades`) is
authoritative and returns a clean JSON record per fill:

```
GET https://data.api.drift.trade/market/SOL-PERP/trades?pageIndex=0&pageSize=20
```

The data API is unauthenticated and rate-limited per source IP. **Empirically
~1 request/second is the safe ceiling** — going hotter (we tried 3 RPS) gets
the IP shadow-banned with HTTP 403 for 5+ minutes. So this tracker:

* uses a single shared token-bucket limiter (default 1 RPS, configurable);
* polls each market in round-robin with the bucket gating order;
* on sustained 403/429 enters a global cool-off (60s default) before resuming;
* dedupes by `fillRecordId` (string-numeric, monotonically increasing).

Both taker and maker addresses are attributed to the trade — we use the
**taker** as the trader because that's the active flow side (for
liquidations we flip; see `_side_from_direction`).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

import aiohttp

from ssl_ctx import get_ssl_context
from trackers import EventKind, Side, WhaleEvent

log = logging.getLogger("drift")

DRIFT_DATA_API = "https://data.api.drift.trade"

# Default markets to track. Bias to the deepest perp markets so the tracker
# stays useful at a conservative ~1 RPS budget. Adding more is a one-line
# config change; the user can increase markets at the cost of refresh latency
# (each market gets polled every `markets * (1/rps)` seconds).
DEFAULT_MARKETS: tuple[str, ...] = (
    "SOL-PERP", "BTC-PERP", "ETH-PERP", "HYPE-PERP",
    "DOGE-PERP", "SUI-PERP", "1MPEPE-PERP", "TRUMP-PERP",
)

# Polite User-Agent so the API team can identify (and rate-shape) us if needed.
_USER_AGENT = "perps-whale-tracker/1.0 (+https://github.com/local)"

# How long to stay quiet after we see sustained 403/429. We restore the
# tracker after this; if it happens again backoff doubles (capped).
_BAN_COOLDOWN_BASE_SEC = 60.0
_BAN_COOLDOWN_MAX_SEC = 600.0


@dataclass
class _MarketState:
    """Per-market cursor."""
    last_fill_record_id: int = 0
    consecutive_errors: int = 0


class _TokenBucket:
    """Tiny async token-bucket. Ensures we don't exceed `rate_per_sec` RPS
    across all markets, irrespective of how many we add.
    """
    def __init__(self, rate_per_sec: float, burst: int = 2) -> None:
        self.rate = max(0.1, rate_per_sec)
        self.capacity = max(1, burst)
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            wait = (1.0 - self.tokens) / self.rate
        await asyncio.sleep(wait)
        # Re-acquire after sleep so we record the token spend.
        async with self._lock:
            self.tokens = max(0.0, self.tokens - 1.0)
            self.last = time.monotonic()


def _coin_from_symbol(symbol: str) -> str:
    """`SOL-PERP` → `SOL`, `1MPEPE-PERP` → `PEPE`."""
    base = symbol.split("-", 1)[0].upper()
    if base.startswith("1M"):
        # Drift uses `1M*` notation for size-scaled markets; the canonical
        # token name is the rest, which lines up with Hyperliquid's `kPEPE`
        # / `kBONK` style only loosely. We strip the `1M` so cross-venue
        # consensus pivots on the underlying coin.
        return base[2:]
    if base.startswith("1K"):
        return base[2:]
    return base


def _side_from_direction(direction: str, action: str) -> Side:
    """Maps Drift's taker direction (`long` / `short`) to our `Side` enum.

    For `liquidate*` actions we flip — the *liquidated* side is the user
    the liquidator was pressing, which is what observers care about.
    """
    direction = (direction or "").lower()
    if action.startswith("liquidate"):
        # liquidation taker is the bot; the "real" trader was the opposite
        return Side.SHORT if direction == "long" else Side.LONG
    return Side.LONG if direction == "long" else Side.SHORT


def _kind_from_action(action: str) -> EventKind:
    a = (action or "").lower()
    if "liquidat" in a:
        return EventKind.LIQUIDATION
    return EventKind.OPEN  # individual fills look like opens; lifecycle is downstream


def _parse_amount(raw) -> float:
    """Drift returns numeric strings — `"0.090000000"` etc. Be lenient."""
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


class DriftTracker:
    def __init__(
        self,
        *,
        threshold_usd: float,
        markets: Optional[list[str]] = None,
        coins: Optional[list[str]] = None,
        poll_interval_sec: float = 10.0,
        page_size: int = 50,
        data_api_url: str = DRIFT_DATA_API,
        session: Optional[aiohttp.ClientSession] = None,
        max_rps: float = 1.0,
    ) -> None:
        self.threshold_usd = threshold_usd
        self.poll_interval_sec = max(1.0, poll_interval_sec)
        self.page_size = page_size
        self.data_api_url = data_api_url.rstrip("/")
        self._owns_session = session is None
        self._session = session

        # Whitelist of base coins — same UX as the Jupiter tracker. If both
        # `markets` and `coins` are given, `coins` further narrows.
        self._markets = list(markets) if markets else list(DEFAULT_MARKETS)
        self._coin_filter: Optional[set[str]] = (
            {c.upper() for c in coins} if coins else None
        )
        self._state: dict[str, _MarketState] = {
            m: _MarketState() for m in self._markets
        }

        # Diagnostics surfaced to the heartbeat logger.
        self._stats = {
            "polls": 0,
            "fills_seen": 0,
            "emitted": 0,
            "errors": 0,
            "ban_events": 0,
        }
        # Global rate ceiling regardless of market count.
        self._bucket = _TokenBucket(rate_per_sec=max_rps, burst=max(2, int(max_rps * 2)))
        # Cooldown when the API blocks us with 403/429.
        self._cool_until: float = 0.0
        self._cool_seconds: float = _BAN_COOLDOWN_BASE_SEC
        # Bounded dedupe — if the API ever stutters and replays records we
        # don't double-emit. fillRecordId is a string in the API, but it's
        # numeric, so we compare integers.
        self._seen: set[str] = set()
        self._seen_order: list[str] = []
        self._seen_max = 4096

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=get_ssl_context(), limit=4),
                headers={
                    "User-Agent": _USER_AGENT,
                    "Accept": "application/json",
                },
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    async def stream(self) -> AsyncIterator[WhaleEvent]:
        """Round-robin poll across markets, yielding whale events as they appear.

        Pacing model:
          * Token bucket caps total RPS (default 1) regardless of market count.
          * Within each pass, markets are polled in order; the bucket gates
            individual calls so they're spaced out.
          * If the API blocks us (403/429), we enter a global cooldown and
            stop polling entirely until the window expires.
        """
        log.info(
            "Drift tracker starting — markets=%d, threshold=$%s, target=%.2f RPS, "
            "per-market refresh ≈ %.0fs",
            len(self._markets), f"{self.threshold_usd:,.0f}",
            self._bucket.rate, len(self._markets) / max(0.1, self._bucket.rate),
        )
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            while True:
                # Honour the global cooldown set by 403/429 handlers.
                now = time.time()
                if now < self._cool_until:
                    await asyncio.sleep(min(5.0, self._cool_until - now))
                    continue
                # One full sweep across all markets.
                for market in self._markets:
                    state = self._state[market]
                    fills = await self._poll_market(market, state)
                    if fills:
                        for ev in self._extract_events(market, fills):
                            self._stats["emitted"] += 1
                            yield ev
                    # If a poll set the cooldown, break early — the outer
                    # loop will respect it on the next iteration.
                    if time.time() < self._cool_until:
                        break
                # Tiny breath between sweeps so we don't busy-loop when all
                # markets are deduped.
                await asyncio.sleep(0.5)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass

    async def _poll_market(self, market: str, state: _MarketState) -> list[dict]:
        """Fetch one page of recent fills for `market`. Returns oldest→newest."""
        # Wait for a token before making the call — this is what enforces
        # the global RPS budget.
        await self._bucket.acquire()
        if time.time() < self._cool_until:
            return []  # cooldown set by another market's response
        session = await self._ensure_session()
        params = {"pageIndex": "0", "pageSize": str(self.page_size)}
        url = f"{self.data_api_url}/market/{market}/trades"
        self._stats["polls"] += 1
        try:
            async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                if r.status != 200:
                    state.consecutive_errors += 1
                    self._stats["errors"] += 1
                    # Trigger a global cooldown for the IP-level blocks. A
                    # single 403/429 from any market means *we* are too hot —
                    # all markets must back off, not just this one.
                    if r.status in (403, 429):
                        self._enter_cooldown(market, r.status)
                        return []
                    if state.consecutive_errors == 1 or state.consecutive_errors % 10 == 0:
                        log.warning(
                            "Drift %s GET → %s (errors=%d)",
                            market, r.status, state.consecutive_errors,
                        )
                    return []
                payload = await r.json()
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            state.consecutive_errors += 1
            self._stats["errors"] += 1
            log.debug("Drift %s network error: %s", market, exc)
            return []
        # Successful poll → reset error counter and (slowly) decay the
        # cooldown ceiling so transient bans don't permanently degrade us.
        state.consecutive_errors = 0
        if self._cool_seconds > _BAN_COOLDOWN_BASE_SEC:
            self._cool_seconds = max(
                _BAN_COOLDOWN_BASE_SEC, self._cool_seconds * 0.95,
            )
        records = payload.get("records") or []
        # API returns newest first; we want oldest→newest so cursor advances
        # monotonically and deltas don't crisscross.
        records.reverse()
        return records

    def _enter_cooldown(self, market: str, status: int) -> None:
        """Pause all polling for `_cool_seconds`, then resume. The cooldown
        doubles each time we hit a fresh ban (capped) so a misbehaving IP
        gets progressively quieter instead of slamming the API forever.
        """
        self._stats["ban_events"] += 1
        self._cool_until = time.time() + self._cool_seconds
        log.warning(
            "Drift %s → HTTP %d — pausing all market polling for %.0fs (ban events=%d)",
            market, status, self._cool_seconds, self._stats["ban_events"],
        )
        self._cool_seconds = min(_BAN_COOLDOWN_MAX_SEC, self._cool_seconds * 2.0)

    def _extract_events(self, market: str, records: list[dict]) -> list[WhaleEvent]:
        state = self._state[market]
        out: list[WhaleEvent] = []
        for rec in records:
            self._stats["fills_seen"] += 1
            try:
                fill_id_raw = rec.get("fillRecordId") or rec.get("fillRecordID") or "0"
                fill_id = int(fill_id_raw)
            except (TypeError, ValueError):
                continue
            if fill_id <= state.last_fill_record_id:
                continue
            state.last_fill_record_id = fill_id

            notional = _parse_amount(rec.get("quoteAssetAmountFilled"))
            if notional < self.threshold_usd:
                continue

            symbol = rec.get("symbol") or market
            coin = _coin_from_symbol(symbol)
            if self._coin_filter is not None and coin.upper() not in self._coin_filter:
                continue

            base_size = _parse_amount(rec.get("baseAssetAmountFilled"))
            price = _parse_amount(rec.get("oraclePrice")) or (
                notional / base_size if base_size > 0 else 0.0
            )
            action = rec.get("action") or "fill"
            taker = rec.get("taker") or ""
            maker = rec.get("maker") or ""
            taker_dir = rec.get("takerOrderDirection") or ""

            dedupe = f"{rec.get('txSig', '')}:{fill_id}"
            if dedupe in self._seen:
                continue
            self._seen.add(dedupe)
            self._seen_order.append(dedupe)
            if len(self._seen_order) > self._seen_max:
                old = self._seen_order.pop(0)
                self._seen.discard(old)

            ts_ms = int((rec.get("ts") or time.time()) * 1000)

            out.append(WhaleEvent(
                venue="Drift",
                kind=_kind_from_action(action),
                coin=coin,
                side=_side_from_direction(taker_dir, action),
                notional_usd=notional,
                price_usd=price,
                size=base_size,
                trader=taker or maker,
                tx_ref=rec.get("txSig") or f"drift:{fill_id}",
                ts_ms=ts_ms,
                pnl_usd=None,  # Drift fills don't carry PnL; lifecycle pairing handles it.
                extra={
                    "symbol": symbol,
                    "fill_record_id": fill_id_raw,
                    "maker": maker,
                    "taker_fee": _parse_amount(rec.get("takerFee")),
                    "action": action,
                    "marketIndex": rec.get("marketIndex"),
                    "filler": rec.get("filler"),
                    "slot": rec.get("slot"),
                },
            ))
        return out

    async def _heartbeat_loop(self) -> None:
        last = dict(self._stats)
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                return
            d_polls = self._stats["polls"] - last["polls"]
            d_fills = self._stats["fills_seen"] - last["fills_seen"]
            d_emitted = self._stats["emitted"] - last["emitted"]
            d_errors = self._stats["errors"] - last["errors"]
            last = dict(self._stats)
            err_note = f", {d_errors} errors" if d_errors else ""
            log.info(
                "Drift heartbeat (60s): %d polls, %d fills examined, %d emitted "
                "above $%s%s",
                d_polls, d_fills, d_emitted, f"{self.threshold_usd:,.0f}", err_note,
            )


__all__ = ["DriftTracker", "DEFAULT_MARKETS"]
