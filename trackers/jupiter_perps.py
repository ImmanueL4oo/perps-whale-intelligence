"""Jupiter Perps (Solana) whale tracker.

Strategy
--------
- Subscribe to Solana RPC `logsSubscribe` filtered to the Jupiter Perpetuals
  program (PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu).
- Each notification carries the full log array of a confirmed transaction.
- Anchor emits events as `Program data: <base64>` lines: 8-byte event
  discriminator + Borsh-serialized struct.
- We decode `IncreasePositionEvent`, `DecreasePositionEvent`, and
  `LiquidateFullPositionEvent` (the only three events relevant to whale flow)
  using a small hand-rolled Borsh reader — no IDL fetch / anchorpy required.
- USD amounts in this program are u64 atomic 6-decimal values (USDC decimals).
  Prices are also 6-decimal. All conversions assume that.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import struct
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from ssl_ctx import get_ssl_context
from trackers import EventKind, Side, WhaleEvent
from trackers.jupiter_instant import (
    InstantDecoded,
    fetch_and_decode as _fetch_and_decode_instant,
)
from trackers.solana_rpc import SolanaRpc

log = logging.getLogger("jupiter_perps")

JUPITER_PERPS_PROGRAM_ID = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"

# Fixed JLP custodies → coin label.
# Source: Jupiter Perps program (PERPH...) constants and their published docs.
CUSTODY_TO_COIN: dict[str, str] = {
    "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz": "SOL",
    "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn": "ETH",
    "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm": "BTC",
    # Stables — should never be a position custody, listed for completeness.
    "G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa": "USDC",
    "4vkNeXiYEUizLdrpdPS1eC2mccyM4NUPRtERrk6ZETkk": "USDT",
}

USD_DECIMALS = 6
USD_ATOMIC = 10**USD_DECIMALS  # 1_000_000


# ── Anchor event discriminator helpers ────────────────────────────────────────
def _disc(event_name: str) -> bytes:
    return hashlib.sha256(f"event:{event_name}".encode()).digest()[:8]


DISC_INCREASE = _disc("IncreasePositionEvent")
DISC_DECREASE = _disc("DecreasePositionEvent")
DISC_LIQUIDATE = _disc("LiquidateFullPositionEvent")

# Instructions Jupiter shipped in their "instant orders" upgrade. These
# instructions DO NOT emit Anchor events via `Program data:` — they only emit
# human-readable `Program log:` lines, which our Borsh decoder can't see.
# Until we implement a proper getTransaction-based decoder for them, we surface
# their presence in the heartbeat so the operator knows why their event count
# may look suspiciously low.
INSTANT_INSTRUCTION_NAMES = frozenset({
    "InstantIncreasePosition",
    "InstantDecreasePosition",
    "InstantIncreasePositionPreSwap",
    "InstantDecreasePositionPreSwap",
})


# ── Tiny Borsh reader ─────────────────────────────────────────────────────────
class _BorshReader:
    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.pos = 0

    def _take(self, n: int) -> bytes:
        if self.pos + n > len(self.buf):
            raise ValueError(f"borsh underflow: need {n} at {self.pos}, have {len(self.buf)}")
        out = self.buf[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self) -> int:  return self._take(1)[0]
    def bool(self) -> bool: return self._take(1)[0] != 0
    def u64(self) -> int: return struct.unpack("<Q", self._take(8))[0]
    def i64(self) -> int: return struct.unpack("<q", self._take(8))[0]
    def u128(self) -> int: return int.from_bytes(self._take(16), "little", signed=False)
    def pubkey(self) -> bytes: return self._take(32)

    def opt_u64(self) -> Optional[int]:
        return self.u64() if self.u8() else None

    def opt_pubkey(self) -> Optional[bytes]:
        return self.pubkey() if self.u8() else None


def _b58(b: bytes) -> str:
    """base58 encode 32-byte pubkey (no external deps)."""
    alphabet = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(b, "big")
    out = b""
    while n:
        n, r = divmod(n, 58)
        out = alphabet[r:r + 1] + out
    pad = 0
    for byte in b:
        if byte == 0:
            pad += 1
        else:
            break
    return (b"1" * pad + out).decode()


# ── Decoded event payloads ────────────────────────────────────────────────────
@dataclass
class _IncreaseEvent:
    position_key: str
    side: int                # 1 = long, 2 = short (Anchor enum order in IDL)
    position_custody: str
    position_size_usd: int   # post-event total size, atomic
    owner: str
    size_usd_delta: int      # this trade's size, atomic
    price: int               # atomic 6-dec
    fee_usd: int


@dataclass
class _DecreaseEvent:
    position_key: str
    side: int
    position_custody: str
    position_size_usd: int   # remaining after decrease
    owner: str
    size_usd_delta: int      # this decrease's size
    has_profit: bool
    pnl_delta: int
    price: int
    fee_usd: int


@dataclass
class _LiquidateEvent:
    position_key: str
    side: int
    position_custody: str
    position_size_usd: int   # full size that got liquidated
    has_profit: bool
    pnl_delta: int
    owner: str
    price: int
    fee_usd: int
    liquidation_fee_usd: int


def _decode_increase(payload: bytes) -> _IncreaseEvent:
    r = _BorshReader(payload)
    position_key = _b58(r.pubkey())
    side = r.u8()
    position_custody = _b58(r.pubkey())
    _position_collateral_custody = r.pubkey()
    position_size_usd = r.u64()
    _position_mint = r.pubkey()
    _position_request_key = r.pubkey()
    _position_request_mint = r.pubkey()
    _position_request_change = r.u8()
    _position_request_type = r.u8()
    _position_request_collateral_delta = r.u64()
    owner = _b58(r.pubkey())
    _pool = r.pubkey()
    size_usd_delta = r.u64()
    _collateral_usd_delta = r.u64()
    _collateral_token_delta = r.u64()
    price = r.u64()
    _price_slippage = r.opt_u64()
    _fee_token = r.u64()
    fee_usd = r.u64()
    _open_time = r.i64()
    _referral = r.opt_pubkey()
    return _IncreaseEvent(
        position_key=position_key,
        side=side,
        position_custody=position_custody,
        position_size_usd=position_size_usd,
        owner=owner,
        size_usd_delta=size_usd_delta,
        price=price,
        fee_usd=fee_usd,
    )


def _decode_decrease(payload: bytes) -> _DecreaseEvent:
    r = _BorshReader(payload)
    position_key = _b58(r.pubkey())
    side = r.u8()
    position_custody = _b58(r.pubkey())
    _position_collateral_custody = r.pubkey()
    position_size_usd = r.u64()
    _position_mint = r.pubkey()
    _position_request_key = r.pubkey()
    _position_request_mint = r.pubkey()
    _position_request_change = r.u8()
    _position_request_type = r.u8()
    has_profit = r.bool()
    pnl_delta = r.u64()
    owner = _b58(r.pubkey())
    _pool = r.pubkey()
    size_usd_delta = r.u64()
    _transfer_amount_usd = r.u64()
    _transfer_token = r.opt_u64()
    price = r.u64()
    _price_slippage = r.opt_u64()
    fee_usd = r.u64()
    _open_time = r.i64()
    _referral = r.opt_pubkey()
    return _DecreaseEvent(
        position_key=position_key,
        side=side,
        position_custody=position_custody,
        position_size_usd=position_size_usd,
        owner=owner,
        size_usd_delta=size_usd_delta,
        has_profit=has_profit,
        pnl_delta=pnl_delta,
        price=price,
        fee_usd=fee_usd,
    )


def _decode_liquidate(payload: bytes) -> _LiquidateEvent:
    r = _BorshReader(payload)
    position_key = _b58(r.pubkey())
    side = r.u8()
    position_custody = _b58(r.pubkey())
    _position_collateral_custody = r.pubkey()
    _position_collateral_mint = r.pubkey()
    _position_mint = r.pubkey()
    position_size_usd = r.u64()
    has_profit = r.bool()
    pnl_delta = r.u64()
    owner = _b58(r.pubkey())
    _pool = r.pubkey()
    _transfer_amount_usd = r.u64()
    _transfer_token = r.u64()
    price = r.u64()
    fee_usd = r.u64()
    liquidation_fee_usd = r.u64()
    _open_time = r.i64()
    return _LiquidateEvent(
        position_key=position_key,
        side=side,
        position_custody=position_custody,
        position_size_usd=position_size_usd,
        has_profit=has_profit,
        pnl_delta=pnl_delta,
        owner=owner,
        price=price,
        fee_usd=fee_usd,
        liquidation_fee_usd=liquidation_fee_usd,
    )


def _try_decode_program_data(b64: str):
    """Return one of the decoded events, or None if it isn't one we care about."""
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return None
    if len(raw) < 8:
        return None
    disc, payload = raw[:8], raw[8:]
    try:
        if disc == DISC_INCREASE:
            return _decode_increase(payload)
        if disc == DISC_DECREASE:
            return _decode_decrease(payload)
        if disc == DISC_LIQUIDATE:
            return _decode_liquidate(payload)
    except Exception as exc:
        log.debug("borsh decode failed: %s", exc)
    return None


# ── Side mapping ──────────────────────────────────────────────────────────────
def _side_from_u8(s: int) -> Side:
    # Anchor enum order in IDL: { None, Long, Short } so 1 = Long, 2 = Short.
    return Side.LONG if s == 1 else Side.SHORT


# ── Tracker ───────────────────────────────────────────────────────────────────
class JupiterPerpsTracker:
    def __init__(
        self,
        *,
        threshold_usd: float,
        rpc_ws_url: str = "wss://api.mainnet-beta.solana.com",
        program_id: str = JUPITER_PERPS_PROGRAM_ID,
        coins: Optional[list[str]] = None,
        rpc: Optional[SolanaRpc] = None,
        max_concurrent_enrich: int = 4,
    ) -> None:
        self.threshold_usd = threshold_usd
        self.rpc_ws_url = rpc_ws_url
        self.program_id = program_id
        self.threshold_atomic = int(threshold_usd * USD_ATOMIC)
        # Optional whitelist of coin labels (e.g. ["SOL", "BTC"]). If None we
        # emit every supported custody.
        self.coins: Optional[set[str]] = (
            {c.upper() for c in coins} if coins else None
        )
        # Dedupe by signature+positionKey+kind to avoid double-emitting if the
        # same tx is delivered twice across reconnects.
        self._seen: set[str] = set()
        self._seen_order: list[str] = []
        self._seen_max = 8192
        # Optional Solana RPC client used to enrich Instant* transactions —
        # they don't emit Anchor events so we have to fetch the tx ourselves.
        self._rpc = rpc
        # Limit concurrent getTransaction calls so a burst of Instant* txs
        # doesn't hammer the public RPC into 429-land.
        self._enrich_sem = asyncio.Semaphore(max_concurrent_enrich)
        # Outbound queue used to feed enriched events back into `_run_once`.
        self._enriched: asyncio.Queue[WhaleEvent] = asyncio.Queue(maxsize=512)
        self._enrich_tasks: set[asyncio.Task] = set()
        # In-flight signatures so we don't enrich the same sig twice (the WS
        # may resend the notification after a flaky disconnect).
        self._seen_enrich_inflight: set[str] = set()

    async def stream(self) -> AsyncIterator[WhaleEvent]:
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

    async def _run_once(self) -> AsyncIterator[WhaleEvent]:
        log.info("connecting %s for program=%s", self.rpc_ws_url, self.program_id)
        async with websockets.connect(
            self.rpc_ws_url,
            ping_interval=30,
            ping_timeout=20,
            max_size=2**24,
            close_timeout=2,
            ssl=get_ssl_context() if self.rpc_ws_url.startswith("wss://") else None,
        ) as ws:
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [self.program_id]},
                    {"commitment": "confirmed"},
                ],
            }))

            # Counters for the periodic heartbeat — confirms whether silence
            # means "no whale activity" vs. "RPC is dropping our subscription"
            # vs. "Jupiter migrated to a format we can't yet decode".
            stats = {
                "notifs": 0, "decoded": 0, "emitted": 0,
                "subscribed": False, "instant_v2_seen": 0,
                "instant_enriched": 0, "instant_emitted": 0,
                "warned_v2": False,
            }
            heartbeat_task = asyncio.create_task(self._heartbeat(stats))
            try:
                while True:
                    # We multiplex two sources: (1) the WS notification stream
                    # and (2) the enriched-Instant queue. Whichever fires first
                    # gets handled; the other side keeps its place. This
                    # avoids head-of-line blocking when an Instant enrichment
                    # is slow vs the live tape.
                    ws_task = asyncio.ensure_future(ws.recv())
                    enriched_task = asyncio.ensure_future(self._enriched.get())
                    done, _ = await asyncio.wait(
                        {ws_task, enriched_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if enriched_task in done:
                        ws_task.cancel()
                        try:
                            ev = enriched_task.result()
                        except Exception:
                            ev = None
                        if ev is not None:
                            stats["emitted"] += 1
                            stats["instant_emitted"] += 1
                            yield ev
                        # Drain any other ready enriched events without blocking.
                        try:
                            while True:
                                ev = self._enriched.get_nowait()
                                stats["emitted"] += 1
                                stats["instant_emitted"] += 1
                                yield ev
                        except asyncio.QueueEmpty:
                            pass
                        continue

                    # ws_task completed first.
                    enriched_task.cancel()
                    try:
                        raw = ws_task.result()
                    except websockets.ConnectionClosed:
                        raise
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # First message back is the subscription ack.
                    if not stats["subscribed"] and msg.get("id") == 1 and "result" in msg:
                        stats["subscribed"] = True
                        log.info(
                            "subscribed to logs (id=%s) — waiting for whale activity ≥ $%s",
                            msg.get("result"),
                            f"{self.threshold_usd:,.0f}",
                        )
                        continue

                    if msg.get("method") != "logsNotification":
                        continue
                    stats["notifs"] += 1
                    params = msg.get("params") or {}
                    value = (params.get("result") or {}).get("value") or {}
                    logs = value.get("logs") or []
                    signature = value.get("signature") or ""
                    err = value.get("err")
                    if err is not None:
                        continue  # failed tx
                    if not logs:
                        continue

                    decoded_count, emitted = self._extract_events(logs, signature)
                    stats["decoded"] += decoded_count
                    for ev in emitted:
                        stats["emitted"] += 1
                        yield ev

                    # Detect any Instant* instruction. If we have an RPC client
                    # configured, kick off async enrichment; otherwise log the
                    # legacy warning so the operator knows what's missing.
                    saw_instant = False
                    for line in logs:
                        if not line.startswith("Program log: Instruction: "):
                            continue
                        inst = line[len("Program log: Instruction: "):].strip()
                        if inst in INSTANT_INSTRUCTION_NAMES:
                            saw_instant = True
                            stats["instant_v2_seen"] += 1
                            break
                    if saw_instant and signature:
                        if self._rpc is not None:
                            self._spawn_enrichment(signature, stats)
                        elif not stats["warned_v2"]:
                            stats["warned_v2"] = True
                            log.warning(
                                "Jupiter Perps Instant* tx detected but no Solana RPC "
                                "client wired up — pass `rpc=SolanaRpc(...)` to "
                                "JupiterPerpsTracker for full coverage. (See README.)"
                            )
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except (asyncio.CancelledError, Exception):
                    pass
                # Cancel any in-flight enrichment tasks so reconnects don't
                # leak fetches across iterations.
                for t in list(self._enrich_tasks):
                    t.cancel()
                if self._enrich_tasks:
                    await asyncio.gather(*self._enrich_tasks, return_exceptions=True)
                    self._enrich_tasks.clear()

    async def _heartbeat(self, stats: dict) -> None:
        """Log a one-line status every 60 s so the user knows the WS is alive
        even when no whale-sized events occur.
        """
        last = {
            "notifs": 0, "decoded": 0, "emitted": 0,
            "instant_v2_seen": 0, "instant_enriched": 0, "instant_emitted": 0,
        }
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                return
            d_notifs = stats["notifs"] - last["notifs"]
            d_decoded = stats["decoded"] - last["decoded"]
            d_emitted = stats["emitted"] - last["emitted"]
            d_v2 = stats["instant_v2_seen"] - last["instant_v2_seen"]
            d_enriched = stats["instant_enriched"] - last["instant_enriched"]
            d_iemit = stats["instant_emitted"] - last["instant_emitted"]
            for k in last:
                last[k] = stats[k]
            if not stats["subscribed"]:
                log.warning(
                    "heartbeat: no subscription ack yet — RPC may be unreachable "
                    "(public Solana RPC is heavily rate-limited; try a paid endpoint)"
                )
                continue
            if d_notifs == 0:
                log.warning(
                    "heartbeat: 0 notifications in last 60s — RPC is silent. "
                    "If this persists, the public endpoint dropped your subscription."
                )
            else:
                if d_v2:
                    v2_note = (
                        f"; {d_v2} Instant* tx ({d_enriched} enriched, "
                        f"{d_iemit} above threshold)"
                    )
                else:
                    v2_note = ""
                log.info(
                    "heartbeat (60s): %d tx notifications, %d events decoded, "
                    "%d above $%s threshold%s",
                    d_notifs, d_decoded, d_emitted,
                    f"{self.threshold_usd:,.0f}", v2_note,
                )

    def _spawn_enrichment(self, signature: str, stats: dict) -> None:
        """Fire-and-forget enrichment of a single Instant* signature. The task
        pushes any resulting `WhaleEvent` onto `self._enriched` for the main
        loop to consume.
        """
        if signature in self._seen_enrich_inflight:
            return
        self._seen_enrich_inflight.add(signature)

        async def _runner() -> None:
            try:
                async with self._enrich_sem:
                    decoded = await _fetch_and_decode_instant(self._rpc, signature)
            except Exception:
                log.debug("instant enrichment crashed for %s", signature, exc_info=True)
                decoded = None
            finally:
                self._seen_enrich_inflight.discard(signature)
            if decoded is None:
                return
            stats["instant_enriched"] += 1
            ev = self._instant_to_whale_event(decoded)
            if ev is None:
                return
            try:
                self._enriched.put_nowait(ev)
            except asyncio.QueueFull:
                # Drop oldest pending — backpressure should never block the
                # live WS reader.
                try:
                    self._enriched.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self._enriched.put_nowait(ev)
                except asyncio.QueueFull:
                    pass

        task = asyncio.create_task(_runner(), name=f"jup-enrich-{signature[:8]}")
        self._enrich_tasks.add(task)
        task.add_done_callback(self._enrich_tasks.discard)

    def _instant_to_whale_event(
        self, decoded: InstantDecoded,
    ) -> Optional[WhaleEvent]:
        """Convert a decoded Instant* payload into a WhaleEvent.
        Returns None if the size is sub-threshold or essential fields are
        missing.
        """
        if decoded.size_usd_delta < self.threshold_atomic:
            return None
        coin = decoded.coin or "?"
        if self.coins is not None and coin.upper() not in self.coins:
            return None
        if decoded.is_increase:
            kind = EventKind.OPEN
        elif decoded.is_decrease:
            kind = EventKind.CLOSE
        else:
            # tp/sl management ix — skip, we only care about size flow
            return None
        side = (
            Side.LONG if decoded.side == "long"
            else Side.SHORT if decoded.side == "short"
            else Side.LONG  # default — UI will mark this as best-effort
        )
        notional = decoded.size_usd_delta / USD_ATOMIC
        dedupe = (
            f"{decoded.signature}:{decoded.owner}:{kind.value}:"
            f"{decoded.size_usd_delta}"
        )
        if dedupe in self._seen:
            return None
        self._seen.add(dedupe)
        self._seen_order.append(dedupe)
        if len(self._seen_order) > self._seen_max:
            old = self._seen_order.pop(0)
            self._seen.discard(old)

        return WhaleEvent(
            venue="JupiterPerps",
            kind=kind,
            coin=coin,
            side=side,
            notional_usd=notional,
            price_usd=0.0,  # oracle price requires a separate fetch we skip here
            size=0.0,       # base-size needs price to convert; downstream tolerates 0
            trader=decoded.owner,
            tx_ref=decoded.signature,
            ts_ms=int(time.time() * 1000),
            pnl_usd=None,
            extra={
                "custody": decoded.custody or "",
                "instant_kind": decoded.kind,
                "side_inferred": True,  # signal to UI that side is heuristic
            },
        )

    def _extract_events(
        self, logs: list[str], signature: str,
    ) -> tuple[int, list[WhaleEvent]]:
        """Returns (decoded_event_count, events_above_threshold)."""
        decoded_count = 0
        out: list[WhaleEvent] = []
        for line in logs:
            if not line.startswith("Program data: "):
                continue
            decoded = _try_decode_program_data(line[len("Program data: "):])
            if decoded is None:
                continue
            decoded_count += 1
            ev = self._to_whale_event(decoded, signature)
            if ev is not None:
                out.append(ev)
        return decoded_count, out

    def _to_whale_event(self, decoded, signature: str) -> Optional[WhaleEvent]:
        if isinstance(decoded, _IncreaseEvent):
            kind = EventKind.OPEN
            atomic_size = decoded.size_usd_delta
            side = _side_from_u8(decoded.side)
            custody = decoded.position_custody
            owner = decoded.owner
            position_key = decoded.position_key
            price_atomic = decoded.price
            pnl_usd = None
        elif isinstance(decoded, _DecreaseEvent):
            kind = EventKind.CLOSE
            atomic_size = decoded.size_usd_delta
            side = _side_from_u8(decoded.side)
            custody = decoded.position_custody
            owner = decoded.owner
            position_key = decoded.position_key
            price_atomic = decoded.price
            pnl_usd = (
                decoded.pnl_delta / USD_ATOMIC * (1 if decoded.has_profit else -1)
            )
        elif isinstance(decoded, _LiquidateEvent):
            kind = EventKind.LIQUIDATION
            atomic_size = decoded.position_size_usd  # whole position liquidated
            side = _side_from_u8(decoded.side)
            custody = decoded.position_custody
            owner = decoded.owner
            position_key = decoded.position_key
            price_atomic = decoded.price
            pnl_usd = (
                decoded.pnl_delta / USD_ATOMIC * (1 if decoded.has_profit else -1)
            )
        else:
            return None

        if atomic_size < self.threshold_atomic:
            return None

        notional = atomic_size / USD_ATOMIC
        price = price_atomic / USD_ATOMIC if price_atomic else 0.0
        coin = CUSTODY_TO_COIN.get(custody, custody[:6] + "…")
        if self.coins is not None and coin.upper() not in self.coins:
            return None
        size_base = (atomic_size / price_atomic) if price_atomic else 0.0

        dedupe = f"{signature}:{position_key}:{kind.value}:{atomic_size}"
        if dedupe in self._seen:
            return None
        self._seen.add(dedupe)
        self._seen_order.append(dedupe)
        if len(self._seen_order) > self._seen_max:
            old = self._seen_order.pop(0)
            self._seen.discard(old)

        return WhaleEvent(
            venue="JupiterPerps",
            kind=kind,
            coin=coin,
            side=side,
            notional_usd=notional,
            price_usd=price,
            size=size_base,
            trader=owner,
            tx_ref=signature,
            ts_ms=int(time.time() * 1000),
            pnl_usd=pnl_usd,
            extra={"position": position_key, "custody": custody},
        )
