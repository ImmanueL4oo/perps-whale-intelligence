"""Solana CEX in/out flow tracker.

Subscribes to Solana RPC log streams for every wallet in `cex_wallets.CEX_WALLETS`
and turns each on-chain transfer that touches one of those wallets into a
structured `CexFlowEvent`. The event tells you:

* which exchange was on the other side of the transfer,
* whether net flow was IN (deposit, bearish — selling pressure) or
  OUT (withdraw, bullish — accumulation),
* which token, how much, and the USD value at fetch time,
* the counterparty wallet (the non-CEX side).

The tracker is intentionally lean: one Solana websocket multiplexes
`logsSubscribe` calls for every wallet, an `asyncio.Semaphore` bounds
concurrent `getTransaction` enrichments so we don't melt public RPCs,
and we use `meta.preTokenBalances` / `postTokenBalances` to compute SPL
deltas (no need to walk instruction trees).

Why log subs and not `accountSubscribe`?
   `accountSubscribe` only tells us a balance changed — not by what or
   with whom. Logs identify the signature so we can decode the full
   transfer set in one call.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import AsyncIterator, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from cex_wallets import (
    CEX_WALLETS,
    CexWallet,
    WSOL_MINT,
    cex_addresses_set,
    lookup_token,
    lookup_wallet,
)
from pricing import PriceFeed
from ssl_ctx import get_ssl_context
from trackers.solana_rpc import SolanaRpc

log = logging.getLogger("cex_flows")


# ── Event model ─────────────────────────────────────────────────────────────
@dataclass
class CexFlowEvent:
    """Single CEX in/out flow event (one mint, one direction, one CEX side).

    A multi-leg transaction (e.g. a Binance-internal sweep that touches two
    Binance wallets and three users) emits multiple `CexFlowEvent`s — one
    per (cex_wallet, mint) leg with non-zero delta. This keeps every event
    semantically simple and analytics-friendly.
    """
    ts_ms: int
    signature: str
    exchange: str            # "Binance", "Coinbase", …
    cex_label: str           # "hot-1", etc.
    cex_wallet: str          # the CEX-side address
    direction: str           # "IN" (deposit to CEX) | "OUT" (withdraw)
    asset: str               # token symbol (SOL, USDC, USDT, JUP …)
    mint: str
    amount: float            # human-units
    amount_usd: float        # 0.0 if no price available
    counterparty: str        # the non-CEX side, best-effort heuristic
    slot: Optional[int] = None
    block_time: Optional[int] = None
    extra: dict = field(default_factory=dict)

    @property
    def dedupe_key(self) -> str:
        return f"{self.signature}|{self.cex_wallet}|{self.mint}|{self.direction}"

    def to_dict(self) -> dict:
        return asdict(self)


# ── Tracker ─────────────────────────────────────────────────────────────────
class CexFlowsTracker:
    def __init__(
        self,
        *,
        ws_url: str,
        rpc: SolanaRpc,
        price_feed: PriceFeed,
        wallets: Optional[list[CexWallet]] = None,
        min_flow_usd: float = 5_000.0,
        max_concurrent_enrich: int = 6,
        max_log_age_sec: float = 60.0,
    ) -> None:
        self.ws_url = ws_url
        self.rpc = rpc
        self.prices = price_feed
        self.wallets: list[CexWallet] = wallets or list(CEX_WALLETS)
        self.wallet_addrs: set[str] = {w.address for w in self.wallets}
        self.min_flow_usd = float(min_flow_usd)
        self.max_log_age_sec = max_log_age_sec
        self._enrich_sem = asyncio.Semaphore(max_concurrent_enrich)
        self._enriched: asyncio.Queue[CexFlowEvent] = asyncio.Queue(maxsize=512)
        self._enrich_tasks: set[asyncio.Task] = set()
        self._inflight: set[str] = set()
        self._dedupe: dict[str, float] = {}        # dedupe by (sig, wallet, mint, dir)
        self._dedupe_max = 4096
        self._stop = asyncio.Event()
        # Heartbeat counters reset every 60s in `_heartbeat_loop`.
        self._stats = {
            "log_notifs": 0,
            "txs_fetched": 0,
            "tx_fetch_failed": 0,
            "events_emitted": 0,
            "below_threshold": 0,
        }

    async def stream(self) -> AsyncIterator[CexFlowEvent]:
        """Yield CEX flow events forever (with auto-reconnect)."""
        log.info(
            "starting CEX flow tracker — %d wallets across %d exchanges, min flow $%s",
            len(self.wallet_addrs),
            len({w.exchange for w in self.wallets}),
            f"{self.min_flow_usd:,.0f}",
        )
        hb = asyncio.create_task(self._heartbeat_loop())
        try:
            while not self._stop.is_set():
                try:
                    async for ev in self._run_once():
                        yield ev
                except (ConnectionClosed, OSError) as exc:
                    log.warning("ws disconnected (%s) — reconnecting in 2s", exc)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("ws loop crashed — restarting in 2s")
                if self._stop.is_set():
                    break
                await asyncio.sleep(2.0)
        finally:
            hb.cancel()

    async def close(self) -> None:
        self._stop.set()
        for t in list(self._enrich_tasks):
            t.cancel()
        if self._enrich_tasks:
            await asyncio.gather(*self._enrich_tasks, return_exceptions=True)

    # ── Implementation ──────────────────────────────────────────────────────
    async def _run_once(self) -> AsyncIterator[CexFlowEvent]:
        """One websocket lifetime: connect, subscribe to every wallet's
        logs, multiplex incoming notifications + enriched events."""
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            max_size=4 * 1024 * 1024,
            ssl=get_ssl_context() if self.ws_url.startswith("wss://") else None,
        ) as ws:
            log.info("ws connected — subscribing to %d wallets", len(self.wallet_addrs))
            sub_ids: dict[int, str] = {}      # rpc-id -> wallet (response correlation)
            for i, addr in enumerate(sorted(self.wallet_addrs)):
                req = {
                    "jsonrpc": "2.0",
                    "id": i + 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [addr]},
                        {"commitment": "confirmed"},
                    ],
                }
                await ws.send(json.dumps(req))
                sub_ids[i + 1] = addr

            ws_task: Optional[asyncio.Task] = None
            enr_task: Optional[asyncio.Task] = None
            try:
                while not self._stop.is_set():
                    if ws_task is None or ws_task.done():
                        ws_task = asyncio.ensure_future(ws.recv())
                    if enr_task is None or enr_task.done():
                        enr_task = asyncio.ensure_future(self._enriched.get())
                    done, _pending = await asyncio.wait(
                        {ws_task, enr_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if ws_task in done:
                        raw = ws_task.result()
                        ws_task = None
                        await self._handle_ws_message(raw, sub_ids)
                    if enr_task in done:
                        ev = enr_task.result()
                        enr_task = None
                        yield ev
            finally:
                for t in (ws_task, enr_task):
                    if t is not None and not t.done():
                        t.cancel()

    async def _handle_ws_message(self, raw: str, sub_ids: dict[int, str]) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        # Subscription ack: {"jsonrpc":"2.0","result":<n>,"id":<reqid>}
        if "id" in msg and "method" not in msg:
            return
        if msg.get("method") != "logsNotification":
            return
        params = msg.get("params") or {}
        result = params.get("result") or {}
        value = result.get("value") or {}
        signature = value.get("signature")
        if not signature:
            return
        # Drop transactions that errored on-chain (we only care about successful flows).
        if value.get("err") is not None:
            return
        self._stats["log_notifs"] += 1
        if signature in self._inflight:
            return
        self._inflight.add(signature)
        # Cap on the inflight set so a long uptime doesn't leak memory.
        if len(self._inflight) > self._dedupe_max:
            # Drop the oldest by clearing roughly half.
            for s in list(self._inflight)[: self._dedupe_max // 2]:
                self._inflight.discard(s)
        slot = result.get("context", {}).get("slot")
        task = asyncio.create_task(self._enrich(signature, slot))
        self._enrich_tasks.add(task)
        task.add_done_callback(self._enrich_tasks.discard)

    async def _enrich(self, signature: str, slot: Optional[int]) -> None:
        """Fetch a tx, decode CEX-touching transfers, push events to queue."""
        async with self._enrich_sem:
            try:
                tx = await self.rpc.get_transaction(signature)
            except Exception:
                self._stats["tx_fetch_failed"] += 1
                self._inflight.discard(signature)
                return
            if tx is None:
                self._stats["tx_fetch_failed"] += 1
                self._inflight.discard(signature)
                return
            self._stats["txs_fetched"] += 1
            # Skip very stale txs picked up by an out-of-band reconnect or
            # backfill — we don't want to flood the dashboard with old data.
            block_time = tx.get("blockTime")
            if block_time is not None:
                age = time.time() - block_time
                if age > self.max_log_age_sec * 5:    # generous: 5×
                    self._inflight.discard(signature)
                    return
            try:
                events = await self._decode_tx(tx, signature, slot)
            except Exception:
                log.exception("decode failed for %s", signature)
                self._inflight.discard(signature)
                return
            for ev in events:
                if ev.amount_usd < self.min_flow_usd:
                    self._stats["below_threshold"] += 1
                    continue
                if ev.dedupe_key in self._dedupe:
                    continue
                self._dedupe[ev.dedupe_key] = time.monotonic()
                if len(self._dedupe) > self._dedupe_max:
                    cutoff = time.monotonic() - 600
                    self._dedupe = {
                        k: t for k, t in self._dedupe.items() if t > cutoff
                    }
                self._stats["events_emitted"] += 1
                try:
                    self._enriched.put_nowait(ev)
                except asyncio.QueueFull:
                    log.warning("enriched queue full — dropping event %s", ev.dedupe_key)

    async def _decode_tx(
        self,
        tx: dict,
        signature: str,
        slot: Optional[int],
    ) -> list[CexFlowEvent]:
        """Walk preTokenBalances/postTokenBalances + native lamport deltas
        to find every (owner, mint) pair whose balance changed — then emit
        one event per CEX-side leg."""
        meta = tx.get("meta") or {}
        block_time = tx.get("blockTime")
        ts_ms = int((block_time or time.time()) * 1000)

        keys = SolanaRpc.all_account_keys(tx)
        if not keys:
            return []

        # ── 1. Build a delta map keyed by owner+mint. ──────────────────────
        # `preTokenBalances` and `postTokenBalances` are arrays with:
        #   accountIndex, mint, owner, uiTokenAmount{ uiAmount, decimals, … }
        deltas: dict[tuple[str, str], dict] = {}
        # owner -> (sym, mint, decimals): used for counterparty lookup later.
        for tb in meta.get("preTokenBalances") or []:
            owner = tb.get("owner")
            mint = tb.get("mint")
            if not owner or not mint:
                continue
            ui = (tb.get("uiTokenAmount") or {}).get("uiAmount") or 0.0
            d = deltas.setdefault((owner, mint), {"pre": 0.0, "post": 0.0})
            d["pre"] = float(ui)
        for tb in meta.get("postTokenBalances") or []:
            owner = tb.get("owner")
            mint = tb.get("mint")
            if not owner or not mint:
                continue
            ui = (tb.get("uiTokenAmount") or {}).get("uiAmount") or 0.0
            d = deltas.setdefault((owner, mint), {"pre": 0.0, "post": 0.0})
            d["post"] = float(ui)

        # ── 2. Add native SOL deltas. ──────────────────────────────────────
        # `pre/postBalances` are aligned to `accountKeys`. They include the
        # fee paid by the fee-payer so we can't naively trust the first
        # account's delta — but for non-fee-payer accounts the delta is the
        # net SOL transfer. We emit native SOL deltas as the WSOL mint so
        # the analytics layer treats them as SOL transparently.
        pre = meta.get("preBalances") or []
        post = meta.get("postBalances") or []
        fee = int(meta.get("fee") or 0)
        for idx, key in enumerate(keys):
            if idx >= len(pre) or idx >= len(post):
                break
            d = (post[idx] - pre[idx]) / 1e9       # lamports → SOL
            # If this account was the fee-payer (idx 0), back out the fee.
            if idx == 0:
                d += fee / 1e9
            if abs(d) < 1e-9:
                continue
            entry = deltas.setdefault((key, WSOL_MINT), {"pre": 0.0, "post": 0.0})
            # Native SOL gets recorded as a single delta on the owner side
            # (same key). We synthesize pre=0, post=d so the rest of the
            # pipeline picks it up uniformly.
            entry["post"] += d

        # ── 3. Filter to legs that actually involve a CEX wallet. ──────────
        cex_addrs = self.wallet_addrs
        cex_legs: list[tuple[str, str, float]] = []     # (cex_wallet, mint, delta)
        non_cex_legs: list[tuple[str, str, float]] = [] # for counterparty matching
        for (owner, mint), d in deltas.items():
            delta = d["post"] - d["pre"]
            if abs(delta) < 1e-9:
                continue
            if owner in cex_addrs:
                cex_legs.append((owner, mint, delta))
            else:
                non_cex_legs.append((owner, mint, delta))
        if not cex_legs:
            return []

        # ── 4. Resolve USD prices for the mints we care about. ─────────────
        unique_mints = list({mint for _, mint, _ in cex_legs})
        prices = await self.prices.get_usd_many(unique_mints)

        events: list[CexFlowEvent] = []
        for cex_wallet_addr, mint, delta in cex_legs:
            wallet = lookup_wallet(cex_wallet_addr)
            if wallet is None:
                continue
            token = lookup_token(mint)
            if token is None:
                # Skip unknown SPLs (no symbol → no useful row in the UI).
                # We still account for SOL/stables which are explicit registry hits.
                continue
            amt = abs(delta)
            usd_price = prices.get(mint)
            usd = (usd_price or 0.0) * amt
            direction = "IN" if delta > 0 else "OUT"
            counterparty = self._best_counterparty(
                non_cex_legs, mint=mint, opposite_sign_of=delta,
            )
            events.append(CexFlowEvent(
                ts_ms=ts_ms,
                signature=signature,
                exchange=wallet.exchange,
                cex_label=wallet.label,
                cex_wallet=cex_wallet_addr,
                direction=direction,
                asset=token.symbol,
                mint=mint,
                amount=amt,
                amount_usd=float(usd),
                counterparty=counterparty,
                slot=slot,
                block_time=block_time,
                extra={"price_usd": usd_price or 0.0},
            ))
        return events

    @staticmethod
    def _best_counterparty(
        legs: list[tuple[str, str, float]],
        *,
        mint: str,
        opposite_sign_of: float,
    ) -> str:
        """Best-effort: pick the non-CEX owner whose mint delta is closest in
        magnitude to ours and has the opposite sign. Falls back to "" when
        no clear match exists (e.g. swap routed through DEX pools)."""
        target_sign = -1 if opposite_sign_of > 0 else 1
        candidates = [
            (owner, abs(delta))
            for owner, m, delta in legs
            if m == mint and ((delta < 0) if target_sign < 0 else (delta > 0))
        ]
        if not candidates:
            return ""
        candidates.sort(key=lambda c: -c[1])
        return candidates[0][0]

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(60.0)
                s = self._stats
                log.info(
                    "heartbeat (60s): %d log notifs, %d txs fetched (%d failed), "
                    "%d events emitted, %d below $%s threshold",
                    s["log_notifs"], s["txs_fetched"], s["tx_fetch_failed"],
                    s["events_emitted"], s["below_threshold"],
                    f"{self.min_flow_usd:,.0f}",
                )
                for k in list(s.keys()):
                    s[k] = 0
        except asyncio.CancelledError:
            return


__all__ = ["CexFlowsTracker", "CexFlowEvent"]
