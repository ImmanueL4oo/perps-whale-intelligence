"""Per-wallet track record service.

For every whale event we surface we also try to answer: "Is this trader
historically profitable, and should I copy their move?"

Hyperliquid
-----------
Single Info-API call (`userFills`, last 2000 fills) gives us every fill's
`closedPnl`. Sum, count wins, slice by 30d, done. Cheap and reliable.

Jupiter Perps
-------------
No public history API. We do a best-effort scan: pull the wallet's last N
signatures from Solana RPC, fetch each transaction, and parse the same
Anchor events the live tracker decodes. Expensive on public RPC — use a
paid endpoint for production or accept that some wallets will time out.

Both fetchers are wrapped in:
- TTL cache (default 5 min) — repeat whales don't re-trigger work
- single-flight — concurrent lookups for the same wallet share one task
- hard timeout per call so the alert pipeline never stalls
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from ssl_ctx import get_ssl_context
from trackers.jupiter_perps import (
    CUSTODY_TO_COIN,
    USD_ATOMIC,
    _DecreaseEvent,
    _LiquidateEvent,
    _try_decode_program_data,
)

log = logging.getLogger("trackrecord")


# ── Trust tier ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Tier:
    label: str
    emoji: str
    color: str  # rich color


TIER_ELITE = Tier("ELITE", "🏆", "bright_magenta")
TIER_TRUSTED = Tier("TRUSTED", "✅", "bright_green")
TIER_MIXED = Tier("MIXED", "⚖️", "yellow")
TIER_POOR = Tier("POOR", "⚠️", "red")
TIER_NEW = Tier("NEW", "❓", "cyan")
TIER_UNKNOWN = Tier("UNKNOWN", "·", "dim")


@dataclass
class ClosedTradeRecord:
    """One realized trade — used for the equity curve + recent-trades list."""
    ts_ms: int
    coin: str
    side: str           # "long" / "short"
    size: float
    price_usd: float
    pnl_usd: float
    fee_usd: float


@dataclass
class CoinPnL:
    coin: str
    trades: int
    win_count: int
    pnl_usd: float


@dataclass
class WalletStats:
    venue: str
    wallet: str
    closed_trades: int
    win_count: int
    realized_pnl_usd: float
    pnl_30d_usd: Optional[float]
    largest_win_usd: float
    largest_loss_usd: float
    avg_trade_pnl_usd: float
    first_trade_ts_ms: Optional[int]
    fee_paid_usd: float
    coverage_note: Optional[str]   # e.g. "(last 2000 fills)" or "(scanned 50 sigs)"

    # Extended fields — populated by the fetchers, optional for existing call sites.
    gross_wins_usd: float = 0.0           # sum of positive PnL trades
    gross_losses_usd: float = 0.0         # sum of negative PnL trades (≤ 0)
    pnl_7d_usd: Optional[float] = None
    recent_trades: list[ClosedTradeRecord] = field(default_factory=list)
    by_coin: dict[str, CoinPnL] = field(default_factory=dict)

    @property
    def winrate(self) -> Optional[float]:
        return (self.win_count / self.closed_trades) if self.closed_trades else None

    @property
    def days_active(self) -> Optional[float]:
        if not self.first_trade_ts_ms:
            return None
        return (time.time() * 1000 - self.first_trade_ts_ms) / 86_400_000

    @property
    def profit_factor(self) -> Optional[float]:
        """sum(positive PnL) / |sum(negative PnL)|. None if no losses recorded."""
        if self.gross_losses_usd >= 0:
            return None
        return self.gross_wins_usd / abs(self.gross_losses_usd)

    @property
    def avg_win_usd(self) -> Optional[float]:
        if self.win_count == 0:
            return None
        return self.gross_wins_usd / self.win_count

    @property
    def avg_loss_usd(self) -> Optional[float]:
        loss_count = self.closed_trades - self.win_count
        if loss_count <= 0:
            return None
        return self.gross_losses_usd / loss_count

    @property
    def score(self) -> Optional[float]:
        """0–100 composite quality score. None if too few samples to judge."""
        n = self.closed_trades
        if n < 5:
            return None

        wr = self.winrate or 0.0
        pnl = self.realized_pnl_usd

        # Component 1 — win rate (0..35). Floor at 35% wr, max at 70% wr.
        wr_pts = max(0.0, min(35.0, (wr - 0.35) / 0.35 * 35.0))

        # Component 2 — realized PnL (-30..+35), log-scaled both ways.
        if pnl >= 1_000:
            pnl_pts = min(35.0, math.log10(pnl / 1_000) * 8.75)
        elif pnl <= -1_000:
            pnl_pts = -min(30.0, math.log10(abs(pnl) / 1_000) * 7.5)
        else:
            pnl_pts = 0.0

        # Component 3 — sample size confidence (0..15). 10 trades → 5, 1000 → 15.
        sample_pts = min(15.0, math.log10(max(1, n)) * 5.0)

        # Component 4 — profit factor (0..15). PF=1 → 0, PF=4 → 15.
        pf = self.profit_factor
        if pf is None and self.gross_wins_usd > 0:
            pf_pts = 15.0
        elif pf is not None and pf > 1.0:
            pf_pts = min(15.0, (pf - 1.0) * 5.0)
        else:
            pf_pts = 0.0

        return max(0.0, min(100.0, wr_pts + pnl_pts + sample_pts + pf_pts))

    def tier(self) -> Tier:
        n = self.closed_trades
        wr = self.winrate or 0.0
        pnl = self.realized_pnl_usd
        if n < 5:
            return TIER_NEW
        if pnl < -100_000 or wr < 0.40:
            return TIER_POOR
        if n >= 20 and wr >= 0.60 and pnl >= 1_000_000:
            return TIER_ELITE
        if n >= 10 and wr >= 0.55 and pnl > 0:
            return TIER_TRUSTED
        return TIER_MIXED


# ── Service ───────────────────────────────────────────────────────────────────
class TrackRecord:
    """Resolve per-wallet historical stats with caching + single-flight."""

    def __init__(
        self,
        *,
        hl_info_url: str = "https://api.hyperliquid.xyz/info",
        sol_rpc_http: str = "https://api.mainnet-beta.solana.com",
        drift_data_api: str = "https://data.api.drift.trade",
        ttl_seconds: float = 300.0,
        jup_signature_limit: int = 100,
        jup_max_tx_fetches: int = 50,
        jup_concurrency: int = 5,
        drift_page_size: int = 200,
    ) -> None:
        self.hl_info_url = hl_info_url
        self.sol_rpc_http = sol_rpc_http
        self.drift_data_api = drift_data_api.rstrip("/")
        self.ttl = ttl_seconds
        self.jup_sig_limit = jup_signature_limit
        self.jup_max_tx = jup_max_tx_fetches
        self.jup_conc = jup_concurrency
        self.drift_page_size = drift_page_size

        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: dict[tuple[str, str], tuple[float, Optional[WalletStats]]] = {}
        self._inflight: dict[tuple[str, str], asyncio.Task[Optional[WalletStats]]] = {}

    async def __aenter__(self) -> "TrackRecord":
        connector = aiohttp.TCPConnector(ssl=get_ssl_context())
        self._session = aiohttp.ClientSession(connector=connector)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    async def get(
        self, venue: str, wallet: str, *, timeout: float = 5.0,
    ) -> Optional[WalletStats]:
        if not wallet or wallet == "?":
            return None
        key = (venue, wallet)

        cached = self._cache.get(key)
        if cached and (time.time() - cached[0]) < self.ttl:
            return cached[1]

        # Single-flight: piggyback on any in-progress fetch for the same wallet.
        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(self._fetch(venue, wallet))
            self._inflight[key] = task
            task.add_done_callback(lambda _t, k=key: self._inflight.pop(k, None))

        try:
            stats = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            self._cache[key] = (time.time(), stats)
            return stats
        except asyncio.TimeoutError:
            log.debug("track record timeout for %s/%s", venue, wallet)
            return None
        except Exception as exc:
            log.debug("track record error for %s/%s: %s", venue, wallet, exc)
            self._cache[key] = (time.time(), None)
            return None

    async def _fetch(self, venue: str, wallet: str) -> Optional[WalletStats]:
        if venue == "Hyperliquid":
            return await self._fetch_hl(wallet)
        if venue == "JupiterPerps":
            return await self._fetch_jup(wallet)
        if venue == "Drift":
            return await self._fetch_drift(wallet)
        return None

    # ── Hyperliquid ───────────────────────────────────────────────────────────
    async def _fetch_hl(self, wallet: str) -> Optional[WalletStats]:
        assert self._session is not None
        try:
            async with self._session.post(
                self.hl_info_url,
                json={"type": "userFills", "user": wallet},
                timeout=10,
            ) as r:
                r.raise_for_status()
                fills = await r.json()
        except Exception as exc:
            log.debug("HL userFills failed for %s: %s", wallet, exc)
            return None

        if not isinstance(fills, list):
            return None

        return _stats_from_hl_fills("Hyperliquid", wallet, fills)

    # ── Jupiter Perps ─────────────────────────────────────────────────────────
    async def _fetch_jup(self, wallet: str) -> Optional[WalletStats]:
        try:
            sigs_resp = await self._sol_rpc(
                "getSignaturesForAddress",
                [wallet, {"limit": self.jup_sig_limit}],
            )
        except Exception as exc:
            log.debug("Jup getSignaturesForAddress failed for %s: %s", wallet, exc)
            return None
        if not isinstance(sigs_resp, list) or not sigs_resp:
            return _empty_stats("JupiterPerps", wallet, "(no recent signatures)")

        signatures = [
            s["signature"] for s in sigs_resp
            if isinstance(s, dict) and not s.get("err") and s.get("signature")
        ][: self.jup_max_tx]

        if not signatures:
            return _empty_stats("JupiterPerps", wallet, "(no successful txs in window)")

        sem = asyncio.Semaphore(self.jup_conc)

        async def fetch_one(sig: str) -> Optional[dict]:
            async with sem:
                try:
                    return await self._sol_rpc(
                        "getTransaction",
                        [
                            sig,
                            {
                                "encoding": "json",
                                "maxSupportedTransactionVersion": 0,
                                "commitment": "confirmed",
                            },
                        ],
                    )
                except Exception:
                    return None

        txs = await asyncio.gather(*(fetch_one(s) for s in signatures))
        return _stats_from_jup_txs("JupiterPerps", wallet, txs, len(signatures))

    # ── Drift Protocol ────────────────────────────────────────────────────────
    async def _fetch_drift(self, wallet: str) -> Optional[WalletStats]:
        """Pull recent fills for a Drift wallet from the public Data API.

        Drift doesn't expose a ready-made `closedPnl` per fill the way HL does
        and there's no settlePnl record endpoint, so realized PnL would
        require pairing entries+exits ourselves. For now we report the stats
        we *can* derive cheaply (volume, fill count, fees, days active) and
        bucket the wallet into the NEW tier so the dashboard shows it
        without pretending to know win-rate.
        """
        assert self._session is not None
        url = f"{self.drift_data_api}/user/{wallet}/trades"
        params = {"pageIndex": "0", "pageSize": str(self.drift_page_size)}
        try:
            async with self._session.get(
                url, params=params,
                headers={
                    "User-Agent": "perps-whale-tracker/1.0",
                    "Accept": "application/json",
                },
                timeout=10,
            ) as r:
                if r.status != 200:
                    return _empty_stats(
                        "Drift", wallet,
                        f"(drift API → {r.status})",
                    )
                payload = await r.json()
        except Exception as exc:
            log.debug("Drift trades fetch failed for %s: %s", wallet, exc)
            return None

        fills = payload.get("records") or []
        if not fills:
            return _empty_stats("Drift", wallet, "(no recent fills)")
        return _stats_from_drift_fills(wallet, fills)

    async def _sol_rpc(self, method: str, params: list[Any]) -> Any:
        assert self._session is not None
        async with self._session.post(
            self.sol_rpc_http,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=15,
        ) as r:
            r.raise_for_status()
            body = await r.json()
            if "error" in body and body["error"]:
                raise RuntimeError(str(body["error"])[:200])
            return body.get("result")


# ── Pure helpers (easy to unit-test) ──────────────────────────────────────────
def _empty_stats(venue: str, wallet: str, note: str) -> WalletStats:
    return WalletStats(
        venue=venue, wallet=wallet,
        closed_trades=0, win_count=0,
        realized_pnl_usd=0.0, pnl_30d_usd=0.0,
        largest_win_usd=0.0, largest_loss_usd=0.0,
        avg_trade_pnl_usd=0.0,
        first_trade_ts_ms=None,
        fee_paid_usd=0.0,
        coverage_note=note,
    )


def _hl_side_from_dir(d: Optional[str]) -> str:
    """Map Hyperliquid `dir` strings to "long"/"short" of the *closed* position."""
    if not d:
        return "long"
    s = d.lower()
    if "close" in s:
        return "long" if "long" in s else "short"
    if "long" in s:
        return "long"
    if "short" in s:
        return "short"
    return "long"


def _stats_from_hl_fills(
    venue: str, wallet: str, fills: list[dict],
) -> WalletStats:
    if not fills:
        return _empty_stats(venue, wallet, "(no fills)")

    now_ms = int(time.time() * 1000)
    cutoff_30d_ms = now_ms - 30 * 86_400_000
    cutoff_7d_ms = now_ms - 7 * 86_400_000

    closed_pnls: list[float] = []
    pnl_30d = 0.0
    pnl_7d = 0.0
    fee_total = 0.0
    gross_wins = 0.0
    gross_losses = 0.0
    first_ts: Optional[int] = None

    by_coin: dict[str, CoinPnL] = {}
    closed_records: list[ClosedTradeRecord] = []

    for f in fills:
        try:
            t = int(f.get("time", 0) or 0)
            pnl = float(f.get("closedPnl", "0") or "0")
            fee = float(f.get("fee", "0") or "0")
            coin = f.get("coin") or "?"
            px = float(f.get("px", "0") or "0")
            sz = float(f.get("sz", "0") or "0")
            d = f.get("dir") or ""
        except (ValueError, TypeError):
            continue

        fee_total += fee
        if t and (first_ts is None or t < first_ts):
            first_ts = t

        if pnl == 0.0:
            continue  # opening fills don't realize anything

        closed_pnls.append(pnl)
        if pnl > 0:
            gross_wins += pnl
        else:
            gross_losses += pnl
        if t >= cutoff_30d_ms:
            pnl_30d += pnl
        if t >= cutoff_7d_ms:
            pnl_7d += pnl

        side = _hl_side_from_dir(d)
        cp = by_coin.get(coin) or CoinPnL(coin=coin, trades=0, win_count=0, pnl_usd=0.0)
        cp.trades += 1
        if pnl > 0:
            cp.win_count += 1
        cp.pnl_usd += pnl
        by_coin[coin] = cp

        closed_records.append(ClosedTradeRecord(
            ts_ms=t, coin=coin, side=side,
            size=sz, price_usd=px, pnl_usd=pnl, fee_usd=fee,
        ))

    closed_trades = len(closed_pnls)
    win_count = sum(1 for p in closed_pnls if p > 0)
    realized = sum(closed_pnls)
    largest_win = max(closed_pnls) if closed_pnls else 0.0
    largest_loss = min(closed_pnls) if closed_pnls else 0.0
    avg = realized / closed_trades if closed_trades else 0.0

    closed_records.sort(key=lambda r: r.ts_ms, reverse=True)

    coverage = f"(last {len(fills)} fills)" if len(fills) >= 2000 else None

    return WalletStats(
        venue=venue, wallet=wallet,
        closed_trades=closed_trades, win_count=win_count,
        realized_pnl_usd=realized, pnl_30d_usd=pnl_30d,
        largest_win_usd=largest_win, largest_loss_usd=largest_loss,
        avg_trade_pnl_usd=avg,
        first_trade_ts_ms=first_ts,
        fee_paid_usd=fee_total,
        coverage_note=coverage,
        gross_wins_usd=gross_wins,
        gross_losses_usd=gross_losses,
        pnl_7d_usd=pnl_7d,
        recent_trades=closed_records[:200],
        by_coin=by_coin,
    )


def _stats_from_jup_txs(
    venue: str,
    wallet: str,
    txs: list[Optional[dict]],
    sig_total: int,
) -> WalletStats:
    closed_pnls: list[float] = []
    pnl_30d = 0.0
    pnl_7d = 0.0
    fee_total = 0.0
    gross_wins = 0.0
    gross_losses = 0.0
    first_ts: Optional[int] = None
    now_ms = int(time.time() * 1000)
    cutoff_30d_ms = now_ms - 30 * 86_400_000
    cutoff_7d_ms = now_ms - 7 * 86_400_000
    fetched = sum(1 for tx in txs if tx)

    by_coin: dict[str, CoinPnL] = {}
    closed_records: list[ClosedTradeRecord] = []

    for tx in txs:
        if not tx:
            continue
        try:
            block_time_ms = int(tx.get("blockTime") or 0) * 1000
            logs = (tx.get("meta") or {}).get("logMessages") or []
        except Exception:
            continue
        for line in logs:
            if not line.startswith("Program data: "):
                continue
            decoded = _try_decode_program_data(line[len("Program data: "):])
            if not isinstance(decoded, (_DecreaseEvent, _LiquidateEvent)):
                continue
            if decoded.owner != wallet:
                continue
            pnl = decoded.pnl_delta / USD_ATOMIC * (1 if decoded.has_profit else -1)
            fee = decoded.fee_usd / USD_ATOMIC
            fee_total += fee
            closed_pnls.append(pnl)
            if pnl > 0:
                gross_wins += pnl
            else:
                gross_losses += pnl
            if block_time_ms and block_time_ms >= cutoff_30d_ms:
                pnl_30d += pnl
            if block_time_ms and block_time_ms >= cutoff_7d_ms:
                pnl_7d += pnl
            if block_time_ms and (first_ts is None or block_time_ms < first_ts):
                first_ts = block_time_ms

            coin = CUSTODY_TO_COIN.get(decoded.position_custody, decoded.position_custody[:6] + "…")
            side = "long" if decoded.side == 1 else "short"
            price_usd = decoded.price / USD_ATOMIC
            # For Decrease, sizeUsdDelta is the trade size; for Liquidate, the
            # whole position. Both live in different fields; pull what's there.
            size_usd = (
                decoded.size_usd_delta if isinstance(decoded, _DecreaseEvent)
                else decoded.position_size_usd
            ) / USD_ATOMIC
            size_base = (size_usd / price_usd) if price_usd else 0.0

            cp = by_coin.get(coin) or CoinPnL(coin=coin, trades=0, win_count=0, pnl_usd=0.0)
            cp.trades += 1
            if pnl > 0:
                cp.win_count += 1
            cp.pnl_usd += pnl
            by_coin[coin] = cp

            closed_records.append(ClosedTradeRecord(
                ts_ms=block_time_ms, coin=coin, side=side,
                size=size_base, price_usd=price_usd, pnl_usd=pnl, fee_usd=fee,
            ))

    closed_trades = len(closed_pnls)
    win_count = sum(1 for p in closed_pnls if p > 0)
    realized = sum(closed_pnls)
    largest_win = max(closed_pnls) if closed_pnls else 0.0
    largest_loss = min(closed_pnls) if closed_pnls else 0.0
    avg = realized / closed_trades if closed_trades else 0.0

    closed_records.sort(key=lambda r: r.ts_ms, reverse=True)

    return WalletStats(
        venue=venue, wallet=wallet,
        closed_trades=closed_trades, win_count=win_count,
        realized_pnl_usd=realized, pnl_30d_usd=pnl_30d,
        largest_win_usd=largest_win, largest_loss_usd=largest_loss,
        avg_trade_pnl_usd=avg,
        first_trade_ts_ms=first_ts,
        fee_paid_usd=fee_total,
        coverage_note=f"(scanned {fetched}/{sig_total} recent sigs)",
        gross_wins_usd=gross_wins,
        gross_losses_usd=gross_losses,
        pnl_7d_usd=pnl_7d,
        recent_trades=closed_records[:200],
        by_coin=by_coin,
    )


def _drift_coin_from_symbol(symbol: str) -> str:
    """Match the live tracker's mapping (`1MPEPE-PERP` → `PEPE`)."""
    base = (symbol or "").split("-", 1)[0].upper()
    if base.startswith(("1M", "1K")):
        return base[2:]
    return base


def _stats_from_drift_fills(wallet: str, fills: list[dict]) -> WalletStats:
    """Best-effort stats for a Drift wallet based on fill history alone.

    We can't compute realized PnL without pairing entries+exits (Drift's data
    API doesn't expose `closedPnl` per fill), so this function reports
    coverage rather than performance:
    * `closed_trades` = number of fills observed
    * `realized_pnl_usd` = 0 (so the wallet falls into the NEW tier, which
      the dashboard already labels with a "?" emoji)
    * `fee_paid_usd` = sum of taker fees + maker rebates (rebates are
      negative so they correctly *reduce* fee paid)
    * `by_coin` aggregates fill *count* per market so the wallet drilldown
      still tells you what they trade.

    The coverage note flags this as "(no realized PnL — fills only)" so the
    UI is honest about why win-rate / PnL are blank.
    """
    if not fills:
        return _empty_stats("Drift", wallet, "(no fills)")

    fill_count = 0
    fee_total = 0.0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    by_coin: dict[str, CoinPnL] = {}
    recent_records: list[ClosedTradeRecord] = []

    for f in fills:
        try:
            ts_ms = int(f.get("ts") or 0) * 1000
        except (TypeError, ValueError):
            ts_ms = 0
        try:
            quote = float(f.get("quoteAssetAmountFilled") or 0.0)
            base = float(f.get("baseAssetAmountFilled") or 0.0)
            oracle = float(f.get("oraclePrice") or 0.0)
        except (TypeError, ValueError):
            quote = base = oracle = 0.0
        if quote <= 0:
            continue
        fill_count += 1
        # Taker pays fee (positive); maker gets rebate (negative). Whichever
        # role this wallet played, sum that field — the API is consistent
        # about which "side" it reports relative to the user being queried.
        is_taker = (f.get("taker") == wallet)
        try:
            if is_taker:
                fee_total += float(f.get("takerFee") or 0.0)
            else:
                fee_total += float(f.get("makerFee") or f.get("makerRebate") or 0.0)
        except (TypeError, ValueError):
            pass

        if ts_ms:
            if first_ts is None or ts_ms < first_ts:
                first_ts = ts_ms
            if last_ts is None or ts_ms > last_ts:
                last_ts = ts_ms

        symbol = f.get("symbol") or ""
        coin = _drift_coin_from_symbol(symbol)
        side = (
            (f.get("takerOrderDirection") or "long") if is_taker
            else (f.get("makerOrderDirection") or "short")
        )

        cp = by_coin.get(coin) or CoinPnL(coin=coin, trades=0, win_count=0, pnl_usd=0.0)
        cp.trades += 1
        by_coin[coin] = cp

        if len(recent_records) < 200:
            price = oracle if oracle > 0 else (quote / base if base > 0 else 0.0)
            recent_records.append(ClosedTradeRecord(
                ts_ms=ts_ms, coin=coin, side=side,
                size=base, price_usd=price,
                pnl_usd=0.0,  # not available
                fee_usd=0.0,  # already accumulated globally
            ))

    recent_records.sort(key=lambda r: r.ts_ms, reverse=True)

    return WalletStats(
        venue="Drift", wallet=wallet,
        closed_trades=fill_count,
        win_count=0,                    # unknowable without PnL pairing
        realized_pnl_usd=0.0,           # unknowable
        pnl_30d_usd=None,
        largest_win_usd=0.0,
        largest_loss_usd=0.0,
        avg_trade_pnl_usd=0.0,
        first_trade_ts_ms=first_ts,
        fee_paid_usd=fee_total,
        coverage_note=f"(last {fill_count} fills — Drift API doesn't expose realized PnL)",
        gross_wins_usd=0.0,
        gross_losses_usd=0.0,
        pnl_7d_usd=None,
        recent_trades=recent_records,
        by_coin=by_coin,
    )
