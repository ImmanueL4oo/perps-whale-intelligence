"""Async USD price feed for SPL tokens.

Used by the CEX flow tracker (and any future cross-asset analytics) to
attach a USD valuation to on-chain transfers without bloating the
dependency tree.

Strategy:

1. **Stables short-circuit** — anything tagged `is_stable` in the local
   registry returns 1.0 instantly.
2. **Local cache with TTL** — Jupiter's Price API rate-limits aggressive
   callers, so we serve cached prices for `ttl_sec` (default 30s) before
   refetching. The cache is keyed by mint pubkey.
3. **Single-flight** — concurrent `get_usd()` calls for the same mint
   don't fan out to N HTTP calls; they all wait on the first request.
4. **Batched fetch** — when the cache misses for many mints simultaneously
   (e.g. dashboard backfill), we coalesce within a 30ms window and ask
   Jupiter for them as a single comma-separated request.
5. **Stale-while-error** — if the upstream is down, we serve the last
   known price rather than zero. Better to undervalue a flow than to
   silently drop it from analytics.

Designed to be `async with PriceFeed(...) as pf:` or constructed once and
shared by every consumer.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from cex_wallets import lookup_token
from ssl_ctx import get_ssl_context

log = logging.getLogger("pricing")


JUPITER_PRICE_URL = "https://api.jup.ag/price/v2"


class PriceFeed:
    def __init__(
        self,
        *,
        url: str = JUPITER_PRICE_URL,
        ttl_sec: float = 30.0,
        stale_grace_sec: float = 600.0,
        request_timeout_sec: float = 5.0,
        coalesce_window_ms: int = 30,
    ) -> None:
        self.url = url
        self.ttl_sec = ttl_sec
        self.stale_grace_sec = stale_grace_sec
        self.request_timeout_sec = request_timeout_sec
        self.coalesce_window = coalesce_window_ms / 1000.0
        self._session: Optional[aiohttp.ClientSession] = None
        # mint -> (price_usd, fetched_at)
        self._cache: dict[str, tuple[float, float]] = {}
        # Pending (un-resolved) requests so concurrent waiters share one fetch.
        self._inflight: dict[str, asyncio.Future[Optional[float]]] = {}
        # Coalescer: a tiny background task waits up to coalesce_window for
        # additional mints to join the next batch request.
        self._batch_lock = asyncio.Lock()
        self.stats = {"hits": 0, "miss": 0, "stale": 0, "errors": 0, "fetches": 0}

    async def __aenter__(self) -> "PriceFeed":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def start(self) -> None:
        if self._session is None:
            connector = aiohttp.TCPConnector(ssl=get_ssl_context(), limit=8)
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={"User-Agent": "perps-whale-tracker/1.0"},
            )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ── Public API ──────────────────────────────────────────────────────────
    async def get_usd(self, mint: str) -> Optional[float]:
        """Return USD price for a mint, or None if unknown.

        Stables short-circuit. Cache hit returns immediately. Cache miss
        coalesces with concurrent waiters and ends in a single HTTP call.
        """
        token = lookup_token(mint)
        if token is not None and token.is_stable:
            return 1.0

        now = time.monotonic()
        cached = self._cache.get(mint)
        if cached is not None and (now - cached[1]) < self.ttl_sec:
            self.stats["hits"] += 1
            return cached[0]

        # Not fresh; fall through to fetch (sharing in-flight if present).
        self.stats["miss"] += 1
        if mint in self._inflight:
            return await self._inflight[mint]
        fut: asyncio.Future[Optional[float]] = asyncio.get_running_loop().create_future()
        self._inflight[mint] = fut
        price: Optional[float] = None
        try:
            price = await self._fetch_one(mint)
        except Exception as exc:
            self.stats["errors"] += 1
            log.debug("price fetch failed for %s: %s", mint, exc)
            price = None
        # Stale-grace: if the fetch came back without a usable price (either
        # raised, or the upstream returned an empty body / 5xx) but we have
        # a recent-ish cache entry, serve that rather than None. Better to
        # undervalue a flow than to drop it from analytics.
        if price is None:
            stale = self._cache.get(mint)
            if stale is not None and (now - stale[1]) < self.stale_grace_sec:
                self.stats["stale"] += 1
                price = stale[0]
        fut.set_result(price)
        self._inflight.pop(mint, None)
        return price

    async def get_usd_many(self, mints: list[str]) -> dict[str, Optional[float]]:
        """Convenience batched fetch. Mints with stable metadata return 1.0
        without a network call. Unknown mints get None."""
        out: dict[str, Optional[float]] = {}
        need: list[str] = []
        now = time.monotonic()
        for m in mints:
            tok = lookup_token(m)
            if tok is not None and tok.is_stable:
                out[m] = 1.0
                continue
            cached = self._cache.get(m)
            if cached is not None and (now - cached[1]) < self.ttl_sec:
                out[m] = cached[0]
                continue
            need.append(m)
        if need:
            fetched = await self._fetch_many(need)
            out.update(fetched)
        return out

    # ── Implementation ──────────────────────────────────────────────────────
    async def _fetch_one(self, mint: str) -> Optional[float]:
        # Sleep briefly to give other concurrent waiters a chance to join
        # the same batch — this is what cuts a burst of N calls down to 1.
        await asyncio.sleep(self.coalesce_window)
        async with self._batch_lock:
            # Re-check cache after the sleep — a peer may have populated it.
            now = time.monotonic()
            cached = self._cache.get(mint)
            if cached is not None and (now - cached[1]) < self.ttl_sec:
                return cached[0]
            mints = [mint] + [
                m for m in self._inflight.keys()
                if m != mint and m not in self._cache
            ]
            results = await self._fetch_many(mints)
            return results.get(mint)

    async def _fetch_many(self, mints: list[str]) -> dict[str, Optional[float]]:
        """Hit Jupiter Price API once for up to ~50 mints."""
        await self.start()
        assert self._session is not None
        out: dict[str, Optional[float]] = {m: None for m in mints}
        if not mints:
            return out
        # Jupiter accepts comma-separated `ids`. Cap at 50 per call to be safe.
        for i in range(0, len(mints), 50):
            chunk = mints[i:i + 50]
            params = {"ids": ",".join(chunk)}
            self.stats["fetches"] += 1
            try:
                async with self._session.get(
                    self.url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout_sec),
                ) as r:
                    if r.status != 200:
                        log.debug("jupiter price %s -> %s", chunk[:3], r.status)
                        self.stats["errors"] += 1
                        continue
                    j = await r.json()
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                log.debug("jupiter price network error: %s", exc)
                self.stats["errors"] += 1
                continue
            data = (j or {}).get("data") or {}
            now = time.monotonic()
            for m in chunk:
                row = data.get(m)
                price: Optional[float] = None
                if isinstance(row, dict):
                    raw = row.get("price")
                    try:
                        price = float(raw) if raw is not None else None
                    except (TypeError, ValueError):
                        price = None
                if price is not None:
                    self._cache[m] = (price, now)
                    out[m] = price
        return out


__all__ = ["PriceFeed", "JUPITER_PRICE_URL"]
