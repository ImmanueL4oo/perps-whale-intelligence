"""Tiny aiohttp-based Solana JSON-RPC client tuned for whale enrichment.

We use this to fetch full transactions when a `logsNotification` only carries
the signature (e.g. Jupiter Perps' `Instant*` instructions, which don't emit
Anchor events). Public RPC endpoints are heavily rate-limited so this client:

* keeps a single `aiohttp.ClientSession` open for connection re-use;
* retries with exponential backoff on 429 / network errors;
* treats `getTransaction` returning `null` (slot not yet finalized) as a
  retriable condition.

Designed to be *small* — anchorpy / solana-py would pull a serious dep tree
for what is essentially three POSTs.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from ssl_ctx import get_ssl_context

log = logging.getLogger("solana_rpc")


class SolanaRpc:
    def __init__(
        self,
        http_url: str = "https://api.mainnet-beta.solana.com",
        *,
        max_retries: int = 4,
        base_backoff_sec: float = 0.6,
        request_timeout_sec: float = 10.0,
    ) -> None:
        self.http_url = http_url
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self.request_timeout_sec = request_timeout_sec
        self._session: Optional[aiohttp.ClientSession] = None
        # Diagnostic counters surfaced via .stats().
        self.stats = {
            "calls": 0,
            "ok": 0,
            "rate_limited": 0,
            "errors": 0,
            "null_results": 0,
        }

    async def __aenter__(self) -> "SolanaRpc":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def start(self) -> None:
        if self._session is None:
            connector = aiohttp.TCPConnector(ssl=get_ssl_context(), limit=8)
            self._session = aiohttp.ClientSession(connector=connector)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def call(self, method: str, params: list[Any]) -> Optional[Any]:
        """Make a JSON-RPC call with retries.

        Returns the `result` field on success; None if every retry exhausts
        without a usable answer. Treats 429 / 502 / 503 specially with a
        slightly more aggressive backoff because public RPC clusters are
        the #1 source of spurious failures.
        """
        await self.start()
        assert self._session is not None
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        backoff = self.base_backoff_sec
        for attempt in range(self.max_retries + 1):
            self.stats["calls"] += 1
            try:
                async with self._session.post(
                    self.http_url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout_sec),
                ) as r:
                    if r.status == 200:
                        j = await r.json()
                        if "error" in j:
                            log.debug(
                                "rpc %s error: %s", method,
                                j["error"].get("message"),
                            )
                            self.stats["errors"] += 1
                        else:
                            res = j.get("result")
                            if res is None:
                                # Common case for getTransaction when the
                                # slot is still in-flight; retry shortly.
                                self.stats["null_results"] += 1
                            else:
                                self.stats["ok"] += 1
                                return res
                    elif r.status in (429, 502, 503, 504):
                        self.stats["rate_limited"] += 1
                        # Tighten backoff on rate-limit specifically.
                        backoff = max(backoff, 1.0)
                    else:
                        body = await r.text()
                        log.debug(
                            "rpc %s -> %s: %s",
                            method, r.status, body[:200],
                        )
                        self.stats["errors"] += 1
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                log.debug("rpc %s network error: %s", method, exc)
                self.stats["errors"] += 1
            if attempt < self.max_retries:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.8, 8.0)
        return None

    # ── convenience wrappers ─────────────────────────────────────────────────
    async def get_transaction(self, signature: str) -> Optional[dict]:
        """Fetch a parsed transaction with v0 support (lookup tables included)."""
        return await self.call("getTransaction", [
            signature,
            {
                "encoding": "json",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ])

    async def get_account_info(self, pubkey: str) -> Optional[dict]:
        """Fetch a single account. Returns the `result.value` dict (or None)."""
        res = await self.call("getAccountInfo", [
            pubkey,
            {"encoding": "base64", "commitment": "confirmed"},
        ])
        if isinstance(res, dict):
            return res.get("value")
        return None

    @staticmethod
    def all_account_keys(tx: dict) -> list[str]:
        """Return the full account-keys array for a (possibly v0) transaction.

        v0 transactions split keys between `message.accountKeys` (static) and
        `meta.loadedAddresses` (resolved from address lookup tables). To do
        anything useful with `instructions[*].accounts` we need the merged
        list in the right order: static first, then writable LUT, then
        readonly LUT. (Solana spec.)
        """
        msg = (tx.get("transaction") or {}).get("message") or {}
        keys = list(msg.get("accountKeys") or [])
        meta = tx.get("meta") or {}
        loaded = meta.get("loadedAddresses") or {}
        keys.extend(loaded.get("writable") or [])
        keys.extend(loaded.get("readonly") or [])
        return keys


__all__ = ["SolanaRpc"]
