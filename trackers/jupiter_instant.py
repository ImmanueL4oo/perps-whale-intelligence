"""Decoder for Jupiter Perpetuals' `Instant*` instructions.

The original `JupiterPerpsTracker` decodes Anchor `Program data:` events that
the legacy `IncreasePosition` / `DecreasePosition` / `LiquidateFullPosition`
instructions emit. In April 2025 Jupiter shipped an "instant" pathway
(`InstantIncreasePosition`, `InstantDecreasePosition`, and the `*PreSwap`
variants) which sidestep the keeper-bot flow and **don't emit `Program data:`
lines**, leaving our existing tracker blind for that path.

This module fills the gap. When the live tracker spots `Instruction: Instant*`
in the log of a confirmed transaction it asks us to enrich the event from the
chain itself:

1. Fetch the full transaction with `getTransaction` (we need the v0 account
   table to resolve LUT-loaded keys).
2. Locate the Jupiter Perps top-level instruction (programId match).
3. Base58-decode the instruction data — first 8 bytes are the Anchor
   discriminator, the next 8 are `sizeUsdDelta: u64` (little-endian, atomic
   USDC i.e. 6 decimals). For `InstantDecrease*` the same offset is the
   *decrease* size.
4. Scan the instruction's account list against the well-known custody map to
   identify the coin (one of SOL / ETH / BTC) and infer side from the presence
   of a stablecoin custody (USDC/USDT means SHORT; otherwise LONG).
5. The first signer (`message.accountKeys[0]`) is the position owner.

What we *don't* try to recover here:
- The exact mid price (we leave `price_usd = 0.0`; the dashboard lifecycle
  layer can substitute the oracle price when needed).
- TP/SL trigger details (those flow through their own request types).

This decoder is intentionally tolerant: any field we can't decode means we
return `None` rather than emit garbage. The fallback path is the existing
"warn the operator" log message.
"""
from __future__ import annotations

import hashlib
import logging
import struct
import time
from dataclasses import dataclass
from typing import Optional

from trackers.solana_rpc import SolanaRpc

log = logging.getLogger("jupiter_instant")

JUPITER_PERPS_PROGRAM_ID = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
USD_DECIMALS = 6
USD_ATOMIC = 10 ** USD_DECIMALS

# Stable custodies are used as collateral for SHORT positions. Their presence
# in the instruction account list is the simplest, most reliable side hint
# (the alternative — decoding the Position account state — needs a separate
# RPC fetch and only confirms post-trade state).
STABLE_CUSTODIES = frozenset({
    "G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa",  # USDC
    "4vkNeXiYEUizLdrpdPS1eC2mccyM4NUPRtERrk6ZETkk",  # USDT
})

# Asset custodies (long-leg). Exposed so the live tracker can share one map.
ASSET_CUSTODY_TO_COIN: dict[str, str] = {
    "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz": "SOL",
    "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn": "ETH",
    "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm": "BTC",
}

# Anchor discriminators: sha256("global:<snake_case>")[:8]. Computed at import
# so a typo here is loud, not silent. We accept all known Instant* variants —
# Jupiter has been shipping new ones every few months.
_INSTANT_INSTRUCTION_NAMES = (
    "instant_increase_position",
    "instant_decrease_position",
    "instant_increase_position_pre_swap",
    "instant_decrease_position_pre_swap",
    "instant_increase_position_2",
    "instant_decrease_position_2",
    "instant_create_tp_sl",
    "instant_update_tp_sl",
    "instant_close_position_request",
)


def _ix_disc(name: str) -> bytes:
    return hashlib.sha256(f"global:{name}".encode()).digest()[:8]


_DISC_TO_KIND: dict[bytes, str] = {}
for _name in _INSTANT_INSTRUCTION_NAMES:
    _DISC_TO_KIND[_ix_disc(_name)] = _name


# ── Base58 decoder (no external deps) ────────────────────────────────────────
_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def b58decode(s: str) -> bytes:
    """Decode a base58 string. Raises ValueError on invalid input."""
    if not s:
        return b""
    n = 0
    for ch in s.encode():
        v = _B58_INDEX.get(ch)
        if v is None:
            raise ValueError(f"invalid base58 character: {chr(ch)!r}")
        n = n * 58 + v
    # Count leading 1's (zero bytes).
    pad = 0
    for ch in s.encode():
        if ch == ord("1"):
            pad += 1
        else:
            break
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * pad + body


# ── Decoded payload ──────────────────────────────────────────────────────────
@dataclass
class InstantDecoded:
    kind: str            # one of _INSTANT_INSTRUCTION_NAMES
    is_increase: bool
    is_decrease: bool
    size_usd_delta: int  # atomic 6-decimal USDC
    owner: str           # base58 wallet
    coin: Optional[str]  # "SOL" / "ETH" / "BTC" / None if no asset custody matched
    custody: Optional[str]
    side: Optional[str]  # "long" / "short" / None if undetermined
    signature: str

    @property
    def notional_usd(self) -> float:
        return self.size_usd_delta / USD_ATOMIC


def decode_jupiter_tx(tx: dict, signature: str) -> Optional[InstantDecoded]:
    """Walk a parsed `getTransaction` response and decode the first Jupiter
    Perps Instant* instruction we recognize. Returns None if nothing usable.

    Args:
        tx: The `result` dict from a `getTransaction` JSON-RPC call.
        signature: The transaction signature (for record-keeping).
    """
    if not tx:
        return None
    keys = SolanaRpc.all_account_keys(tx)
    if not keys:
        return None
    msg = (tx.get("transaction") or {}).get("message") or {}
    instructions = msg.get("instructions") or []
    if not instructions:
        return None

    owner = keys[0]  # first signer

    for ix in instructions:
        prog_idx = ix.get("programIdIndex")
        if prog_idx is None or prog_idx >= len(keys):
            continue
        if keys[prog_idx] != JUPITER_PERPS_PROGRAM_ID:
            continue
        data_b58 = ix.get("data") or ""
        try:
            raw = b58decode(data_b58)
        except ValueError:
            continue
        if len(raw) < 8:
            continue
        disc = raw[:8]
        kind = _DISC_TO_KIND.get(disc)
        if kind is None:
            continue
        is_increase = kind.startswith("instant_increase")
        is_decrease = kind.startswith("instant_decrease")
        # The first u64 of args is sizeUsdDelta for both Increase and Decrease.
        # Older versions used a leading bool/u8 — guard against that by
        # falling back to scanning the next 8 bytes if the first looks
        # implausibly large.
        size_usd_delta = 0
        if len(raw) >= 16:
            try:
                v = struct.unpack("<Q", raw[8:16])[0]
                # Sanity: real position sizes are 1 USDC (1e6) … 200M USDC (2e14).
                if 1 * USD_ATOMIC <= v <= 200_000_000 * USD_ATOMIC:
                    size_usd_delta = v
            except struct.error:
                pass
        if size_usd_delta == 0 and len(raw) >= 17:
            try:
                v = struct.unpack("<Q", raw[9:17])[0]
                if 1 * USD_ATOMIC <= v <= 200_000_000 * USD_ATOMIC:
                    size_usd_delta = v
            except struct.error:
                pass

        # Resolve the instruction's accounts.
        ix_accounts: list[str] = []
        for idx in ix.get("accounts") or []:
            if 0 <= idx < len(keys):
                ix_accounts.append(keys[idx])

        # Find an asset custody (one of SOL/ETH/BTC) and a stable custody
        # (USDC/USDT). Side: stable present → SHORT, otherwise LONG.
        coin: Optional[str] = None
        custody: Optional[str] = None
        has_stable = False
        for acc in ix_accounts:
            if acc in ASSET_CUSTODY_TO_COIN and coin is None:
                coin = ASSET_CUSTODY_TO_COIN[acc]
                custody = acc
            elif acc in STABLE_CUSTODIES:
                has_stable = True

        side: Optional[str]
        if coin is None:
            side = None  # can't infer without the asset custody anchor
        else:
            side = "short" if has_stable else "long"

        return InstantDecoded(
            kind=kind,
            is_increase=is_increase,
            is_decrease=is_decrease,
            size_usd_delta=size_usd_delta,
            owner=owner,
            coin=coin,
            custody=custody,
            side=side,
            signature=signature,
        )
    return None


async def fetch_and_decode(
    rpc: SolanaRpc, signature: str,
) -> Optional[InstantDecoded]:
    """Convenience: fetch + decode in one call. Returns None on either failure."""
    tx = await rpc.get_transaction(signature)
    if not tx:
        return None
    try:
        return decode_jupiter_tx(tx, signature)
    except Exception:
        log.debug("failed to decode jupiter tx %s", signature, exc_info=True)
        return None


__all__ = [
    "InstantDecoded",
    "JUPITER_PERPS_PROGRAM_ID",
    "ASSET_CUSTODY_TO_COIN",
    "STABLE_CUSTODIES",
    "USD_ATOMIC",
    "b58decode",
    "decode_jupiter_tx",
    "fetch_and_decode",
]
