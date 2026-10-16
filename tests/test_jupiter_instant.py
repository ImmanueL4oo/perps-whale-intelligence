"""Tests for the Jupiter Perps Instant* transaction decoder.

The decoder is the lifeline that re-enables coverage for Jupiter's "instant"
order path (which doesn't emit Anchor `Program data:` events).
"""
from __future__ import annotations

import hashlib
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trackers.jupiter_instant import (
    ASSET_CUSTODY_TO_COIN,
    JUPITER_PERPS_PROGRAM_ID,
    STABLE_CUSTODIES,
    USD_ATOMIC,
    b58decode,
    decode_jupiter_tx,
)


# ── b58 round-trip ───────────────────────────────────────────────────────────
def test_b58decode_known_value():
    # "1" decodes to the single zero byte in Solana's flavour of base58.
    assert b58decode("1") == b"\x00"
    # "11" → b"\x00\x00".
    assert b58decode("11") == b"\x00\x00"


def test_b58decode_pubkey_roundtrip():
    # Re-implement the encoder from jupiter_perps._b58 to round-trip a known
    # 32-byte pubkey through both directions.
    from trackers.jupiter_perps import _b58
    pk = bytes(range(32))
    assert b58decode(_b58(pk)) == pk


# ── instruction-data construction helpers ───────────────────────────────────
def _disc(name: str) -> bytes:
    return hashlib.sha256(f"global:{name}".encode()).digest()[:8]


def _encode_b58(raw: bytes) -> str:
    """Mini b58 encoder so tests can build their own instruction data."""
    alphabet = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(raw, "big")
    out = b""
    while n:
        n, r = divmod(n, 58)
        out = alphabet[r:r+1] + out
    pad = 0
    for byte in raw:
        if byte == 0:
            pad += 1
        else:
            break
    return (b"1" * pad + out).decode()


SOL_CUSTODY = next(
    c for c, name in ASSET_CUSTODY_TO_COIN.items() if name == "SOL"
)
USDC_CUSTODY = sorted(STABLE_CUSTODIES)[0]
OWNER = "FakeOwner1111111111111111111111111111111111"
DUMMY = "1111111111111111111111111111111111111111111"


def _make_tx(
    *,
    discriminator: bytes,
    size_usd_delta: int,
    accounts_pubkeys: list[str],
    owner: str = OWNER,
) -> dict:
    """Build a minimal `getTransaction` response shaped exactly like the
    Solana JSON-RPC. Mirrors what real public-RPC payloads look like."""
    raw = discriminator + struct.pack("<Q", size_usd_delta) + b"\x00" * 8
    data_b58 = _encode_b58(raw)
    keys = [owner, JUPITER_PERPS_PROGRAM_ID] + accounts_pubkeys + [DUMMY]
    program_idx = 1
    account_indices = [
        keys.index(p) if p in keys else len(keys) - 1
        for p in accounts_pubkeys
    ]
    return {
        "transaction": {
            "message": {
                "accountKeys": keys,
                "instructions": [
                    {
                        "programIdIndex": program_idx,
                        "accounts": account_indices,
                        "data": data_b58,
                    }
                ],
            }
        },
        "meta": {"loadedAddresses": {"writable": [], "readonly": []}},
    }


# ── decode_jupiter_tx ────────────────────────────────────────────────────────
def test_decode_increase_long_sol():
    tx = _make_tx(
        discriminator=_disc("instant_increase_position"),
        size_usd_delta=2_500_000 * USD_ATOMIC,
        accounts_pubkeys=[SOL_CUSTODY],  # no stable -> long
    )
    decoded = decode_jupiter_tx(tx, "sigA")
    assert decoded is not None
    assert decoded.is_increase
    assert decoded.coin == "SOL"
    assert decoded.custody == SOL_CUSTODY
    assert decoded.side == "long"
    assert decoded.size_usd_delta == 2_500_000 * USD_ATOMIC
    assert decoded.owner == OWNER
    assert decoded.signature == "sigA"
    assert abs(decoded.notional_usd - 2_500_000.0) < 1e-3


def test_decode_increase_short_sol():
    tx = _make_tx(
        discriminator=_disc("instant_increase_position_pre_swap"),
        size_usd_delta=10_000 * USD_ATOMIC,
        accounts_pubkeys=[SOL_CUSTODY, USDC_CUSTODY],  # stable -> short
    )
    decoded = decode_jupiter_tx(tx, "sigB")
    assert decoded is not None
    assert decoded.coin == "SOL"
    assert decoded.side == "short"


def test_decode_decrease_eth():
    eth_custody = next(
        c for c, name in ASSET_CUSTODY_TO_COIN.items() if name == "ETH"
    )
    tx = _make_tx(
        discriminator=_disc("instant_decrease_position"),
        size_usd_delta=5_000 * USD_ATOMIC,
        accounts_pubkeys=[eth_custody],
    )
    decoded = decode_jupiter_tx(tx, "sigC")
    assert decoded is not None
    assert decoded.is_decrease
    assert decoded.coin == "ETH"


def test_decode_unknown_disc_returns_none():
    tx = _make_tx(
        discriminator=b"\x00" * 8,
        size_usd_delta=1_000 * USD_ATOMIC,
        accounts_pubkeys=[SOL_CUSTODY],
    )
    assert decode_jupiter_tx(tx, "sigX") is None


def test_decode_no_jupiter_instruction_returns_none():
    """Tx with no JUP-program instruction yields None (we only enrich JUP)."""
    tx = {
        "transaction": {
            "message": {
                "accountKeys": [OWNER, "OtherProgram1111111111111111111111111111"],
                "instructions": [
                    {"programIdIndex": 1, "accounts": [], "data": ""}
                ],
            }
        },
        "meta": {"loadedAddresses": {"writable": [], "readonly": []}},
    }
    assert decode_jupiter_tx(tx, "sig") is None


def test_decode_accepts_v0_loaded_addresses():
    """Position custody resolved via lookup table — only present in
    `meta.loadedAddresses`, not `message.accountKeys`."""
    raw = _disc("instant_increase_position") + struct.pack("<Q", 2_000_000 * USD_ATOMIC) + b"\x00" * 8
    data_b58 = _encode_b58(raw)
    tx = {
        "transaction": {
            "message": {
                "accountKeys": [OWNER, JUPITER_PERPS_PROGRAM_ID],
                "instructions": [
                    {
                        "programIdIndex": 1,
                        "accounts": [2, 3],  # indices into MERGED key list
                        "data": data_b58,
                    }
                ],
            }
        },
        "meta": {
            "loadedAddresses": {
                "writable": [SOL_CUSTODY],  # idx 2
                "readonly": [USDC_CUSTODY],  # idx 3
            }
        },
    }
    decoded = decode_jupiter_tx(tx, "v0")
    assert decoded is not None
    assert decoded.coin == "SOL"
    assert decoded.side == "short"  # USDC custody resolved from the LUT


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK {name}")
