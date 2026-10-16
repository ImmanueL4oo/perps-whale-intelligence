"""Sanity checks for the CEX wallet + token registry.

We're testing the *shape* of the curated lists, not the specific addresses
(those can rotate over time as exchanges manage hot wallets). The goal is
to catch typos, accidental duplicates, and any token with an obviously-
wrong decimals value.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cex_wallets import (
    CEX_WALLETS,
    TOKEN_REGISTRY,
    WSOL_MINT,
    all_cex_addresses,
    cex_addresses_set,
    lookup_token,
    lookup_wallet,
    merge_user_wallets,
)


def test_no_duplicate_addresses():
    addrs = [w.address for w in CEX_WALLETS]
    assert len(addrs) == len(set(addrs)), "duplicate CEX wallet address detected"


def test_address_pubkey_format():
    # Solana pubkeys base58-encode to 32–44 chars and use the base58 alphabet.
    base58_alpha = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    for w in CEX_WALLETS:
        assert 32 <= len(w.address) <= 44, f"unexpected address length: {w.address}"
        assert set(w.address) <= base58_alpha, f"non-base58 char in {w.address}"


def test_lookup_wallet_returns_metadata():
    sample = CEX_WALLETS[0]
    found = lookup_wallet(sample.address)
    assert found is not None
    assert found.exchange == sample.exchange
    assert lookup_wallet("not-a-pubkey") is None
    assert lookup_wallet(None) is None


def test_lookup_token_round_trip():
    usdc = lookup_token("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    assert usdc is not None
    assert usdc.symbol == "USDC" and usdc.decimals == 6 and usdc.is_stable
    sol = lookup_token(WSOL_MINT)
    assert sol is not None
    assert sol.symbol == "SOL" and sol.decimals == 9 and not sol.is_stable
    assert lookup_token("garbage") is None


def test_token_decimals_are_sensible():
    # No SPL token in the registry should have decimals > 12 (sanity-check
    # for typos like decimals=18 left over from EVM defaults).
    for mint, meta in TOKEN_REGISTRY.items():
        assert 0 <= meta.decimals <= 12, f"weird decimals on {meta.symbol} ({mint})"


def test_at_least_a_handful_of_exchanges():
    exchanges = {w.exchange for w in CEX_WALLETS}
    # Should have Binance + Coinbase + at least 4 others; if someone
    # accidentally truncates the registry this catches it.
    assert "Binance" in exchanges
    assert "Coinbase" in exchanges
    assert len(exchanges) >= 6


def test_cex_addresses_helpers():
    lst = all_cex_addresses()
    s = cex_addresses_set()
    assert len(lst) == len(CEX_WALLETS)
    assert s == set(lst)


def test_merge_user_wallets_overrides_curated():
    sample = CEX_WALLETS[0]
    merged = merge_user_wallets([
        {"address": sample.address, "exchange": "MyOverride", "label": "user"},
        {"address": "FakeAddr1234567890123456789012345678", "exchange": "NewEx", "label": "h"},
    ])
    by_addr = {w.address: w for w in merged}
    assert by_addr[sample.address].exchange == "MyOverride"
    assert "FakeAddr1234567890123456789012345678" in by_addr
