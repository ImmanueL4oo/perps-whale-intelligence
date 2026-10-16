"""Curated registry of Solana CEX hot wallets and major SPL token mints.

The registry powers `trackers.cex_flows`: any SPL or native-SOL transfer that
touches one of these accounts is tagged as a CEX in/out flow. Coverage:

* The big-volume centralized exchanges with documented Solana hot wallets
  (Binance, Coinbase, OKX, Bybit, Kraken, KuCoin, Bitfinex, Gate, MEXC,
  Crypto.com, Robinhood). These are publicly tagged on Solscan/Solana FM
  and lift heavy SOL/USDC/USDT volume on-chain.
* The most-active SPL tokens with reliable pricing on Jupiter (SOL,
  stables, JUP, JTO, BONK, WIF, PYTH, RAY, ORCA, mSOL, jitoSOL …).

Adding new entries is as simple as appending to `CEX_WALLETS` /
`TOKEN_REGISTRY`. The tracker rebinds on next restart.

Sources cross-checked: Solscan account labels, ArkhamIntel public
attribution, Coinbase Solana publicly-disclosed hot wallets.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional


# ── Token metadata ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class TokenMeta:
    mint: str
    symbol: str
    decimals: int
    is_stable: bool = False


# Synthetic "mint" we use for native SOL flows. Solana's wrapped-SOL mint
# (So111…12) shares the same value so we collapse both into one row.
WSOL_MINT = "So11111111111111111111111111111111111111112"


TOKEN_REGISTRY: dict[str, TokenMeta] = {
    # ── Stablecoins (price = $1, no fetch) ─────────────────────────────────
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": TokenMeta(
        mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol="USDC", decimals=6, is_stable=True,
    ),
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11neCwPVMhynL": TokenMeta(
        mint="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11neCwPVMhynL",
        symbol="USDT", decimals=6, is_stable=True,
    ),
    "USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX": TokenMeta(
        mint="USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX",
        symbol="USDH", decimals=6, is_stable=True,
    ),
    "FdmkKVEpcc2VFrcjr7gcsBR1pCFKPvqxGhsX2qjyD3hC": TokenMeta(
        mint="FdmkKVEpcc2VFrcjr7gcsBR1pCFKPvqxGhsX2qjyD3hC",
        symbol="PYUSD", decimals=6, is_stable=True,
    ),

    # ── SOL + LSTs ─────────────────────────────────────────────────────────
    WSOL_MINT: TokenMeta(mint=WSOL_MINT, symbol="SOL", decimals=9),
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": TokenMeta(
        mint="mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
        symbol="mSOL", decimals=9,
    ),
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": TokenMeta(
        mint="J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
        symbol="jitoSOL", decimals=9,
    ),
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1": TokenMeta(
        mint="bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
        symbol="bSOL", decimals=9,
    ),

    # ── Top SPL tokens ─────────────────────────────────────────────────────
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": TokenMeta(
        mint="JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        symbol="JUP", decimals=6,
    ),
    "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL": TokenMeta(
        mint="jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
        symbol="JTO", decimals=9,
    ),
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": TokenMeta(
        mint="DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        symbol="BONK", decimals=5,
    ),
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm": TokenMeta(
        mint="EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
        symbol="WIF", decimals=6,
    ),
    "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3": TokenMeta(
        mint="HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
        symbol="PYTH", decimals=6,
    ),
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R": TokenMeta(
        mint="4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        symbol="RAY", decimals=6,
    ),
    "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE": TokenMeta(
        mint="orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
        symbol="ORCA", decimals=6,
    ),
    "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ": TokenMeta(
        mint="85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",
        symbol="W", decimals=6,
    ),
    "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof": TokenMeta(
        mint="rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof",
        symbol="RENDER", decimals=8,
    ),
    "TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6": TokenMeta(
        mint="TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6",
        symbol="TNSR", decimals=9,
    ),
    "KMNo3nJsBXfcpJTVhZcXLW7RmTwTt4GVFE7suUBo9sS": TokenMeta(
        mint="KMNo3nJsBXfcpJTVhZcXLW7RmTwTt4GVFE7suUBo9sS",
        symbol="KMNO", decimals=6,
    ),
    "hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux": TokenMeta(
        mint="hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux",
        symbol="HNT", decimals=8,
    ),
    "DriFtupJYLTosbwoN8koMbEYSx54aFAVLddWsbksjwg7": TokenMeta(
        mint="DriFtupJYLTosbwoN8koMbEYSx54aFAVLddWsbksjwg7",
        symbol="DRIFT", decimals=6,
    ),
    "63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9": TokenMeta(
        mint="63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9",
        symbol="GIGA", decimals=5,
    ),
    "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY": TokenMeta(
        mint="ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY",
        symbol="MOODENG", decimals=6,
    ),
    "ETAtLmCmsoiEEKfNrHKJ2kYy3MoABhU6NQvpSfij5tDs": TokenMeta(
        mint="ETAtLmCmsoiEEKfNrHKJ2kYy3MoABhU6NQvpSfij5tDs",
        symbol="MEW", decimals=5,
    ),
    "AZsHEMXd36Bj1EMNXhowJajpUXzrKcK57wW4ZGXVa7yR": TokenMeta(
        mint="AZsHEMXd36Bj1EMNXhowJajpUXzrKcK57wW4ZGXVa7yR",
        symbol="GUACAMOLE", decimals=6,
    ),
}


# ── CEX hot-wallet registry ─────────────────────────────────────────────────
@dataclass(frozen=True)
class CexWallet:
    address: str
    exchange: str
    label: str           # human-readable suffix (hot-1, deposits, …)


CEX_WALLETS: list[CexWallet] = [
    # ── Binance ────────────────────────────────────────────────────────────
    CexWallet("5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9", "Binance", "hot-1"),
    CexWallet("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "Binance", "hot-2"),
    CexWallet("2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S", "Binance", "hot-3"),
    CexWallet("3gd3dqgtJ4jWfBfLYTX67DALFetjc5iS72sCgRhCkW2u", "Binance", "cold-1"),
    # ── Coinbase ───────────────────────────────────────────────────────────
    CexWallet("H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS", "Coinbase", "hot-1"),
    CexWallet("2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm", "Coinbase", "hot-2"),
    CexWallet("GJRs4FwHtemZ5ZE9x3FNvJ8TMeGbSSE3GddbkosiUjyY", "Coinbase", "prime"),
    CexWallet("9obNtb7GDoy5HzkPpxnabwj3SAyMQRBxxR4q41bP4WeP", "Coinbase", "hot-3"),
    # ── OKX ────────────────────────────────────────────────────────────────
    CexWallet("5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD", "OKX", "hot-1"),
    CexWallet("FpwQQhQQoEaVu3WU2qZMfF1hx48YyfwsLoRgXG83E99T", "OKX", "hot-2"),
    # ── Kraken ─────────────────────────────────────────────────────────────
    CexWallet("FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5", "Kraken", "hot-1"),
    CexWallet("Bz9thGbRRfwq3EFtFtSKZYnnXio5LXDaRgJDh3NrMAGT", "Kraken", "hot-2"),
    # ── Bybit ──────────────────────────────────────────────────────────────
    CexWallet("AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2", "Bybit", "hot-1"),
    CexWallet("42brAgAVNzMBP7aaktPvAmBSPEkehnFQejiZc53EkJmQ", "Bybit", "hot-2"),
    # ── KuCoin ─────────────────────────────────────────────────────────────
    CexWallet("BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6", "KuCoin", "hot-1"),
    CexWallet("9pwcSyP1k2Hm8XX2WLVuwn1jUx8K8rrXyyy5fiWXVB1Q", "KuCoin", "hot-2"),
    # ── Bitfinex ───────────────────────────────────────────────────────────
    CexWallet("FxteHmLwG9nk1eL4pjNve3Eub2goGkkz6g6TbvdmW46a", "Bitfinex", "hot-1"),
    # ── MEXC ───────────────────────────────────────────────────────────────
    CexWallet("ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ", "MEXC", "hot-1"),
    CexWallet("AobVSwdW9BbpMdJvTqeCN4hPAmh4rHm7vwLnQ5ATSyrS", "MEXC", "hot-2"),
    # ── Gate.io ────────────────────────────────────────────────────────────
    CexWallet("u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w", "Gate", "hot-1"),
    # ── Crypto.com ─────────────────────────────────────────────────────────
    CexWallet("6QJzieMYfp7yr3EdrePaQoG3Ghxs2wM98xSLRu8Xh56U", "Crypto.com", "hot-1"),
    # ── Robinhood ──────────────────────────────────────────────────────────
    CexWallet("9LGS9fxZ7XkLekP3KVaUjbsHCBGfwRjWVbFbpiEKZJiF", "Robinhood", "hot-1"),
    # ── Bitget ─────────────────────────────────────────────────────────────
    CexWallet("A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR", "Bitget", "hot-1"),
    # ── HTX (Huobi) ────────────────────────────────────────────────────────
    CexWallet("AhSKfnoamz1uM7w6TXfNxiqaCWQu6T7PTu8e4N9rN7q1", "HTX", "hot-1"),
]


# ── Lookups ─────────────────────────────────────────────────────────────────
_CEX_BY_ADDR: dict[str, CexWallet] = {w.address: w for w in CEX_WALLETS}


def lookup_wallet(addr: Optional[str]) -> Optional[CexWallet]:
    if not addr:
        return None
    return _CEX_BY_ADDR.get(addr)


def lookup_token(mint: Optional[str]) -> Optional[TokenMeta]:
    if not mint:
        return None
    return TOKEN_REGISTRY.get(mint)


def all_cex_addresses() -> list[str]:
    return [w.address for w in CEX_WALLETS]


def cex_addresses_set() -> set[str]:
    return set(_CEX_BY_ADDR.keys())


def merge_user_wallets(extra: Iterable[dict]) -> list[CexWallet]:
    """Merge the curated list with user-provided wallets (e.g. loaded from
    a JSON file). User entries override on collision so people can rename
    or reclassify a wallet without forking the registry.
    """
    merged: dict[str, CexWallet] = {w.address: w for w in CEX_WALLETS}
    for raw in extra:
        try:
            w = CexWallet(
                address=raw["address"],
                exchange=str(raw.get("exchange", "Unknown")),
                label=str(raw.get("label", "user")),
            )
        except (KeyError, TypeError):
            continue
        merged[w.address] = w
    return list(merged.values())


__all__ = [
    "CexWallet",
    "TokenMeta",
    "WSOL_MINT",
    "CEX_WALLETS",
    "TOKEN_REGISTRY",
    "lookup_wallet",
    "lookup_token",
    "all_cex_addresses",
    "cex_addresses_set",
    "merge_user_wallets",
]
