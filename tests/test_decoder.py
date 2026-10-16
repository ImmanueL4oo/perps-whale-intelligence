"""Round-trip tests for the Jupiter Perps Borsh decoder + Notifier formatting.

We synthesize a known-good byte payload using the same struct rules the
program uses, decode it with our reader, and assert all whale-relevant fields
come back correctly.
"""
from __future__ import annotations

import asyncio
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifier import Notifier
from trackers import EventKind, Side, WhaleEvent
from trackers.jupiter_perps import (
    DISC_DECREASE,
    DISC_INCREASE,
    DISC_LIQUIDATE,
    JupiterPerpsTracker,
    _b58,
    _decode_decrease,
    _decode_increase,
    _decode_liquidate,
)


PUBKEY_A = bytes(range(32))
PUBKEY_B = bytes(range(32, 64))
PUBKEY_C = bytes(reversed(range(32)))


def _u64(x: int) -> bytes: return struct.pack("<Q", x)
def _i64(x: int) -> bytes: return struct.pack("<q", x)
def _opt_none() -> bytes: return b"\x00"
def _opt_u64(x: int) -> bytes: return b"\x01" + _u64(x)


def _build_increase_payload() -> bytes:
    return b"".join([
        PUBKEY_A,                           # positionKey
        bytes([1]),                         # positionSide = Long
        PUBKEY_B,                           # positionCustody (we'll treat as SOL)
        PUBKEY_C,                           # positionCollateralCustody
        _u64(7_500_000 * 1_000_000),        # positionSizeUsd = $7.5M
        PUBKEY_A,                           # positionMint
        PUBKEY_B,                           # positionRequestKey
        PUBKEY_C,                           # positionRequestMint
        bytes([1]),                         # positionRequestChange
        bytes([2]),                         # positionRequestType
        _u64(1234),                         # positionRequestCollateralDelta
        PUBKEY_C,                           # owner
        PUBKEY_A,                           # pool
        _u64(6_000_000 * 1_000_000),        # sizeUsdDelta = $6M  ← whale
        _u64(500_000 * 1_000_000),          # collateralUsdDelta
        _u64(2_500_000_000),                # collateralTokenDelta
        _u64(200 * 1_000_000),              # price = $200
        _opt_none(),                        # priceSlippage
        _u64(123),                          # feeToken
        _u64(15_000 * 1_000_000),           # feeUsd
        _i64(1717000000),                   # openTime
        _opt_none(),                        # referral
    ])


def _build_decrease_payload() -> bytes:
    return b"".join([
        PUBKEY_A, bytes([2]),               # positionKey, side=Short
        PUBKEY_B, PUBKEY_C,                 # custodies
        _u64(2_000_000 * 1_000_000),        # remaining size
        PUBKEY_A, PUBKEY_B, PUBKEY_C,
        bytes([2]), bytes([1]),
        b"\x01",                            # hasProfit = true
        _u64(120_000 * 1_000_000),          # pnlDelta = $120K profit
        PUBKEY_C, PUBKEY_A,                 # owner, pool
        _u64(8_000_000 * 1_000_000),        # sizeUsdDelta = $8M ← whale
        _u64(500_000 * 1_000_000),          # transferAmountUsd
        _opt_u64(500_000_000),              # transferToken
        _u64(60_000 * 1_000_000),           # price = $60K (BTC-ish)
        _opt_none(),                        # priceSlippage
        _u64(10_000 * 1_000_000),           # feeUsd
        _i64(1717111111),                   # openTime
        _opt_none(),
    ])


def _build_liquidate_payload() -> bytes:
    return b"".join([
        PUBKEY_A, bytes([1]),
        PUBKEY_B, PUBKEY_C, PUBKEY_A, PUBKEY_B,  # custodies + mints
        _u64(12_000_000 * 1_000_000),       # positionSizeUsd = $12M ← whale
        b"\x00",                            # hasProfit = false
        _u64(800_000 * 1_000_000),          # pnlDelta = $800K loss
        PUBKEY_C, PUBKEY_A,                 # owner, pool
        _u64(11_000_000 * 1_000_000),
        _u64(50_000 * 1_000_000),
        _u64(3_500 * 1_000_000),            # price = $3500 (ETH-ish)
        _u64(20_000 * 1_000_000),
        _u64(150_000 * 1_000_000),
        _i64(1717222222),
    ])


def test_increase_decode():
    payload = _build_increase_payload()
    ev = _decode_increase(payload)
    assert ev.position_key == _b58(PUBKEY_A)
    assert ev.side == 1
    assert ev.position_custody == _b58(PUBKEY_B)
    assert ev.position_size_usd == 7_500_000 * 1_000_000
    assert ev.size_usd_delta == 6_000_000 * 1_000_000
    assert ev.price == 200 * 1_000_000
    assert ev.owner == _b58(PUBKEY_C)


def test_decrease_decode():
    ev = _decode_decrease(_build_decrease_payload())
    assert ev.side == 2
    assert ev.has_profit is True
    assert ev.pnl_delta == 120_000 * 1_000_000
    assert ev.size_usd_delta == 8_000_000 * 1_000_000
    assert ev.price == 60_000 * 1_000_000


def test_liquidate_decode():
    ev = _decode_liquidate(_build_liquidate_payload())
    assert ev.has_profit is False
    assert ev.position_size_usd == 12_000_000 * 1_000_000
    assert ev.price == 3_500 * 1_000_000


def test_tracker_filters_below_threshold():
    tracker = JupiterPerpsTracker(threshold_usd=10_000_000)
    increase = _decode_increase(_build_increase_payload())  # $6M < $10M
    ev = tracker._to_whale_event(increase, signature="sig1")
    assert ev is None


def test_tracker_emits_whale_open():
    tracker = JupiterPerpsTracker(threshold_usd=5_000_000)
    increase = _decode_increase(_build_increase_payload())  # $6M >= $5M
    ev = tracker._to_whale_event(increase, signature="sig2")
    assert ev is not None
    assert ev.kind == EventKind.OPEN
    assert ev.side == Side.LONG
    assert ev.notional_usd == 6_000_000.0
    assert ev.price_usd == 200.0
    # 6M USD / $200 = 30,000 base units
    assert abs(ev.size - 30_000.0) < 1e-6


def test_tracker_emits_liquidation_with_pnl():
    tracker = JupiterPerpsTracker(threshold_usd=5_000_000)
    liq = _decode_liquidate(_build_liquidate_payload())
    ev = tracker._to_whale_event(liq, signature="sig3")
    assert ev is not None
    assert ev.kind == EventKind.LIQUIDATION
    assert ev.notional_usd == 12_000_000.0
    assert ev.pnl_usd == -800_000.0  # hasProfit=false → negative


def test_tracker_dedupes():
    tracker = JupiterPerpsTracker(threshold_usd=5_000_000)
    inc = _decode_increase(_build_increase_payload())
    a = tracker._to_whale_event(inc, signature="dup-sig")
    b = tracker._to_whale_event(inc, signature="dup-sig")
    assert a is not None
    assert b is None  # second time should be deduped


async def _run_notifier_test():
    # Telegram disabled — only exercises the rich console path.
    async with Notifier() as n:
        n.banner(5_000_000, ["Hyperliquid", "JupiterPerps"])
        await n.emit(WhaleEvent(
            venue="JupiterPerps",
            kind=EventKind.OPEN,
            coin="SOL",
            side=Side.LONG,
            notional_usd=6_000_000,
            price_usd=200.0,
            size=30_000,
            trader=_b58(PUBKEY_C),
            tx_ref="testsig",
            pnl_usd=None,
        ))
        # Same event again → should be silently deduped.
        await n.emit(WhaleEvent(
            venue="JupiterPerps",
            kind=EventKind.OPEN,
            coin="SOL",
            side=Side.LONG,
            notional_usd=6_000_000,
            price_usd=200.0,
            size=30_000,
            trader=_b58(PUBKEY_C),
            tx_ref="testsig",
            pnl_usd=None,
        ))
        await n.emit(WhaleEvent(
            venue="Hyperliquid",
            kind=EventKind.LIQUIDATION,
            coin="ETH",
            side=Side.SHORT,
            notional_usd=12_000_000,
            price_usd=3500.0,
            size=3428.57,
            trader="0x1234567890abcdef1234567890abcdef12345678",
            tx_ref="hl_tid_42",
            pnl_usd=-800_000,
            leverage=20.0,
        ))


def test_notifier_console():
    asyncio.run(_run_notifier_test())


if __name__ == "__main__":
    test_increase_decode()
    test_decrease_decode()
    test_liquidate_decode()
    test_tracker_filters_below_threshold()
    test_tracker_emits_whale_open()
    test_tracker_emits_liquidation_with_pnl()
    test_tracker_dedupes()
    print("decoder + tracker tests OK")
    test_notifier_console()
    print("notifier render OK")
