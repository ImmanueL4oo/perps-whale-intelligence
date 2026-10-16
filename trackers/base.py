"""Shared types used across protocol trackers."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"


class EventKind(str, Enum):
    OPEN = "OPEN"          # new position or material increase
    CLOSE = "CLOSE"        # position closed or materially reduced
    LIQUIDATION = "LIQ"    # forced close


@dataclass
class WhaleEvent:
    """A normalized 'big move' across protocols."""
    venue: str                   # "Hyperliquid" / "JupiterPerps"
    kind: EventKind
    coin: str                    # "BTC", "SOL", ...
    side: Side                   # long or short
    notional_usd: float          # USD notional of THIS event (not total position)
    price_usd: float             # fill / mark price
    size: float                  # base-asset size

    trader: str                  # address / wallet
    tx_ref: str                  # tx hash / signature / unique id (for dedupe)
    ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    pnl_usd: Optional[float] = None
    leverage: Optional[float] = None
    extra: dict = field(default_factory=dict)

    @property
    def dedupe_key(self) -> str:
        return f"{self.venue}:{self.tx_ref}:{self.trader}:{self.coin}:{self.kind.value}"
