"""Rolling buffer + windowed analytics for CEX in/out flow events.

The tracker fires `CexFlowEvent`s; this module owns the in-memory ring of
recent events and answers two questions for the dashboard / Telegram:

1. **What's the current net flow?**    Net USD = OUT − IN per window. Negative means deposits
   dominate (selling pressure). Positive means withdrawals dominate
   (accumulation).

2. **Where is the flow concentrated?**  Per-exchange and per-asset breakdowns plus a list of the
   biggest single transfers in the window.

Designed to be cheap: a single deque, O(N) summary computations bounded by
`recent_size`. Mongo persistence is layered on top in a separate path
(see `bot.py` wiring) so the analyzer can stay synchronous and reusable.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import asdict
from typing import Any, Iterable, Optional

from trackers.cex_flows import CexFlowEvent


def _now_ms() -> int:
    return int(time.time() * 1000)


WINDOWS_MIN: dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "24h": 1440,
}


class CexFlowAnalyzer:
    """In-memory ring buffer + summary helpers.

    The `recent_size` default of 5000 is enough for ~24h of mid-volume
    activity (≈3.5 events/min); the bot trims older entries on every
    `record()` so the analyzer never blows up RAM.
    """

    def __init__(self, recent_size: int = 5000) -> None:
        self._buf: deque[dict[str, Any]] = deque(maxlen=recent_size)
        self._started_at_ms = _now_ms()

    @property
    def started_at_ms(self) -> int:
        return self._started_at_ms

    # ── Ingest ──────────────────────────────────────────────────────────────
    def record(self, ev: CexFlowEvent | dict) -> dict:
        """Append a single event. Accepts any of:

        - a `CexFlowEvent` dataclass (live tracker path);
        - a flat `to_dict()` snapshot (Mongo backfill path);
        - a dashboard payload from `_cex_event_to_payload` (bus path), which
          nests the analyzer-relevant fields under a `cex` sub-dict.

        We normalize to a single flat shape so `summary()` / `buckets()` /
        `_all_window_nets()` only need to look at top-level keys.
        """
        d = self._normalize(ev)
        self._buf.append(d)
        return d

    def warm_up(self, events: Iterable[dict]) -> None:
        for e in events:
            self._buf.append(self._normalize(e))

    @staticmethod
    def _normalize(ev: CexFlowEvent | dict) -> dict:
        if isinstance(ev, CexFlowEvent):
            return ev.to_dict()
        d = dict(ev)
        # Dashboard payload: promote nested cex.* up to top-level so the
        # aggregations don't need to know about the wrapper.
        sub = d.get("cex") if isinstance(d.get("cex"), dict) else None
        if sub:
            for k in (
                "direction",
                "exchange",
                "label",
                "asset",
                "mint",
                "amount",
                "amount_usd",
                "counterparty",
                "signature",
                "slot",
                "block_time",
                "wallet",
            ):
                if k not in d and k in sub:
                    d[k] = sub[k]
            # Map dashboard wallet→cex_wallet for parity with to_dict().
            if "cex_wallet" not in d and "wallet" in sub:
                d["cex_wallet"] = sub["wallet"]
        # If we still don't have amount_usd, fall back to the dashboard's
        # notional_usd (same number, different name).
        if d.get("amount_usd") is None and d.get("notional_usd") is not None:
            d["amount_usd"] = d.get("notional_usd")
        return d

    def recent(self, limit: Optional[int] = None) -> list[dict]:
        if limit is None or limit >= len(self._buf):
            return list(self._buf)
        return list(self._buf)[-limit:]

    # ── Aggregations ────────────────────────────────────────────────────────
    def summary(
        self,
        *,
        window_min: int = 60,
        top_assets: int = 8,
        top_exchanges: int = 8,
        top_transfers: int = 10,
    ) -> dict[str, Any]:
        """Compact dashboard payload for one rolling window.

        Returns:
            {
              "window_min": 60,
              "now_ms": 1717000000000,
              "totals":     { "in_usd": ..., "out_usd": ..., "net_usd": ..., "events": 1234 },
              "by_exchange": [ { "exchange": "Binance", "in_usd": ..., "out_usd": ..., "net_usd": ..., "events": 42 }, … ],
              "by_asset":    [ { "asset": "USDC", "in_usd": ..., "out_usd": ..., "net_usd": ..., "events": 17 }, … ],
              "top_transfers": [ { ..ev.. }, … ],
              "windows":   { "5m": net, "15m": net, … },
            }
        """
        now = _now_ms()
        cutoff = now - window_min * 60_000
        in_window = [e for e in self._buf if (e.get("ts_ms") or 0) >= cutoff]

        totals = {"in_usd": 0.0, "out_usd": 0.0, "net_usd": 0.0, "events": 0}
        by_ex: dict[str, dict[str, float]] = {}
        by_asset: dict[str, dict[str, float]] = {}
        for e in in_window:
            usd = float(e.get("amount_usd") or 0.0)
            direction = e.get("direction")
            ex = e.get("exchange") or "Unknown"
            asset = e.get("asset") or "?"
            ex_row = by_ex.setdefault(ex, {"in_usd": 0.0, "out_usd": 0.0, "events": 0})
            asset_row = by_asset.setdefault(asset, {"in_usd": 0.0, "out_usd": 0.0, "events": 0})
            ex_row["events"] += 1
            asset_row["events"] += 1
            if direction == "IN":
                totals["in_usd"] += usd
                ex_row["in_usd"] += usd
                asset_row["in_usd"] += usd
            else:
                totals["out_usd"] += usd
                ex_row["out_usd"] += usd
                asset_row["out_usd"] += usd
            totals["events"] += 1
        totals["net_usd"] = totals["out_usd"] - totals["in_usd"]

        ex_rows = sorted(
            (
                {
                    "exchange": ex,
                    "in_usd": v["in_usd"],
                    "out_usd": v["out_usd"],
                    "net_usd": v["out_usd"] - v["in_usd"],
                    "gross_usd": v["in_usd"] + v["out_usd"],
                    "events": int(v["events"]),
                }
                for ex, v in by_ex.items()
            ),
            key=lambda r: r["gross_usd"],
            reverse=True,
        )[:top_exchanges]

        asset_rows = sorted(
            (
                {
                    "asset": a,
                    "in_usd": v["in_usd"],
                    "out_usd": v["out_usd"],
                    "net_usd": v["out_usd"] - v["in_usd"],
                    "gross_usd": v["in_usd"] + v["out_usd"],
                    "events": int(v["events"]),
                }
                for a, v in by_asset.items()
            ),
            key=lambda r: r["gross_usd"],
            reverse=True,
        )[:top_assets]

        top_xfers = sorted(
            in_window,
            key=lambda e: e.get("amount_usd") or 0.0,
            reverse=True,
        )[:top_transfers]

        windows = self._all_window_nets()

        # Bias score in [-100, 100]: positive = withdrawals dominate
        # (bullish), negative = deposits dominate (bearish). 0 when no flow.
        gross = totals["in_usd"] + totals["out_usd"]
        bias = (totals["net_usd"] / gross) * 100.0 if gross > 0 else 0.0

        return {
            "window_min": window_min,
            "now_ms": now,
            "totals": totals,
            "bias": bias,
            "by_exchange": ex_rows,
            "by_asset": asset_rows,
            "top_transfers": top_xfers,
            "windows": windows,
        }

    def _all_window_nets(self) -> dict[str, dict[str, float]]:
        """Pre-computed net flow for every standard window — used by the
        dashboard's mini-strip so it doesn't have to call `summary()` 5×."""
        now = _now_ms()
        out: dict[str, dict[str, float]] = {}
        # Iterate once per window to avoid filtering buf 5×.
        for label, mins in WINDOWS_MIN.items():
            cutoff = now - mins * 60_000
            tot_in = 0.0
            tot_out = 0.0
            n = 0
            for e in self._buf:
                ts = e.get("ts_ms") or 0
                if ts < cutoff:
                    continue
                usd = float(e.get("amount_usd") or 0.0)
                if e.get("direction") == "IN":
                    tot_in += usd
                else:
                    tot_out += usd
                n += 1
            out[label] = {
                "in_usd": tot_in,
                "out_usd": tot_out,
                "net_usd": tot_out - tot_in,
                "events": n,
            }
        return out

    def buckets(
        self,
        *,
        window_min: int = 60,
        bucket_min: int = 5,
    ) -> list[dict[str, Any]]:
        """Time-bucketed series for a sparkline: bucket_min granularity over
        the last window_min minutes. Returns oldest→newest."""
        now = _now_ms()
        bucket_ms = bucket_min * 60_000
        cutoff = now - window_min * 60_000
        n_buckets = max(1, window_min // max(1, bucket_min))
        # Edges: [cutoff, cutoff+bucket, …, now]
        rows: list[dict[str, Any]] = []
        for i in range(n_buckets):
            start = cutoff + i * bucket_ms
            rows.append({
                "ts_ms": start,
                "in_usd": 0.0,
                "out_usd": 0.0,
                "net_usd": 0.0,
                "events": 0,
            })
        for e in self._buf:
            ts = e.get("ts_ms") or 0
            if ts < cutoff:
                continue
            idx = int((ts - cutoff) // bucket_ms)
            if idx < 0 or idx >= n_buckets:
                continue
            row = rows[idx]
            usd = float(e.get("amount_usd") or 0.0)
            if e.get("direction") == "IN":
                row["in_usd"] += usd
            else:
                row["out_usd"] += usd
            row["events"] += 1
            row["net_usd"] = row["out_usd"] - row["in_usd"]
        return rows


__all__ = ["CexFlowAnalyzer", "WINDOWS_MIN"]
