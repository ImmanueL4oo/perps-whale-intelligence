"""Analytics over a stream of whale events.

All functions are pure: pass in a list of event dicts (the shape produced by
`event_bus.serialize_event` and stored in MongoDB), get back JSON-safe
structures suitable for direct return from the dashboard's REST endpoints.

Concepts
--------

**Signed flow** — direction-aware "buy/sell pressure" for a single event:

    OPEN  long  →  +notional   (buy)        OPEN  short →  −notional   (sell)
    CLOSE long  →  −notional   (sell)       CLOSE short →  +notional   (buy)
    LIQ   long  →  −notional (forced sell)  LIQ   short →  +notional   (forced buy)

Summing signed flow across events tells you whether the market is being
absorbed by buyers or sellers in aggregate.

**Smart flow** — same as flow, but every event is weighted by the trader's
composite score / 100. Wallets with no score are excluded. This biases the
aggregate toward what historically-profitable wallets are doing.

**Consensus signal** — a cluster of distinct ≥TRUSTED traders going the
*same direction* on the *same coin* inside a sliding time window. The most
actionable copy-trade signal we can extract from the raw stream.

**Smart positions** — best-effort inference of which positions a high-score
wallet is *currently* holding, based on the OPEN/CLOSE/LIQ events we've seen.
Caveats are documented on the function itself; treat output as directional,
not exact.
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any, Iterable

# ELITE > TRUSTED > MIXED > POOR > NEW. Higher number = more trustworthy.
TIER_RANK: dict[str, int] = {
    "NEW": 0,
    "POOR": 1,
    "MIXED": 2,
    "TRUSTED": 3,
    "ELITE": 4,
}

# ── Actor (bot/human) classifier ─────────────────────────────────────────────
# Defaults are tunable per-bot via the BOT_TPD_THRESHOLD / HYBRID_TPD_THRESHOLD
# / ACTOR_MIN_SAMPLES env vars (see config.py and .env.example).
DEFAULT_BOT_TPD = 30.0
DEFAULT_HYBRID_TPD = 5.0
DEFAULT_MIN_SAMPLES = 10

ACTOR_LABELS = ("HUMAN", "HYBRID", "BOT", "UNKNOWN")


def classify_actor(
    stats: dict | object | None,
    *,
    bot_tpd: float = DEFAULT_BOT_TPD,
    hybrid_tpd: float = DEFAULT_HYBRID_TPD,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> tuple[str, float]:
    """Classify a wallet as HUMAN / HYBRID / BOT / UNKNOWN from its history.

    Heuristic: trades_per_day = closed_trades / max(days_active, 1).

      • ≥ `bot_tpd`     → BOT     (HFT / scalping / market-making cadence)
      • ≥ `hybrid_tpd`  → HYBRID  (active manual trader, or supervised bot)
      • <               → HUMAN   (typical discretionary cadence)
      • Less than `min_samples` closed trades → UNKNOWN.

    Accepts either a serialized dict (as stored in event.track_record) or a
    `WalletStats` dataclass — anything with `closed_trades` / `days_active`.
    Returns `(label, trades_per_day)`.
    """
    if stats is None:
        return "UNKNOWN", 0.0

    def _get(name: str, default=None):
        if isinstance(stats, dict):
            return stats.get(name, default)
        return getattr(stats, name, default)

    closed = _get("closed_trades", 0) or 0
    days = _get("days_active", 0) or 0
    if closed < min_samples or days < 1:
        return "UNKNOWN", 0.0
    tpd = float(closed) / max(float(days), 1.0)
    if tpd >= bot_tpd:
        return "BOT", tpd
    if tpd >= hybrid_tpd:
        return "HYBRID", tpd
    return "HUMAN", tpd


def actor_of(ev: dict) -> str:
    """Pluck the actor label off a serialized event (defaulting to UNKNOWN)."""
    tr = ev.get("track_record") or {}
    label = tr.get("actor")
    if label in ACTOR_LABELS:
        return label
    label, _ = classify_actor(tr if tr else None)
    return label


# ── Per-event helpers ────────────────────────────────────────────────────────
def signed_flow(ev: dict) -> float:
    side_long = ev.get("side") == "long"
    kind = ev.get("kind")
    notional = float(ev.get("notional_usd") or 0.0)
    if kind == "OPEN":
        return notional if side_long else -notional
    if kind in ("CLOSE", "LIQ"):
        return -notional if side_long else notional
    return 0.0


def _tier(ev: dict) -> str:
    tr = ev.get("track_record") or {}
    return tr.get("tier") or "NEW"


def _score(ev: dict) -> float:
    tr = ev.get("track_record") or {}
    s = tr.get("score")
    return float(s) if s is not None else 0.0


# ── Coin scoreboard ──────────────────────────────────────────────────────────
DEFAULT_COIN_WINDOWS_MS: tuple[int, ...] = (
    15 * 60_000,         # 15 m
    60 * 60_000,         # 1 h
    24 * 60 * 60_000,    # 24 h
)


def coin_scoreboard(
    events: Iterable[dict],
    *,
    windows_ms: tuple[int, ...] = DEFAULT_COIN_WINDOWS_MS,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Per-coin breakdown ranked by 1-hour notional volume.

    For each coin, returns flow / smart flow / volume / open/close/liq counts /
    tier breakdown over each requested window. Sorted by 1h volume desc so the
    most active markets surface first.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    events = [e for e in events if e.get("coin")]

    by_coin: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        by_coin[e["coin"]].append(e)

    out: list[dict[str, Any]] = []
    for coin, coin_events in by_coin.items():
        windows: dict[str, dict[str, Any]] = {}
        for w in windows_ms:
            cutoff = now_ms - w
            scoped = [e for e in coin_events if e.get("ts_ms", 0) >= cutoff]
            flow = sum(signed_flow(e) for e in scoped)
            volume = sum(float(e.get("notional_usd") or 0.0) for e in scoped)
            opens = sum(1 for e in scoped if e["kind"] == "OPEN")
            closes = sum(1 for e in scoped if e["kind"] == "CLOSE")
            liqs = sum(1 for e in scoped if e["kind"] == "LIQ")
            liq_notional = sum(
                float(e.get("notional_usd") or 0.0)
                for e in scoped if e["kind"] == "LIQ"
            )
            smart_flow = 0.0
            smart_volume = 0.0
            for e in scoped:
                sc = _score(e)
                if sc <= 0:
                    continue
                w_factor = sc / 100.0
                smart_flow += signed_flow(e) * w_factor
                smart_volume += float(e.get("notional_usd") or 0.0) * w_factor
            tiers: dict[str, float] = defaultdict(float)
            for e in scoped:
                tiers[_tier(e)] += float(e.get("notional_usd") or 0.0)
            windows[str(w)] = {
                "flow_usd": flow,
                "volume_usd": volume,
                "trade_count": len(scoped),
                "open_count": opens,
                "close_count": closes,
                "liq_count": liqs,
                "liq_notional_usd": liq_notional,
                "smart_flow_usd": smart_flow,
                "smart_volume_usd": smart_volume,
                "tier_notional_usd": dict(tiers),
            }
        last_event = max(coin_events, key=lambda e: e.get("ts_ms", 0))
        out.append({
            "coin": coin,
            "last_ts_ms": last_event.get("ts_ms", 0),
            "last_price_usd": last_event.get("price_usd"),
            "windows": windows,
            "total_events": len(coin_events),
        })

    # Sort by largest 1h volume; missing window → 0.
    one_hour_key = str(60 * 60_000) if 60 * 60_000 in windows_ms else str(windows_ms[0])
    out.sort(
        key=lambda r: -r["windows"].get(one_hour_key, {}).get("volume_usd", 0.0)
    )
    return out


# ── Consensus signals ────────────────────────────────────────────────────────
def consensus_signals(
    events: Iterable[dict],
    *,
    window_ms: int = 30 * 60_000,
    min_tier_rank: int = TIER_RANK["TRUSTED"],
    min_traders: int = 2,
    now_ms: int | None = None,
    allowed_actors: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Detect clusters of high-tier traders going the same direction on the
    same coin inside a sliding window.

    A signal fires when at least `min_traders` distinct wallets of tier
    ≥ `min_tier_rank` placed an OPEN event of the same side on the same coin
    within the trailing `window_ms`. Stronger signals (more traders, higher
    average score, more notional) are ranked first.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_ms
    actor_set = set(allowed_actors) if allowed_actors else None
    cands = [
        e for e in events
        if e.get("kind") == "OPEN"
        and TIER_RANK.get(_tier(e), 0) >= min_tier_rank
        and e.get("ts_ms", 0) >= cutoff
        and (actor_set is None or actor_of(e) in actor_set)
    ]
    if not cands:
        return []

    groups: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    for e in cands:
        # Keep the highest-notional event per (coin, side, trader).
        key = (e["coin"], e["side"])
        cur = groups[key].get(e["trader"])
        if cur is None or float(e.get("notional_usd") or 0) > float(cur.get("notional_usd") or 0):
            groups[key][e["trader"]] = e

    signals: list[dict[str, Any]] = []
    for (coin, side), traders_map in groups.items():
        if len(traders_map) < min_traders:
            continue
        traders = list(traders_map.values())
        traders.sort(key=lambda e: -_score(e))
        total_notional = sum(float(e.get("notional_usd") or 0.0) for e in traders)
        avg_score = sum(_score(e) for e in traders) / len(traders)
        first_ts = min(e["ts_ms"] for e in traders)
        last_ts = max(e["ts_ms"] for e in traders)
        signals.append({
            "coin": coin,
            "side": side,
            "trader_count": len(traders),
            "total_notional_usd": total_notional,
            "avg_score": avg_score,
            "first_ts_ms": first_ts,
            "last_ts_ms": last_ts,
            "window_ms": window_ms,
            "min_tier_rank": min_tier_rank,
            "traders": [
                {
                    "venue": e["venue"],
                    "trader": e["trader"],
                    "score": _score(e),
                    "tier": _tier(e),
                    "tier_emoji": (e.get("track_record") or {}).get("tier_emoji"),
                    "actor": actor_of(e),
                    "notional_usd": float(e.get("notional_usd") or 0.0),
                    "leverage": e.get("leverage"),
                    "ts_ms": e["ts_ms"],
                }
                for e in traders
            ],
        })
    signals.sort(key=lambda s: (
        -s["trader_count"],
        -s["avg_score"],
        -s["total_notional_usd"],
    ))
    return signals


# ── Smart positions (best-effort) ────────────────────────────────────────────
def smart_positions(
    events: Iterable[dict],
    *,
    min_score: float = 60.0,
    max_age_ms: int = 24 * 60 * 60_000,
    now_ms: int | None = None,
    limit: int = 50,
    allowed_actors: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Best-effort 'currently-open' positions inferred from event history.

    Walks events chronologically per (venue, trader, coin):
      • OPEN events queue as live lots.
      • CLOSE / LIQ on the same key flushes the queue.
    Whatever stays queued is reported as 'still open'. Output is restricted
    to wallets with score ≥ `min_score` and events newer than `max_age_ms`.

    Limitations:
      • Below-threshold rebalances are invisible.
      • OPENs that happened before bot startup are missed entirely.
      • A single CLOSE flushes all prior lots for that wallet+coin. On
        Hyperliquid (single-position-per-coin) this is correct; on venues
        that allow multiple lots it may over-flush.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = now_ms - max_age_ms

    actor_set = set(allowed_actors) if allowed_actors else None
    sorted_events = sorted(events, key=lambda e: e.get("ts_ms", 0))
    lots: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for e in sorted_events:
        if e.get("ts_ms", 0) < cutoff:
            continue
        if _score(e) < min_score:
            continue
        if actor_set is not None and actor_of(e) not in actor_set:
            continue
        key = (e["venue"], e["trader"], e["coin"])
        kind = e.get("kind")
        if kind == "OPEN":
            lots[key].append(e)
        elif kind in ("CLOSE", "LIQ"):
            lots[key].clear()

    rows: list[dict[str, Any]] = []
    for (venue, trader, coin), opens in lots.items():
        if not opens:
            continue
        long_n = sum(float(e.get("notional_usd") or 0.0) for e in opens if e["side"] == "long")
        short_n = sum(float(e.get("notional_usd") or 0.0) for e in opens if e["side"] == "short")
        net = long_n - short_n
        if net == 0:
            continue
        side = "long" if net > 0 else "short"
        notional = abs(net)
        latest = max(opens, key=lambda e: e["ts_ms"])
        tr = latest.get("track_record") or {}
        rows.append({
            "venue": venue,
            "trader": trader,
            "coin": coin,
            "side": side,
            "notional_usd": notional,
            "score": _score(latest),
            "tier": _tier(latest),
            "tier_emoji": tr.get("tier_emoji"),
            "actor": actor_of(latest),
            "winrate": tr.get("winrate"),
            "trades_per_day": tr.get("trades_per_day"),
            "leverage": latest.get("leverage"),
            "last_price_usd": latest.get("price_usd"),
            "opened_at_ms": min(e["ts_ms"] for e in opens),
            "last_seen_ms": latest["ts_ms"],
            "lot_count": len(opens),
        })
    rows.sort(key=lambda r: (-r["score"], -r["notional_usd"]))
    return rows[:limit]


# ── Per-coin time-series (for charting) ──────────────────────────────────────
def flow_timeseries(
    events: Iterable[dict],
    *,
    coin: str | None = None,
    bin_ms: int = 5 * 60_000,
    window_ms: int = 6 * 60 * 60_000,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Time-binned flow / smart-flow / volume series suitable for line charts.

    `coin=None` aggregates across all coins. Bins are aligned to `bin_ms`.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_ms
    bins: dict[int, dict[str, float]] = {}
    n_bins = window_ms // bin_ms + 1
    bin_start = (cutoff // bin_ms) * bin_ms
    for i in range(n_bins):
        b = bin_start + i * bin_ms
        bins[b] = {"flow_usd": 0.0, "smart_flow_usd": 0.0, "volume_usd": 0.0}
    for e in events:
        ts = e.get("ts_ms", 0)
        if ts < cutoff:
            continue
        if coin is not None and e.get("coin") != coin:
            continue
        b = (ts // bin_ms) * bin_ms
        slot = bins.get(b)
        if slot is None:
            continue
        f = signed_flow(e)
        n = float(e.get("notional_usd") or 0.0)
        slot["flow_usd"] += f
        slot["volume_usd"] += n
        sc = _score(e)
        if sc > 0:
            slot["smart_flow_usd"] += f * (sc / 100.0)
    return [
        {"ts_ms": ts, **vals}
        for ts, vals in sorted(bins.items())
    ]


# ── Per-coin top traders ─────────────────────────────────────────────────────
def coin_top_traders(
    events: Iterable[dict],
    *,
    coin: str,
    window_ms: int = 24 * 60 * 60_000,
    limit: int = 10,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_ms
    scoped = [
        e for e in events
        if e.get("coin") == coin and e.get("ts_ms", 0) >= cutoff
    ]
    by_trader: dict[tuple[str, str], dict[str, Any]] = {}
    for e in scoped:
        key = (e["venue"], e["trader"])
        agg = by_trader.get(key) or {
            "venue": e["venue"],
            "trader": e["trader"],
            "trades": 0,
            "long_open_notional": 0.0,
            "short_open_notional": 0.0,
            "score": _score(e),
            "tier": _tier(e),
            "tier_emoji": (e.get("track_record") or {}).get("tier_emoji"),
            "last_ts_ms": 0,
        }
        agg["trades"] += 1
        if e["kind"] == "OPEN":
            if e["side"] == "long":
                agg["long_open_notional"] += float(e.get("notional_usd") or 0.0)
            else:
                agg["short_open_notional"] += float(e.get("notional_usd") or 0.0)
        agg["last_ts_ms"] = max(agg["last_ts_ms"], e.get("ts_ms", 0))
        sc = _score(e)
        if sc > agg["score"]:
            agg["score"] = sc
            agg["tier"] = _tier(e)
            agg["tier_emoji"] = (e.get("track_record") or {}).get("tier_emoji")
        by_trader[key] = agg

    rows = list(by_trader.values())
    for r in rows:
        net = r["long_open_notional"] - r["short_open_notional"]
        r["net_open_notional_usd"] = net
        r["bias"] = "long" if net > 0 else ("short" if net < 0 else "flat")
    rows.sort(key=lambda r: (-r["score"], -abs(r["net_open_notional_usd"])))
    return rows[:limit]


# ── Smart-money positioning ──────────────────────────────────────────────────
def smart_money_positioning(
    events: Iterable[dict],
    *,
    window_ms: int = 4 * 60 * 60_000,
    min_tier_rank: int = TIER_RANK["TRUSTED"],
    now_ms: int | None = None,
    min_traders: int = 2,
    allowed_actors: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Per-coin consensus among ≥TRUSTED traders inside a sliding window.

    For each coin we count the distinct trusted wallets that opened LONG vs.
    SHORT, sum the OPEN notional on each side, and compute a `bias` score:

        bias = (long_notional - short_notional) / (long_notional + short_notional)

    Coins with fewer than `min_traders` distinct trusted wallets are dropped
    so noise doesn't dominate the leaderboard. The output is sorted by the
    absolute bias × trader count — the strongest, broadest signals first.

    Use this to answer "where are the pros positioned right now?" — the
    single most useful trading view in the system.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_ms
    actor_set = set(allowed_actors) if allowed_actors else None
    cands = [
        e for e in events
        if e.get("kind") == "OPEN"
        and TIER_RANK.get(_tier(e), 0) >= min_tier_rank
        and e.get("ts_ms", 0) >= cutoff
        and (actor_set is None or actor_of(e) in actor_set)
    ]
    if not cands:
        return []

    per_coin: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "long_traders": set(),
        "short_traders": set(),
        "long_notional": 0.0,
        "short_notional": 0.0,
        "scores": [],
        "last_ts_ms": 0,
    })
    for e in cands:
        c = per_coin[e["coin"]]
        n = float(e.get("notional_usd") or 0.0)
        if e["side"] == "long":
            c["long_traders"].add(e["trader"])
            c["long_notional"] += n
        else:
            c["short_traders"].add(e["trader"])
            c["short_notional"] += n
        c["scores"].append(_score(e))
        c["last_ts_ms"] = max(c["last_ts_ms"], e.get("ts_ms", 0))

    rows: list[dict[str, Any]] = []
    for coin, c in per_coin.items():
        n_long = len(c["long_traders"])
        n_short = len(c["short_traders"])
        n_total = len(c["long_traders"] | c["short_traders"])
        if n_total < min_traders:
            continue
        gross = c["long_notional"] + c["short_notional"]
        net = c["long_notional"] - c["short_notional"]
        bias = net / gross if gross else 0.0
        if bias > 0.15:
            label = "LONG"
        elif bias < -0.15:
            label = "SHORT"
        else:
            label = "MIXED"
        rows.append({
            "coin": coin,
            "label": label,
            "bias": bias,
            "long_traders": n_long,
            "short_traders": n_short,
            "total_traders": n_total,
            "long_notional_usd": c["long_notional"],
            "short_notional_usd": c["short_notional"],
            "net_notional_usd": net,
            "gross_notional_usd": gross,
            "avg_score": (sum(c["scores"]) / len(c["scores"])) if c["scores"] else 0.0,
            "last_ts_ms": c["last_ts_ms"],
            "window_ms": window_ms,
        })
    rows.sort(key=lambda r: (-abs(r["bias"]) * r["total_traders"], -r["gross_notional_usd"]))
    return rows
