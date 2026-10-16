"""Runtime configuration loaded from environment / .env."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass(frozen=True)
class Config:
    threshold_usd: float
    drop_unpriced: bool

    telegram_token: Optional[str]
    telegram_chat_id: Optional[str]

    hl_enabled: bool
    hl_coins: list[str]
    hl_ws_url: str
    hl_info_url: str

    jup_enabled: bool
    jup_coins: list[str]
    jup_enrich_instant: bool       # use getTransaction to enrich Instant* txs
    jup_enrich_concurrency: int    # max concurrent getTransaction calls
    solana_rpc_http: str
    solana_rpc_ws: str

    drift_enabled: bool
    drift_markets: list[str]       # default markets to poll, e.g. SOL-PERP, BTC-PERP
    drift_coins: list[str]         # extra coin filter (e.g. SOL/BTC/ETH only)
    drift_poll_sec: float
    drift_page_size: int
    drift_data_api: str
    drift_max_rps: float           # global cap; 1 RPS is the empirical safe ceiling

    # CEX in/out flow tracker — subscribes to Solana logs for known
    # centralized-exchange hot wallets and emits IN (deposit) / OUT
    # (withdraw) events with USD valuation. Net flow is a leading indicator
    # of selling pressure / accumulation.
    cex_enabled: bool
    cex_min_flow_usd: float
    cex_max_concurrent_enrich: int
    cex_alert_min_usd: float       # 0 = no Telegram alerts for CEX flows

    web_enabled: bool
    web_host: str
    web_port: int

    mongo_enabled: bool
    mongo_uri: str
    mongo_db: str
    mongo_collection: str

    # Actor (bot/human) classifier thresholds — see analytics.classify_actor.
    bot_tpd_threshold: float
    hybrid_tpd_threshold: float
    actor_min_samples: int

    # Telegram-only signal filters. Console keeps showing every whale; these
    # only gate which events fire a Telegram push. All conditions are AND-ed.
    tg_min_notional_usd: float
    tg_min_winrate: float
    tg_allowed_tiers: list[str]

    # Watchlist: any event from one of these wallets bypasses the Telegram
    # filter entirely. Use to follow specific traders you've already vetted.
    tg_watch_wallets: list[str]

    # Conviction-burst alerting: when N+ distinct TRUSTED/ELITE wallets open
    # the same side on the same coin inside a sliding window, push a special
    # "🔥 conviction" Telegram alert.
    conviction_min_traders: int
    conviction_window_min: float
    conviction_cooldown_min: float

    # Position lifecycle: when a TRUSTED/ELITE wallet closes a position the
    # bot previously alerted on, fire an "EXIT" Telegram alert with realized
    # PnL and time-to-close.
    lifecycle_alerts_enabled: bool

    # Active position polling — fetches clearinghouseState for watchlist +
    # TRUSTED+ wallets every interval and publishes diffs to the bus. Catches
    # sub-threshold rebalances and pre-startup positions.
    poll_enabled: bool
    poll_interval_sec: float
    poll_max_wallets: int
    poll_min_change_usd: float

    # Interactive Telegram command bot. When enabled, the same bot token used
    # for push alerts also long-polls for commands like /snap, /status, /bias.
    tg_command_bot_enabled: bool
    tg_authorized_chats: list[int]
    tg_dashboard_url: str
    tg_snapshot_enabled: bool
    tg_auto_snap_min: float

    # Snapshot service performance knobs.
    snap_settle_ms: int           # extra wait per snapshot for live data to paint
    snap_cache_max_age_sec: float # serve the cache when fresher than this
    snap_auto_refresh_sec: float  # background loop cadence; 0 disables prewarm
    snap_reload_every_sec: float  # force a full reload of the warm page periodically

    @property
    def telegram_configured(self) -> bool:
        return bool(self.telegram_token and self.telegram_chat_id)

    @property
    def coins(self) -> list[str]:
        """Union of all coins this bot is set to track, sorted."""
        return sorted(
            set(self.hl_coins) | set(self.jup_coins) | set(self.drift_coins)
        )

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            threshold_usd=float(os.getenv("WHALE_THRESHOLD_USD", "5000000")),
            drop_unpriced=_bool("DROP_UNPRICED", True),
            telegram_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
            hl_enabled=_bool("HL_ENABLED", True),
            hl_coins=_list("HL_COINS", ["BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE", "SUI", "kPEPE"]),
            hl_ws_url=os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws"),
            hl_info_url=os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info"),
            jup_enabled=_bool("JUP_ENABLED", True),
            jup_coins=_list("JUP_COINS", ["SOL", "ETH", "BTC"]),
            jup_enrich_instant=_bool("JUP_ENRICH_INSTANT", True),
            jup_enrich_concurrency=int(os.getenv("JUP_ENRICH_CONCURRENCY", "4")),
            solana_rpc_http=os.getenv("SOLANA_RPC_HTTP", "https://api.mainnet-beta.solana.com"),
            solana_rpc_ws=os.getenv("SOLANA_RPC_WS", "wss://api.mainnet-beta.solana.com"),
            drift_enabled=_bool("DRIFT_ENABLED", True),
            drift_markets=_list("DRIFT_MARKETS", []),
            drift_coins=_list("DRIFT_COINS", []),
            drift_poll_sec=float(os.getenv("DRIFT_POLL_SEC", "10.0")),
            drift_page_size=int(os.getenv("DRIFT_PAGE_SIZE", "50")),
            drift_data_api=os.getenv("DRIFT_DATA_API", "https://data.api.drift.trade"),
            drift_max_rps=float(os.getenv("DRIFT_MAX_RPS", "1.0")),
            cex_enabled=_bool("CEX_FLOWS_ENABLED", True),
            cex_min_flow_usd=float(os.getenv("CEX_MIN_FLOW_USD", "25000")),
            cex_max_concurrent_enrich=int(os.getenv("CEX_MAX_CONCURRENT_ENRICH", "6")),
            cex_alert_min_usd=float(os.getenv("CEX_ALERT_MIN_USD", "0")),
            web_enabled=_bool("WEB_ENABLED", True),
            web_host=os.getenv("WEB_HOST", "127.0.0.1"),
            web_port=int(os.getenv("WEB_PORT", "8787")),
            mongo_enabled=_bool("MONGO_ENABLED", True),
            mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            mongo_db=os.getenv("MONGO_DB", "perps_whales"),
            mongo_collection=os.getenv("MONGO_COLLECTION", "events"),
            bot_tpd_threshold=float(os.getenv("BOT_TPD_THRESHOLD", "30")),
            hybrid_tpd_threshold=float(os.getenv("HYBRID_TPD_THRESHOLD", "5")),
            actor_min_samples=int(os.getenv("ACTOR_MIN_SAMPLES", "10")),
            # Defaults match the user's "only ping me on real signal" preference:
            # ≥ $400K size, ≥ 61% win rate, TRUSTED or ELITE tier only.
            tg_min_notional_usd=float(os.getenv("TG_MIN_NOTIONAL_USD", "400000")),
            tg_min_winrate=float(os.getenv("TG_MIN_WINRATE", "0.61")),
            tg_allowed_tiers=[
                t.upper()
                for t in _list("TG_ALLOWED_TIERS", ["TRUSTED", "ELITE"])
            ],
            tg_watch_wallets=[
                w.lower() for w in _list("TG_WATCH_WALLETS", [])
            ],
            conviction_min_traders=int(os.getenv("CONVICTION_MIN_TRADERS", "3")),
            conviction_window_min=float(os.getenv("CONVICTION_WINDOW_MIN", "30")),
            conviction_cooldown_min=float(os.getenv("CONVICTION_COOLDOWN_MIN", "30")),
            lifecycle_alerts_enabled=_bool("LIFECYCLE_ALERTS_ENABLED", True),
            poll_enabled=_bool("POLL_ENABLED", True),
            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "30")),
            poll_max_wallets=int(os.getenv("POLL_MAX_WALLETS", "40")),
            poll_min_change_usd=float(os.getenv("POLL_MIN_CHANGE_USD", "1000")),
            tg_command_bot_enabled=_bool("TG_COMMAND_BOT_ENABLED", True),
            tg_authorized_chats=_parse_chat_ids(
                os.getenv("TELEGRAM_CHAT_ID"),
                os.getenv("TELEGRAM_AUTHORIZED_CHATS"),
            ),
            tg_dashboard_url=os.getenv(
                "TG_DASHBOARD_URL",
                f"http://127.0.0.1:{int(os.getenv('WEB_PORT', '8787'))}/",
            ),
            tg_snapshot_enabled=_bool("TG_SNAPSHOT_ENABLED", True),
            tg_auto_snap_min=float(os.getenv("TG_AUTO_SNAP_MIN", "0")),
            snap_settle_ms=int(os.getenv("SNAP_SETTLE_MS", "250")),
            snap_cache_max_age_sec=float(os.getenv("SNAP_CACHE_MAX_AGE_SEC", "2.0")),
            snap_auto_refresh_sec=float(os.getenv("SNAP_AUTO_REFRESH_SEC", "5.0")),
            snap_reload_every_sec=float(os.getenv("SNAP_RELOAD_EVERY_SEC", "300")),
        )


def _parse_chat_ids(*sources: Optional[str]) -> list[int]:
    """Merge any number of comma-separated chat-id strings into ints, dedup."""
    out: list[int] = []
    seen: set[int] = set()
    for s in sources:
        if not s:
            continue
        for tok in s.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                v = int(tok)
            except ValueError:
                continue
            if v not in seen:
                seen.add(v)
                out.append(v)
    return out
