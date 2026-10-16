"""Console + Telegram fan-out for whale events."""
from __future__ import annotations

import asyncio
import io
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from rich.console import Console
from rich.text import Text

from ssl_ctx import get_ssl_context
from trackers import EventKind, Side, WhaleEvent
from trackrecord import WalletStats

log = logging.getLogger(__name__)


def _short_addr(addr: str, head: int = 6, tail: int = 4) -> str:
    if not addr or len(addr) <= head + tail + 1:
        return addr
    return f"{addr[:head]}…{addr[-tail:]}"


def _fmt_usd(x: float) -> str:
    if x is None:
        return "?"
    sign = "-" if x < 0 else ""
    a = abs(x)
    if a >= 1_000_000:
        return f"{sign}${a / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a / 1_000:.1f}K"
    return f"{sign}${a:,.0f}"


def _kind_color(kind: EventKind) -> str:
    return {
        EventKind.OPEN: "bright_green",
        EventKind.CLOSE: "bright_yellow",
        EventKind.LIQUIDATION: "bright_red",
    }[kind]


def _side_color(side: Side) -> str:
    return "green" if side == Side.LONG else "red"


def _explorer_link(venue: str, tx_ref: str) -> Optional[str]:
    if venue.lower().startswith("hyper"):
        # Hyperliquid trades don't have a per-trade explorer, link to the trader instead.
        return None
    if venue.lower().startswith("jupiter"):
        return f"https://solscan.io/tx/{tx_ref}"
    return None


def _render_stats_console(stats: WalletStats) -> Text:
    tier = stats.tier()
    line = Text("    ")
    line.append(f" {tier.emoji} {tier.label} ", style=f"bold black on {tier.color}")
    line.append("  ")

    if stats.closed_trades > 0:
        wr = stats.winrate or 0.0
        wr_color = "green" if wr >= 0.55 else "yellow" if wr >= 0.45 else "red"
        line.append(
            f"{stats.win_count}/{stats.closed_trades}W ({wr*100:.0f}%)",
            style=f"bold {wr_color}",
        )
        line.append("  ")
    else:
        line.append("no closed trades", style="dim")
        if stats.coverage_note:
            line.append(f"  {stats.coverage_note}", style="dim")
        return line

    pnl_color = "green" if stats.realized_pnl_usd >= 0 else "red"
    line.append("PnL all-time ", style="dim")
    line.append(_fmt_usd(stats.realized_pnl_usd), style=f"bold {pnl_color}")

    if stats.pnl_30d_usd is not None:
        c = "green" if stats.pnl_30d_usd >= 0 else "red"
        line.append("  30d ", style="dim")
        line.append(_fmt_usd(stats.pnl_30d_usd), style=c)

    if stats.largest_win_usd:
        line.append("  best ", style="dim")
        line.append(_fmt_usd(stats.largest_win_usd), style="green")
    if stats.largest_loss_usd:
        line.append("  worst ", style="dim")
        line.append(_fmt_usd(stats.largest_loss_usd), style="red")

    if stats.days_active:
        line.append(f"  active {stats.days_active:.0f}d", style="dim")
    if stats.coverage_note:
        line.append(f"  {stats.coverage_note}", style="dim")
    return line


def _render_stats_telegram(stats: WalletStats) -> list[str]:
    tier = stats.tier()
    out = [f"{tier.emoji} <b>Track record: {tier.label}</b>"]
    if stats.closed_trades == 0:
        note = stats.coverage_note or "no closed trades"
        out.append(f"   <i>{note}</i>")
        return out
    wr = (stats.winrate or 0.0) * 100
    out.append(
        f"   📊 <b>{stats.win_count}/{stats.closed_trades}W</b> ({wr:.0f}% wr)  "
        f"💰 PnL: <b>{_fmt_usd(stats.realized_pnl_usd)}</b>"
    )
    extras = []
    if stats.pnl_30d_usd is not None:
        extras.append(f"30d {_fmt_usd(stats.pnl_30d_usd)}")
    if stats.largest_win_usd:
        extras.append(f"best {_fmt_usd(stats.largest_win_usd)}")
    if stats.largest_loss_usd:
        extras.append(f"worst {_fmt_usd(stats.largest_loss_usd)}")
    if stats.days_active:
        extras.append(f"{stats.days_active:.0f}d active")
    if extras:
        out.append("   " + "  ·  ".join(extras))
    if stats.coverage_note:
        out.append(f"   <i>{stats.coverage_note}</i>")
    return out


def _trader_link(venue: str, trader: str) -> Optional[str]:
    if venue.lower().startswith("hyper"):
        return f"https://app.hyperliquid.xyz/explorer/address/{trader}"
    if venue.lower().startswith("jupiter"):
        return f"https://solscan.io/account/{trader}"
    return None


class Notifier:
    def __init__(
        self,
        telegram_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
        dedupe_size: int = 4096,
        tg_min_notional_usd: float = 0.0,
        tg_min_winrate: float = 0.0,
        tg_allowed_tiers: Optional[list[str]] = None,
        tg_watch_wallets: Optional[list[str]] = None,
    ) -> None:
        self.console = Console()
        self._telegram_token = telegram_token
        self._telegram_chat_id = telegram_chat_id
        self._session: Optional[aiohttp.ClientSession] = None
        self._seen: deque[str] = deque(maxlen=dedupe_size)
        self._seen_set: set[str] = set()
        self._tg_min_notional_usd = tg_min_notional_usd
        self._tg_min_winrate = tg_min_winrate
        # Empty / None means "no tier filter".
        self._tg_allowed_tiers: Optional[set[str]] = (
            {t.upper() for t in tg_allowed_tiers} if tg_allowed_tiers else None
        )
        # Watchlist — addresses that bypass every filter. Stored lower-cased
        # so EVM/Solana addresses match without the user worrying about case.
        self._watch_wallets: set[str] = (
            {w.strip().lower() for w in tg_watch_wallets if w.strip()}
            if tg_watch_wallets else set()
        )
        # Counters surfaced via .filter_stats() — useful when the user wonders
        # why their phone is quiet.
        self._tg_sent = 0
        self._tg_filtered: dict[str, int] = {
            "notional": 0, "winrate": 0, "tier": 0, "no_track_record": 0,
        }
        self._tg_watch_pushes = 0
        self._tg_conviction_pushes = 0
        self._tg_lifecycle_pushes = 0

    def is_watched(self, trader: str) -> bool:
        return bool(trader) and trader.lower() in self._watch_wallets

    # ── Runtime state mutators (used by the Telegram command bot) ──────────
    # Push notifications gate. When True, whale alerts and conviction /
    # lifecycle pushes are silently dropped at the Notifier layer so the
    # user can mute their phone without restarting the bot. Console output
    # is unaffected.
    _muted: bool = False  # initialised in __init__ below via setdefault

    @property
    def muted(self) -> bool:
        # Defined as a property so subclasses / tests can override; the
        # underlying attribute is created lazily in __init__.
        return getattr(self, "_muted", False)

    def mute(self) -> None:
        self._muted = True

    def unmute(self) -> None:
        self._muted = False

    def set_min_notional(self, usd: float) -> None:
        self._tg_min_notional_usd = max(0.0, float(usd))

    def set_min_winrate(self, ratio: float) -> None:
        self._tg_min_winrate = max(0.0, min(1.0, float(ratio)))

    def set_allowed_tiers(self, tiers: Optional[list[str]]) -> None:
        if tiers is None or not tiers:
            self._tg_allowed_tiers = None
        else:
            self._tg_allowed_tiers = {t.upper() for t in tiers}

    def add_watch(self, addr: str) -> bool:
        a = (addr or "").strip().lower()
        if not a:
            return False
        if a in self._watch_wallets:
            return False
        self._watch_wallets.add(a)
        return True

    def remove_watch(self, addr: str) -> bool:
        a = (addr or "").strip().lower()
        if a in self._watch_wallets:
            self._watch_wallets.discard(a)
            return True
        return False

    def list_watch(self) -> list[str]:
        return sorted(self._watch_wallets)

    def filter_settings(self) -> dict:
        """Snapshot of all runtime-tunable filter knobs — for /status."""
        return {
            "muted": self.muted,
            "min_notional_usd": self._tg_min_notional_usd,
            "min_winrate": self._tg_min_winrate,
            "allowed_tiers": (
                sorted(self._tg_allowed_tiers)
                if self._tg_allowed_tiers else None
            ),
            "watchlist_size": len(self._watch_wallets),
        }

    async def send_html(self, text: str) -> None:
        """Public escape hatch — used by trading-signal watchers (conviction
        burst, lifecycle exits) to push their own pre-formatted HTML messages
        without going through the WhaleEvent template.

        Bypasses the mute switch on purpose: command-bot replies, snapshot
        status updates, etc. need to reach the user even when alerts are
        muted. Use `send_alert_html()` if you want the mute gate.
        """
        if not (self._telegram_token and self._telegram_chat_id):
            return
        await self._post_telegram(text)

    async def send_alert_html(self, text: str) -> None:
        """Same as send_html but respects the mute switch."""
        if self.muted:
            return
        await self.send_html(text)

    async def send_photo(
        self,
        png: bytes,
        *,
        caption: Optional[str] = None,
        filename: str = "dashboard.png",
        chat_id: Optional[str] = None,
    ) -> bool:
        """Push a single PNG (with optional HTML caption) to Telegram.

        Returns True on success. Used by the command bot's /snap flow.
        Photos must be ≤ 10 MB; we don't enforce that here because the
        dashboard rarely produces anything close to that limit, but
        Telegram will reject oversized payloads with a 400 we log.
        """
        if not (self._telegram_token and self._telegram_chat_id):
            return False
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self._telegram_token}/sendPhoto"
        target_chat = chat_id or self._telegram_chat_id
        form = aiohttp.FormData()
        form.add_field("chat_id", str(target_chat))
        if caption:
            # Telegram limits captions to 1024 chars; truncate defensively.
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
        form.add_field(
            "photo", io.BytesIO(png),
            filename=filename, content_type="image/png",
        )
        try:
            async with self._session.post(url, data=form, timeout=30) as r:
                if r.status == 200:
                    return True
                body = await r.text()
                log.warning("telegram sendPhoto %s: %s", r.status, body[:200])
                return False
        except Exception as exc:
            log.warning("telegram sendPhoto error: %s", exc)
            return False

    async def __aenter__(self) -> "Notifier":
        connector = aiohttp.TCPConnector(ssl=get_ssl_context())
        self._session = aiohttp.ClientSession(connector=connector)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    def banner(self, threshold_usd: float, venues: list[str]) -> None:
        self.console.rule("[bold magenta]Solana Perps Whale Tracker[/bold magenta]")
        self.console.print(
            f"  threshold: [bold]{_fmt_usd(threshold_usd)}[/bold]   "
            f"venues: [cyan]{', '.join(venues) or '(none)'}[/cyan]   "
            f"telegram: {'[green]on[/green]' if self._telegram_token else '[dim]off[/dim]'}",
        )
        if self._telegram_token and self._tg_filter_active():
            tiers = (
                "/".join(sorted(self._tg_allowed_tiers))
                if self._tg_allowed_tiers else "any"
            )
            self.console.print(
                f"  telegram filter: ≥ [bold]{_fmt_usd(self._tg_min_notional_usd)}[/bold]   "
                f"≥ [bold]{self._tg_min_winrate*100:.0f}%[/bold] wr   "
                f"tier in [bold]{tiers}[/bold]",
                style="dim",
            )
        if self._watch_wallets:
            self.console.print(
                f"  watchlist: [bold cyan]{len(self._watch_wallets)}[/bold cyan] "
                f"wallet(s) bypass the filter",
                style="dim",
            )
        self.console.rule()

    def _tg_filter_active(self) -> bool:
        return (
            self._tg_min_notional_usd > 0
            or self._tg_min_winrate > 0
            or self._tg_allowed_tiers is not None
        )

    def filter_stats(self) -> dict:
        return {"sent": self._tg_sent, **self._tg_filtered}

    async def emit(self, ev: WhaleEvent) -> None:
        key = ev.dedupe_key
        if key in self._seen_set:
            return
        self._seen.append(key)
        self._seen_set.add(key)
        if len(self._seen) == self._seen.maxlen:
            # eviction is handled by deque; rebuild the set occasionally.
            self._seen_set = set(self._seen)

        self._print_console(ev)
        if self._telegram_token and self._telegram_chat_id and not self.muted:
            watched = self.is_watched(ev.trader)
            ok, reason = (True, "watchlist") if watched else self._passes_tg_filter(ev)
            if ok:
                self._tg_sent += 1
                if watched:
                    self._tg_watch_pushes += 1
                await self._send_telegram(ev, watched=watched)
            else:
                if reason in self._tg_filtered:
                    self._tg_filtered[reason] += 1
                log.debug(
                    "telegram filter: skipped %s %s %s notional=%.0f reason=%s",
                    ev.venue, ev.kind.value, ev.coin, ev.notional_usd, reason,
                )

    def _passes_tg_filter(self, ev: WhaleEvent) -> tuple[bool, str]:
        """Returns (allowed, reason). reason is the rule that rejected it."""
        if self._tg_min_notional_usd > 0 and ev.notional_usd < self._tg_min_notional_usd:
            return False, "notional"

        # Track-record-dependent rules: only meaningful if we have stats.
        wr_filter = self._tg_min_winrate > 0
        tier_filter = self._tg_allowed_tiers is not None
        if not (wr_filter or tier_filter):
            return True, ""

        stats = ev.extra.get("track_record") if isinstance(ev.extra, dict) else None
        if not isinstance(stats, WalletStats):
            # User explicitly asked for trusted traders — no record => no push.
            return False, "no_track_record"

        if wr_filter:
            wr = stats.winrate or 0.0
            if wr < self._tg_min_winrate:
                return False, "winrate"

        if tier_filter:
            tier_label = stats.tier().label.upper()
            if tier_label not in self._tg_allowed_tiers:
                return False, "tier"

        return True, ""

    # ── console ───────────────────────────────────────────────────────────────
    def _print_console(self, ev: WhaleEvent) -> None:
        ts = datetime.fromtimestamp(ev.ts_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        kind_txt = Text(f" {ev.kind.value} ", style=f"bold black on {_kind_color(ev.kind)}")
        side_txt = Text(ev.side.value.upper(), style=f"bold {_side_color(ev.side)}")
        watch_prefix = "⭐ " if self.is_watched(ev.trader) else ""

        line = Text.assemble(
            (f"{ts}  ", "dim"),
            (watch_prefix, "bold yellow"),
            (f"[{ev.venue}] ", "bold cyan"),
            kind_txt,
            "  ",
            side_txt,
            (f"  {ev.coin}", "bold"),
            (f"  size=", "dim"),
            (f"{ev.size:,.4f}", "white"),
            (f"  px=", "dim"),
            (f"${ev.price_usd:,.4f}", "white"),
            (f"  notional=", "dim"),
            (f"{_fmt_usd(ev.notional_usd)}", "bold yellow"),
            (f"  trader=", "dim"),
            (_short_addr(ev.trader), "magenta"),
        )
        if ev.pnl_usd is not None:
            color = "green" if ev.pnl_usd >= 0 else "red"
            line.append("  pnl=", style="dim")
            line.append(_fmt_usd(ev.pnl_usd), style=f"bold {color}")
        if ev.leverage:
            line.append(f"  lev={ev.leverage:.1f}x", style="dim")
        self.console.print(line)

        stats = ev.extra.get("track_record") if isinstance(ev.extra, dict) else None
        if isinstance(stats, WalletStats):
            self.console.print(_render_stats_console(stats))

    # ── telegram ──────────────────────────────────────────────────────────────
    async def _send_telegram(self, ev: WhaleEvent, *, watched: bool = False) -> None:
        emoji = {"OPEN": "🟢", "CLOSE": "🟡", "LIQ": "🔴"}[ev.kind.value]
        side_emoji = "📈" if ev.side == Side.LONG else "📉"
        title_prefix = "⭐ <b>WATCHED</b>  " if watched else ""
        lines = [
            f"{title_prefix}<b>{emoji} {ev.venue} — {ev.kind.value}</b>",
            f"{side_emoji} <b>{ev.side.value.upper()}</b> <b>{ev.coin}</b>",
            f"💵 Notional: <b>{_fmt_usd(ev.notional_usd)}</b>",
            f"📦 Size: <code>{ev.size:,.4f}</code>",
            f"🏷️ Price: <code>${ev.price_usd:,.4f}</code>",
        ]
        if ev.pnl_usd is not None:
            lines.append(f"💰 PnL: <b>{_fmt_usd(ev.pnl_usd)}</b>")
        if ev.leverage:
            lines.append(f"⚖️ Leverage: <b>{ev.leverage:.1f}x</b>")

        trader_link = _trader_link(ev.venue, ev.trader)
        trader_label = _short_addr(ev.trader)
        if trader_link:
            lines.append(f"👤 Trader: <a href=\"{trader_link}\"><code>{trader_label}</code></a>")
        else:
            lines.append(f"👤 Trader: <code>{trader_label}</code>")

        tx_link = _explorer_link(ev.venue, ev.tx_ref)
        if tx_link:
            lines.append(f"🔗 <a href=\"{tx_link}\">Tx</a>")

        stats = ev.extra.get("track_record") if isinstance(ev.extra, dict) else None
        if isinstance(stats, WalletStats):
            lines.append("")
            lines.extend(_render_stats_telegram(stats))

        text = "\n".join(lines)
        await self._post_telegram(text)

    async def _post_telegram(self, text: str, retries: int = 2) -> None:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self._telegram_token}/sendMessage"
        payload = {
            "chat_id": self._telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        for attempt in range(retries + 1):
            try:
                async with self._session.post(url, json=payload, timeout=10) as r:
                    if r.status == 200:
                        return
                    body = await r.text()
                    log.warning("telegram %s: %s", r.status, body[:200])
                    if r.status == 429:
                        # Telegram rate limit — back off as instructed.
                        try:
                            j = await r.json(content_type=None)
                            retry_after = int(j.get("parameters", {}).get("retry_after", 1))
                        except Exception:
                            retry_after = 2
                        await asyncio.sleep(retry_after + 0.5)
                        continue
            except Exception as exc:
                log.warning("telegram error: %s", exc)
            await asyncio.sleep(1.0 * (attempt + 1))
