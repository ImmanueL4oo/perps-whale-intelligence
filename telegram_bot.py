"""Interactive Telegram command bot for the perp whale tracker.

Long-polls the Telegram Bot API for messages, dispatches commands, and runs
a small scheduled-snapshot task so the user can ask for the dashboard from
their phone.

Architecture
------------
* Single `getUpdates` long-poll loop drives everything. We keep a monotonic
  `update_id` offset so a restart doesn't re-fire commands the user already
  saw a reply to (Telegram retains the queue server-side for ~24h).
* Each command runs in its own task so a slow snapshot doesn't block the
  next `/status`. The dispatcher itself is sync.
* All commands return formatted HTML; long replies use Telegram's HTML
  parse mode and we cap each reply at ~3500 chars (Telegram's hard limit
  is 4096) — anything longer is truncated with a "…" suffix.
* Authorization: an allow-list of `chat_id`s is checked on every update.
  By default the list is `{TELEGRAM_CHAT_ID}` so only the user who set up
  the bot can talk to it. Optional override via `TELEGRAM_AUTHORIZED_CHATS`.
* Mutating commands (/watch /threshold /tier /mute) flow through small
  handles on the Notifier so the change is live without a restart.

The bot is deliberately self-contained — no third-party telegram library —
so we don't pull in another async stack on top of aiohttp.
"""
from __future__ import annotations

import asyncio
import logging
import shlex
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

import aiohttp

import analytics
from dashboard_snapshot import SnapshotResult, SnapshotService
from event_bus import EventBus
from notifier import Notifier
from ssl_ctx import get_ssl_context

log = logging.getLogger("telegram_bot")


def _fmt_usd(x: Optional[float]) -> str:
    if x is None:
        return "?"
    sign = "-" if x < 0 else ""
    a = abs(x)
    if a >= 1_000_000:
        return f"{sign}${a / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a / 1_000:.1f}K"
    return f"{sign}${a:,.0f}"


def _short(addr: str, head: int = 6, tail: int = 4) -> str:
    if not addr or len(addr) <= head + tail + 1:
        return addr
    return f"{addr[:head]}…{addr[-tail:]}"


def _parse_money(raw: str) -> float:
    """Lenient $-amount parser. Accepts:
        '250000', '250_000', '250,000', '$250,000',
        '1.5M', '$1.5m', '500K', '2.3b'
    Raises ValueError if it can't extract a number."""
    s = raw.strip().replace("$", "").replace("_", "").replace(",", "")
    if not s:
        raise ValueError("empty")
    mult = 1.0
    if s[-1].lower() in ("k", "m", "b"):
        mult = {"k": 1_000.0, "m": 1_000_000.0, "b": 1_000_000_000.0}[s[-1].lower()]
        s = s[:-1]
    return float(s) * mult


def _ago(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return "—"
    sec = max(0, int(time.time() - ts_ms / 1000))
    if sec < 60:
        return f"{sec}s ago"
    if sec < 3600:
        return f"{sec // 60}m ago"
    if sec < 86400:
        return f"{sec // 3600}h ago"
    return f"{sec // 86400}d ago"


# ── Command parsing ───────────────────────────────────────────────────────────
@dataclass
class ParsedCommand:
    name: str          # e.g. "snap", "auto" — no leading slash, lowercased
    args: list[str]    # whitespace-split argv
    raw: str           # original text after stripping the command word


def parse_command(text: str) -> Optional[ParsedCommand]:
    """Parse a `/cmd[@bot_username] arg1 arg2 ...` message.

    Returns None for non-command text. Robust against the @botname suffix
    that Telegram appends in group chats.
    """
    if not text or not text.startswith("/"):
        return None
    body = text[1:].strip()
    if not body:
        return None
    head, sep, tail = body.partition(" ")
    cmd = head.lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]  # /status@my_bot → status
    raw = tail.strip() if sep else ""
    try:
        args = shlex.split(raw) if raw else []
    except ValueError:
        # Unmatched quote — fall back to naive split.
        args = raw.split()
    return ParsedCommand(name=cmd, args=args, raw=raw)


# ── Command bot ───────────────────────────────────────────────────────────────
@dataclass
class _AutoSnap:
    enabled: bool = False
    interval_sec: float = 0.0
    last_fired_at: float = 0.0
    target_chats: set[int] = field(default_factory=set)
    full_page: bool = False


class TelegramCommandBot:
    """Long-polling Telegram bot wired to the live tracker state.

    Inputs
    ------
    notifier        — used for sending replies / photos and mutating filter state
    bus             — for /status, /signals, /coin queries
    poller          — for /positions; may be None if polling is disabled
    snapshot        — for /snap; may be None if Playwright isn't installed
    dashboard_url   — passed to snapshot.snap()
    authorized_chats — only these chat IDs may issue commands
    """

    def __init__(
        self,
        *,
        notifier: Notifier,
        bus: EventBus,
        poller: Optional[Any],
        snapshot: Optional[SnapshotService],
        dashboard_url: str,
        token: str,
        authorized_chats: Iterable[int],
        cfg_threshold_usd: float,
        venues: list[str],
    ) -> None:
        self.notifier = notifier
        self.bus = bus
        self.poller = poller
        self.snapshot = snapshot
        self.dashboard_url = dashboard_url
        self.token = token
        self.authorized_chats: set[int] = {int(c) for c in authorized_chats if c}
        self.cfg_threshold_usd = cfg_threshold_usd
        self.venues = venues

        self._session: Optional[aiohttp.ClientSession] = None
        self._update_offset = 0
        self._poll_task: Optional[asyncio.Task] = None
        self._auto_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._auto = _AutoSnap()
        # Tracks in-flight command tasks so shutdown can await them.
        self._inflight: set[asyncio.Task] = set()
        # Awaiting-input map: chat_id → (kind, expires_at_monotonic).
        # Set when a button asks for a typed follow-up (e.g. "Custom
        # threshold", "Add wallet"). Consumed by the next plain-text
        # message from that chat. Times out after 60 s so a forgotten
        # prompt doesn't capture a future genuine command.
        self._awaiting: dict[int, tuple[str, float]] = {}
        self._input_ttl_sec: float = 60.0

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def start(self) -> None:
        connector = aiohttp.TCPConnector(ssl=get_ssl_context())
        self._session = aiohttp.ClientSession(connector=connector)
        # Best-effort: tell Telegram about the commands list so the user gets
        # autocomplete in their chat. Failures here are non-fatal.
        try:
            await self._set_my_commands()
        except Exception:
            log.debug("setMyCommands failed", exc_info=True)
        self._poll_task = asyncio.create_task(self._poll_loop(), name="tg-poll")
        self._auto_task = asyncio.create_task(self._auto_loop(), name="tg-auto-snap")
        log.info(
            "telegram command bot started — authorized chats=%s",
            sorted(self.authorized_chats),
        )

    async def stop(self) -> None:
        self._stop.set()
        for t in (self._poll_task, self._auto_task):
            if t is not None:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        if self._inflight:
            for t in list(self._inflight):
                t.cancel()
            await asyncio.gather(*self._inflight, return_exceptions=True)
        if self._session is not None:
            await self._session.close()

    # ── HTTP helpers ──────────────────────────────────────────────────────────
    async def _get(self, method: str, **params) -> Optional[dict]:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        try:
            async with self._session.get(url, params=params, timeout=60) as r:
                if r.status == 200:
                    j = await r.json()
                    return j
                body = await r.text()
                log.debug("telegram %s -> %s: %s", method, r.status, body[:200])
        except Exception as exc:
            log.debug("telegram %s error: %s", method, exc)
        return None

    async def _post(self, method: str, payload: dict) -> Optional[dict]:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        try:
            async with self._session.post(url, json=payload, timeout=10) as r:
                if r.status == 200:
                    return await r.json()
                body = await r.text()
                log.warning("telegram %s -> %s: %s", method, r.status, body[:200])
        except Exception as exc:
            log.warning("telegram %s error: %s", method, exc)
        return None

    def _fire_chat_action(self, chat_id: int, action: str) -> None:
        """Send sendChatAction without awaiting — pure UX feedback so the
        user sees "typing…" / "uploading photo…" the moment their message
        is dispatched, before the actual reply lands.

        Telegram clears the indicator after ~5 s automatically, so we
        don't have to manage state. Failures are logged at debug only.
        """
        async def _send() -> None:
            try:
                await self._post("sendChatAction", {
                    "chat_id": chat_id, "action": action,
                })
            except Exception:
                log.debug("sendChatAction failed", exc_info=True)
        # Detach — we don't want to block the dispatcher on this.
        asyncio.create_task(_send(), name="tg-chat-action")

    async def _set_my_commands(self) -> None:
        """Register a command list so Telegram clients show autocomplete."""
        cmds = [
            {"command": "menu", "description": "open the button menu"},
            {"command": "help", "description": "show all commands"},
            {"command": "snap", "description": "screenshot the dashboard"},
            {"command": "auto", "description": "schedule periodic snapshots"},
            {"command": "status", "description": "bot health + filter settings"},
            {"command": "bias", "description": "current LONG / SHORT consensus"},
            {"command": "top", "description": "top traders by score"},
            {"command": "coin", "description": "/coin BTC — quick coin stats"},
            {"command": "positions", "description": "live polled positions"},
            {"command": "signals", "description": "current consensus bursts"},
            {"command": "watch", "description": "/watch 0xabc — add wallet"},
            {"command": "unwatch", "description": "/unwatch 0xabc — remove"},
            {"command": "watchlist", "description": "list watched wallets"},
            {"command": "mute", "description": "pause whale push alerts"},
            {"command": "unmute", "description": "resume whale push alerts"},
            {"command": "threshold", "description": "/threshold 250000 — min notional"},
            {"command": "winrate", "description": "/winrate 0.55 — min win rate"},
            {"command": "tier", "description": "/tier ELITE,TRUSTED — allowed tiers"},
        ]
        await self._post("setMyCommands", {"commands": cmds})

    async def _send_text(
        self,
        chat_id: int,
        text: str,
        reply_markup: Optional[dict] = None,
    ) -> Optional[int]:
        """Send a message; return its message_id (so we can later edit it)."""
        # Telegram cap is 4096; leave room for the </b> etc.
        if len(text) > 3800:
            text = text[:3700] + "\n\n…<i>truncated</i>"
        payload: dict = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        resp = await self._post("sendMessage", payload)
        if resp and resp.get("ok"):
            return ((resp.get("result") or {}).get("message_id"))
        return None

    async def _edit_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: Optional[dict] = None,
    ) -> bool:
        """Replace the text + keyboard of an existing message in-place."""
        if len(text) > 3800:
            text = text[:3700] + "\n\n…<i>truncated</i>"
        payload: dict = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        resp = await self._post("editMessageText", payload)
        return bool(resp and resp.get("ok"))

    async def _answer_callback(
        self,
        callback_query_id: str,
        text: Optional[str] = None,
        *,
        show_alert: bool = False,
    ) -> None:
        """Acknowledge a callback so Telegram clears the spinner on the
        button. Optionally show a toast or alert popup."""
        payload: dict = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text[:200]
            payload["show_alert"] = show_alert
        await self._post("answerCallbackQuery", payload)

    # ── long-poll loop ────────────────────────────────────────────────────────
    async def _poll_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                # Long-poll: Telegram returns immediately on the first new
                # update. Keep the timeout tight enough that a graceful stop
                # doesn't take ~30 s to take effect, but large enough that
                # we're not hammering the API in a tight loop when the chat
                # is quiet.
                resp = await self._get(
                    "getUpdates",
                    offset=self._update_offset,
                    timeout=20,
                    allowed_updates='["message","callback_query"]',
                )
                if not resp or not resp.get("ok"):
                    await asyncio.sleep(min(backoff, 10))
                    backoff = min(backoff * 2, 10)
                    continue
                backoff = 1.0
                for update in resp.get("result") or []:
                    self._update_offset = max(
                        self._update_offset, int(update.get("update_id", 0)) + 1,
                    )
                    if "message" in update:
                        self._spawn_handler(update["message"])
                    elif "callback_query" in update:
                        self._spawn_callback(update["callback_query"])
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("telegram poll loop error")
                await asyncio.sleep(2.0)

    def _spawn_handler(self, msg: dict) -> None:
        """Schedule a per-message handler on the event loop."""
        task = asyncio.create_task(self._handle_message(msg), name="tg-cmd")
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    def _spawn_callback(self, cq: dict) -> None:
        task = asyncio.create_task(self._handle_callback(cq), name="tg-cb")
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _handle_message(self, msg: dict) -> None:
        chat = (msg.get("chat") or {})
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip()
        if not isinstance(chat_id, int):
            return
        if self.authorized_chats and chat_id not in self.authorized_chats:
            log.info("rejected message from unauthorized chat %s", chat_id)
            return

        # Awaiting-input flow: previous button asked for a typed value
        # ("Custom threshold", "Add wallet"). Consume it now and skip the
        # normal command parser.
        pending = self._awaiting.get(chat_id)
        if pending is not None and not text.startswith("/"):
            kind, expires_at = pending
            if time.monotonic() > expires_at:
                self._awaiting.pop(chat_id, None)
            else:
                self._awaiting.pop(chat_id, None)
                await self._consume_input(kind, text, chat_id)
                return

        cmd = parse_command(text)
        if cmd is None:
            return  # ignore plain chatter
        try:
            await self._dispatch(cmd, chat_id)
        except Exception:
            log.exception("command handler crashed (%s)", cmd.name)
            await self._send_text(chat_id, "⚠️ Command handler crashed — check logs.")

    async def _handle_callback(self, cq: dict) -> None:
        """Process an inline-keyboard button tap.

        Each `callback_data` is `<bucket>:<arg>` (or just `<bucket>`). We
        ack the callback first so the spinner clears on the user's screen,
        then run the actual handler — many of which simply edit the
        message in place to show the next sub-menu.
        """
        cq_id = cq.get("id")
        data = cq.get("data") or ""
        from_user = cq.get("from") or {}
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        message_id = msg.get("message_id")
        if not isinstance(chat_id, int) or not isinstance(message_id, int):
            return
        if self.authorized_chats and chat_id not in self.authorized_chats:
            log.info("rejected callback from unauthorized chat %s", chat_id)
            if cq_id:
                await self._answer_callback(cq_id, "Not authorized.", show_alert=True)
            return

        # Always answer fast (≤3 s SLA) so the user's tap clears.
        if cq_id:
            asyncio.create_task(self._answer_callback(cq_id))

        try:
            await self._dispatch_callback(data, chat_id, message_id)
        except Exception:
            log.exception("callback dispatch crashed (data=%r)", data)
            await self._send_text(chat_id, "⚠️ Button handler crashed — check logs.")

    # ── command dispatch ──────────────────────────────────────────────────────
    async def _dispatch(self, cmd: ParsedCommand, chat_id: int) -> None:
        name = cmd.name
        # Fire-and-forget UX feedback — user sees "typing…" / "uploading photo…"
        # the instant their message is acked, before we do any real work.
        if name in ("snap", "screenshot", "shot"):
            self._fire_chat_action(chat_id, "upload_photo")
        else:
            self._fire_chat_action(chat_id, "typing")

        # /start and /menu both open the inline keyboard menu — that's the
        # primary UX. Plain /help still dumps the command list as text for
        # power users.
        if name in ("start", "menu"):
            text, kb = self._main_menu()
            await self._send_text(chat_id, text, reply_markup=kb)
            return
        if name == "help":
            await self._send_text(chat_id, self._help_text())
            return
        if name in ("snap", "screenshot", "shot"):
            await self._cmd_snap(cmd, chat_id)
            return
        if name == "auto":
            await self._cmd_auto(cmd, chat_id)
            return
        if name == "status":
            await self._send_text(chat_id, self._status_text())
            return
        if name == "bias":
            await self._send_text(chat_id, self._bias_text())
            return
        if name == "top":
            await self._send_text(chat_id, self._top_text(cmd.args))
            return
        if name == "coin":
            await self._send_text(chat_id, self._coin_text(cmd.args))
            return
        if name == "positions":
            await self._send_text(chat_id, self._positions_text(cmd.args))
            return
        if name == "signals":
            await self._send_text(chat_id, self._signals_text())
            return
        if name == "watch":
            await self._send_text(chat_id, self._watch_text(cmd.args, add=True))
            return
        if name == "unwatch":
            await self._send_text(chat_id, self._watch_text(cmd.args, add=False))
            return
        if name == "watchlist":
            await self._send_text(chat_id, self._watchlist_text())
            return
        if name == "mute":
            self.notifier.mute()
            await self._send_text(chat_id, "🔇 <b>Muted</b> — push alerts paused. Use /unmute to resume.")
            return
        if name == "unmute":
            self.notifier.unmute()
            await self._send_text(chat_id, "🔔 <b>Unmuted</b> — push alerts active.")
            return
        if name == "threshold":
            await self._send_text(chat_id, self._threshold_text(cmd.args))
            return
        if name == "winrate":
            await self._send_text(chat_id, self._winrate_text(cmd.args))
            return
        if name == "tier":
            await self._send_text(chat_id, self._tier_text(cmd.args))
            return
        await self._send_text(chat_id, f"Unknown command: <code>/{name}</code>\nTry /help.")

    # ── command implementations ───────────────────────────────────────────────
    def _help_text(self) -> str:
        return (
            "<b>📱 Whale tracker bot</b>\n\n"
            "<b>Snapshots</b>\n"
            "  /snap — picture of the dashboard\n"
            "  /snap full — full-page (scrolling) capture\n"
            "  /auto on 5 — push a snapshot every 5 minutes\n"
            "  /auto on 5 full — same, but full page\n"
            "  /auto off — stop scheduled snapshots\n"
            "  /auto status — show schedule\n\n"
            "<b>Live state</b>\n"
            "  /status — bot health, filter knobs, freshness\n"
            "  /bias — current LONG/SHORT consensus among ≥TRUSTED traders\n"
            "  /top [N] — top traders by score (default 10)\n"
            "  /coin SYMBOL — flow + smart flow + recent trades on a coin\n"
            "  /positions [min_usd] — live polled positions\n"
            "  /signals — current consensus bursts\n\n"
            "<b>Watchlist</b>\n"
            "  /watch ADDR — alert on every event from this wallet\n"
            "  /unwatch ADDR\n"
            "  /watchlist — current list\n\n"
            "<b>Filters</b> (mutate live, no restart)\n"
            "  /mute /unmute — pause/resume push alerts\n"
            "  /threshold 250000 — min notional for a push\n"
            "  /winrate 0.55 — min trader win rate\n"
            "  /tier ELITE,TRUSTED — allowed tiers (or 'any')\n\n"
            "Tap <b>/menu</b> for a button-based UI.\n"
        )

    # ── Inline-keyboard UI ────────────────────────────────────────────────────
    # Every menu builder returns (text, reply_markup). reply_markup is a
    # plain dict shaped like Telegram's `InlineKeyboardMarkup`. We keep these
    # as raw dicts (no helper class) so it's easy to eyeball the layout.
    #
    # callback_data convention:
    #   "menu:<id>"   — navigate to a sub-menu (edits the current message)
    #   "act:<id>"    — perform an action (sends a new message)
    #   "set:<k>:<v>" — apply a value (updates filter / state, then refreshes)
    #   "ask:<k>"     — prompt user for typed input (custom value, address)
    #   "noop"        — passive label (header / spacer)
    #
    # We never embed user data in callback_data — Telegram caps it at 64 B
    # and addresses are too long. Watchlist entries are picked by index
    # against the live list at render time.

    @staticmethod
    def _kb(rows: list[list[tuple[str, str]]]) -> dict:
        """Tiny helper to build an InlineKeyboardMarkup from `[(label, cb),…]`."""
        return {
            "inline_keyboard": [
                [{"text": label, "callback_data": data} for label, data in row]
                for row in rows
            ],
        }

    def _main_menu(self) -> tuple[str, dict]:
        muted = self.notifier.muted
        text = (
            "<b>📱 Whale tracker</b>\n"
            "<i>Tap a button. Type /help for the full text-command list.</i>"
            + ("\n\n🔇 <b>Push alerts are muted.</b>" if muted else "")
        )
        kb = self._kb([
            [("📊 Dashboard",  "menu:dash"),    ("📡 Status",   "act:status")],
            [("🧭 Bias",       "act:bias"),     ("🔥 Signals",  "act:signals")],
            [("🏅 Top",        "menu:top"),     ("💱 Coins",    "menu:coins")],
            [("🔄 Positions",  "act:positions"),("⭐ Watchlist","menu:watch")],
            [("⚙️ Settings",   "menu:settings"),("⏰ Auto-snap","menu:auto")],
            [("❓ Help",       "act:help"),     ("🔁 Refresh",  "menu:main")],
        ])
        return text, kb

    def _dashboard_menu(self) -> tuple[str, dict]:
        text = (
            "<b>📊 Dashboard</b>\n"
            "Cached snaps are served instantly when ≤2 s old."
        )
        kb = self._kb([
            [("📷 Snap (cached)", "act:snap"),
             ("⚡ Snap fresh",     "act:snap_fresh")],
            [("📜 Full-page",     "act:snap_full")],
            [("⏰ Auto-snap →",   "menu:auto")],
            [("◀️ Back",          "menu:main")],
        ])
        return text, kb

    def _settings_menu(self) -> tuple[str, dict]:
        f = self.notifier.filter_settings()
        tiers = "/".join(f["allowed_tiers"]) if f["allowed_tiers"] else "any"
        muted_btn = ("🔔 Unmute", "act:unmute") if f["muted"] else ("🔇 Mute", "act:mute")
        text = (
            "<b>⚙️ Settings — push filter</b>\n"
            f"  Min notional:  <b>{_fmt_usd(f['min_notional_usd'])}</b>\n"
            f"  Min winrate:   <b>{f['min_winrate'] * 100:.0f}%</b>\n"
            f"  Allowed tiers: <b>{tiers}</b>\n"
            f"  Watchlist:     <b>{f['watchlist_size']}</b>\n"
            f"  Muted:         <b>{'yes' if f['muted'] else 'no'}</b>\n"
        )
        kb = self._kb([
            [muted_btn],
            [("🎚 Threshold →",  "menu:threshold")],
            [("🎯 Winrate →",    "menu:winrate")],
            [("🎫 Tier →",       "menu:tier")],
            [("⭐ Watchlist →",  "menu:watch")],
            [("◀️ Back",         "menu:main")],
        ])
        return text, kb

    def _threshold_menu(self) -> tuple[str, dict]:
        cur = self.notifier.filter_settings()["min_notional_usd"]
        text = (
            "<b>🎚 Min notional</b>\n"
            f"Currently <b>{_fmt_usd(cur)}</b>. Pick a preset or send a custom value."
        )
        kb = self._kb([
            [("$50K",  "set:thresh:50000"),
             ("$100K", "set:thresh:100000"),
             ("$250K", "set:thresh:250000")],
            [("$400K", "set:thresh:400000"),
             ("$1M",   "set:thresh:1000000"),
             ("$5M",   "set:thresh:5000000")],
            [("✏️ Custom…", "ask:thresh")],
            [("◀️ Back",    "menu:settings")],
        ])
        return text, kb

    def _winrate_menu(self) -> tuple[str, dict]:
        cur = self.notifier.filter_settings()["min_winrate"]
        text = (
            "<b>🎯 Min winrate</b>\n"
            f"Currently <b>{cur * 100:.0f}%</b>. Pick a preset or send custom."
        )
        kb = self._kb([
            [("0%",  "set:wr:0"),  ("50%", "set:wr:50"),
             ("55%", "set:wr:55"), ("60%", "set:wr:60")],
            [("61%", "set:wr:61"), ("65%", "set:wr:65"),
             ("70%", "set:wr:70")],
            [("✏️ Custom…", "ask:wr")],
            [("◀️ Back",    "menu:settings")],
        ])
        return text, kb

    def _tier_menu(self) -> tuple[str, dict]:
        cur = self.notifier.filter_settings()["allowed_tiers"]
        cur_lbl = "/".join(cur) if cur else "any"
        text = (
            "<b>🎫 Allowed tiers</b>\n"
            f"Currently <b>{cur_lbl}</b>. ELITE = best, NEW = unscored."
        )
        kb = self._kb([
            [("ELITE only",      "set:tier:ELITE"),
             ("TRUSTED only",    "set:tier:TRUSTED")],
            [("ELITE+TRUSTED",   "set:tier:ELITE,TRUSTED")],
            [("ELITE+TRUSTED+MIXED", "set:tier:ELITE,TRUSTED,MIXED")],
            [("Any tier (off)",  "set:tier:any")],
            [("◀️ Back",         "menu:settings")],
        ])
        return text, kb

    def _top_menu(self) -> tuple[str, dict]:
        text = (
            "<b>🏅 Top traders</b>\n"
            "Pick how many to show (ranked by score this session)."
        )
        kb = self._kb([
            [("Top 5",  "set:top:5"),
             ("Top 10", "set:top:10"),
             ("Top 20", "set:top:20")],
            [("◀️ Back", "menu:main")],
        ])
        return text, kb

    def _coins_menu(self) -> tuple[str, dict]:
        # Build a button grid from the live coin scoreboard so the user
        # taps coins that actually have flow right now. Falls back to the
        # configured tracking list if the scoreboard is empty.
        events = self.bus.recent()
        scoreboard = analytics.coin_scoreboard(events)
        symbols = [c["coin"] for c in scoreboard][:9]
        if not symbols:
            symbols = ["BTC", "ETH", "SOL"]
        rows = [
            [(s, f"set:coin:{s}") for s in symbols[i : i + 3]]
            for i in range(0, len(symbols), 3)
        ]
        rows.append([("◀️ Back", "menu:main")])
        text = (
            "<b>💱 Coins</b>\n"
            f"Tap a coin for a 15 m / 1 h / 24 h breakdown."
            + (f"  ({len(symbols)} active)" if scoreboard else "")
        )
        return text, self._kb(rows)

    def _auto_menu(self) -> tuple[str, dict]:
        a = self._auto
        if a.enabled:
            mode = "full-page" if a.full_page else "viewport"
            line = (
                f"<b>⏰ Auto-snap</b>: ON · every <b>{a.interval_sec / 60:g}</b> min · "
                f"{mode} · {len(a.target_chats)} chat(s)"
            )
        else:
            line = "<b>⏰ Auto-snap</b>: <i>off</i>"
        rows = [
            [("Every 1 min",  "set:auto:1"),
             ("Every 5 min",  "set:auto:5")],
            [("Every 15 min", "set:auto:15"),
             ("Every 30 min", "set:auto:30"),
             ("Every 60 min", "set:auto:60")],
            [
                ("📜 Toggle full-page", "set:auto:full")
                if not a.full_page
                else ("🖼 Toggle viewport", "set:auto:full"),
            ],
            [("🛑 Off", "set:auto:off")],
            [("◀️ Back", "menu:main")],
        ]
        return line, self._kb(rows)

    def _watch_menu(self) -> tuple[str, dict]:
        addrs = self.notifier.list_watch()
        text = (
            f"<b>⭐ Watchlist</b> ({len(addrs)})\n"
            + "\n".join(f"  <code>{a}</code>" for a in addrs[:20])
            + ("\n  <i>…+more</i>" if len(addrs) > 20 else "")
            + ("\n<i>(empty — add the first one)</i>" if not addrs else "")
        )
        rows: list[list[tuple[str, str]]] = [
            [("➕ Add wallet", "ask:watch")],
        ]
        # Removal buttons by index — keeps callback_data short and avoids
        # baking long addresses in.
        for i, a in enumerate(addrs[:10]):
            rows.append([(f"➖ Remove {_short(a)}", f"set:unwatch:{i}")])
        rows.append([("◀️ Back", "menu:main")])
        return text, self._kb(rows)

    # ── callback dispatch ─────────────────────────────────────────────────────
    async def _dispatch_callback(
        self, data: str, chat_id: int, message_id: int,
    ) -> None:
        """Route a button tap to a handler.

        Most navigation just edits the message in place to swap the
        keyboard. Actions ("act:*") send a new reply so the menu stays
        visible and the user can keep tapping.
        """
        if data == "menu:main":
            await self._edit_message(chat_id, message_id, *self._main_menu())
            return
        if data == "menu:dash":
            await self._edit_message(chat_id, message_id, *self._dashboard_menu())
            return
        if data == "menu:settings":
            await self._edit_message(chat_id, message_id, *self._settings_menu())
            return
        if data == "menu:threshold":
            await self._edit_message(chat_id, message_id, *self._threshold_menu())
            return
        if data == "menu:winrate":
            await self._edit_message(chat_id, message_id, *self._winrate_menu())
            return
        if data == "menu:tier":
            await self._edit_message(chat_id, message_id, *self._tier_menu())
            return
        if data == "menu:top":
            await self._edit_message(chat_id, message_id, *self._top_menu())
            return
        if data == "menu:coins":
            await self._edit_message(chat_id, message_id, *self._coins_menu())
            return
        if data == "menu:auto":
            await self._edit_message(chat_id, message_id, *self._auto_menu())
            return
        if data == "menu:watch":
            await self._edit_message(chat_id, message_id, *self._watch_menu())
            return

        if data.startswith("act:"):
            await self._exec_action(data[4:], chat_id, message_id)
            return
        if data.startswith("set:"):
            await self._exec_set(data[4:], chat_id, message_id)
            return
        if data.startswith("ask:"):
            await self._exec_ask(data[4:], chat_id)
            return
        if data == "noop":
            return
        log.debug("unknown callback data: %r", data)

    async def _exec_action(
        self, action: str, chat_id: int, message_id: int,
    ) -> None:
        """One-shot actions triggered by a button tap."""
        if action == "snap":
            self._fire_chat_action(chat_id, "upload_photo")
            await self._cmd_snap(ParsedCommand("snap", [], ""), chat_id)
            return
        if action == "snap_fresh":
            self._fire_chat_action(chat_id, "upload_photo")
            await self._cmd_snap(ParsedCommand("snap", ["fresh"], "fresh"), chat_id)
            return
        if action == "snap_full":
            self._fire_chat_action(chat_id, "upload_photo")
            await self._cmd_snap(ParsedCommand("snap", ["full"], "full"), chat_id)
            return
        self._fire_chat_action(chat_id, "typing")
        if action == "status":
            await self._send_text(chat_id, self._status_text())
        elif action == "bias":
            await self._send_text(chat_id, self._bias_text())
        elif action == "signals":
            await self._send_text(chat_id, self._signals_text())
        elif action == "positions":
            await self._send_text(chat_id, self._positions_text([]))
        elif action == "help":
            await self._send_text(chat_id, self._help_text())
        elif action == "mute":
            self.notifier.mute()
            await self._edit_message(chat_id, message_id, *self._settings_menu())
        elif action == "unmute":
            self.notifier.unmute()
            await self._edit_message(chat_id, message_id, *self._settings_menu())
        else:
            log.debug("unknown action: %r", action)

    async def _exec_set(
        self, payload: str, chat_id: int, message_id: int,
    ) -> None:
        """`set:` callbacks — a single value is being applied."""
        head, _, value = payload.partition(":")
        if head == "thresh":
            try:
                v = float(value)
            except ValueError:
                return
            self.notifier.set_min_notional(v)
            await self._edit_message(chat_id, message_id, *self._threshold_menu())
        elif head == "wr":
            try:
                pct = float(value)
            except ValueError:
                return
            self.notifier.set_min_winrate(pct / 100.0 if pct > 1 else pct)
            await self._edit_message(chat_id, message_id, *self._winrate_menu())
        elif head == "tier":
            if value.lower() in ("any", "off", "none", ""):
                self.notifier.set_allowed_tiers(None)
            else:
                self.notifier.set_allowed_tiers(value.split(","))
            await self._edit_message(chat_id, message_id, *self._tier_menu())
        elif head == "top":
            try:
                n = int(value)
            except ValueError:
                n = 10
            await self._send_text(chat_id, self._top_text([str(n)]))
        elif head == "coin":
            await self._send_text(chat_id, self._coin_text([value]))
        elif head == "auto":
            await self._exec_set_auto(value, chat_id, message_id)
        elif head == "unwatch":
            try:
                idx = int(value)
            except ValueError:
                return
            addrs = self.notifier.list_watch()
            if 0 <= idx < len(addrs):
                self.notifier.remove_watch(addrs[idx])
            await self._edit_message(chat_id, message_id, *self._watch_menu())
        else:
            log.debug("unknown set: %r", payload)

    async def _exec_set_auto(
        self, value: str, chat_id: int, message_id: int,
    ) -> None:
        if value == "off":
            self._auto.enabled = False
            self._auto.target_chats.clear()
        elif value == "full":
            # Toggle full-page mode; doesn't change schedule.
            self._auto.full_page = not self._auto.full_page
        else:
            try:
                minutes = float(value)
            except ValueError:
                return
            self._auto.enabled = True
            self._auto.interval_sec = minutes * 60.0
            self._auto.target_chats.add(chat_id)
            self._auto.last_fired_at = 0.0  # fire one immediately
        await self._edit_message(chat_id, message_id, *self._auto_menu())

    async def _exec_ask(self, kind: str, chat_id: int) -> None:
        """Mark the chat as awaiting a typed value and prompt for it."""
        prompts = {
            "thresh": (
                "✏️ <b>Custom threshold</b>\n"
                "Send the min notional in USD (e.g. <code>175000</code> or "
                "<code>$1.5M</code>). Cancel: send any /command."
            ),
            "wr": (
                "✏️ <b>Custom winrate</b>\n"
                "Send a value between 0 and 1 (or 0–100%). "
                "Cancel: send any /command."
            ),
            "watch": (
                "✏️ <b>Add wallet to watchlist</b>\n"
                "Send the address (0x… for HL, base58 for Solana). "
                "Cancel: send any /command."
            ),
        }
        if kind not in prompts:
            return
        self._awaiting[chat_id] = (kind, time.monotonic() + self._input_ttl_sec)
        await self._send_text(chat_id, prompts[kind])

    async def _consume_input(self, kind: str, text: str, chat_id: int) -> None:
        """The user just typed a follow-up to an `ask:` prompt."""
        if kind == "thresh":
            reply = self._threshold_text([text])
        elif kind == "wr":
            v = text.replace("%", "").strip()
            reply = self._winrate_text([v])
        elif kind == "watch":
            reply = self._watch_text([text.split()[0]], add=True)
        else:
            return
        # Send the confirmation, then re-open the menu so they can tap on.
        await self._send_text(chat_id, reply)
        text2, kb = self._main_menu()
        await self._send_text(chat_id, text2, reply_markup=kb)

    async def _cmd_snap(self, cmd: ParsedCommand, chat_id: int) -> None:
        if self.snapshot is None or not self.snapshot.available:
            reason = (
                self.snapshot.unavailable_reason
                if self.snapshot else "snapshot service not configured"
            )
            await self._send_text(
                chat_id,
                f"📷 Snapshot unavailable.\n<i>{reason}</i>",
            )
            return

        # `/snap fresh` / `/snap nocache` forces a brand-new capture; without
        # that flag we serve the warm cache when it's young enough, which
        # cuts the user-visible latency to a single sendPhoto round trip.
        bypass_cache = any(
            a.lower() in ("fresh", "nocache", "live", "now") for a in cmd.args
        )
        full_page = any(a.lower() in ("full", "-f", "--full") for a in cmd.args)
        result: SnapshotResult = await self.snapshot.snap(
            self.dashboard_url,
            full_page=full_page,
            bypass_cache=bypass_cache,
        )
        if not result.ok:
            await self._send_text(
                chat_id,
                f"📷 Snapshot failed after {result.elapsed_ms} ms.\n<i>{result.error}</i>",
            )
            return
        cache_tag = " · cached" if result.cached else ""
        caption = (
            f"📊 <b>Dashboard</b> — {result.mode}\n"
            f"⏱ {result.elapsed_ms} ms{cache_tag} · "
            f"{len(result.png) // 1024} KB · {datetime_now()}"
        )
        await self.notifier.send_photo(
            result.png, caption=caption,
            filename=f"dashboard-{int(time.time())}.png",
            chat_id=str(chat_id),
        )

    async def _cmd_auto(self, cmd: ParsedCommand, chat_id: int) -> None:
        if not cmd.args:
            await self._send_text(chat_id, self._auto_status_text())
            return
        action = cmd.args[0].lower()
        if action in ("status", "?", "show"):
            await self._send_text(chat_id, self._auto_status_text())
            return
        if action in ("off", "stop", "0", "false", "disable"):
            self._auto.enabled = False
            self._auto.target_chats.clear()
            await self._send_text(chat_id, "🛑 Scheduled snapshots <b>disabled</b>.")
            return
        if action in ("on", "every", "set", "start"):
            try:
                minutes = float(cmd.args[1])
            except (IndexError, ValueError):
                await self._send_text(
                    chat_id,
                    "Usage: <code>/auto on 5</code> (every 5 minutes), "
                    "optional <code>full</code> at the end.",
                )
                return
            if minutes < 1 or minutes > 24 * 60:
                await self._send_text(chat_id, "Interval must be between 1 and 1440 minutes.")
                return
            self._auto.enabled = True
            self._auto.interval_sec = minutes * 60.0
            self._auto.full_page = "full" in [a.lower() for a in cmd.args[2:]]
            self._auto.target_chats.add(chat_id)
            self._auto.last_fired_at = 0.0  # fire one immediately
            await self._send_text(
                chat_id,
                f"⏰ Scheduled snapshots <b>enabled</b> — every "
                f"<b>{minutes:g}</b> min, mode "
                f"<b>{'full-page' if self._auto.full_page else 'viewport'}</b>.",
            )
            return
        await self._send_text(
            chat_id,
            "Usage:\n"
            "<code>/auto on 5</code> — every 5 min\n"
            "<code>/auto on 5 full</code> — full page\n"
            "<code>/auto off</code>\n"
            "<code>/auto status</code>",
        )

    async def _auto_loop(self) -> None:
        """Background task that fires scheduled snapshots."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(5)
                if not self._auto.enabled or not self._auto.target_chats:
                    continue
                now = time.monotonic()
                if now - self._auto.last_fired_at < self._auto.interval_sec:
                    continue
                self._auto.last_fired_at = now
                if self.snapshot is None or not self.snapshot.available:
                    log.debug("auto-snap fired but service unavailable, skipping")
                    continue
                result = await self.snapshot.snap(
                    self.dashboard_url, full_page=self._auto.full_page,
                )
                if not result.ok:
                    log.warning("auto-snap failed: %s", result.error)
                    continue
                caption = (
                    f"⏰ <b>Auto-snap</b> · every "
                    f"{self._auto.interval_sec / 60:g}m · "
                    f"{result.mode} · {result.elapsed_ms}ms"
                )
                for cid in list(self._auto.target_chats):
                    await self.notifier.send_photo(
                        result.png, caption=caption,
                        filename=f"auto-{int(time.time())}.png",
                        chat_id=str(cid),
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("auto-snap loop error")

    def _auto_status_text(self) -> str:
        if not self._auto.enabled:
            return (
                "⏰ <b>Scheduled snapshots</b>: <i>off</i>\n"
                "Enable with <code>/auto on 5</code> (every 5 min)."
            )
        next_in = max(
            0.0,
            self._auto.interval_sec - (time.monotonic() - self._auto.last_fired_at),
        )
        return (
            "⏰ <b>Scheduled snapshots</b>: <b>on</b>\n"
            f"Interval: <b>{self._auto.interval_sec / 60:g}</b> min\n"
            f"Mode: <b>{'full-page' if self._auto.full_page else 'viewport'}</b>\n"
            f"Recipients: <b>{len(self._auto.target_chats)}</b>\n"
            f"Next fire in: <b>{next_in:.0f}s</b>"
        )

    def _status_text(self) -> str:
        recent = self.bus.recent()
        now_ms = int(time.time() * 1000)
        last_ts = max((e.get("ts_ms") or 0 for e in recent), default=0)
        cut1m = now_ms - 60_000
        cut5m = now_ms - 5 * 60_000
        n1m = sum(1 for e in recent if (e.get("ts_ms") or 0) >= cut1m)
        n5m = sum(1 for e in recent if (e.get("ts_ms") or 0) >= cut5m)
        by_venue: dict[str, int] = {}
        for e in recent:
            v = e.get("venue") or "?"
            by_venue[v] = by_venue.get(v, 0) + 1
        venues = ", ".join(f"{v}:{n}" for v, n in sorted(by_venue.items())) or "—"
        f = self.notifier.filter_settings()
        tiers = "/".join(f["allowed_tiers"]) if f["allowed_tiers"] else "any"
        muted_tag = " 🔇" if f["muted"] else ""
        poll_line = "Polling: <i>disabled</i>"
        if self.poller is not None:
            try:
                snaps = len(getattr(self.poller, "snapshots", {}) or {})
                interval = getattr(self.poller, "interval_sec", "?")
                wallets = len(getattr(self.poller, "watchlist", set()) or set())
                poll_line = (
                    f"Polling: every <b>{interval}s</b> · "
                    f"<b>{snaps}</b> snapshot(s) · "
                    f"<b>{wallets}</b> watch wallet(s)"
                )
            except Exception:
                poll_line = "Polling: enabled (introspection failed)"
        snap_line = "Snapshot: <i>not available</i>"
        if self.snapshot is not None and self.snapshot.available:
            snap_line = "Snapshot: <b>ready</b>"
        elif self.snapshot is not None and self.snapshot.unavailable_reason:
            snap_line = f"Snapshot: <i>{self.snapshot.unavailable_reason}</i>"
        return (
            f"<b>📡 Whale tracker · /status{muted_tag}</b>\n"
            f"Venues: <b>{', '.join(self.venues) or '—'}</b>\n"
            f"Events buffered: <b>{len(recent)}</b> "
            f"(by venue: {venues})\n"
            f"Last whale: <b>{_ago(last_ts)}</b>\n"
            f"Throughput: <b>{n1m}</b>/1m · <b>{n5m}</b>/5m\n"
            f"{poll_line}\n"
            f"{snap_line}\n\n"
            f"<b>Filter</b>\n"
            f"  Min notional: <b>{_fmt_usd(f['min_notional_usd'])}</b>\n"
            f"  Min winrate: <b>{f['min_winrate'] * 100:.0f}%</b>\n"
            f"  Allowed tiers: <b>{tiers}</b>\n"
            f"  Watchlist: <b>{f['watchlist_size']}</b> wallet(s)\n"
            f"  Muted: <b>{'yes' if f['muted'] else 'no'}</b>"
        )

    def _bias_text(self) -> str:
        events = self.bus.recent()
        rows = analytics.smart_money_positioning(
            events, window_ms=4 * 3600_000,
            min_tier_rank=analytics.TIER_RANK["TRUSTED"],
            min_traders=2,
        )
        if not rows:
            return (
                "🧭 <b>Bias</b>: <i>not enough trusted-trader OPENs in the last "
                "4 h to call a consensus.</i>"
            )
        # Collapse to a single LONG/SHORT/NEUTRAL signal weighted by gross
        # notional. Same idea as the dashboard's bias card but text-only.
        long_n = sum(r["long_notional_usd"] for r in rows)
        short_n = sum(r["short_notional_usd"] for r in rows)
        gross = long_n + short_n
        bias = (long_n - short_n) / gross if gross else 0.0
        arrow = "▲" if bias > 0.05 else "▼" if bias < -0.05 else "—"
        label = "LONG" if bias > 0.05 else "SHORT" if bias < -0.05 else "NEUTRAL"
        head = (
            f"🧭 <b>Bias: {arrow} {label}</b>  "
            f"({bias * 100:+.0f}% net)\n"
            f"Long ${long_n / 1e6:.2f}M · Short ${short_n / 1e6:.2f}M\n\n"
            f"<b>Top coins</b>\n"
        )
        # Show top 6 by abs(bias × trader count).
        rows_sorted = sorted(
            rows, key=lambda r: abs(r["bias"]) * (r["trader_count"] or 1),
            reverse=True,
        )[:6]
        lines = []
        for r in rows_sorted:
            lp = (r["long_notional_usd"] / r["gross_notional_usd"] * 100) \
                if r["gross_notional_usd"] else 50
            tag = "▲L" if lp >= 60 else "▼S" if lp <= 40 else "≈"
            lines.append(
                f"  <code>{r['coin']:>6}</code>  "
                f"{tag} {lp:>3.0f}% long · "
                f"{r['trader_count']} pros · "
                f"{_fmt_usd(r['gross_notional_usd'])}"
            )
        return head + "\n".join(lines)

    def _top_text(self, args: list[str]) -> str:
        try:
            limit = max(1, min(25, int(args[0]))) if args else 10
        except ValueError:
            limit = 10
        rows = self.bus.leaderboard(limit=limit)
        if not rows:
            return "🏅 <b>Top traders</b>: <i>no scored wallets in this session yet.</i>"
        out = ["🏅 <b>Top traders this session</b>"]
        for i, r in enumerate(rows, 1):
            wr = r.get("winrate")
            wr_s = f"{wr * 100:.0f}%" if wr is not None else "—"
            out.append(
                f"<b>{i:>2}.</b> {r.get('tier_emoji') or ''} "
                f"<code>{_short(r['trader'])}</code> · "
                f"score <b>{(r.get('score') or 0):.0f}</b> · "
                f"wr {wr_s} · "
                f"PnL {_fmt_usd(r.get('realized_pnl_usd'))} · "
                f"{r.get('events_seen', 0)} ev · {r.get('venue', '')}"
            )
        return "\n".join(out)

    def _coin_text(self, args: list[str]) -> str:
        if not args:
            return "Usage: <code>/coin BTC</code>"
        coin = args[0].upper()
        events = self.bus.recent()
        scoreboard = analytics.coin_scoreboard(events)
        row = next((c for c in scoreboard if c["coin"].upper() == coin), None)
        if not row:
            return f"💱 <b>{coin}</b>: <i>no recent activity in the buffer.</i>"
        # Pull 1h window stats — that's the single most useful summary.
        w1h = (row.get("windows") or {}).get(str(60 * 60_000), {})
        w15m = (row.get("windows") or {}).get(str(15 * 60_000), {})
        w24h = (row.get("windows") or {}).get(str(24 * 3600_000), {})
        last_px = row.get("last_price_usd")
        return (
            f"💱 <b>{coin}</b>"
            + (f" · ${last_px:,.4f}" if last_px else "")
            + "\n"
            f"  <b>15m</b>  flow {_fmt_usd(w15m.get('flow_usd'))} · "
            f"vol {_fmt_usd(w15m.get('volume_usd'))}\n"
            f"  <b>1h</b>   flow {_fmt_usd(w1h.get('flow_usd'))} · "
            f"smart {_fmt_usd(w1h.get('smart_flow_usd'))} · "
            f"vol {_fmt_usd(w1h.get('volume_usd'))} · "
            f"liqs {w1h.get('liq_count') or 0}\n"
            f"  <b>24h</b>  flow {_fmt_usd(w24h.get('flow_usd'))} · "
            f"vol {_fmt_usd(w24h.get('volume_usd'))}"
        )

    def _positions_text(self, args: list[str]) -> str:
        if self.poller is None:
            return "🔄 Live polling is <i>disabled</i> (POLL_ENABLED=false)."
        try:
            min_n = float(args[0]) if args else 0.0
        except ValueError:
            min_n = 0.0
        positions = self.poller.latest_positions(min_notional_usd=min_n)
        if not positions:
            return "🔄 No positions above the threshold right now."
        positions = sorted(positions, key=lambda p: -p.notional_usd)[:10]
        lines = ["🔄 <b>Live positions (polled)</b>"]
        for p in positions:
            pnl = ""
            if p.unrealized_pnl_usd is not None:
                pnl_emoji = "🟢" if p.unrealized_pnl_usd >= 0 else "🔴"
                pnl = f" · {pnl_emoji} {_fmt_usd(p.unrealized_pnl_usd)}"
            arrow = "▲" if p.side == "long" else "▼"
            lev = f" · {p.leverage:.1f}x" if p.leverage else ""
            lines.append(
                f"  {arrow} <b>{p.coin}</b> {_fmt_usd(p.notional_usd)}{lev}"
                f" · <code>{_short(p.wallet)}</code>{pnl}"
            )
        return "\n".join(lines)

    def _signals_text(self) -> str:
        events = self.bus.recent()
        sigs = analytics.consensus_signals(
            events, window_ms=30 * 60_000, min_traders=2,
        )
        if not sigs:
            return "🔥 <b>No consensus bursts</b> in the last 30 min."
        lines = ["🔥 <b>Consensus signals · last 30 min</b>"]
        for s in sigs[:6]:
            arrow = "▲" if s["side"] == "long" else "▼"
            lines.append(
                f"  {arrow} <b>{s['trader_count']}</b> {s['side']}s on "
                f"<b>{s['coin']}</b> · "
                f"{_fmt_usd(s['total_notional_usd'])} · "
                f"avg score {s.get('avg_score', 0):.0f}"
            )
        return "\n".join(lines)

    def _watch_text(self, args: list[str], *, add: bool) -> str:
        if not args:
            verb = "watch" if add else "unwatch"
            return f"Usage: <code>/{verb} 0xabc…</code>"
        addr = args[0]
        if add:
            ok = self.notifier.add_watch(addr)
            if ok:
                return f"⭐ <b>Watching</b> <code>{_short(addr)}</code>."
            return f"<i>Already on watchlist:</i> <code>{_short(addr)}</code>"
        ok = self.notifier.remove_watch(addr)
        if ok:
            return f"☆ <b>Removed</b> <code>{_short(addr)}</code>."
        return f"<i>Not on watchlist:</i> <code>{_short(addr)}</code>"

    def _watchlist_text(self) -> str:
        addrs = self.notifier.list_watch()
        if not addrs:
            return "⭐ Watchlist: <i>empty</i>. Add with <code>/watch 0xabc…</code>"
        lines = [f"⭐ <b>Watchlist</b> ({len(addrs)})"]
        for a in addrs[:30]:
            lines.append(f"  <code>{a}</code>")
        if len(addrs) > 30:
            lines.append(f"  …+{len(addrs) - 30} more")
        return "\n".join(lines)

    def _threshold_text(self, args: list[str]) -> str:
        if not args:
            cur = self.notifier.filter_settings()["min_notional_usd"]
            return (
                f"🎚 Min notional currently <b>{_fmt_usd(cur)}</b>.\n"
                f"Set with <code>/threshold 250000</code> (USD)."
            )
        try:
            v = _parse_money(args[0])
        except ValueError:
            return (
                "❌ Couldn't parse a number. Try "
                "<code>/threshold 250000</code> or <code>1.5M</code>."
            )
        if v < 0:
            return "❌ Threshold must be ≥ 0."
        self.notifier.set_min_notional(v)
        return f"✅ Min notional set to <b>{_fmt_usd(v)}</b>."

    def _winrate_text(self, args: list[str]) -> str:
        if not args:
            cur = self.notifier.filter_settings()["min_winrate"]
            return (
                f"🎯 Min winrate currently <b>{cur * 100:.0f}%</b>.\n"
                f"Set with <code>/winrate 0.55</code> (or <code>55</code>)."
            )
        try:
            v = float(args[0].rstrip("%"))
        except ValueError:
            return "❌ Couldn't parse a number."
        if v > 1.0:
            v /= 100.0
        if not (0.0 <= v <= 1.0):
            return "❌ Winrate must be between 0 and 1 (or 0–100%)."
        self.notifier.set_min_winrate(v)
        return f"✅ Min winrate set to <b>{v * 100:.0f}%</b>."

    def _tier_text(self, args: list[str]) -> str:
        if not args:
            cur = self.notifier.filter_settings()["allowed_tiers"]
            shown = "/".join(cur) if cur else "any"
            return (
                f"🎫 Allowed tiers currently <b>{shown}</b>.\n"
                f"Set with <code>/tier ELITE,TRUSTED</code> or <code>/tier any</code>."
            )
        raw = " ".join(args).replace(",", " ")
        toks = [t.upper() for t in raw.split() if t.strip()]
        if any(t in ("ANY", "ALL", "*", "NONE", "OFF") for t in toks):
            self.notifier.set_allowed_tiers(None)
            return "✅ Tier filter cleared (any tier allowed)."
        valid = {"ELITE", "TRUSTED", "MIXED", "POOR", "NEW", "UNKNOWN"}
        chosen = [t for t in toks if t in valid]
        unknown = [t for t in toks if t not in valid]
        if not chosen:
            return f"❌ No valid tier names in {toks}. Valid: {sorted(valid)}"
        self.notifier.set_allowed_tiers(chosen)
        msg = f"✅ Allowed tiers set to <b>{'/'.join(chosen)}</b>."
        if unknown:
            msg += f"\n<i>Ignored unknown:</i> {unknown}"
        return msg


def datetime_now() -> str:
    """Compact UTC stamp for captions."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


__all__ = ["TelegramCommandBot", "parse_command", "ParsedCommand"]
