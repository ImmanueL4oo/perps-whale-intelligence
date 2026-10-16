"""Unit tests for the Telegram command bot — pure-Python pieces only.

We don't actually hit the Telegram API. Instead we exercise:

  • parse_command() — robust against `/cmd@bot_name args`, quoted args,
    empty args, mixed-case command words.
  • Notifier runtime mutators added for the command bot (mute/unmute,
    threshold/winrate/tier setters, watchlist add/remove).
  • Each `_xxx_text()` helper on TelegramCommandBot — these are pure
    functions over the bus + notifier state, so we can build a stub
    bus and assert the rendered HTML.

Snapshot service / aiohttp polling loops are integration concerns and
are intentionally not covered here.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_bus import EventBus
from notifier import Notifier
from telegram_bot import TelegramCommandBot, parse_command


# ── parse_command ────────────────────────────────────────────────────────────
def test_parse_command_simple():
    c = parse_command("/snap")
    assert c is not None and c.name == "snap" and c.args == []


def test_parse_command_with_args():
    c = parse_command("/auto on 5 full")
    assert c.name == "auto"
    assert c.args == ["on", "5", "full"]


def test_parse_command_strips_bot_suffix():
    c = parse_command("/status@my_perp_bot")
    assert c.name == "status"


def test_parse_command_lowercases():
    c = parse_command("/SnaP Full")
    assert c.name == "snap"
    assert c.args == ["Full"]  # args keep original case


def test_parse_command_quoted_args():
    c = parse_command('/coin "BTC PERP"')
    assert c.args == ["BTC PERP"]


def test_parse_command_unmatched_quote_falls_back_to_split():
    c = parse_command("/coin 'BTC")
    assert c.args == ["'BTC"]


def test_parse_command_returns_none_for_non_commands():
    assert parse_command("hello there") is None
    assert parse_command("") is None
    assert parse_command("/") is None


# ── Notifier mutators ─────────────────────────────────────────────────────────
def _notifier() -> Notifier:
    return Notifier(
        telegram_token="x", telegram_chat_id="y",
        tg_min_notional_usd=400_000,
        tg_min_winrate=0.61,
        tg_allowed_tiers=["TRUSTED", "ELITE"],
    )


def test_mute_unmute():
    n = _notifier()
    assert n.muted is False
    n.mute()
    assert n.muted is True
    n.unmute()
    assert n.muted is False


def test_threshold_setter_clamps_negative():
    n = _notifier()
    n.set_min_notional(250_000)
    assert n._tg_min_notional_usd == 250_000
    n.set_min_notional(-1)
    assert n._tg_min_notional_usd == 0.0


def test_winrate_setter_clamps_to_unit_range():
    n = _notifier()
    n.set_min_winrate(0.55)
    assert n._tg_min_winrate == 0.55
    n.set_min_winrate(1.5)
    assert n._tg_min_winrate == 1.0
    n.set_min_winrate(-0.1)
    assert n._tg_min_winrate == 0.0


def test_tier_setter_accepts_none_and_list():
    n = _notifier()
    n.set_allowed_tiers(["elite", "trusted"])
    assert n._tg_allowed_tiers == {"ELITE", "TRUSTED"}
    n.set_allowed_tiers(None)
    assert n._tg_allowed_tiers is None
    n.set_allowed_tiers([])
    assert n._tg_allowed_tiers is None


def test_watch_add_remove_dedupes_case_insensitive():
    n = _notifier()
    assert n.add_watch("0xABC") is True
    assert n.add_watch("0xabc") is False  # already present (case-insensitive)
    assert n.is_watched("0xAbC")
    assert n.list_watch() == ["0xabc"]
    assert n.remove_watch("0XABC") is True
    assert n.remove_watch("0xabc") is False
    assert not n.is_watched("0xabc")


def test_filter_settings_snapshot_shape():
    n = _notifier()
    s = n.filter_settings()
    assert set(s.keys()) == {
        "muted", "min_notional_usd", "min_winrate",
        "allowed_tiers", "watchlist_size",
    }
    assert s["muted"] is False
    assert s["allowed_tiers"] == ["ELITE", "TRUSTED"]


# ── Command rendering ─────────────────────────────────────────────────────────
def _bot() -> TelegramCommandBot:
    return TelegramCommandBot(
        notifier=_notifier(),
        bus=EventBus(),
        poller=None,
        snapshot=None,
        dashboard_url="http://127.0.0.1:8787/",
        token="x",
        authorized_chats=[1],
        cfg_threshold_usd=5_000_000.0,
        venues=["Hyperliquid"],
    )


def test_help_text_lists_every_command():
    txt = _bot()._help_text()
    for c in (
        "/snap", "/auto", "/status", "/bias", "/top", "/coin",
        "/positions", "/signals", "/watch", "/unwatch", "/watchlist",
        "/mute", "/unmute", "/threshold", "/winrate", "/tier",
    ):
        assert c in txt, f"help is missing {c}"


def test_status_text_renders_without_events():
    txt = _bot()._status_text()
    assert "Whale tracker" in txt
    assert "Filter" in txt
    assert "Min notional" in txt
    assert "$400.0K" in txt or "$400K" in txt


def test_threshold_text_no_args_shows_current():
    bot = _bot()
    txt = bot._threshold_text([])
    assert "currently" in txt
    assert "$400" in txt


def test_threshold_text_sets_value_with_human_input():
    bot = _bot()
    txt = bot._threshold_text(["$1,500,000"])
    assert "$1.50M" in txt
    assert bot.notifier.filter_settings()["min_notional_usd"] == 1_500_000


def test_winrate_text_accepts_percent_or_ratio():
    bot = _bot()
    bot._winrate_text(["55"])
    assert bot.notifier.filter_settings()["min_winrate"] == 0.55
    bot._winrate_text(["0.7"])
    assert bot.notifier.filter_settings()["min_winrate"] == 0.7


def test_tier_text_any_clears_filter():
    bot = _bot()
    bot._tier_text(["ELITE"])
    assert bot.notifier.filter_settings()["allowed_tiers"] == ["ELITE"]
    bot._tier_text(["any"])
    assert bot.notifier.filter_settings()["allowed_tiers"] is None


def test_tier_text_rejects_garbage():
    bot = _bot()
    txt = bot._tier_text(["XYZ"])
    assert "No valid tier" in txt
    # Filter unchanged.
    assert bot.notifier.filter_settings()["allowed_tiers"] == ["ELITE", "TRUSTED"]


def test_watch_text_round_trip():
    bot = _bot()
    txt = bot._watch_text(["0xabc"], add=True)
    assert "Watching" in txt
    txt = bot._watch_text(["0xabc"], add=True)
    assert "Already" in txt
    txt = bot._watch_text(["0xabc"], add=False)
    assert "Removed" in txt
    txt = bot._watch_text(["0xabc"], add=False)
    assert "Not on" in txt


def test_watchlist_text_renders_short_addresses():
    bot = _bot()
    bot.notifier.add_watch("0x1234567890abcdef")
    bot.notifier.add_watch("0xfedcba0987654321")
    txt = bot._watchlist_text()
    assert "0x1234567890abcdef" in txt
    assert "0xfedcba0987654321" in txt


def test_top_text_handles_empty_bus():
    txt = _bot()._top_text([])
    assert "no scored wallets" in txt


def test_coin_text_handles_unknown():
    txt = _bot()._coin_text(["XYZ"])
    assert "no recent activity" in txt


def test_signals_text_handles_no_bursts():
    txt = _bot()._signals_text()
    assert "No consensus" in txt


def test_positions_text_when_polling_disabled():
    bot = _bot()
    txt = bot._positions_text([])
    assert "disabled" in txt


# ── Auto-snap state ───────────────────────────────────────────────────────────
def test_auto_status_default_off():
    txt = _bot()._auto_status_text()
    assert "off" in txt


# ── Inline-keyboard menu builders ─────────────────────────────────────────────
def _kb_buttons(kb: dict) -> list[tuple[str, str]]:
    """Flatten an InlineKeyboardMarkup to [(label, callback_data), ...]."""
    out = []
    for row in kb["inline_keyboard"]:
        for btn in row:
            out.append((btn["text"], btn["callback_data"]))
    return out


def test_main_menu_has_all_top_level_entries():
    bot = _bot()
    text, kb = bot._main_menu()
    cbs = {cb for _, cb in _kb_buttons(kb)}
    for expected in (
        "menu:dash", "act:status", "act:bias", "act:signals",
        "menu:top", "menu:coins", "act:positions", "menu:watch",
        "menu:settings", "menu:auto", "act:help", "menu:main",
    ):
        assert expected in cbs, f"main menu missing {expected}"
    assert "Whale tracker" in text


def test_main_menu_shows_muted_state():
    bot = _bot()
    bot.notifier.mute()
    text, _ = bot._main_menu()
    assert "muted" in text.lower()


def test_settings_menu_shows_current_filter():
    bot = _bot()
    text, kb = bot._settings_menu()
    assert "$400" in text
    assert "61%" in text
    cbs = {cb for _, cb in _kb_buttons(kb)}
    # Mute toggle resolves to the right action depending on state.
    assert "act:mute" in cbs
    bot.notifier.mute()
    _, kb2 = bot._settings_menu()
    cbs2 = {cb for _, cb in _kb_buttons(kb2)}
    assert "act:unmute" in cbs2


def test_threshold_menu_includes_custom_and_presets():
    _, kb = _bot()._threshold_menu()
    cbs = {cb for _, cb in _kb_buttons(kb)}
    assert "set:thresh:100000" in cbs
    assert "set:thresh:1000000" in cbs
    assert "ask:thresh" in cbs
    assert "menu:settings" in cbs  # back button


def test_watch_menu_renders_remove_buttons():
    bot = _bot()
    bot.notifier.add_watch("0x1111111111111111")
    bot.notifier.add_watch("0x2222222222222222")
    text, kb = bot._watch_menu()
    assert "0x1111111111111111" in text
    cbs = {cb for _, cb in _kb_buttons(kb)}
    assert "set:unwatch:0" in cbs
    assert "set:unwatch:1" in cbs
    assert "ask:watch" in cbs


def test_coins_menu_falls_back_when_empty():
    text, kb = _bot()._coins_menu()
    cbs = {cb for _, cb in _kb_buttons(kb)}
    assert "set:coin:BTC" in cbs


def test_auto_menu_includes_all_intervals():
    _, kb = _bot()._auto_menu()
    cbs = {cb for _, cb in _kb_buttons(kb)}
    for v in ("set:auto:1", "set:auto:5", "set:auto:15", "set:auto:30",
              "set:auto:60", "set:auto:off", "set:auto:full"):
        assert v in cbs, f"auto menu missing {v}"


# ── Callback dispatch ─────────────────────────────────────────────────────────
async def _capture_dispatch(bot: TelegramCommandBot, data: str):
    """Patch the network methods so we can introspect what would be sent."""
    sent_text: list[tuple[int, str]] = []
    edits: list[tuple[int, int, str]] = []

    async def fake_send(chat_id, text, reply_markup=None):
        sent_text.append((chat_id, text))
        return 99
    async def fake_edit(chat_id, mid, text, reply_markup=None):
        edits.append((chat_id, mid, text))
        return True
    bot._send_text = fake_send
    bot._edit_message = fake_edit
    # Don't actually fire chat actions during tests.
    bot._fire_chat_action = lambda *_a, **_kw: None
    await bot._dispatch_callback(data, chat_id=1, message_id=42)
    return sent_text, edits


def test_callback_menu_navigation_edits_in_place():
    import asyncio
    bot = _bot()
    sent, edits = asyncio.run(_capture_dispatch(bot, "menu:settings"))
    assert sent == [] and len(edits) == 1
    assert "Settings" in edits[0][2]


def test_callback_set_threshold_applies_value():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "set:thresh:250000"))
    assert bot.notifier.filter_settings()["min_notional_usd"] == 250000


def test_callback_set_winrate_handles_percentage_form():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "set:wr:55"))
    assert bot.notifier.filter_settings()["min_winrate"] == 0.55


def test_callback_set_tier_any_clears_filter():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "set:tier:any"))
    assert bot.notifier.filter_settings()["allowed_tiers"] is None


def test_callback_set_tier_combo():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "set:tier:ELITE,TRUSTED"))
    assert bot.notifier.filter_settings()["allowed_tiers"] == ["ELITE", "TRUSTED"]


def test_callback_set_unwatch_removes_by_index():
    import asyncio
    bot = _bot()
    bot.notifier.add_watch("0xaaaa")
    bot.notifier.add_watch("0xbbbb")
    asyncio.run(_capture_dispatch(bot, "set:unwatch:0"))
    remaining = bot.notifier.list_watch()
    assert len(remaining) == 1


def test_callback_act_mute_unmute_toggle():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "act:mute"))
    assert bot.notifier.muted is True
    asyncio.run(_capture_dispatch(bot, "act:unmute"))
    assert bot.notifier.muted is False


def test_callback_set_auto_off_clears_state():
    import asyncio
    bot = _bot()
    bot._auto.enabled = True
    bot._auto.interval_sec = 300
    bot._auto.target_chats.add(1)
    asyncio.run(_capture_dispatch(bot, "set:auto:off"))
    assert bot._auto.enabled is False
    assert bot._auto.target_chats == set()


def test_callback_set_auto_5_enables():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "set:auto:5"))
    assert bot._auto.enabled is True
    assert bot._auto.interval_sec == 300


def test_callback_ask_sets_awaiting_state():
    import asyncio
    bot = _bot()
    asyncio.run(_capture_dispatch(bot, "ask:thresh"))
    assert 1 in bot._awaiting
    assert bot._awaiting[1][0] == "thresh"


def test_consume_input_round_trip():
    import asyncio
    bot = _bot()
    sent: list[str] = []
    async def fake_send(chat_id, text, reply_markup=None):
        sent.append(text); return 99
    bot._send_text = fake_send
    asyncio.run(bot._consume_input("thresh", "$1.2M", chat_id=1))
    assert bot.notifier.filter_settings()["min_notional_usd"] == 1_200_000
    assert any("$1.20M" in t or "$1.2M" in t for t in sent)


def test_unknown_callback_data_doesnt_crash():
    import asyncio
    bot = _bot()
    sent, edits = asyncio.run(_capture_dispatch(bot, "menu:doesnotexist"))
    # No throw. No edit. (Falls through silently.)
    assert sent == [] and edits == []


# ── Run all ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    fns = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
    print(f"telegram bot tests OK ({len(fns)})")
else:
    # Allow `python tests/test_telegram_bot.py` to run automatically.
    fns = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
    print(f"telegram bot tests OK ({len(fns)})")
