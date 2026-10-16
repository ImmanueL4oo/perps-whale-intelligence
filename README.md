# Perps Whale Intelligence

Real-time perpetuals whale tracking and wallet analytics across Hyperliquid, Jupiter, and Drift, with Solana exchange flows, a live dashboard, and Telegram alerts.

Real-time bot that watches **Hyperliquid**, **Jupiter Perps (Solana)**, and **Drift Protocol (Solana)** and pings you whenever a position opens, closes, or gets liquidated above a configurable USD notional (default **$5M**).

Output goes to a pretty terminal feed and (optionally) to a Telegram chat.

```
15:48:21  [Hyperliquid]  OPEN  LONG  BTC  size=82.43  px=$60,532.10  notional=$4.99M  trader=0x9a3f…d12c  lev=10.0x
     🏆 ELITE   312/487W (64%)  PnL all-time $14.20M  30d $2.40M  best $850.0K  worst -$200.0K  active 184d
15:48:44  [JupiterPerps] CLOSE LONG  SOL  size=42,118.93  px=$203.51  notional=$8.57M  trader=AbCd…1234  pnl=+$612.4K
     ✅ TRUSTED  18/26W (69%)  PnL all-time $1.80M  30d $420.0K  (scanned 50/100 recent sigs)
15:49:02  [Hyperliquid]  LIQ   SHORT ETH  size=1,250.00  px=$3,401.22  notional=$4.25M  trader=0x55ee…fa01
     ⚠️ POOR   8/41W (20%)  PnL all-time -$1.20M  30d -$340.0K
```

## Quick start

```bash
cd perps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env to set WHALE_THRESHOLD_USD and (optionally) Telegram creds
python bot.py
# open the dashboard:
open http://localhost:8787
```

Lower the threshold while you're testing so you actually see events:

```bash
python bot.py --threshold 100000
```

Press **Ctrl+C** once to stop. The bot prints `shutting down…`, drains its
WebSockets, and exits within a few seconds. If a connection is hung, a hard
3-second timeout exits anyway. A second Ctrl+C force-quits immediately.

### `./run.sh` — supervised wrapper

For day-to-day use there's a wrapper script that handles venv setup,
dependency refresh, log rotation, auto-restart on crash, and clean shutdown.
It also refuses to launch a second instance so you don't accidentally collide
on the dashboard port or Mongo collection.

```bash
./run.sh                        # foreground, supervised, auto-restart on crash
./run.sh --threshold 100000     # any flag is passed straight through to bot.py
./run.sh --bg --threshold 1e5   # detached background — survives terminal close
./run.sh status                 # is it running? show pid + uptime
./run.sh tail                   # tail -F logs/bot.log
./run.sh logs                   # less +G logs/bot.log
./run.sh stop                   # graceful shutdown (drains WebSockets, ~1s)
```

Background mode writes its PID to `.run.pid` and its output to
`logs/bot.log`. The log is rotated automatically when it crosses 50 MB.
Crashes (any non-zero, non-SIGINT exit) trigger a restart with exponential
backoff capped at 30 s. A clean `./run.sh stop` (or a `Ctrl+C` in foreground
mode) exits without restarting.

## Configuration (`.env`)

| key | default | notes |
| --- | --- | --- |
| `WHALE_THRESHOLD_USD` | `5000000` | USD notional that qualifies as a "whale" |
| `TELEGRAM_BOT_TOKEN` | — | get from [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | — | start the bot, send it any message, then look at `https://api.telegram.org/bot<TOKEN>/getUpdates` |
| `HL_ENABLED` | `true` | toggle Hyperliquid tracker |
| `HL_COINS` | `BTC,ETH,SOL,HYPE,XRP,DOGE,SUI,kPEPE` | which markets to subscribe to |
| `JUP_ENABLED` | `true` | toggle Jupiter Perps tracker |
| `JUP_COINS` | `SOL,ETH,BTC` | subset of Jupiter's three available perp custodies |
| `JUP_ENRICH_INSTANT` | `true` | decode Jupiter's `Instant*` instructions via `getTransaction` (without this you'd miss most of Jupiter's flow — see "Jupiter Instant pathway" below) |
| `JUP_ENRICH_CONCURRENCY` | `4` | max in-flight `getTransaction` calls. Public RPC tolerates 4–8; private RPC can go much higher |
| `SOLANA_RPC_HTTP` | `https://api.mainnet-beta.solana.com` | RPC endpoint for `getTransaction` enrichment. Public works but rate-limits hard — Helius / Triton / QuickNode recommended for production |
| `SOLANA_RPC_WS` | `wss://api.mainnet-beta.solana.com` | RPC websocket for `logsSubscribe`. Same caveat |
| `DRIFT_ENABLED` | `true` | toggle Drift Protocol tracker (Solana's largest perps DEX) |
| `DRIFT_MARKETS` | *empty → curated default (SOL/BTC/ETH/HYPE/DOGE/SUI/PEPE/TRUMP)* | comma-separated Drift symbols to poll |
| `DRIFT_COINS` | *empty → no extra filter* | only emit events for these base tickers |
| `DRIFT_MAX_RPS` | `1.0` | global request budget for Drift's data API. **1 RPS is the empirical safe ceiling** — going hotter (3+ RPS) gets the IP shadow-banned with HTTP 403 for 5+ minutes. With a private proxy raise to 5–10 |
| `DRIFT_POLL_SEC` | `10.0` | per-market refresh hint (actual cadence is gated by `DRIFT_MAX_RPS`) |
| `BOT_TPD_THRESHOLD` | `30` | trades-per-day at or above this → wallet labelled BOT |
| `HYBRID_TPD_THRESHOLD` | `5` | trades-per-day at or above this → HYBRID; below → HUMAN |
| `ACTOR_MIN_SAMPLES` | `10` | min closed trades before assigning a non-`UNKNOWN` actor label |
| `WEB_ENABLED` | `true` | toggle the bundled dashboard |
| `WEB_HOST` | `127.0.0.1` | bind interface; set to `0.0.0.0` to expose on your LAN |
| `WEB_PORT` | `8787` | dashboard port |
| `MONGO_ENABLED` | `true` | toggle MongoDB persistence (the bot keeps running even if Mongo is down) |
| `MONGO_URI` | `mongodb://localhost:27017` | any standard Mongo connection string |
| `MONGO_DB` | `perps_whales` | database name |
| `MONGO_COLLECTION` | `events` | collection name |

## What each tracker does

### Hyperliquid

- Subscribes to the public `trades` WebSocket feed (one channel per coin).
- For every fill where `price * size ≥ threshold`:
  - Calls `clearinghouseState` for the taker to compare its post-fill position against the trade size — that's how `OPEN` vs `CLOSE` is classified.
  - Calls `userFills` to check the `liquidation` flag on the matching trade id; if set, the event is reclassified as `LIQ`.
- Sends an app-level `ping` every 20 s and reconnects with exponential backoff if the socket drops.

### Web dashboard

A self-contained dashboard runs in the same process at `http://localhost:8787` (configurable). Single static HTML file, vanilla JS, Chart.js from CDN — no framework, no build step.

What it shows:

- **Stat cards** — running totals: whale count, total notional, long/short bias, liquidations.
- **Notional volume chart** — last 60 minutes, stacked by venue, 5-minute bins.
- **Trader trust mix** — donut chart of how many alerts came from each tier (ELITE / TRUSTED / MIXED / POOR / NEW).
- **Top traders leaderboard** — every wallet seen this session, ranked by composite score (0–100), with venue, tier, win-rate, PnL, and how many alerts they've fired. Click any row for the full history modal.
- **Live whale feed** — color-coded rows with venue, kind (OPEN/CLOSE/LIQ), side, coin, size, price, notional, PnL, leverage, score badge, tier, win-rate, and the trader's clickable address.
- **Wallet detail modal** — click any address (in the feed or leaderboard) to open a full-screen drilldown:
  - Score gauge + tier + link to the block explorer
  - 9-card stat grid: PnL all-time / 30d / 7d, closed trades, win rate, profit factor, avg win, avg loss, best trade, worst trade, days active, fees paid
  - Equity curve line chart (cumulative realized PnL over their tracked trades)
  - Per-coin horizontal bar chart of realized PnL
  - Per-coin breakdown table with trade count + win rate + PnL
  - Recent closed trades table (last 100, with timestamp, coin, side, size, price, PnL, fee)
- **Filters** — quick toggles by venue, kind, and side.
- **Live indicator** — pulsing green dot when SSE is connected.

How the score is computed (0–100, higher = more trustworthy to copy):

| Component | Range | What it measures |
| --- | --- | --- |
| Win rate | 0…35 | linear from 35% wr → 0 to 70% wr → 35 |
| Realized PnL | −30…+35 | log-scaled. $1K → 0, $10K → ~9, $1M → ~26, $10M → 35. Negative PnL gets a symmetric penalty up to −30 |
| Sample size | 0…15 | log of closed trades — 10 → 5, 100 → 10, 1000 → 15 |
| Profit factor | 0…15 | gross wins / gross losses. PF=1 → 0, PF=4 → 15 |

Wallets with fewer than 5 closed trades return `score = null` (not enough sample) and don't appear in the leaderboard.

How it's served:

| Route | Purpose |
| --- | --- |
| `GET /` | the static HTML |
| `GET /api/config` | bot config (threshold, venues) for the header |
| `GET /api/recent` | the recent ring buffer (initial page load) |
| `GET /api/stats` | live aggregate counts/totals |
| `GET /api/leaderboard?limit=N` | top traders this session, sorted by score |
| `GET /api/wallet/<venue>/<address>` | full track record for a wallet (recent trades, by-coin, score, all stats) |
| `GET /events` | Server-Sent Events stream — every whale event as it fires, plus a 15 s heartbeat so reverse proxies don't close the socket |

The page dedupes against `(venue, tx_ref, trader, coin, kind)` so reconnecting an SSE stream after a network blip never double-counts events.

### Wallet track record

Every alert is enriched with a per-wallet historical track record so you can decide at a glance whether to fade or follow:

- **Hyperliquid**: one `userFills` call (last 2000 fills) → realized PnL, win rate, 30d PnL, best/worst trade, days active.
- **Jupiter Perps**: scans the wallet's last 100 Solana signatures and parses the same Anchor events the live tracker reads → same stats, scoped to whatever fits in the scan window. Public RPC will sometimes time out; a paid RPC eliminates that.

Each result is bucketed into a tier:

| Tier | Criteria |
| --- | --- |
| 🏆 ELITE | ≥20 closed trades, ≥60% win rate, ≥$1M realized PnL |
| ✅ TRUSTED | ≥10 closed trades, ≥55% win rate, positive PnL |
| ⚖️ MIXED | enough sample, neither great nor bad |
| ⚠️ POOR | <40% win rate **or** realized PnL ≤ −$100K |
| ❓ NEW | fewer than 5 closed trades — too small a sample to judge |

Results are cached for 5 minutes per wallet and de-duplicated when the same whale fires multiple alerts in rapid succession (single-flight). Enrichment is bounded by a hard timeout (4 s for HL, 8 s for Jupiter) so the alert pipeline never stalls — if the lookup is slow you still get the alert without the track record line.

### Jupiter Perps

- Subscribes to Solana RPC `logsSubscribe` filtered to the Jupiter Perpetuals program (`PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu`).
- Decodes the three Anchor events that matter for whale flow directly from `Program data:` lines:
  - `IncreasePositionEvent` → `OPEN`
  - `DecreasePositionEvent` → `CLOSE` (with realized PnL)
  - `LiquidateFullPositionEvent` → `LIQ` (with realized PnL)
- The decoder is a small hand-rolled Borsh reader baked against the published IDL — no on-chain IDL fetch and no `anchorpy` runtime requirement.
- Filters on `sizeUsdDelta` (or `positionSizeUsd` for liquidations) ≥ threshold. USD values use 6 atomic decimals.
- The position-asset coin (SOL / ETH / BTC) is resolved from the `positionCustody` pubkey; if Jupiter ever adds new custodies, just append them to `CUSTODY_TO_COIN` in `trackers/jupiter_perps.py`.
- The tracker prints a one-line heartbeat every 60 s with `tx notifications / events decoded / events kept`, so you can tell at a glance whether silence means "no whale activity" vs. "RPC connection died" vs. "subscription never acked".

**Jupiter "Instant" pathway** (added Apr 2025 — most retail flow now uses
this).  `InstantIncreasePosition` / `InstantDecreasePosition` (and the
`…PreSwap` / `…_v2` variants) execute atomically without going through the
keeper bots, so they **do not emit** `Program data:` Anchor events. The bot
covers them via a second pass:

1. The live `logsSubscribe` stream flags any tx whose log contains
   `Instruction: Instant*`.
2. A worker pool (`JUP_ENRICH_CONCURRENCY` in flight) calls
   `getTransaction` on the signature, decodes the Jupiter Perps instruction
   data (8-byte Anchor discriminator + `sizeUsdDelta: u64`), resolves
   the position custody by matching the instruction's account list against
   the well-known custody map, and infers side from the presence of a stable
   collateral custody (USDC/USDT → short, otherwise long).
3. The owner is the first signer of the tx.
4. If `sizeUsdDelta ≥ threshold` and the coin passes the filter, the bot
   emits a normal `WhaleEvent` with `extra.instant_kind` and
   `extra.side_inferred = True` so the dashboard can mark it as
   best-effort-classified.

The heartbeat distinguishes detected vs enriched vs emitted Instant txs:

```
heartbeat (60s): 23 tx notifications, 0 events decoded, 4 above $150K
                 threshold; 6 Instant* tx (6 enriched, 1 above threshold)
```

Set `JUP_ENRICH_INSTANT=false` to fall back to the legacy log-only path.

### Drift Protocol (Solana's largest perps DEX)

- Polls the public Drift Data API (`https://data.api.drift.trade/`) for each
  configured market — no WebSocket, no SDK, no auth required.
- One request per market per cycle returns the most recent fills with
  `taker`, `maker`, `quoteAssetAmountFilled`, `oraclePrice`, and an
  `actionExplanation` distinguishing fills from liquidations.
- A monotonic `fillRecordId` cursor avoids re-emitting fills across polls.
- Whale filter is applied to `quoteAssetAmountFilled`; the **taker** is
  treated as the trader (active flow side). For liquidation actions the side
  is **flipped** so we attribute the position to the liquidated trader, not
  the bot doing the liquidating.

> **Rate limits matter.** Drift's data API is unauthenticated and shared by
> their UI. **1 RPS is the empirical safe ceiling per IP** — going hotter
> (3+ RPS, which is what you'd get with a 3-second poll across ~15 markets)
> gets you shadow-banned with HTTP 403 for 5+ minutes. The tracker enforces
> the budget with a token-bucket limiter (`DRIFT_MAX_RPS`, default 1.0) and
> enters a global cooldown (60 s, doubling on repeat) on any 403/429.
> If you have a private proxy or run from a clean IP, raise `DRIFT_MAX_RPS`
> to 5–10 for sub-second whale latency.

Symbol mapping: Drift uses `1MPEPE-PERP` / `1KBONK-PERP` for size-scaled
markets. The tracker strips the `1M` / `1K` prefix so the coin label aligns
with the rest of the bot's analytics (`PEPE`, `BONK`).

## Telegram setup

1. DM [@BotFather](https://t.me/BotFather), `/newbot`, copy the token.
2. Send your new bot any message.
3. Open `https://api.telegram.org/bot<TOKEN>/getUpdates` and copy the numeric `chat.id`.
4. Put both into `.env`. Restart the bot.

If the bot can't reach Telegram (e.g. 429), it will back off and retry — the console feed keeps running regardless.

### Telegram signal filter

The console keeps showing every whale, but you almost never want every alert
on your phone. The Telegram side has its own filter — three rules, all AND-ed:

| `.env` key | default | rule |
| --- | --- | --- |
| `TG_MIN_NOTIONAL_USD` | `400000` | only push events ≥ this notional |
| `TG_MIN_WINRATE` | `0.61` | only push if the trader's win rate ≥ this |
| `TG_ALLOWED_TIERS` | `TRUSTED,ELITE` | only push if the trader's tier is in this set |

If a tier or win-rate rule is set but the alert has no track record (e.g.
Jupiter wallet with no recent activity, or the lookup timed out), the push
is skipped — better silent than misleading.

Set any of these to `0` / empty to disable that rule. The startup banner
prints the active filter so you can see at a glance what gets through:

```
telegram: on
telegram filter: ≥ $400.0K   ≥ 61% wr   tier in ELITE/TRUSTED
```

### Trading-signal layer

On top of the per-event filter, four extra Telegram features turn the
firehose into something you can actually trade with:

1. **Wallet watchlist** (`TG_WATCH_WALLETS=0xabc,0xdef…`) — these wallets
   bypass all filters. Their alerts are prefixed `⭐ WATCHED`. Use it for
   the specific traders you've decided to follow.
2. **🔥 Conviction-burst alerts** — when `CONVICTION_MIN_TRADERS` distinct
   ≥TRUSTED wallets open the same side on the same coin inside
   `CONVICTION_WINDOW_MIN` minutes, the bot pushes a single summary message
   listing every trader, their score, and total notional. After firing for
   a (coin, side), it waits `CONVICTION_COOLDOWN_MIN` minutes before
   re-alerting on the same pair so you don't get spammed when traders
   trickle in over hours.
3. **🏁 EXIT alerts** (`LIFECYCLE_ALERTS_ENABLED=true`) — when a TRUSTED+
   wallet closes a position the bot previously alerted on, you get a second
   push with realized PnL, hold time, entry/exit prices. Lets you mirror
   the exit if you copy-traded the entry.
4. **Smart-money positioning** — a dashboard widget + `/api/smart-money`
   endpoint showing per-coin net long/short bias among ≥TRUSTED traders
   over the last 4 hours. Answers "where are the pros positioned right
   now?" — the single most actionable view in the system. Bias of
   `+50% LONG` means trusted notional is 75% long / 25% short on that coin.

Conviction history is replayed on startup so a restart doesn't immediately
re-alert on yesterday's clusters; the lifecycle tracker also replays the
seeded history so OPENs from before the restart still fire EXIT alerts when
they eventually close.

### Interactive Telegram bot — `/snap`, `/auto`, `/status`, …

On top of pushing alerts, the bot **listens for commands** from your
Telegram chat. The same token used for alerts also long-polls
`getUpdates`, so your phone doubles as a remote control for the tracker.

Set up:

```env
TG_COMMAND_BOT_ENABLED=true
TELEGRAM_AUTHORIZED_CHATS=          # defaults to TELEGRAM_CHAT_ID; comma-separated for more
TG_SNAPSHOT_ENABLED=true            # /snap captures the dashboard via headless Chromium
TG_DASHBOARD_URL=http://127.0.0.1:8787/
TG_AUTO_SNAP_MIN=0                  # >0 = push a snapshot every N min on startup
```

For `/snap` to work, install Chromium once (the rest of the bot works
without it — the command will just report "snapshot unavailable"):

```bash
pip install playwright
playwright install chromium      # ~90 MB one-time download
```

Authorization is strict by default: only chat IDs in
`TELEGRAM_AUTHORIZED_CHATS` (or, if unset, `TELEGRAM_CHAT_ID`) can issue
commands. Everyone else is silently ignored — copy-paste into
`@BotFather` doesn't accidentally hand control to strangers.

#### Commands

| Command | What it does |
| --- | --- |
| `/help` | List every command. |
| `/snap` | Send a PNG of the dashboard (viewport, ~2 s). |
| `/snap full` | Full-page (scrolling) capture — every card in one tall PNG. |
| `/auto on 5` | Schedule a snapshot every 5 minutes. |
| `/auto on 5 full` | Same, but full-page. |
| `/auto off` / `/auto status` | Stop / inspect the schedule. |
| `/status` | Buffered events, last whale, throughput, poller state, filter knobs. |
| `/bias` | Composite LONG/SHORT/NEUTRAL signal among ≥TRUSTED traders + per-coin breakdown. |
| `/top [N]` | Top N traders by score this session (default 10). |
| `/coin BTC` | Flow / smart flow / volume / liqs across 15 m, 1 h, 24 h. |
| `/positions [min_usd]` | Live polled positions, top 10 by notional. |
| `/signals` | Active consensus bursts in the last 30 minutes. |
| `/watch 0xabc…` | Add a wallet to the watchlist (bypasses all filters). |
| `/unwatch 0xabc…` | Remove. |
| `/watchlist` | List watched wallets. |
| `/mute` / `/unmute` | Pause / resume **push** alerts (commands keep working). |
| `/threshold 250000` | Change the min-notional rule live, no restart. |
| `/winrate 0.55` (or `55`) | Change the min-winrate rule. |
| `/tier ELITE,TRUSTED` (or `any`) | Change the allowed-tiers rule. |

Mutating commands edit the live notifier state — no `.env` reload, no
process restart, no race condition. The startup banner prints the
default filter; `/status` always shows the current values.

A scheduled `/auto on 5` survives across `/snap` / `/status` calls but
is reset on a bot restart (it doesn't persist to disk; bake it into
`TG_AUTO_SNAP_MIN` if you want it always on).

### Active position polling

The trade-tape side of the bot is event-driven — it only sees movements
that fire ≥ `WHALE_THRESHOLD_USD`. The active poller fills in the gaps:

- Every `POLL_INTERVAL_SEC` (default 30 s), it calls Hyperliquid's
  `clearinghouseState` for the union of `TG_WATCH_WALLETS` ∪ `TRUSTED+
  wallets seen in events in the last 24 h`, capped at `POLL_MAX_WALLETS`.
- It snapshots every wallet's full open book (size, entry, mark,
  unrealized PnL, leverage, liquidation price, margin used).
- It diffs successive snapshots and publishes a `POSITION_UPDATE` event
  on the bus for each material change (`open` / `close` / `increase` /
  `decrease` / `flip`), filtering out jitter below `POLL_MIN_CHANGE_USD`.

What you get that the event stream alone can't deliver:

- Positions opened **before** you started the bot.
- Sub-threshold scale-ins (e.g. an ELITE wallet building a position with
  $40 K trades that never trip the $400 K whale filter — the poller sees
  the cumulative delta).
- The wallet's actual unrealized PnL and liquidation price right now.
- A "this wallet went FLAT" signal even when the closing trade itself
  was sub-threshold.

Surfaced in two places on the dashboard:

- The **Live positions** panel — a sortable table of every open lot
  across the polled wallet set, with mark, uPnL, leverage, liq price,
  and the age of the snapshot.
- The **Live whale feed** under a new `🔄 Poll` filter chip — every
  detected diff streams in as a `POSITION_UPDATE` event you can mix
  alongside the trade tape.

API: `GET /api/live-positions` returns `rows` (current open positions),
`wallets` (last-fetch metadata), and `changes` (the most recent diffs).
Set `POLL_ENABLED=false` in `.env` to disable.

### Performance & robustness

A few things make the dashboard feel snappy even under burst tape:

- **Single-pass aggregator on the client.** All status-bar windows
  (1m/5m/15m/1h), the bias-card score, the crowd radar buckets, and
  the all-time totals share **one walk** through the recent-events
  array per refresh, cached for 250 ms. Was ~6 separate scans/sec
  before; now CPU usage stays flat as the buffer grows.
- **Page-visibility-aware tick.** The 1 Hz tape tick and the SSE light
  refresh path both bail out when `document.hidden` — keeps long
  sessions from chewing CPU/battery in background tabs and gives an
  immediate repaint on tab focus.
- **Render error boundaries.** Every render hook runs through a
  `safe()` wrapper that catches and logs (once) so a single malformed
  event can't cascade into a dead dashboard.
- **Stale-tape banner.** The dashboard shows a yellow → red warning
  if SSE has been silent for too long (>90s warns; >5m severe) or the
  stream disconnects, so you never silently stare at a frozen tape.
- **Burst-aware HL classify cache.** When a wallet slices one big
  order into a flurry of sub-prints in the same second, the
  Hyperliquid tracker now reuses the last `clearinghouseState` for
  ~1.5s — same OPEN/CLOSE classification, ~10× fewer info calls,
  no more rate-limit grief during whale bursts.
- **`/api/health` endpoint.** Cheap (sub-ms) JSON for monitoring or
  curl-debugging:

  ```json
  {
    "ok": true,
    "uptime_s": 132.4,
    "events_in_buffer": 500,
    "last_event_age_s": 4.2,
    "by_venue": {"Hyperliquid": {"n_5m": 12, "n_15m": 47, "last_age_s": 4.2}},
    "poller": {"enabled": true, "snapshots": 21, "max_wallets": 40},
    "mongo_available": true
  }
  ```

## Symbol selection

Both venues let you pick exactly which symbols to track.

```bash
# Edit .env to change the persistent default:
HL_COINS=BTC,ETH,SOL,HYPE,XRP,DOGE,SUI,kPEPE
JUP_COINS=SOL,ETH,BTC

# Or override both at once for a single run:
python bot.py --coins BTC,ETH,SOL
```

`--coins` is the union override — Hyperliquid subscribes to the listed tickers, Jupiter emits only events whose custody resolves to one of them (intersected with its three available custodies: SOL / ETH / BTC). The dashboard header shows the active symbol set, and `/api/config` returns it as a JSON list.

## Bot vs human classification

Every alert is tagged with an **actor label** derived from the wallet's closed-trade history:

```
trades_per_day = closed_trades / days_active

≥ 30  → BOT     🤖   high-frequency / scalping / market-making cadence
≥ 5   → HYBRID  ⚙️   active manual trader, or supervised bot
<  5  → HUMAN   🧑   typical discretionary cadence
< 10 closed trades  → UNKNOWN  ❔   not enough data
```

Thresholds are tunable via `BOT_TPD_THRESHOLD` / `HYBRID_TPD_THRESHOLD` / `ACTOR_MIN_SAMPLES`.

What you can do with the label:

- **Filter the live feed** by clicking the `🧑 Human / ⚙️ Hybrid / 🤖 Bot / ❔ Unknown` chips. Multi-select.
- **Hide bots from analytics** with the toggle above the signals banner — applies to consensus signals and smart positions simultaneously. ON by default because most "5 wallets piled into BTC" signals are MM clusters when you don't filter.
- **Query directly** with `?actor=HUMAN,HYBRID` on `/api/signals`, `/api/positions`, `/api/coin/<coin>`. The label also rides along inside every event's `track_record` field on `/api/recent`, `/api/history`, and the SSE stream.

Why this matters: a "consensus signal" of 5 BOT wallets going long is rarely tradable — it's market-making rebalances, not directional conviction. Filtering bots out leaves you with the smaller, higher-signal cluster of humans actually expressing a view.

## Trade signals & analytics

The dashboard now derives actionable trade signals from the raw whale stream. All analytics are pure functions in `analytics.py` and exposed via REST so you can also pipe them into spreadsheets, Telegram alerts, or your own scripts.

### Concepts

- **Signed flow.** Direction-aware buy/sell pressure for each event:

  | Event | Pressure |
  | --- | --- |
  | OPEN long | + (buy) |
  | OPEN short | − (sell) |
  | CLOSE long | − (sell) |
  | CLOSE short | + (buy) |
  | LIQ long | − (forced sell) |
  | LIQ short | + (forced buy) |

  Sum it across a window to see who's absorbing whom.

- **Smart flow.** Same as flow, but each event is weighted by `score / 100` (wallets without a score are excluded). This biases toward what historically-profitable traders are doing.

- **Consensus signal.** The most actionable copy-trade primitive: ≥ 2 distinct **TRUSTED+** wallets going the *same direction* on the *same coin* inside a 30-minute sliding window. Stronger clusters (more traders, higher avg score, more notional) rank first.

- **Smart positions.** Best-effort inference of which positions a high-score wallet is *currently* holding, by walking OPEN/CLOSE/LIQ events. Not exact (we only see ≥-threshold events), but directionally useful for mirroring.

### What the dashboard now shows

At the top: a **signals banner** — one card per consensus cluster, showing direction, coin, total notional, average score, and which wallets are in the cluster. Click a card to drill into the coin.

Below that: a **coin scoreboard** — one row per coin with 15m flow / 1h flow / 1h smart flow / 1h volume / 24h flow / 24h volume / 1h liquidations. Sorted by 1h volume so the hottest markets surface first.

Then a **smart positions** table — for every wallet with `score ≥ 60` (adjustable), the best-effort current position: side, coin, notional, leverage, last-seen time. Click a coin to drill in, click a trader to see their full track record.

Click any coin (in scoreboard, signals card, or smart positions) → opens a **per-coin modal** with:

- 9-card stat grid (1h flow / smart flow / volume / opens / closes / liqs, 24h flow / volume, last price)
- 6-hour net flow chart (5-minute bars + smart-flow line overlay)
- Top traders on this coin with bias / score / trades / long-vs-short open notional
- Recent events on this coin

### REST endpoints

```bash
# Consensus + scoreboard in one call
curl 'http://localhost:8787/api/signals'
curl 'http://localhost:8787/api/signals?window_ms=900000&min_traders=3&min_tier_rank=4'

# Just the coin scoreboard
curl 'http://localhost:8787/api/coins'

# Per-coin deep view
curl 'http://localhost:8787/api/coin/BTC'
curl 'http://localhost:8787/api/coin/SOL?window_ms=21600000&bin_ms=300000'

# Best-effort open positions across high-score wallets
curl 'http://localhost:8787/api/positions?min_score=70&max_age_ms=86400000'
```

`min_tier_rank` mapping: `0=NEW`, `1=POOR`, `2=MIXED`, `3=TRUSTED`, `4=ELITE`. Default for consensus signals is `3`.

### Reading the numbers (quick guide)

- **Smart flow strongly negative** while raw flow is mildly negative → the *quality* wallets are selling harder than the average; treat as bearish even if total flow looks neutral.
- **Consensus longs by ≥ 3 ELITE wallets within 15 min** → the strongest copy-trade signal you can extract; size accordingly and set a hard stop.
- **High 1h liq count + flat smart flow** → forced unwinds without smart-money participation; often marks local capitulation.
- **One wallet appears in smart positions across many coins with the same side** → likely systematic, not idiosyncratic; less informative for any single ticker.

## MongoDB persistence

When `MONGO_ENABLED=true` (default), every whale event is written to MongoDB. The bot keeps running cleanly if Mongo is unreachable — it just logs a warning and runs in-memory only.

**Start a local Mongo** (one of):

```bash
docker run -d --name mongodb -p 27017:27017 -v mongo-data:/data/db mongo:7
brew install mongodb-community && brew services start mongodb-community
```

**Schema.** Each event is stored as one document keyed by `_id = "{venue}|{tx_ref}|{trader}|{coin}|{kind}"`, which makes inserts idempotent — on restart, any event the bot already wrote is silently deduped. Indexes are created on first connect: `ts_ms` desc, `(trader, venue, ts_ms)`, `(venue, ts_ms)`, `(kind, ts_ms)`.

**On startup**, the bot loads the last 500 events from Mongo into the dashboard's replay buffer, so opening the page immediately shows recent history instead of waiting for fresh whales.

**Query the API directly** (JSON, paginated, all filters combinable):

```bash
curl 'http://localhost:8787/api/history?limit=50'
curl 'http://localhost:8787/api/history?venue=Hyperliquid&kind=LIQ&min_notional=1000000'
curl 'http://localhost:8787/api/history?coin=BTC&min_score=70'
curl 'http://localhost:8787/api/history?trader=0xabc&before_ts=1700000000000'
curl 'http://localhost:8787/api/wallet/Hyperliquid/0xabc.../events?limit=200'
```

Filter params: `venue`, `kind` (`OPEN`/`CLOSE`/`LIQ`), `side` (`long`/`short`), `coin`, `tier` (`ELITE`/`TRUSTED`/`MIXED`/`POOR`/`NEW`), `trader` (case-insensitive substring), `min_notional`, `min_score`, `after_ts` / `before_ts` (ms epoch), `limit` (max 1000). All optional.

**Or query Mongo directly** for ad-hoc analytics:

```js
use perps_whales
db.events.countDocuments()
db.events.find({ venue: "Hyperliquid", kind: "LIQ" }).sort({ ts_ms: -1 }).limit(10)
db.events.aggregate([
  { $match: { kind: "OPEN" } },
  { $group: { _id: "$coin", n: { $sum: 1 }, notional: { $sum: "$notional_usd" } } },
  { $sort: { notional: -1 } }
])
```

## Operational notes

- **Public Solana RPC will not keep up** with the Jupiter Perps program log volume. For production-grade reliability, set `SOLANA_RPC_WS` to a paid provider's WebSocket URL.
- Hyperliquid limits each IP to 1,000 WebSocket subscriptions and 100 connections — the default 8-coin list is well under that.
- Open/Close classification on Hyperliquid is best-effort. The `notional`, `side`, `coin`, `price`, and `size` fields are always derived from the actual fill and are correct.
- Dedup is handled per-tracker in-memory and in Mongo via the deterministic `_id`. Restarting the bot won't double-persist events you've already seen.

## Repo layout

```
perps/
├── bot.py                       # entrypoint
├── config.py                    # env loader
├── notifier.py                  # rich console + Telegram fan-out
├── trackrecord.py               # per-wallet PnL/winrate enrichment + trust tier
├── event_bus.py                 # in-process pub/sub + JSON serialization
├── analytics.py                 # signed flow / consensus / smart positions / time-series
├── storage.py                   # MongoDB persistence (motor)
├── webserver.py                 # aiohttp dashboard + SSE + REST history + analytics
├── static/index.html            # the entire UI
├── trackers/
│   ├── base.py                  # WhaleEvent dataclass
│   ├── hyperliquid.py           # WS trades + classification + liq detection
│   └── jupiter_perps.py         # logsSubscribe + Anchor event decoding
├── requirements.txt
└── .env.example
```
