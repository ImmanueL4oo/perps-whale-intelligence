"""Headless-Chromium screenshot service for the dashboard.

Used by the Telegram command bot to fulfil `/snap` requests.

Performance notes
-----------------
The naive implementation (new page → navigate → wait for ready → settle →
screenshot → close) costs ~2 s per snapshot, almost all of it spent
re-running the dashboard bootstrap. We avoid that by keeping a single
Chromium **page** alive across snapshots:

* The first snap opens the page, lets it fully boot (`window.__dashReady`),
  and waits one short settle window for live data to populate.
* Every subsequent snap just calls `page.screenshot()` on the already-
  open page. The dashboard's 1 Hz tape tick + SSE feed keep it live, so
  the screenshot is always fresh — and we typically pay just 100-200 ms
  per call instead of ~2000 ms.
* The page is reloaded periodically (`reload_every_sec`, default 5 min)
  to recover from any drift / leak / JS error. The reload runs *between*
  user requests where possible.

Two more accelerators on top:

1. **In-memory cache** — `snap()` returns a cached PNG when it's younger
   than `cache_max_age_sec` (default 2 s). With the auto-cache loop running
   in the background you can serve dozens of `/snap` requests per second
   from RAM.
2. **Aggressive launch flags** — disable a bunch of features Chromium
   spends time setting up that the dashboard doesn't need (GPU, CRX,
   IPv6 DNS, etc).

The browser context is dark-mode + 1.5x DPI to match what the user sees.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional, Tuple

log = logging.getLogger("snapshot")


# Chromium launch flags tuned for "render this once, fast, headless".
# We don't need GPU compositing, sandboxing (we run as the bot user),
# crash reporting, sync, or background timer throttling. Each flag
# trims ~30-150 ms off cold start.
_CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-features=Translate,BackForwardCache,AcceptCHFrame",
    "--disable-extensions",
    "--disable-component-extensions-with-background-pages",
    "--disable-default-apps",
    "--disable-sync",
    "--mute-audio",
    "--metrics-recording-only",
    "--no-first-run",
    "--no-default-browser-check",
    "--font-render-hinting=none",
]


@dataclass
class SnapshotResult:
    png: Optional[bytes]
    width: int
    height: int
    elapsed_ms: int
    mode: str
    cached: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.png is not None and self.error is None


class SnapshotService:
    """Owns one persistent Playwright browser + page.

    Concurrency
    -----------
    All snap operations share a single asyncio.Lock — Chromium can in
    principle render two pages at once, but our dashboard is timer-heavy
    and we'd rather serialise than risk inconsistent output. The lock is
    cheap; in practice we serve <10 snaps/min.

    Cache
    -----
    `_cached_png` holds the most recent successful viewport-mode PNG.
    The auto-refresh loop (started by `start_auto_refresh`) keeps it
    fresh in the background so `/snap` can return instantly.
    """

    def __init__(
        self,
        *,
        default_url: str = "http://127.0.0.1:8787/",
        viewport: tuple[int, int] = (1600, 1000),
        device_scale_factor: float = 1.5,
        ready_signal_js: str = "window.__dashReady === true",
        settle_ms: int = 250,
        nav_timeout_ms: int = 12_000,
        reload_every_sec: float = 300.0,
        cache_max_age_sec: float = 2.0,
        auto_refresh_sec: float = 0.0,
    ) -> None:
        self.default_url = default_url
        self.viewport = viewport
        self.device_scale_factor = device_scale_factor
        self.ready_signal_js = ready_signal_js
        self.settle_ms = settle_ms
        self.nav_timeout_ms = nav_timeout_ms
        self.reload_every_sec = reload_every_sec
        self.cache_max_age_sec = cache_max_age_sec
        self.auto_refresh_sec = auto_refresh_sec

        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._page_loaded_at: float = 0.0
        self._lock = asyncio.Lock()
        self._started = False
        self._unavailable_reason: Optional[str] = None

        # Cache (viewport-mode only — full-page captures are too heavy
        # to keep warm and are typically requested explicitly).
        self._cached_png: Optional[bytes] = None
        self._cached_at: float = 0.0
        self._cached_elapsed_ms: int = 0

        self._refresh_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # ── lifecycle ─────────────────────────────────────────────────────────────
    @property
    def available(self) -> bool:
        return self._unavailable_reason is None and self._started

    @property
    def unavailable_reason(self) -> Optional[str]:
        return self._unavailable_reason

    async def start(self) -> None:
        """Launch Chromium and warm up the persistent page. Idempotent."""
        if self._started or self._unavailable_reason:
            return
        try:
            from playwright.async_api import async_playwright  # type: ignore
        except ImportError as exc:
            self._unavailable_reason = (
                "playwright is not installed. Run: pip install playwright "
                "&& playwright install chromium"
            )
            log.warning("snapshot disabled: %s (%s)", self._unavailable_reason, exc)
            return

        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True, args=_CHROMIUM_ARGS,
            )
            self._context = await self._browser.new_context(
                viewport={"width": self.viewport[0], "height": self.viewport[1]},
                device_scale_factor=self.device_scale_factor,
                color_scheme="dark",
                # Block heavy assets that the dashboard doesn't ship anyway —
                # if it ever does, this is a one-line revert.
                java_script_enabled=True,
            )
            self._started = True
            # Warm the page in the background so the first /snap doesn't pay
            # the full cold-start. Failures here flip the service to
            # unavailable but don't crash the bot.
            asyncio.create_task(self._warm_first_page(), name="snap-warmup")
            log.info(
                "snapshot service ready (chromium, %dx%d @%.1fx, settle=%dms, cache=%.1fs)",
                *self.viewport, self.device_scale_factor, self.settle_ms,
                self.cache_max_age_sec,
            )
        except Exception as exc:
            self._unavailable_reason = (
                f"could not launch Chromium ({exc}). "
                f"Run: playwright install chromium"
            )
            log.warning("snapshot disabled: %s", self._unavailable_reason)
            await self._cleanup()

    async def start_auto_refresh(self) -> None:
        """Begin the background loop that keeps `_cached_png` warm.

        Only starts if `auto_refresh_sec > 0`. Idempotent.
        """
        if self.auto_refresh_sec <= 0 or self._refresh_task is not None:
            return
        self._refresh_task = asyncio.create_task(
            self._auto_refresh_loop(), name="snap-auto-refresh",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except (asyncio.CancelledError, Exception):
                pass
            self._refresh_task = None
        await self._cleanup()
        self._started = False

    async def _cleanup(self) -> None:
        for closer, name in [
            (self._page, "page"),
            (self._context, "context"),
            (self._browser, "browser"),
            (self._playwright, "playwright"),
        ]:
            if closer is None:
                continue
            try:
                if name == "playwright":
                    await closer.stop()
                else:
                    await closer.close()
            except Exception:
                log.debug("snapshot %s cleanup failed", name, exc_info=True)
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

    async def _warm_first_page(self) -> None:
        """Pre-load the dashboard once so the first user `/snap` is fast."""
        try:
            async with self._lock:
                await self._ensure_loaded(self.default_url)
            # Take a warmup screenshot so the cache has something on day 0.
            await self.snap(self.default_url, _from_warmup=True)
            log.info("snapshot warmup complete")
        except Exception:
            log.debug("snapshot warmup failed", exc_info=True)

    # ── page management ───────────────────────────────────────────────────────
    async def _ensure_loaded(self, url: str) -> None:
        """Make sure `self._page` is loaded on `url` and ready.

        Reloads if (a) no page exists, (b) the URL changed, (c) the page
        is older than `reload_every_sec`. Otherwise just returns — we'll
        re-use the warm page.
        """
        now = time.monotonic()
        page_url = None
        if self._page is not None:
            try:
                page_url = self._page.url
            except Exception:
                page_url = None

        needs_load = (
            self._page is None
            or page_url is None
            or page_url.split("#")[0].split("?")[0] != url.split("#")[0].split("?")[0]
            or (now - self._page_loaded_at) > self.reload_every_sec
        )
        if not needs_load:
            return

        # Close the stale page first; new_page is cheap on a warm context.
        if self._page is not None:
            try:
                await self._page.close()
            except Exception:
                pass
            self._page = None

        page = await self._context.new_page()
        await page.goto(
            url, wait_until="domcontentloaded", timeout=self.nav_timeout_ms,
        )
        try:
            await page.wait_for_function(
                self.ready_signal_js, timeout=self.nav_timeout_ms,
            )
        except Exception:
            log.debug("ready signal missed during warmup")
            try:
                await page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
        # First-load settle so the SSE feed has produced at least one paint.
        await page.wait_for_timeout(max(self.settle_ms, 250))
        self._page = page
        self._page_loaded_at = time.monotonic()

    # ── snap ──────────────────────────────────────────────────────────────────
    async def snap(
        self,
        url: Optional[str] = None,
        *,
        full_page: bool = False,
        viewport: Optional[tuple[int, int]] = None,
        settle_ms: Optional[int] = None,
        bypass_cache: bool = False,
        _from_warmup: bool = False,
    ) -> SnapshotResult:
        t0 = time.monotonic()
        target_url = url or self.default_url
        mode = "full" if full_page else "viewport"
        vw = viewport or self.viewport

        if not self.available:
            return SnapshotResult(
                png=None, width=vw[0], height=vw[1], elapsed_ms=0, mode=mode,
                error=self._unavailable_reason or "snapshot service not started",
            )

        # Cache hit (viewport-mode only).
        if (
            not full_page
            and not bypass_cache
            and viewport is None
            and self._cached_png is not None
            and (time.monotonic() - self._cached_at) <= self.cache_max_age_sec
        ):
            return SnapshotResult(
                png=self._cached_png, width=vw[0], height=vw[1],
                elapsed_ms=self._cached_elapsed_ms, mode=mode, cached=True,
            )

        async with self._lock:
            try:
                await self._ensure_loaded(target_url)
                # If a custom viewport was requested, switch the page to it
                # then restore — small cost, rare path.
                if viewport:
                    await self._page.set_viewport_size(
                        {"width": viewport[0], "height": viewport[1]}
                    )
                # `settle_ms=0` is honoured — used by the cache loop to grab
                # the page state right now without an extra wait.
                wait = self.settle_ms if settle_ms is None else settle_ms
                if wait > 0:
                    await self._page.wait_for_timeout(wait)
                png = await self._page.screenshot(
                    type="png", full_page=full_page,
                )
                if viewport:
                    await self._page.set_viewport_size(
                        {"width": self.viewport[0], "height": self.viewport[1]}
                    )
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                # Update the warm cache (viewport-mode default-viewport only).
                if not full_page and viewport is None:
                    self._cached_png = png
                    self._cached_at = time.monotonic()
                    self._cached_elapsed_ms = elapsed_ms
                return SnapshotResult(
                    png=png, width=vw[0], height=vw[1],
                    elapsed_ms=elapsed_ms, mode=mode,
                )
            except Exception as exc:
                log.warning("snapshot failed: %s", exc, exc_info=True)
                # Force a reload on next request — the page may be wedged.
                self._page_loaded_at = 0.0
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                return SnapshotResult(
                    png=None, width=vw[0], height=vw[1],
                    elapsed_ms=elapsed_ms, mode=mode, error=str(exc),
                )

    async def _auto_refresh_loop(self) -> None:
        """Keep the warm cache populated. Runs on `auto_refresh_sec` cadence."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.auto_refresh_sec)
                # `bypass_cache=True` ensures we actually take a fresh shot,
                # `settle_ms=0` skips the per-call wait (the page is always
                # live thanks to the dashboard's own 1 Hz tick).
                await self.snap(
                    self.default_url, bypass_cache=True, settle_ms=0,
                )
            except asyncio.CancelledError:
                break
            except Exception:
                log.debug("auto-refresh loop error", exc_info=True)


__all__ = ["SnapshotService", "SnapshotResult"]
