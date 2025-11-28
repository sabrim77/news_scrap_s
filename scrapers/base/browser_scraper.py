# scrapers/base/browser_scraper.py

import logging
import random
import time
from contextlib import AbstractContextManager
from typing import Optional
from urllib.parse import urlparse

from playwright.sync_api import (
    Playwright,
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

logger = logging.getLogger("browser_scraper")


class BrowserScraper(AbstractContextManager):
    """
    Enterprise-grade Playwright scraper.

    Features:
        - Persistent browser/context/page
        - Rotating User-Agent + viewport spoofing
        - Block-page detection (Cloudflare, bot checks, WAF)
        - Automatic retries
        - Scroll-to-bottom for lazy-loaded contents
        - Resource blocking (ads/trackers → speed boost)
        - Per-domain polite delay
        - Full HTML snapshot
    """

    def __init__(
        self,
        timeout_ms: int = 15000,
        headless: bool = True,
        min_delay: float = 3.0,
        max_delay: float = 7.0,
        max_retries: int = 2,
        scroll: bool = True,
    ):
        self.timeout_ms = timeout_ms
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.scroll = scroll

        self._playwright: Optional[Playwright] = None
        self._browser = None
        self._context = None
        self._page = None

        self._last_request_ts: dict[str, float] = {}

        # Rotating user agents for browser too
        self._user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36",

            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.3 Safari/605.1.15",

            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36",
        ]

    # ------------------------------------------------------------------
    # Context Manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "BrowserScraper":
        logger.info("Starting Playwright (Chromium, headless=%s)...", self.headless)

        ua = random.choice(self._user_agents)
        width = random.randint(1100, 1400)
        height = random.randint(750, 900)

        self._playwright = sync_playwright().start()

        self._browser = self._playwright.chromium.launch(headless=self.headless)

        self._context = self._browser.new_context(
            user_agent=ua,
            viewport={"width": width, "height": height},
            java_script_enabled=True,
        )

        # Block unnecessary resources (makes scraping 40–70% faster)
        self._context.route(
            "**/*",
            lambda route, request: route.abort()
            if request.resource_type in {"image", "media", "font", "stylesheet", "other"}
            else route.continue_(),
        )

        self._page = self._context.new_page()
        self._page.set_default_timeout(self.timeout_ms)

        return self

    def __exit__(self, exc_type, exc, tb):
        logger.info("Shutting down Playwright browser...")
        try:
            if self._page:
                self._page.close()
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
            if self._playwright:
                self._playwright.stop()
        finally:
            self._page = None
            self._context = None
            self._browser = None
            self._playwright = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sleep_if_needed(self, netloc: str) -> None:
        last = self._last_request_ts.get(netloc)
        if last:
            elapsed = time.time() - last
            target = random.uniform(self.min_delay, self.max_delay)
            if elapsed < target:
                time.sleep(target - elapsed)

    def _update_last_ts(self, netloc: str) -> None:
        self._last_request_ts[netloc] = time.time()

    def _scroll_page(self):
        if not self.scroll:
            return

        try:
            self._page.evaluate(
                """
                () => {
                    window.scrollBy(0, document.body.scrollHeight);
                }
                """
            )
            time.sleep(1.0)
        except Exception:
            pass

    def _is_block_page(self, html_lower: str) -> bool:
        block_signals = [
            "cloudflare",
            "verify you are human",
            "checking your browser",
            "access denied",
            "bot detection",
            "captcha",
        ]
        return any(s in html_lower for s in block_signals)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_html(self, url: str) -> Optional[str]:
        """
        Navigate with a real browser and return the full HTML as string.
        Includes:
            - Retries
            - WAF/bot-detection checks
            - Lazy-load fix (scroll)
        """

        if not self._page:
            raise RuntimeError(
                "BrowserScraper must be used inside a context: "
                "with BrowserScraper() as browser:"
            )

        netloc = urlparse(url).netloc
        self._sleep_if_needed(netloc)

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info("Browser GET %s (attempt %d/%d)", url, attempt, self.max_retries)

                self._page.goto(url, wait_until="networkidle")

                # Scroll to load lazy contents (optional)
                self._scroll_page()

                html = self._page.content()
                html_lower = html.lower()

                # Block-page detection
                if self._is_block_page(html_lower):
                    logger.warning("Browser detected block/WAF for %s", url)
                    time.sleep(3 * attempt)
                    continue

                self._update_last_ts(netloc)
                return html

            except PlaywrightTimeoutError:
                logger.warning("Timeout loading %s (attempt %d)", url, attempt)
            except Exception as e:
                logger.exception("Error loading %s: %s", url, e)

            time.sleep(2 * attempt)

        logger.error("Browser giving up on %s after %d attempts", url, self.max_retries)
        return None
