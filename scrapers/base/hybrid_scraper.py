# scrapers/base/hybrid_scraper.py

from __future__ import annotations

import logging
from typing import Optional, Iterable
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from .browser_scraper import BrowserScraper

logger = logging.getLogger("hybrid_scraper")

# Status codes that usually indicate blocking / WAF / transient backend issues
BLOCK_STATUS_CODES = {403, 429, 503, 520, 521, 522}


class HybridScraper:
    """
    High-level HTML fetcher with:
        - Simple HTTP first (BaseScraper)
        - Smart block detection (status + HTML heuristic)
        - Automatic browser fallback (BrowserScraper)
        - Per-domain "hard domain" preference for browser mode

    Usage:
        base = BaseScraper()
        with BrowserScraper() as browser:
            hybrid = HybridScraper(
                base_scraper=base,
                browser_scraper=browser,
                hard_domains={"www.prothomalo.com"},
            )
            soup = hybrid.fetch_html("https://www.prothomalo.com", mode="auto")
    """

    def __init__(
        self,
        base_scraper: BaseScraper,
        browser_scraper: Optional[BrowserScraper] = None,
        hard_domains: Optional[Iterable[str]] = None,
        min_block_html_len: int = 2000,
    ) -> None:
        self.base = base_scraper
        self.browser = browser_scraper
        self.hard_domains = set(hard_domains or [])
        self.min_block_html_len = min_block_html_len

    # -----------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------

    @staticmethod
    def _get_netloc(url: str) -> str:
        return urlparse(url).netloc

    def _looks_like_block_page(self, html: str, final_url: str) -> bool:
        """
        Heuristic: detect common WAF / block / challenge pages.
        """
        if not html or len(html) < self.min_block_html_len:
            # Very short HTML → often error/placeholder/challenge
            logger.debug(
                "HTML very short (%d bytes) for %s → suspicious",
                len(html),
                final_url,
            )
            return True

        lower = html.lower()
        suspicious_keywords = [
            "access denied",
            "request blocked",
            "just a moment",
            "cloudflare",
            "incapsula",
            "are you a robot",
            "verify you are human",
            "checking your browser",
            "to continue, please verify",
            "bot detection",
            "captcha",
        ]
        if any(k in lower for k in suspicious_keywords):
            logger.debug("Block keywords detected in HTML for %s", final_url)
            return True

        return False

    # -----------------------------------------------------
    # Public API
    # -----------------------------------------------------

    def fetch_html(self, url: str, mode: str = "auto") -> Optional[BeautifulSoup]:
        """
        Fetch HTML and return BeautifulSoup or None.

        mode:
            - "simple"  → use only BaseScraper (never fallback)
            - "browser" → use only BrowserScraper (requires self.browser)
            - "auto"    → try BaseScraper; if blocked/suspicious → browser fallback
        """
        netloc = self._get_netloc(url)

        if mode not in ("simple", "browser", "auto"):
            raise ValueError(
                f"Unknown mode '{mode}', expected 'simple', 'browser', or 'auto'"
            )

        # Hard-domain → prefer browser directly in AUTO mode
        if mode == "auto" and netloc in self.hard_domains and self.browser:
            logger.info(
                "Domain %s in hard_domains → using browser directly for %s",
                netloc,
                url,
            )
            return self._fetch_with_browser(url)

        # Browser-only mode
        if mode == "browser":
            if not self.browser:
                raise RuntimeError(
                    "HybridScraper: 'browser' mode requested but no browser_scraper provided"
                )
            return self._fetch_with_browser(url)

        # ---------------- SIMPLE PATH ----------------
        soup, blocked = self._fetch_with_base_and_check(url)

        if mode == "simple":
            # Caller explicitly requested simple path only
            return soup

        # AUTO mode:
        # If we got a good soup and it doesn't look blocked → return it
        if soup is not None and not blocked:
            return soup

        # Otherwise try browser fallback if available
        if self.browser:
            logger.info(
                "Falling back to browser for %s (blocked=%s, netloc=%s)",
                url,
                blocked,
                netloc,
            )
            return self._fetch_with_browser(url)

        logger.warning(
            "HybridScraper wanted browser fallback for %s but no browser_scraper is set",
            url,
        )
        # Could be None or suspicious HTML soup; better to return something than crash
        return soup

    # -----------------------------------------------------
    # BaseScraper path
    # -----------------------------------------------------

    def _fetch_with_base_and_check(
        self,
        url: str,
    ) -> tuple[Optional[BeautifulSoup], bool]:
        """
        Use BaseScraper.get so we can inspect status_code.

        Returns:
            (soup, blocked_flag)
        """
        try:
            resp = self.base.get(url)
        except Exception as exc:
            logger.exception("BaseScraper error while GET %s: %s", url, exc)
            return None, True

        if resp is None:
            logger.warning("BaseScraper returned None for %s", url)
            return None, True

        status = resp.status_code
        final_url = str(resp.url)

        # Hard block statuses
        if status in BLOCK_STATUS_CODES:
            logger.warning(
                "BaseScraper got block status %d for %s",
                status,
                final_url,
            )
            return None, True

        # Non-200 but not "hard" → treat as likely blocked/temporary
        if status != 200:
            logger.warning(
                "BaseScraper got non-OK status %d for %s",
                status,
                final_url,
            )
            return None, True

        html = resp.text or ""
        blocked = self._looks_like_block_page(html, final_url)

        soup = BeautifulSoup(html, "html.parser")
        return soup, blocked

    # -----------------------------------------------------
    # Browser path
    # -----------------------------------------------------

    def _fetch_with_browser(self, url: str) -> Optional[BeautifulSoup]:
        if not self.browser:
            logger.error(
                "HybridScraper._fetch_with_browser called but browser_scraper is None"
            )
            return None

        html = self.browser.fetch_html(url)
        if not html:
            logger.warning("BrowserScraper.fetch_html returned empty for %s", url)
            return None

        return BeautifulSoup(html, "html.parser")
