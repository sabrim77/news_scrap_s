# core/article_fetcher.py
"""
High-level HTML article fetcher.

Responsibilities:
    - Resolve the correct scraping mode for a portal
    - Use BaseScraper or browser/hybrid scrapers when available
    - Return BeautifulSoup or None
    - Central place for error handling during fetch
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, TYPE_CHECKING

from bs4 import BeautifulSoup

from config.portals import PORTALS, get_portal
from scrapers.base.base_scraper import BaseScraper

if TYPE_CHECKING:
    from scrapers.base.browser_scraper import BrowserScraper
    from scrapers.base.hybrid_scraper import HybridScraper

logger = logging.getLogger("article_fetcher")

# ============================================================
# Shared scraper singletons
# ============================================================

_BASE = BaseScraper()

_BROWSER: Optional["BrowserScraper"] = None
_HYBRID_BY_PORTAL: Dict[str, "HybridScraper"] = {}


# ============================================================
# Helpers
# ============================================================

def _get_browser() -> Optional["BrowserScraper"]:
    """
    Lazily initialize a shared BrowserScraper instance.

    Returns:
        BrowserScraper or None
    """
    global _BROWSER

    if _BROWSER is not None:
        return _BROWSER

    try:
        from scrapers.base.browser_scraper import BrowserScraper as BrowserScraperImpl
    except Exception as exc:
        logger.warning("Playwright/browser scraper unavailable: %s", exc)
        return None

    try:
        logger.info("Initializing BrowserScraper (Playwright headless)...")
        browser = BrowserScraperImpl(headless=True)

        # IMPORTANT:
        # Keep the browser open for the entire process lifetime.
        browser.__enter__()
        _BROWSER = browser
        return _BROWSER
    except Exception as exc:
        logger.exception("Failed to initialize BrowserScraper: %s", exc)
        return None


def _get_hybrid(portal_id: str) -> Optional["HybridScraper"]:
    """
    Return a HybridScraper instance for this portal.
    Creates and caches on first use.
    """
    if portal_id in _HYBRID_BY_PORTAL:
        return _HYBRID_BY_PORTAL[portal_id]

    try:
        from scrapers.base.hybrid_scraper import HybridScraper as HybridScraperImpl
    except Exception as exc:
        logger.warning("HybridScraper unavailable (Playwright missing?): %s", exc)
        return None

    cfg = get_portal(portal_id) or {}
    hard_domains = cfg.get("hard_domains", []) or []

    browser = _get_browser()
    if browser is None:
        return None

    try:
        hybrid = HybridScraperImpl(
            base_scraper=_BASE,
            browser_scraper=browser,
            hard_domains=hard_domains,
        )
        _HYBRID_BY_PORTAL[portal_id] = hybrid

        logger.info(
            "Created HybridScraper for %s (hard_domains=%s)",
            portal_id,
            hard_domains,
        )
        return hybrid
    except Exception as exc:
        logger.exception("Unable to create HybridScraper for %s: %s", portal_id, exc)
        return None


# ============================================================
# Public API
# ============================================================

def fetch_article_soup(portal: str, url: str) -> Optional[BeautifulSoup]:
    """
    Unified HTML fetcher used by the runner.

    scrape_mode:
        simple     → BaseScraper
        hybrid     → Base + Browser fallback
        browser    → Browser-only
        rss_only   → skip
    """

    cfg = PORTALS.get(portal)
    if cfg is None:
        logger.error("fetch_article_soup: unknown portal '%s'", portal)
        return None

    if cfg.get("enabled", True) is False:
        logger.info("Portal '%s' disabled → skip HTML", portal)
        return None

    mode = cfg.get("scrape_mode", "simple")

    logger.info("Fetching: portal=%s | mode=%s | url=%s", portal, mode, url)

    # --------------------------------------------------------
    # RSS-ONLY → never fetch HTML
    # --------------------------------------------------------
    if mode == "rss_only":
        return None

    # --------------------------------------------------------
    # SIMPLE MODE
    # --------------------------------------------------------
    if mode == "simple":
        try:
            return _BASE.fetch_html(url)
        except Exception as exc:
            logger.warning("Simple fetch failed for %s (%s)", url, exc)
            return None

    # --------------------------------------------------------
    # BROWSER or HYBRID MODE
    # --------------------------------------------------------
    hybrid = _get_hybrid(portal)

    if hybrid is None:
        # fallback: simple
        logger.warning("Hybrid/browser unavailable → fallback simple for %s", url)
        try:
            return _BASE.fetch_html(url)
        except Exception as exc:
            logger.warning("Fallback simple fetch failed for %s (%s)", url, exc)
            return None

    hybrid_mode = "browser" if mode == "browser" else "auto"

    try:
        return hybrid.fetch_html(url, mode=hybrid_mode)
    except Exception as exc:
        logger.exception("Hybrid fetch failed for %s: %s", url, exc)
        return None
