# config/portals.py
"""
Portal configuration for the entire scraping system.

This module defines:
- A schema for each portal entry
- All enabled/disabled Bangla, English, and international news portals
- RSS endpoints
- Scraping modes
- Hard domains for browser fallback
- Validation helpers

Scrape modes:
    "rss_only"  → only RSS title/summary, no HTML fetch
    "simple"    → simple BaseScraper only
    "browser"   → BrowserScraper only
    "hybrid"    → simple → browser fallback (recommended)

This file is designed to be:
- Clean
- Extensible
- Safe for production
- Easy to manage when adding new portals
"""

from typing import Dict, List, Optional, Iterator, TypedDict


# ---------------------------------------------------------
# Typed schema for safety (industry practice)
# ---------------------------------------------------------

class PortalConfig(TypedDict, total=False):
    rss: List[str]
    enabled: bool
    scrape_mode: str            # "simple", "hybrid", "browser", "rss_only"
    hard_domains: List[str]     # domains that need browser fallback
    language: str               # optional: "bangla" | "english" | "mixed"
    country: str                # optional: "bd", "international"
    notes: str                  # optional developer notes


# ---------------------------------------------------------
# PORTAL REGISTRY
# ---------------------------------------------------------

PORTALS: Dict[str, PortalConfig] = {

    # ======================================================
    # 🇧🇩 BANGLADESH — BANGLA NEWS PORTALS
    # ======================================================

    "bdnews24": {
        "rss": [
            "https://bdnews24.com/feed/rss.xml",
            "https://bdnews24.com/bangladesh/rss.xml",
            "https://bdnews24.com/politics/rss.xml",
            "https://bdnews24.com/world/rss.xml",
        ],
        "enabled": True,
        "scrape_mode": "browser",               # heavy JS
        "hard_domains": ["bdnews24.com"],
        "language": "bangla",
        "country": "bd",
    },

    "prothomalo": {
        "rss": [
            "https://en.prothomalo.com/rss",
            "https://www.prothomalo.com/feed",
        ],
        "enabled": True,
        # Was "hybrid" → make RSS-only for now due to heavy WAF
        "scrape_mode": "rss_only",
        "hard_domains": ["prothomalo.com", "en.prothomalo.com"],
        "language": "bangla",
        "country": "bd",
    },

    "banglatribune": {
        "rss": [
            "https://www.banglatribune.com/feed/rss.xml",
            "https://www.banglatribune.com/feed",
        ],
        "enabled": True,
        "scrape_mode": "hybrid",
        "language": "bangla",
        "country": "bd",
    },

    "banglanews24": {
        "rss": [
            "https://www.banglanews24.com/rss/1",
            "https://www.banglanews24.com/rss/3",
            "https://www.banglanews24.com/rss/11",
        ],
        "enabled": True,
        "scrape_mode": "rss_only",              # anti-bot heavy
        "language": "bangla",
        "country": "bd",
    },

    "kalerkantho": {
        "rss": ["https://www.kalerkantho.com/rss.xml"],
        "enabled": True,
        # Was "browser" → RSS-only to avoid repeated WAF blocking
        "scrape_mode": "rss_only",
        "language": "bangla",
        "country": "bd",
    },

    "risingbd": {
        "rss": [
            "https://www.risingbd.com/rss.xml",
            "https://www.risingbd.com/rss/rss.xml",
        ],
        "enabled": True,
        "scrape_mode": "simple",
        "language": "bangla",
        "country": "bd",
    },

    "jagonews24": {
        "rss": [
            "https://www.jagonews24.com/rss.xml",
            "https://www.jagonews24.com/rss/rss.xml",
        ],
        "enabled": True,
        # Was "simple" → RSS-only because HTML fetch often blocked
        "scrape_mode": "rss_only",
        "language": "bangla",
        "country": "bd",
    },

    "samakal": {
        "rss": ["https://samakal.com/rss.xml"],
        "enabled": True,
        "scrape_mode": "hybrid",
        "language": "bangla",
        "country": "bd",
    },

    "ittefaq": {
        "rss": ["https://www.ittefaq.com.bd/rss.xml"],
        "enabled": True,
        "scrape_mode": "hybrid",
        "language": "bangla",
        "country": "bd",
    },

    "deshrupantor": {
        "rss": ["https://www.deshrupantor.com/rss.xml"],
        "enabled": True,
        "scrape_mode": "simple",
        "language": "bangla",
        "country": "bd",
    },


    # ------------------------------------------------------
    # 💬 DISABLED BD PORTALS (enable later)
    # ------------------------------------------------------

    "channel24": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "browser",
        "language": "bangla",
        "country": "bd",
        "notes": "Enable when RSS endpoints available.",
    },

    "independent24": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "browser",
        "language": "bangla",
        "country": "bd",
    },

    "somoynews": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "browser",
        "language": "bangla",
        "country": "bd",
    },


    # ======================================================
    # 🌍 INTERNATIONAL NEWS PORTALS
    # ======================================================

    "bbc": {
        "rss": [
            "https://feeds.bbci.co.uk/news/rss.xml",
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            "https://feeds.bbci.co.uk/news/asia/rss.xml",
        ],
        "enabled": True,
        "scrape_mode": "simple",
        "language": "english",
        "country": "international",
    },

    "thedailystar": {
        "rss": [
            "https://www.thedailystar.net/frontpage/rss.xml",
            "https://www.thedailystar.net/opinion/rss.xml",
            "https://www.thedailystar.net/world/rss.xml",
        ],
        "enabled": True,
        "scrape_mode": "rss_only",
        "language": "english",
        "country": "international",
    },

    "cnn": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "browser",
        "language": "english",
        "country": "international",
    },

    "reuters": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "simple",
        "language": "english",
        "country": "international",
    },

    "aljazeera": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "simple",
        "language": "english",
        "country": "international",
    },

    "nytimes": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "rss_only",
        "language": "english",
        "country": "international",
    },

    "guardian": {
        "rss": [],
        "enabled": False,
        "scrape_mode": "simple",
        "language": "english",
        "country": "international",
    },
}


# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def get_portal(portal_id: str) -> Optional[PortalConfig]:
    """Return a single portal configuration."""
    return PORTALS.get(portal_id)


def iter_enabled_portals() -> Iterator[tuple[str, PortalConfig]]:
    """Yield only enabled portals."""
    for pid, cfg in PORTALS.items():
        if cfg.get("enabled", True):
            yield pid, cfg


def validate_portals() -> None:
    """Validate all portal entries on startup."""
    allowed_modes = {"simple", "hybrid", "browser", "rss_only"}

    for pid, cfg in PORTALS.items():
        if "scrape_mode" not in cfg:
            raise ValueError(f"Portal '{pid}' missing scrape_mode")

        if cfg["scrape_mode"] not in allowed_modes:
            raise ValueError(
                f"Portal '{pid}' has invalid scrape_mode '{cfg['scrape_mode']}'"
            )

        if "rss" not in cfg:
            raise ValueError(f"Portal '{pid}' missing RSS list")

        if not isinstance(cfg["rss"], list):
            raise ValueError(f"Portal '{pid}' RSS must be a list")

        # Optional fields are allowed
        # Hard domains must be list
        if "hard_domains" in cfg and not isinstance(cfg["hard_domains"], list):
            raise ValueError(f"Portal '{pid}' hard_domains must be a list")
