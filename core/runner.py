# core/runner.py
"""
Orchestrates the entire scraping pipeline:

    1) Initialize DB
    2) Collect RSS items
    3) Fetch HTML (simple/hybrid/browser)
    4) Parse article
    5) Classify topic
    6) Save into DB
    7) Run single cycle OR interval loop
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, Callable, Any, Optional
from urllib.parse import urlparse

from config.portals import PORTALS
from core.rss_collector import collect
from core.article_fetcher import fetch_article_soup
from core import db
from core.topic_classifier import classify_topic

# Import BD parsers
from scrapers.bd import (
    jagonews24,
    risingbd,
    prothomalo,
    banglatribune,
    banglanews24,
    bdnews24,
    samakal,
    ittefaq,
    kalerkantho,
    deshrupantor,
)

# Import International
from scrapers.international import (
    bbc,
    thedailystar,
)

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------

logger = logging.getLogger("runner")
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -------------------------------------------------------------------------
# Parser Registry (auto-wired)
# -------------------------------------------------------------------------

PARSERS: Dict[str, Callable[[Any], Dict[str, Optional[str]]]] = {
    # BD
    "jagonews24": jagonews24.parse,
    "risingbd": risingbd.parse,
    "prothomalo": prothomalo.parse,
    "banglatribune": banglatribune.parse,
    "banglanews24": banglanews24.parse,
    "bdnews24": bdnews24.parse,
    "samakal": samakal.parse,
    "ittefaq": ittefaq.parse,
    "kalerkantho":  kalerkantho.parse,
    "deshrupantor": deshrupantor.parse,

    # International
    "bbc": bbc.parse,
    "thedailystar": thedailystar.parse,
}

# -------------------------------------------------------------------------
# Helpers: title derivation
# -------------------------------------------------------------------------


def _title_from_url(url: str) -> Optional[str]:
    """
    Fallback: make a human-ish title from URL slug.
    e.g. https://.../poison-the-plate-4044126 -> 'Poison the plate'
    """
    try:
        path = urlparse(url).path or ""
        slug = path.rstrip("/").split("/")[-1]
        # drop extension if any
        if "." in slug:
            slug = slug.split(".", 1)[0]

        slug = slug.replace("-", " ").replace("_", " ").strip()
        if not slug:
            return None

        # Basic beautify
        slug = slug[0].upper() + slug[1:]
        return slug
    except Exception:
        return None


def _derive_title(item: Dict[str, Any]) -> Optional[str]:
    """
    Try multiple RSS fields, then fall back to URL slug.
    Priority:
        1) item['title']
        2) item['summary']
        3) item['description']
        4) slug from item['link']
    """
    for key in ("title", "summary", "description"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    link = item.get("link")
    if isinstance(link, str) and link:
        slug_title = _title_from_url(link)
        if slug_title:
            return slug_title

    return None

# -------------------------------------------------------------------------
# Save Helper
# -------------------------------------------------------------------------


def save_article_to_db(
    portal: str,
    link: str,
    title: Optional[str],
    body: Optional[str],
    rss_date: Optional[str],
    topic: Optional[str],
) -> bool:
    """Unified DB insert."""
    return db.insert_news(
        portal=portal,
        url=link,
        title=title,
        content=body,
        topic=topic,
        pub_date=rss_date,
    )

# -------------------------------------------------------------------------
# Process Single Item
# -------------------------------------------------------------------------


def process_item(item: Dict[str, Any]) -> bool:
    """
    Process one RSS item:
        RSS → Fetch HTML → Parse → Classify → DB

    Returns:
        True = saved
        False = skipped / failed
    """

    source = item["source"]
    link = item["link"]

    # Try to get a robust RSS title (not just item["title"])
    rss_title: Optional[str] = _derive_title(item)
    rss_date = item.get("rss_date")

    cfg = PORTALS.get(source, {})
    enabled = cfg.get("enabled", True)
    mode = cfg.get("scrape_mode", "simple")  # simple | hybrid | browser | rss_only

    if not enabled:
        logger.info("Skipping disabled portal: %s", source)
        return False

    parser = PARSERS.get(source)

    logger.info("Process: %s | %s [mode=%s]", source, link, mode)

    title: Optional[str] = None
    body: Optional[str] = None

    # --------------- RSS ONLY MODE -----------------
    if mode == "rss_only":
        title = rss_title
        body = None

    else:
        # --------------- FETCH HTML -----------------
        try:
            soup = fetch_article_soup(source, link)
        except Exception as err:
            logger.warning("Fetch crash: %s (%s)", link, err)
            return False

        if soup is None:
            logger.info("Soup=None → fallback to RSS-only")
            title = rss_title
            body = None
        else:
            # --------------- PARSER -----------------
            if parser:
                try:
                    parsed = parser(soup) or {}
                except Exception as err:
                    logger.warning("Parser crash for %s: %s", source, err)
                    parsed = {}

                # Parsed title has highest priority; fallback to RSS-derived title
                title = parsed.get("title") or rss_title
                body = parsed.get("body")

            else:
                # No parser registered → rely only on RSS
                title = rss_title
                body = None

    # Final safety: still no title? Try deriving again from URL alone.
    if not title:
        fallback = _title_from_url(link)
        if fallback:
            logger.debug("Derived title from URL for %s: %s", link, fallback)
            title = fallback

    # --------------- VALIDATION -----------------
    if not title and not body:
        logger.info("Skip empty (title/body missing): %s", link)
        return False

    # --------------- TOPIC CLASSIFIER -----------------
    topic = classify_topic(
        portal=source,
        url=link,
        title=title or "",
        body=body or "",
    )

    # --------------- SAVE -----------------
    ok = save_article_to_db(
        portal=source,
        link=link,
        title=title,
        body=body,
        rss_date=rss_date,
        topic=topic,
    )

    if ok:
        logger.info("Saved: %s [topic=%s]", title or "(no title)", topic)
        return True

    return False

# -------------------------------------------------------------------------
# One Full Cycle
# -------------------------------------------------------------------------


def run_single_cycle() -> None:
    logger.info("=== Single cycle started ===")

    # Ensure DB exists
    db.init_db()

    items = collect()
    logger.info("RSS total collected: %d", len(items))

    total = len(items)
    saved = 0
    skipped = 0

    stats = defaultdict(lambda: {"total": 0, "saved": 0, "skipped": 0})

    for item in items:
        portal = item.get("source", "unknown")
        stats[portal]["total"] += 1

        ok = process_item(item)
        if ok:
            saved += 1
            stats[portal]["saved"] += 1
        else:
            skipped += 1
            stats[portal]["skipped"] += 1

    logger.info("SUMMARY: total=%d | saved=%d | skipped=%d", total, saved, skipped)

    logger.info("--- Per Portal Stats ---")
    for portal, s in stats.items():
        logger.info(
            "  %s → total=%d | saved=%d | skipped=%d",
            portal,
            s["total"],
            s["saved"],
            s["skipped"],
        )

# -------------------------------------------------------------------------
# Loop Mode
# -------------------------------------------------------------------------


def run_loop(interval_minutes: int) -> None:
    logger.info("Starting loop, interval=%d min", interval_minutes)

    while True:
        start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("=== New Cycle (%s) ===", start)

        run_single_cycle()

        logger.info("Sleeping %d minutes...", interval_minutes)
        time.sleep(interval_minutes * 60)

# -------------------------------------------------------------------------
# CLI Entry
# -------------------------------------------------------------------------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="News Scraper Runner")
    parser.add_argument(
        "interval",
        nargs="?",
        type=int,
        help="Run in loop mode (minutes). Empty → single cycle.",
    )

    args = parser.parse_args()

    if args.interval:
        run_loop(args.interval)
    else:
        run_single_cycle()


if __name__ == "__main__":
    main()
