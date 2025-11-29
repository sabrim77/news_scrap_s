# core/fetch.py
"""
News fetching layer for the portal-based scraping system.

Responsibilities:
- Normalize user keyword queries (single word, comma-separated, full sentence).
- Iterate over configured RSS portals and fetch matching entries.
- Insert fetched articles into SQLite via `core.db`.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any
import re

import feedparser

from core import db
from config.portals import iter_enabled_portals, PortalConfig


# ---------------------------------------------------------------------
# Helpers: parsing & normalization
# ---------------------------------------------------------------------


def _parse_published(entry) -> datetime | None:
    """
    Convert feedparser's published/updated fields to a datetime, if possible.
    """
    if getattr(entry, "published_parsed", None):
        try:
            tt = entry.published_parsed
            return datetime(*tt[:6])
        except Exception:
            return None
    return None


def _normalize_keywords(raw: str) -> List[str]:
    """
    Normalize user query for *fetching*.

    Examples:
        "bitcoin"                      → ["bitcoin"]
        "bitcoin, tesla, nvidia"       → ["bitcoin", "tesla", "nvidia"]
        "what is happening in bitcoin" → ["what is happening in bitcoin"]

    Rules:
        - If there is at least one comma → split by comma into multiple keywords.
        - Otherwise, treat the whole string (even a sentence) as one keyword.
    """
    if raw is None:
        return []

    q = raw.strip()
    if not q:
        return []

    # If the user uses commas, we assume *multiple* separate keywords
    if "," in q:
        parts = [p.strip() for p in q.split(",")]
        return [p for p in parts if p]

    # No comma: treat the entire thing as a single query/phrase
    return [q]


def _entry_matches_keyword(entry, keyword: str) -> bool:
    """
    Check if an RSS entry matches the keyword (case-insensitive)
    in title or summary. Supports single word or multi-word phrase.
    """
    kw = (keyword or "").strip()
    if not kw:
        return False

    pattern = re.compile(re.escape(kw), re.IGNORECASE)

    title = (entry.get("title") or "").strip()
    summary = (entry.get("summary") or "").strip()

    return bool(pattern.search(title) or pattern.search(summary))


def _portal_matches_lang_country(
    cfg: PortalConfig,
    lang: str | None,
    country: str | None,
) -> bool:
    """
    Optional portal filter by language and country tags.

    - If lang is provided → only portals with matching `language` are used.
    - If country is provided → only portals with matching `country` are used.
    - If a field is missing on a portal, it does NOT block that portal.

    Examples:
        lang="bangla"          → match language="bangla"
        lang="english"         → match language="english"
        country="bd"           → match country="bd"
        country="international"→ match country="international"
    """
    if lang:
        portal_lang = (cfg.get("language") or "").lower()
        if portal_lang and portal_lang != lang.lower():
            return False

    if country:
        portal_country = (cfg.get("country") or "").lower()
        if portal_country and portal_country != country.lower():
            return False

    return True


def _normalize_lang_for_portals(lang: str | None) -> str | None:
    """
    Map API-style lang codes to portal language tags.

    Examples:
        "en" / "eng" / "english" → "english"
        "bn" / "bengali" / "bangla" → "bangla"
        None / ""                → None (no filter)
    """
    if not lang:
        return None

    l = lang.lower()

    if l in {"en", "eng", "english"}:
        return "english"
    if l in {"bn", "bengali", "bangla"}:
        return "bangla"

    # Unknown → don't filter by language
    return None


def _normalize_country_for_portals(country: str | None) -> str | None:
    """
    Map API-style country codes to portal country tags.

    Examples:
        "BD" / "bd" / "bangladesh" → "bd"
        "intl" / "international"  → "international"
        None / ""                 → None (no filter)
    """
    if not country:
        return None

    c = country.lower()

    if c in {"bd", "bangladesh"}:
        return "bd"
    if c in {"intl", "international", "world"}:
        return "international"

    # Unknown → don't filter by country
    return None


# ---------------------------------------------------------------------
# Fetch operations
# ---------------------------------------------------------------------


def fetch_news_for_keyword(
    keyword: str,
    lang: str | None = None,
    country: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch latest news for a *single* keyword from all enabled portals' RSS feeds.

    Behaviour:
        - Iterates over all enabled portals from config/portals.py.
        - Optionally filters portals by language/country:
              lang    → mapped via _normalize_lang_for_portals()
              country → mapped via _normalize_country_for_portals()
        - Parses each RSS URL in cfg["rss"].
        - Filters entries by keyword in title/summary (case-insensitive).
        - Inserts all matched articles into SQLite via db.insert_articles().

    Args:
        keyword:  Search keyword/phrase to match in RSS title/summary.
        lang:     Optional language filter ("en", "bn", "english", "bangla", etc.).
        country:  Optional country filter ("BD", "international", etc.).

    Returns:
        Flat list of article dicts (also inserted into the DB).
    """
    keyword = (keyword or "").strip()
    if not keyword:
        return []

    portal_lang = _normalize_lang_for_portals(lang)
    portal_country = _normalize_country_for_portals(country)

    articles: List[Dict[str, Any]] = []

    # Iterate all enabled portals from config/portals.py
    for portal_id, cfg in iter_enabled_portals():
        # Skip portals without RSS
        rss_list = cfg.get("rss") or []
        if not rss_list:
            continue

        # Optional language/country filtering
        if not _portal_matches_lang_country(cfg, portal_lang, portal_country):
            continue

        for rss_url in rss_list:
            if not rss_url:
                continue

            try:
                feed = feedparser.parse(rss_url)
            except Exception:
                # If a portal/feed is temporarily broken, skip it
                continue

            for entry in getattr(feed, "entries", []):
                title = (entry.get("title") or "").strip()
                link = (entry.get("link") or "").strip()
                summary = (entry.get("summary") or "").strip()

                if not title or not link:
                    continue

                # Keyword match in title/summary
                if not _entry_matches_keyword(entry, keyword):
                    continue

                published_at = _parse_published(entry)

                articles.append(
                    {
                        "title": title,
                        "url": link,
                        "summary": summary,
                        "content": None,          # can later be replaced with full HTML content
                        "source": portal_id,      # maps to 'portal' in DB
                        "keyword": keyword,       # maps to 'topic' in DB
                        "published_at": published_at,
                    }
                )

    if articles:
        db.insert_articles(articles)

    return articles


def fetch_news_for_query(
    raw_query: str,
    lang: str | None = None,
    country: str | None = None,
) -> Dict[str, Any]:
    """
    High-level fetch that understands:
        - One word:          "bitcoin"
        - Comma-separated:   "bitcoin, tesla, nvidia"
        - Full sentence:     "what is happening with bitcoin price today"
        - Bangla phrases:    "দেশে ফেরার সিদ্ধান্ত"
        - Mixed keywords:    "তারেক,রহমান" → ["তারেক", "রহমান"]

    It first normalizes the raw query into a list of keywords using `_normalize_keywords`,
    then calls `fetch_news_for_keyword` for each keyword.

    Args:
        raw_query: Raw user query string (exactly what user types).
        lang:      Optional language filter passed to `fetch_news_for_keyword`.
        country:   Optional country filter passed to `fetch_news_for_keyword`.

    Returns:
        {
          "keywords": ["bitcoin", "tesla"],
          "total_fetched": 25,
          "by_keyword": {
            "bitcoin": [...],
            "tesla":   [...],
          }
        }
    """
    keywords = _normalize_keywords(raw_query)
    if not keywords:
        return {"keywords": [], "total_fetched": 0, "by_keyword": {}}

    all_articles: List[Dict[str, Any]] = []
    by_keyword: Dict[str, List[Dict[str, Any]]] = {}

    for kw in keywords:
        articles = fetch_news_for_keyword(kw, lang=lang, country=country)
        by_keyword[kw] = articles
        all_articles.extend(articles)

    return {
        "keywords": keywords,
        "total_fetched": len(all_articles),
        "by_keyword": by_keyword,
    }


def fetch_news_from_user_query(
    user_input: str,
    lang: str | None = None,
    country: str | None = None,
) -> Dict[str, Any]:
    """
    Convenience wrapper for user-facing keyword input.

    Accepts exactly what the user types in a search box, for example:
        - "দেশে ফেরার সিদ্ধান্ত"
        - "তারেক,রহমান"
        - "তারেক রহমান"
        - "bitcoin, tesla, nvidia"

    Internally this just calls `fetch_news_for_query`, which:
        - Normalizes comma-separated lists vs single phrases.
        - Fetches and stores portal news per keyword.
    """
    return fetch_news_for_query(user_input, lang=lang, country=country)
