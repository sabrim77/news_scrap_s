# scrapers/bd/kalerkantho.py

from __future__ import annotations

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.kalerkantho")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Kaler Kantho article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("kalerkantho.parse called with soup=None")
        return {"title": None, "body": None}

    # 1) Clean noisy elements
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, "
        ".share, .social, .social-share, .tags, "
        ".related, .related-news, .ad, .ads, .advertisement"
    ):
        tag.decompose()

    # 2) Title
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("h3") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) Content container candidates (site layout evolves; keep generous)
    candidates = (
        "div#news-details",
        "div.news-details",
        "div.details",
        "div.details-inner",
        "div.article-content",
        "div.content",
        "article",
        "main",
    )
    container = None
    for sel in candidates:
        node = soup.select_one(sel)
        if node:
            container = node
            break
    if container is None:
        logger.debug("kalerkantho: no specific content container found, using full page")
        container = soup

    # 4) Paragraph extraction with guards
    MIN_CHARS = 25
    MAX_CHARS = 1200
    SHORT_BLACKLIST_LEN = 120  # drop short promo/footer lines only if below this

    blacklist = [
        "কালের কণ্ঠ",         # paper name/bylines/promos
        "kalerkontho",        # alternative spelling used in assets/links
        "kaler kantho",
        "আরও পড়ুন",
        "আরও পড়ুন",
        "আরও খবর",
        "read more",
        "সর্বশেষ সংবাদ",
        "top news",
    ]

    def clean_text(text: str) -> Optional[str]:
        if not text:
            return None
        ln = len(text)
        if ln < MIN_CHARS or ln > MAX_CHARS:
            return None
        lower = text.lower()
        # Skip short promotional/credit lines that contain blacklisted tokens
        if any(b in lower for b in blacklist) and ln < SHORT_BLACKLIST_LEN:
            return None
        return text

    parts: list[str] = []

    # Primary: <p> blocks
    for p in container.find_all("p"):
        txt = p.get_text(" ", strip=True)
        cleaned = clean_text(txt)
        if cleaned:
            parts.append(cleaned)

    # Fallback: stripped strings if paragraphs failed
    if not parts:
        logger.debug("kalerkantho: no usable <p>, falling back to stripped_strings")
        for chunk in container.stripped_strings:
            txt = (chunk or "").strip()
            cleaned = clean_text(txt)
            if cleaned:
                parts.append(cleaned)

    body = "\n\n".join(parts).strip() if parts else None

    return {"title": title, "body": body}
