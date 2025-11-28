# scrapers/bd/samakal.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.samakal")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Samakal article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("samakal.parse called with soup=None")
        return {"title": None, "body": None}

    # ------------------------------------------------------------
    # 1) Remove unwanted / noisy tags
    # ------------------------------------------------------------
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, "
        ".share, .social, .tags, .related, .related-news, "
        ".ad, .ads, .advertisement"
    ):
        tag.decompose()

    # ------------------------------------------------------------
    # 2) Title extraction
    # ------------------------------------------------------------
    title_tag = (
        soup.find("h1")
        or soup.find("h2")
        or soup.select_one("h1.title")
        or soup.find("title")
    )
    title = title_tag.get_text(strip=True) if title_tag else None

    # ------------------------------------------------------------
    # 3) Main article container guess
    #    These selectors are generic but tuned for BD news layouts.
    # ------------------------------------------------------------
    candidates = [
        "div.news-details",
        "div#news-details",
        "div.details",
        "div.details-inner",
        "div.article-content",
        "div.article",
        "div.content",
        "article",
        "main",
    ]

    container: Optional[BeautifulSoup] = None
    for sel in candidates:
        node = soup.select_one(sel)
        if node:
            container = node
            break

    if container is None:
        logger.debug("samakal: no specific content container found, using full page")
        container = soup

    # ------------------------------------------------------------
    # 4) Extract clean paragraphs
    # ------------------------------------------------------------
    MIN_CHARS = 25
    MAX_CHARS = 1300

    blacklist = [
        "সমকাল",          # paper name
        "samakal.com",
        "আরও পড়ুন",
        "আরও পড়ুন",
        "আরো পড়ুন",
        "আরো পড়ুন",
        "আরও খবর",
        "read more",
        "সর্বশেষ সংবাদ",
        "top news",
    ]

    def clean_text(text: str) -> Optional[str]:
        if not text:
            return None
        if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
            return None

        lower = text.lower()
        for bad in blacklist:
            if bad in lower:
                # short footer/promo → drop
                if len(text) < 120:
                    return None
        return text

    body_parts: list[str] = []

    # Primary: <p> tags
    for p in container.find_all("p"):
        txt = p.get_text(" ", strip=True)
        cleaned = clean_text(txt)
        if cleaned:
            body_parts.append(cleaned)

    # Fallback: stripped_strings if <p> gave nothing
    if not body_parts:
        logger.debug("samakal: no usable <p>, falling back to stripped_strings")
        for chunk in container.stripped_strings:
            txt = chunk.strip()
            cleaned = clean_text(txt)
            if cleaned:
                body_parts.append(cleaned)

    body = "\n\n".join(body_parts).strip() if body_parts else None

    return {
        "title": title,
        "body": body,
    }
