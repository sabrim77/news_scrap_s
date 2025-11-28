# scrapers/bd/risingbd.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.risingbd")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Risingbd article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("risingbd.parse called with soup=None")
        return {"title": None, "body": None}

    # ------------------------------------------------------------
    # 1) Remove unwanted tags
    # ------------------------------------------------------------
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, "
        ".share, .tags, .related-news, .related, "
        ".ad, .ads, .advertisement"
    ):
        tag.decompose()

    # ------------------------------------------------------------
    # 2) Title extraction
    # ------------------------------------------------------------
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # ------------------------------------------------------------
    # 3) Main content container guess
    # ------------------------------------------------------------
    candidates = [
        "div#news-details",
        "div.news-details",
        "div.details",
        "div.details-inner",
        "div.article",
        "div.article-content",
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
        logger.debug("risingbd: no specific content container found, using full page")
        container = soup

    # ------------------------------------------------------------
    # 4) Extract clean paragraphs
    # ------------------------------------------------------------
    MIN_CHARS = 25
    MAX_CHARS = 1200

    blacklist = [
        "আরও পড়ুন",
        "আরও পড়ুন",
        "আরো পড়ুন",
        "আরো পড়ুন",
        "risingbd.com",
        "রাইজিংবিডি",
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
                # Short footer/promo → drop
                if len(text) < 120:
                    return None
        return text

    body_parts: list[str] = []

    # Primary pass: <p> tags
    for p in container.find_all("p"):
        txt = p.get_text(" ", strip=True)
        cleaned = clean_text(txt)
        if cleaned:
            body_parts.append(cleaned)

    # Fallback: stripped_strings if <p> gave nothing
    if not body_parts:
        logger.debug("risingbd: no usable <p>, falling back to stripped_strings")
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
