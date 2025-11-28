# scrapers/bd/kalerkontho.py

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
        logger.warning("kalerkontho.parse called with soup=None")
        return {"title": None, "body": None}

    # ------------------------------------------------------------
    # 1) Remove unwanted tags
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
    #    Kaler Kantho সাধারণত <h1> বা বড় শিরোনাম ট্যাগ ব্যবহার করে
    # ------------------------------------------------------------
    title_tag = (
        soup.find("h1")
        or soup.find("h2")
        or soup.find("h3")
        or soup.find("title")
    )
    title = title_tag.get_text(strip=True) if title_tag else None

    # ------------------------------------------------------------
    # 3) Main article container guess
    #    Real site layout অনুযায়ী এগুলো পরে fine-tune করা যাবে
    # ------------------------------------------------------------
    candidates = [
        "div#news-details",
        "div.news-details",
        "div.details",
        "div.details-inner",
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
        logger.debug("kalerkontho: no specific content container found, using full page")
        container = soup

    # ------------------------------------------------------------
    # 4) Extract clean paragraphs
    # ------------------------------------------------------------
    MIN_CHARS = 25
    MAX_CHARS = 1200

    blacklist = [
        "কালের কণ্ঠ",       # paper name (bylines, promos)
        "kalerkontho",
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
        if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
            return None

        lower = text.lower()
        for bad in blacklist:
            if bad in lower:
                # Small footer/promo → drop
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
        logger.debug("kalerkontho: no usable <p>, falling back to stripped_strings")
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
