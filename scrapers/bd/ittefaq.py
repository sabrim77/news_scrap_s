# scrapers/bd/ittefaq.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.ittefaq")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Ittefaq article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("ittefaq.parse called with soup=None")
        return {"title": None, "body": None}

    # 1) Unwanted tag cleanup
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, .share, .social, .tags, "
        ".related-news, .related, .ad, .ads, .advertisement"
    ):
        tag.decompose()

    # 2) Title: সাধারণত <h1>, fallback h2/title
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) Main content container guess
    # Ittefaq usually uses some "details" / "content" style wrappers
    candidates = [
        "div#news-details",
        "div.news-details",
        "div.details",
        "div.article-content",
        "div.content",
        "article",
        "main",
    ]

    container: Optional[BeautifulSoup] = None
    for sel in candidates:
        c = soup.select_one(sel)
        if c:
            container = c
            break

    if container is None:
        logger.debug("ittefaq: no specific container found, using whole page")
        container = soup

    MIN_CHARS = 25
    MAX_CHARS = 1200

    cleaned_parts: list[str] = []

    # 4) Prefer <p> tags
    for p in container.find_all("p"):
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
            continue

        lower = text.lower()

        # Common junk / footer / promo lines
        if any(
            bad in lower
            for bad in [
                "ইত্তেফাক",          # paper name
                "ittefaq",
                "আরও পড়ুন",
                "আরও পড়ুন",
                "আরও খবর",
                "read more",
                "সর্বশেষ সংবাদ",
            ]
        ):
            if len(text) < 120:
                # ছোট footer/provo line সরাসরি বাদ
                continue

        cleaned_parts.append(text)

    # 5) Fallback: if no good <p>, use stripped_strings
    if not cleaned_parts:
        logger.debug(
            "ittefaq: no usable <p> content, falling back to stripped_strings"
        )
        for t in container.stripped_strings:
            text = t.strip()
            if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
                continue

            lower = text.lower()
            if any(
                bad in lower
                for bad in [
                    "ইত্তেফাক",
                    "ittefaq",
                    "আরও পড়ুন",
                    "আরও পড়ুন",
                    "আরও খবর",
                    "read more",
                    "সর্বশেষ সংবাদ",
                ]
            ):
                if len(text) < 120:
                    continue

            cleaned_parts.append(text)

    body = "\n\n".join(cleaned_parts).strip() if cleaned_parts else None

    return {
        "title": title,
        "body": body,
    }
