# scrapers/bd/banglanews24.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.banglanews24")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Parse a BanglaNews24 article page into title + body.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("banglanews24.parse called with soup=None")
        return {"title": None, "body": None}

    # 1) Remove global junk elements
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        ".share, .tags, .related-news, .related, "
        ".social-share, .ad, .ads, .advertisement"
    ):
        tag.decompose()

    # 2) Title
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) Main content candidates
    candidates = [
        "div.details",
        "div.details-content",
        "div.news-details",
        "div.article-content",
        "div#content",
        "div.content",
    ]

    container = None
    for sel in candidates:
        c = soup.select_one(sel)
        if c:
            container = c
            break

    if container is None:
        logger.debug("banglanews24: no specific content container, using full soup")
        container = soup

    MIN_CHARS = 25

    cleaned_parts: list[str] = []

    # First attempt: <p> elements
    for p in container.find_all("p"):
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        if len(text) < MIN_CHARS:
            continue

        if any(
            bad in text
            for bad in [
                "বাংলানিউজ২৪.কম",
                "বাংলা নিউজ",
                "আরও পড়ুন",
                "আরও পড়ুন",
                "আরও খবর",
            ]
        ):
            if len(text) < 80:
                continue

        cleaned_parts.append(text)

    # Fallback: stripped_strings if <p> based extraction fails
    if not cleaned_parts:
        logger.debug("banglanews24: no content from <p>, using stripped_strings")
        for text in container.stripped_strings:
            text = text.strip()
            if len(text) < MIN_CHARS:
                continue
            if any(
                bad in text
                for bad in [
                    "বাংলানিউজ২৪.কম",
                    "বাংলা নিউজ",
                    "আরও পড়ুন",
                    "আরও পড়ুন",
                ]
            ):
                if len(text) < 80:
                    continue
            cleaned_parts.append(text)

    body: Optional[str]
    if cleaned_parts:
        body = "\n\n".join(cleaned_parts).strip()
    else:
        body = None

    return {"title": title, "body": body}
