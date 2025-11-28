# scrapers/bd/deshrupantor.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.deshrupantor")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Desh Rupantor article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("deshrupantor.parse called with soup=None")
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
    candidates = [
        "div#news-details",
        "div.news-details",
        "div.details",
        "div.article-body",
        "div.article-content",
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
        logger.debug("deshrupantor: no specific container found, using whole page")
        container = soup

    MIN_CHARS = 25
    MAX_CHARS = 1200  # guard against giant junk blocks

    cleaned_parts: list[str] = []

    # First preference: <p> tags
    for p in container.find_all("p"):
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
            continue

        lower = text.lower()

        # Common junk / footer filters
        if any(
            bad in lower
            for bad in [
                "দেশ রূপান্তর",          # newspaper name, bylines, etc.
                "desh rupantor",
                "আরও পড়ুন",
                "আরও পড়ুন",
                "আরও খবর",
                "read more",
            ]
        ):
            # ছোট footer হলে পুরোপুরি বাদ দেই
            if len(text) < 120:
                continue

        cleaned_parts.append(text)

    # Fallback: no good <p> → use stripped_strings
    if not cleaned_parts:
        logger.debug(
            "deshrupantor: no content from <p>, falling back to stripped_strings"
        )
        for t in container.stripped_strings:
            text = t.strip()
            if len(text) < MIN_CHARS or len(text) > MAX_CHARS:
                continue

            lower = text.lower()
            if any(
                bad in lower
                for bad in [
                    "দেশ রূপান্তর",
                    "desh rupantor",
                    "আরও পড়ুন",
                    "আরও পড়ুন",
                    "আরও খবর",
                    "read more",
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
