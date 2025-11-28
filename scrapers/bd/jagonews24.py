# scrapers/bd/jagonews24.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.jagonews24")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    JagoNews24 article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    if soup is None:
        logger.warning("jagonews24.parse called with soup=None")
        return {"title": None, "body": None}

    # ------------------------------------------------------------
    # 1) Remove unwanted tags
    # ------------------------------------------------------------
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, "
        ".share, .tags, .related-news, .related, "
        ".social-share, .ad, .ads, .advertisement"
    ):
        tag.decompose()

    # ------------------------------------------------------------
    # 2) Title extraction
    # ------------------------------------------------------------
    title_tag = (
        soup.find("h1")
        or soup.find("h2")
        or soup.find("h3")
        or soup.find("title")
    )
    title = title_tag.get_text(strip=True) if title_tag else None

    # ------------------------------------------------------------
    # 3) Identify the correct article content container
    # ------------------------------------------------------------
    candidates = [
        "div.details",
        "div.details-content",
        "div.article-content",
        "div#content",
        "div#myText",
        "div.news-details",
        "div.details-inner",
        "article",
        "main",
    ]

    container = None
    for sel in candidates:
        block = soup.select_one(sel)
        if block:
            container = block
            break

    if container is None:
        logger.debug("jagonews24: no specific content container found → using full page")
        container = soup

    # ------------------------------------------------------------
    # 4) Extract paragraphs
    # ------------------------------------------------------------
    MIN_CHARS = 25
    MAX_CHARS = 1200

    blacklist = [
        "জাগোনিউজ২৪.কম",
        "আরও পড়ুন",
        "আরও পড়ুন",
        "আরও খবর",
        "সর্বোচ্চ পঠিত",
        "সর্বশেষ",
    ]

    def clean_text(text: str) -> Optional[str]:
        """Apply filtering rules."""
        if not text or len(text) < MIN_CHARS or len(text) > MAX_CHARS:
            return None

        lower = text.lower()

        for bad in blacklist:
            if bad in lower:
                # Short promo/footer → skip
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

    # Fallback if no paragraphs
    if not body_parts:
        logger.debug("jagonews24: <p> extraction empty → fallback to stripped_strings")
        for raw in container.stripped_strings:
            txt = raw.strip()
            cleaned = clean_text(txt)
            if cleaned:
                body_parts.append(cleaned)

    # ------------------------------------------------------------
    # 5) Join into final body text
    # ------------------------------------------------------------
    body = "\n\n".join(body_parts).strip() if body_parts else None

    return {
        "title": title,
        "body": body,
    }
