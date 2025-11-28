# scrapers/bd/prothomalo.py

import logging
from typing import Dict, Optional, Any

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.prothomalo")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Prothom Alo article parser.

    Returns:
        {
            "title": str | None,
            "body": str | None,
        }
    """
    if soup is None:
        logger.warning("prothomalo.parse called with soup=None")
        return {"title": None, "body": None}

    # ------------------------------------------------------------
    # 1) Remove unwanted / noisy tags
    # ------------------------------------------------------------
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, "
        ".share, .social, .tags, .related-stories, .related, "
        ".ad, .ads, .advertisement"
    ):
        tag.decompose()

    # ------------------------------------------------------------
    # 2) Title extraction
    # ------------------------------------------------------------
    title_el = (
        soup.select_one("h1")
        or soup.select_one("h1.headline")
        or soup.select_one("h1.title")
        or soup.find("title")
    )
    title = title_el.get_text(strip=True) if title_el else None

    # ------------------------------------------------------------
    # 3) Main content container guess
    #    Prothom Alo uses different layouts over time, so we keep a list.
    # ------------------------------------------------------------
    candidates = [
        "div.story-element",             # typical article body wrapper
        "div.story-elements",
        "div.article-body",
        "div.article-content",
        "article",
        "main",
        "div[class*='content']",
    ]

    container: Optional[BeautifulSoup] = None
    for sel in candidates:
        node = soup.select_one(sel)
        if node:
            container = node
            break

    if container is None:
        logger.debug("prothomalo: no specific container found, using full page")
        container = soup

    # ------------------------------------------------------------
    # 4) Extract clean paragraphs with filters
    # ------------------------------------------------------------
    MIN_CHARS = 25
    MAX_CHARS = 1400

    blacklist = [
        "প্রথম আলো",        # paper name
        "prothom alo",
        "prothomalo.com",
        "আরও পড়ুন",
        "আরও পড়ুন",
        "আরও খবর",
        "read more",
        "সর্বশেষ সংবাদ",
        "টপ নিউজ",
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
                # ছোট footer/promo হলে ড্রপ করবো
                if len(text) < 120:
                    return None
        return text

    body_parts: list[str] = []

    # Primary pass: paragraphs from likely container(s)
    for p in container.find_all("p"):
        txt = p.get_text(" ", strip=True)
        cleaned = clean_text(txt)
        if cleaned:
            body_parts.append(cleaned)

    # Fallback 1: if no content from <p>, scan article/main
    if not body_parts:
        logger.debug("prothomalo: no usable <p>, trying article/main paragraphs")
        for p in soup.select("article p, main p"):
            txt = p.get_text(" ", strip=True)
            cleaned = clean_text(txt)
            if cleaned:
                body_parts.append(cleaned)

    # Fallback 2: generic content divs
    if not body_parts:
        logger.debug("prothomalo: still empty, trying generic content containers")
        for p in soup.select("div[class*='content'] p"):
            txt = p.get_text(" ", strip=True)
            cleaned = clean_text(txt)
            if cleaned:
                body_parts.append(cleaned)

    # Fallback 3: stripped_strings
    if not body_parts:
        logger.debug("prothomalo: final fallback to stripped_strings")
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
