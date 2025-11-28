# scrapers/bd/bdnews24.py

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("scraper.bdnews24")


def parse(soup: Optional[BeautifulSoup]) -> Dict[str, Optional[str]]:
    """
    Parse a bdnews24.com article page into title + body text.

    Returns:
        {
            "title": str | None,
            "body": str | None
        }
    """
    # Safety: soup None হলে সরাসরি empty
    if soup is None:
        logger.warning("bdnews24.parse called with soup=None")
        return {"title": None, "body": None}

    # 1) অপ্রয়োজনীয় ট্যাগ clean করা
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, .share, .social, .tags, "
        ".related, .related-stories, .ad, .ads, .advertisement"
    ):
        tag.decompose()

    # 2) Title (bdnews24 সাধারণত <h1>)
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) সম্ভাব্য article container গুলো
    # bdnews24 এ সাধারণত <article>, <main> + কিছু wrapper div থাকে
    candidates = [
        "article",
        "main",
        "div.story__content",        # সম্ভাব্য নতুন layout
        "div.story-content",         # পুরনো/কমন pattern
        "div.article-content",
        "div#content",
    ]

    container: Optional[BeautifulSoup] = None
    for sel in candidates:
        c = soup.select_one(sel)
        if c:
            container = c
            break

    # কিছুই না পেলে fallback: পুরো পেজ
    if container is None:
        logger.debug("bdnews24: no specific container found, using whole page")
        container = soup

    # 4) Paragraph → main body
    MIN_WORDS = 5       # খুব ছোট junk বাদ
    MAX_WORDS = 200     # অস্বাভাবিক বড় footer/terms বাদ

    body_parts: list[str] = []
    for p in container.find_all("p"):
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        words = text.split()
        if not (MIN_WORDS <= len(words) <= MAX_WORDS):
            continue

        lower = text.lower()

        # Typical bdnews24 junk / credits / follow lines ফিল্টার
        if (
            "bdnews24.com" in lower and len(words) < 12
            or lower.startswith("follow bdnews24.com")
            or lower.startswith("follow us on")
        ):
            continue

        body_parts.append(text)

    body_text = " ".join(body_parts).strip()

    return {
        "title": title,
        "body": body_text if body_text else None,
    }
