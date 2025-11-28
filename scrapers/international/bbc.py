# scrapers/international/bbc.py

def parse(soup):
    """
    Parse a BBC news article page into title + body text.

    Returns:
        {"title": str|None, "body": str|None}
    """
    # Safety: soup None হলে সরাসরি empty রিটার্ন
    if soup is None:
        return {"title": None, "body": None}

    # 1) অপ্রয়োজনীয় ট্যাগ clean করা
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, .share, .social, .promo, .tags"
    ):
        tag.decompose()

    # 2) Title (BBC সাধারণত <h1> ব্যবহার করে)
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) Article container খোঁজা
    # BBC প্রায়ই <article>, <main>, বা বিশেষ wrapper div ব্যবহার করে
    article_container = (
        soup.find("article")
        or soup.find("main")
        or soup.select_one("div.ssrcss-1072xwf-ArticleWrapper")
        or soup
    )

    # 4) Paragraph সংগ্রহ
    paragraphs = article_container.find_all("p")

    MIN_WORDS = 5       # খুব ছোট junk বাদ
    MAX_WORDS = 200     # absurdly বড় footer/terms বাদ

    cleaned_parts = []
    for p in paragraphs:
        text = p.get_text(" ", strip=True)
        words = text.split()

        # খুব ছোট / খুব বড় junk বাদ
        if not (MIN_WORDS <= len(words) <= MAX_WORDS):
            continue

        # BBC specific common junk lines
        lower = text.lower()
        if (
            "bbc" in lower and len(words) < 8   # "BBC News", "BBC Sport" ইত্যাদি
            or lower.startswith("image caption")
            or lower.startswith("video caption")
        ):
            continue

        cleaned_parts.append(text)

    body_text = " ".join(cleaned_parts).strip()

    return {
        "title": title,
        "body": body_text if body_text else None,
    }
