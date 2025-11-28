# scrapers/international/thedailystar.py

def parse(soup):
    """
    Parse a The Daily Star news article page into title + body text.

    Returns:
        {"title": str|None, "body": str|None}
    """
    if soup is None:
        return {"title": None, "body": None}

    # 1) অপ্রয়োজনীয় জিনিসগুলো কেটে দিই
    for tag in soup.select(
        "script, style, noscript, iframe, aside, "
        "header nav, footer, .share, .social, .tags, .related-stories"
    ):
        tag.decompose()

    # 2) Title: সাধারণত <h1>
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    # 3) Article container খুঁজে বের করা
    # The Daily Star কনটেন্ট অনেক সময় .field-item / articleBody / article etc.-এ থাকে
    article_container = (
        soup.find("div", attrs={"itemprop": "articleBody"})
        or soup.find("div", class_="field-item")
        or soup.find("div", class_="content")
        or soup.find("article")
        or soup.find("main")
        or soup
    )

    # 4) Paragraph গুলো তুলি
    paragraphs = article_container.find_all("p")

    MIN_WORDS = 4      # খুব ছোট লাইন বাদ
    MAX_WORDS = 180    # খুব বড় footer/junk বাদ

    cleaned_parts = []
    for p in paragraphs:
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        words = text.split()

        if not (MIN_WORDS <= len(words) <= MAX_WORDS):
            continue

        lower = text.lower()

        # Common junk filter (byline, credit, etc.)
        if (
            "the daily star" in lower and len(words) < 10
            or lower.startswith("photo:")
            or lower.startswith("photos:")
            or lower.startswith("read more")
        ):
            continue

        cleaned_parts.append(text)

    body_text = " ".join(cleaned_parts).strip()

    return {
        "title": title,
        "body": body_text if body_text else None,
    }
