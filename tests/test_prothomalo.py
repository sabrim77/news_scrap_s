# tests/test_prothomalo.py

from core.article_fetcher import fetch_article_soup
from scrapers.bd.prothomalo import parse


# 🔁 Replace this with ANY real Prothom Alo article URL
URL = "https://www.prothomalo.com/bangladesh/..."

PORTAL = "prothomalo"


def main():
    soup = fetch_article_soup(PORTAL, URL)
    print("Soup is None?", soup is None)

    if soup is None:
        return

    parsed = parse(soup)
    title = parsed.get("title")
    body = parsed.get("body")

    print("\n=== TITLE ===")
    print(title)

    print("\n=== BODY PREVIEW (first 800 chars) ===")
    if body:
        print(body[:800])
    else:
        print("(no body)")


if __name__ == "__main__":
    main()
