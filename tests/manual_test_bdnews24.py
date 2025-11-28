# tests/manual_test_bdnews24_list.py

import logging
from scrapers.base.base_scraper import BaseScraper

logging.basicConfig(level=logging.DEBUG)

def main():
    url = "https://bangla.bdnews24.com/world"
    scraper = BaseScraper()
    soup = scraper.fetch_html(url)

    if soup is None:
        print("❌ Could not fetch section page (maybe blocked)")
        return

    print("✅ Section page fetched OK")

    # কিছু article link বের করি
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # শুধু news-type লিঙ্ক ধরার জন্য simple filter (চাইলে refine করতে পারো)
        if "/world/" in href or "/bangladesh/" in href:
            if href.startswith("/"):
                href = "https://bangla.bdnews24.com" + href
            if href.startswith("https://bangla.bdnews24.com"):
                links.append(href)

    links = list(dict.fromkeys(links))  # unique

    print(f"Found {len(links)} candidate article links")
    for i, link in enumerate(links[:5], start=1):
        print(f"[{i}] {link}")

if __name__ == "__main__":
    main()
