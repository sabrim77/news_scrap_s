# tests/manual_test_risingbd.py

from scrapers.base.base_scraper import BaseScraper
from scrapers.bd import risingbd

URL = "https://www.risingbd.com/sports/news/630317"  # বা অন্য কোনো article

bs = BaseScraper()
soup = bs.fetch_html(URL)
data = risingbd.parse(soup)

print("TITLE:", repr(data.get("title")))
body = data.get("body") or ""
print("BODY_LEN:", len(body))
print("BODY_PREVIEW:", body[:600])
