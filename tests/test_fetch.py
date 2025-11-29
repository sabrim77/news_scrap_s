# tests/test_fetch.py

from core import db, fetch


def run_one_query(raw_query: str) -> None:
    print("=" * 80)
    print(f"Fetching news for query: {raw_query!r}")

    result = fetch.fetch_news_for_query(
        raw_query,
        lang="bangla",   # Bangla BD portals; set to None,None if you want ALL
        country="BD",
    )

    print("Normalized keywords:", result["keywords"])
    print("Total fetched:", result["total_fetched"], "\n")

    for kw, articles in result["by_keyword"].items():
        print(f"--- keyword={kw!r} ({len(articles)} articles) ---")
        for idx, a in enumerate(articles[:5], start=1):
            print(f"[{idx}] {a['source']} | {a['title']}")
            print(f"     {a['url']}")
        print()

    print("DB rows now:", db.count())
    print()


def main() -> None:
    db.init_db()

    # 1) Full sentence
    run_one_query("দেশে ফেরার সিদ্ধান্ত")

    # 2) Comma-separated multiple keywords
    run_one_query("তারেক,রহমান")

    # 3) Single phrase
    run_one_query("তারেক রহমান")


if __name__ == "__main__":
    main()
