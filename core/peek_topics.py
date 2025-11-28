# core/peek_topics.py

import sqlite3
from pathlib import Path

# news.db is at project root: D:/guru-2/news.db
DB_PATH = Path(__file__).resolve().parent.parent / "news.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT portal, topic, COUNT(*)
        FROM news
        GROUP BY portal, topic
        ORDER BY portal, topic;
    """)

    rows = cur.fetchall()
    conn.close()

    print("=== Topic distribution ===")
    for portal, topic, count in rows:
        # handle NULL topics from older rows
        topic_str = topic or "NULL"
        print(f"{str(portal):12} | {topic_str:12} | {count}")


if __name__ == "__main__":
    main()
