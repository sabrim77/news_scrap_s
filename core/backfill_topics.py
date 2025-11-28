# core/backfill_topics.py

import sqlite3
from pathlib import Path

from core.topic_classifier import classify_topic  # adjust if your package name differs

DB_PATH = Path(__file__).resolve().parent.parent / "news.db"


def backfill_topics(batch_size: int = 200):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # For convenience, we’ll run in small batches
    while True:
        cur.execute("""
            SELECT id, portal, url, title, content
            FROM news
            WHERE topic IS NULL
            LIMIT ?;
        """, (batch_size,))

        rows = cur.fetchall()
        if not rows:
            break  # nothing left to backfill

        print(f"Processing batch of {len(rows)} rows...")

        for row in rows:
            _id, portal, url, title, content = row
            portal = portal or ""
            url = url or ""
            title = title or ""
            body = content or ""

            topic = classify_topic(
                portal=portal,
                url=url,
                title=title,
                body=body,
            )

            # Update this row
            cur.execute(
                "UPDATE news SET topic = ? WHERE id = ?",
                (topic, _id),
            )

        conn.commit()
        print("Batch committed.")

    conn.close()
    print("Backfill complete.")


if __name__ == "__main__":
    backfill_topics(batch_size=200)
