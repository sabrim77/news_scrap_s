# core/peek_latest.py

import sqlite3
from pathlib import Path
import argparse
from textwrap import shorten

DB_PATH = Path(__file__).resolve().parent.parent / "news.db"


def main():
    parser = argparse.ArgumentParser(
        description="Peek latest news articles with optional filters."
    )
    parser.add_argument(
        "--portal",
        type=str,
        default=None,
        help="Filter by portal (e.g. jagonews24, risingbd, bbc, thedailystar)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Filter by topic (e.g. sports, politics, health, economy, other)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of rows to show (default: 20)",
    )

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    where_clauses = []
    params = []

    if args.portal:
        where_clauses.append("portal = ?")
        params.append(args.portal)

    if args.topic:
        where_clauses.append("topic = ?")
        params.append(args.topic)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT id, portal, topic, title, pub_date
        FROM news
        {where_sql}
        ORDER BY id DESC
        LIMIT ?;
    """
    params.append(args.limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    print("=== Latest news ===")
    if args.portal:
        print(f"Portal filter: {args.portal}")
    if args.topic:
        print(f"Topic filter : {args.topic}")
    print(f"Limit        : {args.limit}")
    print("-" * 80)

    for _id, portal, topic, title, pub_date in rows:
        topic_str = topic or "NULL"
        title_str = title or "(no title)"
        title_short = shorten(title_str, width=60, placeholder="...")

        print(f"[{_id:5}] {portal:12} | {topic_str:12} | {pub_date or '—'}")
        print(f"       {title_short}")
        print("-" * 80)


if __name__ == "__main__":
    main()
