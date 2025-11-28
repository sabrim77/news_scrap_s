# core/db.py
"""
Database Layer for News Scraper System
--------------------------------------

Features:
    ✓ Automatic schema creation
    ✓ WAL mode (fast + safe)
    ✓ Singleton DB connection
    ✓ Safe parameterized queries
    ✓ Fully typed (Pylance/mypy friendly)
    ✓ Helper methods (insert, exists, count, get_latest, get_by_url)
"""

from __future__ import annotations

import sqlite3
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

log = logging.getLogger("db")


# -----------------------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "news.db"


# -----------------------------------------------------------------------------
# SINGLETON CONNECTION
# -----------------------------------------------------------------------------

_CONN: Optional[sqlite3.Connection] = None


def _get_conn() -> sqlite3.Connection:
    """
    Return singleton SQLite connection with WAL mode enabled.
    Thread-safe for Python (check_same_thread=False).
    """
    global _CONN

    if _CONN is not None:
        return _CONN

    try:
        conn = sqlite3.connect(
            DB_PATH,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )

        conn.row_factory = sqlite3.Row  # return dict-like rows

        # WAL mode → fastest/safest for scrapers
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA foreign_keys = ON;")

        _CONN = conn
        log.info("SQLite connection created at %s", DB_PATH)
        return conn

    except Exception as exc:
        log.exception("DB connection failed: %s", exc)
        raise


# -----------------------------------------------------------------------------
# SCHEMA
# -----------------------------------------------------------------------------

def init_db() -> None:
    """
    Create database schema if needed. Safe to call multiple times.
    """
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            title TEXT,
            content TEXT,
            topic TEXT,
            pub_date TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )

    conn.commit()
    log.info("DB initialized & schema ensured.")


# -----------------------------------------------------------------------------
# INSERT
# -----------------------------------------------------------------------------

def insert_news(
    portal: str,
    url: str,
    title: Optional[str],
    content: Optional[str],
    topic: Optional[str] = None,
    pub_date: Optional[str] = None,
) -> bool:
    """
    Insert a new article.
    Returns:
        True  → row inserted
        False → duplicate URL (ignored)
    """
    conn = _get_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO news (portal, url, title, content, topic, pub_date)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (portal, url, title, content, topic, pub_date),
        )
        conn.commit()

        inserted = cur.rowcount > 0
        if inserted:
            log.info("Inserted: %s (%s)", url, portal)
        else:
            log.debug("Duplicate skipped: %s", url)

        return inserted

    except Exception as exc:
        log.exception("Insert failed for %s: %s", url, exc)
        return False


# -----------------------------------------------------------------------------
# READ OPERATIONS
# -----------------------------------------------------------------------------

def exists(url: str) -> bool:
    """Return True if URL already stored."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM news WHERE url = ? LIMIT 1", (url,))
    return cur.fetchone() is not None


def get_by_url(url: str) -> Optional[Dict[str, Any]]:
    """Return article as dict."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM news WHERE url = ?", (url,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_latest(limit: int = 50) -> List[Dict[str, Any]]:
    """Return latest N articles."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT * FROM news
        ORDER BY id DESC
        LIMIT ?;
        """,
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


def count() -> int:
    """Total article count."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM news;")
    row = cur.fetchone()

    return int(row["c"]) if row else 0


# -----------------------------------------------------------------------------
# SHUTDOWN
# -----------------------------------------------------------------------------

def close() -> None:
    """Close DB connection (optional)."""
    global _CONN

    if _CONN is not None:
        _CONN.close()
        log.info("DB connection closed.")
        _CONN = None
