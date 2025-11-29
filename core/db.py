# core/db.py
"""
SQLite persistence layer for the news scraping and search system.

Responsibilities:
- Manage a singleton SQLite connection with sensible pragmas.
- Create and maintain the core `news` table.
- Create and keep in sync the FTS5 virtual table `news_fts`.
- Provide insert and search helpers used by the rest of the application.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------

log = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Global paths & connection
# --------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "news.db"

_CONN: Optional[sqlite3.Connection] = None


# --------------------------------------------------------------------
# Connection & schema management
# --------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """
    Return a singleton SQLite connection with WAL mode enabled.

    The connection is created on first use and cached in `_CONN`.
    `check_same_thread=False` is used so the connection can be shared
    across threads in this process (Python-level thread safety).
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
        conn.row_factory = sqlite3.Row

        # Pragmas for performance + durability
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


def _create_base_schema(conn: sqlite3.Connection) -> None:
    """
    Create the core 'news' table and supporting indexes if they don't exist.

    Table: news
        - id         INTEGER PRIMARY KEY AUTOINCREMENT
        - portal     TEXT NOT NULL           (e.g. 'prothomalo', 'bdnews24')
        - url        TEXT NOT NULL UNIQUE    (article URL)
        - title      TEXT
        - content    TEXT                    (full text or summary)
        - topic      TEXT                    (keyword/topic used when fetching)
        - pub_date   TEXT                    (ISO datetime string)
        - created_at TEXT                    (insert timestamp, default now)
    """
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

    # Optional index to speed portal/date queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_portal_pubdate
        ON news(portal, pub_date);
        """
    )

    conn.commit()


def _fts_available(conn: sqlite3.Connection) -> bool:
    """
    Check if FTS5 is available in this SQLite build.

    Returns:
        True  → FTS5 virtual tables can be created.
        False → FTS5 not available; the system will fall back to LIKE search.
    """
    try:
        _ = conn.execute("SELECT sqlite_version();").fetchone()

        # Probe FTS5 availability using a temporary virtual table
        cur = conn.cursor()
        cur.execute("CREATE VIRTUAL TABLE temp.__fts_probe USING fts5(x);")
        cur.execute("DROP TABLE temp.__fts_probe;")
        return True

    except sqlite3.OperationalError as exc:
        log.warning("FTS5 not available: %s", exc)
        return False
    except Exception as exc:
        log.warning("Unexpected error while probing FTS5: %s", exc)
        return False


def _create_fts_schema(conn: sqlite3.Connection) -> None:
    """
    Create the FTS5 virtual table and triggers synced to 'news'.

    Implementation details:
        - Uses content='news' and content_rowid='id' to mirror the base table.
        - Triggers (INSERT/UPDATE/DELETE) keep `news_fts` in sync.
        - On first run, performs a one-time backfill from `news`.
    """
    if not _fts_available(conn):
        return

    cur = conn.cursor()

    # FTS virtual table
    cur.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS news_fts
        USING fts5(
            title,
            content,
            topic,
            portal,
            url,
            content='news',
            content_rowid='id',
            tokenize='unicode61'
        );
        """
    )

    # Triggers to keep FTS in sync with 'news'
    cur.executescript(
        """
        CREATE TRIGGER IF NOT EXISTS news_ai AFTER INSERT ON news BEGIN
            INSERT INTO news_fts(rowid, title, content, topic, portal, url)
            VALUES (new.id, new.title, new.content, new.topic, new.portal, new.url);
        END;

        CREATE TRIGGER IF NOT EXISTS news_ad AFTER DELETE ON news BEGIN
            INSERT INTO news_fts(news_fts, rowid, title, content, topic, portal, url)
            VALUES('delete', old.id, old.title, old.content, old.topic, old.portal, old.url);
        END;

        CREATE TRIGGER IF NOT EXISTS news_au AFTER UPDATE ON news BEGIN
            INSERT INTO news_fts(news_fts, rowid, title, content, topic, portal, url)
            VALUES('delete', old.id, old.title, old.content, old.topic, old.portal, old.url);
            INSERT INTO news_fts(rowid, title, content, topic, portal, url)
            VALUES (new.id, new.title, new.content, new.topic, new.portal, new.url);
        END;
        """
    )

    # Backfill once if empty
    cur.execute("SELECT count(*) AS c FROM news_fts;")
    if int(cur.fetchone()["c"]) == 0:
        log.info("FTS backfill starting...")
        cur.execute(
            """
            INSERT INTO news_fts(rowid, title, content, topic, portal, url)
            SELECT id, title, content, topic, portal, url FROM news;
            """
        )
        log.info("FTS backfill done.")

    conn.commit()


def init_db() -> None:
    """
    Initialize the database if needed.

    This function is safe to call multiple times. It ensures:
        - Base `news` table and indexes exist.
        - FTS5 virtual table and triggers exist (if FTS5 is available).
    """
    conn = _get_conn()
    _create_base_schema(conn)
    _create_fts_schema(conn)
    log.info("DB initialized & schema ensured.")


# --------------------------------------------------------------------
# INSERT operations
# --------------------------------------------------------------------

def insert_news(
    portal: str,
    url: str,
    title: Optional[str],
    content: Optional[str],
    topic: Optional[str] = None,
    pub_date: Optional[str] = None,
) -> bool:
    """
    Insert a single article row into `news`.

    Args:
        portal:   Portal identifier (e.g. 'prothomalo', 'bdnews24').
        url:      Unique URL of the article.
        title:    Title/headline text (optional).
        content:  Article body or summary (optional).
        topic:    Topic/keyword associated with this article (optional).
        pub_date: Publication datetime as ISO string (optional).

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


def insert_articles(articles: List[Dict[str, Any]]) -> int:
    """
    Batch insert for a list of article dicts.

    Expected keys in each article dict (not all required):

        Core fields:
            - "title":         str | None
            - "url":           str (required; used as UNIQUE key)
            - "content":       str | None   (full body if available)
            - "summary":       str | None   (fallback if "content" is missing)

        Source / metadata:
            - "source":        str | None   → stored as `portal`
            - "portal":        str | None   → alternative for `source`
            - "keyword":       str | None   → stored as `topic`
            - "topic":         str | None   → alternative way to set `topic`

        Publication time:
            - "published_at":  datetime | str | None
            - "pub_date":      datetime | str | None

    Mapping to DB columns:
        - portal     ← article["source"] or article["portal"] or "unknown"
        - url        ← article["url"]
        - title      ← article["title"]
        - content    ← article["content"] or article["summary"]
        - topic      ← article["keyword"] or article["topic"]
        - pub_date   ← ISO string derived from `published_at` / `pub_date`

    Returns:
        Number of newly inserted rows (duplicates are ignored).
    """
    if not articles:
        return 0

    conn = _get_conn()
    cur = conn.cursor()
    inserted = 0

    for a in articles:
        url = a.get("url")
        if not url:
            # Skip entries without URL: nothing to key on, nothing to de-duplicate.
            continue

        portal = a.get("source") or a.get("portal") or "unknown"
        title = a.get("title")
        content = a.get("content") or a.get("summary")
        topic = a.get("keyword") or a.get("topic")

        pub_date = a.get("pub_date") or a.get("published_at")
        if pub_date is not None and not isinstance(pub_date, str):
            # Try to convert datetime → ISO string
            try:
                pub_date = pub_date.isoformat()  # type: ignore[attr-defined]
            except Exception:
                pub_date = str(pub_date)

        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO news (portal, url, title, content, topic, pub_date)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (portal, url, title, content, topic, pub_date),
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception as exc:
            log.exception("Batch insert failed for %s: %s", url, exc)

    conn.commit()
    log.info("Batch insert: %s/%s new rows", inserted, len(articles))
    return inserted


# --------------------------------------------------------------------
# READ operations
# --------------------------------------------------------------------

def exists(url: str) -> bool:
    """
    Check if a given URL already exists in `news`.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM news WHERE url = ? LIMIT 1;", (url,))
    return cur.fetchone() is not None


def get_by_url(url: str) -> Optional[Dict[str, Any]]:
    """
    Return a single article row by URL as a dict, or None if not found.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM news WHERE url = ?;", (url,))
    row = cur.fetchone()
    return dict(row) if row else None


def get_latest(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Return the latest N rows (ordered by id DESC).
    """
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


def get_latest_by_topic(
    topic: str,
    portal: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Return the latest N rows for a given topic (keyword) stored in `news.topic`.

    Args:
        topic:   The keyword/topic string used when fetching (e.g. "বাংলাদেশ").
        portal:  Optional portal filter (e.g. "prothomalo", "bdnews24").
                 If None, results from all portals are returned.
        limit:   Maximum number of rows to return.

    Returns:
        List of row dicts ordered by newest first.
    """
    topic = (topic or "").strip()
    if not topic:
        return []

    conn = _get_conn()
    cur = conn.cursor()

    if portal:
        cur.execute(
            """
            SELECT *
            FROM news
            WHERE topic = ? AND portal = ?
            ORDER BY id DESC
            LIMIT ?;
            """,
            (topic, portal, limit),
        )
    else:
        cur.execute(
            """
            SELECT *
            FROM news
            WHERE topic = ?
            ORDER BY id DESC
            LIMIT ?;
            """,
            (topic, limit),
        )

    return [dict(r) for r in cur.fetchall()]


def count() -> int:
    """
    Return total number of rows in `news`.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM news;")
    row = cur.fetchone()
    return int(row["c"]) if row else 0


# --------------------------------------------------------------------
# SEARCH operations
# --------------------------------------------------------------------

def _build_match_query(
    terms: List[str],
    strategy: str,
    proximity: int,
) -> str:
    """
    Build FTS5 MATCH expression from individual terms.

    strategy:
        - 'phrase' → entire query as a single phrase
        - 'near'   → tokens joined with NEAR/k
        - 'any'    → OR between terms
        - 'all'    → AND between terms
    """
    esc = lambda t: t.replace('"', '""')  # escape quotes by doubling

    if not terms:
        return ""

    if strategy in ("phrase",):
        return '"' + " ".join(esc(t) for t in terms) + '"'

    if strategy in ("near",):
        if len(terms) == 1:
            return esc(terms[0])
        seq: List[str] = []
        for i, t in enumerate(terms):
            seq.append(esc(t))
            if i < len(terms) - 1:
                seq.append("NEAR/" + str(max(1, proximity)))
        return " ".join(seq)

    # 'any' (OR) and 'all' / 'auto' (AND)
    op = " OR " if strategy == "any" else " AND "
    return op.join(esc(t) for t in terms)


def _like_params(terms: List[str], any_mode: bool) -> Tuple[str, List[str]]:
    """
    Fallback LIKE query builder (if FTS5 not available).

    Matches title/content with %term%.

    Returns:
        where_clause, params
    """
    if not terms:
        return "1=1", []

    clauses: List[str] = []
    params: List[str] = []

    for t in terms:
        clauses.append("(title LIKE ? OR content LIKE ?)")
        pat = f"%{t}%"
        params.extend([pat, pat])

    where = (" OR " if any_mode else " AND ").join(clauses)
    return where, params


def search_news(
    query: str,
    strategy: str = "auto",
    proximity: int = 1,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Keyword/phrase search over `news`.

    Args:
        query:     Raw user query string.
        strategy:  Search strategy:
                     - "auto"  : AND over tokens; quoted phrases honored (ANDed)
                     - "all"   : AND over tokens
                     - "any"   : OR over tokens
                     - "phrase": exact phrase match
                     - "near"  : tokens within k words (NEAR/proximity)
        proximity: Used when strategy="near" (default=1 = side-by-side)
        limit:     Maximum number of rows to return.

    Returns:
        List of row dicts ordered by newest first.
    """
    conn = _get_conn()
    cur = conn.cursor()

    q = (query or "").strip()
    if not q:
        return []

    import re

    def _fts_escape(s: str) -> str:
        return s.replace('"', '""')

    # Extract phrases in quotes; remainder split on whitespace
    phrases = [m.strip('"') for m in re.findall(r'"([^"]+)"', q)]
    remainder = re.sub(r'"[^"]+"', " ", q).strip()
    tokens = [t for t in re.split(r"\s+", remainder) if t] if remainder else []

    if strategy == "auto":
        # Phrases AND tokens; phrases kept quoted
        parts: List[str] = []
        for ph in phrases:
            parts.append('"' + _fts_escape(ph) + '"')
        if tokens:
            parts.append(_build_match_query(tokens, "all", proximity))
        match_expr = (
            " AND ".join(parts) if parts else _build_match_query(tokens, "all", proximity)
        )

    elif strategy == "phrase":
        # Exact phrase over the whole query string
        match_expr = '"' + _fts_escape(q) + '"'

    else:
        # 'any' | 'all' | 'near'
        terms: List[str] = phrases if (strategy == "near" and phrases) else (tokens or phrases)
        match_expr = _build_match_query(terms, strategy, proximity)

    # Try FTS first
    try:
        cur.execute(
            """
            SELECT n.*
            FROM news_fts
            JOIN news n ON n.id = news_fts.rowid
            WHERE news_fts MATCH ?
            ORDER BY n.id DESC
            LIMIT ?;
            """,
            (match_expr, limit),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    except sqlite3.OperationalError as exc:
        log.warning("FTS search failed (%s), falling back to LIKE", exc)

    # LIKE fallback (if FTS not available or MATCH failed)
    any_mode = strategy == "any"
    where, params = _like_params(phrases + tokens, any_mode)

    cur.execute(
        f"""
        SELECT *
        FROM news
        WHERE {where}
        ORDER BY id DESC
        LIMIT ?;
        """,
        (*params, limit),
    )
    return [dict(r) for r in cur.fetchall()]


# --------------------------------------------------------------------
# Shutdown
# --------------------------------------------------------------------

def close() -> None:
    """
    Close the SQLite connection (optional).

    Safe to call multiple times. After closing, a new connection will be
    created automatically on the next call to any DB helper.
    """
    global _CONN
    if _CONN is not None:
        try:
            _CONN.close()
            log.info("DB connection closed.")
        finally:
            _CONN = None
