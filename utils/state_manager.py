# utils/state_manager.py
"""
State Manager
-------------

Purpose:
    - Track which article URLs have already been processed
    - Avoid re-scraping the same articles
    - Persist "seen" state between runs

Implementation:
    - Uses MD5 hash of URL for compact storage
    - Stores hashes in JSON file at: <project_root>/data/seen.json
"""

from __future__ import annotations

import json
import hashlib
import logging
from pathlib import Path
from typing import Set

logger = logging.getLogger("state_manager")

# ---------------------------------------------------------------------------
# File location: <project_root>/data/seen.json
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT_ROOT / "data" / "seen.json"
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


class StateManager:
    """Manage seen URLs using hashed storage in a JSON file."""

    def __init__(self, state_file: Path = STATE_FILE) -> None:
        self.state_file: Path = Path(state_file)
        self._seen: Set[str] = self._load()

    # ---------------- Internal helpers ---------------- #

    def _load(self) -> Set[str]:
        """Load seen hashes from file. Returns an empty set if missing/invalid."""
        if not self.state_file.exists():
            logger.info("State file not found, starting fresh: %s", self.state_file)
            return set()

        try:
            with self.state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                logger.warning("State file format invalid, resetting: %s", self.state_file)
                return set()
            return set(data)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "Failed to load state file %s (%s). Starting with empty state.",
                self.state_file,
                exc,
            )
            return set()

    def _save(self) -> None:
        """Save current seen set to file."""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with self.state_file.open("w", encoding="utf-8") as f:
                json.dump(sorted(self._seen), f, ensure_ascii=False, indent=2)
        except OSError as exc:
            logger.error("Failed to save state file %s (%s)", self.state_file, exc)

    @staticmethod
    def _hash(url: str) -> str:
        """Return MD5 hash of a URL."""
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    # ---------------- Public API ---------------- #

    def seen(self, url: str) -> bool:
        """Check if URL has been seen before."""
        return self._hash(url) in self._seen

    def mark_seen(self, url: str) -> None:
        """
        Mark URL as seen and save immediately.

        Note: For extremely high throughput, you could batch and call a
        separate `flush()` method. For your current scale, immediate save
        keeps things simple and robust.
        """
        h = self._hash(url)
        if h in self._seen:
            return
        self._seen.add(h)
        self._save()
        logger.debug("Marked seen: %s", url)


# Module-level convenience functions (backwards compatible)
_manager = StateManager()

seen = _manager.seen
mark_seen = _manager.mark_seen
