"""Disk persistence helpers for a key-value store."""

from __future__ import annotations

import json
import os
import tempfile
from threading import Lock
from typing import Any, Dict


class PersistenceManager:
    """Manages loading and saving key-value data to a JSON file on disk.

    All operations are guarded by a process-local lock for thread safety.
    Writes are atomic via temp file + os.replace.
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._lock = Lock()

    def save(self, data: Dict[str, Any]) -> None:
        """Atomically write the given dict to disk as JSON.

        This writes to a temp file in the same directory and then uses
        ``os.replace`` to atomically swap it into place.
        """

        with self._lock:
            directory = os.path.dirname(os.path.abspath(self._file_path)) or "."
            os.makedirs(directory, exist_ok=True)

            fd, tmp_path = tempfile.mkstemp(prefix=".kvstore-", suffix=".tmp", dir=directory)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())

                os.replace(tmp_path, self._file_path)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    # Best-effort cleanup; atomic replace already completed or file vanished.
                    pass

    def load(self) -> Dict[str, Any]:
        """Load and return the persisted dict from disk.

        Returns:
            The loaded dict, or an empty dict if the file does not exist.
        """

        with self._lock:
            if not os.path.exists(self._file_path):
                return {}
            with open(self._file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                return data
            raise ValueError("Persisted data is not a JSON object")

    def clear(self) -> None:
        """Delete the persistence file if it exists."""

        with self._lock:
            try:
                os.remove(self._file_path)
            except FileNotFoundError:
                return

