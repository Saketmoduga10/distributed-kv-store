"""Simple in-memory key-value store."""

from threading import Lock
from typing import Any, Dict, Generic, Optional, TypeVar, cast

from persistence import PersistenceManager

K = TypeVar("K")
V = TypeVar("V")


class InMemoryKVStore(Generic[K, V]):
    """In-memory mapping from keys to values, protected by a lock for thread safety."""

    def __init__(self, persistence_manager: Optional[PersistenceManager] = None) -> None:
        self._lock = Lock()
        self._data: Dict[K, V] = {}
        self._persistence_manager = persistence_manager

    def put(self, key: K, value: V) -> None:
        """Store ``value`` under ``key``, replacing any existing value for that key.

        Args:
            key: The key to associate with ``value``.
            value: The value to store.
        """
        with self._lock:
            self._data[key] = value
            if self._persistence_manager is not None:
                self._persistence_manager.save(cast(Dict[str, Any], dict(self._data)))

    def get(self, key: K) -> V:
        """Return the value stored for ``key``.

        Args:
            key: The key to look up.

        Returns:
            The value associated with ``key``.

        Raises:
            KeyError: If ``key`` is not present.
        """
        with self._lock:
            if key not in self._data:
                raise KeyError(key)
            return self._data[key]

    def delete(self, key: K) -> None:
        """Remove ``key`` and its value from the store.

        Args:
            key: The key to remove.

        Raises:
            KeyError: If ``key`` is not present.
        """
        with self._lock:
            if key not in self._data:
                raise KeyError(key)
            del self._data[key]
            if self._persistence_manager is not None:
                self._persistence_manager.save(cast(Dict[str, Any], dict(self._data)))

    def get_all(self) -> Dict[K, V]:
        """Return a shallow copy of all key-value pairs in the store.

        Returns:
            A new dict containing every ``key -> value`` mapping.
        """
        with self._lock:
            return dict(self._data)

    def load_from_disk(self) -> None:
        """Load persisted data from disk and replace the current in-memory state.

        Raises:
            RuntimeError: If no persistence manager was provided.
            ValueError: If the persisted data is not a JSON object.
        """

        if self._persistence_manager is None:
            raise RuntimeError("PersistenceManager not configured")

        loaded = self._persistence_manager.load()
        with self._lock:
            # Persisted data is JSON, so keys are strings.
            self._data = cast(Dict[K, V], dict(loaded))
