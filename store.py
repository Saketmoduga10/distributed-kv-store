"""Simple in-memory key-value store."""

from threading import Lock
from typing import Dict, Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class InMemoryKVStore(Generic[K, V]):
    """In-memory mapping from keys to values, protected by a lock for thread safety."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._data: Dict[K, V] = {}

    def put(self, key: K, value: V) -> None:
        """Store ``value`` under ``key``, replacing any existing value for that key.

        Args:
            key: The key to associate with ``value``.
            value: The value to store.
        """
        with self._lock:
            self._data[key] = value

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

    def get_all(self) -> Dict[K, V]:
        """Return a shallow copy of all key-value pairs in the store.

        Returns:
            A new dict containing every ``key -> value`` mapping.
        """
        with self._lock:
            return dict(self._data)
