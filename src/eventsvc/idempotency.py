"""Deduplication, because at-least-once means what it says.

Offsets are committed *after* the handler runs, so a crash between those two
points redelivers the record — that is the design, not a bug: the alternative
(commit first) silently drops work. Redelivery is only safe if handling twice is
the same as handling once, and most handlers are not naturally idempotent.

So the worker keeps a short memory of event ids it has already completed. This
is not exactly-once — nothing here is — it is a cheap filter that turns the
common duplicate (a redelivery seconds after a rebalance) into a no-op. The
store is a protocol so a real deployment can point it at Redis with a TTL and
get the same behaviour across every replica.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Protocol, runtime_checkable

from .clock import Clock, SystemClock


@runtime_checkable
class IdempotencyStore(Protocol):
    """Remembers which event ids have already been processed."""

    def seen(self, event_id: str) -> bool:
        """True if ``event_id`` has been marked and has not expired."""

    def mark(self, event_id: str) -> None:
        """Record ``event_id`` as processed."""

    def __len__(self) -> int:
        """How many ids are currently remembered."""


class InMemoryIdempotencyStore:
    """A bounded, TTL'd LRU of processed event ids.

    Both bounds are deliberate. The TTL keeps the store from growing forever, and
    the size cap keeps a traffic spike from turning the dedupe cache into the
    reason the process runs out of memory. Evicting early only costs a duplicate
    handler call, which is the situation this exists to make survivable anyway.
    """

    def __init__(self, *, max_size: int = 10_000, ttl: float = 3600.0, clock: Clock | None = None):
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if ttl <= 0:
            raise ValueError("ttl must be > 0")
        self.max_size = max_size
        self.ttl = ttl
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        self._entries: OrderedDict[str, float] = OrderedDict()

    def seen(self, event_id: str) -> bool:
        with self._lock:
            expires_at = self._entries.get(event_id)
            if expires_at is None:
                return False
            if expires_at <= self._clock.now():
                del self._entries[event_id]
                return False
            self._entries.move_to_end(event_id)
            return True

    def mark(self, event_id: str) -> None:
        with self._lock:
            self._entries[event_id] = self._clock.now() + self.ttl
            self._entries.move_to_end(event_id)
            self._evict()

    def _evict(self) -> None:
        now = self._clock.now()
        for event_id, expires_at in list(self._entries.items()):
            if expires_at > now:
                break  # insertion order is expiry order for a fixed TTL
            del self._entries[event_id]
        while len(self._entries) > self.max_size:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


class NullIdempotencyStore:
    """Deduplication turned off — every delivery reaches the handler."""

    __slots__ = ()

    def seen(self, event_id: str) -> bool:
        return False

    def mark(self, event_id: str) -> None:
        """No-op."""

    def __len__(self) -> int:
        return 0
