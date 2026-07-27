"""Time as an injected dependency.

Retry scheduling is the one place where this service is genuinely time-dependent,
so time is a seam rather than a call to :func:`time.time`. Tests drive a
:class:`ManualClock` and get a delay ladder that would take a minute of wall time
to exercise in a few microseconds.
"""

from __future__ import annotations

import threading
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """The slice of the ``time`` module this project depends on."""

    def now(self) -> float:
        """Seconds since the epoch."""

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds`` (a no-op for non-positive values)."""


class SystemClock:
    """Wall-clock time. The production default."""

    __slots__ = ()

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """A clock that only moves when told to.

    ``sleep`` advances the clock instead of blocking, so a worker waiting on a
    60-second retry tier returns immediately while still observing the delay.
    """

    __slots__ = ("_lock", "_now")

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)
        self._lock = threading.RLock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> float:
        """Move the clock forward and return the new time."""
        with self._lock:
            if seconds > 0:
                self._now += float(seconds)
            return self._now
