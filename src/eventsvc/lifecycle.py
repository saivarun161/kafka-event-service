"""Shutdown: turning a SIGTERM into a drain instead of a dropped batch.

An orchestrator stops a consumer the same way it stops anything else — SIGTERM,
then SIGKILL after a grace period. What happens in between is entirely up to the
process, and the default is bad: the process dies mid-batch, the offsets for the
records it already handled were never committed, and the group rebalances onto a
partition whose last few records are about to be redelivered.

At-least-once means that redelivery is *safe*, not that it is free. Every dropped
batch is duplicate work downstream, and the dedupe store only absorbs the records
that actually finished. So shutdown is worth doing properly:

1. Stop accepting new records — the workers finish the batch they are holding.
2. Commit those offsets.
3. Leave the group, so the rebalance happens once, cleanly, after the work is done.

:func:`shutdown_on_signals` covers step 0 (noticing the signal at all) and
:meth:`~eventsvc.service.EventService.stop` covers the rest, reporting what it
managed to drain in a :class:`ShutdownReport`.
"""

from __future__ import annotations

import contextlib
import signal
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

DEFAULT_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)
"""What an orchestrator actually sends: SIGTERM from Kubernetes or systemd,
SIGINT from a developer's Ctrl-C. Both mean "wind down", so both drain."""


@dataclass(frozen=True)
class ShutdownReport:
    """What the drain achieved — worth logging, and worth asserting on in tests.

    A shutdown that silently gave up on two workers looks exactly like a clean
    one from the outside, which is why :attr:`stragglers` is reported by name
    rather than collapsed into a boolean.
    """

    workers: int = 0
    drained: int = 0
    stragglers: tuple[str, ...] = ()
    duration: float = 0.0
    signal_name: str | None = None

    @property
    def clean(self) -> bool:
        """True when every worker finished its batch and left the group itself."""
        return not self.stragglers

    def as_dict(self) -> dict[str, Any]:
        return {
            "workers": self.workers,
            "drained": self.drained,
            "stragglers": list(self.stragglers),
            "duration": round(self.duration, 6),
            "clean": self.clean,
            "signal": self.signal_name,
        }

    def __str__(self) -> str:
        state = "clean" if self.clean else f"forced ({len(self.stragglers)} straggler(s))"
        return f"drained {self.drained}/{self.workers} worker(s) in {self.duration:.3f}s — {state}"


@dataclass
class SignalWatch:
    """The event a signal handler sets, plus which signal set it."""

    event: threading.Event = field(default_factory=threading.Event)
    received: signal.Signals | None = None
    installed: tuple[signal.Signals, ...] = ()

    def wait(self, timeout: float | None = None) -> bool:
        return self.event.wait(timeout)

    def request(self) -> None:
        """Ask for shutdown without a signal — an admin endpoint, or a test."""
        self.event.set()

    @property
    def signal_name(self) -> str | None:
        return None if self.received is None else self.received.name


@contextlib.contextmanager
def shutdown_on_signals(
    *signals: signal.Signals,
    event: threading.Event | None = None,
    on_signal: Callable[[signal.Signals], None] | None = None,
) -> Iterator[SignalWatch]:
    """Install drain-on-signal handlers for the duration of the block.

    The handler does exactly one thing — set an event — because a signal handler
    runs between two bytecodes of whatever thread happened to be executing, and
    draining a consumer group from there is a good way to deadlock on a lock the
    interrupted thread already holds. The real work happens on the thread that is
    waiting on the event.

    Previous handlers are restored on exit, so a library caller (or a test) does
    not permanently repoint the process's SIGINT.

    Handlers can only be installed from the main thread; from any other thread
    Python raises and this yields a watch nobody will signal. That is reported in
    :attr:`SignalWatch.installed` rather than raised, because a worker thread
    that cannot listen for signals is still perfectly able to shut down when
    :meth:`SignalWatch.request` is called.
    """
    watch = SignalWatch(event=event or threading.Event())
    previous: dict[signal.Signals, Any] = {}

    def _handle(signum: int, frame: Any) -> None:
        watch.received = signal.Signals(signum)
        watch.event.set()
        if on_signal is not None:
            on_signal(watch.received)

    for sig in signals or DEFAULT_SIGNALS:
        try:
            previous[sig] = signal.signal(sig, _handle)
        except (ValueError, OSError, RuntimeError):
            # Not the main thread, or a signal this platform does not have.
            continue
    watch.installed = tuple(previous)
    try:
        yield watch
    finally:
        for sig, handler in previous.items():
            with contextlib.suppress(ValueError, OSError, RuntimeError):
                signal.signal(sig, handler)
