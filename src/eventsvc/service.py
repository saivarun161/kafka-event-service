"""Wiring: one source topic becomes a fleet of workers.

:class:`EventService` owns the topology derived from a :class:`RetryPolicy` —
the source topic, one worker per retry tier, and the dead-letter topic — and
runs it either cooperatively on the calling thread (tests, the demo, anything
that wants determinism) or with one thread per worker (the long-running mode).
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from .broker import Broker
from .clock import Clock, SystemClock
from .dlq import DeadLetterQueue
from .idempotency import IdempotencyStore, InMemoryIdempotencyStore
from .lifecycle import DEFAULT_SIGNALS, ShutdownReport, shutdown_on_signals
from .metrics import Metrics
from .retry import RetryPolicy, dlq_topic
from .worker import Handler, Worker, WorkerStats


@dataclass
class EventService:
    """A handler bound to a topic, with the full retry/DLQ topology around it."""

    broker: Broker
    topic: str
    handler: Handler
    group_id: str | None = None
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    clock: Clock = field(default_factory=SystemClock)
    metrics: Metrics = field(default_factory=Metrics)
    idempotency: IdempotencyStore = field(default_factory=InMemoryIdempotencyStore)
    partitions: int = 3
    consumers_per_topic: int = 1

    def __post_init__(self) -> None:
        if self.group_id is None:
            self.group_id = f"{self.topic}-workers"
        if self.consumers_per_topic < 1:
            raise ValueError("consumers_per_topic must be >= 1")

        # Create the whole topology up front so it is visible (and identically
        # partitioned) before the first record flows.
        for name in self.policy.topics_for(self.topic):
            self.broker.create_topic(name, self.partitions)

        self.workers: list[Worker] = []
        consumed_topics = (self.topic, *self.policy.retry_topics(self.topic))
        for name in consumed_topics:
            for replica in range(self.consumers_per_topic):
                self.workers.append(
                    Worker(
                        broker=self.broker,
                        topic=name,
                        group_id=self.group_id,
                        handler=self.handler,
                        policy=self.policy,
                        clock=self.clock,
                        metrics=self.metrics,
                        idempotency=self.idempotency,
                        client_id=f"{self.group_id}-{name}-{replica}",
                    )
                )

        self.dlq = DeadLetterQueue(
            self.broker,
            self.topic,
            group_id=f"{self.group_id}-dlq",
            clock=self.clock,
        )
        self._threads: list[threading.Thread] = []
        self._stopped = False

    # -- cooperative mode --------------------------------------------------

    def run_until_idle(self, *, idle_rounds: int = 3, max_rounds: int = 10_000) -> WorkerStats:
        """Single-threaded scheduler: round-robin the workers until nothing moves.

        Between rounds, if every worker is idle but some retry record exists that
        is not due yet, the clock is advanced (or slept) to the earliest due time
        instead of spinning. Under a :class:`~eventsvc.clock.ManualClock` a
        60-second ladder completes in microseconds; under the system clock the
        behaviour matches production.
        """
        idle = 0
        for _ in range(max_rounds):
            handled = sum(worker.poll_once(blocking=False) for worker in self.workers)
            if handled:
                idle = 0
                continue
            due_times = [w.pending_due_at for w in self.workers if w.pending_due_at is not None]
            if due_times:
                wait = max(0.0, min(due_times) - self.clock.now())
                # Nudge past the boundary so the record is strictly due on the
                # next round rather than equal-to-now flaky.
                self.clock.sleep(wait + 1e-6)
                idle = 0
                continue
            idle += 1
            if idle >= idle_rounds:
                break
        return self.stats()

    # -- threaded mode -----------------------------------------------------

    def start(self) -> None:
        """One daemon thread per worker."""
        if self._threads:
            raise RuntimeError("service is already running")
        if self._stopped:
            # A clean shutdown leaves the consumer group, which is the point of
            # it. Restarting would hand every worker a closed consumer and the
            # threads would die one poll later — fail here, where it is legible.
            raise RuntimeError(
                "service has already been shut down; build a new EventService to restart"
            )
        for worker in self.workers:
            # Clear any previous shutdown before the thread exists, so a stop()
            # that arrives moments after start() cannot be raced away.
            worker.reset()
            thread = threading.Thread(target=worker.run, name=f"worker-{worker.topic}", daemon=True)
            self._threads.append(thread)
            thread.start()

    def stop(self, timeout: float = 5.0, *, drain: bool = True) -> ShutdownReport:
        """Wind the fleet down inside one ``timeout`` and report what drained.

        ``timeout`` is a deadline for the whole shutdown, not per worker: an
        orchestrator's grace period does not get longer because the service has
        more retry tiers, and a per-thread timeout would let a five-worker
        topology take five times as long as advertised to die.

        A worker that finished is closed — which is what makes it leave the
        consumer group, so the group rebalances once, after the work is done.
        A straggler still inside its handler is deliberately *not* closed:
        yanking the consumer out from under a running handler produces exactly
        the mid-batch rebalance this shutdown path exists to avoid. It is named
        in the report instead, and the process exit releases it.
        """
        # Real monotonic time, not the injected clock: this deadline governs
        # ``Thread.join``, which waits in wall time no matter what a ManualClock
        # believes, and an orchestrator's grace period is wall time too.
        started = time.monotonic()
        for worker in self.workers:
            worker.stop(drain=drain)

        deadline = started + max(0.0, timeout)
        # Tracked by identity, not by name: a report is free to contain two
        # workers with the same label, but closing the wrong one's consumer
        # because of it is the exact bug this method is here to prevent.
        stragglers: list[Worker] = []
        for thread, worker in zip(self._threads, self.workers, strict=False):
            thread.join(max(0.0, deadline - time.monotonic()))
            if thread.is_alive():
                stragglers.append(worker)
        self._threads.clear()

        stuck = {id(worker) for worker in stragglers}
        for worker in self.workers:
            if id(worker) not in stuck:
                worker.close()
        self._stopped = True

        return ShutdownReport(
            workers=len(self.workers),
            drained=len(self.workers) - len(stragglers),
            stragglers=tuple(worker.name for worker in stragglers),
            duration=time.monotonic() - started,
        )

    def run_forever(
        self,
        *,
        timeout: float = 5.0,
        signals: Sequence[signal.Signals] = DEFAULT_SIGNALS,
        stop_when: threading.Event | None = None,
        ready: threading.Event | None = None,
    ) -> ShutdownReport:
        """Start the fleet, block until a shutdown signal, then drain.

        The long-running entry point: what a container actually runs. ``stop_when``
        lets something other than a signal ask for the same orderly drain (an
        admin endpoint, a supervising thread, a test), and ``ready`` is set once
        the handlers are installed, so a caller can know the process is listening
        before it sends anything.
        """
        self.start()
        try:
            with shutdown_on_signals(*signals, event=stop_when) as watch:
                if ready is not None:
                    ready.set()
                watch.wait()
                signal_name = watch.signal_name
        except BaseException:
            self.stop(timeout)
            raise
        report = self.stop(timeout)
        return replace(report, signal_name=signal_name)

    # -- reporting ---------------------------------------------------------

    def stats(self) -> WorkerStats:
        total = WorkerStats(topic=self.topic)
        for worker in self.workers:
            total = total.merged(worker.stats, topic=self.topic)
        return total

    def stats_by_topic(self) -> dict[str, WorkerStats]:
        by_topic: dict[str, WorkerStats] = {}
        for worker in self.workers:
            existing = by_topic.get(worker.topic)
            by_topic[worker.topic] = (
                worker.stats
                if existing is None
                else existing.merged(worker.stats, topic=worker.topic)
            )
        return by_topic

    def dead_letters(self) -> list[Any]:
        return self.dlq.entries()

    @property
    def dlq_topic(self) -> str:
        return dlq_topic(self.topic)
