"""Wiring: one source topic becomes a fleet of workers.

:class:`EventService` owns the topology derived from a :class:`RetryPolicy` —
the source topic, one worker per retry tier, and the dead-letter topic — and
runs it either cooperatively on the calling thread (tests, the demo, anything
that wants determinism) or with one thread per worker (the long-running mode).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from .broker import Broker
from .clock import Clock, SystemClock
from .dlq import DeadLetterQueue
from .idempotency import IdempotencyStore, InMemoryIdempotencyStore
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
        for worker in self.workers:
            thread = threading.Thread(target=worker.run, name=f"worker-{worker.topic}", daemon=True)
            self._threads.append(thread)
            thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        for worker in self.workers:
            worker.stop()
        for thread in self._threads:
            thread.join(timeout)
        self._threads.clear()
        for worker in self.workers:
            worker.close()

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
