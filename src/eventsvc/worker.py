"""The consume loop: one consumer, one topic, one set of routing rules.

A worker is deliberately small and unaware of which tier it is serving. The
worker on ``orders`` and the worker on ``orders.retry.9s`` are the same class
with the same code path — the record's headers, not the worker's configuration,
decide where a failure goes next. That is what keeps the ladder from growing a
special case per tier.

Ordering of operations in the loop is the part worth reading closely:

1. poll a batch
2. for each record: dedupe, invoke the handler, and on failure *republish* to the
   next tier before returning
3. commit once, after the whole batch

Committing last is what makes this at-least-once. A crash anywhere in step 2
replays the batch, which is survivable; committing first would make it
at-most-once and lose the batch instead.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .broker import Broker, Consumer, Producer
from .clock import Clock, SystemClock
from .envelope import (
    NOT_BEFORE,
    attempt_of,
    event_id_of,
    failure_headers,
    source_topic,
)
from .errors import PermanentError
from .idempotency import IdempotencyStore, NullIdempotencyStore
from .metrics import Metrics, Outcome
from .retry import RetryPolicy, dlq_topic, retry_topic
from .types import Message, TopicPartition

Handler = Callable[[Message], None]
"""What the caller supplies. Returning normally means success; raising
:class:`~eventsvc.errors.PermanentError` skips the ladder; any other exception
is treated as transient."""


@dataclass
class WorkerStats:
    """Per-worker counters, mirrored by the Prometheus metrics."""

    topic: str = ""
    consumed: int = 0
    ok: int = 0
    retried: int = 0
    dead_lettered: int = 0
    duplicates: int = 0
    polls: int = 0

    def record(self, outcome: str) -> None:
        if outcome == Outcome.OK:
            self.ok += 1
        elif outcome == Outcome.RETRIED:
            self.retried += 1
        elif outcome == Outcome.DEAD_LETTERED:
            self.dead_lettered += 1
        elif outcome == Outcome.DUPLICATE:
            self.duplicates += 1

    def merged(self, other: WorkerStats, topic: str = "") -> WorkerStats:
        return WorkerStats(
            topic=topic,
            consumed=self.consumed + other.consumed,
            ok=self.ok + other.ok,
            retried=self.retried + other.retried,
            dead_lettered=self.dead_lettered + other.dead_lettered,
            duplicates=self.duplicates + other.duplicates,
            polls=self.polls + other.polls,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "consumed": self.consumed,
            "ok": self.ok,
            "retried": self.retried,
            "dead_lettered": self.dead_lettered,
            "duplicates": self.duplicates,
            "polls": self.polls,
        }


@dataclass
class Worker:
    """Consumes one topic and routes every failure to its next hop.

    ``blocking`` selects how a not-yet-due retry record is handled: block until it
    comes due (correct for a dedicated retry-tier thread) or rewind the partition
    and return (needed by the cooperative single-threaded scheduler, which has to
    give the other tiers a turn).
    """

    broker: Broker
    topic: str
    group_id: str
    handler: Handler
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    clock: Clock = field(default_factory=SystemClock)
    metrics: Metrics = field(default_factory=Metrics)
    idempotency: IdempotencyStore = field(default_factory=NullIdempotencyStore)
    batch_size: int = 16
    poll_timeout: float = 0.05
    blocking: bool = True
    client_id: str | None = None

    _consumer: Consumer = field(init=False, repr=False)
    _producer: Producer = field(init=False, repr=False)
    stats: WorkerStats = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._consumer = self.broker.consumer(self.group_id, client_id=self.client_id)
        self._consumer.subscribe([self.topic])
        self._producer = self.broker.producer()
        self._stopping = threading.Event()
        self._pending_due_at: float | None = None
        self.stats = WorkerStats(topic=self.topic)
        self.handler_name = getattr(self.handler, "__name__", type(self.handler).__name__)

    # -- scheduling hints --------------------------------------------------

    @property
    def pending_due_at(self) -> float | None:
        """When the earliest deferred retry record becomes due, if any.

        The cooperative scheduler uses this to sleep exactly long enough instead
        of spinning: if every worker is idle and one is holding a record until
        ``t``, there is nothing to do until ``t``.
        """
        return self._pending_due_at

    def _defer_until(self, due_at: float) -> None:
        if self._pending_due_at is None or due_at < self._pending_due_at:
            self._pending_due_at = due_at

    # -- the loop ----------------------------------------------------------

    def poll_once(self, *, blocking: bool | None = None) -> int:
        """Poll once and process what comes back. Returns the number handled."""
        block = self.blocking if blocking is None else blocking
        self._pending_due_at = None
        batch = self._consumer.poll(self.batch_size, self.poll_timeout)
        self.stats.polls += 1
        self._record_lag()
        if not batch:
            return 0

        by_partition: dict[TopicPartition, list[Message]] = {}
        for message in batch:
            by_partition.setdefault(message.topic_partition, []).append(message)

        handled = 0
        for tp, messages in by_partition.items():
            for message in messages:
                due_at = message.header_float(NOT_BEFORE, 0.0)
                if due_at > self.clock.now() and not self._wait_until(due_at, block):
                    # Not due (or we were told to stop): rewind this partition to
                    # the record's offset so it — and everything behind it — is
                    # redelivered rather than skipped.
                    self._consumer.seek(tp, message.offset)
                    self._defer_until(due_at)
                    break
                self._handle(message)
                handled += 1

        self._consumer.commit()
        self.metrics.commits.labels(self.topic, self.group_id).inc()
        return handled

    def _wait_until(self, due_at: float, blocking: bool) -> bool:
        """Wait for ``due_at``. Returns False if the record should be put back."""
        if not blocking:
            return False
        while not self._stopping.is_set():
            remaining = due_at - self.clock.now()
            if remaining <= 0:
                return True
            # Wake up at least once per poll interval so stop() stays responsive
            # even when the tier delay is measured in minutes.
            self.clock.sleep(min(remaining, self.poll_timeout))
        return False

    def drain(self, *, idle_rounds: int = 2, max_rounds: int = 10_000) -> int:
        """Poll until ``idle_rounds`` consecutive polls come back empty.

        Cooperative by construction — it never blocks on a retry delay — so the
        caller stays in control of when to advance time.
        """
        handled = 0
        idle = 0
        for _ in range(max_rounds):
            count = self.poll_once(blocking=False)
            handled += count
            idle = 0 if count else idle + 1
            if idle >= idle_rounds:
                break
        return handled

    def run(self) -> WorkerStats:
        """Block, consuming until :meth:`stop` is called. Used by the threaded service."""
        self._stopping.clear()
        while not self._stopping.is_set():
            if self.poll_once(blocking=True) == 0:
                self.clock.sleep(self.poll_timeout)
        return self.stats

    def stop(self) -> None:
        self._stopping.set()

    def close(self) -> None:
        self.stop()
        self._consumer.close()
        self._producer.flush()
        self._producer.close()

    # -- per-record handling ----------------------------------------------

    def _handle(self, message: Message) -> str:
        self.stats.consumed += 1
        self.metrics.consumed.labels(message.topic, self.group_id).inc()

        event_id = event_id_of(message)
        if self.idempotency.seen(event_id):
            return self._finish(message, Outcome.DUPLICATE)

        attempt = attempt_of(message) + 1
        started = time.perf_counter()
        self.metrics.inflight.labels(self.group_id).inc()
        try:
            self.handler(message)
        except PermanentError as exc:
            return self._dead_letter(message, exc, attempt)
        except Exception as exc:  # any handler error not marked permanent is retriable
            return self._retry(message, exc, attempt)
        else:
            self.idempotency.mark(event_id)
            return self._finish(message, Outcome.OK)
        finally:
            self.metrics.inflight.labels(self.group_id).dec()
            self.metrics.handler_seconds.labels(message.topic, self.handler_name).observe(
                time.perf_counter() - started
            )

    def _retry(self, message: Message, error: BaseException, attempt: int) -> str:
        delay = self.policy.delay_for_attempt(attempt)
        if delay is None:
            return self._dead_letter(message, error, attempt)

        now = self.clock.now()
        target = retry_topic(source_topic(message), delay)
        self._producer.send(
            target,
            message.value,
            key=message.key,
            headers=failure_headers(
                message, error, attempt=attempt, now=now, not_before=now + delay
            ),
        )
        self.metrics.retries.labels(source_topic(message), str(attempt)).inc()
        return self._finish(message, Outcome.RETRIED)

    def _dead_letter(self, message: Message, error: BaseException, attempt: int) -> str:
        now = self.clock.now()
        origin = source_topic(message)
        self._producer.send(
            dlq_topic(origin),
            message.value,
            key=message.key,
            headers=failure_headers(message, error, attempt=attempt, now=now, not_before=None),
        )
        self.metrics.dead_letters.labels(origin, type(error).__name__).inc()
        return self._finish(message, Outcome.DEAD_LETTERED)

    def _finish(self, message: Message, outcome: str) -> str:
        self.stats.record(outcome)
        self.metrics.processed.labels(message.topic, self.group_id, outcome).inc()
        return outcome

    def _record_lag(self) -> None:
        for tp, lag in self._consumer.lag().items():
            self.metrics.lag.labels(tp.topic, str(tp.partition), self.group_id).set(lag)
