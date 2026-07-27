"""An in-memory broker that implements the log, not a mock of it.

This is the piece that makes the project runnable with no infrastructure. It is
not a queue with a ``list.pop()`` in it — that would let the worker's tests pass
while the real deployment broke on the semantics that actually matter. Instead
it reproduces the four behaviours the worker depends on:

* **Partitioned append-only logs.** Records are never removed by reading; a
  partition is a list and an offset is an index into it.
* **Consumer groups with range assignment.** Two consumers in one group split the
  partitions; a third joining rebalances all of them.
* **Position separate from committed offset.** A consumer that dies without
  committing loses its progress, and the records are redelivered — the
  at-least-once guarantee the idempotency layer exists to absorb.
* **Seek.** Needed by the retry-tier workers to put back a record that is not due.
"""

from __future__ import annotations

import copy
import itertools
import threading
from collections.abc import Sequence
from typing import Any

from .broker import partition_for_key
from .clock import Clock, SystemClock
from .errors import BrokerError
from .types import Headers, Message, RecordMetadata, TopicPartition


class InMemoryBroker:
    """A single-process stand-in for a Kafka cluster.

    Thread-safe: the demo and :meth:`EventService.start` run one worker per topic
    on its own thread, and they all share one broker.
    """

    def __init__(self, *, default_partitions: int = 1, clock: Clock | None = None) -> None:
        if default_partitions < 1:
            raise ValueError("default_partitions must be >= 1")
        self._default_partitions = default_partitions
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        self._logs: dict[str, list[list[Message]]] = {}
        # (group, topic) -> {partition: committed offset}
        self._committed: dict[tuple[str, str], dict[int, int]] = {}
        # (group, topic) -> ordered member client ids, used for range assignment
        self._members: dict[tuple[str, str], list[str]] = {}
        self._round_robin: dict[str, itertools.count[int]] = {}
        self._client_ids = itertools.count(1)

    # -- administration ---------------------------------------------------

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        if partitions < 1:
            raise ValueError("partitions must be >= 1")
        with self._lock:
            if topic not in self._logs:
                self._logs[topic] = [[] for _ in range(partitions)]
                self._round_robin[topic] = itertools.count()

    def topics(self) -> list[str]:
        with self._lock:
            return sorted(self._logs)

    def partition_count(self, topic: str) -> int:
        with self._lock:
            if topic not in self._logs:
                raise BrokerError(f"unknown topic: {topic}")
            return len(self._logs[topic])

    def end_offset(self, tp: TopicPartition) -> int:
        """The offset that will be assigned to the next record on ``tp``."""
        with self._lock:
            return len(self._logs[tp.topic][tp.partition])

    def log(self, topic: str) -> list[Message]:
        """Every record in ``topic``, partition by partition. For tests and the CLI."""
        with self._lock:
            if topic not in self._logs:
                return []
            return [copy.deepcopy(m) for partition in self._logs[topic] for m in partition]

    # -- clients ----------------------------------------------------------

    def producer(self) -> InMemoryProducer:
        return InMemoryProducer(self)

    def consumer(self, group_id: str, *, client_id: str | None = None) -> InMemoryConsumer:
        return InMemoryConsumer(self, group_id, client_id or f"consumer-{next(self._client_ids)}")

    def close(self) -> None:  # pragma: no cover - nothing to release
        """Present for protocol parity with the Kafka broker."""

    # -- internals used by the clients ------------------------------------

    def _append(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None,
        headers: Headers | None,
    ) -> RecordMetadata:
        with self._lock:
            if topic not in self._logs:
                self.create_topic(topic, self._default_partitions)
            partitions = self._logs[topic]
            index = partition_for_key(key, len(partitions), next(self._round_robin[topic]))
            offset = len(partitions[index])
            record = Message(
                topic=topic,
                partition=index,
                offset=offset,
                key=key,
                # Copied on the way in and on the way out: a handler that mutates
                # what it was given must not corrupt the log.
                value=copy.deepcopy(value),
                headers=dict(headers or {}),
                timestamp=self._clock.now(),
            )
            partitions[index].append(record)
            return RecordMetadata(topic, index, offset)

    def _join(self, group_id: str, topic: str, client_id: str) -> None:
        with self._lock:
            if topic not in self._logs:
                self.create_topic(topic, self._default_partitions)
            members = self._members.setdefault((group_id, topic), [])
            if client_id not in members:
                members.append(client_id)
            self._committed.setdefault((group_id, topic), {})

    def _leave(self, group_id: str, topic: str, client_id: str) -> None:
        with self._lock:
            members = self._members.get((group_id, topic))
            if members and client_id in members:
                members.remove(client_id)

    def _assignment(self, group_id: str, topic: str, client_id: str) -> list[int]:
        """Range assignment: partitions split into contiguous blocks by member rank.

        Recomputed on every call, which is how a rebalance happens here — a new
        member joining immediately narrows everyone else's assignment.
        """
        with self._lock:
            members = self._members.get((group_id, topic), [])
            if client_id not in members:
                return []
            partitions = len(self._logs[topic])
            rank = members.index(client_id)
            count = len(members)
            base, extra = divmod(partitions, count)
            start = rank * base + min(rank, extra)
            width = base + (1 if rank < extra else 0)
            return list(range(start, start + width))

    def _committed_offset(self, group_id: str, tp: TopicPartition) -> int:
        with self._lock:
            return self._committed.setdefault((group_id, tp.topic), {}).get(tp.partition, 0)

    def _commit(self, group_id: str, offsets: dict[TopicPartition, int]) -> None:
        with self._lock:
            for tp, offset in offsets.items():
                self._committed.setdefault((group_id, tp.topic), {})[tp.partition] = offset

    def _fetch(self, tp: TopicPartition, start: int, limit: int) -> list[Message]:
        with self._lock:
            partition = self._logs[tp.topic][tp.partition]
            return [copy.deepcopy(m) for m in partition[start : start + limit]]


class InMemoryProducer:
    """Synchronous producer: :meth:`send` returns only once the record is in the log."""

    __slots__ = ("_broker",)

    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker

    def send(
        self,
        topic: str,
        value: dict[str, Any],
        *,
        key: str | None = None,
        headers: Headers | None = None,
    ) -> RecordMetadata:
        return self._broker._append(topic, value, key, headers)

    def flush(self, timeout: float | None = None) -> None:
        """No-op: sends are already durable by the time they return."""

    def close(self) -> None:
        """No-op."""


class InMemoryConsumer:
    """A group member with its own read positions."""

    def __init__(self, broker: InMemoryBroker, group_id: str, client_id: str) -> None:
        self._broker = broker
        self.group_id = group_id
        self.client_id = client_id
        self._topics: list[str] = []
        self._positions: dict[TopicPartition, int] = {}
        self._closed = False

    def subscribe(self, topics: Sequence[str]) -> None:
        if self._closed:
            raise BrokerError("consumer is closed")
        for topic in topics:
            if topic not in self._topics:
                self._topics.append(topic)
            self._broker._join(self.group_id, topic, self.client_id)

    def assignment(self) -> list[TopicPartition]:
        assigned: list[TopicPartition] = []
        for topic in self._topics:
            for partition in self._broker._assignment(self.group_id, topic, self.client_id):
                assigned.append(TopicPartition(topic, partition))
        # Drop positions for partitions this consumer no longer owns. Uncommitted
        # progress on them is lost, exactly as it would be after a rebalance.
        owned = set(assigned)
        for tp in list(self._positions):
            if tp not in owned:
                del self._positions[tp]
        return assigned

    def position(self, tp: TopicPartition) -> int:
        if tp not in self._positions:
            self._positions[tp] = self._broker._committed_offset(self.group_id, tp)
        return self._positions[tp]

    def seek(self, tp: TopicPartition, offset: int) -> None:
        if offset < 0:
            raise ValueError("offset must be >= 0")
        self._positions[tp] = offset

    def poll(self, max_records: int = 1, timeout: float = 0.1) -> list[Message]:
        if self._closed:
            raise BrokerError("consumer is closed")
        fetched: list[Message] = []
        for tp in self.assignment():
            if len(fetched) >= max_records:
                break
            start = self.position(tp)
            batch = self._broker._fetch(tp, start, max_records - len(fetched))
            if batch:
                self._positions[tp] = start + len(batch)
                fetched.extend(batch)
        return fetched

    def commit(self) -> None:
        if self._positions:
            self._broker._commit(self.group_id, dict(self._positions))

    def lag(self) -> dict[TopicPartition, int]:
        return {
            tp: max(0, self._broker.end_offset(tp) - self.position(tp)) for tp in self.assignment()
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for topic in self._topics:
            self._broker._leave(self.group_id, topic, self.client_id)
        self._positions.clear()
