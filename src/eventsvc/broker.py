"""The broker seam.

Everything above this line — the worker, the retry ladder, the dead-letter
queue, the metrics — is written against these three protocols and nothing else.
That is what lets the identical service run against an in-memory log on a laptop
and against a real Kafka cluster in CI, with no branch in the business logic.

The protocols are deliberately a *subset* of the Kafka consumer API, chosen so
that the in-memory implementation can honour all of it exactly rather than
approximating: partitioned append-only logs, consumer groups with range
assignment, positions that are separate from committed offsets, and ``seek``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable
from zlib import crc32

from .types import Headers, Message, RecordMetadata, TopicPartition


@runtime_checkable
class Producer(Protocol):
    """Writes records to topics."""

    def send(
        self,
        topic: str,
        value: dict[str, Any],
        *,
        key: str | None = None,
        headers: Headers | None = None,
    ) -> RecordMetadata:
        """Append a record and return where it landed."""

    def flush(self, timeout: float | None = None) -> None:
        """Block until every buffered record has been acknowledged."""

    def close(self) -> None:
        """Release resources."""


@runtime_checkable
class Consumer(Protocol):
    """Reads records as a member of a consumer group."""

    def subscribe(self, topics: Sequence[str]) -> None:
        """Join the group for ``topics`` and trigger an assignment."""

    def assignment(self) -> list[TopicPartition]:
        """The partitions currently owned by this consumer."""

    def poll(self, max_records: int = 1, timeout: float = 0.1) -> list[Message]:
        """Fetch up to ``max_records`` records from the assigned partitions."""

    def position(self, tp: TopicPartition) -> int:
        """The offset of the next record ``poll`` will return for ``tp``."""

    def seek(self, tp: TopicPartition, offset: int) -> None:
        """Move the read position, e.g. to re-read a record that is not due yet."""

    def commit(self) -> None:
        """Persist the current positions as the group's committed offsets."""

    def lag(self) -> dict[TopicPartition, int]:
        """Records produced but not yet returned by this consumer, per partition."""

    def close(self) -> None:
        """Leave the group, releasing partitions for reassignment."""


@runtime_checkable
class Broker(Protocol):
    """A factory for producers and consumers, plus minimal topic administration."""

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create ``topic`` if it does not already exist."""

    def topics(self) -> list[str]:
        """Every known topic name."""

    def partition_count(self, topic: str) -> int:
        """How many partitions ``topic`` has."""

    def producer(self) -> Producer:
        """A new producer."""

    def consumer(self, group_id: str, *, client_id: str | None = None) -> Consumer:
        """A new consumer in group ``group_id``."""

    def close(self) -> None:
        """Release resources."""


def partition_for_key(key: str | None, partitions: int, fallback: int = 0) -> int:
    """Map ``key`` to a partition the way a Kafka default partitioner does.

    A CRC32 of the key bytes, modulo the partition count. Keyless records fall
    back to a caller-supplied counter so they spread round-robin. Using the same
    rule in both brokers means "all events for one order land on one partition,
    and therefore stay ordered" is a property tests can pin locally.
    """
    if partitions <= 1:
        return 0
    if key is None:
        return fallback % partitions
    return crc32(key.encode("utf-8")) % partitions
