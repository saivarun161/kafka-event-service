"""Value types shared by every broker implementation.

These deliberately mirror the shape of a Kafka record without importing a Kafka
client, so the same objects flow through the in-memory broker and the real one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

Headers = dict[str, str]
"""Record headers. Kept ``str -> str``; the Kafka adapter encodes to UTF-8 bytes."""


@dataclass(frozen=True, slots=True, order=True)
class TopicPartition:
    """A single partition of a topic — the unit of assignment and of offsets."""

    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}[{self.partition}]"


@dataclass(frozen=True, slots=True)
class RecordMetadata:
    """Where a produced record landed."""

    topic: str
    partition: int
    offset: int

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)


@dataclass(frozen=True, slots=True)
class Message:
    """A consumed record.

    ``value`` is already decoded into a mapping: every broker in this project
    speaks JSON, which keeps handlers free of serialization concerns.
    """

    topic: str
    partition: int
    offset: int
    key: str | None = None
    value: dict[str, Any] = field(default_factory=dict)
    headers: Headers = field(default_factory=dict)
    timestamp: float = 0.0

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)

    def header_int(self, name: str, default: int = 0) -> int:
        raw = self.headers.get(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def header_float(self, name: str, default: float = 0.0) -> float:
        raw = self.headers.get(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    def __str__(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset} key={self.key}"
