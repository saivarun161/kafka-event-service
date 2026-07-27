"""Failure provenance carried in record headers.

A dead-letter record whose only content is the original payload is close to
useless at 3am: you can see *what* failed but not where it came from, how many
times it was tried, or what the exception was. So every retried and dead-lettered
record carries its history in headers, leaving the payload byte-identical to what
was originally produced. That matters for replay — the record put back on the
source topic is the original, not a re-wrapped copy that downstream consumers
would have to unwrap.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from .types import Headers, Message

EVENT_ID = "x-event-id"
ATTEMPT = "x-attempt"
ORIGINAL_TOPIC = "x-original-topic"
ORIGINAL_PARTITION = "x-original-partition"
ORIGINAL_OFFSET = "x-original-offset"
NOT_BEFORE = "x-not-before"
ERROR_TYPE = "x-error-type"
ERROR_MESSAGE = "x-error-message"
FIRST_FAILED_AT = "x-first-failed-at"
FAILED_AT = "x-failed-at"
REPLAYED_FROM = "x-replayed-from"
REPLAY_COUNT = "x-replay-count"

MAX_ERROR_MESSAGE = 512
"""Exception text is truncated: brokers cap header size, and stack-sized headers
are how a dead-letter topic quietly becomes the biggest topic in the cluster."""


def new_event_id() -> str:
    return uuid.uuid4().hex


def source_topic(message: Message) -> str:
    """The topic a record was *originally* produced to.

    For a record on the source topic that is its own topic; for one that has been
    through the retry ladder it is the header written on the first failure.
    """
    return message.header(ORIGINAL_TOPIC) or message.topic


def attempt_of(message: Message) -> int:
    """How many times this record has already been handed to a handler."""
    return message.header_int(ATTEMPT, 0)


def event_id_of(message: Message) -> str:
    """A stable identity for deduplication: explicit header, else key, else offset."""
    return message.header(EVENT_ID) or message.key or f"{message.topic}:{message.offset}"


@dataclass(frozen=True, slots=True)
class FailureInfo:
    """The failure history reconstructed from a record's headers."""

    event_id: str
    original_topic: str
    original_partition: int
    original_offset: int
    attempts: int
    error_type: str
    error_message: str
    first_failed_at: float
    failed_at: float

    @classmethod
    def from_message(cls, message: Message) -> FailureInfo:
        return cls(
            event_id=event_id_of(message),
            original_topic=source_topic(message),
            original_partition=message.header_int(ORIGINAL_PARTITION, message.partition),
            original_offset=message.header_int(ORIGINAL_OFFSET, message.offset),
            attempts=attempt_of(message),
            error_type=message.header(ERROR_TYPE) or "unknown",
            error_message=message.header(ERROR_MESSAGE) or "",
            first_failed_at=message.header_float(FIRST_FAILED_AT),
            failed_at=message.header_float(FAILED_AT),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "original_topic": self.original_topic,
            "original_partition": self.original_partition,
            "original_offset": self.original_offset,
            "attempts": self.attempts,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "first_failed_at": self.first_failed_at,
            "failed_at": self.failed_at,
        }


def failure_headers(
    message: Message,
    error: BaseException,
    *,
    attempt: int,
    now: float,
    not_before: float | None = None,
) -> Headers:
    """Headers for the next hop — a retry tier or the dead-letter topic.

    Provenance fields are written once, on the first failure, and preserved
    afterwards, so ``original_offset`` still points at the record on the source
    topic no matter how many tiers it has been through.
    """
    headers = dict(message.headers)
    headers.setdefault(EVENT_ID, event_id_of(message))
    headers.setdefault(ORIGINAL_TOPIC, message.topic)
    headers.setdefault(ORIGINAL_PARTITION, str(message.partition))
    headers.setdefault(ORIGINAL_OFFSET, str(message.offset))
    headers.setdefault(FIRST_FAILED_AT, f"{now:.6f}")
    headers[ATTEMPT] = str(attempt)
    headers[ERROR_TYPE] = type(error).__name__
    headers[ERROR_MESSAGE] = str(error)[:MAX_ERROR_MESSAGE]
    headers[FAILED_AT] = f"{now:.6f}"
    if not_before is None:
        headers.pop(NOT_BEFORE, None)
    else:
        headers[NOT_BEFORE] = f"{not_before:.6f}"
    return headers


def replay_headers(message: Message) -> Headers:
    """Headers for a record being replayed from the dead-letter topic.

    The attempt counter resets — a replay is a fresh delivery, and it should get
    the full retry ladder again — but the provenance and a replay counter stay,
    so a record that has been round-tripped twice is visible as such.
    """
    headers = dict(message.headers)
    headers[REPLAYED_FROM] = message.topic
    headers[REPLAY_COUNT] = str(message.header_int(REPLAY_COUNT, 0) + 1)
    headers[ATTEMPT] = "0"
    for name in (NOT_BEFORE, ERROR_TYPE, ERROR_MESSAGE, FAILED_AT):
        headers.pop(name, None)
    return headers
