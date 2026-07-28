"""Dead-letter queue tooling: inspect, summarize, replay.

A dead-letter topic that can only be written is a landfill. The operations that
make it a queue are reading it with the failure context intact, aggregating it
("42 records, 40 of them KeyError from one bad deploy"), and replaying selected
records back onto the source topic once the underlying cause is fixed.

Replay puts back the *original payload* with a reset attempt counter, so a
replayed record gets the full retry ladder again. A replay-count header survives
the round trip — a record on its third replay is a signal that the fix was not.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from .broker import Broker
from .clock import Clock, SystemClock
from .envelope import REPLAY_COUNT, FailureInfo, replay_headers
from .retry import dlq_topic
from .types import Message


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A dead-lettered record with its reconstructed failure history."""

    message: Message
    info: FailureInfo

    @property
    def replay_count(self) -> int:
        return self.message.header_int(REPLAY_COUNT, 0)

    def as_dict(self) -> dict[str, Any]:
        return {
            "offset": self.message.offset,
            "partition": self.message.partition,
            "key": self.message.key,
            "value": self.message.value,
            "replay_count": self.replay_count,
            **self.info.as_dict(),
        }


class DeadLetterQueue:
    """A reader/replayer for one source topic's dead-letter topic."""

    def __init__(
        self,
        broker: Broker,
        source: str,
        *,
        group_id: str | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.broker = broker
        self.source = source
        self.topic = dlq_topic(source)
        self.group_id = group_id or f"{source}-dlq-tools"
        self.clock = clock or SystemClock()

    def entries(
        self, limit: int | None = None, *, poll_timeout: float = 1.0, empty_polls: int = 5
    ) -> list[DeadLetter]:
        """Read the dead-letter topic from the beginning, without consuming it.

        Uses a throwaway consumer group so inspection never moves the offsets of
        any real consumer — reading a DLQ must not be a destructive act.

        A read only concludes after ``empty_polls`` consecutive empty polls: on a
        real broker the first polls come back empty while the group is still
        joining, and a single empty poll would misread a populated DLQ as empty.
        """
        consumer = self.broker.consumer(f"{self.group_id}-view-{id(self)}")
        try:
            consumer.subscribe([self.topic])
            entries: list[DeadLetter] = []
            empty = 0
            while (limit is None or len(entries) < limit) and empty < empty_polls:
                batch = consumer.poll(max_records=256, timeout=poll_timeout)
                if not batch:
                    empty += 1
                    continue
                empty = 0
                for message in batch:
                    entries.append(DeadLetter(message, FailureInfo.from_message(message)))
                    if limit is not None and len(entries) >= limit:
                        break
            entries.sort(key=lambda e: (e.info.failed_at, e.message.partition, e.message.offset))
            return entries
        finally:
            consumer.close()

    def summary(self) -> dict[str, Any]:
        """The 3am view: how many, failing how, since when."""
        entries = self.entries()
        by_error = Counter(entry.info.error_type for entry in entries)
        return {
            "topic": self.topic,
            "total": len(entries),
            "by_error": dict(by_error.most_common()),
            "replayed_before": sum(1 for e in entries if e.replay_count > 0),
            "oldest_failed_at": min((e.info.first_failed_at for e in entries), default=None),
            "newest_failed_at": max((e.info.failed_at for e in entries), default=None),
        }

    def matching(
        self,
        select: Callable[[DeadLetter], bool] | None = None,
        limit: int | None = None,
    ) -> list[DeadLetter]:
        """The dead letters :meth:`replay` would move, without moving them.

        Selection is factored out of the replay itself so a caller can show an
        operator exactly what a replay is about to touch — a dry run and the real
        thing then agree by construction rather than by two filters staying in
        sync.
        """
        chosen: list[DeadLetter] = []
        for entry in self.entries():
            if select is not None and not select(entry):
                continue
            chosen.append(entry)
            if limit is not None and len(chosen) >= limit:
                break
        return chosen

    def replay_all(self, entries: Sequence[DeadLetter]) -> list[DeadLetter]:
        """Republish exactly these dead letters, and return them.

        Taking the entries rather than a filter is what lets a caller replay
        precisely the records it already showed someone: the confirmation and
        the write act on one list, so nothing that arrived in between rides
        along on an approval it was never part of.
        """
        if not entries:
            return []
        producer = self.broker.producer()
        try:
            for entry in entries:
                producer.send(
                    entry.info.original_topic,
                    entry.message.value,
                    key=entry.message.key,
                    headers=replay_headers(entry.message),
                )
            producer.flush()
        finally:
            producer.close()
        return list(entries)

    def replay_entries(
        self,
        *,
        select: Callable[[DeadLetter], bool] | None = None,
        limit: int | None = None,
    ) -> list[DeadLetter]:
        """Republish matching dead letters and return the ones that were moved.

        The entries, not just a count: after a replay the operator's next move is
        to follow those records through the logs, which needs their event ids.
        """
        return self.replay_all(self.matching(select, limit))

    def replay(
        self,
        *,
        select: Callable[[DeadLetter], bool] | None = None,
        limit: int | None = None,
    ) -> int:
        """Republish matching dead letters to their original topic.

        Returns how many were replayed. ``select`` filters (default: everything);
        ``limit`` caps the count for a canary replay — after a bad deploy you
        replay five records, watch them land, then replay the rest.
        """
        return len(self.replay_entries(select=select, limit=limit))
