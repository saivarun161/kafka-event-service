"""Consumer lag, measured from outside the group.

Lag is the first number anyone looks at when a consumer is suspected, and the
obvious way to produce it is from inside the consumer: ask it what it owns, ask
the broker for the high watermark, subtract. That is what
:meth:`Consumer.lag <eventsvc.broker.Consumer.lag>` does, and it has a failure
mode that matters more than its convenience — **it can only report partitions
that a living consumer is currently assigned.**

So when the consumers crash, the lag series does not spike. It stops being
exported at all. Prometheus holds the last value for five minutes and then the
partition simply vanishes from the graph, and an alert written the natural way
(``eventsvc_consumer_lag > 10000``) never fires, because there is no longer a
series to evaluate. The outage that most needs the metric is the one that
deletes it.

This module measures the same quantity from the other side. It reads the
group's **committed offsets** and the **log end offsets** out of the broker's
metadata — the group coordinator and the partition leaders — and never
subscribes to anything. Consequences worth the module:

* **A dead group still reports.** Every partition of every topic is covered
  whether or not anyone is consuming it, so lag keeps climbing visibly after the
  last worker dies. That is the signal.
* **Measuring does not perturb.** An exporter that subscribed in order to
  observe would join the rebalance, take a share of the partitions, and consume
  records the real workers then never see. Observation would cause the incident.
* **It can run anywhere.** A sidecar, a cron job, or in-process next to the
  workers — it needs a broker address and a group name, not a seat in the group.

The honest edge cases are kept distinct rather than flattened into a number:
a partition the group has never committed reports ``committed=None`` (a group
that has not started is not the same as one caught up at offset 0), and a
committed offset that has fallen behind the low watermark is flagged, because
that group's next read will silently skip records that retention already
deleted.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from .broker import OffsetReader
from .clock import Clock, SystemClock
from .errors import BrokerError
from .metrics import Metrics
from .types import TopicPartition

DEFAULT_INTERVAL = 15.0
"""Seconds between samples in :meth:`LagExporter.run`. Lag is a trend, not an event."""


@dataclass(frozen=True, slots=True)
class PartitionLag:
    """One partition's position: where the group committed, and where the log ends."""

    partition: TopicPartition
    committed: int | None
    low: int
    high: int

    @property
    def topic(self) -> str:
        return self.partition.topic

    @property
    def lag(self) -> int:
        """Records the group has not consumed yet.

        Measured from the committed offset, or — where the group has never
        committed — from the low watermark, because a consumer starting fresh
        with ``auto.offset.reset=earliest`` really does have the whole retained
        partition ahead of it. Clamped at the low watermark so a stale commit
        pointing at deleted records cannot inflate the backlog past what exists.
        """
        floor = self.low if self.committed is None else max(self.committed, self.low)
        return max(0, self.high - floor)

    @property
    def uncommitted(self) -> bool:
        """The group has never committed here: nothing has consumed this partition."""
        return self.committed is None

    @property
    def behind_retention(self) -> bool:
        """The commit points before the first surviving record — those records are gone.

        Not a lag problem but a data-loss one, and invisible in the lag number
        itself: the group will resume from the low watermark and the skipped
        records are never delivered to anyone.
        """
        return self.committed is not None and self.committed < self.low

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition.partition,
            "committed": self.committed,
            "low": self.low,
            "high": self.high,
            "lag": self.lag,
            "uncommitted": self.uncommitted,
            "behind_retention": self.behind_retention,
        }


@dataclass(frozen=True, slots=True)
class LagSnapshot:
    """Every partition of every watched topic, as of one moment."""

    group: str
    taken_at: float
    partitions: tuple[PartitionLag, ...] = ()
    unknown_topics: tuple[str, ...] = ()
    """Watched topics the broker does not have. A retry tier can be configured
    before it is created; a monitoring loop must not die of it."""

    @property
    def total(self) -> int:
        return sum(entry.lag for entry in self.partitions)

    @property
    def max_lag(self) -> int:
        """The worst partition. The one that decides whether anybody is paged."""
        return max((entry.lag for entry in self.partitions), default=0)

    @property
    def worst(self) -> PartitionLag | None:
        """The partition holding :attr:`max_lag`, ties broken by partition order."""
        return max(self.partitions, key=lambda e: (e.lag, -e.partition.partition), default=None)

    def by_topic(self) -> dict[str, int]:
        """Total lag per topic, in the order the topics were watched."""
        totals: dict[str, int] = {}
        for entry in self.partitions:
            totals[entry.topic] = totals.get(entry.topic, 0) + entry.lag
        return totals

    def uncommitted(self) -> tuple[PartitionLag, ...]:
        return tuple(entry for entry in self.partitions if entry.uncommitted)

    def behind_retention(self) -> tuple[PartitionLag, ...]:
        return tuple(entry for entry in self.partitions if entry.behind_retention)

    def as_dict(self) -> dict[str, Any]:
        return {
            "group": self.group,
            "taken_at": self.taken_at,
            "total_lag": self.total,
            "max_lag": self.max_lag,
            "by_topic": self.by_topic(),
            "partitions": [entry.as_dict() for entry in self.partitions],
            "unknown_topics": list(self.unknown_topics),
        }


class LagExporter:
    """Samples a group's lag from the broker's metadata and publishes it.

    ``broker`` only has to satisfy :class:`~eventsvc.broker.OffsetReader` —
    ``watermarks`` and ``committed``. Both brokers in this project do, so the
    exporter is exercised offline against the in-memory log and runs unchanged
    against Kafka.
    """

    def __init__(
        self,
        broker: OffsetReader,
        group_id: str,
        topics: Sequence[str],
        *,
        metrics: Metrics | None = None,
        clock: Clock | None = None,
    ) -> None:
        if not topics:
            raise ValueError("at least one topic is required")
        self.broker = broker
        self.group_id = group_id
        self.topics = tuple(topics)
        self.metrics = metrics if metrics is not None else Metrics()
        self.clock = clock or SystemClock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    # -- sampling ----------------------------------------------------------

    def snapshot(self) -> LagSnapshot:
        """Read every watched topic once. Does not touch the metrics."""
        entries: list[PartitionLag] = []
        unknown: list[str] = []
        for topic in self.topics:
            try:
                watermarks = self.broker.watermarks(topic)
                committed = self.broker.committed(self.group_id, topic)
            except BrokerError:
                # A topic that does not exist yet is a fact about the deployment,
                # not an error in the exporter: report it and keep sampling the
                # rest, or one un-created retry tier takes the whole loop down.
                unknown.append(topic)
                continue
            for tp in sorted(watermarks):
                low, high = watermarks[tp]
                entries.append(PartitionLag(tp, committed.get(tp), low, high))
        return LagSnapshot(
            group=self.group_id,
            taken_at=self.clock.now(),
            partitions=tuple(entries),
            unknown_topics=tuple(unknown),
        )

    def export(self) -> LagSnapshot:
        """Sample once and publish the result to the Prometheus gauges."""
        snapshot = self.snapshot()
        self.publish(snapshot)
        return snapshot

    def publish(self, snapshot: LagSnapshot) -> None:
        """Write ``snapshot`` to the gauges.

        Committed and end offsets are exported alongside the lag because lag on
        its own cannot say *which side* stalled: a flat backlog is either a
        stopped consumer or a stopped producer, and those are opposite pages.
        Comparing ``rate(committed_offset)`` with ``rate(log_end_offset)``
        separates them in one panel.

        A partition with no commit sets no ``committed_offset`` sample at all.
        Publishing a zero there would be a lie that ``rate()`` reads as a
        consumer sitting perfectly still.
        """
        for entry in snapshot.partitions:
            labels = {
                "topic": entry.topic,
                "partition": str(entry.partition.partition),
                "group": snapshot.group,
            }
            self.metrics.lag.labels(**labels).set(entry.lag)
            self.metrics.log_end_offset.labels(
                topic=entry.topic, partition=str(entry.partition.partition)
            ).set(entry.high)
            if entry.committed is not None:
                self.metrics.committed_offset.labels(**labels).set(entry.committed)

    # -- running as a loop -------------------------------------------------

    def run(
        self,
        interval: float = DEFAULT_INTERVAL,
        *,
        rounds: int | None = None,
        stop: threading.Event | None = None,
    ) -> LagSnapshot | None:
        """Sample every ``interval`` seconds until stopped, returning the last snapshot.

        Blocks the calling thread — this is the body of :meth:`start`, and also
        what a standalone exporter process runs directly. ``rounds`` bounds the
        loop for a caller that wants exactly N samples. The wait happens on the
        stop event rather than the clock, so a shutdown is not held up by an
        interval that has only just begun.
        """
        if interval <= 0:
            raise ValueError("interval must be > 0")
        event = stop if stop is not None else self._stop
        event.clear()
        last: LagSnapshot | None = None
        taken = 0
        while not event.is_set():
            last = self.export()
            taken += 1
            if rounds is not None and taken >= rounds:
                break
            if event.wait(interval):
                break
        return last

    def start(self, interval: float = DEFAULT_INTERVAL) -> None:
        """Run :meth:`run` on a daemon thread."""
        if self._thread is not None:
            raise RuntimeError("exporter is already running")
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run, args=(interval,), name=f"lag-exporter-{self.group_id}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> bool:
        """Ask the loop to finish and wait for it. Returns whether the thread ended."""
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    def __enter__(self) -> LagExporter:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()


def format_snapshot(snapshot: LagSnapshot, *, verbose: bool = False) -> Iterable[str]:
    """Render a snapshot the way an operator reads it: totals first, detail on request."""
    yield f"── lag — group {snapshot.group!r}, {snapshot.total} record(s) behind"
    totals = snapshot.by_topic()
    if totals:
        width = max(len(name) for name in totals)
        for topic, total in totals.items():
            yield f"  {topic:<{width}}  {total}"
    worst = snapshot.worst
    if worst is not None and worst.lag:
        yield f"\n  worst partition: {worst.partition} — {worst.lag} behind"
    stale = snapshot.uncommitted()
    if stale:
        names = ", ".join(str(entry.partition) for entry in stale[:6])
        more = f" (+{len(stale) - 6} more)" if len(stale) > 6 else ""
        yield f"  never committed: {names}{more}"
    lost = snapshot.behind_retention()
    if lost:
        names = ", ".join(str(entry.partition) for entry in lost)
        yield f"  behind retention: {names} — records aged out before the group read them"
    if snapshot.unknown_topics:
        yield f"  not on the broker: {', '.join(snapshot.unknown_topics)}"
    if verbose:
        yield ""
        for entry in snapshot.partitions:
            committed = "-" if entry.committed is None else str(entry.committed)
            yield (
                f"  {entry.partition!s:<28} committed={committed:<8} "
                f"end={entry.high:<8} lag={entry.lag}"
            )
