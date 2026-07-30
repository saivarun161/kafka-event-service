"""The lag exporter: correct arithmetic, and — the point of it — measured from outside.

Two properties carry this module and both are pinned here: the exporter reports
partitions that no live consumer owns (the crashed-fleet case that
``Consumer.lag`` cannot see), and reading lag leaves the group exactly as it was.
"""

import threading
import time

import pytest

from eventsvc import EventService, InMemoryBroker, LagExporter, ManualClock, Metrics
from eventsvc.cli import run_lag
from eventsvc.errors import BrokerError
from eventsvc.lag import LagSnapshot, PartitionLag, format_snapshot
from eventsvc.types import TopicPartition


@pytest.fixture
def broker():
    return InMemoryBroker(default_partitions=3, clock=ManualClock())


def produce(broker, topic, count, *, start=0):
    producer = broker.producer()
    for index in range(start, start + count):
        # Keys chosen only for spread; the partitioner decides where they land.
        producer.send(topic, {"n": index}, key=f"k{index}")
    producer.close()


def consume(broker, topic, group, *, records=100, commit=True):
    """Read as a real group member would, so the committed offsets are real ones."""
    consumer = broker.consumer(group)
    consumer.subscribe([topic])
    got = consumer.poll(max_records=records, timeout=0.1)
    if commit:
        consumer.commit()
    consumer.close()
    return got


# -- the broker's side of it ----------------------------------------------


def test_watermarks_cover_every_partition_including_empty_ones(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 4)

    marks = broker.watermarks("orders")
    assert set(marks) == {TopicPartition("orders", index) for index in range(3)}
    assert sum(high for _low, high in marks.values()) == 4
    assert all(low == 0 for low, _high in marks.values())


def test_committed_distinguishes_never_committed_from_committed_at_zero(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 2)

    assert broker.committed("readers", "orders") == {TopicPartition("orders", 0): None}

    # A consumer that joins and commits without reading anything commits 0 —
    # a different situation, and it must not read back as "never ran".
    consumer = broker.consumer("readers")
    consumer.subscribe(["orders"])
    consumer.position(TopicPartition("orders", 0))
    consumer.commit()
    consumer.close()
    assert broker.committed("readers", "orders") == {TopicPartition("orders", 0): 0}


def test_offset_reads_reject_unknown_topics(broker):
    with pytest.raises(BrokerError):
        broker.watermarks("nope")
    with pytest.raises(BrokerError):
        broker.committed("g", "nope")


# -- lag arithmetic --------------------------------------------------------


def test_lag_counts_from_the_commit():
    entry = PartitionLag(TopicPartition("orders", 0), committed=4, low=0, high=10)
    assert entry.lag == 6
    assert not entry.uncommitted
    assert not entry.behind_retention


def test_uncommitted_partition_counts_the_whole_retained_backlog():
    entry = PartitionLag(TopicPartition("orders", 0), committed=None, low=0, high=10)
    assert entry.lag == 10
    assert entry.uncommitted


def test_commit_behind_retention_is_flagged_and_does_not_inflate_the_lag():
    # Retention deleted offsets 0-99 before this group read them. Its next read
    # jumps to 100, so the backlog is 20 — not the 120 a naive subtraction gives.
    entry = PartitionLag(TopicPartition("orders", 0), committed=0, low=100, high=120)
    assert entry.lag == 20
    assert entry.behind_retention


def test_caught_up_partition_has_no_lag():
    entry = PartitionLag(TopicPartition("orders", 0), committed=10, low=0, high=10)
    assert entry.lag == 0


# -- snapshots -------------------------------------------------------------


def test_snapshot_totals_and_worst_partition(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 12)
    exporter = LagExporter(broker, "orders-workers", ["orders"], clock=ManualClock())

    snapshot = exporter.snapshot()
    assert snapshot.group == "orders-workers"
    assert snapshot.total == 12
    assert len(snapshot.partitions) == 3
    assert snapshot.max_lag == max(entry.lag for entry in snapshot.partitions)
    assert snapshot.worst.lag == snapshot.max_lag
    assert snapshot.by_topic() == {"orders": 12}


def test_snapshot_reflects_progress_after_a_group_commits(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 12)
    exporter = LagExporter(broker, "orders-workers", ["orders"])
    assert exporter.snapshot().total == 12

    consume(broker, "orders", "orders-workers", records=5)
    after = exporter.snapshot()
    assert after.total == 7
    assert len(after.uncommitted()) < 3


def test_lag_is_still_reported_after_every_consumer_dies(broker):
    """The whole reason the exporter exists.

    A consumer's own ``lag()`` reports its assignment, so once the fleet is gone
    it reports nothing at all and the alert has no series to fire on. The
    exporter reads the group's committed offsets instead, so the backlog stays
    visible — and keeps growing — with nobody consuming.
    """
    broker.create_topic("orders", 3)
    produce(broker, "orders", 6)
    consumer = broker.consumer("orders-workers")
    consumer.subscribe(["orders"])
    consumer.poll(max_records=6, timeout=0.1)
    consumer.commit()

    exporter = LagExporter(broker, "orders-workers", ["orders"])
    assert exporter.snapshot().total == 0

    consumer.close()  # the fleet dies
    assert consumer.lag() == {}  # ... and takes the inside view with it
    produce(broker, "orders", 9, start=100)

    snapshot = exporter.snapshot()
    assert snapshot.total == 9
    assert not snapshot.uncommitted()  # the group exists; it is simply not running


def test_reading_lag_does_not_join_the_group_or_move_offsets(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 9)
    worker = broker.consumer("orders-workers", client_id="worker-1")
    worker.subscribe(["orders"])
    before_assignment = worker.assignment()
    assert len(before_assignment) == 3

    exporter = LagExporter(broker, "orders-workers", ["orders"])
    for _ in range(3):
        exporter.snapshot()

    # A second member would halve this assignment; an exporter must not be one.
    assert worker.assignment() == before_assignment
    assert broker._members[("orders-workers", "orders")] == ["worker-1"]
    # And nothing was consumed out from under it.
    assert len(worker.poll(max_records=9, timeout=0.1)) == 9
    worker.close()


def test_unknown_topics_are_reported_not_raised(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 2)
    exporter = LagExporter(broker, "g", ["orders", "orders.retry.9s"])

    snapshot = exporter.snapshot()
    assert snapshot.unknown_topics == ("orders.retry.9s",)
    assert snapshot.total == 2


def test_snapshot_is_serializable(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 3)
    exporter = LagExporter(broker, "g", ["orders"], clock=ManualClock(start=1000.0))

    payload = exporter.snapshot().as_dict()
    assert payload["group"] == "g"
    assert payload["taken_at"] == 1000.0
    assert payload["total_lag"] == 3
    assert payload["by_topic"] == {"orders": 3}
    assert payload["partitions"][0]["uncommitted"] is True


def test_empty_snapshot_has_no_worst_partition():
    snapshot = LagSnapshot(group="g", taken_at=0.0)
    assert snapshot.total == 0
    assert snapshot.max_lag == 0
    assert snapshot.worst is None


def test_exporter_requires_topics(broker):
    with pytest.raises(ValueError, match="at least one topic"):
        LagExporter(broker, "g", [])


# -- metrics ---------------------------------------------------------------


def test_export_publishes_lag_and_both_offsets(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 5)
    consume(broker, "orders", "orders-workers", records=2)
    metrics = Metrics()
    exporter = LagExporter(broker, "orders-workers", ["orders"], metrics=metrics)

    exporter.export()
    labels = {"topic": "orders", "partition": "0", "group": "orders-workers"}
    assert metrics.value("eventsvc_consumer_lag", **labels) == 3
    assert metrics.value("eventsvc_committed_offset", **labels) == 2
    assert metrics.value("eventsvc_log_end_offset", topic="orders", partition="0") == 5


def test_uncommitted_partition_publishes_no_committed_offset_sample(broker):
    """A zero here would read as a consumer sitting still, which is a lie."""
    broker.create_topic("orders", 1)
    produce(broker, "orders", 4)
    metrics = Metrics()
    LagExporter(broker, "orders-workers", ["orders"], metrics=metrics).export()

    labels = {"topic": "orders", "partition": "0", "group": "orders-workers"}
    assert metrics.value("eventsvc_consumer_lag", **labels) == 4
    assert metrics.registry.get_sample_value("eventsvc_committed_offset", labels) is None


def test_repeated_exports_overwrite_rather_than_accumulate(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 4)
    metrics = Metrics()
    exporter = LagExporter(broker, "orders-workers", ["orders"], metrics=metrics)
    exporter.export()
    consume(broker, "orders", "orders-workers", records=4)
    exporter.export()

    labels = {"topic": "orders", "partition": "0", "group": "orders-workers"}
    assert metrics.value("eventsvc_consumer_lag", **labels) == 0


# -- the sampling loop -----------------------------------------------------


class CountingReader:
    """An :class:`~eventsvc.broker.OffsetReader` and nothing else.

    Doubles as a check that the exporter really does depend on the two-method
    protocol rather than on a whole broker.
    """

    def __init__(self, broker):
        self._broker = broker
        self.reads = 0

    def watermarks(self, topic):
        self.reads += 1
        return self._broker.watermarks(topic)

    def committed(self, group_id, topic):
        return self._broker.committed(group_id, topic)


def test_run_takes_exactly_the_requested_number_of_samples(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 2)
    reader = CountingReader(broker)
    exporter = LagExporter(reader, "g", ["orders"])

    last = exporter.run(interval=0.01, rounds=3)
    assert reader.reads == 3
    assert last.total == 2


def test_run_returns_promptly_when_stopped_mid_interval(broker):
    broker.create_topic("orders", 1)
    stop = threading.Event()
    exporter = LagExporter(broker, "g", ["orders"])
    # The interval is waited on the stop event, not slept through: setting the
    # event has to end the loop now rather than half a minute from now.
    threading.Timer(0.05, stop.set).start()
    started = time.monotonic()
    exporter.run(interval=30.0, stop=stop)
    assert time.monotonic() - started < 5.0


def test_run_rejects_a_non_positive_interval(broker):
    broker.create_topic("orders", 1)
    with pytest.raises(ValueError, match="interval must be > 0"):
        LagExporter(broker, "g", ["orders"]).run(interval=0)


def test_start_and_stop_run_the_loop_on_a_thread(broker):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 3)
    metrics = Metrics()
    exporter = LagExporter(broker, "g", ["orders"], metrics=metrics)

    with exporter:
        for _ in range(200):
            if metrics.value("eventsvc_consumer_lag", topic="orders", partition="0", group="g"):
                break
            threading.Event().wait(0.01)
    assert metrics.value("eventsvc_consumer_lag", topic="orders", partition="0", group="g") == 3
    assert exporter._thread is None


def test_starting_twice_is_an_error(broker):
    broker.create_topic("orders", 1)
    exporter = LagExporter(broker, "g", ["orders"])
    exporter.start(interval=30.0)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            exporter.start()
    finally:
        assert exporter.stop()


def test_stopping_an_unstarted_exporter_is_fine(broker):
    broker.create_topic("orders", 1)
    assert LagExporter(broker, "g", ["orders"]).stop() is True


# -- service integration ---------------------------------------------------


def test_service_exporter_watches_the_whole_topology(broker):
    service = EventService(
        broker=broker, topic="orders", handler=lambda message: None, clock=ManualClock()
    )
    exporter = service.lag_exporter()

    assert exporter.group_id == "orders-workers"
    assert exporter.topics == service.policy.topics_for("orders")
    assert "orders.dlq" in exporter.topics
    assert exporter.metrics is service.metrics
    assert exporter.snapshot().unknown_topics == ()


def test_service_exporter_can_leave_out_the_dlq(broker):
    service = EventService(
        broker=broker, topic="orders", handler=lambda message: None, clock=ManualClock()
    )
    exporter = service.lag_exporter(include_dlq=False)
    assert "orders.dlq" not in exporter.topics
    assert "orders.retry.1s" in exporter.topics


def test_dead_letters_show_up_as_lag_on_the_dlq_topic(broker):
    """Nothing consumes the DLQ, so its lag is the count of unhandled dead letters."""

    def always_fails(message):
        raise RuntimeError("nope")

    service = EventService(broker=broker, topic="orders", handler=always_fails, clock=ManualClock())
    produce(broker, "orders", 2)
    service.run_until_idle()

    snapshot = service.lag_exporter().snapshot()
    assert snapshot.by_topic()["orders.dlq"] == 2
    assert snapshot.by_topic()["orders"] == 0  # the source was drained and committed


# -- rendering and the CLI -------------------------------------------------


def test_format_snapshot_leads_with_the_total(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 7)
    snapshot = LagExporter(broker, "orders-workers", ["orders"]).snapshot()

    lines = list(format_snapshot(snapshot))
    assert "7 record(s) behind" in lines[0]
    assert "orders-workers" in lines[0]
    assert any("never committed" in line for line in lines)


def test_format_snapshot_verbose_lists_every_partition(broker):
    broker.create_topic("orders", 3)
    produce(broker, "orders", 7)
    consume(broker, "orders", "orders-workers", records=7)
    snapshot = LagExporter(broker, "orders-workers", ["orders"]).snapshot()

    body = "\n".join(format_snapshot(snapshot, verbose=True))
    for partition in range(3):
        assert f"orders[{partition}]" in body
    assert "committed=" in body


def test_format_snapshot_names_partitions_behind_retention():
    snapshot = LagSnapshot(
        group="g",
        taken_at=0.0,
        partitions=(PartitionLag(TopicPartition("orders", 1), committed=0, low=50, high=60),),
    )
    body = "\n".join(format_snapshot(snapshot))
    assert "behind retention: orders[1]" in body
    assert "aged out" in body


def test_run_lag_prints_once_by_default(broker, capsys):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 4)
    exporter = LagExporter(broker, "orders-workers", ["orders"])

    assert run_lag(exporter) == 0
    assert capsys.readouterr().out.count("── lag") == 1


def test_run_lag_watch_stops_after_the_requested_samples(broker, capsys):
    broker.create_topic("orders", 1)
    produce(broker, "orders", 4)
    # A ManualClock makes --watch instant: the interval is advanced, not waited.
    exporter = LagExporter(broker, "orders-workers", ["orders"], clock=ManualClock())

    assert run_lag(exporter, watch=30.0, samples=3) == 0
    assert capsys.readouterr().out.count("── lag") == 3


def test_run_lag_json_is_machine_readable(broker, capsys):
    import json

    broker.create_topic("orders", 2)
    produce(broker, "orders", 5)
    exporter = LagExporter(broker, "orders-workers", ["orders"])

    assert run_lag(exporter, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total_lag"] == 5
    assert len(payload["partitions"]) == 2


def test_lag_command_refuses_without_a_broker(capsys):
    from eventsvc.cli import main

    assert main(["lag"]) == 2
    assert "needs --kafka" in capsys.readouterr().err


def test_lag_command_parses_the_topology_flags():
    from eventsvc.cli import build_parser

    args = build_parser().parse_args(
        ["lag", "--topic", "payments", "--group", "billing", "--kafka", "b:9092", "--watch", "5"]
    )
    assert args.topic == "payments"
    assert args.group == "billing"
    assert args.watch == 5.0
    assert args.source_only is False
