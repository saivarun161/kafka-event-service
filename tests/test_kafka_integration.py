"""Integration tests against a real Kafka broker.

Skipped unless ``KAFKA_BOOTSTRAP_SERVERS`` is set (CI provides a Kafka service
container; locally, point it at any reachable broker). Topic names are
suffixed with a per-run id so repeated runs on a shared broker do not collide.

The point of this file is the project's central claim: the service code that
runs here is byte-for-byte the code the in-memory tests exercise — only the
broker fixture differs.
"""

import os
import time
import uuid

import pytest

from eventsvc import EventService, RetryPolicy
from eventsvc.samples import OrderHandler, produce_orders

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(
        not BOOTSTRAP, reason="KAFKA_BOOTSTRAP_SERVERS not set; no broker to test against"
    ),
]


@pytest.fixture
def broker():
    kafka = pytest.importorskip("eventsvc.kafka", reason="confluent-kafka not installed")
    broker = kafka.KafkaBroker(BOOTSTRAP, default_partitions=3)
    yield broker
    broker.close()


@pytest.fixture
def topic(broker):
    name = f"orders-{uuid.uuid4().hex[:8]}"
    broker.create_topic(name, 3)
    return name


def test_roundtrip_produce_consume(broker, topic):
    producer = broker.producer()
    metadata = producer.send(topic, {"n": 1}, key="acme", headers={"x-event-id": "evt-1"})
    assert metadata.topic == topic
    producer.close()

    consumer = broker.consumer(f"{topic}-readers")
    try:
        consumer.subscribe([topic])
        messages = []
        for _ in range(50):
            messages = consumer.poll(max_records=10, timeout=1.0)
            if messages:
                break
        assert len(messages) == 1
        message = messages[0]
        assert message.value == {"n": 1}
        assert message.key == "acme"
        assert message.header("x-event-id") == "evt-1"
    finally:
        consumer.close()


def test_committed_offsets_survive_consumer_restart(broker, topic):
    producer = broker.producer()
    producer.send(topic, {"n": 1}, key="k")
    producer.close()
    group = f"{topic}-restart"

    consumer = broker.consumer(group)
    consumer.subscribe([topic])
    got = []
    for _ in range(50):
        got = consumer.poll(max_records=10, timeout=1.0)
        if got:
            break
    assert len(got) == 1
    consumer.commit()
    consumer.close()

    replacement = broker.consumer(group)
    try:
        replacement.subscribe([topic])
        for _ in range(5):
            assert replacement.poll(max_records=10, timeout=1.0) == []
    finally:
        replacement.close()


def test_full_service_story_on_real_kafka(broker, topic):
    """The sample order stream, the retry ladder, the DLQ, and a replay —
    identical semantics to the in-memory run, on a real broker."""
    handler = OrderHandler()
    service = EventService(
        broker=broker,
        topic=topic,
        handler=handler,
        # Tight delays: the ladder is exercised for real, without minutes of CI time.
        policy=RetryPolicy(max_attempts=4, base_delay=0.2, multiplier=2.0, max_delay=1.0),
        partitions=3,
    )
    produce_orders(broker.producer(), topic)

    service.start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            stats = service.stats()
            if stats.ok >= 10 and stats.dead_lettered >= 2:
                break
            time.sleep(0.5)
    finally:
        service.stop(timeout=10)

    stats = service.stats()
    assert stats.ok == 10
    assert stats.dead_lettered == 2

    processed = set(handler.processed_ids())
    assert "ord-1003" in processed and "ord-1008" in processed  # recovered by the ladder
    assert "ord-1005" not in processed and "ord-1011" not in processed

    dead = service.dlq.entries()
    dead_ids = {entry.message.value["order_id"] for entry in dead}
    assert dead_ids == {"ord-1005", "ord-1011"}
    exhausted = next(e for e in dead if e.message.value["order_id"] == "ord-1011")
    assert exhausted.info.attempts == 4
    assert exhausted.info.original_topic == topic

    # Replay after "fixing" the downstream: the retriable failure recovers.
    handler.downstream_fixed = True
    assert service.dlq.replay(select=lambda e: e.info.error_type != "PermanentError") == 1
    replay_service = EventService(
        broker=broker,
        topic=topic,
        handler=handler,
        group_id=f"{topic}-workers",
        policy=RetryPolicy(max_attempts=4, base_delay=0.2, multiplier=2.0, max_delay=1.0),
        partitions=3,
    )
    replay_service.start()
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            if "ord-1011" in handler.processed_ids():
                break
            time.sleep(0.5)
    finally:
        replay_service.stop(timeout=10)
    assert "ord-1011" in handler.processed_ids()


def test_lag_is_read_from_broker_metadata_without_joining_the_group(broker, topic):
    """The exporter's two claims, against a real coordinator.

    On a real broker the distinction it depends on is not a detail of the
    in-memory implementation: an unread group genuinely has ``OFFSET_INVALID``
    at the coordinator, and a real rebalance would genuinely halve a member's
    assignment if the exporter joined.
    """
    from eventsvc import LagExporter

    group = f"{topic}-workers"
    producer = broker.producer()
    for index in range(9):
        producer.send(topic, {"n": index}, key=f"k{index}")
    producer.close()

    exporter = LagExporter(broker, group, [topic])
    before = exporter.snapshot()
    assert len(before.partitions) == 3
    assert before.total == 9
    # Nothing has ever committed here: that must not read back as offset 0.
    assert len(before.uncommitted()) == 3
    assert all(entry.low == 0 for entry in before.partitions)

    consumer = broker.consumer(group, client_id="worker-1")
    consumer.subscribe([topic])
    consumed = []
    deadline = time.time() + 60
    while len(consumed) < 9 and time.time() < deadline:
        consumed.extend(consumer.poll(max_records=9, timeout=1.0))
    assert len(consumed) == 9
    consumer.commit()
    assignment = consumer.assignment()
    assert len(assignment) == 3

    caught_up = exporter.snapshot()
    assert caught_up.total == 0
    assert not caught_up.uncommitted()
    # Sampling repeatedly must not cost the live member any partitions: a second
    # group member would take a share of them in the rebalance.
    for _ in range(3):
        exporter.snapshot()
    assert sorted(consumer.assignment()) == sorted(assignment)
    consumer.close()

    # With the fleet gone the group still reports — that is the whole point.
    producer = broker.producer()
    for index in range(4):
        producer.send(topic, {"n": 100 + index}, key=f"k{index}")
    producer.close()
    after = exporter.snapshot()
    assert after.total == 4
    assert not after.uncommitted()
