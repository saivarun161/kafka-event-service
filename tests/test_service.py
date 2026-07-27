"""The assembled service: topology creation, both run modes, end-to-end flow."""

import pytest

from eventsvc import (
    EventService,
    InMemoryBroker,
    ManualClock,
    Metrics,
    RetryPolicy,
    SystemClock,
)
from eventsvc.samples import OrderHandler, produce_orders, sample_orders


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def broker(clock):
    return InMemoryBroker(default_partitions=3, clock=clock)


def build_service(broker, clock, handler, **kwargs):
    kwargs.setdefault("policy", RetryPolicy())
    return EventService(
        broker=broker, topic="orders", handler=handler, clock=clock, partitions=3, **kwargs
    )


def test_topology_is_created_up_front(broker, clock):
    build_service(broker, clock, lambda m: None)
    assert broker.topics() == [
        "orders",
        "orders.dlq",
        "orders.retry.1s",
        "orders.retry.3s",
        "orders.retry.9s",
    ]
    assert all(broker.partition_count(t) == 3 for t in broker.topics())


def test_sample_stream_end_to_end(broker, clock):
    """The full story: flakes recover through the ladder, lost causes land in the DLQ."""
    handler = OrderHandler()
    service = build_service(broker, clock, handler)
    produced = produce_orders(broker.producer(), "orders")
    stats = service.run_until_idle()

    assert produced == 12
    # ord-1005 (unknown product) and ord-1011 (fails forever) are dead-lettered.
    assert stats.dead_lettered == 2
    # ord-1003 flakes twice and ord-1008 once: 3 tier hops for those, plus
    # ord-1011 burning the whole ladder (3 hops) = 6 republishes.
    assert stats.retried == 6
    assert stats.ok == 10
    processed = set(handler.processed_ids())
    assert "ord-1003" in processed  # recovered on the second tier
    assert "ord-1008" in processed  # recovered on the first tier
    assert "ord-1005" not in processed
    assert "ord-1011" not in processed


def test_retry_delays_are_actually_observed(broker, clock):
    """A record that flakes twice cannot complete before base + base*3 seconds."""
    handler = OrderHandler()
    service = build_service(broker, clock, handler)
    start = clock.now()
    broker.producer().send(
        "orders",
        {"order_id": "ord-1", "customer": "acme", "product": "desk", "fail_times": 2},
        key="acme",
        headers={"x-event-id": "evt-1"},
    )
    service.run_until_idle()
    assert handler.processed_ids() == ["ord-1"]
    assert clock.now() - start >= 3.999999  # 1s tier + 3s tier (tiny scheduler nudge aside)


def test_ordering_preserved_per_key_through_success_path(broker, clock):
    seen = []

    def handler(message):
        seen.append(message.value["n"])

    service = build_service(broker, clock, handler)
    producer = broker.producer()
    for n in range(8):
        producer.send("orders", {"n": n}, key="one-customer", headers={"x-event-id": f"e{n}"})
    service.run_until_idle()
    assert seen == list(range(8))


def test_consumers_per_topic_splits_the_partitions(broker, clock):
    service = build_service(broker, clock, lambda m: None, consumers_per_topic=3)
    source_workers = [w for w in service.workers if w.topic == "orders"]
    assert len(source_workers) == 3
    assignments = [set(w._consumer.assignment()) for w in source_workers]
    assert sum(len(a) for a in assignments) == 3
    assert len(set.union(*assignments)) == 3  # every partition owned exactly once, no overlap


def test_duplicate_deliveries_are_absorbed(broker, clock):
    handler = OrderHandler()
    service = build_service(broker, clock, handler)
    order = {"order_id": "ord-1", "customer": "acme", "product": "lamp"}
    producer = broker.producer()
    producer.send("orders", order, key="acme", headers={"x-event-id": "same-id"})
    producer.send("orders", order, key="acme", headers={"x-event-id": "same-id"})
    stats = service.run_until_idle()
    assert stats.ok == 1
    assert stats.duplicates == 1
    assert len(handler.processed) == 1


def test_threaded_mode_processes_the_stream():
    """Same workload, real threads, real clock, tight delays."""
    clock = SystemClock()
    broker = InMemoryBroker(default_partitions=3, clock=clock)
    handler = OrderHandler()
    service = EventService(
        broker=broker,
        topic="orders",
        handler=handler,
        policy=RetryPolicy(max_attempts=4, base_delay=0.05, multiplier=2.0, max_delay=0.2),
        clock=clock,
        partitions=3,
    )
    produce_orders(broker.producer(), "orders")
    service.start()
    try:
        deadline = clock.now() + 10.0
        while clock.now() < deadline:
            stats = service.stats()
            if stats.ok >= 10 and stats.dead_lettered >= 2:
                break
            clock.sleep(0.05)
    finally:
        service.stop()

    stats = service.stats()
    assert stats.ok == 10
    assert stats.dead_lettered == 2
    assert len(service.dead_letters()) == 2


def test_threaded_mode_cannot_start_twice(broker, clock):
    service = build_service(broker, clock, lambda m: None)
    service.start()
    try:
        with pytest.raises(RuntimeError):
            service.start()
    finally:
        service.stop()


def test_stats_by_topic_covers_the_ladder(broker, clock):
    handler = OrderHandler()
    service = build_service(broker, clock, handler)
    produce_orders(broker.producer(), "orders")
    service.run_until_idle()
    by_topic = service.stats_by_topic()
    assert by_topic["orders"].consumed == 12
    assert by_topic["orders.retry.1s"].consumed == 3  # ord-1003, ord-1008, ord-1011
    assert by_topic["orders.retry.3s"].consumed == 2  # ord-1003, ord-1011
    assert by_topic["orders.retry.9s"].consumed == 1  # ord-1011


def test_metrics_expose_the_run(broker, clock):
    metrics = Metrics()
    handler = OrderHandler()
    service = build_service(broker, clock, handler, metrics=metrics)
    produce_orders(broker.producer(), "orders")
    service.run_until_idle()

    assert (
        metrics.value(
            "eventsvc_messages_processed_total",
            topic="orders",
            group="orders-workers",
            outcome="ok",
        )
        == 8
    )
    assert metrics.value("eventsvc_dead_letters_total", topic="orders", error="PermanentError") == 1
    assert metrics.value("eventsvc_dead_letters_total", topic="orders", error="RetriableError") == 1
    rendered = metrics.render()
    assert "eventsvc_consumer_lag" in rendered
    assert "eventsvc_handler_seconds_bucket" in rendered


def test_sample_orders_are_json_serializable():
    import json

    assert json.loads(json.dumps(sample_orders())) == sample_orders()
