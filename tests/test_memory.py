"""The in-memory broker must honour log semantics, not queue semantics."""

import pytest

from eventsvc import BrokerError, InMemoryBroker, ManualClock, TopicPartition
from eventsvc.broker import partition_for_key


@pytest.fixture
def broker():
    return InMemoryBroker(default_partitions=3, clock=ManualClock())


def test_produce_returns_topic_partition_offset(broker):
    producer = broker.producer()
    first = producer.send("orders", {"n": 1}, key="acme")
    second = producer.send("orders", {"n": 2}, key="acme")
    assert first.topic == "orders"
    assert second.partition == first.partition  # same key -> same partition
    assert second.offset == first.offset + 1


def test_same_key_preserves_order_within_partition(broker):
    producer = broker.producer()
    for n in range(10):
        producer.send("orders", {"n": n}, key="acme")
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    values = [m.value["n"] for m in consumer.poll(max_records=100)]
    assert values == list(range(10))


def test_keyless_records_spread_round_robin(broker):
    producer = broker.producer()
    for n in range(6):
        producer.send("orders", {"n": n})
    partitions = {m.partition for m in broker.log("orders")}
    assert partitions == {0, 1, 2}


def test_partition_for_key_is_stable():
    assert partition_for_key("acme", 3) == partition_for_key("acme", 3)
    assert partition_for_key(None, 3, fallback=4) == 1
    assert partition_for_key("anything", 1) == 0


def test_reading_does_not_remove_records(broker):
    broker.producer().send("orders", {"n": 1})
    c1 = broker.consumer("g1")
    c1.subscribe(["orders"])
    assert len(c1.poll()) == 1
    c2 = broker.consumer("g2")
    c2.subscribe(["orders"])
    assert len(c2.poll()) == 1  # a second group sees the same record


def test_consumer_groups_split_partitions(broker):
    broker.create_topic("orders", 3)
    a = broker.consumer("g1", client_id="a")
    b = broker.consumer("g1", client_id="b")
    a.subscribe(["orders"])
    b.subscribe(["orders"])
    owned_a = set(a.assignment())
    owned_b = set(b.assignment())
    assert owned_a.isdisjoint(owned_b)
    assert len(owned_a) + len(owned_b) == 3


def test_new_member_triggers_rebalance(broker):
    broker.create_topic("orders", 4)
    a = broker.consumer("g1", client_id="a")
    a.subscribe(["orders"])
    assert len(a.assignment()) == 4
    b = broker.consumer("g1", client_id="b")
    b.subscribe(["orders"])
    assert len(a.assignment()) == 2
    assert len(b.assignment()) == 2
    b.close()
    assert len(a.assignment()) == 4  # leaving hands the partitions back


def test_each_group_gets_every_record_once_across_members(broker):
    broker.create_topic("orders", 4)
    producer = broker.producer()
    for n in range(20):
        producer.send("orders", {"n": n}, key=f"k{n}")
    a = broker.consumer("g1", client_id="a")
    b = broker.consumer("g1", client_id="b")
    a.subscribe(["orders"])
    b.subscribe(["orders"])
    seen = [m.value["n"] for m in a.poll(max_records=100)]
    seen += [m.value["n"] for m in b.poll(max_records=100)]
    assert sorted(seen) == list(range(20))


def test_uncommitted_progress_is_lost_on_restart(broker):
    """The at-least-once contract: no commit means redelivery."""
    broker.producer().send("orders", {"n": 1}, key="k")
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    assert len(consumer.poll()) == 1
    consumer.close()  # died without committing

    replacement = broker.consumer("g1")
    replacement.subscribe(["orders"])
    assert len(replacement.poll()) == 1  # redelivered


def test_committed_progress_survives_restart(broker):
    broker.producer().send("orders", {"n": 1}, key="k")
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    consumer.poll()
    consumer.commit()
    consumer.close()

    replacement = broker.consumer("g1")
    replacement.subscribe(["orders"])
    assert replacement.poll() == []  # not redelivered


def test_seek_rewinds_and_replays(broker):
    producer = broker.producer()
    for n in range(3):
        producer.send("orders", {"n": n}, key="k")
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    first = consumer.poll(max_records=3)
    assert [m.value["n"] for m in first] == [0, 1, 2]
    consumer.seek(first[0].topic_partition, 1)
    again = consumer.poll(max_records=3)
    assert [m.value["n"] for m in again] == [1, 2]


def test_lag_counts_unconsumed_records(broker):
    broker.create_topic("orders", 1)
    producer = broker.producer()
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    for n in range(5):
        producer.send("orders", {"n": n}, key="k")
    assert consumer.lag() == {TopicPartition("orders", 0): 5}
    consumer.poll(max_records=2)
    assert consumer.lag() == {TopicPartition("orders", 0): 3}


def test_log_isolation_from_handler_mutation(broker):
    producer = broker.producer()
    payload = {"items": [1, 2]}
    producer.send("orders", payload, key="k")
    payload["items"].append(3)  # caller mutates after send
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    message = consumer.poll()[0]
    message.value["items"].append(4)  # consumer mutates what it was given
    assert broker.log("orders")[0].value == {"items": [1, 2]}


def test_unknown_topic_partition_count_raises(broker):
    with pytest.raises(BrokerError):
        broker.partition_count("nope")


def test_closed_consumer_rejects_poll(broker):
    consumer = broker.consumer("g1")
    consumer.subscribe(["orders"])
    consumer.close()
    with pytest.raises(BrokerError):
        consumer.poll()
