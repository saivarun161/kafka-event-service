"""Dead-letter tooling: inspection is non-destructive, replay is selective."""

import pytest

from eventsvc import (
    DeadLetterQueue,
    EventService,
    InMemoryBroker,
    ManualClock,
    PermanentError,
    RetriableError,
    RetryPolicy,
)
from eventsvc.envelope import REPLAY_COUNT, new_event_id


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def broker(clock):
    return InMemoryBroker(default_partitions=2, clock=clock)


def failing_handler(message):
    order = message.value
    if order.get("bad_schema"):
        raise PermanentError(f"bad schema in {order['order_id']}")
    if order.get("down"):
        raise RetriableError(f"downstream 503 for {order['order_id']}")


def run_service(broker, clock, handler):
    service = EventService(
        broker=broker,
        topic="orders",
        handler=handler,
        policy=RetryPolicy(max_attempts=2),
        clock=clock,
        partitions=2,
    )
    return service


def send(broker, order):
    broker.producer().send(
        "orders", order, key=order["order_id"], headers={"x-event-id": new_event_id()}
    )


def test_entries_carry_failure_history(broker, clock):
    service = run_service(broker, clock, failing_handler)
    send(broker, {"order_id": "ord-1", "bad_schema": True})
    send(broker, {"order_id": "ord-2", "down": True})
    service.run_until_idle()

    entries = service.dlq.entries()
    assert len(entries) == 2
    by_id = {e.message.value["order_id"]: e for e in entries}
    assert by_id["ord-1"].info.error_type == "PermanentError"
    assert by_id["ord-1"].info.attempts == 1
    assert by_id["ord-2"].info.error_type == "RetriableError"
    assert by_id["ord-2"].info.attempts == 2  # source attempt + one retry tier
    assert by_id["ord-2"].info.original_topic == "orders"


def test_inspection_is_repeatable_and_non_destructive(broker, clock):
    service = run_service(broker, clock, failing_handler)
    send(broker, {"order_id": "ord-1", "bad_schema": True})
    service.run_until_idle()

    dlq = DeadLetterQueue(broker, "orders", clock=clock)
    assert len(dlq.entries()) == 1
    assert len(dlq.entries()) == 1  # reading twice sees the same records


def test_summary_aggregates_by_error(broker, clock):
    service = run_service(broker, clock, failing_handler)
    send(broker, {"order_id": "ord-1", "bad_schema": True})
    send(broker, {"order_id": "ord-2", "bad_schema": True})
    send(broker, {"order_id": "ord-3", "down": True})
    service.run_until_idle()

    summary = service.dlq.summary()
    assert summary["topic"] == "orders.dlq"
    assert summary["total"] == 3
    assert summary["by_error"] == {"PermanentError": 2, "RetriableError": 1}
    assert summary["replayed_before"] == 0
    assert summary["oldest_failed_at"] <= summary["newest_failed_at"]


def test_empty_dlq_summary(broker, clock):
    service = run_service(broker, clock, failing_handler)
    summary = service.dlq.summary()
    assert summary["total"] == 0
    assert summary["oldest_failed_at"] is None


def test_replay_returns_records_to_the_source_topic(broker, clock):
    handler_state = {"fixed": False}

    def handler(message):
        if message.value.get("down") and not handler_state["fixed"]:
            raise RetriableError("still down")

    service = run_service(broker, clock, handler)
    send(broker, {"order_id": "ord-1", "down": True})
    service.run_until_idle()
    assert service.dlq.summary()["total"] == 1

    handler_state["fixed"] = True
    assert service.dlq.replay() == 1
    stats = service.run_until_idle()
    assert stats.ok >= 1

    # The replayed record went back with its history attached.
    replays = [m for m in broker.log("orders") if m.header_int(REPLAY_COUNT) > 0]
    assert len(replays) == 1
    assert replays[0].value == {"order_id": "ord-1", "down": True}
    assert replays[0].header("x-attempt") == "0"


def test_replay_select_filters(broker, clock):
    service = run_service(broker, clock, failing_handler)
    send(broker, {"order_id": "ord-1", "bad_schema": True})
    send(broker, {"order_id": "ord-2", "down": True})
    service.run_until_idle()

    replayed = service.dlq.replay(select=lambda e: e.info.error_type == "RetriableError")
    assert replayed == 1


def test_replay_limit_is_a_canary(broker, clock):
    service = run_service(broker, clock, failing_handler)
    for n in range(5):
        send(broker, {"order_id": f"ord-{n}", "down": True})
    service.run_until_idle()
    assert service.dlq.replay(limit=2) == 2


def test_replayed_and_refailed_records_show_their_round_trips(broker, clock):
    service = run_service(broker, clock, failing_handler)
    send(broker, {"order_id": "ord-1", "down": True})
    service.run_until_idle()

    service.dlq.replay()  # nothing was fixed, so it will fail again
    service.run_until_idle()

    entries = service.dlq.entries()
    round_tripped = [e for e in entries if e.replay_count > 0]
    assert len(round_tripped) == 1  # the second landing carries replay_count=1
    assert service.dlq.summary()["replayed_before"] == 1
