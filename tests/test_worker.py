"""The worker's routing rules: success, retry tiers, DLQ, deferral, commits."""

import pytest

from eventsvc import (
    InMemoryBroker,
    InMemoryIdempotencyStore,
    ManualClock,
    Metrics,
    PermanentError,
    RetriableError,
    RetryPolicy,
    Worker,
)
from eventsvc.envelope import NOT_BEFORE, attempt_of


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def broker(clock):
    broker = InMemoryBroker(default_partitions=1, clock=clock)
    broker.create_topic("orders", 1)
    return broker


def make_worker(broker, clock, handler, topic="orders", **kwargs):
    kwargs.setdefault("metrics", Metrics())
    return Worker(
        broker=broker,
        topic=topic,
        group_id="workers",
        handler=handler,
        policy=RetryPolicy(),
        clock=clock,
        **kwargs,
    )


def test_success_commits_and_counts(broker, clock):
    seen = []
    worker = make_worker(broker, clock, seen.append)
    broker.producer().send("orders", {"n": 1}, key="k")
    assert worker.poll_once() == 1
    assert worker.stats.ok == 1
    assert seen[0].value == {"n": 1}
    # Committed: a restarted worker in the same group sees nothing.
    worker.close()
    replacement = make_worker(broker, clock, seen.append)
    assert replacement.poll_once() == 0


def test_retriable_failure_republishes_to_first_tier(broker, clock):
    def handler(message):
        raise RetriableError("downstream flaked")

    worker = make_worker(broker, clock, handler)
    broker.producer().send("orders", {"n": 1}, key="k")
    worker.poll_once()

    assert worker.stats.retried == 1
    retried = broker.log("orders.retry.1s")
    assert len(retried) == 1
    record = retried[0]
    assert record.value == {"n": 1}
    assert attempt_of(record) == 1
    assert record.header_float(NOT_BEFORE) == pytest.approx(clock.now() + 1.0)


def test_any_unexpected_exception_is_treated_as_retriable(broker, clock):
    def handler(message):
        raise KeyError("surprise")

    worker = make_worker(broker, clock, handler)
    broker.producer().send("orders", {"n": 1}, key="k")
    worker.poll_once()
    assert worker.stats.retried == 1
    assert broker.log("orders.retry.1s")[0].header("x-error-type") == "KeyError"


def test_permanent_failure_skips_the_ladder(broker, clock):
    def handler(message):
        raise PermanentError("bad schema")

    worker = make_worker(broker, clock, handler)
    broker.producer().send("orders", {"n": 1}, key="k")
    worker.poll_once()

    assert worker.stats.dead_lettered == 1
    assert broker.log("orders.retry.1s") == []
    dead = broker.log("orders.dlq")
    assert len(dead) == 1
    assert dead[0].header("x-error-type") == "PermanentError"


def test_exhausted_ladder_dead_letters_with_original_coordinates(broker, clock):
    def handler(message):
        raise RetriableError("always down")

    source = make_worker(broker, clock, handler)
    broker.producer().send("orders", {"n": 1}, key="k")
    source.poll_once()

    # Walk the record through every tier by hand.
    for tier in ("orders.retry.1s", "orders.retry.3s", "orders.retry.9s"):
        tier_worker = make_worker(broker, clock, handler, topic=tier)
        clock.advance(10)
        assert tier_worker.poll_once() == 1

    dead = broker.log("orders.dlq")
    assert len(dead) == 1
    assert attempt_of(dead[0]) == 4
    assert dead[0].header("x-original-topic") == "orders"
    assert dead[0].header("x-original-offset") == "0"
    assert dead[0].value == {"n": 1}  # payload untouched by four failures


def test_not_due_record_is_deferred_and_partition_rewound(broker, clock):
    handled = []
    worker = make_worker(broker, clock, handled.append, topic="orders.retry.1s", blocking=False)
    broker.create_topic("orders.retry.1s", 1)
    broker.producer().send(
        "orders.retry.1s",
        {"n": 1},
        key="k",
        headers={NOT_BEFORE: f"{clock.now() + 5:.6f}"},
    )
    assert worker.poll_once() == 0
    assert handled == []
    assert worker.pending_due_at == pytest.approx(clock.now() + 5)

    clock.advance(5.1)
    assert worker.poll_once() == 1
    assert handled[0].value == {"n": 1}


def test_blocking_worker_waits_out_the_delay(broker, clock):
    handled = []
    worker = make_worker(broker, clock, handled.append, topic="orders.retry.1s", blocking=True)
    broker.create_topic("orders.retry.1s", 1)
    broker.producer().send(
        "orders.retry.1s",
        {"n": 1},
        key="k",
        headers={NOT_BEFORE: f"{clock.now() + 2:.6f}"},
    )
    # Under ManualClock, sleep advances time, so the blocking wait completes.
    assert worker.poll_once() == 1
    assert handled[0].value == {"n": 1}


def test_deferral_does_not_skip_records_behind_the_head(broker, clock):
    handled = []
    worker = make_worker(broker, clock, handled.append, topic="orders.retry.1s", blocking=False)
    broker.create_topic("orders.retry.1s", 1)
    producer = broker.producer()
    producer.send(
        "orders.retry.1s", {"n": 1}, key="k", headers={NOT_BEFORE: f"{clock.now() + 5:.6f}"}
    )
    producer.send(
        "orders.retry.1s", {"n": 2}, key="k", headers={NOT_BEFORE: f"{clock.now() + 6:.6f}"}
    )
    worker.poll_once()
    assert handled == []
    clock.advance(7)
    worker.poll_once()
    assert [m.value["n"] for m in handled] == [1, 2]  # both delivered, in order


def test_duplicate_event_ids_are_skipped(broker, clock):
    handled = []
    worker = make_worker(
        broker, clock, handled.append, idempotency=InMemoryIdempotencyStore(clock=clock)
    )
    producer = broker.producer()
    producer.send("orders", {"n": 1}, key="k", headers={"x-event-id": "evt-1"})
    producer.send("orders", {"n": 1}, key="k", headers={"x-event-id": "evt-1"})
    worker.poll_once()
    assert len(handled) == 1
    assert worker.stats.duplicates == 1


def test_failed_records_are_not_marked_processed(broker, clock):
    """A failure must stay retryable: only success marks the event id."""
    attempts = []

    def handler(message):
        attempts.append(attempt_of(message))
        if len(attempts) == 1:
            raise RetriableError("first delivery fails")

    store = InMemoryIdempotencyStore(clock=clock)
    worker = make_worker(broker, clock, handler, idempotency=store)
    broker.producer().send("orders", {"n": 1}, key="k", headers={"x-event-id": "evt-1"})
    worker.poll_once()

    tier = make_worker(broker, clock, handler, topic="orders.retry.1s", idempotency=store)
    clock.advance(2)
    tier.poll_once()
    assert attempts == [0, 1]
    assert tier.stats.ok == 1


def test_crash_before_commit_redelivers(broker, clock):
    """Kill the worker mid-batch: the whole batch comes back."""

    class Boom(RuntimeError):
        pass

    def exploding(message):
        raise Boom()

    class CrashingWorker(Worker):
        def _handle(self, message):
            raise Boom()  # simulates the process dying, not a handler error

    worker = CrashingWorker(
        broker=broker,
        topic="orders",
        group_id="workers",
        handler=exploding,
        policy=RetryPolicy(),
        clock=clock,
        metrics=Metrics(),
    )
    broker.producer().send("orders", {"n": 1}, key="k")
    with pytest.raises(Boom):
        worker.poll_once()
    worker.close()  # the dead process leaves the group without committing

    handled = []
    replacement = make_worker(broker, clock, handled.append)
    assert replacement.poll_once() == 1  # nothing was committed, so it is redelivered
    assert handled[0].value == {"n": 1}


def test_metrics_reflect_outcomes(broker, clock):
    metrics = Metrics()

    def handler(message):
        if message.value["n"] == 2:
            raise RetriableError("flake")

    worker = make_worker(broker, clock, handler, metrics=metrics)
    producer = broker.producer()
    producer.send("orders", {"n": 1}, key="a")
    producer.send("orders", {"n": 2}, key="b")
    worker.poll_once()

    assert (
        metrics.value(
            "eventsvc_messages_processed_total",
            topic="orders",
            group="workers",
            outcome="ok",
        )
        == 1
    )
    assert (
        metrics.value(
            "eventsvc_messages_processed_total",
            topic="orders",
            group="workers",
            outcome="retried",
        )
        == 1
    )
    assert metrics.value("eventsvc_retry_attempts_total", topic="orders", attempt="1") == 1
    assert metrics.value("eventsvc_offset_commits_total", topic="orders", group="workers") == 1
