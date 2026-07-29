"""Graceful shutdown: the signal watch, the report, and the drain itself.

The signal tests send *real* signals to this process rather than calling the
handler directly — a handler that is never actually installed on SIGTERM would
pass every mock-based test and still let the container die mid-batch.
"""

import os
import signal
import threading
import time

import pytest

from eventsvc import (
    EventService,
    InMemoryBroker,
    RetryPolicy,
    ShutdownReport,
    SystemClock,
    shutdown_on_signals,
)
from eventsvc.lifecycle import DEFAULT_SIGNALS, SignalWatch


def build_service(handler, **kwargs):
    clock = SystemClock()
    kwargs.setdefault(
        "policy", RetryPolicy(max_attempts=3, base_delay=0.02, multiplier=2.0, max_delay=0.1)
    )
    return EventService(
        broker=InMemoryBroker(default_partitions=2, clock=clock),
        topic="orders",
        handler=handler,
        clock=clock,
        partitions=2,
        **kwargs,
    )


# -- the signal watch ------------------------------------------------------


def test_sigterm_sets_the_watch():
    with shutdown_on_signals(signal.SIGTERM) as watch:
        assert not watch.event.is_set()
        os.kill(os.getpid(), signal.SIGTERM)
        assert watch.wait(2.0)
        assert watch.received is signal.SIGTERM
        assert watch.signal_name == "SIGTERM"


def test_sigint_drains_rather_than_raising_keyboardinterrupt():
    """Ctrl-C during a drain must not become an exception out of the wait."""
    with shutdown_on_signals(signal.SIGINT) as watch:
        os.kill(os.getpid(), signal.SIGINT)
        assert watch.wait(2.0)
        assert watch.received is signal.SIGINT


def test_previous_handler_is_restored_on_exit():
    sentinel = signal.getsignal(signal.SIGTERM)
    with shutdown_on_signals(signal.SIGTERM):
        assert signal.getsignal(signal.SIGTERM) is not sentinel
    assert signal.getsignal(signal.SIGTERM) is sentinel


def test_on_signal_callback_sees_which_signal_arrived():
    seen = []
    with shutdown_on_signals(signal.SIGTERM, on_signal=seen.append) as watch:
        os.kill(os.getpid(), signal.SIGTERM)
        watch.wait(2.0)
    assert seen == [signal.SIGTERM]


def test_default_signals_are_installed_when_none_are_named():
    with shutdown_on_signals() as watch:
        assert set(watch.installed) == set(DEFAULT_SIGNALS)


def test_watch_can_be_requested_without_a_signal():
    watch = SignalWatch()
    assert not watch.wait(0)
    watch.request()
    assert watch.wait(0)
    assert watch.signal_name is None


def test_handler_installation_is_skipped_off_the_main_thread():
    """A worker thread cannot install handlers; that is reported, not raised."""
    result = {}

    def run():
        with shutdown_on_signals(signal.SIGTERM) as watch:
            result["installed"] = watch.installed
            watch.request()
            result["usable"] = watch.wait(0)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(5.0)
    assert result["installed"] == ()
    assert result["usable"] is True  # still shuts down when asked directly


# -- the report ------------------------------------------------------------


def test_report_is_clean_only_without_stragglers():
    assert ShutdownReport(workers=3, drained=3).clean
    assert not ShutdownReport(workers=3, drained=2, stragglers=("orders-1",)).clean


def test_report_serializes_for_a_log_line():
    report = ShutdownReport(
        workers=2, drained=1, stragglers=("w-1",), duration=1.5, signal_name="SIGTERM"
    )
    assert report.as_dict() == {
        "workers": 2,
        "drained": 1,
        "stragglers": ["w-1"],
        "duration": 1.5,
        "clean": False,
        "signal": "SIGTERM",
    }
    assert "1/2" in str(report) and "straggler" in str(report)


# -- the drain -------------------------------------------------------------


def test_stop_drains_every_worker_and_reports_it():
    service = build_service(lambda m: None)
    service.start()
    report = service.stop(timeout=5.0)
    assert report.clean
    assert report.workers == len(service.workers)
    assert report.drained == report.workers
    assert report.stragglers == ()
    assert all(not w.draining for w in service.workers)


def test_in_flight_record_is_finished_and_committed_before_the_group_is_left():
    """The whole point: a handler running when SIGTERM lands still completes."""
    entered = threading.Event()
    release = threading.Event()
    completed = []

    def slow_handler(message):
        entered.set()
        release.wait(5.0)
        completed.append(message.value["order_id"])

    service = build_service(slow_handler)
    service.broker.producer().send(
        "orders", {"order_id": "ord-1"}, key="acme", headers={"x-event-id": "e1"}
    )
    service.start()
    assert entered.wait(5.0), "handler never started"

    # SIGTERM lands mid-handler; the drain must wait for it rather than closing
    # the consumer underneath it.
    stopped = {}
    stopper = threading.Thread(target=lambda: stopped.update(report=service.stop(timeout=5.0)))
    stopper.start()
    time.sleep(0.05)
    assert completed == []  # still in the handler, shutdown is waiting
    release.set()
    stopper.join(10.0)

    assert completed == ["ord-1"]
    assert stopped["report"].clean
    assert service.stats().ok == 1


def test_a_straggler_is_named_rather_than_closed_out_from_under():
    """When the grace period runs out, say so — and leave the stuck one alone."""
    release = threading.Event()
    entered = threading.Event()

    def stuck_handler(message):
        entered.set()
        release.wait(30.0)

    service = build_service(stuck_handler)
    service.broker.producer().send(
        "orders", {"order_id": "ord-1"}, key="acme", headers={"x-event-id": "e1"}
    )
    service.start()
    assert entered.wait(5.0)
    try:
        report = service.stop(timeout=0.2)
        assert not report.clean
        assert report.drained == report.workers - 1
        assert len(report.stragglers) == 1
        # The stuck worker keeps its consumer: closing it here is the mid-batch
        # rebalance the drain exists to avoid.
        stuck = next(w for w in service.workers if w.name in report.stragglers)
        assert not stuck._consumer._closed
    finally:
        release.set()


def test_the_deadline_covers_the_whole_fleet_not_each_worker():
    """A five-tier topology must not take five grace periods to die."""
    service = build_service(
        lambda m: None,
        policy=RetryPolicy(max_attempts=6, base_delay=0.02, multiplier=2.0, max_delay=0.5),
    )
    assert len(service.workers) == 6  # source + five retry tiers
    service.start()
    started = time.monotonic()
    report = service.stop(timeout=0.5)
    elapsed = time.monotonic() - started
    assert elapsed < 0.5 + 0.4, f"shutdown took {elapsed:.2f}s against a 0.5s deadline"
    assert report.duration == pytest.approx(elapsed, abs=0.2)


def test_stop_without_start_is_a_no_op_shutdown():
    service = build_service(lambda m: None)
    report = service.stop()
    assert report.clean
    assert report.drained == report.workers


def test_run_forever_drains_on_a_real_sigterm():
    handled = threading.Event()
    service = build_service(lambda m: handled.set())
    service.broker.producer().send(
        "orders", {"order_id": "ord-1"}, key="acme", headers={"x-event-id": "e1"}
    )

    ready = threading.Event()
    backstop = threading.Event()
    used_backstop = threading.Event()

    def signaller():
        # The handlers are installed before `ready` is set, so the signal cannot
        # arrive too early to be caught.
        ready.wait(5.0)
        os.kill(os.getpid(), signal.SIGTERM)
        # Never let a lost signal hang the suite; the assertion below still
        # fails if this is what actually stopped the service.
        if not backstop.wait(10.0):
            used_backstop.set()
            backstop.set()

    thread = threading.Thread(target=signaller)
    thread.start()
    report = service.run_forever(timeout=5.0, stop_when=backstop, ready=ready)
    backstop.set()
    thread.join(15.0)

    assert not used_backstop.is_set(), "SIGTERM did not stop the service"
    assert report.signal_name == "SIGTERM"
    assert report.clean
    assert handled.is_set()


def test_run_forever_also_stops_when_asked_directly():
    service = build_service(lambda m: None)
    ready = threading.Event()
    stop_when = threading.Event()

    def ask_to_stop():
        ready.wait(5.0)
        stop_when.set()

    thread = threading.Thread(target=ask_to_stop)
    thread.start()
    report = service.run_forever(timeout=5.0, stop_when=stop_when, ready=ready)
    thread.join(5.0)
    assert report.clean
    assert report.signal_name is None  # not a signal — an explicit request


def test_stop_immediately_after_start_is_not_raced_away():
    """A shutdown that lands microseconds after start() must still be seen."""
    service = build_service(lambda m: None)
    service.start()
    report = service.stop(timeout=2.0)
    assert report.clean, f"workers ignored the stop: {report.stragglers}"


def test_a_drained_service_refuses_to_restart():
    """Draining leaves the group; restarting would hand workers a dead consumer."""
    service = build_service(lambda m: None)
    service.start()
    assert service.stop(timeout=5.0).clean
    with pytest.raises(RuntimeError, match="already been shut down"):
        service.start()


def test_stopping_twice_is_harmless():
    service = build_service(lambda m: None)
    service.start()
    assert service.stop(timeout=5.0).clean
    second = service.stop(timeout=5.0)
    assert second.clean
    assert second.drained == second.workers


def test_duplicate_worker_labels_do_not_close_the_wrong_consumer():
    """Stragglers are tracked by identity; a shared label must not leak a close."""
    release = threading.Event()
    entered = threading.Event()

    def stuck_handler(message):
        entered.set()
        release.wait(30.0)

    service = build_service(stuck_handler)
    for worker in service.workers:
        worker.client_id = "same-label-everywhere"

    service.broker.producer().send(
        "orders", {"order_id": "ord-1"}, key="acme", headers={"x-event-id": "e1"}
    )
    service.start()
    assert entered.wait(5.0)
    try:
        report = service.stop(timeout=0.2)
        assert report.stragglers == ("same-label-everywhere",)
        stuck = [w for w in service.workers if w.topic == "orders"]
        assert not any(w._consumer._closed for w in stuck)
        # Every other worker drained and did leave the group.
        assert all(w._consumer._closed for w in service.workers if w.topic != "orders")
    finally:
        release.set()
