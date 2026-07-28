"""The CLI: the demo runs to completion and reports honestly."""

import json

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
from eventsvc.cli import (
    _dlq_select,
    build_parser,
    main,
    run_dlq_list,
    run_dlq_replay,
    run_dlq_summary,
)
from eventsvc.envelope import REPLAY_COUNT


def test_no_command_prints_help(capsys):
    assert main([]) == 2
    assert "demo" in capsys.readouterr().out


def test_version(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(["--version"])
    assert excinfo.value.code == 0
    assert "eventsvc" in capsys.readouterr().out


def test_topics_command_prints_topology(capsys):
    assert main(["topics", "--topic", "payments", "--max-attempts", "3"]) == 0
    out = capsys.readouterr().out
    assert "payments.retry.1s" in out
    assert "payments.retry.3s" in out
    assert "payments.dlq" in out
    assert "payments.retry.9s" not in out  # 3 attempts -> only 2 tiers


def test_demo_runs_the_full_story(capsys):
    assert main(["demo"]) == 0
    out = capsys.readouterr().out
    assert "12 order(s)" in out
    assert "dead-lettered: 2" in out
    assert "ord-1005" in out and "PermanentError" in out
    assert "replay" in out
    assert "ord-1011" in out


def test_demo_json_output_is_machine_readable(capsys):
    assert main(["demo", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["produced"] == 12
    assert payload["stats"]["dead_lettered"] == 2
    assert payload["replayed"] == 1
    assert payload["stats_after_replay"]["ok"] == 1
    dead_ids = {d["value"]["order_id"] for d in payload["dead_letters"]}
    assert dead_ids == {"ord-1005", "ord-1011"}
    assert "ord-1011" in payload["processed_order_ids"]
    assert "ord-1005" not in payload["processed_order_ids"]


def test_demo_no_replay_leaves_the_dlq_alone(capsys):
    assert main(["demo", "--json", "--no-replay"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["replayed"] == 0
    assert payload["stats_after_replay"] is None
    assert len(payload["dead_letters"]) == 2


def test_demo_metrics_flag_prints_exposition(capsys):
    assert main(["demo", "--metrics"]) == 0
    out = capsys.readouterr().out
    assert "eventsvc_messages_processed_total" in out
    assert "eventsvc_consumer_lag" in out


# -- eventsvc dlq ----------------------------------------------------------


@pytest.fixture
def dlq():
    """A populated dead-letter topic: two permanent failures and two retriable."""
    clock = ManualClock()
    broker = InMemoryBroker(default_partitions=2, clock=clock)

    def handler(message):
        if message.value.get("bad_schema"):
            raise PermanentError(f"unknown product in {message.value['order_id']}")
        if message.value.get("down"):
            raise RetriableError("payment gateway timeout")

    service = EventService(
        broker=broker,
        topic="orders",
        handler=handler,
        policy=RetryPolicy(max_attempts=2),
        clock=clock,
        partitions=2,
    )
    producer = broker.producer()
    for order_id, flag in [
        ("ord-1", "bad_schema"),
        ("ord-2", "down"),
        ("ord-3", "down"),
        ("ord-4", "bad_schema"),
    ]:
        producer.send("orders", {"order_id": order_id, flag: True}, key=order_id)
    service.run_until_idle()
    assert service.dlq.summary()["total"] == 4
    return service.dlq


def replayed_keys(dlq):
    return sorted(m.key for m in dlq.broker.log("orders") if m.header_int(REPLAY_COUNT, 0) > 0)


def parse_dlq(argv):
    """Parse a `dlq` command line the way main() would, for filter tests."""
    return build_parser().parse_args(argv)


def test_dlq_without_a_broker_explains_why(capsys):
    assert main(["dlq", "summary"]) == 2
    err = capsys.readouterr().err
    assert "--kafka" in err
    assert "in-memory broker does not" in err


def test_dlq_without_a_subcommand_prints_help(capsys):
    assert main(["dlq"]) == 2
    out = capsys.readouterr().out
    assert "summary" in out and "replay" in out


def test_dlq_replay_refuses_when_there_is_no_terminal_to_confirm_at(dlq, capsys):
    # The real default guard, not an injected fake: pytest's stdin is not a TTY,
    # which is exactly the piped/cron case that must not replay silently.
    assert run_dlq_replay(dlq) == 2
    captured = capsys.readouterr()
    assert "about to replay" in captured.out
    assert "--yes" in captured.err
    assert replayed_keys(dlq) == []


def test_dlq_summary_aggregates_by_error(dlq, capsys):
    assert run_dlq_summary(dlq) == 0
    out = capsys.readouterr().out
    assert "orders.dlq — 4 dead letter(s)" in out
    assert "PermanentError" in out and "RetriableError" in out
    assert "oldest failure" in out


def test_dlq_summary_json(dlq, capsys):
    assert run_dlq_summary(dlq, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total"] == 4
    assert payload["by_error"] == {"PermanentError": 2, "RetriableError": 2}


def test_dlq_summary_of_an_empty_topic_says_so(capsys):
    empty = DeadLetterQueue(InMemoryBroker(), "orders", clock=ManualClock())
    assert run_dlq_summary(empty) == 0
    assert "0 dead letter(s)" in capsys.readouterr().out


def test_dlq_list_shows_failure_history(dlq, capsys):
    assert run_dlq_list(dlq) == 0
    out = capsys.readouterr().out
    assert "4 matching dead letter(s)" in out
    assert "key=ord-1" in out
    assert "payment gateway timeout" in out
    assert "attempts=" in out


def test_dlq_list_filters_by_error_type(dlq, capsys):
    args = parse_dlq(["dlq", "list", "--error-type", "RetriableError"])
    assert run_dlq_list(dlq, select=_dlq_select(args), as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert {r["key"] for r in payload} == {"ord-2", "ord-3"}


def test_dlq_list_limit_caps_the_output(dlq, capsys):
    assert run_dlq_list(dlq, limit=2, as_json=True) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2


def test_dlq_replay_skips_permanent_failures_by_default(dlq, capsys):
    args = parse_dlq(["dlq", "replay"])
    assert run_dlq_replay(dlq, select=_dlq_select(args), assume_yes=True) == 0
    assert replayed_keys(dlq) == ["ord-2", "ord-3"]
    assert "replayed 2 record(s) to 'orders'" in capsys.readouterr().out


def test_dlq_replay_include_permanent_takes_everything(dlq):
    args = parse_dlq(["dlq", "replay", "--include-permanent"])
    assert run_dlq_replay(dlq, select=_dlq_select(args), assume_yes=True) == 0
    assert replayed_keys(dlq) == ["ord-1", "ord-2", "ord-3", "ord-4"]


def test_dlq_replay_named_error_type_beats_the_permanent_default(dlq):
    # Asking for PermanentError by name must not be silently overruled by the
    # default that skips it.
    args = parse_dlq(["dlq", "replay", "--error-type", "PermanentError"])
    assert run_dlq_replay(dlq, select=_dlq_select(args), assume_yes=True) == 0
    assert replayed_keys(dlq) == ["ord-1", "ord-4"]


def test_dlq_replay_dry_run_writes_nothing(dlq, capsys):
    assert run_dlq_replay(dlq, dry_run=True, assume_yes=True) == 0
    out = capsys.readouterr().out
    assert "would replay 4 record(s)" in out
    assert replayed_keys(dlq) == []
    assert dlq.summary()["total"] == 4


def test_dlq_replay_refuses_without_confirmation(dlq, capsys):
    # Nothing confirms, so nothing moves — and the exit code says so.
    assert run_dlq_replay(dlq, confirm=lambda entries, topic: False) == 2
    assert replayed_keys(dlq) == []


def test_dlq_replay_confirmation_sees_what_will_move(dlq):
    seen = {}

    def confirm(entries, topic):
        seen["keys"] = sorted(e.message.key for e in entries)
        seen["topic"] = topic
        return True

    assert run_dlq_replay(dlq, limit=2, confirm=confirm) == 0
    assert seen["topic"] == "orders"
    assert seen["keys"] == replayed_keys(dlq)  # exactly what was approved


def test_dlq_replay_of_nothing_is_not_an_error(capsys):
    empty = DeadLetterQueue(InMemoryBroker(), "orders", clock=ManualClock())
    assert run_dlq_replay(empty, assume_yes=True) == 0
    assert "replayed 0 record(s)" in capsys.readouterr().out


def test_dlq_replay_json_reports_the_moved_event_ids(dlq, capsys):
    assert run_dlq_replay(dlq, limit=1, assume_yes=True, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["dry_run"] is False
    assert payload["target_topic"] == "orders"
    assert payload["records"][0]["event_id"]


def test_demo_respects_policy_flags(capsys):
    # With a single attempt there is no ladder: every flaky order goes straight
    # to the DLQ on its first failure.
    assert main(["demo", "--json", "--no-replay", "--max-attempts", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stats"]["retried"] == 0
    assert payload["stats"]["dead_lettered"] == 4  # 1003, 1005, 1008, 1011
