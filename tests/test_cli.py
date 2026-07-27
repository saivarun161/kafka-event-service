"""The CLI: the demo runs to completion and reports honestly."""

import json

import pytest

from eventsvc.cli import main


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


def test_demo_respects_policy_flags(capsys):
    # With a single attempt there is no ladder: every flaky order goes straight
    # to the DLQ on its first failure.
    assert main(["demo", "--json", "--no-replay", "--max-attempts", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stats"]["retried"] == 0
    assert payload["stats"]["dead_lettered"] == 4  # 1003, 1005, 1008, 1011
