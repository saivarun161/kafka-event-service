"""Failure provenance headers: written once, preserved across hops."""

from eventsvc.envelope import (
    ATTEMPT,
    ERROR_MESSAGE,
    MAX_ERROR_MESSAGE,
    NOT_BEFORE,
    REPLAY_COUNT,
    REPLAYED_FROM,
    FailureInfo,
    attempt_of,
    event_id_of,
    failure_headers,
    replay_headers,
    source_topic,
)
from eventsvc.types import Message


def make_message(**overrides):
    defaults = {
        "topic": "orders",
        "partition": 2,
        "offset": 41,
        "key": "acme",
        "value": {"order_id": "ord-1"},
        "headers": {},
    }
    defaults.update(overrides)
    return Message(**defaults)


def test_first_failure_stamps_provenance():
    message = make_message()
    headers = failure_headers(message, ValueError("boom"), attempt=1, now=100.0, not_before=101.0)
    assert headers["x-original-topic"] == "orders"
    assert headers["x-original-partition"] == "2"
    assert headers["x-original-offset"] == "41"
    assert headers["x-first-failed-at"] == "100.000000"
    assert headers[ATTEMPT] == "1"
    assert headers["x-error-type"] == "ValueError"
    assert headers[NOT_BEFORE] == "101.000000"


def test_later_hops_preserve_the_original_coordinates():
    first = failure_headers(make_message(), ValueError("a"), attempt=1, now=100.0)
    hop = make_message(topic="orders.retry.1s", partition=0, offset=7, headers=first)
    second = failure_headers(hop, KeyError("b"), attempt=2, now=200.0)
    assert second["x-original-topic"] == "orders"
    assert second["x-original-offset"] == "41"
    assert second["x-first-failed-at"] == "100.000000"
    assert second["x-failed-at"] == "200.000000"
    assert second[ATTEMPT] == "2"
    assert second["x-error-type"] == "KeyError"


def test_dead_letter_headers_drop_not_before():
    first = failure_headers(make_message(), ValueError("a"), attempt=1, now=100.0, not_before=101.0)
    hop = make_message(topic="orders.retry.1s", headers=first)
    final = failure_headers(hop, ValueError("a"), attempt=4, now=300.0, not_before=None)
    assert NOT_BEFORE not in final


def test_error_message_is_truncated():
    headers = failure_headers(make_message(), ValueError("x" * 10_000), attempt=1, now=0.0)
    assert len(headers[ERROR_MESSAGE]) == MAX_ERROR_MESSAGE


def test_event_id_precedence():
    assert event_id_of(make_message(headers={"x-event-id": "explicit"})) == "explicit"
    assert event_id_of(make_message()) == "acme"  # falls back to the key
    assert event_id_of(make_message(key=None)) == "orders:41"  # then to coordinates


def test_attempt_and_source_defaults():
    message = make_message()
    assert attempt_of(message) == 0
    assert source_topic(message) == "orders"


def test_replay_resets_attempts_but_keeps_history():
    first = failure_headers(make_message(), ValueError("a"), attempt=4, now=100.0)
    dead = make_message(topic="orders.dlq", headers=first)
    replay = replay_headers(dead)
    assert replay[ATTEMPT] == "0"
    assert replay[REPLAYED_FROM] == "orders.dlq"
    assert replay[REPLAY_COUNT] == "1"
    assert replay["x-original-topic"] == "orders"
    assert "x-error-type" not in replay

    # A second round trip increments the replay counter.
    dead_again = make_message(topic="orders.dlq", headers=replay)
    assert replay_headers(dead_again)[REPLAY_COUNT] == "2"


def test_failure_info_reconstructs_history():
    headers = failure_headers(make_message(), ValueError("boom"), attempt=3, now=100.0)
    dead = make_message(topic="orders.dlq", partition=0, offset=5, headers=headers)
    info = FailureInfo.from_message(dead)
    assert info.original_topic == "orders"
    assert info.original_partition == 2
    assert info.original_offset == 41
    assert info.attempts == 3
    assert info.error_type == "ValueError"
    assert info.first_failed_at == 100.0
    assert info.as_dict()["error_message"] == "boom"


def test_malformed_numeric_headers_fall_back():
    message = make_message(headers={"x-attempt": "not-a-number"})
    assert attempt_of(message) == 0
    assert message.header_float("x-attempt", 1.5) == 1.5
