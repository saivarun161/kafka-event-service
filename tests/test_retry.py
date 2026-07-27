"""Retry policy math and the topic naming derived from it."""

import pytest

from eventsvc import RetryPolicy, dlq_topic, retry_topic
from eventsvc.retry import format_delay


def test_default_ladder_is_bounded_exponential():
    policy = RetryPolicy()  # 4 attempts, base 1s, x3
    assert policy.delays() == (1.0, 3.0, 9.0)


def test_max_delay_caps_the_ladder():
    policy = RetryPolicy(max_attempts=6, base_delay=10.0, multiplier=4.0, max_delay=60.0)
    assert policy.delays() == (10.0, 40.0, 60.0, 60.0, 60.0)


def test_delay_for_attempt_walks_the_ladder():
    policy = RetryPolicy()
    assert policy.delay_for_attempt(1) == 1.0
    assert policy.delay_for_attempt(2) == 3.0
    assert policy.delay_for_attempt(3) == 9.0
    assert policy.delay_for_attempt(4) is None  # exhausted
    assert policy.delay_for_attempt(0) is None  # nonsense input


def test_single_attempt_policy_never_retries():
    policy = RetryPolicy(max_attempts=1)
    assert policy.delays() == ()
    assert policy.delay_for_attempt(1) is None


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(0.2, "200ms"), (1.0, "1s"), (1.5, "1.5s"), (9.0, "9s"), (60.0, "60s")],
)
def test_format_delay(seconds, expected):
    assert format_delay(seconds) == expected


def test_topic_names_embed_the_delay():
    assert retry_topic("orders", 3.0) == "orders.retry.3s"
    assert retry_topic("orders", 0.2) == "orders.retry.200ms"
    assert dlq_topic("orders") == "orders.dlq"


def test_capped_tiers_share_one_topic():
    policy = RetryPolicy(max_attempts=6, base_delay=10.0, multiplier=4.0, max_delay=60.0)
    assert policy.retry_topics("orders") == (
        "orders.retry.10s",
        "orders.retry.40s",
        "orders.retry.60s",
    )


def test_topics_for_lists_the_whole_topology():
    assert RetryPolicy().topics_for("orders") == (
        "orders",
        "orders.retry.1s",
        "orders.retry.3s",
        "orders.retry.9s",
        "orders.dlq",
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": 0},
        {"multiplier": 0.5},
        {"base_delay": 5.0, "max_delay": 1.0},
    ],
)
def test_invalid_policies_are_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)
