"""Prometheus instrumentation.

The metric set is chosen to answer the three questions an on-call engineer
actually asks about a consumer, in order: *am I falling behind* (lag), *is the
work succeeding* (outcomes), and *why is it slow* (handler latency). Retry depth
and dead-letter rate are the two that distinguish a service that is degrading
from one that is merely busy — throughput can look perfectly healthy while every
record is quietly taking four attempts to land.

Everything is registered against an explicit :class:`CollectorRegistry` rather
than the process-global default, so two services in one process (or two tests in
one session) do not collide on metric names.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client import start_http_server as _start_http_server

NAMESPACE = "eventsvc"

LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class Outcome:
    """The terminal states a delivery can reach. Used as a metric label value."""

    OK = "ok"
    RETRIED = "retried"
    DEAD_LETTERED = "dead_lettered"
    DUPLICATE = "duplicate"
    ALL = (OK, RETRIED, DEAD_LETTERED, DUPLICATE)


class Metrics:
    """The service's metric surface, bound to one registry."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()

        self.consumed = Counter(
            f"{NAMESPACE}_messages_consumed",
            "Records returned by poll and handed to the worker.",
            ["topic", "group"],
            registry=self.registry,
        )
        self.processed = Counter(
            f"{NAMESPACE}_messages_processed",
            "Deliveries that reached a terminal outcome.",
            ["topic", "group", "outcome"],
            registry=self.registry,
        )
        self.handler_seconds = Histogram(
            f"{NAMESPACE}_handler_seconds",
            "Wall time spent inside the handler, successes and failures alike.",
            ["topic", "handler"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.retries = Counter(
            f"{NAMESPACE}_retry_attempts",
            "Records republished to a retry tier, labelled by the attempt just failed.",
            ["topic", "attempt"],
            registry=self.registry,
        )
        self.dead_letters = Counter(
            f"{NAMESPACE}_dead_letters",
            "Records sent to a dead-letter topic, labelled by the failing exception.",
            ["topic", "error"],
            registry=self.registry,
        )
        self.commits = Counter(
            f"{NAMESPACE}_offset_commits",
            "Offset commits issued by the worker.",
            ["topic", "group"],
            registry=self.registry,
        )
        self.lag = Gauge(
            f"{NAMESPACE}_consumer_lag",
            "Records produced to a partition but not yet consumed by this group.",
            ["topic", "partition", "group"],
            registry=self.registry,
        )
        self.inflight = Gauge(
            f"{NAMESPACE}_inflight_messages",
            "Records currently inside a handler.",
            ["group"],
            registry=self.registry,
        )
        # The two halves of the lag subtraction, exported separately. Lag alone
        # cannot say which side of it stalled — a backlog that stops growing is
        # either a dead consumer or a dead producer, and those page different
        # people. Their rates, side by side, answer it immediately.
        self.committed_offset = Gauge(
            f"{NAMESPACE}_committed_offset",
            "Last offset committed by the group, per partition.",
            ["topic", "partition", "group"],
            registry=self.registry,
        )
        self.log_end_offset = Gauge(
            f"{NAMESPACE}_log_end_offset",
            "Offset of the next record to be written to a partition.",
            ["topic", "partition"],
            registry=self.registry,
        )

    def render(self) -> str:
        """The registry in Prometheus text exposition format."""
        return generate_latest(self.registry).decode("utf-8")

    def value(self, name: str, **labels: str) -> float:
        """Read one sample back. Primarily for tests and the CLI summary."""
        sample = self.registry.get_sample_value(name, labels or None)
        return 0.0 if sample is None else sample

    def serve(self, port: int = 9000, addr: str = "0.0.0.0") -> None:
        """Expose ``/metrics`` on ``port`` in a background thread."""
        _start_http_server(port, addr=addr, registry=self.registry)
