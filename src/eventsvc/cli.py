"""The ``eventsvc`` command: a self-contained demo of the whole topology.

``eventsvc demo`` produces the sample order stream, runs the service to
completion on the in-memory broker, prints what happened to every record, and
finishes with a DLQ summary, a replay, and (optionally) the raw Prometheus
exposition. ``--kafka`` points the identical run at a real broker.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import __version__
from .broker import Broker
from .clock import Clock, ManualClock, SystemClock
from .memory import InMemoryBroker
from .metrics import Metrics
from .retry import RetryPolicy
from .samples import OrderHandler, produce_orders
from .service import EventService
from .worker import WorkerStats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eventsvc",
        description="Event-driven microservice demo: consumer groups, retry tiers, DLQ, metrics.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    demo = sub.add_parser("demo", help="run the sample order stream through the full topology")
    demo.add_argument("--topic", default="orders", help="source topic name (default: orders)")
    demo.add_argument("--partitions", type=int, default=3, help="partitions per topic (default: 3)")
    demo.add_argument(
        "--max-attempts", type=int, default=4, help="handler attempts before the DLQ (default: 4)"
    )
    demo.add_argument(
        "--base-delay", type=float, default=1.0, help="first retry tier delay in seconds"
    )
    demo.add_argument(
        "--real-time",
        action="store_true",
        help="use the system clock (actually wait out retry delays) instead of simulated time",
    )
    demo.add_argument(
        "--kafka",
        metavar="BOOTSTRAP",
        help="run against a real Kafka broker at this bootstrap address instead of in-memory",
    )
    demo.add_argument("--no-replay", action="store_true", help="skip the DLQ replay step")
    demo.add_argument("--metrics", action="store_true", help="print the Prometheus exposition")
    demo.add_argument("--json", action="store_true", help="emit results as JSON")

    topics = sub.add_parser("topics", help="print the topic topology for a retry policy")
    topics.add_argument("--topic", default="orders")
    topics.add_argument("--max-attempts", type=int, default=4)
    topics.add_argument("--base-delay", type=float, default=1.0)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2
    if args.command == "topics":
        return _cmd_topics(args)
    return _cmd_demo(args)


def _cmd_topics(args: argparse.Namespace) -> int:
    policy = RetryPolicy(max_attempts=args.max_attempts, base_delay=args.base_delay)
    print(f"topology for {args.topic!r} (max_attempts={policy.max_attempts}):")
    print(f"  source : {args.topic}")
    for delay, name in zip(policy.delays(), policy.retry_topics(args.topic), strict=False):
        print(f"  retry  : {name}  (+{delay:g}s)")
    print(f"  dlq    : {args.topic}.dlq")
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    policy = RetryPolicy(max_attempts=args.max_attempts, base_delay=args.base_delay)
    clock: Clock = SystemClock() if (args.real_time or args.kafka) else ManualClock()

    broker: Broker
    if args.kafka:
        from .kafka import KafkaBroker  # imported lazily: needs the [kafka] extra

        broker = KafkaBroker(args.kafka, default_partitions=args.partitions)
    else:
        broker = InMemoryBroker(default_partitions=args.partitions, clock=clock)

    handler = OrderHandler()
    metrics = Metrics()
    service = EventService(
        broker=broker,
        topic=args.topic,
        handler=handler,
        policy=policy,
        clock=clock,
        metrics=metrics,
        partitions=args.partitions,
    )

    produced = produce_orders(broker.producer(), args.topic)
    stats = service.run_until_idle()
    dead = service.dead_letters()
    summary = service.dlq.summary()

    replayed = 0
    replay_stats = None
    if not args.no_replay and dead:
        # The replay story: the flaky downstream has been fixed, so retriable
        # failures are worth another pass. The permanently-rejected order is
        # not — replaying it would just make the same round trip again.
        handler.downstream_fixed = True
        before = service.stats()
        replayed = service.dlq.replay(select=lambda e: e.info.error_type != "PermanentError")
        service.run_until_idle()
        after = service.stats()
        replay_stats = WorkerStats(
            topic=args.topic,
            consumed=after.consumed - before.consumed,
            ok=after.ok - before.ok,
            retried=after.retried - before.retried,
            dead_lettered=after.dead_lettered - before.dead_lettered,
            duplicates=after.duplicates - before.duplicates,
        )

    if args.json:
        payload: dict[str, Any] = {
            "produced": produced,
            "stats": stats.as_dict(),
            "dead_letters": [entry.as_dict() for entry in dead],
            "dlq_summary": summary,
            "replayed": replayed,
            "stats_after_replay": replay_stats.as_dict() if replay_stats else None,
            "processed_order_ids": handler.processed_ids(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_report(args, service, handler, produced, replayed, replay_stats)

    if args.metrics:
        print()
        print(metrics.render())

    service.stop()
    broker.close()
    return 0


def _print_report(
    args: argparse.Namespace,
    service: EventService,
    handler: OrderHandler,
    produced: int,
    replayed: int,
    replay_stats: Any,
) -> None:
    stats = service.stats()
    print(f"── demo — {produced} order(s) through {args.topic!r} on ", end="")
    print("real Kafka" if args.kafka else "the in-memory broker")
    print(f"  handled ok:    {stats.ok}")
    print(f"  retried:       {stats.retried}")
    print(f"  dead-lettered: {stats.dead_lettered}")
    print(f"  duplicates:    {stats.duplicates}")

    print("\n── per-topic")
    for topic, topic_stats in sorted(service.stats_by_topic().items()):
        print(
            f"  {topic:<24} consumed={topic_stats.consumed:<3} ok={topic_stats.ok:<3} "
            f"retried={topic_stats.retried:<3} dlq={topic_stats.dead_lettered}"
        )

    summary = service.dlq.summary()
    print(f"\n── dead letters — {summary['total']} on {summary['topic']}")
    for entry in service.dead_letters():
        info = entry.info
        print(
            f"  {info.event_id[:8]}… {entry.message.value.get('order_id', '?'):<9} "
            f"attempts={info.attempts} {info.error_type}: {info.error_message}"
        )

    if replayed:
        print(f"\n── replay — downstream fixed; {replayed} record(s) republished to {args.topic!r}")
        print(f"  handled ok after replay: {replay_stats.ok}")
        remaining = service.dlq.summary()["total"] - replayed
        print(f"  not replayed: {remaining} (permanent failures stay put)")

    print(f"\n── processed orders: {', '.join(handler.processed_ids())}")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
