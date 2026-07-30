"""The ``eventsvc`` command: a demo of the whole topology, and DLQ operations.

``eventsvc demo`` produces the sample order stream, runs the service to
completion on the in-memory broker, prints what happened to every record, and
finishes with a DLQ summary, a replay, and (optionally) the raw Prometheus
exposition. ``--kafka`` points the identical run at a real broker.

``eventsvc dlq`` is the on-call half: point it at a real broker to summarize,
list, and selectively replay a dead-letter topic without writing a script at
3am. Replay writes to production topics, so it is guarded — see
:func:`run_dlq_replay`.

``eventsvc lag`` is the question asked before either of those: how far behind is
the group, right now. It reads the answer out of broker metadata, so it is safe
to run against production repeatedly — it never joins the group it measures.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any

from . import __version__
from .broker import Broker
from .clock import Clock, ManualClock, SystemClock
from .dlq import DeadLetter, DeadLetterQueue
from .lag import LagExporter, format_snapshot
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

    lag = sub.add_parser("lag", help="report a consumer group's lag without joining the group")
    lag.add_argument("--topic", default="orders", help="source topic (default: orders)")
    lag.add_argument(
        "--group",
        metavar="ID",
        help="consumer group to measure (default: <topic>-workers, the EventService default)",
    )
    lag.add_argument(
        "--kafka",
        metavar="BOOTSTRAP",
        help="bootstrap address of the broker holding the topics",
    )
    lag.add_argument("--max-attempts", type=int, default=4, help="tiers to include (default: 4)")
    lag.add_argument("--base-delay", type=float, default=1.0, help="first retry tier delay")
    lag.add_argument(
        "--source-only", action="store_true", help="measure the source topic, not the whole ladder"
    )
    lag.add_argument("--verbose", action="store_true", help="one line per partition")
    lag.add_argument(
        "--watch",
        type=float,
        metavar="SECONDS",
        help="keep sampling every SECONDS instead of printing once",
    )
    lag.add_argument("--samples", type=int, metavar="N", help="with --watch, stop after N samples")
    lag.add_argument("--json", action="store_true", help="emit results as JSON")

    _add_dlq_parser(sub)
    return parser


def _add_dlq_parser(sub: argparse._SubParsersAction) -> None:
    dlq = sub.add_parser("dlq", help="inspect and replay a dead-letter topic on a real broker")
    dlq.set_defaults(dlq_parser=dlq)
    dlq_sub = dlq.add_subparsers(dest="dlq_command")

    def common(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        parser.add_argument("--topic", default="orders", help="source topic (default: orders)")
        parser.add_argument(
            "--kafka",
            metavar="BOOTSTRAP",
            help="bootstrap address of the broker holding the dead-letter topic",
        )
        parser.add_argument("--json", action="store_true", help="emit results as JSON")
        return parser

    def filters(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        parser.add_argument(
            "--error-type",
            action="append",
            metavar="NAME",
            help="only records whose last failure was this exception type (repeatable)",
        )
        parser.add_argument(
            "--key", action="append", metavar="KEY", help="only records with this key (repeatable)"
        )
        parser.add_argument(
            "--max-replays",
            type=int,
            metavar="N",
            help="skip records already replayed more than N times",
        )
        parser.add_argument("--limit", type=int, metavar="N", help="stop after N matching records")
        return parser

    common(filters(dlq_sub.add_parser("list", help="list dead letters with their failure history")))
    common(dlq_sub.add_parser("summary", help="aggregate the dead-letter topic by error type"))

    replay = common(filters(dlq_sub.add_parser("replay", help="republish dead letters")))
    replay.add_argument(
        "--include-permanent",
        action="store_true",
        help="also replay PermanentError records (skipped by default: they fail the same way)",
    )
    replay.add_argument(
        "--dry-run", action="store_true", help="show exactly what would be replayed, write nothing"
    )
    replay.add_argument(
        "--yes", action="store_true", help="skip the confirmation prompt (required when piped)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2
    if args.command == "topics":
        return _cmd_topics(args)
    if args.command == "dlq":
        return _cmd_dlq(args)
    if args.command == "lag":
        return _cmd_lag(args)
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

    # Sampled last, so it describes the topology as the demo leaves it: the
    # source topic drained, and whatever is still sitting in the DLQ.
    lag = service.lag_exporter().export()

    if args.json:
        payload: dict[str, Any] = {
            "produced": produced,
            "stats": stats.as_dict(),
            "lag": lag.as_dict(),
            "dead_letters": [entry.as_dict() for entry in dead],
            "dlq_summary": summary,
            "replayed": replayed,
            "stats_after_replay": replay_stats.as_dict() if replay_stats else None,
            "processed_order_ids": handler.processed_ids(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_report(args, service, handler, produced, replayed, replay_stats)
        print()
        for line in format_snapshot(lag):
            print(line)

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


# -- lag: how far behind, without disturbing anything ----------------------


def _cmd_lag(args: argparse.Namespace) -> int:
    if not args.kafka:
        # Same reason `dlq` insists: the in-memory broker dies with the process
        # that created it, so a separate invocation would measure an empty log
        # belonging to nobody and confidently report a lag of zero.
        print(
            "eventsvc lag needs --kafka BOOTSTRAP: a consumer group to measure has to outlive "
            "the command, and the in-memory broker does not.",
            file=sys.stderr,
        )
        return 2

    from .kafka import KafkaBroker  # imported lazily: needs the [kafka] extra

    policy = RetryPolicy(max_attempts=args.max_attempts, base_delay=args.base_delay)
    topics = (args.topic,) if args.source_only else policy.topics_for(args.topic)
    broker = KafkaBroker(args.kafka)
    try:
        exporter = LagExporter(broker, args.group or f"{args.topic}-workers", topics)
        return run_lag(
            exporter,
            verbose=args.verbose,
            as_json=args.json,
            watch=args.watch,
            samples=args.samples,
        )
    finally:
        broker.close()


def run_lag(
    exporter: LagExporter,
    *,
    verbose: bool = False,
    as_json: bool = False,
    watch: float | None = None,
    samples: int | None = None,
) -> int:
    """Print the group's lag once, or every ``watch`` seconds until interrupted.

    Reading lag is a pure observation — no group is joined and no offset moves —
    so unlike ``dlq replay`` this command needs no confirmation and no dry run.
    """
    printed = 0
    while True:
        snapshot = exporter.export()
        if as_json:
            print(json.dumps(snapshot.as_dict(), indent=2, sort_keys=True))
        else:
            if printed:
                print()
            for line in format_snapshot(snapshot, verbose=verbose):
                print(line)
        printed += 1
        if watch is None or (samples is not None and printed >= samples):
            return 0
        try:
            exporter.clock.sleep(watch)
        except KeyboardInterrupt:
            return 0


# -- dlq: the on-call commands ---------------------------------------------


def _cmd_dlq(args: argparse.Namespace) -> int:
    if args.dlq_command is None:
        args.dlq_parser.print_help()
        return 2
    if not args.kafka:
        # The in-memory broker lives in the process that created it, so there is
        # nothing for a separate `eventsvc dlq` invocation to read. Saying so
        # beats printing a convincing, permanently-empty dead-letter topic.
        print(
            "eventsvc dlq needs --kafka BOOTSTRAP: a dead-letter topic to operate on has to "
            "outlive the command, and the in-memory broker does not.",
            file=sys.stderr,
        )
        return 2

    from .kafka import KafkaBroker  # imported lazily: needs the [kafka] extra

    broker = KafkaBroker(args.kafka)
    try:
        dlq = DeadLetterQueue(broker, args.topic)
        if args.dlq_command == "summary":
            return run_dlq_summary(dlq, as_json=args.json)
        if args.dlq_command == "list":
            return run_dlq_list(dlq, select=_dlq_select(args), limit=args.limit, as_json=args.json)
        return run_dlq_replay(
            dlq,
            select=_dlq_select(args),
            limit=args.limit,
            dry_run=args.dry_run,
            assume_yes=args.yes,
            as_json=args.json,
        )
    finally:
        broker.close()


def _dlq_select(args: argparse.Namespace) -> Callable[[DeadLetter], bool] | None:
    """Build one predicate from the filter flags, or ``None`` for everything.

    ``PermanentError`` records are excluded from a replay by default — a failure
    that is permanent by definition will make the identical round trip — but an
    explicit ``--error-type`` wins, so asking for them by name is not silently
    overruled by the default.
    """
    error_types = set(args.error_type or ())
    keys = set(args.key or ())
    max_replays = args.max_replays
    # Only `replay` carries --include-permanent; `list` shows everything it finds.
    is_replay = hasattr(args, "include_permanent")
    skip_permanent = is_replay and not args.include_permanent and not error_types

    if not (error_types or keys or max_replays is not None or skip_permanent):
        return None

    def select(entry: DeadLetter) -> bool:
        if error_types and entry.info.error_type not in error_types:
            return False
        if keys and entry.message.key not in keys:
            return False
        if max_replays is not None and entry.replay_count > max_replays:
            return False
        return not (skip_permanent and entry.info.error_type == "PermanentError")

    return select


def run_dlq_summary(dlq: DeadLetterQueue, *, as_json: bool = False) -> int:
    """Aggregate the dead-letter topic: how many, failing how, since when."""
    summary = dlq.summary()
    if as_json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    print(f"── {summary['topic']} — {summary['total']} dead letter(s)")
    if not summary["total"]:
        return 0
    width = max(len(name) for name in summary["by_error"])
    digits = max(len(str(count)) for count in summary["by_error"].values())
    for error, count in summary["by_error"].items():
        print(f"  {error:<{width}}  {count:>{digits}}")
    now = dlq.clock.now()
    print(f"\n  oldest failure : {_format_age(now - summary['oldest_failed_at'])} ago")
    print(f"  newest failure : {_format_age(now - summary['newest_failed_at'])} ago")
    if summary["replayed_before"]:
        print(f"  replayed before: {summary['replayed_before']} (the earlier fix did not hold)")
    return 0


def run_dlq_list(
    dlq: DeadLetterQueue,
    *,
    select: Callable[[DeadLetter], bool] | None = None,
    limit: int | None = None,
    as_json: bool = False,
) -> int:
    """List matching dead letters with the failure history that makes them actionable."""
    entries = dlq.matching(select, limit)
    if as_json:
        print(json.dumps([entry.as_dict() for entry in entries], indent=2, sort_keys=True))
        return 0

    print(f"── {dlq.topic} — {len(entries)} matching dead letter(s)")
    now = dlq.clock.now()
    for entry in entries:
        print(f"  {_format_entry(entry, now)}")
    return 0


def run_dlq_replay(
    dlq: DeadLetterQueue,
    *,
    select: Callable[[DeadLetter], bool] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    assume_yes: bool = False,
    as_json: bool = False,
    confirm: Callable[[list[DeadLetter], str], bool] | None = None,
) -> int:
    """Republish matching dead letters, after showing the operator what it will touch.

    Replay writes to a live topic, so nothing moves until the selection has been
    printed and confirmed. Confirmation is interactive by default and refuses
    outright when stdin is not a terminal: a piped or cron-driven replay must say
    ``--yes`` deliberately rather than inherit a prompt nobody sees.
    """
    entries = dlq.matching(select, limit)
    topic = entries[0].info.original_topic if entries else dlq.source

    if not entries or dry_run:
        _emit_replay_result(entries, topic, as_json=as_json, dry_run=dry_run)
        return 0

    if not assume_yes:
        ask = confirm if confirm is not None else _prompt_replay
        if not ask(entries, topic):
            return 2

    # Replay the entries that were confirmed, not a re-read of the topic: a
    # record dead-lettered in the meantime was never on the list anyone saw.
    moved = dlq.replay_all(entries)
    _emit_replay_result(moved, topic, as_json=as_json, dry_run=False)
    return 0


def _prompt_replay(entries: list[DeadLetter], topic: str) -> bool:
    now = SystemClock().now()
    print(f"── about to replay {len(entries)} record(s) to {topic!r}")
    for entry in entries[:10]:
        print(f"  {_format_entry(entry, now)}")
    if len(entries) > 10:
        print(f"  … and {len(entries) - 10} more")
    if not sys.stdin.isatty():
        print("\nrefusing to replay without a terminal to confirm at: pass --yes.", file=sys.stderr)
        return False
    answer = input(f"replay {len(entries)} record(s) to {topic!r}? [y/N] ").strip().lower()
    if answer in {"y", "yes"}:
        return True
    print("aborted; nothing was replayed.")
    return False


def _emit_replay_result(
    entries: list[DeadLetter], topic: str, *, as_json: bool, dry_run: bool
) -> None:
    if as_json:
        print(
            json.dumps(
                {
                    "dry_run": dry_run,
                    "target_topic": topic,
                    "count": len(entries),
                    "records": [entry.as_dict() for entry in entries],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    verb = "would replay" if dry_run else "replayed"
    print(f"── {verb} {len(entries)} record(s) to {topic!r}")
    for entry in entries:
        print(f"  {entry.info.event_id}  {entry.info.error_type}: {entry.info.error_message}")


def _format_entry(entry: DeadLetter, now: float) -> str:
    info = entry.info
    replays = f"  replays={entry.replay_count}" if entry.replay_count else ""
    return (
        f"{info.event_id[:12]:<12} key={entry.message.key or '-':<12} "
        f"attempts={info.attempts}{replays}  {_format_age(now - info.failed_at):>4} ago  "
        f"{info.error_type}: {info.error_message}"
    )


def _format_age(seconds: float) -> str:
    """Render an age the way an operator reads it: coarse, and never negative."""
    seconds = max(0.0, seconds)
    for size, suffix in ((86400.0, "d"), (3600.0, "h"), (60.0, "m")):
        if seconds >= size:
            return f"{seconds / size:.0f}{suffix}"
    return f"{seconds:.0f}s"


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
