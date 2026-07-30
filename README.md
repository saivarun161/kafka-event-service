# kafka-event-service

[![CI](https://github.com/saivarun161/kafka-event-service/actions/workflows/ci.yml/badge.svg)](https://github.com/saivarun161/kafka-event-service/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An **event-driven microservice skeleton** with the failure-handling machinery that real deployments need and tutorials skip: **consumer groups**, **tiered retry topics** with exponential backoff, a **dead-letter queue you can actually operate** (inspect, aggregate, selectively replay), **at-least-once delivery with idempotent handling**, a **consumer-lag exporter** that keeps reporting after the consumers themselves are gone, and **Prometheus metrics** chosen for on-call use.

The whole service is written against a small broker protocol. Locally and in the test suite it runs on an in-memory broker that faithfully implements log semantics — partitions, consumer groups, offsets, seek — so there is nothing to install. In CI the *same service code* runs against a real Kafka broker in a service container, and the same story passes.

```text
                                    ┌──────────────────────┐
 producer ──► orders ──► consumer ──┤ handler              │
              (3 partitions)        └──┬────────┬──────────┘
                                   ok  │        │ fail
                                       ▼        ▼
                                    commit   ┌─ retriable? ──────────┐
                                             │ yes: republish to     │  no (PermanentError):
                                             │ the next retry tier   │  straight to the DLQ
                                             ▼                       ▼
              orders.retry.1s ──► consumer ──► handler ──fail──►┐
              orders.retry.3s ──► consumer ──► handler ──fail──►┤
              orders.retry.9s ──► consumer ──► handler ──fail──►┤
                                                                ▼
              orders.dlq  ◄─────────────────────── (attempts exhausted)
                  │
                  └──► inspect / summarize / replay ──► back onto orders
```

## Why retry *topics* instead of retrying in place

The obvious way to retry is to `sleep()` inside the consumer and call the handler again. It is also the fastest way to take down a partition: a consumer owns its partitions exclusively, so a 30-second backoff on one bad record stalls every healthy record queued behind it. That is **head-of-line blocking**, and at any real volume it turns one flaky downstream into a full outage.

So failure is handled by *republishing*, never by waiting on the hot path:

- A failed record is sent to a **retry topic named for its delay** (`orders.retry.3s`), stamped with a `x-not-before` header, and the source consumer commits and moves on immediately.
- Each retry tier gets its own consumer. Because every record on `orders.retry.3s` carries the same delay, the topic is due **in storage order** — so that consumer *can* safely block on its head record. Waiting is fine once the only thing behind you is other things that are also waiting.
- The tiers form a bounded exponential ladder (default 1s → 3s → 9s, capped). A record that exhausts them is dead-lettered with its full failure history in headers.

Errors are routed by type: any exception is treated as transient and earns a retry, but a handler can raise `PermanentError` ("unknown product id" will never succeed) to skip the ladder entirely — retrying a failure that cannot succeed only delays the dead-letter signal by minutes.

## Quickstart — 60 seconds, no broker, no API keys

```bash
git clone https://github.com/saivarun161/kafka-event-service.git
cd kafka-event-service
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

eventsvc demo          # the full topology on the in-memory broker
```

The demo pushes 12 synthetic orders through the service. Nine are clean; two hit a flaky payment gateway (they recover through the retry ladder); one references a product that does not exist (permanent); one fails every attempt (exhausts the ladder). Then the DLQ is inspected and replayed:

```text
── demo — 12 order(s) through 'orders' on the in-memory broker
  handled ok:    10
  retried:       6
  dead-lettered: 2

── per-topic
  orders                   consumed=12  ok=8   retried=3   dlq=1
  orders.retry.1s          consumed=3   ok=1   retried=2   dlq=0
  orders.retry.3s          consumed=2   ok=1   retried=1   dlq=0
  orders.retry.9s          consumed=1   ok=0   retried=0   dlq=1

── dead letters — 2 on orders.dlq
  e6597c24… ord-1005  attempts=1 PermanentError: unknown product: 'hoverboard'
  787dfeff… ord-1011  attempts=4 RetriableError: payment gateway timeout (attempt 4)

── replay — downstream fixed; 1 record(s) republished to 'orders'
  handled ok after replay: 1
  not replayed: 1 (permanent failures stay put)

── lag — group 'orders-workers', 2 record(s) behind
  orders           0
  orders.retry.1s  0
  orders.retry.3s  0
  orders.retry.9s  0
  orders.dlq       2
```

Every tier is drained and committed; the two records still counted as backlog are the dead letters, on a topic nothing consumes. That last block is read from the broker's metadata rather than from the workers — see [lag, measured from outside the group](#lag-measured-from-outside-the-group).

The demo completes instantly even though the ladder spans 13 seconds of delay, because time is injected: under the simulated clock the scheduler jumps straight to the next due record. Run `eventsvc demo --real-time` to wait the delays out for real, `--json` for machine-readable output, `--metrics` to dump the Prometheus exposition, and `eventsvc topics` to print the topic topology for any policy.

Same demo, real broker: `eventsvc demo --kafka localhost:9092` (needs `pip install -e ".[dev,kafka]"`). Against a real broker there are also the two on-call commands: `eventsvc dlq` — [operating the dead-letter queue](#operating-the-dlq-from-the-command-line) without writing a script — and `eventsvc lag` — [how far behind the group is](#lag-measured-from-outside-the-group), without joining it.

## Using it as a library

```python
from eventsvc import EventService, InMemoryBroker, PermanentError, RetryPolicy

def handle(message):
    order = message.value                      # already-decoded JSON dict
    if order["product"] not in CATALOG:
        raise PermanentError(f"unknown product {order['product']!r}")
    charge(order)                              # any exception here -> retry ladder

service = EventService(
    broker=InMemoryBroker(default_partitions=3),   # or KafkaBroker("localhost:9092")
    topic="orders",
    handler=handle,
    policy=RetryPolicy(max_attempts=4, base_delay=1.0, multiplier=3.0),
)
service.start()                                # one consumer thread per topic tier
...
service.stop()
```

Operating the dead-letter queue:

```python
service.dlq.summary()
# {'topic': 'orders.dlq', 'total': 42, 'by_error': {'KeyError': 40, 'Timeout': 2}, ...}

service.dlq.replay(select=lambda e: e.info.error_type == "KeyError", limit=5)
# canary: replay 5, watch them land, then replay the rest
```

## Operating the DLQ from the command line

A dead-letter queue you can only reach from a Python REPL is one you will not reach at 3am. `eventsvc dlq` is the same tooling as a command, pointed at a real broker:

```bash
eventsvc dlq summary --topic orders --kafka broker:9092
```

```text
── orders.dlq — 47 dead letter(s)
  RetriableError  45
  PermanentError   2

  oldest failure : 3h ago
  newest failure : 2m ago
```

```bash
eventsvc dlq list --topic orders --kafka broker:9092 --error-type RetriableError --limit 3
```

```text
── orders.dlq — 3 matching dead letter(s)
  054992fb7e63 key=ord-1001     attempts=4   12m ago  RetriableError: payment gateway timeout
  7666cea01eb6 key=ord-1042     attempts=4    9m ago  RetriableError: payment gateway timeout
  b1880b6b0b12 key=ord-1043     attempts=4  replays=1   2m ago  RetriableError: payment gateway timeout
```

Then replay, once the downstream is actually fixed:

```bash
# see what it would touch — writes nothing
eventsvc dlq replay --kafka broker:9092 --error-type RetriableError --dry-run

# canary five, watch them land, then take the rest
eventsvc dlq replay --kafka broker:9092 --error-type RetriableError --limit 5
```

Replay republishes to a live topic, so it is deliberately not the convenient path:

- **The selection is shown before anything moves**, and `--dry-run` shows it without moving anything at all.
- **Confirmation is required.** With no terminal to prompt at — a pipe, a cron job, a CI step — the command *refuses* rather than falling through a prompt nobody would see. Automation has to pass `--yes` on purpose.
- **What was confirmed is what moves.** The approved records are replayed by identity, so a record that lands in the DLQ between the preview and the write does not ride along on an approval it was never part of.
- **`PermanentError` records are skipped by default** — a failure that is permanent by definition will make the identical round trip. Naming the type explicitly (`--error-type PermanentError`) overrides that; the default never silently discards a filter you asked for.

Filters (`--error-type`, `--key`, `--max-replays`, `--limit`) are shared by `list` and `replay`, so the command that showed you the damage is the command that fixes it, with one flag changed. `--json` on any of them gives machine-readable output for a runbook.

`eventsvc dlq` requires `--kafka`: the in-memory broker dies with the process that created it, so there would be nothing for a separate invocation to read, and a convincing but permanently-empty DLQ view is worse than an error message.

## What the failure headers buy you

Every retried or dead-lettered record carries its history in headers while the **payload stays byte-identical to the original** — so a replayed record needs no unwrapping by downstream consumers:

| Header | Meaning |
|---|---|
| `x-event-id` | stable identity used for deduplication |
| `x-attempt` | handler invocations so far; reset to 0 on replay |
| `x-original-topic` / `-partition` / `-offset` | where the record first lived, preserved across every hop |
| `x-not-before` | when a retry-tier record becomes due |
| `x-error-type` / `x-error-message` | the last failure (message truncated — headers must stay small) |
| `x-first-failed-at` / `x-failed-at` | first and latest failure times |
| `x-replay-count` | how many DLQ round trips this record has made |

A dead letter is therefore diagnosable on its own: what failed, how it failed, how many times, where it came from, and whether someone has already tried replaying it.

## Delivery guarantees, stated honestly

- **At-least-once.** Offsets are committed *after* the handler runs. A crash between handling and committing redelivers the batch — by design; committing first would silently drop it. The test suite kills a worker mid-batch and asserts the redelivery.
- **Duplicates are absorbed, not prevented.** A bounded, TTL'd idempotency store filters event ids that already succeeded. It is a protocol (`IdempotencyStore`), so a deployment can back it with Redis and share it across replicas. Failures are deliberately *not* marked — a failed record must stay retryable.
- **Ordering per key, not globally.** Records are partitioned by key with the same CRC32 rule on both brokers, so all events for one customer stay in one partition and arrive in order — until a record detours through the retry ladder, which trades per-key ordering for keeping the partition unblocked. That trade is the whole point of tiered retries.

## Shutting down without losing the batch

A deployment stops a consumer the same way it stops anything else: SIGTERM, then
SIGKILL once the grace period expires. What happens in between is the process's
problem, and the default is bad — the worker dies mid-batch, the offsets for the
records it *did* handle were never committed, and the group rebalances onto a
partition whose last few records are about to be redelivered. At-least-once makes
that redelivery safe, not free: every dropped batch is duplicate work downstream,
and the dedupe store only absorbs the records that actually finished.

`run_forever` is the long-running entry point, and it drains:

```python
service = EventService(broker=KafkaBroker("broker:9092"), topic="orders", handler=handle)
report = service.run_forever(timeout=25.0)      # blocks until SIGTERM/SIGINT
log.info("shutdown: %s", report)
# shutdown: drained 4/4 worker(s) in 0.412s — clean
```

The order is what matters:

1. **Stop accepting new records.** Each worker finishes the batch already in its
   hands — the loop only re-checks the stop flag between batches.
2. **Commit those offsets**, so the work that just completed is not repeated.
3. **Then** close the consumer and leave the group, so the rebalance happens once,
   after the work is done, instead of in the middle of it.

Some details that are easy to get wrong:

- **The timeout is a deadline for the whole fleet, not for each worker.** A grace
  period does not get longer because the topology grew two more retry tiers, and
  joining five threads with a five-second timeout each is a twenty-five second
  shutdown wearing a five-second label. Set it below the orchestrator's
  `terminationGracePeriodSeconds`.
- **A worker still inside its handler when the deadline expires is named, not
  killed.** Closing its consumer is precisely the mid-batch rebalance this path
  exists to avoid, so the report lists it as a straggler and the process exit
  releases it. `report.clean` is false, and that is a real signal — a shutdown
  that quietly gave up on two workers otherwise looks exactly like a good one.
- **The signal handler only sets an event.** It runs between two bytecodes of
  whichever thread was executing, so draining a consumer group from inside it is
  a good way to deadlock on a lock that thread already holds. The waiting thread
  does the actual work.
- **`stop(drain=False)` stops between records**, handing the rest of the batch
  back for redelivery. That is the second-signal behaviour — when the grace
  period is nearly up, being redelivered beats being killed mid-commit.

`shutdown_on_signals` is usable on its own, and restores the previous handlers on
exit so importing this library does not permanently repoint your process's SIGINT.

## Lag, measured from outside the group

The obvious place to compute consumer lag is inside the consumer: ask it what it owns, ask the broker for the high watermark, subtract. `Consumer.lag()` does exactly that, and it has a failure mode worth more than its convenience — **it can only report partitions a living consumer is currently assigned.**

So when the fleet crashes, the lag series does not spike. It stops being exported at all. Prometheus holds the last value for a few minutes and then the partition disappears from the graph, and the alert anyone would actually write —

```promql
eventsvc_consumer_lag > 10000
```

— never fires, because there is no series left to evaluate it against. The outage that most needs the metric is the one that deletes it.

`LagExporter` measures the same quantity from the other side, reading the group's committed offsets from the coordinator and the log end offsets from the partition leaders:

```python
exporter = service.lag_exporter()      # the whole topology: source, retry tiers, DLQ
exporter.start(interval=15.0)          # daemon thread; also a context manager

snapshot = exporter.snapshot()         # or sample synchronously
snapshot.total, snapshot.max_lag       # (1043, 601)
snapshot.worst                         # PartitionLag(orders.retry.9s[0], committed=88, high=689)
```

What that buys, in order of how much it matters:

- **A dead group still reports.** Every partition of every watched topic is covered whether or not anything is consuming it, so the backlog keeps climbing visibly after the last worker dies.
- **Measuring does not perturb.** An exporter that subscribed in order to observe would be counted as a member, take a share of the partitions in the rebalance, and consume records the real workers then never see. Observation would cause the incident.
- **The retry tiers are visible.** A backlog on `orders.retry.9s` is a failing downstream that the source topic cannot show, because those records were committed on the source the moment they were republished. Nothing consumes `orders.dlq` at all, so its lag is simply the number of dead letters nobody has dealt with — a good thing to alert on.
- **It runs anywhere.** In-process beside the workers, or as a sidecar with no relationship to them: it needs a broker address and a group name, not a seat in the group.

Two distinctions are deliberately kept rather than flattened into one number:

- **Never committed is not offset zero.** A partition the group has never committed reports `committed=None` and sets no `eventsvc_committed_offset` sample at all — publishing a zero there is a lie that `rate()` reads as a consumer sitting perfectly still. "Nothing has ever run here" and "caught up at the start of the log" are different pages.
- **A commit behind the low watermark is data loss, not lag.** If retention deleted records before the group read them, the group resumes from the low watermark and those records are never delivered to anyone. The lag number is honest about the backlog that still exists (it counts from the low watermark, not the stale commit) and the partition is flagged separately.

From the command line, against a real broker:

```bash
eventsvc lag --topic orders --kafka broker:9092
```

```text
── lag — group 'orders-workers', 1043 record(s) behind
  orders           437
  orders.retry.1s  0
  orders.retry.3s  0
  orders.retry.9s  604
  orders.dlq       2

  worst partition: orders.retry.9s[0] — 601 behind
```

`--verbose` gives a line per partition with the committed and end offsets, `--json` is the runbook form, and `--watch 10` keeps sampling instead of printing once. Reading lag moves no offsets and joins no group, so unlike `dlq replay` it needs no confirmation and no dry run — it is safe to point at production in a loop.

## Metrics

`Metrics.serve(port)` exposes `/metrics`; the set is picked to answer the on-call questions in the order they get asked:

| Question | Metric |
|---|---|
| Am I falling behind? | `eventsvc_consumer_lag{topic,partition,group}` |
| Is the work succeeding? | `eventsvc_messages_processed{outcome=ok\|retried\|dead_lettered\|duplicate}` |
| Is it degrading? | `eventsvc_retry_attempts{attempt}` and `eventsvc_dead_letters{error}` |
| Why is it slow? | `eventsvc_handler_seconds` histogram, `eventsvc_inflight_messages` |
| Which side stalled? | `eventsvc_committed_offset` vs `eventsvc_log_end_offset` |

Retry depth is the early-warning signal the others miss: throughput looks healthy while every record quietly takes four attempts.

The last row is the one that turns a graph into a diagnosis. A backlog that stops growing is either a consumer that died or a producer that did, and lag alone flattens out identically for both; the two halves of the subtraction exported separately settle it with `rate(eventsvc_committed_offset)` against `rate(eventsvc_log_end_offset)`.

## Design decisions

- **A broker protocol, two implementations.** The service depends on `Producer` / `Consumer` / `Broker` protocols — a deliberately small subset of the Kafka API. The in-memory broker implements *log semantics* (append-only partitions, consumer groups with range assignment and rebalancing, committed offsets separate from positions, seek), not a `list.pop()` queue — otherwise the tests would pass while the real deployment broke on redelivery, rebalances, or replays. CI runs the same end-to-end story against a real Kafka container to keep the protocol honest.
- **Delay-named retry topics.** `orders.retry.9s` documents itself in any broker UI, and two ladder tiers that collapse to the same capped delay correctly share one topic.
- **Time is injected.** The `Clock` protocol is the only source of time. Tests and the demo drive a manual clock, so a 13-second ladder is exercised — delays observed, ordering asserted — in under a second, deterministically.
- **The worker is tier-agnostic.** One worker class serves the source topic and every retry tier; the record's headers decide the next hop. No special case per tier, and the ladder's length is pure configuration.
- **Replay restores the original.** Replayed records return to the source topic with the original payload, a reset attempt counter (a replay deserves the full ladder), and an incremented `x-replay-count` — so a record on its third round trip is visible evidence that the fix wasn't.
- **Synchronous, flushed produces in the Kafka adapter.** Each republish is acknowledged before the offset commit that depends on it. A high-volume deployment would batch; this codebase optimizes for a correctness argument you can actually follow.

## Project layout

```text
src/eventsvc/
  types.py        Message, TopicPartition, RecordMetadata
  broker.py       the Producer/Consumer/Broker protocols + shared partitioner
  memory.py       in-memory broker: partitions, groups, rebalancing, offsets, seek
  kafka.py        the same protocols over confluent-kafka (optional extra)
  clock.py        Clock protocol; SystemClock and ManualClock
  retry.py        RetryPolicy, delay ladder math, topic naming
  envelope.py     failure-provenance headers; replay header rules
  worker.py       the consume loop: dedupe -> handle -> route -> commit
  service.py      topology wiring; cooperative and threaded run modes
  dlq.py          dead-letter inspection, summary, selective replay
  lag.py          consumer lag read from broker metadata, not from a member
  lifecycle.py    signal watch, drain-on-SIGTERM, shutdown reporting
  idempotency.py  bounded TTL'd dedupe store (protocol + in-memory impl)
  metrics.py      Prometheus metric surface
  samples.py      deterministic demo workload
  cli.py          eventsvc demo / topics / dlq
```

## Roadmap

- [x] Broker protocol with an in-memory implementation that has real log semantics
- [x] Tiered retry topics, failure-provenance headers, and dead-lettering
- [x] At-least-once delivery with a pluggable idempotency store
- [x] DLQ inspection, aggregation, and selective replay
- [x] Prometheus metrics chosen for on-call use
- [x] Kafka adapter, exercised against a real broker in CI
- [x] `eventsvc dlq` — operate the dead-letter queue from the command line
- [x] Consumer-lag exporter that reads committed offsets without joining the group
- [ ] Redis-backed idempotency store so dedupe survives a restart and spans replicas
- [ ] Batch handlers: hand a worker a slice of records, commit once
- [x] Graceful drain on SIGTERM — finish in-flight records before the rebalance
- [ ] Schema validation at the edge, so a malformed payload is permanent by construction
- [ ] `py.typed` and a strict type-check gate in CI

## Development

```bash
pip install -e ".[dev]"
pytest                        # 172 tests, all offline
ruff check src tests

# integration tests against any reachable broker:
pip install -e ".[dev,kafka]"
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest -m kafka
```

## License

[MIT](LICENSE) © Varun Kammadanam
