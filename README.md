# kafka-event-service

[![CI](https://github.com/saivarun161/kafka-event-service/actions/workflows/ci.yml/badge.svg)](https://github.com/saivarun161/kafka-event-service/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An **event-driven microservice skeleton** with the failure-handling machinery that real deployments need and tutorials skip: **consumer groups**, **tiered retry topics** with exponential backoff, a **dead-letter queue you can actually operate** (inspect, aggregate, selectively replay), **at-least-once delivery with idempotent handling**, and **Prometheus metrics** chosen for on-call use.

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
```

The demo completes instantly even though the ladder spans 13 seconds of delay, because time is injected: under the simulated clock the scheduler jumps straight to the next due record. Run `eventsvc demo --real-time` to wait the delays out for real, `--json` for machine-readable output, `--metrics` to dump the Prometheus exposition, and `eventsvc topics` to print the topic topology for any policy.

Same demo, real broker: `eventsvc demo --kafka localhost:9092` (needs `pip install -e ".[dev,kafka]"`).

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

## Metrics

`Metrics.serve(port)` exposes `/metrics`; the set is picked to answer the on-call questions in the order they get asked:

| Question | Metric |
|---|---|
| Am I falling behind? | `eventsvc_consumer_lag{topic,partition,group}` |
| Is the work succeeding? | `eventsvc_messages_processed{outcome=ok\|retried\|dead_lettered\|duplicate}` |
| Is it degrading? | `eventsvc_retry_attempts{attempt}` and `eventsvc_dead_letters{error}` |
| Why is it slow? | `eventsvc_handler_seconds` histogram, `eventsvc_inflight_messages` |

Retry depth is the early-warning signal the others miss: throughput looks healthy while every record quietly takes four attempts.

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
  idempotency.py  bounded TTL'd dedupe store (protocol + in-memory impl)
  metrics.py      Prometheus metric surface
  samples.py      deterministic demo workload
  cli.py          eventsvc demo / eventsvc topics
```

## Development

```bash
pip install -e ".[dev]"
pytest                        # 86 tests, all offline, < 1s
ruff check src tests

# integration tests against any reachable broker:
pip install -e ".[dev,kafka]"
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest -m kafka
```

## License

[MIT](LICENSE) © Varun Kammadanam
