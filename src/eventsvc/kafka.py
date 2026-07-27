"""The real-broker adapter, on confluent-kafka.

Everything above the broker protocols is broker-agnostic; this module is the
only file in the package that imports a Kafka client, and it is imported lazily
so the default install stays dependency-light (``pip install
"kafka-event-service[kafka]"`` to enable it).

Design choices worth noting:

* **JSON values, UTF-8 headers.** Matches the in-memory broker so the rest of
  the stack cannot tell which one it is on.
* **Manual commits, ``enable.auto.commit=false``.** The worker's correctness
  argument depends on committing *after* the handler; auto-commit would quietly
  reorder that.
* **Synchronous ``send``.** Each produce is flushed before returning, trading
  throughput for the same happens-before guarantee the in-memory broker gives.
  A high-volume deployment would batch; this adapter optimizes for being
  obviously correct against the same test suite.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from .errors import BrokerError
from .types import Headers, Message, RecordMetadata, TopicPartition


def _require_confluent() -> Any:
    try:
        import confluent_kafka
        import confluent_kafka.admin  # the admin subpackage is not imported implicitly
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise BrokerError(
            "confluent-kafka is not installed; "
            'install with: pip install "kafka-event-service[kafka]"'
        ) from exc
    return confluent_kafka


class KafkaBroker:
    """A :class:`~eventsvc.broker.Broker` backed by a real Kafka cluster."""

    def __init__(self, bootstrap_servers: str, *, default_partitions: int = 3) -> None:
        self._kafka = _require_confluent()
        self.bootstrap_servers = bootstrap_servers
        self._default_partitions = default_partitions
        self._admin = self._kafka.admin.AdminClient({"bootstrap.servers": bootstrap_servers})

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        new_topic = self._kafka.admin.NewTopic(
            topic, num_partitions=partitions, replication_factor=1
        )
        futures = self._admin.create_topics([new_topic])
        try:
            futures[topic].result(timeout=30)
        except self._kafka.KafkaException as exc:
            error = exc.args[0]
            if error.code() != self._kafka.KafkaError.TOPIC_ALREADY_EXISTS:
                raise BrokerError(f"could not create topic {topic}: {error}") from exc

    def topics(self) -> list[str]:
        metadata = self._admin.list_topics(timeout=10)
        return sorted(name for name in metadata.topics if not name.startswith("__"))

    def partition_count(self, topic: str) -> int:
        metadata = self._admin.list_topics(topic=topic, timeout=10)
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            raise BrokerError(f"unknown topic: {topic}")
        return len(info.partitions)

    def producer(self) -> KafkaProducerAdapter:
        return KafkaProducerAdapter(self._kafka, self.bootstrap_servers)

    def consumer(self, group_id: str, *, client_id: str | None = None) -> KafkaConsumerAdapter:
        return KafkaConsumerAdapter(self._kafka, self.bootstrap_servers, group_id, client_id)

    def close(self) -> None:
        """The admin client has no explicit close; nothing to release."""


class KafkaProducerAdapter:
    """Producer protocol over ``confluent_kafka.Producer``."""

    def __init__(self, kafka: Any, bootstrap_servers: str) -> None:
        self._kafka = kafka
        self._producer = kafka.Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def send(
        self,
        topic: str,
        value: dict[str, Any],
        *,
        key: str | None = None,
        headers: Headers | None = None,
    ) -> RecordMetadata:
        delivered: dict[str, Any] = {}

        def on_delivery(err: Any, msg: Any) -> None:
            delivered["err"] = err
            delivered["msg"] = msg

        self._producer.produce(
            topic,
            value=json.dumps(value, sort_keys=True).encode("utf-8"),
            key=key.encode("utf-8") if key is not None else None,
            headers=[(k, v.encode("utf-8")) for k, v in (headers or {}).items()],
            on_delivery=on_delivery,
        )
        self._producer.flush(30)
        err = delivered.get("err")
        if err is not None:
            raise BrokerError(f"produce to {topic} failed: {err}")
        msg = delivered["msg"]
        return RecordMetadata(msg.topic(), msg.partition(), msg.offset())

    def flush(self, timeout: float | None = None) -> None:
        self._producer.flush(timeout if timeout is not None else 30)

    def close(self) -> None:
        self.flush()


class KafkaConsumerAdapter:
    """Consumer protocol over ``confluent_kafka.Consumer``."""

    def __init__(
        self, kafka: Any, bootstrap_servers: str, group_id: str, client_id: str | None
    ) -> None:
        self._kafka = kafka
        self.group_id = group_id
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
        if client_id is not None:
            config["client.id"] = client_id
        self._consumer = kafka.Consumer(config)
        self._closed = False

    def subscribe(self, topics: Sequence[str]) -> None:
        self._consumer.subscribe(list(topics))

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(tp.topic, tp.partition) for tp in self._consumer.assignment()]

    def poll(self, max_records: int = 1, timeout: float = 0.1) -> list[Message]:
        messages: list[Message] = []
        deadline_budget = timeout
        while len(messages) < max_records:
            raw = self._consumer.poll(deadline_budget)
            if raw is None:
                break
            deadline_budget = 0.05  # once records are flowing, drain quickly
            if raw.error() is not None:
                if raw.error().code() == self._kafka.KafkaError._PARTITION_EOF:
                    continue
                raise BrokerError(str(raw.error()))
            messages.append(self._decode(raw))
        return messages

    def _decode(self, raw: Any) -> Message:
        try:
            value = json.loads(raw.value().decode("utf-8")) if raw.value() else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            value = {"_raw": repr(raw.value())}
        if not isinstance(value, dict):
            value = {"_value": value}
        headers = {
            k: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v)
            for k, v in (raw.headers() or [])
        }
        timestamp_type, timestamp_ms = raw.timestamp()
        return Message(
            topic=raw.topic(),
            partition=raw.partition(),
            offset=raw.offset(),
            key=raw.key().decode("utf-8") if raw.key() else None,
            value=value,
            headers=headers,
            timestamp=timestamp_ms / 1000 if timestamp_type else 0.0,
        )

    def position(self, tp: TopicPartition) -> int:
        ktp = self._kafka.TopicPartition(tp.topic, tp.partition)
        positions = self._consumer.position([ktp])
        offset = positions[0].offset
        return max(0, offset)

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self._consumer.seek(self._kafka.TopicPartition(tp.topic, tp.partition, offset))

    def commit(self) -> None:
        try:
            self._consumer.commit(asynchronous=False)
        except self._kafka.KafkaException as exc:
            error = exc.args[0]
            # Committing with nothing consumed yet is not an error condition.
            if error.code() != self._kafka.KafkaError._NO_OFFSET:
                raise BrokerError(f"commit failed: {error}") from exc

    def lag(self) -> dict[TopicPartition, int]:
        lags: dict[TopicPartition, int] = {}
        for tp in self.assignment():
            ktp = self._kafka.TopicPartition(tp.topic, tp.partition)
            try:
                _low, high = self._consumer.get_watermark_offsets(ktp, timeout=5)
            except self._kafka.KafkaException:
                continue
            lags[tp] = max(0, high - self.position(tp))
        return lags

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._consumer.close()
