"""kafka-event-service: an event-driven microservice skeleton.

Consumer groups, tiered retry topics, a dead-letter queue with replay, and
Prometheus metrics — written against a small broker protocol, so the identical
service runs on an in-memory broker locally and on real Kafka in CI.
"""

from .broker import Broker, Consumer, Producer
from .clock import Clock, ManualClock, SystemClock
from .dlq import DeadLetter, DeadLetterQueue
from .errors import BrokerError, EventServiceError, PermanentError, RetriableError
from .idempotency import IdempotencyStore, InMemoryIdempotencyStore, NullIdempotencyStore
from .lifecycle import DEFAULT_SIGNALS, ShutdownReport, SignalWatch, shutdown_on_signals
from .memory import InMemoryBroker
from .metrics import Metrics, Outcome
from .retry import RetryPolicy, dlq_topic, retry_topic
from .service import EventService
from .types import Message, RecordMetadata, TopicPartition
from .worker import Worker, WorkerStats

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_SIGNALS",
    "Broker",
    "BrokerError",
    "Clock",
    "Consumer",
    "DeadLetter",
    "DeadLetterQueue",
    "EventService",
    "EventServiceError",
    "IdempotencyStore",
    "InMemoryBroker",
    "InMemoryIdempotencyStore",
    "ManualClock",
    "Message",
    "Metrics",
    "NullIdempotencyStore",
    "Outcome",
    "PermanentError",
    "Producer",
    "RecordMetadata",
    "RetriableError",
    "RetryPolicy",
    "ShutdownReport",
    "SignalWatch",
    "SystemClock",
    "TopicPartition",
    "Worker",
    "WorkerStats",
    "dlq_topic",
    "retry_topic",
    "shutdown_on_signals",
]
