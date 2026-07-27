"""A synthetic order stream and a handler with controllable failure modes.

This is the demo's workload: an ``orders`` topic where some payments time out a
couple of times before succeeding (the retry ladder's happy case) and some
orders reference a product that does not exist (permanent — straight to the
DLQ). The failure behaviour is driven by the *payload*, not by randomness, so
every run of the demo and every test tells exactly the same story.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .envelope import attempt_of, new_event_id
from .errors import PermanentError, RetriableError
from .types import Message

KNOWN_PRODUCTS = frozenset(
    {"keyboard", "monitor", "desk", "chair", "lamp", "cable", "webcam", "headset"}
)


def sample_orders() -> list[dict[str, Any]]:
    """Twelve orders: nine clean, two that flake, one that can never succeed."""
    orders: list[dict[str, Any]] = [
        {"order_id": "ord-1001", "customer": "acme", "product": "keyboard", "quantity": 2},
        {"order_id": "ord-1002", "customer": "globex", "product": "monitor", "quantity": 1},
        # Payment gateway flakes twice, then succeeds on the third attempt.
        {
            "order_id": "ord-1003",
            "customer": "initech",
            "product": "desk",
            "quantity": 1,
            "fail_times": 2,
        },
        {"order_id": "ord-1004", "customer": "acme", "product": "chair", "quantity": 4},
        # References a product that does not exist: permanently rejected.
        {"order_id": "ord-1005", "customer": "hooli", "product": "hoverboard", "quantity": 1},
        {"order_id": "ord-1006", "customer": "globex", "product": "lamp", "quantity": 3},
        {"order_id": "ord-1007", "customer": "initech", "product": "cable", "quantity": 10},
        # One flake, recovered by the first retry tier.
        {
            "order_id": "ord-1008",
            "customer": "acme",
            "product": "webcam",
            "quantity": 1,
            "fail_times": 1,
        },
        {"order_id": "ord-1009", "customer": "hooli", "product": "headset", "quantity": 2},
        {"order_id": "ord-1010", "customer": "globex", "product": "keyboard", "quantity": 1},
        # Fails more times than the ladder allows: exhausts retries, lands in the DLQ.
        {
            "order_id": "ord-1011",
            "customer": "initech",
            "product": "monitor",
            "quantity": 2,
            "fail_times": 99,
        },
        {"order_id": "ord-1012", "customer": "acme", "product": "desk", "quantity": 1},
    ]
    return orders


@dataclass
class OrderHandler:
    """Processes an order, failing exactly as the payload instructs.

    ``fail_times: N`` simulates a downstream that rejects the first N attempts —
    the attempt counter in the record's headers decides whether this delivery is
    one of the doomed ones. An unknown product is a :class:`PermanentError`.

    ``downstream_fixed`` models the operational moment a DLQ replay exists for:
    the flaky dependency has been repaired, so ``fail_times`` stops applying and
    replayed records can finally succeed. Permanent failures stay permanent.
    """

    downstream_fixed: bool = False
    processed: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, message: Message) -> None:
        order = message.value
        product = order.get("product")
        if product not in KNOWN_PRODUCTS:
            raise PermanentError(f"unknown product: {product!r}")

        attempt = attempt_of(message) + 1
        fail_times = 0 if self.downstream_fixed else int(order.get("fail_times", 0))
        if attempt <= fail_times:
            raise RetriableError(
                f"payment gateway timeout for {order.get('order_id')} (attempt {attempt})"
            )

        self.processed.append(dict(order))

    def processed_ids(self) -> list[str]:
        return [order.get("order_id", "?") for order in self.processed]


def produce_orders(producer: Any, topic: str, orders: list[dict[str, Any]] | None = None) -> int:
    """Publish the sample orders, keyed by customer so each customer stays ordered."""
    orders = sample_orders() if orders is None else orders
    for order in orders:
        producer.send(
            topic,
            order,
            key=order.get("customer"),
            headers={"x-event-id": new_event_id()},
        )
    producer.flush()
    return len(orders)
