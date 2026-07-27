"""The retry ladder: delay tiers expressed as topics.

The naive way to retry inside a consumer is ``time.sleep()`` and try again. It
is also the fastest way to take down a partition: the consumer owns that
partition exclusively, so a 30-second backoff on one bad record stalls every
healthy record queued behind it. That is head-of-line blocking, and at any real
volume it turns a single flaky downstream into a total outage.

The fix is to move the wait off the hot path. A failed record is *republished*
to a retry topic whose entire purpose is one fixed delay, and the source-topic
consumer commits and moves on immediately. Each retry topic gets its own
consumer, and because every record on ``orders.retry.3s`` was scheduled with the
same 3-second delay, the records are due in exactly the order they are stored —
so that consumer *can* safely block on its head record. Waiting is fine once the
only thing behind you is other things that are also waiting.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BASE_DELAY = 1.0
DEFAULT_MULTIPLIER = 3.0
DEFAULT_MAX_DELAY = 60.0


def format_delay(seconds: float) -> str:
    """Render a delay as a topic-name-safe suffix: ``200ms``, ``1s``, ``1.5s``."""
    milliseconds = round(seconds * 1000)
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    if milliseconds % 1000 == 0:
        return f"{milliseconds // 1000}s"
    return f"{milliseconds / 1000:g}s"


def retry_topic(source: str, delay: float) -> str:
    """The retry topic for ``source`` at a given delay, e.g. ``orders.retry.3s``.

    Naming a tier after its delay rather than its index is what makes the topic
    list self-documenting in a broker UI: ``orders.retry.9s`` says what it is
    without a lookup, and two tiers that collapse to the same delay after the
    cap correctly share one topic.
    """
    return f"{source}.retry.{format_delay(delay)}"


def dlq_topic(source: str) -> str:
    """The dead-letter topic for ``source``."""
    return f"{source}.dlq"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential backoff.

    ``max_attempts`` counts handler invocations, not retries: with the default of
    4 a record is tried once from the source topic and up to three more times
    from the ladder before it is dead-lettered.
    """

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay: float = DEFAULT_BASE_DELAY
    multiplier: float = DEFAULT_MULTIPLIER
    max_delay: float = DEFAULT_MAX_DELAY

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")

    def delays(self) -> tuple[float, ...]:
        """The delay before each retry, in order. Length is ``max_attempts - 1``."""
        return tuple(
            min(self.base_delay * self.multiplier**index, self.max_delay)
            for index in range(self.max_attempts - 1)
        )

    def delay_for_attempt(self, attempt: int) -> float | None:
        """The wait before attempt number ``attempt + 1``, or ``None`` if exhausted.

        ``attempt`` is the number of tries already made, so ``delay_for_attempt(1)``
        is the pause after the first failure.
        """
        if attempt < 1 or attempt >= self.max_attempts:
            return None
        return self.delays()[attempt - 1]

    def retry_topics(self, source: str) -> tuple[str, ...]:
        """Every retry topic for ``source``, deduplicated, in tier order."""
        names: list[str] = []
        for delay in self.delays():
            name = retry_topic(source, delay)
            if name not in names:
                names.append(name)
        return tuple(names)

    def topics_for(self, source: str) -> tuple[str, ...]:
        """Source topic, retry ladder, and dead-letter topic — every topic in play."""
        return (source, *self.retry_topics(source), dlq_topic(source))
