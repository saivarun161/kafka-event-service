"""The error taxonomy that drives routing.

The distinction matters more than it looks: retrying a failure that can never
succeed burns the whole delay ladder and delays the dead-letter signal by
minutes, while dead-lettering a transient blip throws away a message that would
have worked on the next attempt.
"""

from __future__ import annotations


class EventServiceError(Exception):
    """Base class for every error raised by this package."""


class RetriableError(EventServiceError):
    """A transient failure — a timeout, a 503, a locked row.

    Not required: any exception a handler raises is treated as retriable. This
    class exists so handlers can say so explicitly.
    """


class PermanentError(EventServiceError):
    """A failure that will fail identically forever — bad schema, unknown enum.

    Skips the retry ladder entirely and goes straight to the dead-letter topic.
    """


class BrokerError(EventServiceError):
    """The broker rejected an operation or is unreachable."""
