"""Errors raised by intake and effect handlers."""

from __future__ import annotations

from .core.types import OutboxLane


class PermanentError(Exception):
    """Raise from a judge, a sink or a source to skip retries: the judge or sink dead-letters at once, and ``attach`` stops."""

    permanent = True


def is_permanent(error: BaseException | None) -> bool:
    """Duck-typed, so any error with ``permanent = True`` counts, as in the TypeScript package."""
    return getattr(error, "permanent", False) is True


class OutboxFullError(Exception):
    """``push`` and a source's ``emit`` raise this when a scope's outbox lane is at its cap. Nothing is dropped."""

    def __init__(self, scope: str, lane: OutboxLane, size: int) -> None:
        super().__init__(f'attend: outbox for scope "{scope}" holds {size} undelivered {"decisions" if lane == "decisions" else "dead letters"}')
        self.scope = scope
        self.lane = lane
        self.size = size


class ConflictError(Exception):
    """``push``, ``end`` and ``flush`` raise this once the compare-and-swap budget is spent. Safe to retry."""

    def __init__(self, scope: str, attempts: int) -> None:
        super().__init__(f'attend: gave up on scope "{scope}" after {attempts} conflicts')
        self.scope = scope


class BackpressureError(Exception):
    """Intake exceeded the queued-input or scheduler-lag limit."""

    def __init__(self, retry_after_ms: int, queued: int) -> None:
        super().__init__(f"attend: over the admission limit with {queued} inputs queued; retry in {retry_after_ms} ms")
        self.retry_after_ms = retry_after_ms
        self.queued = queued


def message_of(error: BaseException) -> str:
    """What ``error.message`` is in JavaScript: the first argument, or the text of the error."""
    if len(error.args) == 1 and isinstance(error.args[0], str):
        return error.args[0]
    return str(error)


def name_of(error: BaseException) -> str:
    """The class name, as ``error.name`` would be in JavaScript, with Python's ``Exception`` as ``Error``."""
    name = type(error).__name__
    return "Error" if name == "Exception" else name
