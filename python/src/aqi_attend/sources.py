"""An in-memory event log, and a resumable source over any event log. Ports of ``memoryLog`` and ``logSource``."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol, cast

from ._json import clone
from .clock import Clock, sleep, system_clock
from .core.types import Event, LogEntry, SourceBatch
from .signal import AbortSignal


class EventLog(Protocol):
    """An append-only log per scope for producers outside the process. At-least-once: the reducer dedups by event id."""

    async def append(self, scope: str, events: list[Event]) -> None: ...

    async def read(self, scope: str, after: str | None, *, limit: int, block_ms: float | None = None) -> list[LogEntry]: ...


class _Partition:
    __slots__ = ("entries", "next", "waiters")

    def __init__(self) -> None:
        self.next = 1
        self.entries: list[LogEntry] = []
        self.waiters: set[Callable[[], None]] = set()


class MemoryLog:
    """An in-memory event log with optional retention. Positions are twenty digits, so they compare as strings."""

    def __init__(self, retain: int | None = None) -> None:
        self._retain = retain
        self._partitions: dict[str, _Partition] = {}

    def _partition(self, scope: str) -> _Partition:
        partition = self._partitions.get(scope)
        if partition is None:
            partition = _Partition()
            self._partitions[scope] = partition
        return partition

    def _page(self, scope: str, after: str | None, limit: int) -> list[LogEntry]:
        entries = self._partition(scope).entries
        start = 0 if after is None else next((i for i, entry in enumerate(entries) if entry["position"] > after), -1)
        if start == -1:
            return []
        return [{"position": entry["position"], "event": cast(Event, clone(entry["event"]))} for entry in entries[start : start + limit]]

    async def append(self, scope: str, events: list[Event]) -> None:
        partition = self._partition(scope)
        for event in events:
            partition.entries.append({"position": str(partition.next).zfill(20), "event": cast(Event, clone(event))})
            partition.next += 1
        if self._retain is not None and len(partition.entries) > self._retain:
            del partition.entries[: len(partition.entries) - self._retain]
        for wake in list(partition.waiters):
            wake()

    async def read(self, scope: str, after: str | None, *, limit: int, block_ms: float | None = None) -> list[LogEntry]:
        first = self._page(scope, after, limit)
        if first or block_ms is None or block_ms <= 0:
            return first
        partition = self._partition(scope)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def done() -> None:
            if not future.done():
                future.set_result(None)

        timer = loop.call_later(block_ms / 1000, done)
        partition.waiters.add(done)
        try:
            await future
        finally:
            timer.cancel()
            partition.waiters.discard(done)
        return self._page(scope, after, limit)


def memory_log(*, retain: int | None = None) -> MemoryLog:
    return MemoryLog(retain)


class LogSource:
    """A resumable source over an event log, keyed ``log:<scope>``, with blocking reads and paced polls."""

    def __init__(self, log: EventLog, scope: str, *, limit: int = 100, block_ms: float = 1000, poll_ms: float = 250, clock: Clock | None = None) -> None:
        self.key = f"log:{scope}"
        self._log = log
        self._scope = scope
        self._limit = limit
        self._block_ms = block_ms
        self._poll_ms = poll_ms
        self._clock = clock if clock is not None else system_clock()

    async def subscribe(self, cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], signal: AbortSignal, /) -> None:
        after = cursor
        while not signal.aborted:
            entries = await self._log.read(self._scope, after, limit=self._limit, block_ms=self._block_ms)
            if signal.aborted:
                return
            if entries:
                await emit({"events": [entry["event"] for entry in entries], "cursor": entries[-1]["position"]})
                after = entries[-1]["position"]
            else:
                await sleep(self._clock, self._poll_ms, signal)


def log_source(log: EventLog, scope: str, *, limit: int = 100, block_ms: float = 1000, poll_ms: float = 250, clock: Clock | None = None) -> LogSource:
    """A source for ``attend.attach(scope, ...)`` that reads ``scope``'s events from ``log`` and resumes from the persisted cursor."""
    return LogSource(log, scope, limit=limit, block_ms=block_ms, poll_ms=poll_ms, clock=clock)
