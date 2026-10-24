"""Snapshots and inputs held by reference, a due index bucketed by second, a bounded decision log and a random epoch."""

from __future__ import annotations

import asyncio
import bisect
import math
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import cast

from ..core.types import LoggedInput
from ..signal import AbortSignal
from .types import (
    CommitOutcome,
    DecisionKind,
    DecisionLog,
    DecisionPage,
    DecisionQuery,
    DecisionRow,
    DueKey,
    LogBounds,
    Retention,
    ScopeStore,
    ScopeWrite,
    Snapshot,
    StoredScope,
)


@dataclass(slots=True)
class _Stored:
    version: int
    snapshot: Snapshot | None
    tail: list[LoggedInput]
    due: float | None


@dataclass(slots=True)
class _Scoped:
    seqs: list[int] = field(default_factory=lambda: list[int]())
    head: int = 0


def _sequence(position: str) -> int:
    if not position.isascii() or not position.isdigit() or str(int(position)) != position or int(position) > 9007199254740991:
        raise TypeError(f'attend: invalid position "{position}"')
    return int(position)


def _bucket(due: float) -> float:
    """``Math.floor(due / 1000)``: the second a due value falls in, with Infinity kept as it is."""
    return math.floor(due / 1000) if math.isfinite(due) else due


def _now_ms() -> float:
    return time.time_ns() // 1_000_000


class MemoryStore(ScopeStore):
    """The in-memory ScopeStore: atomic within one process, lost when it exits."""

    durable = False

    def __init__(self, *, max_rows: int | None = None, max_age_ms: float | None = None) -> None:
        self._scopes: dict[str, _Stored] = {}
        self._buckets: dict[float, set[str]] = {}
        self._keys: list[float] = []
        self._max_rows = max_rows
        self._max_age_ms = max_age_ms
        self._log = _MemoryLog(self)
        self.log = self._log

    def _remove_due(self, scope: str, due: float | None) -> None:
        if due is None:
            return
        key = _bucket(due)
        bucket = self._buckets[key]
        bucket.discard(scope)
        if not bucket:
            del self._buckets[key]
            del self._keys[bisect.bisect_left(self._keys, key)]

    async def load(self, scope: str) -> StoredScope | None:
        stored = self._scopes.get(scope)
        return {"version": stored.version, "snapshot": stored.snapshot, "tail": list(stored.tail)} if stored is not None else None

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        if len(writes) > 1:
            names: set[str] = set()
            for write in writes:
                if write["scope"] in names:
                    raise TypeError(f'attend: a commit names scope "{write["scope"]}" twice')
                names.add(write["scope"])
        outcomes: list[CommitOutcome] = []
        log = self._log
        before = log.last
        for write in writes:
            scope = write["scope"]
            stored = self._scopes.get(scope)
            if (stored.version if stored is not None else 0) != write["expectedVersion"]:
                outcomes.append("conflict")
                continue
            if stored is None:
                stored = _Stored(0, None, [], None)
                self._scopes[scope] = stored
            if write["snapshot"] is not None:
                stored.snapshot = write["snapshot"]
                stored.tail = []
            else:
                stored.tail.extend(write["inputs"])
            stored.version = write["expectedVersion"] + len(write["inputs"])
            if "due" in write:
                due = write["due"]
                self._remove_due(scope, stored.due)
                stored.due = due
                if due is not None:
                    key = _bucket(due)
                    bucket = self._buckets.get(key)
                    if bucket is None:
                        bucket = set[str]()
                        self._buckets[key] = bucket
                        bisect.insort(self._keys, key)
                    bucket.add(scope)
            for append in write["appends"]:
                log.append(cast(DecisionRow, {**append, "position": ""}))
            outcomes.append("ok")
        if log.last > before:
            log.retain(self._max_rows if self._max_rows is not None else 10_000, self._max_age_ms)
            log.wake()
        return outcomes

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        result: list[DueKey] = []
        if limit <= 0:
            return result
        end = _bucket(now)
        start = bisect.bisect_left(self._keys, _bucket(after["due"])) if after is not None else 0
        for i in range(start, len(self._keys)):
            key = self._keys[i]
            if key > end:
                break
            matches: list[DueKey] = []
            for scope in self._buckets[key]:
                due = self._scopes[scope].due
                assert due is not None
                if due <= now and (after is None or due > after["due"] or (due == after["due"] and _utf16(scope) > _utf16(after["scope"]))):
                    matches.append({"due": due, "scope": scope})
            matches.sort(key=lambda match: (match["due"], _utf16(match["scope"])))
            for match in matches:
                result.append(match)
                if len(result) >= limit:
                    return result
        return result

    async def remove(self, scope: str) -> None:
        stored = self._scopes.pop(scope, None)
        if stored is not None:
            self._remove_due(scope, stored.due)


def _utf16(text: str) -> bytes:
    return text.encode("utf-16-be", "surrogatepass")


class _MemoryLog(DecisionLog):
    def __init__(self, store: MemoryStore) -> None:
        self._store = store
        self._epoch = str(uuid.uuid4())
        self._ring: list[DecisionRow | None] = []
        self._head = 0
        self.last = 0
        self._by_scope: dict[str, _Scoped] = {}
        self._by_kind: dict[DecisionKind, dict[str, DecisionRow]] = {"decision": {}, "deadLetter": {}}
        self._waiters: set[Callable[[], None]] = set()

    def append(self, row: DecisionRow) -> None:
        kind = row["kind"]
        if row["id"] in self._by_kind[kind]:
            return
        self.last += 1
        row["position"] = str(self.last)
        self._ring.append(row)
        self._by_kind[kind][row["id"]] = row
        scoped = self._by_scope.get(row["scope"])
        if scoped is None:
            scoped = _Scoped()
            self._by_scope[row["scope"]] = scoped
        scoped.seqs.append(self.last)

    def wake(self) -> None:
        for wake in list(self._waiters):
            wake()

    def _drop_oldest(self) -> None:
        row = self._ring[self._head]
        assert row is not None
        self._ring[self._head] = None
        self._head += 1
        del self._by_kind[row["kind"]][row["id"]]
        scoped = self._by_scope[row["scope"]]
        scoped.head += 1
        if scoped.head == len(scoped.seqs):
            del self._by_scope[row["scope"]]
        elif scoped.head >= 16 and scoped.head * 2 >= len(scoped.seqs):
            scoped.seqs = scoped.seqs[scoped.head :]
            scoped.head = 0
        if self._head >= 1024 and self._head * 2 >= len(self._ring):
            self._ring = self._ring[self._head :]
            self._head = 0

    def retain(self, max_rows: int | None, age: float | None) -> None:
        if max_rows is not None:
            while len(self._ring) - self._head > max(max_rows, 1):
                self._drop_oldest()
        if age is not None:
            cutoff = _now_ms() - age
            while len(self._ring) - self._head > 1:
                row = self._ring[self._head]
                assert row is not None
                if not row["at"] < cutoff:
                    break
                self._drop_oldest()

    async def epoch(self) -> str:
        return self._epoch

    async def bounds(self) -> LogBounds:
        first = self._ring[self._head] if self._head < len(self._ring) else None
        return {"first": first["position"] if first is not None else None, "last": None if self.last == 0 else str(self.last)}

    async def read(self, query: DecisionQuery) -> DecisionPage:
        after = 0 if query["after"] is None else _sequence(query["after"])
        ring = self._ring
        first = self.last - (len(ring) - self._head) + 1
        scoped = None if "scope" not in query else self._by_scope.get(query["scope"], _Scoped())
        if scoped is not None:
            index = bisect.bisect_left(scoped.seqs, after + 1, scoped.head)
            end = len(scoped.seqs)
        else:
            index = self._head + max(0, after + 1 - first)
            end = len(ring)
        rows: list[DecisionRow] = []
        limit = query["limit"]
        scan_limit = query.get("scanLimit", 4 * limit)
        prefix = query.get("scopePrefix")
        kinds = query.get("kinds")
        routes = query.get("routes")
        since = query.get("sinceAt")
        examined = 0
        checked = query["after"]
        while index < end:
            if len(rows) >= limit or examined >= scan_limit:
                return {"rows": rows, "checked": checked, "more": True}
            row = ring[self._head + (scoped.seqs[index] - first)] if scoped is not None else ring[index]
            assert row is not None
            index += 1
            examined += 1
            checked = row["position"]
            if prefix is not None and not row["scope"].startswith(prefix):
                continue
            if kinds is not None and row["kind"] not in kinds:
                continue
            if routes is not None and (row["route"] is None or row["route"] not in routes):
                continue
            if since is not None and row["at"] < since:
                continue
            rows.append(row)
        return {"rows": rows, "checked": None if self.last == 0 and query["after"] is None else str(max(self.last, after)), "more": False}

    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None:
        return self._by_kind[kind].get(id)

    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        seq = 0 if after is None else _sequence(after)
        if signal.aborted or self.last > seq:
            return
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def finish() -> None:
            if not future.done():
                future.set_result(None)

        def wake() -> None:
            if self.last > seq:
                finish()

        timer = loop.call_later(max(0, block_ms) / 1000, finish)
        remove = signal.add_listener(finish)
        self._waiters.add(wake)
        try:
            await future
        finally:
            timer.cancel()
            remove()
            self._waiters.discard(wake)

    async def trim(self, retention: Retention) -> None:
        self.retain(retention.get("maxRows"), retention.get("olderThanMs"))

    def compare(self, a: str, b: str) -> int:
        return _sequence(a) - _sequence(b)


def memory_store(*, max_rows: int | None = None, max_age_ms: float | None = None) -> MemoryStore:
    """The in-memory store. ``max_rows`` (default 10,000) and ``max_age_ms`` bound the decision log."""
    return MemoryStore(max_rows=max_rows, max_age_ms=max_age_ms)
