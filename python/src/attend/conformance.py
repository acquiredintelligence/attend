"""Store conformance: every guarantee of ``ScopeStore`` and ``DecisionLog``, as cases. A port of ``src/testing/conformance.ts``.

``make`` returns a fresh, empty store per case.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from ._json import clone, stringify
from .core.types import Decision, DecisionAppend, DeliveredRoute, Entry, LoggedInput, ScopeRecord, SettlePolicy, Window
from .signal import AbortController, AbortSignal
from .store.types import (
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
from .testing import Case, assert_equal, assert_ok

POLICY: SettlePolicy = {
    "closingGraceMs": 500,
    "streamGapMs": 1000,
    "passiveQuietMs": 4000,
    "waitHorizonMs": 10_000,
    "maxWaits": 2,
    "maxEvents": 250,
    "maxWindowMs": 60_000,
    "retryBackoffMs": [1000, 4000, 16_000],
    "judgeTimeoutMs": 15_000,
    "deliveryTimeoutMs": 10_000,
    "recentCount": 8,
    "seenCount": 1000,
    "maxOutbox": 1000,
    "maxDeadLetters": 1000,
    "deadLetterBackoffMaxMs": 60_000,
}

type Make = Callable[[], Awaitable[ScopeStore] | ScopeStore]


def logged(id: str, now: float = 1000) -> LoggedInput:
    return {"input": {"kind": "events", "events": [{"id": id, "at": now, "name": "changed", "data": {"id": id}, "kind": "closing"}]}, "now": now}


def _window(id: str) -> Window:
    entry: Entry = {"id": f"event:{id}", "at": 1, "name": "changed", "data": {"nested": [None, True, "héllo 🌍\u0000"]}, "text": "héllo 🌍\u0000", "kind": "closing", "seenAt": 2}
    return {"id": id, "events": [entry], "reason": "closing", "waits": 0, "dropped": 0, "gap": False, "settledAt": 3}


def _decision(id: str, scope: str, route: DeliveredRoute = "interrupt", at: float = 1000) -> Decision:
    return {
        "id": id,
        "scope": scope,
        "route": route,
        "at": at,
        "window": _window(id),
        "confidence": 0.8,
        "predicates": {"relevant": 0.7},
        "probabilities": {"interrupt": 0.4, "queue": 0.2, "note": 0.1, "ask": 0.1, "skip": 0.1, "wait": 0.1},
    }


def append(id: str, scope: str = "s", route: DeliveredRoute = "interrupt", at: float = 1000) -> DecisionAppend:
    return {"kind": "decision", "id": id, "scope": scope, "route": route, "at": at, "body": _decision(id, scope, route, at)}


def letter(id: str, scope: str = "s", stage: str = "judge", at: float = 1000) -> DecisionAppend:
    failed = _decision(id, scope, "interrupt", at) if stage == "sink" else None
    return {
        "kind": "deadLetter",
        "id": id,
        "scope": scope,
        "route": failed["route"] if failed is not None else None,
        "at": at,
        "body": {"id": id, "scope": scope, "stage": stage, "window": _window(id), "decision": failed, "message": "could not deliver", "permanent": False, "attempts": 3, "at": at},  # type: ignore[typeddict-item]
    }


def record(scope: str, version: int) -> ScopeRecord:
    return {
        "scope": scope,
        "version": version,
        "cursors": {},
        "seen": [],
        "buffer": {"entries": [], "dropped": 0, "gap": False, "forcedAt": None},
        "batch": None,
        "outbox": {"decisions": [], "deadLetters": []},
        "recent": [],
        "dueAt": None,
    }


_UNSET: Any = object()


def write(
    scope: str, expected: int = 0, *, inputs: list[LoggedInput] | None = None, appends: list[DecisionAppend] | None = None, snapshot: Snapshot | None = None, due: Any = _UNSET
) -> ScopeWrite:
    result: ScopeWrite = {
        "scope": scope,
        "expectedVersion": expected,
        "inputs": inputs if inputs is not None else [logged(f"{scope}:{expected}")],
        "appends": appends or [],
        "snapshot": snapshot,
    }
    if due is not _UNSET:
        result["due"] = due
    result["serialized"] = {
        "inputs": [stringify(item) for item in result["inputs"]],
        "snapshot": None if snapshot is None else stringify(snapshot["record"]),
        "appends": [stringify(row["body"]) for row in result["appends"]],
    }
    return result


async def _rejects(run: Callable[[], Awaitable[object]]) -> None:
    try:
        await run()
    except Exception:
        return
    raise AssertionError("expected rejection")


class _Watched:
    """Keeps every value the store returned, and every write it was given, beside a copy, to check nothing changed them later."""

    def __init__(self) -> None:
        self.kept: list[tuple[object, object]] = []

    def keep[T](self, value: T) -> T:
        self.kept.append((value, clone(value)))
        return value

    def check(self) -> None:
        for i, (value, copy) in enumerate(self.kept):
            assert_equal(value, copy, f"returned value {i} unchanged by later calls")


class _WatchedLog(DecisionLog):
    def __init__(self, inner: DecisionLog, watched: _Watched) -> None:
        self._inner = inner
        self._watched = watched

    async def epoch(self) -> str:
        return await self._inner.epoch()

    async def bounds(self) -> LogBounds:
        return self._watched.keep(await self._inner.bounds())

    async def read(self, query: DecisionQuery) -> DecisionPage:
        return self._watched.keep(await self._inner.read(query))

    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None:
        return self._watched.keep(await self._inner.find(kind, id))

    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        await self._inner.wait(after, block_ms, signal)

    async def trim(self, retention: Retention) -> None:
        await self._inner.trim(retention)

    def compare(self, a: str, b: str) -> int:
        return self._inner.compare(a, b)


class _WatchedStore(ScopeStore):
    """The store under test, keeping every value its reads return and every write it is given, as ``watched`` does in TypeScript.

    After a case closes the store, none of them may have changed, so every read shape a case uses
    also checks aliasing.
    """

    def __init__(self, inner: ScopeStore, watched: _Watched) -> None:
        self.durable = inner.durable
        self._inner = inner
        self._watched = watched
        self.log = _WatchedLog(inner.log, watched)

    async def load(self, scope: str) -> StoredScope | None:
        return self._watched.keep(await self._inner.load(scope))

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        self._watched.keep(writes)
        return self._watched.keep(await self._inner.commit(writes))

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        return self._watched.keep(await self._inner.due(now, limit, after))

    async def remove(self, scope: str) -> None:
        await self._inner.remove(scope)

    async def close(self) -> None:
        await self._inner.close()


async def _fresh(make: Make) -> ScopeStore:
    made = make()
    return await made if isinstance(made, Awaitable) else made


def _cases(make: Make, cases: Sequence[tuple[str, Callable[[ScopeStore, _Watched], Awaitable[None]]]]) -> list[Case]:
    def case(name: str, run: Callable[[ScopeStore, _Watched], Awaitable[None]]) -> Case:
        async def go() -> None:
            watched = _Watched()
            store = _WatchedStore(await _fresh(make), watched)
            try:
                await run(store, watched)
            finally:
                await store.close()
            await asyncio.sleep(0)
            watched.check()

        return Case(name, go)

    return [case(name, run) for name, run in cases]


def scope_store_conformance(make: Make) -> list[Case]:
    """Every guarantee in ScopeStore's docstrings."""

    async def missing(store: ScopeStore, w: _Watched) -> None:
        assert_equal(w.keep(await store.load("missing")), None, "load")

    async def created(store: ScopeStore, w: _Watched) -> None:
        inputs: list[LoggedInput] = [logged("a"), logged("b"), logged("c"), {"input": {"kind": "flush"}, "now": 1001}]
        assert_equal(await store.commit([write("s", 0, inputs=inputs)]), ["ok"], "created")
        assert_equal(w.keep(await store.load("s")), {"version": 4, "snapshot": None, "tail": inputs}, "ordered inputs")

    async def counts(store: ScopeStore, w: _Watched) -> None:
        first = [logged("a"), logged("b"), logged("c")]
        second = [logged("d"), logged("e")]
        assert_equal(await store.commit([write("s", 0, inputs=first)]), ["ok"], "first commit")
        assert_equal(await store.commit([write("s", 3, inputs=second)]), ["ok"], "second commit")
        assert_equal(await store.load("s"), {"version": 5, "snapshot": None, "tail": [*first, *second]}, "version")

    async def stale(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s")])
        await store.commit([write("s", 1)])
        before = w.keep(await store.load("s"))
        assert_equal(await store.commit([write("s", 1, appends=[append("stale")], due=1000)]), ["conflict"], "stale")
        assert_equal(await store.load("s"), before, "unchanged")
        assert_equal(await store.log.find("decision", "stale"), None, "no stale append")
        assert_equal(await store.due(1000, 10), [], "no stale due")

    async def twice(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s")])
        before = await store.load("s")
        assert_equal(await store.commit([write("s")]), ["conflict"], "create twice")
        assert_equal(await store.load("s"), before, "unchanged")

    async def contenders(store: ScopeStore, w: _Watched) -> None:
        first = logged("first")
        await store.commit([write("s", 0, inputs=[first])])
        inputs = [logged(f"contender:{i}") for i in range(20)]
        outcomes = await asyncio.gather(*(store.commit([write("s", 1, inputs=[item])]) for item in inputs))
        assert_equal(sum(1 for [result] in outcomes if result == "ok"), 1, "exactly one winner")
        winner = inputs[next(i for i, [result] in enumerate(outcomes) if result == "ok")]
        assert_equal(await store.load("s"), {"version": 2, "snapshot": None, "tail": [first, winner]}, "winner persisted")

    async def grouped(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("a"), write("b")])
        before = await store.load("a")
        b = write("b", 1)
        c = write("c")
        assert_equal(await store.commit([b, write("a", 0, appends=[append("conflict", "a")]), c]), ["ok", "conflict", "ok"], "outcomes")
        loaded = await store.load("b")
        assert_equal(loaded["version"] if loaded else None, 2, "b committed")
        assert_equal(await store.load("a"), before, "a unchanged")
        assert_equal(await store.load("c"), {"version": 1, "snapshot": None, "tail": c["inputs"]}, "c committed")
        assert_equal(await store.log.find("decision", "conflict"), None, "no conflicting append")

    async def duplicate_scope(store: ScopeStore, w: _Watched) -> None:
        await _rejects(lambda: store.commit([write("a", 0, appends=[append("a", "a")], due=1000), write("b", 0, appends=[append("b", "b")], due=2000), write("b")]))
        assert_equal(await store.load("a"), None, "a absent")
        assert_equal(await store.load("b"), None, "b absent")
        assert_equal(await store.log.read({"after": None, "limit": 10}), {"rows": [], "checked": None, "more": False}, "no rows")
        assert_equal(await store.due(3000, 10), [], "no due values")

    async def snapshots(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s", 0, inputs=[logged("a"), logged("b"), logged("c")])])
        snapshot: Snapshot = {"version": 5, "record": {**record("s", 5), "seen": ["d", "e"]}, "policy": POLICY, "reducer": 2}
        await store.commit([write("s", 3, inputs=[logged("d"), logged("e")], snapshot=snapshot)])
        assert_equal(w.keep(await store.load("s")), {"version": 5, "snapshot": snapshot, "tail": []}, "snapshot replaced")
        item = logged("f")
        await store.commit([write("s", 5, inputs=[item])])
        assert_equal(await store.load("s"), {"version": 6, "snapshot": snapshot, "tail": [item]}, "tail after snapshot")
        replacement: Snapshot = {"version": 7, "record": record("s", 7), "policy": POLICY, "reducer": 3}
        await store.commit([write("s", 6, snapshot=replacement)])
        assert_equal(await store.load("s"), {"version": 7, "snapshot": replacement, "tail": []}, "older snapshot replaced")

    async def large(store: ScopeStore, w: _Watched) -> None:
        entries: list[Entry] = [
            {"id": f"e{i}", "at": i, "seenAt": i + 1, "name": "n", "text": "héllo 🌍\u0000", "data": {"i": i, "nested": [None, {"yes": True}]}, "kind": "passive"}
            for i in range(250)
        ]
        d = _decision("delivered", "s")
        lettered = letter("failed", "s")
        snapshot = cast(
            Snapshot,
            {
                "version": 1,
                "policy": POLICY,
                "reducer": 1,
                "record": {
                    **record("s", 1),
                    "cursors": {"log:a": "🌍\u0000", "mcp:b": None},
                    "seen": [f"seen:{i}" for i in range(1000)],
                    "buffer": {"entries": entries, "dropped": 4, "gap": True, "forcedAt": 12, "endedAt": 13},
                    "batch": {"window": _window("held"), "phase": {"phase": "judging", "attempt": 2, "failures": 1, "leaseUntil": 5000, "offerWait": True}},
                    "outbox": {
                        "decisions": [
                            {"item": {"id": d["id"], "kind": "decision", "decision": d}, "phase": {"phase": "inFlight", "attempt": 1, "failures": 0, "leaseUntil": 4000}}
                        ],
                        "deadLetters": [
                            {
                                "item": {"id": lettered["id"], "kind": "deadLetter", "letter": lettered["body"]},
                                "phase": {"phase": "retrying", "attempt": 2, "failures": 1, "at": 3000},
                            }
                        ],  # type: ignore[typeddict-item]
                    },
                    "recent": [{"id": "prior", "route": "note", "at": 1}],
                    "dueAt": 3000,
                },
            },
        )
        await store.commit([write("s", 0, snapshot=snapshot)])
        tail = [logged("tail:🌍"), logged("tail:\u0000")]
        await store.commit([write("s", 1, inputs=tail)])
        assert_equal(w.keep(await store.load("s")), {"version": 3, "snapshot": snapshot, "tail": tail}, "large round trip")

    async def due_order(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("a", 0, due=3000), write("b", 0, due=1000), write("c", 0, due=2000), write("d", 0, due=9000), write("e", 0, due=1000), write("f", 0, due=None)])
        expected = [{"due": 1000, "scope": "b"}, {"due": 1000, "scope": "e"}, {"due": 2000, "scope": "c"}, {"due": 3000, "scope": "a"}]
        assert_equal(w.keep(await store.due(3000, 10)), expected, "due order")
        assert_equal(await store.due(3000, 10), expected, "read does not claim")
        assert_equal(await store.due(3000, 2), expected[:2], "limit")
        assert_equal(await store.due(3000, 10, {"due": 1000, "scope": "b"}), expected[1:], "inside tie")
        assert_equal(await store.due(3000, 10, {"due": 1000, "scope": "e"}), expected[2:], "after tie")
        assert_equal(await store.due(999, 10), [], "none yet")

    async def due_keep(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s", 0, due=2000)])
        await store.commit([write("s", 1)])
        assert_equal(await store.due(2000, 10), [{"due": 2000, "scope": "s"}], "omitted keeps")
        await store.commit([write("s", 2, due=None)])
        assert_equal(await store.due(2000, 10), [], "null clears")

    async def removes(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s", 0, due=1000)])
        await store.remove("s")
        assert_equal(await store.load("s"), None, "removed")
        assert_equal(await store.due(2000, 10), [], "not due")
        assert_equal(await store.commit([write("s")]), ["ok"], "recreated")

    async def punctuation(store: ScopeStore, w: _Watched) -> None:
        scopes = ["a", "a/b", "a:b", "a%b", "a_b", "ünï", "a/b/c"]
        for i, scope in enumerate(scopes):
            await store.commit([write(scope, 0, due=(i + 1) * 1000)])
        for scope in scopes:
            assert_equal(await store.load(scope), {"version": 1, "snapshot": None, "tail": [logged(f"{scope}:0")]}, scope)
        assert_equal([key["scope"] for key in await store.due(10_000, 10)], scopes, "separate due values")

    async def unchanged(store: ScopeStore, w: _Watched) -> None:
        first: Snapshot = {"version": 2, "record": record("s", 2), "policy": POLICY, "reducer": 1}
        await store.commit([write("s", 0, inputs=[logged("a"), logged("b")], snapshot=first, appends=[append("one")], due=1000), write("t", 0), write("u", 0, due=1000)])
        w.keep(await store.load("s"))
        w.keep(await store.load("t"))
        await store.commit([write("s", 2, appends=[letter("two")])])
        w.keep(await store.load("s"))
        bounds = w.keep(await store.log.bounds())
        for query in ({}, {"scope": "s"}, {"scopePrefix": "s"}, {"kinds": ["deadLetter"]}):
            w.keep(await _read(store.log, **query))
        w.keep(await _read(store.log, after=bounds["first"]))
        w.keep(await _read(store.log, after=bounds["last"]))
        w.keep(await _read(store.log, routes=["interrupt"]))
        w.keep(await _read(store.log, sinceAt=0))
        w.keep(await _read(store.log, after=bounds["first"], scope="s"))
        page = w.keep(await _read(store.log, limit=1))
        [first_due] = w.keep(await store.due(1000, 1))
        [second_due] = w.keep(await store.due(1000, 1, first_due))
        w.keep(await store.due(1000, 1, second_due))
        w.keep(await store.log.find("decision", "one"))
        w.keep(await store.due(1000, 10))
        outcomes = await store.commit([write("s", 3, inputs=[logged("c")], appends=[append("one", "s", "queue", 2000), letter("two", "s", "sink", 2000)], due=1500), write("t", 1)])
        assert_equal(outcomes, ["ok", "ok"], "duplicate appends commit")
        assert_equal(await store.commit([write("s", 4, inputs=[logged("d")], appends=[append("three")], due=2000)]), ["ok"], "later commit")
        w.keep(await store.log.find("decision", "three"))
        w.keep(await _read(store.log, after=page["checked"], limit=1))
        w.keep(await store.log.find("deadLetter", "two"))
        w.keep(await store.log.bounds())
        w.keep(await store.due(2000, 10))
        await store.log.trim({"maxRows": 1})
        snapshot: Snapshot = {"version": 6, "record": record("s", 6), "policy": POLICY, "reducer": 1}
        outcome = await store.commit([write("s", 5, snapshot=snapshot, appends=[append("four"), letter("five"), append("one", "s", "note", 3000)], due=None)])
        assert_equal(outcome, ["ok"], "snapshot commit")
        w.keep(await _read(store.log))
        w.keep(await store.log.find("decision", "one"))
        w.keep(await store.log.bounds())
        w.keep(await store.due(3000, 10))
        w.keep(await store.load("s"))
        await store.log.trim({"olderThanMs": 1})
        await store.remove("s")
        recreated: Snapshot = {"version": 1, "record": record("s", 1), "policy": POLICY, "reducer": 2}
        assert_equal(await store.commit([write("s", 0, snapshot=recreated, appends=[append("six")])]), ["ok"], "re-created")
        await store.remove("t")
        w.check()

    return _cases(
        make,
        [
            ("load of a missing scope is null", missing),
            ("a commit at version 0 creates the scope and load returns its inputs in order", created),
            ("the stored version counts committed inputs", counts),
            ("a write at a stale version conflicts and changes nothing", stale),
            ("a write at version 0 on an existing scope conflicts", twice),
            ("N concurrent commits at one version give exactly one ok", contenders),
            ("a conflicting write in a group leaves the other writes committed, with outcomes in order", grouped),
            ("a group naming one scope twice rejects and applies nothing", duplicate_scope),
            ("a snapshot replaces the stored one and trims the tail", snapshots),
            ("a snapshot and its tail round-trip with unicode, a NUL, a 250-entry window and a 1,000-id ring", large),
            ("due returns scopes due by now, ascending by due then scope, paged after a key", due_order),
            ("an omitted due keeps the stored value and a null due clears it", due_keep),
            ("remove forgets the scope, its due value and its version", removes),
            ("scopes that are prefixes of each other or hold punctuation stay separate", punctuation),
            ("what a read returned is unchanged by later commits, trims and removes", unchanged),
        ],
    )


async def _seed(store: ScopeStore, rows: list[DecisionAppend]) -> None:
    for row in rows:
        loaded = await store.load(row["scope"])
        version = loaded["version"] if loaded is not None else 0
        assert_equal(await store.commit([write(row["scope"], version, appends=[row])]), ["ok"], "seed")


def _ids(rows: Sequence[DecisionRow]) -> list[str]:
    return [row["id"] for row in rows]


def _without_position(row: DecisionRow) -> dict[str, object]:
    return {key: value for key, value in row.items() if key != "position"}


async def _read(log: DecisionLog, **query: Any) -> DecisionPage:
    return await log.read(cast(DecisionQuery, {"after": None, "limit": 10, **query}))


def decision_log_conformance(make: Make, *, concurrent: Callable[[], Awaitable[tuple[ScopeStore, ScopeStore]]] | None = None) -> list[Case]:
    """Every guarantee in DecisionLog's docstrings. With ``concurrent``, also that a reader never skips a row while two writers commit."""

    async def empty(store: ScopeStore, w: _Watched) -> None:
        assert_equal(w.keep(await store.log.bounds()), {"first": None, "last": None}, "empty bounds")
        assert_equal(await _read(store.log), {"rows": [], "checked": None, "more": False}, "empty page")

    async def committed(store: ScopeStore, w: _Watched) -> None:
        await store.commit([write("s", 0, appends=[append("committed")])])
        assert_equal(_ids((await _read(store.log))["rows"]), ["committed"], "committed row")
        assert_equal(await store.commit([write("s", 0, appends=[append("conflict")])]), ["conflict"], "conflict")
        await _rejects(lambda: store.commit([write("a", 0, appends=[append("rejected", "a")]), write("a")]))
        assert_equal(await store.log.find("decision", "conflict"), None, "no conflict row")
        assert_equal(await store.log.find("decision", "rejected"), None, "no rejected row")
        assert_equal(_ids((await _read(store.log))["rows"]), ["committed"], "only committed")

    async def ordered(store: ScopeStore, w: _Watched) -> None:
        appends = [append("one", "a"), append("two", "b"), letter("three", "b"), append("four", "a")]
        await store.commit([write("a", 0, appends=appends[:1])])
        await store.commit([write("b", 0, appends=appends[1:3])])
        await store.commit([write("a", 1, appends=appends[3:])])
        rows = w.keep((await _read(store.log))["rows"])
        assert_equal([_without_position(row) for row in rows], appends, "append and commit order")
        for i in range(1, len(rows)):
            assert_ok(store.log.compare(rows[i - 1]["position"], rows[i]["position"]) < 0, "strictly increasing")
        assert_equal(await store.log.bounds(), {"first": rows[0]["position"], "last": rows[3]["position"]}, "bounds")

    async def paged(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [append(f"{i + 1}") for i in range(5)])
        rows = (await _read(store.log))["rows"]
        page = await store.log.read({"after": rows[0]["position"], "limit": 2})
        assert_equal(page, {"rows": rows[1:3], "checked": rows[2]["position"], "more": True}, "middle page")
        assert_equal(await store.log.read({"after": page["checked"], "limit": 2}), {"rows": rows[3:], "checked": rows[4]["position"], "more": False}, "last page")
        assert_equal(await store.log.read({"after": rows[4]["position"], "limit": 2}), {"rows": [], "checked": rows[4]["position"], "more": False}, "head never goes backwards")

    async def checked(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [*(letter(f"letter:{i}") for i in range(6)), append("decision")])
        rows = (await _read(store.log))["rows"]
        assert_equal(await _read(store.log, routes=["queue"]), {"rows": [], "checked": rows[6]["position"], "more": False}, "exhausted without matches")
        first = await _read(store.log, kinds=["decision"], limit=1, scanLimit=4)
        assert_equal(first, {"rows": [], "checked": rows[3]["position"], "more": True}, "scan limit")
        assert_equal(
            await _read(store.log, after=first["checked"], kinds=["decision"], limit=2, scanLimit=4),
            {"rows": rows[6:], "checked": rows[6]["position"], "more": False},
            "continued scan",
        )

    async def scoped(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [*(append(f"before:{i}", "b") for i in range(50)), append("a", "a"), *(append(f"after:{i}", "b") for i in range(50))])
        row = await store.log.find("decision", "a")
        head = (await store.log.bounds())["last"]
        assert_equal(await _read(store.log, scope="a", limit=2, scanLimit=4), {"rows": [row], "checked": head, "more": False}, "scope index")

    async def prefix_scan(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [*(append(f"other:{i}", "other") for i in range(6)), append("mine", "p/1")])
        rows = (await _read(store.log))["rows"]
        first = await _read(store.log, scopePrefix="p/", limit=1, scanLimit=4)
        assert_equal(first, {"rows": [], "checked": rows[3]["position"], "more": True}, "stops at the scan limit")
        assert_equal(
            await _read(store.log, after=first["checked"], scopePrefix="p/", limit=2, scanLimit=4),
            {"rows": rows[6:], "checked": rows[6]["position"], "more": False},
            "continued scan",
        )

    async def prefix_match(store: ScopeStore, w: _Watched) -> None:
        scopes = ["p/x", "p/y", "px", "p%", "p_", "q"]
        await _seed(store, [append(scope, scope) for scope in scopes])
        for prefix, expected in [("p/", ["p/x", "p/y"]), ("p%", ["p%"]), ("p_", ["p_"]), ("", scopes)]:
            assert_equal(_ids((await _read(store.log, scopePrefix=prefix))["rows"]), expected, f"prefix {prefix}")

    async def filters(store: ScopeStore, w: _Watched) -> None:
        await _seed(
            store,
            [
                append("interrupt", "s", "interrupt", 1000),
                append("queue", "s", "queue", 2000),
                append("note", "s", "note", 3000),
                letter("judge", "s", "judge", 4000),
                letter("sink", "s", "sink", 5000),
            ],
        )

        async def query(**filter: Any) -> list[str]:
            return _ids((await _read(store.log, **filter))["rows"])

        assert_equal(await query(kinds=["decision"]), ["interrupt", "queue", "note"], "decisions")
        assert_equal(await query(kinds=["deadLetter"]), ["judge", "sink"], "letters")
        assert_equal(await query(routes=["interrupt"]), ["interrupt", "sink"], "null route excluded")
        assert_equal(await query(routes=["queue", "note"]), ["queue", "note"], "route list")
        assert_equal(await query(sinceAt=3000), ["note", "judge", "sink"], "inclusive since")
        assert_equal(await query(kinds=["deadLetter"], routes=["interrupt"], sinceAt=5000), ["sink"], "combined")

    async def found(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [append("same"), letter("same", "s", "sink")])
        rows = (await _read(store.log))["rows"]
        assert_equal(w.keep(await store.log.find("decision", "same")), rows[0], "decision")
        assert_equal(await store.log.find("deadLetter", "same"), rows[1], "dead letter")
        assert_equal(await store.log.find("decision", "unknown"), None, "missing")

    async def skipped(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [append("same")])
        before = await _read(store.log)
        assert_equal(await store.commit([write("s", 1, appends=[append("same", "s", "queue", 2000)])]), ["ok"], "duplicate commits")
        loaded = await store.load("s")
        assert_equal(loaded["version"] if loaded else None, 2, "version advanced")
        assert_equal(await _read(store.log), before, "first row retained")

    async def waits(store: ScopeStore, w: _Watched) -> None:
        await _seed(store, [append("first")])
        first = (await store.log.bounds())["last"]
        abort = AbortController()
        try:
            pending = asyncio.ensure_future(store.log.wait(first, 5000, abort.signal))
            start = time.monotonic()
            await _seed(store, [append("second")])
            await asyncio.wait_for(pending, 3.9)
            assert_ok(time.monotonic() - start < 4, "append wakes waiter")
            head = (await store.log.bounds())["last"]
            timeout_at = time.monotonic()
            await asyncio.wait_for(store.log.wait(head, 30, abort.signal), 1.9)
            assert_ok(time.monotonic() - timeout_at >= 0.02, "wait did not return immediately")
            cancelled = AbortController()
            waiting = asyncio.ensure_future(store.log.wait(head, 60_000, cancelled.signal))
            await asyncio.sleep(0)
            cancelled.abort()
            await asyncio.wait_for(waiting, 0.9)
            await asyncio.wait_for(store.log.wait(first, 60_000, abort.signal), 0.9)
        finally:
            abort.abort()

    async def trims(store: ScopeStore, w: _Watched) -> None:
        old = time.time() * 1000 - 10 * 86_400_000
        await _seed(store, [append(f"old:{i}", "s", "interrupt", old + i) for i in range(5)])
        rows = (await _read(store.log))["rows"]
        await store.log.trim({"maxRows": 2})
        assert_equal((await _read(store.log))["rows"], rows[3:], "newest two")
        assert_equal(await store.log.bounds(), {"first": rows[3]["position"], "last": rows[4]["position"]}, "trimmed bounds")
        await store.log.trim({"maxRows": 0})
        assert_equal((await _read(store.log))["rows"], rows[4:], "head survives zero")
        await _seed(store, [append("old:5", "s", "interrupt", old + 5), append("now", "s", "interrupt", time.time() * 1000)])
        await store.log.trim({"olderThanMs": 86_400_000})
        assert_equal(_ids((await _read(store.log))["rows"]), ["now"], "age")
        await store.log.trim({"olderThanMs": 0, "maxRows": 0})
        assert_equal(_ids((await _read(store.log))["rows"]), ["now"], "head always survives")

    async def epoch(store: ScopeStore, w: _Watched) -> None:
        value = await store.log.epoch()
        assert_ok(isinstance(value, str) and len(value) > 0, "nonempty epoch")  # pyright: ignore[reportUnnecessaryIsInstance]
        assert_equal(await store.log.epoch(), value, "stable")
        if not store.durable:
            other = await _fresh(make)
            try:
                assert_ok(await other.log.epoch() != value, "distinct incarnations")
            finally:
                await other.close()

    result = _cases(
        make,
        [
            ("an empty log has no bounds and reads nothing", empty),
            ("rows appear only when their write commits", committed),
            ("rows read back in commit order with strictly increasing positions", ordered),
            ("read returns rows strictly after a position and pages by limit", paged),
            ("checked reaches the head when a read is exhausted and the last row examined when it stops early", checked),
            ("a scoped read examines only that scope", scoped),
            ("a prefix read counts every row it examines toward scanLimit, like kinds and routes", prefix_scan),
            ("a prefix read matches only scopes that start with the prefix", prefix_match),
            ("kinds, routes and sinceAt filter rows", filters),
            ("find returns a row by kind and id", found),
            ("an append whose kind and id are already logged is skipped and its write still commits", skipped),
            ("wait resolves on an append, at the timeout and on abort", waits),
            ("trim keeps the newest rows by count or age and never drops the head", trims),
            ("the epoch is stable on one instance and new for each non-durable instance", epoch),
        ],
    )
    if concurrent is not None:
        together = concurrent

        async def shared_epoch() -> None:
            watched = _Watched()
            first, second = await together()
            a, b = _WatchedStore(first, watched), _WatchedStore(second, watched)
            try:
                assert_ok(a.durable and b.durable, "both durable")
                value = await a.log.epoch()
                assert_equal(await b.log.epoch(), value, "shared epoch")
            finally:
                await asyncio.gather(a.close(), b.close())
            await asyncio.sleep(0)
            watched.check()

        async def never_skips() -> None:
            watched = _Watched()
            first, second = await together()
            a, b = _WatchedStore(first, watched), _WatchedStore(second, watched)
            try:
                commits = asyncio.gather(
                    *(
                        store.commit([write(f"writer:{writer}:{i}", 0, appends=[append(f"writer:{writer}:{i}", f"writer:{writer}:{i}")])])
                        for writer, store in enumerate((a, b))
                        for i in range(100)
                    ),
                    return_exceptions=True,
                )
                seen: set[str] = set()
                after: str | None = None
                previous: str | None = None
                end = time.monotonic() + 15
                while len(seen) < 200 and time.monotonic() < end:
                    page = await a.log.read({"after": after, "limit": 7})
                    for row in page["rows"]:
                        assert_ok(row["id"] not in seen, "no duplicate rows")
                        if previous is not None:
                            assert_ok(a.log.compare(previous, row["position"]) < 0, "positions strictly increase")
                        previous = row["position"]
                        seen.add(row["id"])
                    if after is not None and page["checked"] is not None:
                        assert_ok(a.log.compare(after, page["checked"]) <= 0, "cursor never retreats")
                    after = page["checked"]
                    if not page["more"]:
                        await asyncio.sleep(0.001)
                expected = sorted(f"writer:{writer}:{i}" for writer in (0, 1) for i in range(100))
                assert_equal(sorted(seen), expected, "every committed id exactly once")
                for outcome in await commits:
                    if isinstance(outcome, BaseException):
                        raise outcome
                    assert_equal(outcome, ["ok"], "concurrent write committed")
            finally:
                await asyncio.gather(a.close(), b.close())
            await asyncio.sleep(0)
            watched.check()

        result.append(Case("two stores on one database share a durable epoch", shared_epoch))
        result.append(Case("a reader never skips a row while two writers commit concurrently", never_skips))
    return result


__all__ = ["decision_log_conformance", "scope_store_conformance"]
