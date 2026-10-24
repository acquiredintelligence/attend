"""``attend.testing``: a fake clock, a scripted judge, a recording sink, the crash harness and the crash suites.

Runner-agnostic: each suite returns named async cases that any test runner can run.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from ._json import stable_json
from .attend import Attend, DeadLetterHandler, create_attend
from .clock import drain
from .core.threshold import suggest_threshold
from .core.types import DeadLetter, Decision, Event, JudgeInput, Judgment, Kind, ScopeRecord, Trace
from .errors import PermanentError
from .shell.read import read_record
from .signal import AbortController, AbortSignal
from .store.memory import memory_store
from .store.types import CommitOutcome, DecisionKind, DecisionLog, DecisionPage, DecisionQuery, DecisionRow, DueKey, LogBounds, Retention, ScopeStore, ScopeWrite, StoredScope

__all__ = [
    "Case",
    "CrashError",
    "FakeClock",
    "Harness",
    "RecordingSink",
    "ScriptedJudge",
    "crash_suite",
    "drain",
    "fake_clock",
    "harness",
    "multi_scope_crash_suite",
    "recording_sink",
    "scripted_judge",
    "suggest_threshold",
]


# ─── clock ───────────────────────────────────────────────────────────────────


@dataclass(slots=True, eq=False)
class _Timer:
    at: float
    seq: int
    fn: Callable[[], None]


class FakeClock:
    """Time that moves only when told to. ``advance`` fires every timer due on the way, in order, and lets each one's work finish."""

    def __init__(self, start: float = 0) -> None:
        self._now = start
        self._seq = 0
        self._timers: list[_Timer] = []

    def now(self) -> float:
        return self._now

    def at(self, at: float, fn: Callable[[], None]) -> Callable[[], None]:
        timer = _Timer(at, self._seq, fn)
        self._seq += 1
        self._timers.append(timer)

        def cancel() -> None:
            with contextlib.suppress(ValueError):
                self._timers.remove(timer)

        return cancel

    def _earliest(self) -> _Timer | None:
        best: _Timer | None = None
        for timer in self._timers:
            if best is None or timer.at < best.at or (timer.at == best.at and timer.seq < best.seq):
                best = timer
        return best

    def next_at(self) -> float | None:
        """The earliest armed timer, or None."""
        earliest = self._earliest()
        return earliest.at if earliest is not None else None

    async def set(self, at: float) -> None:
        """Jumps to an instant, firing timers on the way."""
        await drain()
        while (earliest := self._earliest()) is not None and earliest.at <= at:
            self._now = max(self._now, earliest.at)
            self._timers.remove(earliest)
            earliest.fn()
            await drain()
        self._now = max(self._now, at)
        await drain()

    async def advance(self, ms: float) -> None:
        """Moves time forward by ``ms``."""
        await self.set(self._now + ms)


def fake_clock(start: float = 0) -> FakeClock:
    return FakeClock(start)


# ─── judge and sink doubles ──────────────────────────────────────────────────

type ScriptedAnswer = Judgment | Mapping[str, Any]
"""A judgment, ``{"fail": message}`` (with ``"permanent": True``), or ``{"hang": True}``."""


class ScriptedJudge:
    """Answers in order from a script; the last entry repeats. Records every input in ``calls``."""

    def __init__(self, script: Sequence[ScriptedAnswer]) -> None:
        self._script = list(script)
        self.calls: list[JudgeInput] = []

    async def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Judgment:
        if not self._script:
            raise RuntimeError("scripted_judge: empty script")
        answer = self._script[min(len(self.calls), len(self._script) - 1)]
        self.calls.append(input)
        if answer.get("hang"):
            await signal.wait()
            raise Exception("aborted")
        if "fail" in answer:
            message = str(answer["fail"])
            raise PermanentError(message) if answer.get("permanent") else Exception(message)
        return cast(Judgment, dict(answer))


def scripted_judge(script: Sequence[ScriptedAnswer]) -> ScriptedJudge:
    return ScriptedJudge(script)


class RecordingSink:
    """Records ``delivered`` and ``attempts``. ``fail_next(n)`` makes the next ``n`` deliveries raise."""

    def __init__(self) -> None:
        self.delivered: list[Decision] = []
        self.attempts: dict[str, int] = {}
        self._failing = 0
        self._permanent = False

    def fail_next(self, n: int, permanent: bool = False) -> None:
        self._failing = n
        self._permanent = permanent

    async def deliver(self, decision: Decision, signal: AbortSignal, /) -> None:
        self.attempts[decision["id"]] = self.attempts.get(decision["id"], 0) + 1
        if self._failing > 0:
            self._failing -= 1
            raise PermanentError("sink refused") if self._permanent else Exception("sink failed")
        self.delivered.append(decision)


def recording_sink() -> RecordingSink:
    return RecordingSink()


@dataclass(slots=True)
class Case:
    """A named async conformance case."""

    name: str
    run: Callable[[], Awaitable[None]]


class ConformanceError(AssertionError):
    pass


def assert_equal(actual: object, expected: object, message: str) -> None:
    a = stable_json(actual)
    e = stable_json(expected)
    if a != e:
        raise ConformanceError(f"{message}\n  expected: {e}\n  actual:   {a}")


def assert_ok(value: object, message: str) -> None:
    if not value:
        raise ConformanceError(message)


# ─── crash harness ───────────────────────────────────────────────────────────


class CrashError(Exception):
    """Raised by a crashed instance's store, and by the harness once a scheduled crash struck."""

    def __init__(self) -> None:
        super().__init__("attend: crashed")


type CrashPoint = Literal["before", "after"]


class _Guarded(ScopeStore):
    """A view of a store whose every call fails once its instance is dead, and that can crash at a commit."""

    def __init__(
        self,
        inner: ScopeStore,
        guard: Callable[[Callable[[], Awaitable[Any]]], Awaitable[Any]],
        commit: Callable[[Sequence[ScopeWrite]], Awaitable[list[CommitOutcome]]],
        load: Callable[[str], Awaitable[StoredScope | None]] | None = None,
    ) -> None:
        self.durable = inner.durable
        self._inner = inner
        self._guard = guard
        self._commit = commit
        self._loader = load
        self.log = _GuardedLog(inner.log, guard)

    async def load(self, scope: str) -> StoredScope | None:
        if self._loader is not None:
            return await self._loader(scope)
        return cast(StoredScope | None, await self._guard(lambda: self._inner.load(scope)))

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        return await self._commit(writes)

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        return cast(list[DueKey], await self._guard(lambda: self._inner.due(now, limit, after)))

    async def remove(self, scope: str) -> None:
        await self._guard(lambda: self._inner.remove(scope))


class _GuardedLog(DecisionLog):
    def __init__(self, inner: DecisionLog, guard: Callable[[Callable[[], Awaitable[Any]]], Awaitable[Any]]) -> None:
        self._inner = inner
        self._guard = guard

    async def epoch(self) -> str:
        return cast(str, await self._guard(self._inner.epoch))

    async def bounds(self) -> LogBounds:
        return cast(LogBounds, await self._guard(self._inner.bounds))

    async def read(self, query: DecisionQuery) -> DecisionPage:
        return cast(DecisionPage, await self._guard(lambda: self._inner.read(query)))

    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None:
        return cast(DecisionRow | None, await self._guard(lambda: self._inner.find(kind, id)))

    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        await self._guard(lambda: self._inner.wait(after, block_ms, signal))

    async def trim(self, retention: Retention) -> None:
        await self._guard(lambda: self._inner.trim(retention))

    def compare(self, a: str, b: str) -> int:
        return self._inner.compare(a, b)


@dataclass(slots=True, eq=False)
class _Instance:
    attend: Attend
    controller: AbortController
    done: asyncio.Task[None]
    dead: list[bool]


class Harness:
    """Runs attend on one scope with a fake clock over a store it can crash at any commit."""

    def __init__(self, config: Mapping[str, Any], *, scope: str = "scope", store: ScopeStore | None = None, start: float = 0, sweep_ms: float = 1000) -> None:
        self._config = dict(config)
        self.scope = scope
        self.store = store if store is not None else memory_store()
        self.clock = fake_clock(start)
        self._sweep_ms = sweep_ms
        self.dead_letters: list[DeadLetter] = []
        self.traces: list[Trace] = []
        self.commits = 0
        self._pending = 0
        self._schedule: tuple[int, CrashPoint] | None = None
        self.crashed = False
        self._current: _Instance | None = None

    @property
    def attend(self) -> Attend:
        assert self._current is not None
        return self._current.attend

    def _start(self) -> _Instance:
        controller = AbortController()
        dead = [False]
        inner = self.store

        async def guard(work: Callable[[], Awaitable[Any]]) -> Any:
            if dead[0]:
                raise CrashError()
            self._pending += 1
            try:
                return await work()
            finally:
                self._pending -= 1

        def strike(n: int, point: CrashPoint) -> None:
            if self._schedule is None or self._schedule != (n, point):
                return
            self._schedule = None
            self.crashed = True
            dead[0] = True
            controller.abort()
            raise CrashError()

        async def commit(writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
            async def work() -> list[CommitOutcome]:
                self.commits += 1
                n = self.commits
                strike(n, "before")
                result = await inner.commit(writes)
                strike(n, "after")
                return result

            return cast(list[CommitOutcome], await guard(work))

        config = self._config
        user_dead_letter = cast(DeadLetterHandler | Literal["log"] | None, config.get("dead_letter"))
        user_observe = cast(Callable[[Trace], None] | None, config.get("observe"))

        async def dead_letter(letter: DeadLetter, signal: AbortSignal) -> None:
            if callable(user_dead_letter):
                result = user_dead_letter(letter, signal)
                if result is not None:
                    await result
            self.dead_letters.append(letter)

        def observe(trace: Trace) -> None:
            self.traces.append(trace)
            if user_observe is not None:
                user_observe(trace)

        options: dict[str, Any] = {**config, "store": _Guarded(inner, guard, commit), "clock": self.clock, "dead_letter": dead_letter, "observe": observe}
        attend = create_attend(**options)
        done = asyncio.ensure_future(attend.run(signal=controller.signal, sweep_ms=self._sweep_ms))
        return _Instance(attend, controller, done, dead)

    async def start(self) -> Harness:
        self._current = self._start()
        await self.idle()
        return self

    async def idle(self) -> None:
        """Returns once no store call is in flight. Work that waits on real time is invisible to it."""
        while True:
            await drain()
            if self._pending == 0:
                return
            await asyncio.sleep(0.001)

    def _check(self) -> None:
        if self.crashed:
            raise CrashError()

    async def push(self, events: Sequence[Mapping[str, Any]]) -> None:
        """Pushes into the scope and waits until the runner has nothing left to do. Raises ``CrashError`` if a crash struck."""
        await self.attend.push(self.scope, list(events))
        await self.idle()
        self._check()

    async def advance(self, ms: float) -> None:
        """Advances the clock, firing timers one at a time and going idle after each. Raises ``CrashError`` at the instant a crash struck."""
        target = self.clock.now() + ms
        await self.idle()
        self._check()
        while (next_at := self.clock.next_at()) is not None and next_at <= target:
            await self.clock.set(next_at)
            await self.idle()
            self._check()
        await self.clock.set(target)
        await self.idle()
        self._check()

    async def flush(self) -> None:
        await self.attend.flush(self.scope)
        await self.advance(0)

    def crash(self) -> None:
        """Kills the runner now. The store keeps whatever was committed."""
        assert self._current is not None
        self.crashed = True
        self._current.dead[0] = True
        self._current.controller.abort()

    def crash_at(self, commit: int, point: CrashPoint) -> None:
        """Schedules a crash at the nth commit from now."""
        self._schedule = (self.commits + commit, point)

    async def restart(self) -> None:
        """Starts a fresh runner over the same store, as a restarted process would, and lets it recover."""
        assert self._current is not None
        self.crash()
        await asyncio.gather(self._current.done, return_exceptions=True)
        self.crashed = False
        self._current = self._start()
        await self.idle()

    async def stop(self) -> None:
        assert self._current is not None
        self._current.controller.abort()
        await self._current.done

    async def inspect(self) -> ScopeRecord | None:
        sink = self._config.get("sink")
        dead_letter = self._config.get("dead_letter", "log")
        return await read_record(self.store, self.scope, "log" if sink == "log" else "lane", "log" if dead_letter == "log" else "lane")


async def harness(config: Mapping[str, Any], *, scope: str = "scope", store: ScopeStore | None = None, start: float = 0, sweep_ms: float = 1000) -> Harness:
    """A started harness. ``config`` holds ``create_attend`` keywords other than ``store`` and ``clock``."""
    return await Harness(config, scope=scope, store=store, start=start, sweep_ms=sweep_ms).start()


# ─── crash suite ─────────────────────────────────────────────────────────────


def _session_kind(event: Event) -> Kind:
    return {"close": "closing", "say": "flush", "look": "passive"}.get(event["name"], "consequence")  # type: ignore[return-value]


SESSION_POLICY: dict[str, Any] = {"judge_timeout_ms": 1000, "delivery_timeout_ms": 2000, "retry_backoff_ms": [1000, 1000, 1000]}
"""Short leases and backoffs, so a stranded call recovers well inside the gaps the session leaves between acts."""

SESSION_DECISIONS = ["cf8923dfae84f930", "cf8927dfae84fffc"]
SESSION_LETTERS = ["cf892adfae850515", "eaf0348d05594776"]
MULTI_DECISIONS = [
    "4b0d164deed6032f",
    "57926a9b506cd3a1",
    "57926e9b506cda6d",
    "6be88691d9775406",
    "6be88f91d9776351",
    "6be89391d9776a1d",
    "d29b92ea485105d3",
    "d29b96ea48510c9f",
    "d29b9dea48511884",
]
MULTI_LETTERS = ["51a3be624f9cf887", "57926b9b506cd554", "6be89091d9776504", "ba2ddaa47740a48a", "c43155c9c7ae9dd8", "d29b93ea48510786"]


@dataclass(slots=True)
class _SessionResult:
    decisions: list[str]
    letters: list[str]
    commits: int


async def _session(store: ScopeStore, crash: tuple[int, CrashPoint] | None) -> _SessionResult:
    """A closing act delivered, a passive act the judge waits on and then merges with a closing act whose delivery exhausts the sink,
    a flush whose first judge call fails, an act the judge refuses for good, and an act the judge skips."""
    delivered: set[str] = set()
    flaked: set[str] = set()

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        ids = [event["id"] for event in input["window"]["events"]]
        if "f" in ids:
            raise PermanentError("refused")
        if "e" in ids and input["window"]["id"] not in flaked:
            flaked.add(input["window"]["id"])
            raise Exception("flaky")
        if "b" in ids and "d" not in ids and any(route["route"] == "wait" for route in input["routes"]):
            return {"route": "wait"}
        if "d" in ids:
            return {"route": "interrupt"}
        if "e" in ids:
            return {"route": "note"}
        if "g" in ids:
            return {"route": "skip"}
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if decision["route"] == "interrupt":
            raise Exception("sink down")
        delivered.add(decision["id"])

    h = await harness({"judge": judge, "sink": sink, "kind": _session_kind, "settle": SESSION_POLICY}, store=store, sweep_ms=10_000)
    if crash is not None:
        h.crash_at(*crash)

    async def survive(op: Callable[[], Awaitable[None]]) -> None:
        try:
            await op()
        except CrashError:
            await h.restart()
            await op()

    async def push(id: str, name: str) -> None:
        await survive(lambda: h.push([{"id": id, "at": 0, "name": name, "data": {}, "text": f"{name} {id}"}]))

    async def advance(ms: float) -> None:
        target = h.clock.now() + ms
        await survive(lambda: h.advance(target - h.clock.now()))

    await push("a", "close")
    await advance(600)
    await advance(8000)
    await push("b", "look")
    await advance(4100)
    await push("d", "close")
    await advance(600)
    await advance(25_000)
    await push("e", "say")
    await advance(1500)
    await advance(8000)
    await push("f", "close")
    await advance(600)
    await advance(8000)
    await push("g", "close")
    await advance(600)
    await advance(60_000)
    await h.stop()
    await store.close()
    return _SessionResult(sorted(delivered), sorted({letter["id"] for letter in h.dead_letters}), h.commits)


def crash_suite(make: Callable[[], Awaitable[ScopeStore] | ScopeStore]) -> list[Case]:
    """The scripted session runs once cleanly, then once per commit boundary with a crash there, restarting over the same store.

    Every run must produce the same delivered decision ids and dead-letter ids.
    """

    async def fresh() -> ScopeStore:
        made = make()
        return await made if isinstance(made, Awaitable) else made

    def survives(point: CrashPoint) -> Callable[[], Awaitable[None]]:
        async def run() -> None:
            clean = await _session(await fresh(), None)
            for commit in range(1, clean.commits + 1):
                crashed = await _session(await fresh(), (commit, point))
                assert_equal(crashed.decisions, clean.decisions, f"decisions after a crash {point} commit {commit}")
                assert_equal(crashed.letters, clean.letters, f"dead letters after a crash {point} commit {commit}")

        return run

    async def clean() -> None:
        result = await _session(await fresh(), None)
        assert_equal(result.decisions, SESSION_DECISIONS, "decisions")
        assert_equal(result.letters, SESSION_LETTERS, "dead letters")

    return [
        Case("the scripted session delivers two decisions and two dead letters", clean),
        Case("the same ids survive a crash before every commit", survives("before")),
        Case("the same ids survive a crash after every commit", survives("after")),
    ]


@dataclass(slots=True)
class _MultiGroup:
    scopes: list[str]
    outcomes: list[CommitOutcome]
    snapshot: bool
    effect: bool


@dataclass(slots=True)
class _MultiResult:
    decisions: list[str]
    letters: list[str]
    groups: list[_MultiGroup]
    commits: int
    struck: tuple[bool, bool] | None


@dataclass(slots=True, eq=False)
class _MultiInstance:
    attend: Attend
    controller: AbortController
    done: asyncio.Task[None]
    reloading: set[str]
    dead: list[bool]


async def _multi_session(store: ScopeStore, crash: tuple[int, CrashPoint] | None) -> _MultiResult:
    """Three scopes on two runners over one store, with the crash session's acts in each, so groups conflict and recover."""
    clock = fake_clock()
    groups: list[_MultiGroup] = []
    delivered: set[str] = set()
    letters: set[str] = set()
    flaked: set[str] = set()
    scopes = ["m/x", "m/y", "m/z"]
    counters = {"commits": 0, "pending": 0}
    state: dict[str, Any] = {"victim": None, "struck": None}
    instances: list[_MultiInstance] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        ids = {event["id"] for event in input["window"]["events"]}

        def has(id: str) -> bool:
            return f"{input['scope']}/{id}" in ids

        if has("f"):
            raise PermanentError("refused")
        if has("e") and input["window"]["id"] not in flaked:
            flaked.add(input["window"]["id"])
            raise Exception("flaky")
        if has("b") and not has("d") and any(route["route"] == "wait" for route in input["routes"]):
            return {"route": "wait"}
        return {"route": "interrupt" if has("d") else "note" if has("e") else "skip" if has("g") else "queue"}

    def start(index: int) -> _MultiInstance:
        controller = AbortController()
        dead = [False]
        # After a conflict the shell backs off in real time, then reloads the scope: no store call is in flight meanwhile.
        reloading: set[str] = set()

        async def guard(work: Callable[[], Awaitable[Any]]) -> Any:
            if dead[0]:
                raise CrashError()
            counters["pending"] += 1
            try:
                return await work()
            finally:
                counters["pending"] -= 1

        async def load(scope: str) -> StoredScope | None:
            async def work() -> StoredScope | None:
                reloading.discard(scope)
                return await store.load(scope)

            return cast(StoredScope | None, await guard(work))

        async def commit(writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
            async def work() -> list[CommitOutcome]:
                counters["commits"] += 1
                n = counters["commits"]
                snapshot = any(write["snapshot"] is not None for write in writes)
                effect = any(logged["input"]["kind"] in ("tick", "judged", "judgeFailed") for write in writes for logged in write["inputs"])

                def strike(point: CrashPoint) -> None:
                    if state["struck"] is not None or crash is None or crash != (n, point):
                        return
                    state["struck"] = (snapshot, effect)
                    state["victim"] = index
                    dead[0] = True
                    reloading.clear()
                    controller.abort()
                    raise CrashError()

                strike("before")
                outcomes = await store.commit(writes)
                for write, outcome in zip(writes, outcomes, strict=True):
                    if outcome == "conflict":
                        reloading.add(write["scope"])
                groups.append(_MultiGroup([write["scope"] for write in writes], list(outcomes), snapshot, effect))
                strike("after")
                return outcomes

            return cast(list[CommitOutcome], await guard(work))

        def observe(trace: Trace) -> None:
            if trace["kind"] == "abandoned":
                reloading.discard(trace["scope"])

        async def sink(decision: Decision, signal: AbortSignal) -> None:
            if decision["route"] == "interrupt":
                raise Exception("sink down")
            delivered.add(decision["id"])

        def dead_letter(letter: DeadLetter, signal: AbortSignal) -> None:
            letters.add(letter["id"])

        attend = create_attend(
            store=_Guarded(store, guard, commit, load),
            clock=clock,
            kind=_session_kind,
            judge=judge,
            settle=SESSION_POLICY,
            observe=observe,
            sink=sink,
            dead_letter=dead_letter,
        )
        done = asyncio.ensure_future(attend.run(signal=controller.signal, sweep_ms=10_000))
        return _MultiInstance(attend, controller, done, reloading, dead)

    instances.extend([start(0), start(1)])

    async def idle() -> None:
        while True:
            await drain()
            if counters["pending"] == 0 and not any(instance.reloading for instance in instances):
                return
            await asyncio.sleep(0.001)

    def check() -> None:
        if state["victim"] is not None:
            raise CrashError()

    async def survive(work: Callable[[], Awaitable[None]]) -> None:
        try:
            await work()
            await idle()
            check()
        except CrashError:
            if state["victim"] is None:
                raise
            index = cast(int, state["victim"])
            await asyncio.gather(instances[index].done, return_exceptions=True)
            state["victim"] = None
            instances[index] = start(index)
            await idle()
            await work()
            await idle()
            check()

    async def push(index: int, id: str, name: str, targets: list[str] = scopes) -> None:
        async def work() -> None:
            await asyncio.gather(*(instances[index].attend.push(scope, [{"id": f"{scope}/{id}", "at": 0, "name": name, "data": {}, "text": f"{name} {id}"}]) for scope in targets))

        await survive(work)

    async def advance(ms: float) -> None:
        target = clock.now() + ms

        async def work() -> None:
            while (next_at := clock.next_at()) is not None and next_at <= target:
                await clock.set(next_at)
                await idle()
                check()
            await clock.set(target)

        await survive(work)

    try:
        await idle()
        await push(0, "a", "close")
        await advance(600)
        await advance(8000)
        await push(1, "c", "out", ["m/x"])
        await push(0, "h", "close")
        await advance(600)
        await advance(8000)
        await push(1, "b", "look")
        await advance(4100)
        await push(0, "d", "close")
        await advance(600)
        await advance(25_000)
        await push(1, "e", "say")
        await advance(1500)
        await advance(8000)
        await push(0, "f", "close")
        await advance(600)
        await advance(8000)
        await push(1, "g", "close")
        await advance(600)
        await advance(60_000)
        return _MultiResult(sorted(delivered), sorted(letters), groups, counters["commits"], cast(tuple[bool, bool] | None, state["struck"]))
    finally:
        for instance in instances:
            instance.controller.abort()
        await asyncio.gather(*(instance.done for instance in instances), return_exceptions=True)
        await store.close()


def multi_scope_crash_suite(make: Callable[[], Awaitable[ScopeStore] | ScopeStore]) -> list[Case]:
    """Cross-scope recovery, including mixed conflicts, snapshots and effect-start boundaries."""

    async def fresh() -> ScopeStore:
        made = make()
        return await made if isinstance(made, Awaitable) else made

    def verify(result: _MultiResult) -> None:
        assert_equal(result.decisions, MULTI_DECISIONS, "multi-scope decisions")
        assert_equal(result.letters, MULTI_LETTERS, "multi-scope dead letters")

    async def clean() -> None:
        result = await _multi_session(await fresh(), None)
        verify(result)
        assert_ok(any(len(group.scopes) == 3 for group in result.groups), "a group of three")
        assert_ok(any("conflict" in group.outcomes and "ok" in group.outcomes for group in result.groups), "mixed outcomes")
        assert_ok(any(group.snapshot for group in result.groups), "a snapshot commit")

    def survives(point: CrashPoint) -> Callable[[], Awaitable[None]]:
        async def run() -> None:
            clean = await _multi_session(await fresh(), None)
            verify(clean)
            snapshots = 0
            effects = 0
            for commit in range(1, clean.commits + 1):
                result = await _multi_session(await fresh(), (commit, point))
                verify(result)
                if result.struck is not None and result.struck[0]:
                    snapshots += 1
                if result.struck is not None and result.struck[1]:
                    effects += 1
            assert_ok(snapshots > 0, "crashed during a snapshot")
            assert_ok(effects > 0, "crashed between commit and effect start")

        return run

    return [
        Case("three scopes group commits and isolate a conflict beside successful writes", clean),
        Case("three scopes keep their ids after a crash before every group", survives("before")),
        Case("three scopes keep their ids after a crash after every group", survives("after")),
    ]
