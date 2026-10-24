"""Owns each scope's mutable state and commits immutable groups across scopes.

A port of ``src/shell/scope.ts``. Inputs applied before a write starts share a commit, writes for
different scopes share one sorted group, a conflict reloads the scope and re-applies its raw
inputs after a jittered backoff, and effects run only once their write is durable.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .._json import stable_json, stringify, utf8_length, utf16_length
from ..clock import Clock
from ..core.reducer import REDUCER_VERSION, apply, next_due
from ..core.state import initial_state, quiescent, to_record
from ..core.types import ApplyConfig, ApplyOutput, DecisionAppend, Effect, EventsInput, Input, LoggedInput, ScopeState, Trace
from ..core.window import estimate_rendered_bytes
from ..errors import BackpressureError, ConflictError, OutboxFullError
from ..signal import AbortSignal
from ..store.types import CommitOutcome, ScopeStore, ScopeWrite, SerializedWrite
from .copy import record_bytes
from .read import replay
from .scheduler import Scheduler

_MAX_CONFLICTS = 8


def _consume(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()


@dataclass(slots=True, eq=False)
class RawInput:
    """An input and its callers, retained until commit or abandonment."""

    input: Input
    signal: AbortSignal | None
    reject_hook: Callable[[BaseException], None] | None
    done: asyncio.Future[None]
    count: int = 1
    conflicts: int = 0

    def resolve(self) -> None:
        if not self.done.done():
            self.done.set_result(None)

    def reject(self, error: BaseException) -> None:
        if not self.done.done():
            self.done.set_exception(error)


@dataclass(slots=True, eq=False)
class StagedWrite:
    """An open write and the raw inputs and effects whose fate it determines."""

    write: ScopeWrite
    state: ScopeState
    serialized: SerializedWrite | None
    raw: list[RawInput] = field(default_factory=lambda: list[RawInput]())
    effects: list[Effect] = field(default_factory=lambda: list[Effect]())
    bytes: int = 0
    snapshot_bytes: int = 0


@dataclass(slots=True, eq=False)
class ScopeSlot:
    """The owned state, queued callers and persistence accounting for one scope."""

    scope: str
    last_touch: float
    state: ScopeState | None = None
    loading: Awaitable[None] | None = None
    staged: StagedWrite | None = None
    inflight: StagedWrite | None = None
    queued: list[RawInput] = field(default_factory=lambda: list[RawInput]())
    tail_bytes: int = 0
    tail_count: int = 0
    snapshot_bytes: int = 0
    snapshot_needed: bool = True
    force_write: bool = False
    stored_due: float | None = None
    has_stored_due: bool = False


def _input_kind(input: Input) -> str:
    return input["kind"]


def _ceil_second(at: float) -> float:
    """``Math.ceil(at / 1000) * 1000``, which keeps Infinity and NaN as they are."""
    return math.ceil(at / 1000) * 1000 if math.isfinite(at) else at


def _follow(done: asyncio.Future[None]) -> asyncio.Future[None]:
    """A future of the caller's own that settles with ``done``, so cancelling it never cancels the shared input."""
    mine: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    mine.add_done_callback(_consume)

    def settle(source: asyncio.Future[None]) -> None:
        if mine.done():
            return
        if source.cancelled():
            mine.cancel()
        elif (error := source.exception()) is not None:
            mine.set_exception(error)
        else:
            mine.set_result(None)

    done.add_done_callback(settle)
    return mine


class Scopes:
    def __init__(
        self,
        *,
        store: ScopeStore,
        clock: Clock,
        config: ApplyConfig,
        max_queued_inputs: int,
        max_lag_ms: float | None,
        idle_ms: float,
        snapshot: str,
        scheduler: Scheduler,
        running: Callable[[], bool],
        execute: Callable[[str, Effect], None],
        observe: Callable[[Trace], None],
    ) -> None:
        self._store = store
        self._clock = clock
        self._config = config
        self._max_queued = max_queued_inputs
        self._max_lag_ms = max_lag_ms
        self._idle_ms = idle_ms
        self._snapshot = snapshot
        self._scheduler = scheduler
        self._running = running
        self._execute = execute
        self._observe = observe
        self._slots: dict[str, ScopeSlot] = {}
        self._staged: dict[ScopeSlot, None] = {}
        self._pending: set[asyncio.Future[None]] = set()
        self._queued = 0
        self._committing = False
        self._scheduled = False
        self._tasks: set[asyncio.Task[None]] = set()

    def _spawn(self, work: Awaitable[None]) -> None:
        task = asyncio.ensure_future(work)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _slot_for(self, scope: str) -> ScopeSlot:
        slot = self._slots.get(scope)
        if slot is None:
            slot = ScopeSlot(scope, self._clock.now())
            self._slots[scope] = slot
        return slot

    def _reject(self, raw: RawInput, error: BaseException) -> None:
        if raw.reject_hook is not None:
            raw.reject_hook(error)
        raw.reject(error)

    def _arm(self, slot: ScopeSlot) -> None:
        if self._running():
            self._scheduler.arm(slot.scope, slot.state.due_at if slot.state is not None else None)

    def _load(self, slot: ScopeSlot) -> Awaitable[None]:
        if slot.loading is not None:
            return slot.loading
        if slot.state is not None:
            done = asyncio.get_running_loop().create_future()
            done.set_result(None)
            return done
        task = asyncio.ensure_future(self._load_now(slot))
        slot.loading = task
        return task

    async def _load_now(self, slot: ScopeSlot) -> None:
        config = self._config
        try:
            stored = await self._store.load(slot.scope)
            slot.state = initial_state(slot.scope) if stored is None else replay(slot.scope, stored, config.decisions, config.dead_letters)
            slot.tail_count = len(stored["tail"]) if stored is not None else 0
            slot.tail_bytes = sum(self._input_bytes(logged) for logged in stored["tail"]) if stored is not None else 0
            snapshot = stored["snapshot"] if stored is not None else None
            slot.snapshot_bytes = self._bytes(snapshot["record"]) if snapshot is not None else 0
            slot.snapshot_needed = snapshot is None
            slot.force_write = False
            slot.has_stored_due = False
            if snapshot is not None:
                upgraded = snapshot["reducer"] != REDUCER_VERSION
                if upgraded or stable_json(snapshot["policy"]) != stable_json(config.policy):
                    slot.state.due_at = next_due(config.policy, slot.state)
                    slot.snapshot_needed = True
                    slot.force_write = True
                if upgraded:
                    self._observe({"kind": "replayedAcrossUpgrade", "scope": slot.scope, "from": snapshot["reducer"], "to": REDUCER_VERSION})
            self._arm(slot)
        except Exception as error:
            slot.state = None
            raws, slot.queued = slot.queued, []
            for raw in raws:
                self._queued -= raw.count
                self._reject(raw, error)
        finally:
            # Also on cancellation, so inputs queued behind the load are applied or loaded again.
            slot.loading = None
            self._pump(slot)

    def _bytes(self, value: object) -> int:
        return utf8_length(stringify(value)) if self._store.durable else record_bytes(value)

    def _input_bytes(self, logged: LoggedInput) -> int:
        if self._store.durable:
            return utf8_length(stringify(logged))
        input = logged["input"]
        if input["kind"] != "events":
            return 64
        return 64 + sum(estimate_rendered_bytes(event) + utf16_length(event["id"]) for event in input["events"])

    def _request_flush(self) -> None:
        if self._committing or self._scheduled or not self._staged:
            return
        self._scheduled = True

        def run() -> None:
            self._scheduled = False
            self._flush()

        asyncio.get_running_loop().call_soon(run)

    def _stage(self, slot: ScopeSlot, state: ScopeState) -> StagedWrite:
        if slot.staged is not None:
            return slot.staged
        serialized: SerializedWrite | None = {"inputs": [], "snapshot": None, "appends": []} if self._store.durable else None
        write: ScopeWrite = {"scope": slot.scope, "expectedVersion": state.version, "inputs": [], "snapshot": None, "appends": []}
        result = StagedWrite(write, state, serialized)
        slot.staged = result
        self._staged[slot] = None
        return result

    def _transition(self, slot: ScopeSlot, state: ScopeState, input: Input, now: float) -> StagedWrite | None:
        out = ApplyOutput()
        logged: LoggedInput = {"input": input, "now": now}
        apply(self._config, state, logged, out)
        if out.changed or slot.force_write:
            current = self._stage(slot, state)
            current.write["inputs"].append(logged)
            state.version += 1
            if current.serialized is not None:
                text = stringify(logged)
                current.serialized["inputs"].append(text)
                current.bytes += utf8_length(text)
                current.serialized["appends"].extend(stringify(append["body"]) for append in out.appends)
            else:
                current.bytes += self._input_bytes(logged)
            appends: list[DecisionAppend] = current.write["appends"]
            appends.extend(out.appends)
            current.effects.extend(out.effects)
            slot.force_write = False
            return current
        if slot.staged is not None:
            slot.staged.effects.extend(out.effects)
        else:
            for effect in out.effects:
                self._execute(slot.scope, effect)
        return slot.staged

    def _apply_raw(self, slot: ScopeSlot, raw: RawInput) -> None:
        state = slot.state
        assert state is not None
        slot.last_touch = self._clock.now()
        try:
            if raw.signal is not None:
                raw.signal.throw_if_aborted()
            input = raw.input
            policy = self._config.policy
            if input["kind"] == "events" and input["events"]:
                if len(state.decisions) >= policy["maxOutbox"]:
                    raise OutboxFullError(slot.scope, "decisions", len(state.decisions))
                if len(state.dead_letters) >= policy["maxDeadLetters"]:
                    raise OutboxFullError(slot.scope, "deadLetters", len(state.dead_letters))
            now = self._clock.now()
            if self._running() and input["kind"] in ("events", "flush", "end") and state.due_at is not None and state.due_at < now:
                self._transition(slot, state, {"kind": "tick"}, now)
            current = self._transition(slot, state, input, now)
            if current is not None:
                current.raw.append(raw)
            else:
                raw.resolve()
            self._arm(slot)
            self._request_flush()
        except Exception as error:
            self._reject(raw, error)
            if not isinstance(error, OutboxFullError) and not (raw.signal is not None and raw.signal.aborted):
                slot.state = None
            self._request_flush()

    def _pump(self, slot: ScopeSlot) -> None:
        if slot.inflight is not None or slot.loading is not None:
            return
        if slot.state is None:
            if slot.queued and slot.staged is None:
                self._load(slot)
            return
        while slot.queued and slot.state is not None and slot.inflight is None and slot.loading is None:  # pyright: ignore[reportUnnecessaryComparison]
            raw = slot.queued.pop(0)
            self._queued -= raw.count
            self._apply_raw(slot, raw)

    def submit(
        self,
        scope: str,
        input: Input,
        external: bool = False,
        signal: AbortSignal | None = None,
        reject_hook: Callable[[BaseException], None] | None = None,
    ) -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        if external:
            lag = self._scheduler.lag_ms()
            over_queue = self._queued >= self._max_queued
            over_lag = self._max_lag_ms is not None and lag > self._max_lag_ms
            if over_queue or over_lag:
                error = BackpressureError(math.ceil(lag) if over_lag else 100, self._queued)
                self._observe({"kind": "backpressure", "queued": self._queued, "lagMs": lag})
                if reject_hook is not None:
                    reject_hook(error)
                failed: asyncio.Future[None] = loop.create_future()
                failed.set_exception(error)
                failed.add_done_callback(_consume)
                return failed
        slot = self._slot_for(scope)
        last = slot.queued[-1] if slot.queued else None
        if last is not None and input["kind"] == "events" and last.input["kind"] == "events" and last.input.get("source") == input.get("source") and last.signal is signal:
            merged: EventsInput = last.input
            merged["events"].extend(input["events"])
            if "cursor" in input:
                merged["cursor"] = input["cursor"]
            if input.get("truncated"):
                merged["truncated"] = True
            last.count += 1
            self._queued += 1
            return _follow(last.done)
        if last is not None and input["kind"] == "tick" and last.input["kind"] == "tick":
            return _follow(last.done)
        done: asyncio.Future[None] = loop.create_future()
        done.add_done_callback(_consume)
        raw = RawInput(input, signal, reject_hook, done)
        self._pending.add(done)
        done.add_done_callback(self._pending.discard)
        slot.queued.append(raw)
        self._queued += 1
        self._pump(slot)
        return _follow(done)

    def _prepare(self, slot: ScopeSlot) -> StagedWrite:
        current = slot.staged
        assert current is not None
        write = current.write
        state = current.state
        if slot.snapshot_needed or self._snapshot == "always" or slot.tail_count + len(write["inputs"]) > 4096 or slot.tail_bytes + current.bytes >= max(slot.snapshot_bytes, 4096):
            record = to_record(state)
            write["snapshot"] = {"version": state.version, "record": record, "policy": self._config.policy, "reducer": REDUCER_VERSION}
            if current.serialized is not None:
                current.serialized["snapshot"] = stringify(record)
                current.snapshot_bytes = utf8_length(current.serialized["snapshot"])
            else:
                current.snapshot_bytes = record_bytes(record)
        due = None if state.due_at is None else _ceil_second(state.due_at)
        if not slot.has_stored_due or due != slot.stored_due:
            write["due"] = due
        else:
            write.pop("due", None)
        if current.serialized is not None:
            write["serialized"] = current.serialized
        return current

    def _outcome(self, slot: ScopeSlot, current: StagedWrite, result: CommitOutcome | BaseException, failed: bool = False) -> Awaitable[None] | None:
        slot.inflight = None
        if failed:
            slot.state = None
            error = result if isinstance(result, BaseException) else RuntimeError(str(result))
            for raw in current.raw:
                self._reject(raw, error)
        elif result == "conflict":
            self._observe({"kind": "conflict", "scope": slot.scope, "version": current.write["expectedVersion"]})
            slot.state = None
            attempt = 1 + max([0, *(raw.conflicts for raw in current.raw)])
            retry: list[RawInput] = []
            for raw in current.raw:
                raw.conflicts = attempt
                if attempt >= _MAX_CONFLICTS:
                    self._observe({"kind": "abandoned", "scope": slot.scope, "input": _input_kind(raw.input)})
                    self._reject(raw, ConflictError(slot.scope, _MAX_CONFLICTS))
                else:
                    retry.append(raw)
            slot.queued[:0] = retry
            self._queued += sum(raw.count for raw in retry)
            loop = asyncio.get_running_loop()
            backoff: asyncio.Future[None] = loop.create_future()
            loop.call_later((2 * attempt + random.random() * 4) / 1000, lambda: None if backoff.done() else backoff.set_result(None))
            slot.loading = backoff

            async def resume() -> None:
                try:
                    await asyncio.shield(backoff)
                finally:
                    # Also on cancellation, so the scope never stays behind a backoff nobody will finish.
                    if slot.loading is backoff:
                        slot.loading = None
                    self._pump(slot)

            return resume()
        else:
            write = current.write
            if write["snapshot"] is not None:
                slot.tail_bytes = 0
                slot.tail_count = 0
                slot.snapshot_bytes = current.snapshot_bytes
                slot.snapshot_needed = False
            else:
                slot.tail_bytes += current.bytes
                slot.tail_count += len(write["inputs"])
            if "due" in write:
                slot.stored_due = write["due"]
                slot.has_stored_due = True
            for effect in current.effects:
                self._execute(slot.scope, effect)
            for raw in current.raw:
                raw.resolve()
        self._pump(slot)
        return None

    async def _commit(self, group: list[tuple[ScopeSlot, StagedWrite]]) -> None:
        try:
            results = await self._store.commit([current.write for _, current in group])
        except Exception as error:
            if len(group) == 1:
                slot, current = group[0]
                self._outcome(slot, current, error, True)
            else:
                middle = len(group) // 2
                await self._commit(group[:middle])
                await self._commit(group[middle:])
            return
        waits = [wait for wait in (self._outcome(slot, current, results[i]) for i, (slot, current) in enumerate(group)) if wait is not None]
        if waits:
            await asyncio.gather(*waits)

    def _flush(self) -> None:
        if self._committing or not self._staged:
            return
        self._committing = True
        group: list[tuple[ScopeSlot, StagedWrite]] = []
        size = 0
        for slot in list(self._staged):
            try:
                current = self._prepare(slot)
            except Exception as error:
                # A write that cannot be prepared fails alone, as a failed commit would, and never holds the queue.
                failed = slot.staged
                assert failed is not None
                slot.staged = None
                del self._staged[slot]
                self._outcome(slot, failed, error, True)
                continue
            estimate = current.bytes + current.snapshot_bytes
            if group and (len(group) >= 512 or size + estimate > 4 * 1024 * 1024):
                break
            slot.inflight = current
            slot.staged = None
            del self._staged[slot]
            size += estimate
            group.append((slot, current))
        group.sort(key=lambda pair: pair[0].scope.encode("utf-16-be", "surrogatepass"))
        if not group:
            self._committing = False
            self._request_flush()
            return

        async def commit() -> None:
            try:
                await self._commit(group)
            finally:
                self._committing = False
                self._request_flush()

        self._spawn(commit())

    async def touch(self, scope: str) -> float | None:
        slot = self._slot_for(scope)
        if slot.state is None:
            # Shielded: the load is the slot's own, and cancelling the sweep that waits on it must not cancel it.
            await asyncio.shield(self._load(slot))
        self._arm(slot)
        return slot.state.due_at if slot.state is not None else None

    def due(self, scope: str) -> float | None:
        slot = self._slots.get(scope)
        return slot.state.due_at if slot is not None and slot.state is not None else None

    def arm_all(self) -> None:
        for slot in self._slots.values():
            self._arm(slot)

    def evict(self) -> None:
        now = self._clock.now()
        for scope, slot in list(self._slots.items()):
            if (
                slot.state is not None
                and quiescent(slot.state)
                and slot.staged is None
                and slot.inflight is None
                and slot.loading is None
                and not slot.queued
                and slot.last_touch + self._idle_ms <= now
            ):
                del self._slots[scope]

    def pending(self) -> Awaitable[None]:
        """Waits for every input submitted so far to commit or fail. Never raises, and never cancels a caller's future."""
        snapshot = list(self._pending)

        async def wait() -> None:
            if snapshot:
                await asyncio.wait(snapshot)

        return wait()
