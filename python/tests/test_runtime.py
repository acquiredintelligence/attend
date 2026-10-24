"""asyncio edges the TypeScript runtime cannot have: cancellation, eager futures, and Python number semantics."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence
from typing import Any

import pytest

from attend import AbortController, AbortSignal, Decision, JudgeInput, Judgment, create_attend
from attend._json import stable_json
from attend.core.reducer import _dead_letter_delay  # pyright: ignore[reportPrivateUsage]
from attend.store.memory import MemoryStore
from attend.store.types import CommitOutcome, ScopeWrite, StoredScope
from attend.testing import fake_clock


async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
    return {"route": "queue"}


async def settles(work: Any, seconds: float = 2) -> bool:
    """True when ``work`` finishes within ``seconds``. Never cancels it, so a hang fails the test instead of hanging it."""
    future = asyncio.ensure_future(work)
    done, _ = await asyncio.wait({future}, timeout=seconds)
    return future in done


def collect(delivered: list[Decision]) -> Any:
    def sink(decision: Decision, signal: AbortSignal) -> None:
        delivered.append(decision)

    return sink


class GatedStore(MemoryStore):
    """A memory store whose loads wait at a gate while it is closed."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.gate.set()

    async def load(self, scope: str) -> StoredScope | None:
        await self.gate.wait()
        return await super().load(scope)


async def test_cancelling_a_sweep_mid_load_strands_no_input() -> None:
    store = GatedStore()
    delivered: list[Decision] = []
    attend = create_attend(store=store, clock=fake_clock(), kind={"close": "closing"}, judge=judge, sink=collect(delivered))
    store.gate.clear()
    pushed = attend.push("s", {"id": "a", "name": "close"})
    touched = asyncio.ensure_future(attend._scopes.touch("s"))  # pyright: ignore[reportPrivateUsage]
    await asyncio.sleep(0)
    touched.cancel()
    assert await settles(touched)
    store.gate.set()
    assert await settles(pushed), "the push queued behind the cancelled load never landed"
    record = await attend.inspect("s")
    assert record is not None and [e["id"] for e in record["buffer"]["entries"]] == ["a"]


async def test_cancelling_the_run_mid_load_lets_queued_pushes_land_and_the_run_finish() -> None:
    store = GatedStore()
    clock = fake_clock()
    seed = create_attend(store=store, clock=clock, kind={"close": "closing"}, judge=judge, sink=lambda d, s: None)
    await seed.push("s", {"id": "a", "name": "close"})
    attend = create_attend(store=store, clock=clock, kind={"close": "closing"}, judge=judge, sink=lambda d, s: None)
    store.gate.clear()
    running = asyncio.ensure_future(attend.run())
    await asyncio.sleep(0.01)
    pushed = attend.push("s", {"id": "b", "name": "close"})
    running.cancel()
    await asyncio.sleep(0)
    store.gate.set()
    assert await settles(pushed), "the push queued behind the cancelled load never landed"
    assert await settles(running), "the cancelled run never finished"
    assert running.cancelled()
    record = await attend.inspect("s")
    assert record is not None and [e["id"] for e in record["buffer"]["entries"]] == ["a", "b"]


class RacingStore(MemoryStore):
    """Reports the first commit of each scope as a conflict, as a writer racing on another process would."""

    def __init__(self) -> None:
        super().__init__()
        self.lost: set[str] = set()

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        fresh = [w for w in writes if w["scope"] not in self.lost]
        if fresh:
            self.lost.update(w["scope"] for w in fresh)
            return ["conflict" for _ in writes]
        return await super().commit(writes)


async def test_cancelling_a_wait_on_a_conflict_backoff_strands_no_input() -> None:
    store = RacingStore()
    attend = create_attend(store=store, clock=fake_clock(), kind={"close": "closing"}, judge=judge, sink=lambda d, s: None)
    pushed = attend.push("s", {"id": "a", "name": "close"})
    for _ in range(100):
        await asyncio.sleep(0)
        if attend._scopes._slots["s"].loading is not None:  # pyright: ignore[reportPrivateUsage]
            break
    touched = asyncio.ensure_future(attend._scopes.touch("s"))  # pyright: ignore[reportPrivateUsage]
    await asyncio.sleep(0)
    touched.cancel()
    assert await settles(touched)
    assert await settles(pushed), "the push behind the cancelled backoff never landed"
    assert await settles(attend.push("s", {"id": "b", "name": "close"})), "the scope stayed behind a cancelled backoff"
    record = await attend.inspect("s")
    assert record is not None and [e["id"] for e in record["buffer"]["entries"]] == ["a", "b"]


async def test_an_infinite_setting_is_carried_through_as_in_typescript() -> None:
    clock = fake_clock(1000)
    delivered: list[Decision] = []
    attend = create_attend(clock=clock, kind={"close": "closing"}, judge=judge, sink=collect(delivered), settle={"judge_timeout_ms": math.inf})
    running = asyncio.ensure_future(attend.run())
    await asyncio.sleep(0)
    await attend.push("s", {"id": "a", "name": "close"})
    await clock.advance(500)
    assert await settles(attend.push("t", {"id": "b", "name": "close"})), "commits stalled after an infinite deadline"
    await clock.advance(500)
    assert await settles(attend.stop())
    assert await settles(running)
    assert [d["scope"] for d in delivered] == ["s", "t"]


async def test_a_write_that_cannot_be_prepared_fails_alone() -> None:
    clock = fake_clock(1000)
    attend = create_attend(clock=clock, kind={"close": "closing"}, judge=judge, sink=lambda d, s: None)
    scopes: Any = attend._scopes  # pyright: ignore[reportPrivateUsage]
    original = scopes._prepare

    def prepare(slot: Any) -> Any:
        if slot.scope == "bad":
            raise RuntimeError("cannot prepare")
        return original(slot)

    scopes._prepare = prepare
    bad = attend.push("bad", {"id": "a", "name": "close"})
    good = attend.push("good", {"id": "b", "name": "close"})
    assert await settles(bad) and isinstance(bad.exception(), RuntimeError)
    assert await settles(good), "a write that could not be prepared held up another scope"
    scopes._prepare = original
    assert await settles(attend.push("bad", {"id": "c", "name": "close"}))


def test_the_dead_letter_backoff_caps_past_any_float_exponent() -> None:
    assert _dead_letter_delay({"retryBackoffMs": [1000.0, 4000.0, 16000.0], "deadLetterBackoffMaxMs": 60000}, 1030) == 60000  # type: ignore[typeddict-item]
    assert _dead_letter_delay({"retryBackoffMs": [], "deadLetterBackoffMaxMs": 60000.0}, 5000) == 60000  # type: ignore[typeddict-item]
    assert _dead_letter_delay({"retryBackoffMs": [1000, 4000, 16000], "deadLetterBackoffMaxMs": 60000}, 4) == 32000  # type: ignore[typeddict-item]


async def test_cancelling_one_merged_push_leaves_the_others_and_the_event() -> None:
    store = GatedStore()
    attend = create_attend(store=store, clock=fake_clock(), kind={"close": "closing"}, judge=judge, sink=lambda d, s: None)
    store.gate.clear()
    first = attend.push("s", {"id": "a", "name": "close"})
    second = attend.push("s", {"id": "b", "name": "close"})
    third = attend.push("s", {"id": "c", "name": "close"})
    assert first is not second
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(second, 0.01)
    store.gate.set()
    assert await settles(first) and await settles(third)
    assert first.exception() is None and third.exception() is None
    record = await attend.inspect("s")
    assert record is not None and [e["id"] for e in record["buffer"]["entries"]] == ["a", "b", "c"]


async def test_stop_straight_after_starting_a_run_task_stops_it() -> None:
    attend = create_attend(judge=judge, kind={}, sink=lambda d, s: None)
    task = asyncio.create_task(attend.run())
    assert await settles(attend.stop())
    assert await settles(task), "stop() before the run task started did not stop it"
    assert attend.running is False
    again = asyncio.create_task(attend.run())
    await asyncio.sleep(0)
    assert attend.running is True
    assert await settles(attend.stop())
    assert await settles(again)


def test_stable_json_hoists_integer_keys_as_typescript_does() -> None:
    # node: JSON.stringify of the sorted entries rebuilt with Object.fromEntries.
    assert stable_json({"b": 1, "10": 2, "9": 3, "a": {"2": 0, "1": 0, "z": 0}}) == '{"9":3,"10":2,"a":{"1":0,"2":0,"z":0},"b":1}'


async def test_any_stops_listening_once_it_aborts_and_race_cancels_its_work() -> None:
    parent = AbortController()
    for _ in range(50):
        child = AbortController()
        AbortSignal.any([parent.signal, child.signal])
        child.abort()
    assert len(parent.signal._listeners) == 0  # pyright: ignore[reportPrivateUsage]

    finished: list[str] = []

    async def work() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            finished.append("work ended")

    caller = asyncio.ensure_future(AbortController().signal.race(work()))
    await asyncio.sleep(0)
    caller.cancel()
    await asyncio.gather(caller, return_exceptions=True)
    await asyncio.sleep(0)
    assert finished == ["work ended"]
