"""Independent counted triggers and background retention through the native API."""

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, TypedDict

import pytest

from aqi_attend import AbortSignal, Attend, Event, JudgeInput, Judgment, SourceBatch, Window, create_attend, memory_store
from aqi_attend._json import clone
from aqi_attend.core.reducer import apply
from aqi_attend.core.state import assert_invariants, initial_state, to_record
from aqi_attend.core.types import ApplyConfig, ApplyOutput, Input, default_policy
from aqi_attend.testing import FakeClock, fake_clock

type Answer = Literal["wait", "queue", "fail"]


class Offer(TypedDict):
    at: float
    window: Window
    wait: bool


def event(id: str, at: float = 0, name: str = "reading") -> Event:
    value: Event = {"id": id, "at": at, "observedAt": at, "name": name, "data": {}}
    if name == "reading":
        value["quietForMs"] = 10
    return value


async def skip(input: JudgeInput, signal: AbortSignal) -> Judgment:
    return {"route": "skip"}


@asynccontextmanager
async def run(
    answers: list[Answer], options: dict[str, Any] | None = None
) -> AsyncGenerator[tuple[FakeClock, Callable[[list[Event]], Awaitable[None]], list[Offer], Attend], None]:
    clock = fake_clock()
    offers: list[Offer] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        offers.append({"at": clock.now(), "window": input["window"], "wait": any(r["route"] == "wait" for r in input["routes"])})
        answer = answers[len(offers) - 1] if len(offers) <= len(answers) else "queue"
        if answer == "fail":
            raise RuntimeError("controlled failure")
        return {"route": answer}

    attend = create_attend(
        clock=clock,
        kind={"reading": "continuous", "meta": "context", "result": "consequence"},
        judge=judge,
        sink="log",
        settle={"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}, "wait_horizon_ms": 20, "retry_backoff_ms": [30], **(options or {})},
    )
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())

    async def put(events: list[Event]) -> None:
        async def prepare(signal: AbortSignal) -> SourceBatch:
            return {"events": events}

        await intake.submit(prepare)

    try:
        yield clock, put, offers, attend
    finally:
        await intake.close()
        await attend.stop()
        await running


async def test_counted_equality_preserves_independent_background() -> None:
    async with run(["queue"]) as (_, put, offers, _attend):
        await put([event("x", 0, "meta"), event("y", 0, "result")])
        await put([event("a"), event("b")])
        assert offers == []
        await put([event("c")])
        assert [([e["id"] for e in o["window"]["events"]], o["wait"], o["window"]["dropped"]) for o in offers] == [(["x", "y", "a", "b", "c"], False, 0)]


async def test_background_retention_is_fifo_without_settling() -> None:
    async with run([]) as (_, put, offers, attend):
        await put([event("x", 0, "meta"), event("y", 0, "result"), event("z", 0, "meta")])
        assert offers == []
        state = await attend.inspect("s")
        assert state is not None
        assert [e["id"] for e in state["buffer"]["entries"]] == ["y", "z"]
        assert state["buffer"]["dropped"] == 1


@pytest.mark.parametrize("answer", ["wait", "fail"])
async def test_retention_spans_held_and_fresh_immediately(answer: Answer) -> None:
    async with run([answer, "queue"]) as (clock, put, offers, attend):
        await put([event("a"), event("x", 0, "meta"), event("y", 0, "result")])
        await clock.advance(10)
        await put([event("z", 10, "meta")])
        state = await attend.inspect("s")
        assert state is not None and state["batch"] is not None
        assert [e["id"] for e in state["batch"]["window"]["events"]] == ["a", "y"]
        assert state["batch"]["window"]["dropped"] == 1
        assert [e["id"] for e in state["buffer"]["entries"]] == ["z"]
        await clock.advance(20 if answer == "wait" else 30)
        assert [e["id"] for e in offers[1]["window"]["events"]] == ["a", "y", "z"]
        assert offers[1]["window"]["dropped"] == 1


async def test_atomic_input_does_not_split_or_lose_counted_events() -> None:
    async with run(["queue"]) as (_, put, offers, _attend):
        await put([event("a"), event("x", 0, "meta"), event("b"), event("y", 0, "result"), event("c"), event("z", 0, "meta"), event("d")])
        assert len(offers) == 1
        assert [e["id"] for e in offers[0]["window"]["events"]] == ["a", "b", "y", "c", "z", "d"]
        assert offers[0]["wait"] is False
        assert offers[0]["window"]["dropped"] == 1


async def test_zero_background_retains_counted_events() -> None:
    async with run(["queue"], {"event_limits": {"counted": 1, "background": 0}}) as (_, put, offers, _attend):
        await put([event("x", 0, "meta"), event("a"), event("y", 0, "result")])
        assert [e["id"] for e in offers[0]["window"]["events"]] == ["a"]
        assert offers[0]["window"]["dropped"] == 2


@pytest.mark.parametrize("continuation", [None, "separate"])
def test_independent_limits_require_explicit_accumulate(continuation: str | None) -> None:
    with pytest.raises((TypeError, ValueError)):
        create_attend(kind={}, judge=skip, sink="log", settle={"continuation": continuation, "event_limits": {"counted": 3, "background": 2}})


def test_independent_limits_refuse_explicit_scalar() -> None:
    with pytest.raises((TypeError, ValueError)):
        create_attend(kind={}, judge=skip, sink="log", settle={"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}, "max_events": 9})


@pytest.mark.parametrize("limits", [None, {"counted": 4, "background": 2}, {"counted": 3, "background": 1}])
async def test_unresolved_policy_change_rejects_without_write(limits: dict[str, int] | None) -> None:
    store = memory_store()
    clock = fake_clock()
    first = create_attend(
        store=store, clock=clock, kind={"meta": "context"}, judge=skip, sink="log", settle={"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}}
    )
    await first.push("s", event("x", 0, "meta"))
    before = await store.load("s")
    changed = {} if limits is None else {"event_limits": limits}
    second = create_attend(store=store, clock=clock, kind={"meta": "context"}, judge=skip, sink="log", settle={"continuation": "accumulate", **changed})
    with pytest.raises(ValueError, match="unresolved"):
        await second.push("s", event("y", 0, "meta"))
    assert await store.load("s") == before


@pytest.mark.parametrize("kind", ["judged", "judgeFailed", "judgeReleased", "judgeDeferred"])
def test_direct_writer_transition_retains_state_without_mutating_offer(kind: str) -> None:
    policy = default_policy()
    policy.update(continuation="accumulate", eventLimits={"counted": 3, "background": 2}, seenCount=0, retryBackoffMs=[30])
    config = ApplyConfig(policy, "log", "log", True)
    state = initial_state("s")

    def send(input: Input, now: float) -> ApplyOutput:
        out = ApplyOutput()
        apply(config, state, {"input": input, "now": now}, out)
        assert_invariants(state, policy)
        return out

    send({"kind": "events", "events": [{**event("a"), "kind": "continuous"}, {**event("x", 0, "meta"), "kind": "context"}, {**event("y", 0, "result"), "kind": "consequence"}]}, 0)
    started = next(e for e in send({"kind": "tick"}, 10).effects if e["kind"] == "judge")
    assert started["kind"] == "judge"
    original = clone(started["window"])
    send(
        {
            "kind": "events",
            "truncated": True,
            "events": [{**event("z", 11, "meta"), "kind": "context"}, {**event("w", 11, "result"), "kind": "consequence"}, {**event("v", 11, "meta"), "kind": "context"}],
        },
        11,
    )
    assert started["window"] == original
    completion: Input
    if kind == "judged":
        completion = {"kind": "judged", "batchId": original["id"], "attempt": started["attempt"], "judgment": {"route": "wait"}}
    elif kind == "judgeFailed":
        completion = {"kind": "judgeFailed", "batchId": original["id"], "attempt": started["attempt"], "message": "controlled", "permanent": False}
    elif kind == "judgeDeferred":
        completion = {"kind": "judgeDeferred", "batchId": original["id"], "attempt": started["attempt"], "until": 42}
    else:
        completion = {"kind": "judgeReleased", "batchId": original["id"], "attempt": started["attempt"]}
    send(completion, 12)
    assert started["window"] == original
    record = to_record(state)
    assert record["batch"] is not None
    assert [e["id"] for e in record["batch"]["window"]["events"]] == ["a"]
    assert record["batch"]["window"]["dropped"] == 2
    assert [e["id"] for e in record["buffer"]["entries"]] == ["w", "v"]
    assert record["buffer"]["dropped"] == 1
    assert record["buffer"]["gap"] is True
    assert state.known == {"a": 1, "w": 1, "v": 1}
    assert record["batch"]["window"]["id"] != original["id"]


@pytest.mark.parametrize(
    "limits",
    [
        None,
        [],
        {},
        {"counted": 0, "background": 1},
        {"counted": True, "background": 1},
        {"counted": 1.5, "background": 1},
        {"counted": 1, "background": -1},
        {"counted": 1, "background": float("inf")},
        {"counted": 2**53, "background": 0},
        {"counted": 1},
        {"counted": 1, "background": 0, "extra": 1},
    ],
)
def test_invalid_limits_are_refused(limits: object) -> None:
    with pytest.raises(ValueError):
        create_attend(kind={}, judge=skip, sink="log", settle={"continuation": "accumulate", "event_limits": limits})


async def test_supplied_pair_is_copied() -> None:
    limits = {"counted": 2, "background": 1}
    async with run(["queue"], {"event_limits": limits}) as (_, put, offers, _attend):
        limits["counted"] = 9
        await put([event("a"), event("b")])
        assert len(offers) == 1
        assert offers[0]["wait"] is False


async def test_deduplication_precedes_counted_equality() -> None:
    async with run(["queue"]) as (_, put, offers, _attend):
        await put([event("a"), event("a"), event("b")])
        assert offers == []
        await put([event("c")])
        assert [e["id"] for e in offers[0]["window"]["events"]] == ["a", "b", "c"]


LEGACY = json.loads((Path(__file__).resolve().parents[2] / "conformance/fixtures/event-limits-legacy.json").read_text())


@pytest.mark.parametrize("item", LEGACY["cases"], ids=[c["name"] for c in LEGACY["cases"]])
async def test_revision_four_snapshots_and_tails_keep_saved_policy(item: dict[str, Any]) -> None:
    store, clock = memory_store(), fake_clock(item["now"])
    for writes in item["writes"]:
        assert await store.commit(writes) == ["ok"] * len(writes)
    assert await store.load("s") == item["stored"]
    assert item["stored"]["snapshot"]["reducer"] == 4 and item["stored"]["tail"]
    engine = create_attend(
        store=store, clock=clock, kind={"meta": "context"}, judge=skip, sink="log", settle={"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}}
    )
    assert await engine.inspect("s") == item["expected"]
    write = engine.push("s", event("next", item["now"], "meta"))
    if item["name"] == "complete":
        await write
        stored = await store.load("s")
        assert stored is not None and stored["snapshot"] is not None
        assert stored["snapshot"]["reducer"] == 5
        assert stored["snapshot"]["policy"].get("eventLimits") == {"counted": 3, "background": 2}
        record = await engine.inspect("s")
        assert record is not None and record["recent"] == item["expected"]["recent"]
    else:
        with pytest.raises(ValueError, match="unresolved"):
            await write
        assert await store.load("s") == item["stored"]


@pytest.mark.parametrize("answer", ["wait", "fail"])
async def test_same_policy_resumes_persisted_state(answer: Answer) -> None:
    clock, store = fake_clock(), memory_store()
    settle = {"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}, "wait_horizon_ms": 20, "retry_backoff_ms": [30]}

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        if answer == "fail":
            raise RuntimeError("controlled")
        return {"route": answer}

    first = create_attend(store=store, clock=clock, kind={"reading": "continuous", "meta": "context"}, judge=judge, sink="log", settle=settle)
    intake = first.open_intake("s")
    run = asyncio.create_task(first.run())

    async def initial(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("a"), event("x", 0, "meta"), event("y", 0, "meta")]}

    async def fresh(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("z", 15, "meta")]}

    await intake.submit(initial)
    await clock.advance(15)
    await intake.submit(fresh)
    await intake.close()
    await first.stop()
    await run
    before = await first.inspect("s")
    seen: list[list[str]] = []

    async def finish(input: JudgeInput, signal: AbortSignal) -> Judgment:
        seen.append([e["id"] for e in input["window"]["events"]])
        return {"route": "queue"}

    second = create_attend(store=store, clock=clock, kind={}, judge=finish, sink="log", settle=settle)
    running = asyncio.create_task(second.run())
    try:
        await second.push("s", [])
        assert await second.inspect("s") == before
        await clock.advance(25)
        assert seen == [["a", "y", "z"]]
    finally:
        await second.stop()
        await running
