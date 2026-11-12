"""Independent counted triggers and background retention through the native API."""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, TypedDict

import pytest

from aqi_attend import AbortSignal, Attend, Event, JudgeInput, Judgment, SourceBatch, Window, create_attend, memory_store
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


async def test_unresolved_policy_change_rejects_without_write() -> None:
    store = memory_store()
    clock = fake_clock()
    first = create_attend(
        store=store, clock=clock, kind={"meta": "context"}, judge=skip, sink="log", settle={"continuation": "accumulate", "event_limits": {"counted": 3, "background": 2}}
    )
    await first.push("s", event("x", 0, "meta"))
    before = await store.load("s")
    second = create_attend(store=store, clock=clock, kind={"meta": "context"}, judge=skip, sink="log", settle={"continuation": "accumulate"})
    with pytest.raises(ValueError, match="unresolved"):
        await second.push("s", event("y", 0, "meta"))
    assert await store.load("s") == before
