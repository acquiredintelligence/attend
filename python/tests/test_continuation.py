"""An opted-in unresolved window retains data and counters through waits/retries."""

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, TypedDict

import pytest

from aqi_attend import AbortSignal, Event, JudgeInput, Judgment, SourceBatch, create_attend, memory_store
from aqi_attend.testing import FakeClock, fake_clock, scripted_judge

type Answer = Literal["wait", "queue", "fail"]


class Offer(TypedDict):
    at: float
    ids: list[str]
    wait: bool
    waits: int


def event(id: str, observed: float, quiet: float = 10) -> Event:
    return {"id": id, "at": observed, "observedAt": observed, "quietForMs": quiet, "name": "reading", "text": "q" * 20, "data": {}}


@asynccontextmanager
async def run(
    answers: list[Answer], *, settle: dict[str, Any] | None = None, start: float = 0
) -> AsyncGenerator[tuple[FakeClock, Callable[[list[Event]], Awaitable[None]], list[Offer]], None]:
    clock = fake_clock(start)
    calls: list[Offer] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        calls.append(
            Offer(at=clock.now(), ids=[e["id"] for e in input["window"]["events"]], wait=any(r["route"] == "wait" for r in input["routes"]), waits=input["window"]["waits"])
        )
        answer = answers[len(calls) - 1] if len(calls) <= len(answers) else "queue"
        if answer == "fail":
            raise RuntimeError("controlled outage")
        return {"route": answer}

    attend = create_attend(
        clock=clock,
        kind={"reading": "continuous"},
        judge=judge,
        sink="log",
        settle={"continuation": "accumulate", "stream_gap_ms": 10, "wait_horizon_ms": 20, "max_waits": 2, "retry_backoff_ms": [30, 60], **(settle or {})},
    )
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())

    async def put(events: list[Event]) -> None:
        async def prepare(signal: AbortSignal) -> SourceBatch:
            return {"events": events}

        await intake.submit(prepare)

    try:
        yield clock, put, calls
    finally:
        await intake.close()
        await attend.stop()
        await running


async def test_horizon_reoffers_until_wait_allowance_is_used() -> None:
    async with run(["wait", "wait", "queue"]) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(50)
        assert calls == [
            {"at": 10, "ids": ["a"], "wait": True, "waits": 0},
            {"at": 30, "ids": ["a"], "wait": True, "waits": 1},
            {"at": 50, "ids": ["a"], "wait": False, "waits": 2},
        ]


async def test_horizon_absorbs_fresh_before_quiet() -> None:
    async with run(["wait", "wait", "queue"]) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(25)
        await put([event("b", 25, 100)])
        await clock.advance(25)
        assert calls == [
            {"at": 10, "ids": ["a"], "wait": True, "waits": 0},
            {"at": 30, "ids": ["a", "b"], "wait": True, "waits": 1},
            {"at": 50, "ids": ["a", "b"], "wait": False, "waits": 2},
        ]


@pytest.mark.parametrize("quiet", [10, 100])
async def test_retry_absorbs_fresh_at_not_before(quiet: float) -> None:
    async with run(["fail", "queue"]) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(15)
        await put([event("b", 15, quiet)])
        await clock.advance(24)
        assert len(calls) == 1
        await clock.advance(1)
        assert calls == [{"at": 10, "ids": ["a"], "wait": True, "waits": 0}, {"at": 40, "ids": ["a", "b"], "wait": True, "waits": 0}]


async def test_wait_retains_provider_failure_allowance() -> None:
    async with run(["fail", "wait", "fail", "queue"]) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(120)
        assert [c["at"] for c in calls] == [10, 40, 60, 120]
        assert [c["waits"] for c in calls] == [0, 0, 1, 1]


@pytest.mark.parametrize("fresh", [1, 2])
async def test_merged_count_evaluates_all_entries(fresh: int) -> None:
    async with run(["fail", "queue"], settle={"max_events": 2}) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(15)
        await put([event(f"b{i}", 15, 100) for i in range(fresh)])
        await clock.advance(25)
        assert calls[1] == {"at": 40, "ids": ["a", *[f"b{i}" for i in range(fresh)]], "wait": fresh == 1, "waits": 0}


async def test_age_withholds_wait_at_initial_quiet_tie() -> None:
    async with run(["queue"], settle={"max_window_ms": 10}) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(10)
        assert calls == [{"at": 10, "ids": ["a"], "wait": False, "waits": 0}]


async def test_older_absorbed_observation_participates_in_age() -> None:
    async with run(["fail", "queue"], settle={"max_window_ms": 100}, start=100) as (clock, put, calls):
        await put([event("a", 100)])
        await clock.advance(20)
        await put([event("b", 0, 1000)])
        assert len(calls) == 1
        await clock.advance(20)
        assert calls[1] == {"at": 140, "ids": ["a", "b"], "wait": False, "waits": 0}


@pytest.mark.parametrize("answer", ["wait", "fail"])
async def test_combined_byte_threshold(answer: Answer) -> None:
    async with run([answer, "queue"], settle={"max_window_bytes": 100}) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(15)
        await put([event("b", 15, 100)])
        if answer == "fail":
            assert len(calls) == 1
            await clock.advance(25)
        assert calls[1] == {"at": 15 if answer == "wait" else 40, "ids": ["a", "b"], "wait": False, "waits": 1 if answer == "wait" else 0}


@pytest.mark.parametrize("before_mode", ["separate", "accumulate"])
async def test_cannot_change_continuation_with_open_window(before_mode: str) -> None:
    store, clock = memory_store(), fake_clock()
    previous = create_attend(store=store, clock=clock, kind={"reading": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log", settle={"continuation": before_mode})
    await previous.push("s", event("a", 0))
    before = await previous.inspect("s")
    next_engine = create_attend(
        store=store,
        clock=clock,
        kind={"reading": "continuous"},
        judge=scripted_judge([{"route": "queue"}]),
        sink="log",
        settle={"continuation": "accumulate" if before_mode == "separate" else "separate"},
    )
    with pytest.raises(ValueError, match="continuation"):
        await next_engine.push("s", event("b", 0))
    assert await next_engine.inspect("s") == before


@pytest.mark.parametrize("value", [None, False, 0, "other"])
def test_refuses_invalid_continuation(value: object) -> None:
    with pytest.raises(ValueError, match="continuation"):
        create_attend(kind={}, judge=scripted_judge([]), sink="log", settle={"continuation": value})


LEGACY: dict[str, Any] = json.loads((Path(__file__).parents[2] / "conformance/fixtures/continuation-legacy.json").read_text())


@pytest.mark.parametrize("item", LEGACY["cases"], ids=[item["name"] for item in LEGACY["cases"]])
async def test_revision_three_read_and_mode_boundary(item: dict[str, Any]) -> None:
    store, clock = memory_store(), fake_clock(item["through"])
    for writes in item["writes"]:
        assert await store.commit(writes) == ["ok"] * len(writes)
    assert await store.load("scope") == item["stored"]
    engine = create_attend(store=store, clock=clock, kind={"reading": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log", settle={"continuation": "accumulate"})
    assert await engine.inspect("scope") == item["expected"]
    pending = engine.push("scope", event("b", item["through"]))
    if item["name"] == "complete":
        await pending
        stored = await store.load("scope")
        assert stored is not None and stored["snapshot"] is not None
        assert stored["snapshot"]["policy"].get("continuation") == "accumulate"
        assert stored["snapshot"]["reducer"] == 4
        record = await engine.inspect("scope")
        assert record is not None and record["recent"] == item["expected"]["recent"]
    else:
        with pytest.raises(ValueError, match="continuation"):
            await pending
        assert await store.load("scope") == item["stored"]
