"""An opted-in unresolved window retains data and counters through waits/retries."""

import asyncio
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, TypedDict

import pytest

from aqi_attend import AbortSignal, Event, JudgeInput, Judgment, SourceBatch, create_attend, memory_store
from aqi_attend.attend import Attend
from aqi_attend.core.state import from_record
from aqi_attend.core.types import default_policy
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


@pytest.mark.parametrize("control", ["flush", "end"])
async def test_explicit_control_retains_meaning_at_retry(control: str) -> None:
    clock = fake_clock()
    calls: list[tuple[float, str, bool]] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        calls.append((clock.now(), input["window"]["reason"], any(r["route"] == "wait" for r in input["routes"])))
        if len(calls) == 1:
            raise RuntimeError("controlled outage")
        return {"route": "queue"}

    engine = create_attend(clock=clock, kind={"reading": "continuous"}, sink="log", judge=judge, settle={"continuation": "accumulate", "retry_backoff_ms": [30]})
    running = asyncio.create_task(engine.run())
    try:
        await engine.push("s", event("a", 0))
        await clock.advance(15)
        if control == "end":
            await engine.push("s", event("b", 15, 100))
        finish = engine.flush if control == "flush" else engine.end
        await finish("s")
        await clock.advance(24)
        assert len(calls) == 1
        await clock.advance(1)
        assert calls[1] == (40, "forced" if control == "flush" else "closing", control == "end")
        record = await engine.inspect("s")
        assert record is not None and record["buffer"]["forcedAt"] is None
    finally:
        await engine.stop()
        await running


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


@pytest.mark.parametrize("cadence", ["ratio", "always"])
async def test_failure_budget_survives_waiting_restart(cadence: Literal["ratio", "always"]) -> None:
    store, clock = memory_store(), fake_clock()
    first_calls = 0
    resumed_calls: list[float] = []
    settle = {"continuation": "accumulate", "stream_gap_ms": 10, "wait_horizon_ms": 20, "retry_backoff_ms": [30, 60]}

    async def first_judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        nonlocal first_calls
        first_calls += 1
        if first_calls == 1:
            raise RuntimeError("outage")
        return {"route": "wait"}

    async def next_judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        resumed_calls.append(clock.now())
        if len(resumed_calls) == 1:
            raise RuntimeError("outage")
        return {"route": "queue"}

    first = Attend(store=store, clock=clock, kind={"reading": "continuous"}, sink="log", judge=first_judge, settle=settle, snapshot=cadence)
    intake = first.open_intake("s")
    running = asyncio.create_task(first.run())

    async def prepare(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("a", 0)]}

    await intake.submit(prepare)
    await clock.advance(40)
    record = await first.inspect("s")
    assert record is not None and record["batch"] is not None
    assert record["batch"]["phase"]["phase"] == "waiting" and record["batch"]["phase"].get("failures") == 1
    await intake.close()
    await first.stop()
    await running
    resumed = create_attend(store=store, clock=clock, kind={"reading": "continuous"}, sink="log", judge=next_judge, settle=settle)
    next_intake = resumed.open_intake("s")
    next_run = asyncio.create_task(resumed.run())
    try:
        await clock.advance(0)
        assert clock.next_at() == 60
        await clock.advance(80)
        assert resumed_calls == [60, 120]
        record = await resumed.inspect("s")
        assert record is not None and record["batch"] is None
    finally:
        await next_intake.close()
        await resumed.stop()
        await next_run


def test_accumulating_snapshot_requires_waiting_failure_count() -> None:
    waiting = next(item for item in LEGACY["cases"] if item["name"] == "waiting-snapshot")
    policy = default_policy()
    policy["continuation"] = "accumulate"
    with pytest.raises(ValueError, match="requires failures"):
        from_record(waiting["expected"], policy)


async def test_exhaustion_after_wait_keeps_native_dead_letter() -> None:
    store, clock = memory_store(), fake_clock()
    judge = scripted_judge([{"fail": "outage"}, {"route": "wait"}, {"fail": "outage"}, {"fail": "outage"}])
    engine = create_attend(
        store=store,
        clock=clock,
        kind={"reading": "continuous"},
        judge=judge,
        sink="log",
        settle={"continuation": "accumulate", "wait_horizon_ms": 20, "retry_backoff_ms": [30, 60]},
    )
    intake = engine.open_intake("s")
    running = asyncio.create_task(engine.run())

    async def prepare(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("a", 0)]}

    try:
        await intake.submit(prepare)
        await clock.advance(120)
        page = await store.log.read({"after": None, "scope": "s", "limit": 10})
        assert len(page["rows"]) == 1
        row = page["rows"][0]
        assert row["kind"] == "deadLetter" and row["at"] == 120
        assert row["body"]["stage"] == "judge" and row["body"]["attempts"] == 4 and row["body"]["window"]["waits"] == 1
        record = await engine.inspect("s")
        assert record is not None and record["batch"] is None
    finally:
        await intake.close()
        await engine.stop()
        await running


@pytest.mark.parametrize("limit", [134, 135])
async def test_whole_window_byte_threshold(limit: int) -> None:
    async with run(["wait", "queue"], settle={"max_window_bytes": limit}) as (clock, put, calls):
        await put([event("a", 0)])
        await clock.advance(15)
        await put([event("b", 15, 100)])
        await clock.advance(15)
        assert calls[1] == {"at": 15 if limit == 134 else 30, "ids": ["a", "b"], "wait": limit == 135, "waits": 1}


@pytest.mark.parametrize("metadata", ["gap", "dropped"])
async def test_empty_fresh_metadata_belongs_to_unresolved_retry(metadata: str) -> None:
    clock = fake_clock()
    calls: list[tuple[list[str], bool, int]] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        window = input["window"]
        calls.append(([e["id"] for e in window["events"]], window["gap"], window["dropped"]))
        if len(calls) == 1:
            raise RuntimeError("outage")
        return {"route": "queue"}

    engine = create_attend(
        clock=clock,
        kind={"reading": "continuous", "out": "consequence"},
        judge=judge,
        sink="log",
        settle={"continuation": "accumulate", "max_window_ms": 100, "retry_backoff_ms": [30]},
    )
    intake = engine.open_intake("s")
    running = asyncio.create_task(engine.run())

    async def first(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("a", 0)]}

    async def loss(signal: AbortSignal) -> SourceBatch:
        return (
            {"events": [], "truncated": True, "cursor": "2"}
            if metadata == "gap"
            else {"events": [{"id": "old", "at": -100, "observedAt": -100, "name": "out", "data": {}}], "cursor": "2"}
        )

    async def later(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("next", 40)]}

    try:
        await intake.submit(first)
        await clock.advance(15)
        await intake.submit(loss, source="feed")
        await clock.advance(25)
        assert calls[1] == (["a"], metadata == "gap", 1 if metadata == "dropped" else 0)
        record = await engine.inspect("s")
        assert record is not None and record["buffer"] == {"entries": [], "gap": False, "dropped": 0, "forcedAt": None}
        assert record["cursors"] == {"feed": "2"}
        await intake.submit(later)
        await clock.advance(10)
        assert calls[2] == (["next"], False, 0)
    finally:
        await intake.close()
        await engine.stop()
        await running


async def test_gap_at_horizon_retains_wait_allowance() -> None:
    clock, judge = fake_clock(), scripted_judge([{"route": "wait"}, {"route": "queue"}])
    engine = create_attend(clock=clock, kind={"reading": "continuous"}, judge=judge, sink="log", settle={"continuation": "accumulate", "wait_horizon_ms": 20})
    intake = engine.open_intake("s")
    running = asyncio.create_task(engine.run())

    async def first(signal: AbortSignal) -> SourceBatch:
        return {"events": [event("a", 0)]}

    async def gap(signal: AbortSignal) -> SourceBatch:
        return {"events": [], "truncated": True}

    try:
        await intake.submit(first)
        await clock.advance(15)
        await intake.submit(gap)
        await clock.advance(15)
        assert judge.calls[1]["window"]["gap"]
        assert any(r["route"] == "wait" for r in judge.calls[1]["routes"])
        record = await engine.inspect("s")
        assert record is not None and not record["buffer"]["gap"]
    finally:
        await intake.close()
        await engine.stop()
        await running
