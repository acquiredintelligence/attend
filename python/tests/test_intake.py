"""Local intake owns raw ordering before preparation can yield."""

import asyncio

import pytest

from aqi_attend import AbortController, AbortSignal, Decision, JudgeInput, Judgment, create_attend
from aqi_attend.core.types import SourceBatch
from aqi_attend.testing import fake_clock, scripted_judge


def batch(id: str, at: float = 0, quiet: float = 100) -> SourceBatch:
    return {"events": [{"id": id, "name": "move", "at": at, "observedAt": at, "quietForMs": quiet, "data": {}}]}


async def test_preparation_owns_timer_and_registered_backlog() -> None:
    clock = fake_clock()
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        await intake.submit(first)
        await clock.advance(90)
        release = asyncio.Event()

        async def filtered(signal: AbortSignal) -> SourceBatch:
            await release.wait()
            return {"events": [], "cursor": "1"}

        filtering = intake.submit(filtered, source="feed")
        await clock.advance(5)

        async def later(signal: AbortSignal) -> SourceBatch:
            return batch("b", 95, 300)

        queued = intake.submit(later)
        await clock.advance(195)
        assert judge.calls == []
        release.set()
        await filtering
        await queued
        assert judge.calls == []
        await clock.advance(105)
        assert [call["window"]["settledAt"] for call in judge.calls] == [395]
        assert [event["id"] for event in judge.calls[0]["window"]["events"]] == ["a", "b"]
        record = await attend.inspect("s")
        assert record is not None and record["cursors"] == {"feed": "1"}
    finally:
        await intake.close()
        await attend.stop()
        await running


async def test_raw_arrivals_wait_for_judgment_and_delivery() -> None:
    clock = fake_clock()
    started = asyncio.Event()
    release = asyncio.Event()
    sink_started = asyncio.Event()
    sink_release = asyncio.Event()
    offers: list[tuple[float, list[str]]] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        offers.append((clock.now(), [event["id"] for event in input["window"]["events"]]))
        if len(offers) == 1:
            started.set()
            await release.wait()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if decision["window"]["events"][0]["id"] == "a":
            sink_started.set()
            await sink_release.wait()

    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink=sink)
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    prepared: list[str] = []
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        await intake.submit(first)
        await clock.advance(100)
        await started.wait()
        await clock.advance(50)

        async def second(signal: AbortSignal) -> SourceBatch:
            prepared.append("b")
            return batch("b", 150)

        pending = intake.submit(second)
        release.set()
        await clock.advance(0)
        await sink_started.wait()
        await clock.advance(550)
        assert prepared == []
        sink_release.set()
        await pending
        assert offers == [(100, ["a"]), (700, ["b"])]
    finally:
        release.set()
        sink_release.set()
        await intake.close()
        await attend.stop()
        await running


async def test_abort_before_callback_and_owned_scope_refusal() -> None:
    clock = fake_clock()
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    called: list[bool] = []
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            called.append(True)
            return batch("a")

        controller = AbortController()
        pending = intake.submit(prepare, signal=controller.signal)
        controller.abort()
        with pytest.raises(Exception):
            await pending
        assert called == []
        assert await attend.inspect("s") is None
        with pytest.raises(Exception, match="intake"):
            await attend.push("s", {"id": "other", "name": "move"})
    finally:
        await intake.close()
        await attend.stop()
        await running
