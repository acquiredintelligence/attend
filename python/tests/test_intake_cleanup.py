"""A withdrawn receipt and a finished preparation callback are different facts."""

import asyncio

import pytest

from aqi_attend import AbortController, AbortSignal, Decision, IntakeError, JudgeInput, Judgment, SourceBatch, create_attend
from aqi_attend.attend import JudgeContext
from aqi_attend.signal import AbortError
from aqi_attend.testing import fake_clock, scripted_judge


@pytest.mark.parametrize("via", ["close", "stop", "cancel_runner", "cancel_runner_twice"])
async def test_close_joins_cooperative_preparation_cleanup(via: str) -> None:
    clock = fake_clock()
    engine = create_attend(clock=clock, kind={}, judge=scripted_judge([]), sink="log")
    intake = engine.open_intake("scope")
    running = asyncio.create_task(engine.run())
    started, cleaning, release, ended = [asyncio.Event() for _ in range(4)]

    async def prepare(signal: AbortSignal) -> SourceBatch:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            ended.set()
        return {"events": []}

    try:
        receipt = intake.submit(prepare)
        await started.wait()
        closing = asyncio.create_task(intake.close() if via == "close" else engine.stop())
        await cleaning.wait()
        await clock.advance(0)
        assert receipt.done()
        with pytest.raises(IntakeError):
            await receipt
        assert not closing.done() and not ended.is_set()
        if via in ("cancel_runner", "cancel_runner_twice"):
            running.cancel()
            await clock.advance(0)
            assert not closing.done()
            if via == "cancel_runner_twice":
                running.cancel()
                await clock.advance(0)
                assert not closing.done() and not running.done()
            other = asyncio.create_task(intake.close())
            await clock.advance(0)
            assert not other.done()
        release.set()
        await closing
        await intake.close()
        assert ended.is_set()
        assert await engine.inspect("scope") is None
    finally:
        release.set()
        await engine.stop()
        await asyncio.gather(running, return_exceptions=True)


async def test_close_joins_an_earlier_withdrawn_callback_without_delaying_its_receipt() -> None:
    clock = fake_clock()
    engine = create_attend(clock=clock, kind={}, judge=scripted_judge([]), sink="log", max_queued_inputs=1)
    intake = engine.open_intake("scope")
    running = asyncio.create_task(engine.run())
    started, cleaning, release = [asyncio.Event() for _ in range(3)]
    controller = AbortController()

    async def prepare(signal: AbortSignal) -> SourceBatch:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
        return {"events": [], "cursor": "late"}

    async def next_input(signal: AbortSignal) -> SourceBatch:
        return {"events": [], "cursor": "kept"}

    try:
        receipt = intake.submit(prepare, source="feed", signal=controller.signal)
        await started.wait()
        controller.abort()
        with pytest.raises(AbortError):
            await receipt
        await cleaning.wait()
        await intake.submit(next_input, source="feed")
        closing = asyncio.create_task(intake.close())
        cancelled_waiter = asyncio.create_task(intake.close())
        await clock.advance(0)
        cancelled_waiter.cancel()
        await asyncio.gather(cancelled_waiter, return_exceptions=True)
        assert not closing.done()
        release.set()
        await closing
        record = await engine.inspect("scope")
        assert record is not None and record["cursors"] == {"feed": "kept"}
    finally:
        release.set()
        await engine.stop()
        await running


@pytest.mark.parametrize("eager", [False, True])
async def test_abort_during_callback_creation_cancels_the_returned_task(eager: bool) -> None:
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    clock = fake_clock()
    engine = create_attend(clock=clock, kind={}, judge=scripted_judge([]), sink="log")
    intake = engine.open_intake("scope")
    running = asyncio.create_task(engine.run())
    controller = AbortController()
    called: list[bool] = []
    created: list[asyncio.Task[SourceBatch]] = []

    async def work() -> SourceBatch:
        called.append(True)
        await asyncio.Event().wait()
        return {"events": []}

    def prepare(signal: AbortSignal) -> asyncio.Task[SourceBatch]:
        controller.abort()
        task = asyncio.create_task(work())
        created.append(task)
        return task

    try:
        with pytest.raises(AbortError):
            await intake.submit(prepare, signal=controller.signal)
        await clock.advance(0)
        assert called == ([True] if eager else [])
        assert created[0].cancelled()
        await intake.close()
    finally:
        for task in created:
            task.cancel()
        await asyncio.gather(*created, return_exceptions=True)
        await engine.stop()
        await running
        loop.set_task_factory(previous_factory)


async def test_close_consumes_a_withdrawn_callback_that_later_fails() -> None:
    clock = fake_clock()
    engine = create_attend(clock=clock, kind={}, judge=scripted_judge([]), sink="log")
    intake = engine.open_intake("scope")
    running = asyncio.create_task(engine.run())
    started, cleaning, release = [asyncio.Event() for _ in range(3)]

    async def prepare(signal: AbortSignal) -> SourceBatch:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaning.set()
            await release.wait()
            raise ValueError("late failure") from None
        return {"events": []}

    try:
        receipt = intake.submit(prepare)
        await started.wait()
        closing = asyncio.create_task(intake.close())
        await cleaning.wait()
        await clock.advance(0)
        assert not closing.done()
        release.set()
        await closing
        with pytest.raises(IntakeError):
            await receipt
        assert await engine.inspect("scope") is None
    finally:
        release.set()
        await engine.stop()
        await running


@pytest.mark.parametrize("where", ["prepare", "context", "judge", "sink"])
async def test_stop_refuses_to_join_an_owned_callback(where: str) -> None:
    reached: list[str] = []

    async def stop_inside() -> None:
        with pytest.raises(IntakeError, match="own callback"):
            await engine.stop()
        reached.append(where)

    async def context(scope: str, signal: AbortSignal) -> JudgeContext:
        if where == "context":
            await stop_inside()
        return {}

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        if where == "judge":
            await stop_inside()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if where == "sink":
            await stop_inside()

    async def prepare(signal: AbortSignal) -> SourceBatch:
        if where == "prepare":
            await stop_inside()
        return {"events": [{"id": "a", "name": "message", "at": 0, "data": {}}]}

    engine = create_attend(clock=fake_clock(), kind={"message": "flush"}, context=context, judge=judge, sink=sink)
    intake = engine.open_intake("scope")
    running = asyncio.create_task(engine.run())
    try:
        await intake.submit(prepare)
        assert reached == [where]
        assert engine.running
    finally:
        await intake.close()
        await engine.stop()
        await running
