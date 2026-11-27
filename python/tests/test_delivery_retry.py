"""The saved sink retry policy controls recovery and exhaustion."""

import asyncio

import pytest

from aqi_attend import AbortSignal, create_attend, memory_store
from aqi_attend.attend import settle_policy
from aqi_attend.core.types import Decision, JudgeInput, Judgment, Kind
from aqi_attend.shell.read import replay
from aqi_attend.testing import fake_clock


async def test_resume_sink_retry_with_a_changed_schedule() -> None:
    clock = fake_clock()
    store = memory_store()
    kinds: dict[str, Kind] = {"message": "flush"}

    async def judge(_input: JudgeInput, _signal: AbortSignal) -> Judgment:
        return {"route": "note"}

    async def fail(_decision: Decision, _signal: AbortSignal) -> None:
        raise RuntimeError("store unavailable")

    schedule = [10]
    first = create_attend(clock=clock, store=store, kind=kinds, judge=judge, sink=fail, settle={"retry_backoff_ms": [1000], "delivery_retry_backoff_ms": schedule})
    schedule[0] = 500
    running = asyncio.create_task(first.run())
    try:
        await first.push("s", {"id": "a", "name": "message"})
        await clock.advance(0)
        record = await first.inspect("s")
        assert record is not None
        assert record["outbox"]["decisions"][0]["phase"] == {"phase": "retrying", "attempt": 1, "failures": 1, "at": 10}
        saved = await store.load("s")
        assert saved is not None and saved["snapshot"] is not None
        assert saved["snapshot"]["policy"].get("deliveryRetryBackoffMs") == [10]
        assert any(logged["input"]["kind"] == "deliveryFailed" for logged in saved["tail"])
        legacy = saved.copy()
        legacy["snapshot"] = {**saved["snapshot"], "reducer": 7}
        assert replay("s", legacy, "lane", "log").due_at == 1000
    finally:
        await first.stop()
        await running

    attempts: list[float] = []

    async def sink(_decision: Decision, _signal: AbortSignal) -> None:
        attempts.append(clock.now())
        if len(attempts) == 1:
            raise RuntimeError("store unavailable")

    second = create_attend(clock=clock, store=store, kind=kinds, judge=judge, sink=sink, settle={"retry_backoff_ms": [1000], "delivery_retry_backoff_ms": [50, 60]})
    resumed = asyncio.create_task(second.run())
    try:
        await clock.advance(9)
        assert attempts == []
        await clock.advance(1)
        assert attempts == [10]
        await clock.advance(60)
        assert attempts == [10, 70]
    finally:
        await second.stop()
        await resumed
        await store.close()


@pytest.mark.parametrize("value", [None, 10, "10", [True], ["10"], [-1], [float("nan")], [float("inf")], [10**310]])
def test_refuses_invalid_delivery_retry_schedule(value: object) -> None:
    with pytest.raises(TypeError, match="deliveryRetryBackoffMs"):
        settle_policy({"delivery_retry_backoff_ms": value})
