import asyncio
from copy import deepcopy

import pytest

from aqi_attend import AbortSignal, Decision, JudgeInput, Judgment, create_attend
from aqi_attend.core.types import JsonValue
from aqi_attend.store.memory import memory_store
from aqi_attend.testing import fake_clock, recording_sink


async def test_data_survives_failed_delivery_and_retained_store_restart() -> None:
    clock = fake_clock()
    store = memory_store()
    data: dict[str, JsonValue] = {"revision": 7, "tags": ["alpha"]}
    attempts: list[Decision] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        return {"route": "note", "data": data}

    async def fail(decision: Decision, signal: AbortSignal) -> None:
        attempts.append(deepcopy(decision))
        delivered = decision.get("data")
        if isinstance(delivered, dict):
            delivered["revision"] = 99
        raise RuntimeError("retry")

    first = create_attend(clock=clock, store=store, kind={"change": "closing"}, settle={"closing_grace_ms": 1, "retry_backoff_ms": [10]}, judge=judge, sink=fail)
    first_run = asyncio.create_task(first.run())
    try:
        await first.push("scope", {"id": "a", "name": "change", "at": 0, "data": {}})
        await clock.advance(1)
        assert len(attempts) == 1
        assert attempts[0].get("data") == {"revision": 7, "tags": ["alpha"]}
        data["tags"] = ["changed after judgment"]
        record = await first.inspect("scope")
        assert record is not None
        assert record["recent"][0].get("data") == {"revision": 7, "tags": ["alpha"]}
    finally:
        await first.stop()
        await first_run

    sink = recording_sink()
    recent: list[JsonValue] = []

    async def later(input: JudgeInput, signal: AbortSignal) -> Judgment:
        prior = input["recent"][0].get("data")
        recent.append(deepcopy(prior))
        if isinstance(prior, dict):
            prior["revision"] = 100
        return {"route": "note"}

    second = create_attend(clock=clock, store=store, kind={"change": "closing"}, settle={"closing_grace_ms": 1, "retry_backoff_ms": [10]}, judge=later, sink=sink)
    second_run = asyncio.create_task(second.run())
    try:
        await clock.advance(10)
        assert len(sink.delivered) == 1
        assert sink.delivered[0]["id"] == attempts[0]["id"]
        assert sink.delivered[0].get("data") == {"revision": 7, "tags": ["alpha"]}
        await second.push("scope", {"id": "b", "name": "change", "at": 10, "data": {}})
        await clock.advance(1)
        assert recent == [{"revision": 7, "tags": ["alpha"]}]
        record = await second.inspect("scope")
        assert record is not None
        assert record["recent"][0].get("data") == {"revision": 7, "tags": ["alpha"]}
    finally:
        await second.stop()
        await second_run
    assert clock.next_at() is None


@pytest.mark.parametrize("data", [None, False, 0, "", [], {}])
async def test_explicit_json_data(data: JsonValue) -> None:
    clock = fake_clock()
    sink = recording_sink()

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        return {"route": "note", "data": data}

    attend = create_attend(clock=clock, kind={"change": "closing"}, settle={"closing_grace_ms": 1}, sink=sink, judge=judge)
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("scope", {"id": "a", "name": "change", "at": 0, "data": {}})
        await clock.advance(1)
        assert "data" in sink.delivered[0] and sink.delivered[0]["data"] == data
        record = await attend.inspect("scope")
        assert record is not None
        assert "data" in record["recent"][0] and record["recent"][0]["data"] == data
    finally:
        await attend.stop()
        await running
