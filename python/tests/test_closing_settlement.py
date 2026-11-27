"""Closing durations and consequence completion at the event boundary."""

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from aqi_attend import AbortController, AbortSignal, create_attend, memory_store
from aqi_attend.core.types import Kind, SourceBatch
from aqi_attend.testing import fake_clock, scripted_judge


async def test_attached_completion_replays_with_its_cursor() -> None:
    clock = fake_clock(100)
    store = memory_store()
    judge = scripted_judge([{"route": "note"}])
    kinds: dict[str, Kind] = {"save": "closing", "move": "continuous", "out": "consequence"}
    writer = create_attend(clock=clock, store=store, judge=judge, sink="log", kind=kinds)
    controller = AbortController()

    class Feed:
        key = "feed"

        async def subscribe(self, _cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], _signal: AbortSignal) -> None:
            try:
                await emit({"events": [{"id": "done", "at": 0, "name": "out", "data": {}, "observedAt": 80, "settles": True}], "cursor": "2"})
            finally:
                controller.abort()

    try:
        await writer.push("s", [{"id": "act", "name": "save", "quietForMs": 2000}, {"id": "stream", "name": "move", "quietForMs": 2000}])
        await writer.attach("s", Feed(), signal=controller.signal)
        stored = await store.load("s")
        assert stored is not None and stored["snapshot"] is not None
        assert stored["snapshot"]["record"]["buffer"]["entries"][0].get("quietForMs") == 2000
        tail_input = stored["tail"][0]["input"]
        assert tail_input["kind"] == "events" and tail_input["events"][0].get("settles") is True
    finally:
        await writer.stop()

    reader = create_attend(clock=clock, store=store, judge=judge, sink="log", kind=kinds)
    running = asyncio.create_task(reader.run())
    try:
        record = await reader.inspect("s")
        assert record is not None and record["dueAt"] == record["buffer"].get("endedAt") == 80
        assert record["cursors"] == {"feed": "2"}
        await clock.advance(0)
        (call,) = judge.calls
        assert any(route["route"] == "wait" for route in call["routes"])
        window = call["window"]
        assert (window["reason"], window["settledAt"]) == ("closing", 100)
        assert [event["id"] for event in window["events"]] == ["act", "stream", "done"]
        assert window["events"][0].get("quietForMs") == 2000
        assert window["events"][2].get("settles") is True
    finally:
        await reader.stop()
        await running
        await store.close()


@pytest.mark.parametrize("settles", [False, None, 1, "true"])
async def test_invalid_completion_refuses_whole_batch(settles: object) -> None:
    attend = create_attend(clock=fake_clock(100), kind={"save": "closing", "out": "consequence"}, judge=scripted_judge([]), sink="log")
    try:
        with pytest.raises(TypeError, match="settles"):
            await attend.push("s", [{"name": "save"}, {"name": "out", "settles": settles}])
        assert await attend.inspect("s") is None
    finally:
        await attend.stop()
        await attend.store.close()


async def test_a_tail_under_an_older_snapshot_keeps_settles_as_metadata() -> None:
    from aqi_attend.core.step import initial_record
    from aqi_attend.core.types import default_policy
    from aqi_attend.store.types import ScopeWrite

    store = memory_store()
    policy = default_policy()
    record = initial_record("s")
    record.update(
        version=1,
        seen=["save"],
        buffer={"entries": [{"id": "save", "at": 0, "name": "save", "data": {}, "kind": "closing", "seenAt": 100}], "dropped": 0, "gap": False, "forcedAt": None},
        dueAt=100 + policy["closingGraceMs"],
    )
    checkpoint: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 0,
        "inputs": [{"now": 100, "input": {"kind": "tick"}}],
        "snapshot": {"version": 1, "record": record, "policy": policy, "reducer": 6},
        "appends": [],
    }
    tail: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 1,
        "snapshot": None,
        "appends": [],
        "inputs": [
            {
                "now": 110,
                "input": {"kind": "events", "events": [{"id": "out", "at": 0, "name": "out", "data": {}, "kind": "consequence", "settles": True}]},
            }
        ],
    }
    assert await store.commit([checkpoint]) == ["ok"]
    assert await store.commit([tail]) == ["ok"]
    attend = create_attend(store=store, clock=fake_clock(110), kind={"save": "closing", "out": "consequence"}, judge=scripted_judge([]), sink="log")
    try:
        loaded = await attend.inspect("s")
        assert loaded is not None
        assert loaded["dueAt"] == 100 + policy["closingGraceMs"]
        assert loaded["buffer"].get("endedAt") is None
        assert [event.get("settles") for event in loaded["buffer"]["entries"]] == [None, True]
    finally:
        await attend.stop()
        await store.close()
