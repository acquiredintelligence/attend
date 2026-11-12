"""Continuous events can carry distinct quiet durations within one window."""

import asyncio

import pytest

from aqi_attend import create_attend
from aqi_attend.core.types import Buffer, Entry, Kind, default_policy
from aqi_attend.core.window import deadline
from aqi_attend.testing import fake_clock, recording_sink, scripted_judge


def entry(id: str, seen_at: float, *, observed_at: float | None = None, quiet: float | None = None, kind: Kind = "continuous") -> Entry:
    result: Entry = {"id": id, "at": -1, "name": "move", "data": {}, "kind": kind, "seenAt": seen_at}
    if observed_at is not None:
        result["observedAt"] = observed_at
    if quiet is not None:
        result["quietForMs"] = quiet
    return result


def buffer(*entries: Entry) -> Buffer:
    return {"entries": list(entries), "dropped": 0, "gap": False, "forcedAt": None}


@pytest.mark.parametrize(("first", "second", "due"), [(40, 90, 210), (90, 40, 190)])
async def test_continuous_durations_wait_for_latest_end(first: float, second: float, due: float) -> None:
    clock = fake_clock(100)
    judge = scripted_judge([{"route": "queue"}])
    sink = recording_sink()
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink=sink)
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("s", {"id": "a", "name": "move", "quietForMs": first})
        await clock.advance(20)
        await attend.push("s", {"id": "b", "name": "move", "quietForMs": second})
        record = await attend.inspect("s")
        assert record is not None and record["dueAt"] == due
        await clock.advance(due - 121)
        assert len(judge.calls) == 0
        await clock.advance(1)
        assert [call["window"]["settledAt"] for call in judge.calls] == [due]
        assert [(event["id"], event.get("quietForMs"), event["seenAt"]) for event in sink.delivered[0]["window"]["events"]] == [("a", first, 100), ("b", second, 120)]
    finally:
        await attend.stop()
        await running
        await attend.store.close()


def test_mixed_and_out_of_order_durations_take_maximum_event_end() -> None:
    policy = default_policy()
    policy["streamGapMs"] = 30
    assert deadline(policy, buffer(entry("a", 200, observed_at=100, quiet=90), entry("b", 200, observed_at=140, quiet=20))) == (190, "stream_quiet")
    assert deadline(policy, buffer(entry("a", 200, observed_at=140, quiet=20), entry("b", 200, observed_at=100, quiet=90))) == (190, "stream_quiet")
    assert deadline(policy, buffer(entry("a", 100), entry("b", 120, quiet=90))) == (210, "stream_quiet")
    assert deadline(policy, buffer(entry("a", 100, quiet=90), entry("b", 180))) == (210, "stream_quiet")


def test_absent_durations_keep_backward_clock_behavior_and_explicit_ones_include_prior_entries() -> None:
    policy = default_policy()
    policy["streamGapMs"] = 30
    assert deadline(policy, buffer(entry("a", 200), entry("b", 100))) == (130, "stream_quiet")
    assert deadline(policy, buffer(entry("a", 200), entry("b", 100), entry("c", 50, quiet=0))) == (230, "stream_quiet")
    assert deadline(policy, buffer(entry("a", 200, observed_at=120), entry("b", 200, observed_at=100))) == (150, "stream_quiet")


def test_other_triggers_remain_independent() -> None:
    move = entry("a", 100, quiet=90)
    policy = default_policy()
    policy["passiveQuietMs"] = 15
    policy["closingGraceMs"] = 10
    assert deadline(policy, buffer(move, entry("b", 105, kind="passive"))) == (120, "passive_quiet")
    assert deadline(policy, buffer(move, entry("b", 110, kind="closing"))) == (120, "closing")
    assert deadline(policy, buffer(move, entry("b", 115, kind="flush"))) == (115, "flush")
    policy["maxWindowMs"] = 50
    assert deadline(policy, buffer(move)) == (150, "age")
    policy["maxEvents"] = 0
    assert deadline(policy, buffer(move)) == (100, "overflow")


@pytest.mark.parametrize(
    "quiet",
    [None, True, "10", float("nan"), float("inf"), -float("inf"), -1, 10**1000],
    ids=["null", "boolean", "string", "nan", "positive-infinity", "negative-infinity", "negative", "outside-number-range"],
)
async def test_invalid_duration_refuses_whole_batch(quiet: object) -> None:
    attend = create_attend(clock=fake_clock(100), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        with pytest.raises(TypeError, match="quietForMs"):
            await attend.push("s", [{"id": "a", "name": "move"}, {"id": "b", "name": "move", "quietForMs": quiet}])
        assert await attend.inspect("s") is None
    finally:
        await attend.stop()
        await attend.store.close()


@pytest.mark.parametrize("kind", ["closing", "passive", "flush", "consequence", "context"])
async def test_duration_on_other_kind_refuses_whole_batch(kind: Kind) -> None:
    attend = create_attend(clock=fake_clock(100), kind=lambda event: "continuous" if event["name"] == "move" else kind, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        with pytest.raises(TypeError, match="quietForMs"):
            await attend.push("s", [{"id": "a", "name": "move"}, {"id": "b", "name": "other", "quietForMs": 0}])
        assert await attend.inspect("s") is None
    finally:
        await attend.stop()
        await attend.store.close()


async def test_zero_duration_and_late_observation_settle_at_processing_time() -> None:
    clock = fake_clock(100)
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink="log")
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("s", {"id": "a", "name": "move", "observedAt": 10, "quietForMs": 0})
        await clock.advance(0)
        assert [call["window"]["settledAt"] for call in judge.calls] == [100]
        event = judge.calls[0]["window"]["events"][0]
        assert (event["at"], event.get("observedAt"), event["seenAt"], event.get("quietForMs")) == (100, 10, 100, 0)
    finally:
        await attend.stop()
        await running
        await attend.store.close()
