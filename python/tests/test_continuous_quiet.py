"""Continuous events can carry distinct quiet durations within one window."""

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from aqi_attend import AbortController, AbortSignal, create_attend, memory_store
from aqi_attend.core.step import initial_record
from aqi_attend.core.types import Buffer, Entry, Kind, SourceBatch, default_policy
from aqi_attend.core.window import deadline
from aqi_attend.store.types import ScopeWrite
from aqi_attend.testing import ScriptedAnswer, fake_clock, harness, recording_sink, scripted_judge


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


@pytest.mark.parametrize("kind", ["continuous", "passive"])
async def test_source_duration_admission_controls_cursor(kind: Kind) -> None:
    clock = fake_clock(100)
    attend = create_attend(clock=clock, kind={"move": kind}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    controller = AbortController()
    emitted = asyncio.Event()
    refused = False

    class Feed:
        key = "feed"

        async def subscribe(self, cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], signal: AbortSignal) -> None:
            nonlocal refused
            try:
                await emit({"events": [{"id": "a", "at": 0, "name": "move", "data": {}, "observedAt": 100, "quietForMs": 40}], "cursor": "1"})
            except TypeError:
                refused = True
            finally:
                emitted.set()
            ended = asyncio.Event()
            remove = signal.add_listener(ended.set)
            try:
                if not signal.aborted:
                    await ended.wait()
            finally:
                remove()

    running = asyncio.create_task(attend.run())
    attached = asyncio.create_task(attend.attach("s", Feed(), signal=controller.signal))
    try:
        await emitted.wait()
        assert refused == (kind != "continuous")
        record = await attend.inspect("s")
        if refused:
            assert record is None
        else:
            assert record is not None and record["dueAt"] == 140 and record["cursors"] == {"feed": "1"}
            assert record["buffer"]["entries"][0].get("quietForMs") == 40
    finally:
        controller.abort()
        await attached
        await attend.stop()
        await running
        await attend.store.close()


@pytest.mark.parametrize(("answer", "phase", "delay"), [({"route": "wait"}, "waiting", 10_000), ({"fail": "temporary"}, "retrying", 1000)])
async def test_duration_survives_crash_without_changing_processing_clock(answer: ScriptedAnswer, phase: str, delay: float) -> None:
    judge = scripted_judge([answer, {"route": "queue"}])
    h = await harness({"kind": {"move": "continuous"}, "judge": judge, "sink": recording_sink()})
    try:
        await h.push([{"id": "a", "at": -1, "name": "move", "data": {}, "quietForMs": 40}])
        await h.advance(40)
        record = await h.inspect()
        assert record is not None and record["batch"] is not None and record["batch"]["phase"]["phase"] == phase
        h.crash()
        await h.restart()
        await h.advance(delay - 1)
        assert len(judge.calls) == 1
        await h.advance(1)
        assert [call["now"] for call in judge.calls] == [40, 40 + delay]
        assert [call["window"]["settledAt"] for call in judge.calls] == [40, 40]
        assert judge.calls[1]["window"]["events"][0].get("quietForMs") == 40
    finally:
        await h.stop()


async def test_live_policy_change_recomputes_only_omitted_durations() -> None:
    store = memory_store()
    clock = fake_clock(100)
    first = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log", settle={"stream_gap_ms": 80})
    await first.push("s", [{"id": "a", "name": "move"}, {"id": "b", "name": "move", "quietForMs": 40}])
    record = await first.inspect("s")
    assert record is not None and record["dueAt"] == 180
    await first.stop()
    second = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log", settle={"stream_gap_ms": 20})
    try:
        before = await second.inspect("s")
        assert before is not None and before["dueAt"] == 180
        await second.push("s", [])
        record = await second.inspect("s")
        assert record is not None and record["dueAt"] == 140
        assert [event.get("quietForMs") for event in record["buffer"]["entries"]] == [None, 40]
    finally:
        await second.stop()
        await store.close()


async def test_revision_two_snapshot_and_mixed_tail_rebuild_end_times() -> None:
    store = memory_store()
    policy = default_policy()
    policy["streamGapMs"] = 30
    record = initial_record("s")
    record.update(version=1, seen=["a"], buffer=buffer(entry("a", 100, observed_at=100)), dueAt=130)
    checkpoint: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 0,
        "inputs": [{"now": 100, "input": {"kind": "tick"}}],
        "snapshot": {"version": 1, "record": record, "policy": policy, "reducer": 2},
        "appends": [],
    }
    tail: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 1,
        "snapshot": None,
        "appends": [],
        "inputs": [
            {
                "now": 120,
                "input": {
                    "kind": "events",
                    "events": [
                        {"id": "b", "at": 0, "name": "move", "data": {}, "kind": "continuous", "observedAt": 110, "quietForMs": 90},
                        {"id": "c", "at": 0, "name": "move", "data": {}, "kind": "continuous"},
                    ],
                },
            }
        ],
    }
    assert await store.commit([checkpoint]) == ["ok"]
    assert await store.commit([tail]) == ["ok"]
    attend = create_attend(store=store, clock=fake_clock(120), settle={"stream_gap_ms": 30}, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        await attend.push("s", [])
        loaded = await attend.inspect("s")
        assert loaded is not None and loaded["dueAt"] == 200
        assert [(event["id"], event.get("quietForMs"), event["seenAt"]) for event in loaded["buffer"]["entries"]] == [("a", None, 100), ("b", 90, 120), ("c", None, 120)]
        stored = await store.load("s")
        assert stored is not None and stored["snapshot"] is not None and stored["snapshot"]["reducer"] == 5
    finally:
        await attend.stop()
        await store.close()


async def test_explicit_duration_ignores_unused_infinite_default_gap() -> None:
    clock = fake_clock(100)
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(clock=clock, kind={"move": "continuous"}, settle={"stream_gap_ms": float("inf")}, judge=judge, sink="log")
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("s", {"id": "a", "name": "move", "quietForMs": 40})
        record = await attend.inspect("s")
        assert record is not None and record["dueAt"] == 140
        await clock.advance(40)
        assert [call["window"]["settledAt"] for call in judge.calls] == [140]
    finally:
        await attend.stop()
        await running
        await attend.store.close()
