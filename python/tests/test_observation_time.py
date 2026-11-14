"""Source observation and processing clocks remain independent."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence

import pytest

from aqi_attend import AbortController, AbortSignal, JudgeInput, Judgment, create_attend, memory_store
from aqi_attend.core.step import initial_record
from aqi_attend.core.types import Buffer, Entry, Kind, SourceBatch, default_policy
from aqi_attend.core.window import deadline, window_deadline, window_from, window_prune
from aqi_attend.store.memory import MemoryStore
from aqi_attend.store.types import CommitOutcome, ScopeWrite
from aqi_attend.testing import ScriptedAnswer, fake_clock, harness, recording_sink, scripted_judge


def entry(id: str, seen_at: float, observed_at: float | None = None, kind: Kind = "continuous") -> Entry:
    result: Entry = {"id": id, "at": -1, "name": "change", "data": {}, "kind": kind, "seenAt": seen_at}
    if observed_at is not None:
        result["observedAt"] = observed_at
    return result


def buffer(*entries: Entry) -> Buffer:
    return {"entries": list(entries), "dropped": 0, "gap": False, "forcedAt": None}


async def test_source_observation_sets_quiet_deadline_without_changing_other_clocks() -> None:
    clock = fake_clock(1000)
    judge = scripted_judge([{"route": "queue"}])
    sink = recording_sink()
    attend = create_attend(clock=clock, kind={"change": "continuous"}, judge=judge, sink=sink)
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("s", {"id": "a", "at": -1, "observedAt": 100, "name": "change"})
        record = await attend.inspect("s")
        assert record is not None and record["dueAt"] == 1100
        await clock.advance(99)
        assert len(judge.calls) == 0
        await clock.advance(1)
        assert [call["window"]["settledAt"] for call in judge.calls] == [1100]
        event = sink.delivered[0]["window"]["events"][0]
        assert "observedAt" in event
        assert (event["at"], event["observedAt"], event["seenAt"]) == (-1, 100, 1000)
        assert sink.delivered[0]["at"] == 1100
    finally:
        await attend.stop()
        await running
        await attend.store.close()


@pytest.mark.parametrize(
    "observed_at",
    [None, True, "100", float("nan"), float("inf"), -float("inf"), 1001, -(10**1000)],
    ids=["null", "boolean", "string", "nan", "positive-infinity", "negative-infinity", "future", "outside-number-range"],
)
async def test_invalid_or_future_observation_is_rejected_before_admission(observed_at: object) -> None:
    attend = create_attend(clock=fake_clock(1000), kind={"change": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        with pytest.raises(TypeError, match="observedAt"):
            await attend.push("s", {"id": "a", "name": "change", "observedAt": observed_at})
        assert await attend.inspect("s") is None
    finally:
        await attend.stop()
        await attend.store.close()


@pytest.mark.parametrize(("kind", "gap", "reason"), [("closing", 500, "closing"), ("continuous", 1000, "stream_quiet"), ("passive", 4000, "passive_quiet")])
def test_out_of_order_observations_do_not_shorten_quiet_time(kind: Kind, gap: float, reason: str) -> None:
    assert deadline(default_policy(), buffer(entry("a", 1000, 900, kind), entry("b", 1000, 100, kind))) == (900 + gap, reason)


def test_source_age_and_default_backward_clock_behavior() -> None:
    policy = default_policy()
    policy["streamGapMs"] = 100_000
    assert deadline(policy, buffer(entry("a", 1000, 900), entry("b", 1000, 0))) == (60_000, "age")
    assert deadline(policy, buffer(entry("a", 1000), entry("b", 500))) == (61_000, "age")
    assert deadline(default_policy(), buffer(entry("a", 1000), entry("b", 500))) == (1500, "stream_quiet")
    assert deadline(default_policy(), buffer(entry("a", 1000), entry("b", 500, 350))) == (2000, "stream_quiet")


def test_old_background_observation_behind_fresh_is_pruned_in_arrival_order() -> None:
    open = window_from("s", buffer(entry("fresh", 1000, 900, "context"), entry("old", 1000, 100, "context"), entry("latest", 1000, 950, "context")))
    removed = window_prune(open, 200, 1000)
    assert removed is not None and [event["id"] for event in removed] == ["old"]
    assert [event["id"] for event in open.entries] == ["fresh", "latest"]
    assert open.dropped == 1
    assert window_deadline(default_policy(), open) is None


def test_flush_count_and_byte_deadlines_keep_ingestion_time() -> None:
    policy = default_policy()
    assert deadline(policy, buffer(entry("a", 1000, 100, "flush"))) == (1000, "flush")
    policy["maxEvents"] = 0
    assert deadline(policy, buffer(entry("a", 1000, 100))) == (1000, "overflow")
    policy = default_policy()
    policy["maxWindowBytes"] = 0
    assert deadline(policy, buffer(entry("a", 1000, 100))) == (1000, "size")


async def test_late_settlement_and_judge_lease_use_processing_time() -> None:
    clock = fake_clock(2000)
    response: asyncio.Future[Judgment] = asyncio.get_running_loop().create_future()
    calls = 0

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        nonlocal calls
        calls += 1
        return await response

    attend = create_attend(clock=clock, kind={"change": "continuous"}, judge=judge, sink="log", settle={"judge_timeout_ms": 100})
    running = asyncio.create_task(attend.run())
    try:
        await attend.push("s", {"id": "a", "name": "change", "observedAt": 100})
        await clock.advance(0)
        record = await attend.inspect("s")
        assert record is not None and record["batch"] is not None
        assert record["batch"]["window"]["settledAt"] == 2000
        phase = record["batch"]["phase"]
        assert phase["phase"] == "judging" and phase["leaseUntil"] == 2200
        assert calls == 1
        await clock.advance(99)
        assert calls == 1
    finally:
        if not response.done():
            response.set_result({"route": "skip"})
        await clock.advance(0)
        await attend.stop()
        await running
        await attend.store.close()


async def test_snapshot_and_tail_recovery_preserve_observation_clocks() -> None:
    store = memory_store()
    clock = fake_clock(1000)
    first = create_attend(store=store, clock=clock, kind={"change": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    await first.push("s", {"id": "a", "name": "change", "observedAt": 100})
    await first.push("s", {"id": "b", "name": "change", "observedAt": 200})
    await first.stop()
    stored = await store.load("s")
    assert stored is not None and stored["snapshot"] is not None
    assert stored["snapshot"]["record"]["buffer"]["entries"][0].get("observedAt") == 100
    assert len(stored["tail"]) > 0
    judge = scripted_judge([{"route": "queue"}])
    second = create_attend(store=store, clock=clock, kind={"change": "continuous"}, judge=judge, sink="log")
    running = asyncio.create_task(second.run())
    try:
        await second.push("s", [])
        record = await second.inspect("s")
        assert record is not None and record["dueAt"] == 1200
        await clock.advance(199)
        assert len(judge.calls) == 0
        await clock.advance(1)
        assert [(event["id"], event.get("observedAt"), event["seenAt"]) for event in judge.calls[0]["window"]["events"]] == [("a", 100, 1000), ("b", 200, 1000)]
        assert judge.calls[0]["window"]["settledAt"] == 1200
    finally:
        await second.stop()
        await running
        await store.close()


@pytest.mark.parametrize(("answer", "phase", "delay"), [({"route": "wait"}, "waiting", 10_000), ({"fail": "temporary"}, "retrying", 1000)])
async def test_source_time_survives_crash_while_held(answer: ScriptedAnswer, phase: str, delay: float) -> None:
    judge = scripted_judge([answer, {"route": "queue"}])
    h = await harness({"kind": {"change": "continuous"}, "judge": judge, "sink": recording_sink()})
    try:
        await h.advance(1000)
        await h.push([{"id": "a", "at": -1, "name": "change", "data": {}, "observedAt": 100}])
        await h.advance(100)
        record = await h.inspect()
        assert record is not None and record["batch"] is not None and record["batch"]["phase"]["phase"] == phase
        h.crash()
        await h.restart()
        await h.advance(delay - 1)
        assert len(judge.calls) == 1
        await h.advance(1)
        assert [call["now"] for call in judge.calls] == [1100, 1100 + delay]
        assert [call["window"]["settledAt"] for call in judge.calls] == [1100, 1100]
        event = judge.calls[1]["window"]["events"][0]
        assert (event.get("observedAt"), event["seenAt"]) == (100, 1000)
    finally:
        await h.stop()


@pytest.mark.parametrize("observed_at", [100, 1001])
async def test_source_attachment_checks_observation_before_cursor_commit(observed_at: float) -> None:
    clock = fake_clock(1000)
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(clock=clock, kind={"change": "continuous"}, judge=judge, sink="log")
    controller = AbortController()
    emitted = asyncio.Event()
    refused = False

    class Feed:
        key = "feed"

        async def subscribe(self, cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], signal: AbortSignal) -> None:
            nonlocal refused
            try:
                await emit({"events": [{"id": "a", "at": -1, "name": "change", "data": {}, "observedAt": observed_at}], "cursor": "1"})
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
        assert refused == (observed_at > 1000)
        record = await attend.inspect("s")
        if refused:
            assert record is None
        else:
            assert record is not None and record["cursors"] == {"feed": "1"}
            await clock.advance(100)
            event = judge.calls[0]["window"]["events"][0]
            assert (event.get("observedAt"), event["seenAt"]) == (100, 1000)
    finally:
        controller.abort()
        await attached
        await attend.stop()
        await running
        await attend.store.close()


async def test_prior_reducer_snapshot_and_tail_keep_ingestion_deadlines() -> None:
    store = memory_store()
    policy = default_policy()
    record = initial_record("s")
    record.update(version=1, seen=["a"], buffer=buffer(entry("a", 1000)), dueAt=2000)
    checkpoint: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 0,
        "inputs": [{"now": 1000, "input": {"kind": "tick"}}],
        "snapshot": {"version": 1, "record": record, "policy": policy, "reducer": 1},
        "appends": [],
    }
    tail: ScopeWrite = {
        "scope": "s",
        "expectedVersion": 1,
        "snapshot": None,
        "appends": [],
        "inputs": [{"now": 1100, "input": {"kind": "events", "events": [{"id": "b", "at": -1, "name": "change", "data": {}, "kind": "continuous"}]}}],
    }
    assert await store.commit([checkpoint]) == ["ok"]
    assert await store.commit([tail]) == ["ok"]
    attend = create_attend(store=store, clock=fake_clock(1200), kind={"change": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        await attend.push("s", [])
        loaded = await attend.inspect("s")
        assert loaded is not None and loaded["dueAt"] == 2100
        assert [(event["id"], event["seenAt"], event.get("observedAt")) for event in loaded["buffer"]["entries"]] == [("a", 1000, None), ("b", 1100, None)]
        stored = await store.load("s")
        assert stored is not None and stored["snapshot"] is not None and stored["snapshot"]["reducer"] == 4
    finally:
        await attend.stop()
        await store.close()


async def test_conflicting_write_reloads_without_replacing_observation_time() -> None:
    clock = fake_clock(1000)
    raced = asyncio.Event()

    class RacingStore(MemoryStore):
        injected = False

        async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
            if not self.injected:
                self.injected = True
                assert [write["scope"] for write in writes] == ["s"]
                rival: ScopeWrite = {
                    "scope": "s",
                    "expectedVersion": 0,
                    "snapshot": None,
                    "appends": [],
                    "inputs": [{"now": 1000, "input": {"kind": "events", "events": [{"id": "racer", "at": 0, "name": "context", "kind": "context", "data": {}}]}}],
                }
                await super().commit([rival])
                raced.set()
                return ["conflict" for _ in writes]
            return await super().commit(writes)

    store = RacingStore()
    attend = create_attend(store=store, clock=clock, kind={"change": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    try:
        pushed = attend.push("s", {"id": "a", "name": "change", "observedAt": 100, "quietForMs": 1000})
        await raced.wait()
        await clock.advance(50)
        await pushed
        record = await attend.inspect("s")
        assert record is not None and [event["id"] for event in record["buffer"]["entries"]] == ["racer", "a"]
        assert record["buffer"]["entries"][1].get("observedAt") == 100
        assert record["buffer"]["entries"][1].get("quietForMs") == 1000
        assert record["dueAt"] == 1100
    finally:
        await attend.stop()
        await store.close()
