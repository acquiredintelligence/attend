"""Source observation and processing clocks remain independent."""

import asyncio

import pytest

from aqi_attend import create_attend
from aqi_attend.core.types import Buffer, Entry, Kind, default_policy
from aqi_attend.core.window import deadline, window_deadline, window_from, window_prune
from aqi_attend.testing import fake_clock, recording_sink, scripted_judge


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


def test_out_of_order_observations_do_not_shorten_quiet_time() -> None:
    assert deadline(default_policy(), buffer(entry("a", 1000, 900), entry("b", 1000, 100))) == (1900, "stream_quiet")


def test_source_age_and_default_backward_clock_behavior() -> None:
    policy = default_policy()
    policy["streamGapMs"] = 100_000
    assert deadline(policy, buffer(entry("a", 1000, 900), entry("b", 1000, 0))) == (60_000, "age")
    assert deadline(policy, buffer(entry("a", 1000), entry("b", 500))) == (61_000, "age")
    assert deadline(default_policy(), buffer(entry("a", 1000), entry("b", 500))) == (1500, "stream_quiet")
    assert deadline(default_policy(), buffer(entry("a", 1000), entry("b", 500, 400))) == (2000, "stream_quiet")


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
