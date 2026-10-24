"""The crash suites over every store: crash just before and just after every commit, and keep the same ids."""

from __future__ import annotations

import pytest

from aqi_attend import window_id
from aqi_attend.testing import CrashError, crash_suite, harness, multi_scope_crash_suite, recording_sink, scripted_judge

from .stores import STORES

ID_A = "cf8923dfae84f930"
ID_AB = "dc811072354ed32b"

CASES = [(store, case) for store, make in STORES for case in [*crash_suite(make), *multi_scope_crash_suite(make)]]


@pytest.mark.parametrize(("store", "case"), CASES, ids=[f"{store}: {case.name}" for store, case in CASES])
async def test_crash_suite(store: str, case: object) -> None:
    await case.run()  # type: ignore[attr-defined]


def test_pinned_ids_equal_the_typescript_ids() -> None:
    assert window_id("scope", ["a"]) == ID_A
    assert window_id("scope", ["a", "b"]) == ID_AB
    for scope, ids, expected in [
        ("m/x", ["m/x/a"], "57926a9b506cd3a1"),
        ("m/x", ["m/x/c", "m/x/h"], "4b0d164deed6032f"),
        ("m/x", ["m/x/b", "m/x/d"], "ba2ddaa47740a48a"),
        ("m/y", ["m/y/b", "m/y/d"], "51a3be624f9cf887"),
        ("m/z", ["m/z/b", "m/z/d"], "c43155c9c7ae9dd8"),
    ]:
        assert window_id(scope, ids) == expected


def _kind(event: dict[str, object]) -> str:
    return "closing" if event["name"] == "close" else "consequence"


def _event(id: str) -> dict[str, object]:
    return {"id": id, "at": 0, "name": "close", "data": {}}


async def test_an_open_window_survives_a_restart_and_settles_on_time() -> None:
    judge = scripted_judge([{"route": "queue"}])
    sink = recording_sink()
    h = await harness({"judge": judge, "sink": sink, "kind": _kind})
    await h.push([_event("a")])
    await h.restart()
    await h.advance(499)
    assert judge.calls == []
    await h.advance(1)
    assert [e["id"] for e in judge.calls[0]["window"]["events"]] == ["a"]
    assert [d["id"] for d in sink.delivered] == [ID_A]
    await h.stop()


async def test_an_undelivered_decision_survives_a_crash_between_the_judge_and_the_sink() -> None:
    judge = scripted_judge([{"route": "queue"}])
    sink = recording_sink()
    h = await harness({"judge": judge, "sink": sink, "kind": _kind})
    await h.push([_event("a")])
    h.crash_at(2, "after")
    with pytest.raises(CrashError):
        await h.advance(500)
    assert sink.delivered == []
    record = await h.inspect()
    assert record is not None
    head = record["outbox"]["decisions"][0]
    assert head["item"]["id"] == ID_A
    assert head["phase"] == {"phase": "inFlight", "attempt": 1, "failures": 0, "leaseUntil": 10_500}
    await h.restart()
    assert sink.delivered == []
    await h.advance(10_000)
    assert any(t["kind"] == "retrying" and t.get("stage") == "sink" and t["message"] == "delivery lease expired" for t in h.traces)
    await h.advance(1000)
    assert [d["id"] for d in sink.delivered] == [ID_A]
    await h.stop()


async def test_a_dead_instance_never_writes_again() -> None:
    h = await harness({"judge": scripted_judge([{"route": "queue"}]), "sink": recording_sink(), "kind": _kind})
    old = h.attend
    h.crash()
    await h.restart()
    with pytest.raises(CrashError):
        await old.push("scope", [_event("a")])
    assert await h.inspect() is None
    await h.stop()
