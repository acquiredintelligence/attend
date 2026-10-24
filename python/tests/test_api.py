"""The public API around the shared fixtures: what ``test/lean.test.ts`` and the source and subscription cases of
``test/attend.test.ts`` pin, in Python form."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

from attend import AbortController, Decision, JudgeInput, Judgment, Kind, PermanentError, create_attend, default_notice, memory_store, routes
from attend.signal import AbortSignal
from attend.sources import log_source, memory_log
from attend.testing import fake_clock, recording_sink, scripted_judge

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
ID_AB = "f9df8ba2df127ca6"


async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
    return {"route": "interrupt" if input["window"]["reason"] == "flush" else "note"}


async def test_a_lean_config_delivers_single_events_pushed_without_ids() -> None:
    clock = fake_clock()
    delivered: list[Decision] = []
    attend = create_attend(clock=clock, judge=judge, kind={"user.message": "flush", "build.failed": "closing"}, sink=lambda decision, signal: delivered.append(decision))
    running = asyncio.ensure_future(attend.run())
    await attend.push("session-1", {"name": "build.failed", "text": "Build 4521 failed: 1 test"})
    await clock.advance(100)
    await attend.push("session-1", {"name": "user.message", "text": "can you look?"})
    await clock.advance(1)
    await attend.stop()
    await running
    assert [(d["route"], d["window"]["reason"], [e["name"] for e in d["window"]["events"]]) for d in delivered] == [("interrupt", "flush", ["build.failed", "user.message"])]
    failed, message = delivered[0]["window"]["events"]
    assert UUID.fullmatch(failed["id"]) and failed["id"] != message["id"]
    assert [failed["at"], message["at"], failed["data"], message["data"]] == [0, 100, {}, {}]
    assert default_notice(delivered[0]) == "Build 4521 failed: 1 test\ncan you look?\nAttend to this now, even mid-task."


async def test_a_kind_map_gives_unlisted_names_consequence_and_rejects_unknown_kinds() -> None:
    with pytest.raises(ValueError):
        create_attend(judge=judge, kind={"user.message": "shout"}, sink=lambda d, s: None)  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        create_attend(judge=judge, kind=[], sink=lambda d, s: None)  # type: ignore[arg-type]
    clock = fake_clock()
    delivered: list[Decision] = []
    attend = create_attend(clock=clock, judge=judge, kind={"user.message": "flush"}, sink=lambda decision, signal: delivered.append(decision))
    running = asyncio.ensure_future(attend.run())
    await attend.push("s", {"name": "log.line", "text": "pnpm test"})
    await clock.advance(10_000)
    assert delivered == []
    await attend.push("s", {"name": "user.message", "text": "any news?"})
    await clock.advance(1)
    await attend.stop()
    await running
    assert [[e["name"] for e in d["window"]["events"]] for d in delivered] == [["log.line", "user.message"]]


async def test_an_explicit_id_drops_a_redelivery_and_bad_data_is_rejected() -> None:
    clock = fake_clock()
    delivered: list[Decision] = []
    attend = create_attend(clock=clock, judge=judge, kind={"user.message": "flush"}, sink=lambda decision, signal: delivered.append(decision))
    running = asyncio.ensure_future(attend.run())
    await attend.push("s", [{"id": "m1", "name": "user.message", "text": "hello"}])
    await clock.advance(1)
    await attend.push("s", {"id": "m1", "name": "user.message", "text": "hello"})
    await clock.advance(1)
    with pytest.raises(TypeError, match="event data must be a JSON object"):
        await attend.push("s", {"name": "x", "data": 5})
    with pytest.raises(TypeError, match="events must be an event or a list of events"):
        await attend.push("s", None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="event data must be JSON"):
        await attend.push("s", {"name": "x", "data": {"when": object()}})
    await attend.stop()
    await running
    assert [[e["id"] for e in d["window"]["events"]] for d in delivered] == [["m1"]]


async def test_without_a_dead_letter_handler_a_dead_letter_is_written_to_the_log() -> None:
    clock = fake_clock()
    store = memory_store()

    async def refuse(input: JudgeInput, signal: AbortSignal) -> Judgment:
        raise PermanentError("refused")

    attend = create_attend(clock=clock, store=store, kind={"user.message": "flush"}, sink=lambda d, s: None, judge=refuse)
    running = asyncio.ensure_future(attend.run())
    await attend.push("s", {"id": "m1", "name": "user.message", "text": "hello"})
    await clock.advance(1)
    await attend.stop()
    await running
    page = await store.log.read({"after": None, "limit": 10, "kinds": ["deadLetter"]})
    assert [(row["kind"], row["scope"]) for row in page["rows"]] == [("deadLetter", "s")]


async def test_run_lasts_until_stop_refuses_to_run_twice_and_runs_again() -> None:
    clock = fake_clock()
    delivered: list[str] = []
    attend = create_attend(clock=clock, judge=judge, kind={"user.message": "flush"}, sink=lambda decision, signal: delivered.append(decision["route"]))
    await attend.stop()
    first = asyncio.ensure_future(attend.run())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="already running"):
        await attend.run()
    await attend.stop()
    await first
    second = asyncio.ensure_future(attend.run())
    await asyncio.sleep(0)
    await attend.push("s", {"name": "user.message", "text": "back again"})
    await clock.advance(1)
    await attend.stop()
    await second
    assert delivered == ["interrupt"]


async def test_stop_ends_a_run_that_has_its_own_signal_and_cancelling_the_task_stops_it() -> None:
    attend = create_attend(judge=judge, kind={}, sink=lambda d, s: None)
    controller = AbortController()
    running = asyncio.ensure_future(attend.run(signal=controller.signal))
    await asyncio.sleep(0)
    await attend.stop()
    await running
    assert controller.signal.aborted is False
    task = asyncio.ensure_future(attend.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attend.running is False


async def test_routes_dispatches_each_delivered_route_to_its_handler() -> None:
    seen: list[str] = []

    def handler(name: str) -> Any:
        async def handle(decision: Decision, signal: AbortSignal) -> None:
            seen.append(f"{name}:{decision['id']}")

        return handle

    sink = routes(interrupt=handler("interrupt"), queue=handler("queue"), note=handler("note"), ask=handler("ask"))
    for route in ("interrupt", "queue", "note", "ask"):
        decision: Any = {"id": "w", "route": route}
        await sink.deliver(decision, AbortController().signal)  # type: ignore[misc]
    assert seen == ["interrupt:w", "queue:w", "note:w", "ask:w"]


async def test_log_source_feeds_a_scope_and_resumes_from_the_persisted_cursor() -> None:
    store = memory_store()
    log = memory_log()
    await log.append("s", [{"id": "a", "at": 0, "name": "close", "data": {}}, {"id": "b", "at": 0, "name": "close", "data": {}}])
    kinds: dict[str, Kind] = {"close": "closing"}

    async def session(script: list[Any]) -> tuple[Any, Any, Any]:
        clock = fake_clock(1000)
        sink = recording_sink()
        judged = scripted_judge(script)
        attend = create_attend(store=store, clock=clock, kind=kinds, judge=judged, sink=sink)
        controller = AbortController()
        running = asyncio.ensure_future(attend.run(signal=controller.signal))
        stop = AbortController()
        attached = asyncio.ensure_future(attend.attach("s", log_source(log, "s", block_ms=20, poll_ms=0, clock=clock), signal=stop.signal))
        await asyncio.sleep(0.03)
        await clock.advance(500)
        stop.abort()
        await attached
        controller.abort()
        await running
        return attend, sink, judged

    first, sink, _ = await session([{"route": "queue"}])
    assert [d["id"] for d in sink.delivered] == [ID_AB]
    record = await first.inspect("s")
    assert record is not None and record["cursors"] == {"log:s": "00000000000000000002"}
    await log.append("s", [{"id": "c", "at": 0, "name": "close", "data": {}}])
    _, _, judged = await session([{"route": "queue"}])
    assert [e["id"] for e in judged.calls[0]["window"]["events"]] == ["c"]


async def test_a_second_attach_of_one_key_raises_and_a_permanent_source_error_stops_attach() -> None:
    attend = create_attend(judge=judge, kind={}, sink=lambda d, s: None)
    stop = AbortController()

    class Idle:
        key = "k"

        async def subscribe(self, cursor: str | None, emit: Any, signal: AbortSignal, /) -> None:
            await signal.wait()

    class Revoked:
        key = "fatal"

        async def subscribe(self, cursor: str | None, emit: Any, signal: AbortSignal, /) -> None:
            raise PermanentError("revoked")

    attached = asyncio.ensure_future(attend.attach("s", Idle(), signal=stop.signal))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match='source "k" is already attached to scope "s"'):
        await attend.attach("s", Idle())
    with pytest.raises(PermanentError, match="revoked"):
        await attend.attach("s", Revoked())
    stop.abort()
    await attached


async def test_decision_subscriptions_start_at_the_head_and_page_copies() -> None:
    clock = fake_clock(1000)
    store = memory_store()
    attend = create_attend(store=store, clock=clock, kind={"say": "flush"}, judge=judge, sink="log")
    running = asyncio.ensure_future(attend.run())
    controller = AbortController()
    pages = attend.decisions(signal=controller.signal)
    first = await anext(pages)
    assert first == {"rows": [], "cursor": f"v1.{await store.log.epoch()}.", "truncated": False}
    await attend.push("s", {"id": "a", "name": "say"})
    await clock.advance(0)
    page = await anext(pages)
    assert [row["id"] for row in page["rows"]] == ["3ecbd4f2ef91c78f"]
    copied: Any = page["rows"][0]["body"]
    copied["route"] = "note"
    found: Any = await store.log.find("decision", "3ecbd4f2ef91c78f")
    assert found is not None and found["body"]["route"] == "interrupt"
    controller.abort()
    with pytest.raises(StopAsyncIteration):
        await anext(pages)
    await attend.stop()
    await running
