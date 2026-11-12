"""Local intake owns raw ordering before preparation can yield."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import pytest

from aqi_attend import AbortController, AbortSignal, BackpressureError, Decision, IntakeError, JudgeInput, Judgment, create_attend
from aqi_attend.clock import drain
from aqi_attend.core.types import SourceBatch
from aqi_attend.signal import AbortError
from aqi_attend.store.memory import MemoryStore
from aqi_attend.store.types import CommitOutcome, ScopeWrite
from aqi_attend.testing import fake_clock, scripted_judge


def batch(id: str, at: float = 0, quiet: float = 100) -> SourceBatch:
    return {"events": [{"id": id, "name": "move", "at": at, "observedAt": at, "quietForMs": quiet, "data": {}}]}


async def test_preparation_owns_timer_and_registered_backlog() -> None:
    clock = fake_clock()
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        await intake.submit(first)
        await clock.advance(90)
        release = asyncio.Event()

        async def filtered(signal: AbortSignal) -> SourceBatch:
            await release.wait()
            return {"events": [], "cursor": "1"}

        filtering = intake.submit(filtered, source="feed")
        await clock.advance(5)

        async def later(signal: AbortSignal) -> SourceBatch:
            return batch("b", 95, 300)

        queued = intake.submit(later)
        await clock.advance(195)
        assert judge.calls == []
        release.set()
        await filtering
        await queued
        assert judge.calls == []
        await clock.advance(105)
        assert [call["window"]["settledAt"] for call in judge.calls] == [395]
        assert [event["id"] for event in judge.calls[0]["window"]["events"]] == ["a", "b"]
        record = await attend.inspect("s")
        assert record is not None and record["cursors"] == {"feed": "1"}
    finally:
        await intake.close()
        await attend.stop()
        await running


async def test_raw_arrivals_wait_for_judgment_and_delivery() -> None:
    clock = fake_clock()
    started = asyncio.Event()
    release = asyncio.Event()
    sink_started = asyncio.Event()
    sink_release = asyncio.Event()
    offers: list[tuple[float, list[str]]] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        offers.append((clock.now(), [event["id"] for event in input["window"]["events"]]))
        if len(offers) == 1:
            started.set()
            await release.wait()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if decision["window"]["events"][0]["id"] == "a":
            sink_started.set()
            await sink_release.wait()

    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink=sink)
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    prepared: list[str] = []
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        await intake.submit(first)
        await clock.advance(100)
        await started.wait()
        await clock.advance(50)

        async def second(signal: AbortSignal) -> SourceBatch:
            prepared.append("b")
            return batch("b", 150)

        pending = intake.submit(second)
        release.set()
        await clock.advance(0)
        await sink_started.wait()
        await clock.advance(550)
        assert prepared == []
        sink_release.set()
        await pending
        assert offers == [(100, ["a"]), (700, ["b"])]
    finally:
        release.set()
        sink_release.set()
        await intake.close()
        await attend.stop()
        await running


async def test_abort_before_callback_and_owned_scope_refusal() -> None:
    clock = fake_clock()
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    called: list[bool] = []
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            called.append(True)
            return batch("a")

        controller = AbortController()
        pending = intake.submit(prepare, signal=controller.signal)
        controller.abort()
        with pytest.raises(AbortError):
            await pending
        assert called == []
        assert await attend.inspect("s") is None
        with pytest.raises(Exception, match="intake"):
            await attend.push("s", {"id": "other", "name": "move"})
        with pytest.raises(IntakeError):
            await attend.end("s")
        with pytest.raises(IntakeError):
            await attend.flush("s")

        class RefusedSource:
            key = "feed"

            async def subscribe(self, cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], signal: AbortSignal, /) -> None:
                raise AssertionError("must not subscribe")

        with pytest.raises(IntakeError):
            await attend.attach("s", RefusedSource())
        with pytest.raises(IntakeError, match="already active"):
            attend.open_intake("s")
    finally:
        await intake.close()
        await attend.stop()
        await running


async def test_queued_abort_refunds_its_reservation_without_preparing() -> None:
    attend = create_attend(clock=fake_clock(), max_queued_inputs=2, kind={}, judge=scripted_judge([]), sink="log")
    intake = attend.open_intake("s")
    started, release = asyncio.Event(), asyncio.Event()
    called: list[bool] = []

    async def first(signal: AbortSignal) -> SourceBatch:
        started.set()
        await release.wait()
        return {"events": []}

    async def second(signal: AbortSignal) -> SourceBatch:
        called.append(True)
        return {"events": [], "cursor": "bad"}

    with pytest.raises(IntakeError, match="running"):
        intake.submit(first)
    running = asyncio.create_task(attend.run())
    try:
        pending = intake.submit(first)
        await started.wait()
        controller = AbortController()
        queued = intake.submit(second, source="feed", signal=controller.signal)
        controller.abort()
        with pytest.raises(AbortError):
            await queued
        await attend.push("other", {"id": "c", "name": "output"})
        release.set()
        await pending
        assert not called
        record = await attend.inspect("s")
        assert record is None or record["cursors"] == {}
        with pytest.raises(IntakeError, match="already active"):
            attend.open_intake("other")
    finally:
        release.set()
        await intake.close()
        await attend.stop()
        await running


@dataclass
class Timer:
    at: float
    callback: Callable[[], None]
    cancelled: bool = False


class ManualClock:
    def __init__(self) -> None:
        self.time: float = 0
        self.timers: list[Timer] = []

    def now(self) -> float:
        return self.time

    def at(self, at: float, fn: Callable[[], None]) -> Callable[[], None]:
        timer = Timer(at, fn)
        self.timers.append(timer)

        def cancel() -> None:
            timer.cancelled = True

        return cancel

    def fire(self, at: float) -> None:
        self.time = at
        for timer in sorted([t for t in self.timers if not t.cancelled and t.at <= at], key=lambda t: t.at):
            timer.cancelled = True
            timer.callback()


class GatedStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.gates: dict[str, asyncio.Event] = {}
        self.entered: dict[str, asyncio.Event] = {}

    def hold(self, kind: str) -> None:
        self.gates[kind] = asyncio.Event()
        self.entered[kind] = asyncio.Event()

    def release_all(self) -> None:
        for gate in self.gates.values():
            gate.set()

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        for write in writes:
            for logged in write["inputs"]:
                kind = logged["input"]["kind"]
                if kind in self.gates and not self.gates[kind].is_set():
                    self.entered[kind].set()
                    await self.gates[kind].wait()
        return await super().commit(writes)


async def test_timer_claims_intake_before_its_commit() -> None:
    clock = ManualClock()
    first_result = asyncio.Event()
    first_delivery = asyncio.Event()
    calls: list[tuple[float, str]] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        id = input["window"]["events"][0]["id"]
        calls.append((clock.now(), id))
        if id == "a":
            await first_result.wait()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if decision["window"]["events"][0]["id"] == "a":
            await first_delivery.wait()

    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=judge, sink=sink)
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run(sweep_ms=10**12))
    prepared: list[float] = []
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        await intake.submit(first)
        clock.fire(100)
        stored = await attend.inspect("s")
        assert stored is not None and stored["batch"] is None

        async def second(signal: AbortSignal) -> SourceBatch:
            prepared.append(clock.now())
            return batch("b", 100)

        pending = intake.submit(second)
        await drain()
        assert calls == [(100, "a")]
        clock.fire(150)
        first_result.set()
        await drain()
        clock.fire(200)
        await drain()
        assert prepared == [] and calls == [(100, "a")]
        clock.fire(700)
        first_delivery.set()
        await pending
        assert prepared == [700] and calls == [(100, "a"), (700, "b")]
    finally:
        first_result.set()
        first_delivery.set()
        await attend.stop()
        await running


@pytest.mark.parametrize("timer", [False, True])
async def test_close_and_receipt_wait_for_release_commit(timer: bool) -> None:
    store = GatedStore()
    clock = fake_clock()
    judge = scripted_judge([{"route": "queue"}])
    attend = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=judge, sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run(sweep_ms=10**12))
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            return batch("a", quiet=100 if timer else 0)

        store.hold("judgeReleased")
        if timer:
            await intake.submit(prepare)
            store.hold("tick")
            await clock.advance(100)
            receipt = None
            held = "tick"
        else:
            store.hold("events")
            receipt = intake.submit(prepare)
            held = "events"
        await store.entered[held].wait()
        closed = asyncio.create_task(intake.close())
        store.gates[held].set()
        await store.entered["judgeReleased"].wait()
        await drain()
        assert not closed.done()
        assert receipt is None or not receipt.done()
        assert judge.calls == []
        store.gates["judgeReleased"].set()
        await closed
        if receipt is not None:
            await receipt
        record = await attend.inspect("s")
        assert record is not None and record["batch"] is not None and record["batch"]["phase"]["phase"] == "retrying"
    finally:
        store.release_all()
        await attend.stop()
        await running


async def test_deferral_commit_belongs_to_the_receipt_but_future_retry_does_not() -> None:
    store = GatedStore()
    store.hold("judgeDeferred")
    clock = fake_clock()
    started, release = asyncio.Event(), asyncio.Event()

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        started.set()
        await release.wait()
        return {"route": "skip"}

    attend = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=judge, sink="log", judge_concurrency=1)
    first, second = attend.open_intake("first"), attend.open_intake("second")
    running = asyncio.create_task(attend.run(sweep_ms=10**12))
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            return batch("a", quiet=0)

        one = first.submit(prepare)
        await started.wait()
        two = second.submit(prepare)
        await store.entered["judgeDeferred"].wait()
        await drain()
        assert not two.done()
        store.gates["judgeDeferred"].set()
        await two
        record = await attend.inspect("second")
        assert record is not None and record["batch"] is not None and record["batch"]["phase"]["phase"] == "retrying"
        assert not one.done()
    finally:
        store.release_all()
        release.set()
        await attend.stop()
        await running


async def test_normal_result_queued_before_close_is_fenced_until_application() -> None:
    store = GatedStore()
    store.hold("delivered")
    clock = fake_clock()
    release_b, returned_b = asyncio.Event(), asyncio.Event()
    deliveries = 0

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        if input["window"]["events"][0]["id"] == "b":
            await release_b.wait()
            returned_b.set()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        nonlocal deliveries
        deliveries += 1
        if deliveries == 1:
            raise RuntimeError("controlled failure")

    attend = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=judge, sink=sink, settle={"retry_backoff_ms": [1000]})
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run(sweep_ms=10**12))
    try:

        async def first(signal: AbortSignal) -> SourceBatch:
            return batch("a", quiet=0)

        async def second(signal: AbortSignal) -> SourceBatch:
            return batch("b", quiet=1000)

        await intake.submit(first)
        await intake.submit(second)
        await clock.advance(1000)
        await store.entered["delivered"].wait()
        release_b.set()
        await returned_b.wait()
        closed = asyncio.create_task(intake.close())
        await clock.advance(10)
        store.gates["delivered"].set()
        await closed
        record = await attend.inspect("s")
        assert record is not None and record["batch"] is not None and record["batch"]["phase"]["phase"] == "retrying"
        assert record["outbox"]["decisions"] == [] and len(record["recent"]) == 1
    finally:
        store.release_all()
        release_b.set()
        await attend.stop()
        await running


async def test_engine_budget_reserves_preparation_and_refunds_cancellation() -> None:
    clock = fake_clock()
    attend = create_attend(clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log", max_queued_inputs=1)
    first, second = attend.open_intake("first"), attend.open_intake("second")
    running = asyncio.create_task(attend.run())
    release = asyncio.Event()
    controller = AbortController()
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            await release.wait()
            return batch("a")

        pending = first.submit(prepare, signal=controller.signal)
        with pytest.raises(BackpressureError):
            second.submit(prepare)
        with pytest.raises(BackpressureError):
            await attend.push("ordinary", {"id": "ordinary", "name": "move"})
        controller.abort()
        with pytest.raises(AbortError):
            await pending
        release.set()
        await second.submit(prepare)
        await attend.push("ordinary", {"id": "ordinary", "name": "move"})
    finally:
        release.set()
        await attend.stop()
        await running


@pytest.mark.parametrize("during", ["prepare", "judge", "sink"])
async def test_same_scope_reentrancy_refuses_without_deadlock(during: str) -> None:
    refused: list[str] = []

    async def nested(signal: AbortSignal) -> SourceBatch:
        return batch("nested")

    def attempt() -> None:
        with pytest.raises(IntakeError, match="own callback"):
            intake.submit(nested)
        with pytest.raises(IntakeError, match="own callback"):
            _closed = intake.close()
        refused.append(during)

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        if during == "judge":
            attempt()
        return {"route": "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        if during == "sink":
            attempt()

    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=judge, sink=sink)
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            if during == "prepare":
                attempt()
            return batch("a", quiet=0)

        await intake.submit(prepare)
        assert refused == [during]
    finally:
        await attend.stop()
        await running


async def test_preparation_failure_keeps_cursor_and_allows_next_registration() -> None:
    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def failed(signal: AbortSignal) -> SourceBatch:
            raise ValueError("controlled preparation failure")

        with pytest.raises(ValueError, match="controlled preparation"):
            await intake.submit(failed, source="feed")
        assert await attend.inspect("s") is None

        async def filtered(signal: AbortSignal) -> SourceBatch:
            return {"events": [], "cursor": "2"}

        await intake.submit(filtered, source="feed")
        record = await attend.inspect("s")
        assert record is not None and record["cursors"] == {"feed": "2"}
    finally:
        await attend.stop()
        await running


async def test_observer_cancellation_does_not_withdraw_registered_input() -> None:
    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    release = asyncio.Event()
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            await release.wait()
            return batch("a")

        receipt = intake.submit(prepare)
        receipt.cancel()
        release.set()
        await drain()
        record = await attend.inspect("s")
        assert record is not None and record["seen"] == ["a"]
    finally:
        release.set()
        await attend.stop()
        await running


async def test_close_during_load_refuses_unapplied_input() -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class Store(MemoryStore):
        async def load(self, scope: str):
            entered.set()
            await release.wait()
            return await super().load(scope)

    attend = create_attend(store=Store(), clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        pending = intake.submit(prepare)
        await entered.wait()
        closed = asyncio.create_task(intake.close())
        release.set()
        await closed
        with pytest.raises(IntakeError, match="closed"):
            await pending
        assert await attend.inspect("s") is None
    finally:
        release.set()
        await attend.stop()
        await running


async def test_applied_prefix_can_commit_after_close() -> None:
    store = GatedStore()
    store.hold("events")
    clock = fake_clock()
    attend = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            return batch("a")

        pending = intake.submit(prepare)
        await store.entered["events"].wait()
        await clock.advance(10)
        closed = asyncio.create_task(intake.close())
        await clock.advance(10)
        store.release_all()
        await pending
        await closed
        record = await attend.inspect("s")
        assert record is not None and record["buffer"]["entries"][0]["seenAt"] == 0
    finally:
        store.release_all()
        await attend.stop()
        await running


async def test_internal_write_failure_rejects_the_operation_without_undoing_input() -> None:
    class Store(MemoryStore):
        async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
            if any(logged["input"]["kind"] == "judged" for write in writes for logged in write["inputs"]):
                raise RuntimeError("controlled completion failure")
            return await super().commit(writes)

    attend = create_attend(store=Store(), clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    try:

        async def prepare(signal: AbortSignal) -> SourceBatch:
            return batch("a", quiet=0)

        with pytest.raises(RuntimeError, match="controlled completion"):
            await intake.submit(prepare)
        record = await attend.inspect("s")
        assert record is not None and record["seen"] == ["a"] and record["recent"] == []
    finally:
        await attend.stop()
        await running


async def test_retained_store_recreation_resumes_native_release() -> None:
    store = MemoryStore()
    clock = fake_clock()
    release = asyncio.Event()
    entered = asyncio.Event()

    async def held(input: JudgeInput, signal: AbortSignal) -> Judgment:
        entered.set()
        await release.wait()
        return {"route": "queue"}

    first = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=held, sink="log")
    intake = first.open_intake("s")
    run_first = asyncio.create_task(first.run())

    async def prepare(signal: AbortSignal) -> SourceBatch:
        return batch("a", quiet=0)

    receipt = intake.submit(prepare)
    await entered.wait()
    close = asyncio.create_task(intake.close())
    release.set()
    await close
    await receipt
    await first.stop()
    await run_first
    judge = scripted_judge([{"route": "queue"}])
    second = create_attend(store=store, clock=clock, kind={"move": "continuous"}, judge=judge, sink="log")
    second.open_intake("s")
    run_second = asyncio.create_task(second.run())
    try:
        await clock.advance(0)
        assert [event["id"] for event in judge.calls[0]["window"]["events"]] == ["a"]
        record = await second.inspect("s")
        assert record is not None and record["batch"] is None and record["recent"][0]["route"] == "queue"
    finally:
        await second.stop()
        await run_second


async def test_late_preparation_cannot_admit_after_withdrawal() -> None:
    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    intake = attend.open_intake("s")
    running = asyncio.create_task(attend.run())
    started, release, returned = asyncio.Event(), asyncio.Event(), asyncio.Event()
    controller = AbortController()
    try:

        async def late(signal: AbortSignal) -> SourceBatch:
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
            returned.set()
            return batch("late")

        pending = intake.submit(late, signal=controller.signal)
        await started.wait()
        controller.abort()
        with pytest.raises(AbortError):
            await pending
        await intake.close()
        release.set()
        await returned.wait()
        await drain()
        assert await attend.inspect("s") is None
    finally:
        release.set()
        await attend.stop()
        await running


async def test_other_owned_scope_progresses_during_preparation() -> None:
    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    first, second = attend.open_intake("first"), attend.open_intake("second")
    running = asyncio.create_task(attend.run())
    release = asyncio.Event()
    try:

        async def blocked(signal: AbortSignal) -> SourceBatch:
            await release.wait()
            return batch("a")

        async def immediate(signal: AbortSignal) -> SourceBatch:
            return batch("b", quiet=0)

        pending = first.submit(blocked)
        await second.submit(immediate)
        assert not pending.done()
        record = await attend.inspect("second")
        assert record is not None and record["recent"][0]["route"] == "queue"
        release.set()
        await pending
    finally:
        release.set()
        await attend.stop()
        await running


@pytest.mark.parametrize("capacity", [0, -1, True, 2**53])
def test_capacity_is_a_positive_portable_integer(capacity: int) -> None:
    attend = create_attend(clock=fake_clock(), kind={"move": "continuous"}, judge=scripted_judge([{"route": "queue"}]), sink="log")
    with pytest.raises(ValueError, match="capacity"):
        attend.open_intake("s", capacity=capacity)
