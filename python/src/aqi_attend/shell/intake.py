"""Exclusive local ordering for preparation, writes and immediate effects."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from ..clock import Clock
from ..core.types import Event, EventsInput, KindedEvent, SourceBatch, Trace
from ..errors import BackpressureError, IntakeError, message_of
from ..signal import AbortController, AbortSignal
from .scope import Scopes

type Prepare = Callable[[AbortSignal], Awaitable[SourceBatch]]
_owner: ContextVar[tuple[object, str] | None] = ContextVar("attend_intake_owner", default=None)


def owns_callback(identity: object) -> bool:
    owner = _owner.get()
    return owner is not None and owner[0] is identity


def _consume(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()


@dataclass(slots=True, eq=False)
class _Operation:
    prepare: Prepare
    source: str | None
    controller: AbortController
    done: asyncio.Future[None]
    unlink: Callable[[], None]
    release_capacity: Callable[[], None]


@dataclass(slots=True)
class _Activity:
    """None identifies a timer operation; either kind owns a completion barrier."""

    operation: _Operation | None
    finished: asyncio.Future[None]
    failure: BaseException | None = None


class Intake:
    """Registers raw preparation in order and awaits its immediate causal work."""

    def __init__(
        self,
        scope: str,
        *,
        identity: object,
        scopes: Scopes,
        clock: Clock,
        classify: Callable[[Sequence[Event]], list[KindedEvent]],
        running: Callable[[], bool],
        observe: Callable[[Trace], None],
        capacity: int,
    ) -> None:
        self.scope = scope
        self._identity = identity
        self._scopes = scopes
        self._clock = clock
        self._classify = classify
        self._running = running
        self._observe = observe
        self._capacity = capacity
        self._queue: deque[_Operation] = deque()
        self._active: _Activity | None = None
        self._timer_pending = False
        self._work = 0
        self._work_done = asyncio.Event()
        self._work_done.set()
        self._closed: asyncio.Future[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._preparations: set[asyncio.Future[SourceBatch]] = set()
        self._completion = AbortController()

    @property
    def closed(self) -> bool:
        return self._closed is not None

    @property
    def completion_signal(self) -> AbortSignal:
        return self._completion.signal

    def _check_context(self) -> None:
        if _owner.get() == (self._identity, self.scope):
            raise IntakeError("attend: cannot await this intake from its own callback")

    def submit(self, prepare: Prepare, *, source: str | None = None, signal: AbortSignal | None = None) -> asyncio.Future[None]:
        """Register before returning. Cancelling the returned future only detaches its observer."""
        self._check_context()
        if self.closed:
            raise IntakeError("attend: intake closed")
        if not self._running():
            raise IntakeError("attend: intake requires a running engine")
        pending = len(self._queue) + int(self._active is not None and self._active.operation is not None)
        if pending >= self._capacity:
            raise BackpressureError(100, pending)
        controller = AbortController()
        unlink = signal.add_listener(lambda: controller.abort(signal.reason)) if signal is not None else lambda: None
        try:
            controller.signal.throw_if_aborted()
            release_capacity = self._scopes.reserve_input()
        except BaseException:
            unlink()
            raise
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        done.add_done_callback(_consume)
        operation = _Operation(prepare, source, controller, done, unlink, release_capacity)
        self._queue.append(operation)

        def withdraw() -> None:
            if operation in self._queue:
                self._queue.remove(operation)
                operation.unlink()
                operation.release_capacity()
                if not done.done():
                    reason = controller.signal.reason
                    assert reason is not None
                    done.set_exception(reason)

        controller.signal.add_listener(withdraw)
        self._pump()
        return done

    def wake(self) -> None:
        if not self.closed:
            self._timer_pending = True
            self._pump()

    def _spawn(self, work: Coroutine[Any, Any, None]) -> None:
        def start() -> None:
            task = asyncio.create_task(work)
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            task.add_done_callback(_consume)

        asyncio.get_running_loop().call_soon(start)

    def _pump(self) -> None:
        if self.closed or self._active is not None:
            return
        operation = self._queue.popleft() if self._queue else None
        if operation is None:
            if not self._timer_pending:
                return
            self._timer_pending = False
            due = self._scopes.due(self.scope)
            if due is None or due > self._clock.now():
                return
        activity = _Activity(operation, asyncio.get_running_loop().create_future())
        self._active = activity
        self._spawn(self._perform(activity))

    async def _settle(self, activity: _Activity) -> None:
        try:
            await self._scopes.submit(self.scope, {"kind": "tick"})
        except BaseException as error:
            activity.failure = error
            raise
        await self._work_done.wait()
        if activity.failure is not None:
            raise activity.failure

    async def _perform(self, activity: _Activity) -> None:
        token = _owner.set((self._identity, self.scope))
        operation = activity.operation
        try:
            if operation is None:
                await self._settle(activity)
                return
            signal = operation.controller.signal
            signal.throw_if_aborted()
            preparation = asyncio.ensure_future(operation.prepare(signal))
            self._preparations.add(preparation)
            preparation.add_done_callback(self._prepared)
            if signal.aborted:
                preparation.cancel()
            batch = await signal.race(preparation)
            signal.throw_if_aborted()
            events = self._classify(batch["events"])
            input: EventsInput = {"kind": "events", "events": events}
            if operation.source is not None:
                input["source"] = operation.source
                if "cursor" in batch:
                    input["cursor"] = batch["cursor"]
            if batch.get("truncated"):
                input["truncated"] = True
            # Transfer this slot to Scopes synchronously; it must not be charged twice.
            operation.release_capacity()
            try:
                await self._scopes.submit(self.scope, input, True, signal, catch_up=False)
            except BaseException as error:
                if not signal.aborted:
                    activity.failure = error
                raise
            if events:
                await self._settle(activity)
            if not operation.done.done():
                operation.done.set_result(None)
        except BaseException as error:
            if operation is not None:
                if not operation.done.done():
                    operation.done.set_exception(error)
            elif not isinstance(error, asyncio.CancelledError):
                self._observe({"kind": "failed", "scope": self.scope, "input": "tick", "message": message_of(error)})
        finally:
            if operation is not None:
                operation.unlink()
                operation.release_capacity()
            _owner.reset(token)
            self._active = None
            activity.finished.set_result(None)
            self._pump()

    def _prepared(self, future: asyncio.Future[SourceBatch]) -> None:
        self._preparations.discard(future)
        if not future.cancelled():
            future.exception()

    def _begin_work(self) -> _Activity | None:
        self._work += 1
        self._work_done.clear()
        return self._active

    def _end_work(self, activity: _Activity | None, error: BaseException | None = None) -> None:
        if activity is not None and error is not None and activity.failure is None:
            activity.failure = error
        self._work -= 1
        if self._work == 0:
            self._work_done.set()

    def track_receipt(self, future: asyncio.Future[None]) -> None:
        """Internal writes without user callbacks still belong to the causal chain."""
        activity = self._begin_work()

        def finished(done: asyncio.Future[None]) -> None:
            error = asyncio.CancelledError() if done.cancelled() else done.exception()
            self._end_work(activity, error)

        future.add_done_callback(finished)

    def callback(self, work: Awaitable[None]) -> Coroutine[Any, Any, None]:
        activity = self._begin_work()

        async def run() -> None:
            token = _owner.set((self._identity, self.scope))
            error: BaseException | None = None
            try:
                await work
            except BaseException as failed:
                if not self.closed:
                    error = failed
                raise
            finally:
                _owner.reset(token)
                self._end_work(activity, error)

        return run()

    def fence(self) -> asyncio.Future[None]:
        """Fence before returning; engine shutdown may call this from an owned callback."""
        if self._closed is not None:
            return self._closed
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        future.add_done_callback(_consume)
        self._closed = future
        self._timer_pending = False
        reason = IntakeError("attend: intake closed")
        self._completion.abort(reason)
        for operation in list(self._queue):
            operation.controller.abort(reason)
        activity = self._active
        if activity is not None and activity.operation is not None:
            activity.operation.controller.abort(reason)

        async def join() -> None:
            try:
                if activity is not None:
                    await activity.finished
                await self._work_done.wait()
                await asyncio.gather(*tuple(self._preparations), return_exceptions=True)
                if activity is not None and activity.failure is not None:
                    raise activity.failure
                future.set_result(None)
            except BaseException as error:
                future.set_exception(error)

        self._spawn(join())
        return future

    def close(self) -> Coroutine[Any, Any, None]:
        """Stop new admission and join owned work. This does not resolve future retries."""
        self._check_context()
        future = self.fence()

        async def wait() -> None:
            await asyncio.shield(future)

        return wait()
