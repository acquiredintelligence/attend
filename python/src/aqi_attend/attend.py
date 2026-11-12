"""``create_attend``: the scope-owning shell over a store, and its runner. A port of ``src/attend.ts``."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from typing import Any, Literal, Protocol, TypedDict, cast, runtime_checkable

from ._json import clone
from .clock import Clock, sleep, system_clock
from .core.threshold import threshold_route
from .core.types import (
    DEFAULT_POLICY,
    KINDS,
    ROUTES,
    ApplyConfig,
    DeadLetter,
    Decision,
    DeliveredInput,
    DeliveredRoute,
    DeliverEffect,
    DeliveryFailedInput,
    DeliveryReleasedInput,
    Effect,
    Event,
    EventsInput,
    Input,
    InputKind,
    JsonObject,
    JudgeBudget,
    JudgedInput,
    JudgeEffect,
    JudgeFailedInput,
    JudgeImage,
    JudgeInput,
    JudgeReleasedInput,
    Judgment,
    Kind,
    KindedEvent,
    OfferedRoute,
    ReleasedInput,
    Route,
    ScopeRecord,
    SettlePolicy,
    SourceBatch,
    Trace,
)
from .errors import ConflictError, IntakeError, is_permanent, message_of
from .shell.budget import window_bytes_for
from .shell.copy import copy_decision, copy_event, copy_letter, copy_window
from .shell.decisions import DecisionCursorPage, read_decisions
from .shell.intake import Intake, owns_callback
from .shell.read import read_record
from .shell.scheduler import Scheduler
from .shell.scope import Scopes
from .signal import AbortController, AbortSignal
from .store.memory import memory_store
from .store.types import DecisionKind, ScopeStore

type MaybeAwaitable[T] = T | Awaitable[T]
type EffectResult = JudgedInput | JudgeFailedInput | DeliveredInput | DeliveryFailedInput | ReleasedInput


class JudgeContext(TypedDict, total=False):
    """Live context fetched when the judge is called, and again on every retry."""

    facts: JsonObject
    predicates: dict[str, str]


class Judge(Protocol):
    """Decides how the agent hears about a window. Raise to retry; raise a ``PermanentError`` to dead-letter at once.

    A route that was not offered counts as a retryable failure. Must honour ``signal``. It may
    declare a ``budget`` attribute.
    """

    def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Awaitable[Judgment]: ...


@runtime_checkable
class Sink(Protocol):
    """Delivers decisions to the agent, at least once and idempotent on ``decision["id"]``."""

    def deliver(self, decision: Decision, signal: AbortSignal, /) -> MaybeAwaitable[None]: ...


type SinkFunction = Callable[[Decision, AbortSignal], MaybeAwaitable[None]]
type DeadLetterHandler = Callable[[DeadLetter, AbortSignal], MaybeAwaitable[None]]
type ContextFunction = Callable[[str, AbortSignal], MaybeAwaitable[JudgeContext]]
type CaptureFunction = Callable[[str, AbortSignal], Awaitable[JudgeImage | list[JudgeImage] | None]]


class Source(Protocol):
    """Something that feeds a scope. ``emit`` returns once the batch is persisted; the source awaits it before producing more."""

    key: str

    def subscribe(self, cursor: str | None, emit: Callable[[SourceBatch], Awaitable[None]], signal: AbortSignal, /) -> Awaitable[None]: ...


_SETTLE_KEYS: dict[str, str] = {
    "continuation": "continuation",
    "closing_grace_ms": "closingGraceMs",
    "stream_gap_ms": "streamGapMs",
    "passive_quiet_ms": "passiveQuietMs",
    "wait_horizon_ms": "waitHorizonMs",
    "max_waits": "maxWaits",
    "max_events": "maxEvents",
    "max_window_ms": "maxWindowMs",
    "retry_backoff_ms": "retryBackoffMs",
    "judge_timeout_ms": "judgeTimeoutMs",
    "delivery_timeout_ms": "deliveryTimeoutMs",
    "recent_count": "recentCount",
    "seen_count": "seenCount",
    "max_outbox": "maxOutbox",
    "max_dead_letters": "maxDeadLetters",
    "dead_letter_backoff_max_ms": "deadLetterBackoffMaxMs",
    "max_window_bytes": "maxWindowBytes",
}


def settle_policy(settle: Mapping[str, Any] | None = None) -> SettlePolicy:
    """The defaults with ``settle`` (snake_case names, as ``create_attend`` takes them) applied."""
    policy = cast(dict[str, Any], dict(DEFAULT_POLICY))
    policy["retryBackoffMs"] = list(DEFAULT_POLICY["retryBackoffMs"])
    for name, value in (settle or {}).items():
        key = _SETTLE_KEYS.get(name)
        if key is None:
            raise TypeError(f"attend: unknown settle option {name!r}")
        if name == "continuation" and (not isinstance(value, str) or value not in ("separate", "accumulate")):
            raise ValueError("attend: continuation must be separate or accumulate")
        policy[key] = list(value) if name == "retry_backoff_ms" else value
    return cast(SettlePolicy, policy)


def _maybe[T](value: MaybeAwaitable[T]) -> Awaitable[T]:
    async def wrap() -> T:
        result = value
        if inspect.isawaitable(result):
            return await result
        return result

    return wrap()


def _number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _consume(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


class Attend:
    """Intake, inspection, subscriptions and the runner. Make one with ``create_attend``."""

    def __init__(
        self,
        *,
        kind: Callable[[Event], Kind] | Mapping[str, Kind],
        judge: Judge,
        sink: Sink | SinkFunction | Literal["log"],
        store: ScopeStore | None = None,
        dead_letter: DeadLetterHandler | Literal["log"] = "log",
        settle: Mapping[str, Any] | None = None,
        window_share: float = 0.5,
        act_threshold: float | None = None,
        guidance: str | None = None,
        criteria: Mapping[Route, str] | None = None,
        predicates: Mapping[str, str] | None = None,
        context: ContextFunction | None = None,
        capture: CaptureFunction | None = None,
        capture_timeout_ms: float = 500,
        observe: Callable[[Trace], None] | None = None,
        clock: Clock | None = None,
        max_queued_inputs: int = 20_000,
        max_lag_ms: float | None = None,
        judge_concurrency: int | None = None,
        cache_idle_ms: float = 300_000,
        snapshot: Literal["ratio", "always"] = "ratio",
    ) -> None:
        if not _number(window_share) or not (0 < window_share <= 1):
            raise ValueError("attend: window_share must be in (0, 1]")
        if act_threshold is not None and (not _number(act_threshold) or not (0 < act_threshold <= 1)):
            raise ValueError("attend: act_threshold must be in (0, 1]")
        if judge_concurrency is not None and (not _number(judge_concurrency) or not float(judge_concurrency).is_integer() or judge_concurrency <= 0):
            raise ValueError("attend: judge_concurrency must be a positive integer")
        policy = settle_policy(settle)
        budget = cast(JudgeBudget | None, getattr(judge, "budget", None))
        if budget is not None:
            size = window_bytes_for(budget, window_share)
            if not math.isfinite(size) or size <= 0:
                raise ValueError("attend: judge window budget must be positive and finite")
            if settle is None or "max_window_bytes" not in settle:
                policy["maxWindowBytes"] = size
        self._policy = policy
        self._clock = clock if clock is not None else system_clock()
        self._store = store if store is not None else memory_store()
        self._kind = _kind_function(kind)
        self._judge = judge
        self._sink: Sink | Literal["log"]
        if isinstance(sink, str):
            self._sink = "log"
        elif isinstance(sink, Sink):
            self._sink = sink
        else:
            self._sink = _FunctionSink(sink)
        self._dead_letter = dead_letter
        self._act_threshold = act_threshold
        self._guidance = guidance
        self._criteria = dict(criteria or {})
        self._predicates = dict(predicates or {})
        self._context = context
        self._capture = capture
        self._capture_timeout_ms = capture_timeout_ms
        self._observer = observe
        self._judge_concurrency = judge_concurrency
        self._modes: tuple[Literal["lane", "log"], Literal["lane", "log"]] = ("log" if self._sink == "log" else "lane", "log" if dead_letter == "log" else "lane")
        self._sources: set[AbortController] = set()
        self._attached: set[tuple[str, str]] = set()
        self._intakes: dict[str, Intake] = {}
        self._intake_identity = object()
        self._calls: dict[asyncio.Task[None], float] = {}
        self._releases: set[asyncio.Future[None]] = set()
        self._running: AbortSignal | None = None
        self._stopper: AbortController | None = None
        self._finished: asyncio.Future[None] | None = None
        self._judges = 0
        self._scheduler = Scheduler(self._clock, self._on_due)
        self._scopes = Scopes(
            store=self._store,
            clock=self._clock,
            config=ApplyConfig(policy, self._modes[0], self._modes[1], observe is not None),
            max_queued_inputs=max_queued_inputs,
            max_lag_ms=max_lag_ms,
            idle_ms=cache_idle_ms,
            snapshot=snapshot,
            scheduler=self._scheduler,
            running=lambda: self._running is not None,
            execute=self._execute,
            observe=self._observe,
        )

    @property
    def store(self) -> ScopeStore:
        return self._store

    def open_intake(self, scope: str, *, capacity: int = 20_000) -> Intake:
        """Own local input preparation and immediate work for an untouched scope."""
        if type(capacity) is not int or not 0 < capacity <= 9007199254740991:
            raise ValueError("attend: intake capacity must be a positive safe integer")
        if scope in self._intakes or self._scopes.has(scope) or any(owner == scope for owner, _ in self._attached):
            raise IntakeError("attend: scope already active")
        intake = Intake(
            scope,
            identity=self._intake_identity,
            scopes=self._scopes,
            clock=self._clock,
            classify=self._classify,
            running=lambda: self.running,
            observe=self._observe,
            capacity=capacity,
        )
        self._intakes[scope] = intake
        return intake

    @property
    def running(self) -> bool:
        """True from ``run()`` until it is stopped."""
        return self._running is not None

    @property
    def policy(self) -> SettlePolicy:
        return self._policy

    # ─── intake ──────────────────────────────────────────────────────────────

    def push(self, scope: str, events: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> asyncio.Future[None]:
        """Pushes one event or several. The returned future resolves once they are persisted.

        An event takes ``name`` and optionally ``id`` (default a random UUID; pass your own to drop
        redeliveries), ``at`` (default the clock), ``data`` (default ``{}``) and ``text``.
        """
        try:
            if scope in self._intakes:
                raise IntakeError("attend: scope has exclusive intake")
            return self._scopes.submit(scope, {"kind": "events", "events": self._classify(events)}, True)
        except Exception as error:
            return self._failed(error)

    def end(self, scope: str) -> asyncio.Future[None]:
        """Settles the open window at the next tick, reason ``closing``, with ``wait`` still offered."""
        if scope in self._intakes:
            return self._failed(IntakeError("attend: scope has exclusive intake"))
        return self._scopes.submit(scope, {"kind": "end"}, True)

    def flush(self, scope: str) -> asyncio.Future[None]:
        """Settles the open window at the next tick, reason ``forced``, with ``wait`` withheld."""
        if scope in self._intakes:
            return self._failed(IntakeError("attend: scope has exclusive intake"))
        return self._scopes.submit(scope, {"kind": "flush"}, True)

    async def inspect(self, scope: str) -> ScopeRecord | None:
        """The record as stored: load, replay and ``to_record``, then a deep copy."""
        return await read_record(self._store, scope, *self._modes)

    def decisions(
        self,
        *,
        signal: AbortSignal,
        cursor: str | None = None,
        max_age_ms: float | None = None,
        page_size: int = 100,
        scope: str | None = None,
        scope_prefix: str | None = None,
        routes: Sequence[DeliveredRoute] | None = None,
        kinds: Sequence[DecisionKind] | None = None,
    ) -> AsyncIterator[DecisionCursorPage]:
        """Reads the store's decision log in-process, with the same cursors MCP subscribers use."""
        return read_decisions(
            self._store.log,
            self._clock,
            signal=signal,
            cursor=cursor,
            max_age_ms=max_age_ms,
            page_size=page_size,
            scope=scope,
            scope_prefix=scope_prefix,
            routes=routes,
            kinds=kinds,
        )

    def _failed(self, error: BaseException) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        future.set_exception(error)
        future.add_done_callback(_consume)
        return future

    def _classify(self, given: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> list[KindedEvent]:
        if isinstance(given, Mapping):
            events: Sequence[Mapping[str, Any]] = [given]
        elif isinstance(given, (list, tuple)):
            events = given
        else:
            raise TypeError("attend: events must be an event or a list of events")
        fill = (lambda: str(uuid.uuid4()), self._clock.now)
        classified: list[KindedEvent] = []
        for event in events:
            copied = copy_event(event, fill)
            if "observedAt" in copied and copied["observedAt"] > self._clock.now():
                raise TypeError("attend: observedAt must not be in the future")
            kind = self._kind(copied)
            if kind not in KINDS:
                raise TypeError("attend: invalid event kind")
            if "quietForMs" in copied and kind != "continuous":
                raise TypeError("attend: quietForMs requires a continuous event")
            classified.append(cast(KindedEvent, {**copied, "kind": kind}))
        return classified

    # ─── effects ─────────────────────────────────────────────────────────────

    def _observe(self, trace: Trace) -> None:
        if self._observer is None:
            return
        with contextlib.suppress(Exception):
            self._observer(clone(trace) if trace.get("kind") in ("deadLettered", "dropped") else trace)

    def _on_due(self, scope: str) -> None:
        due = self._scopes.due(scope)
        if due is None:
            return
        if due > self._clock.now():
            self._scheduler.arm(scope, due)
        else:
            self._tick(scope)

    def _tick(self, scope: str) -> None:
        if (intake := self._intakes.get(scope)) is not None:
            intake.wake()
            return
        self._background(scope, "tick", self._scopes.submit(scope, {"kind": "tick"}))

    def _background(self, scope: str, input: InputKind, future: asyncio.Future[None]) -> asyncio.Future[None]:
        """Observes a failure of background work as a ``failed`` trace. The returned future never fails."""
        if (intake := self._intakes.get(scope)) is not None:
            intake.track_receipt(future)
        settled: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def done(f: asyncio.Future[None]) -> None:
            if not f.cancelled():
                error = f.exception()
                if error is not None and not isinstance(error, ConflictError):
                    self._observe({"kind": "failed", "scope": scope, "input": input, "message": message_of(error)})
            if not settled.done():
                settled.set_result(None)

        future.add_done_callback(done)
        return settled

    def _release(self, scope: str, release: Input) -> None:
        released = self._background(scope, release["kind"], self._scopes.submit(scope, release))
        self._releases.add(released)
        released.add_done_callback(self._releases.discard)

    def _call(self, scope: str, lease_until: float, work: Awaitable[None]) -> None:
        intake = self._intakes.get(scope)
        task = asyncio.ensure_future(intake.callback(work) if intake is not None else work)
        self._calls[task] = lease_until
        task.add_done_callback(lambda t: self._calls.pop(t, None))

    def _execute(self, scope: str, effect: Effect) -> None:
        intake = self._intakes.get(scope)
        if intake is not None and intake.closed and effect["kind"] != "trace":
            if effect["kind"] == "judge":
                self._release(scope, {"kind": "judgeReleased", "batchId": effect["batchId"], "attempt": effect["attempt"]})
            else:
                delivery = effect["delivery"]
                self._release(scope, {"kind": "deliveryReleased", "deliveryId": delivery["item"]["id"], "attempt": delivery["phase"]["attempt"]})
            return
        if effect["kind"] == "trace":
            self._observe(effect["trace"])
        elif effect["kind"] == "judge":
            if self._running is None:
                self._release(scope, {"kind": "judgeReleased", "batchId": effect["batchId"], "attempt": effect["attempt"]})
                return
            if self._judge_concurrency is not None and self._judges >= self._judge_concurrency:
                until = self._clock.now() + 100
                self._background(
                    scope, "judgeDeferred", self._scopes.submit(scope, {"kind": "judgeDeferred", "batchId": effect["batchId"], "attempt": effect["attempt"], "until": until})
                )
                return
            self._judges += 1
            controller, cancel = self._effect_controller(scope, self._clock.now() + self._policy["judgeTimeoutMs"], "attend: judge timed out")
            self._call(scope, effect["leaseUntil"], self._run_judge(scope, effect, controller, cancel))
        else:
            if self._running is None:
                self._release(scope, {"kind": "deliveryReleased", "deliveryId": effect["delivery"]["item"]["id"], "attempt": effect["delivery"]["phase"]["attempt"]})
                return
            controller, cancel = self._effect_controller(scope, effect["delivery"]["phase"]["leaseUntil"], "attend: delivery timed out")
            self._call(scope, effect["delivery"]["phase"]["leaseUntil"], self._run_deliver(scope, effect, controller, cancel))

    def _effect_controller(self, scope: str, deadline: float, message: str) -> tuple[AbortController, Callable[[], None]]:
        controller = AbortController()
        timer = self._clock.at(deadline, lambda: controller.abort(Exception(message)))
        intake = self._intakes.get(scope)
        remove = intake.completion_signal.add_listener(lambda: controller.abort(intake.completion_signal.reason)) if intake is not None else lambda: None

        def cancel() -> None:
            timer()
            remove()

        return controller, cancel

    async def _run_judge(self, scope: str, effect: JudgeEffect, controller: AbortController, cancel: Callable[[], None]) -> None:
        batch_id = effect["batchId"]
        attempt = effect["attempt"]
        signal = controller.signal
        result: JudgedInput | JudgeFailedInput | JudgeReleasedInput
        try:
            if scope in self._intakes:
                signal.throw_if_aborted()
            live: JudgeContext = await _maybe(self._context(scope, signal)) if self._context is not None else {}
            routes: list[OfferedRoute] = []
            for route in effect["offered"]:
                criterion = self._criteria.get(route)
                routes.append({"route": route} if criterion is None else {"route": route, "criterion": criterion})
            asked = {**self._predicates, **(live.get("predicates") or {})}
            input: JudgeInput = {
                "scope": scope,
                "window": await copy_window(effect["window"]),
                "routes": routes,
                "predicates": [{"name": name, "question": question} for name, question in asked.items()],
                "facts": live.get("facts") or {},
                "recent": clone(effect["recent"]),
                "now": self._clock.now(),
            }
            if self._guidance is not None:
                input["guidance"] = self._guidance
            if scope in self._intakes:
                signal.throw_if_aborted()
            images = await self._captured(scope, batch_id, signal) if self._capture is not None else []
            if images:
                input["images"] = images
            if scope in self._intakes:
                signal.throw_if_aborted()
            judgment: object = await self._judge(input, signal)
            if scope in self._intakes:
                signal.throw_if_aborted()
            if _is_judgment(judgment):
                threshold = None if self._act_threshold is None else threshold_route(judgment, effect["offered"], self._act_threshold)
                if threshold is not None:
                    self._observe({"kind": "routeThresholded", "scope": scope, "batchId": batch_id, "from": judgment["route"], "to": threshold["route"], "act": threshold["act"]})
                chosen = cast(Judgment, {**judgment, "route": threshold["route"]}) if threshold is not None else judgment
                result = {"kind": "judged", "batchId": batch_id, "attempt": attempt, "judgment": cast(Judgment, clone(chosen))}
            else:
                result = {"kind": "judgeFailed", "batchId": batch_id, "attempt": attempt, "message": "malformed judgment", "permanent": False}
        except asyncio.CancelledError:
            intake = self._intakes.get(scope)
            if intake is None or not intake.closed:
                raise
            result = {"kind": "judgeReleased", "batchId": batch_id, "attempt": attempt}
        except Exception as error:
            result = {"kind": "judgeFailed", "batchId": batch_id, "attempt": attempt, "message": message_of(error), "permanent": is_permanent(error)}
        finally:
            cancel()
            self._judges -= 1
        await self._background(scope, "judgeReleased" if result["kind"] == "judgeReleased" else "judged", self._submit_completion(scope, result))

    async def _captured(self, scope: str, batch_id: str, judge_signal: AbortSignal) -> list[JudgeImage]:
        capture = self._capture
        assert capture is not None
        controller = AbortController()
        remove = judge_signal.add_listener(lambda: controller.abort(judge_signal.reason))
        cancel = self._clock.at(self._clock.now() + self._capture_timeout_ms, lambda: controller.abort(Exception("attend: capture timed out")))
        try:
            result = await controller.signal.race(capture(scope, controller.signal))
            return [] if result is None else result if isinstance(result, list) else [result]
        except Exception as error:
            self._observe({"kind": "captureFailed", "scope": scope, "batchId": batch_id, "message": message_of(error)})
            return []
        finally:
            cancel()
            remove()

    async def _run_deliver(self, scope: str, effect: DeliverEffect, controller: AbortController, cancel: Callable[[], None]) -> None:
        item = effect["delivery"]["item"]
        attempt = effect["delivery"]["phase"]["attempt"]
        result: DeliveredInput | DeliveryFailedInput | DeliveryReleasedInput
        try:
            if scope in self._intakes:
                controller.signal.throw_if_aborted()
            sink = self._sink
            handler = self._dead_letter
            if item["kind"] == "decision" and not isinstance(sink, str):
                decision = await copy_decision(item["decision"])
                if scope in self._intakes:
                    controller.signal.throw_if_aborted()
                await _maybe(sink.deliver(decision, controller.signal))
            elif item["kind"] == "deadLetter" and not isinstance(handler, str):
                letter = await copy_letter(item["letter"])
                if scope in self._intakes:
                    controller.signal.throw_if_aborted()
                await _maybe(handler(letter, controller.signal))
            result = {"kind": "delivered", "deliveryId": item["id"], "attempt": attempt}
        except asyncio.CancelledError:
            intake = self._intakes.get(scope)
            if intake is None or not intake.closed:
                raise
            result = {"kind": "deliveryReleased", "deliveryId": item["id"], "attempt": attempt}
        except Exception as error:
            result = {"kind": "deliveryFailed", "deliveryId": item["id"], "attempt": attempt, "message": message_of(error), "permanent": is_permanent(error)}
        finally:
            cancel()
        await self._background(scope, "deliveryReleased" if result["kind"] == "deliveryReleased" else "delivered", self._submit_completion(scope, result))

    # ─── sources ─────────────────────────────────────────────────────────────

    def _submit_completion(self, scope: str, result: EffectResult) -> asyncio.Future[None]:
        if result["kind"] == "judgeReleased" or result["kind"] == "deliveryReleased":
            return self._scopes.submit(scope, result)
        intake = self._intakes.get(scope)
        if intake is None:
            return self._scopes.submit(scope, result)
        release: ReleasedInput
        if result["kind"] == "judged" or result["kind"] == "judgeFailed":
            release = {"kind": "judgeReleased", "batchId": result["batchId"], "attempt": result["attempt"]}
        else:
            release = {"kind": "deliveryReleased", "deliveryId": result["deliveryId"], "attempt": result["attempt"]}
        if intake.closed:
            return self._scopes.submit(scope, release)
        signal = intake.completion_signal
        pending = self._scopes.submit(scope, result, signal=signal)

        async def finish() -> None:
            try:
                await pending
            except BaseException as error:
                if not signal.aborted or error is not signal.reason:
                    raise
                await self._scopes.submit(scope, release)

        return asyncio.create_task(finish())

    async def attach(self, scope: str, source: Source, *, signal: AbortSignal | None = None) -> None:
        """Subscribes ``scope`` to ``source`` from its persisted cursor, and resubscribes after a backoff until stopped."""
        if scope in self._intakes:
            raise IntakeError("attend: scope has exclusive intake")
        slot = (scope, source.key)
        if slot in self._attached:
            raise RuntimeError(f'attend: source "{source.key}" is already attached to scope "{scope}"')
        self._attached.add(slot)
        controller = AbortController()
        remove = signal.add_listener(controller.abort) if signal is not None else (lambda: None)
        self._sources.add(controller)
        failures = 0
        try:
            while not controller.signal.aborted:
                subscription = AbortController()
                combined = AbortSignal.any([controller.signal, subscription.signal])
                pending: list[asyncio.Future[None]] = []

                async def write(batch: SourceBatch, combined: AbortSignal = combined, subscription: AbortController = subscription) -> None:
                    nonlocal failures
                    try:
                        combined.throw_if_aborted()
                        input: EventsInput = {"kind": "events", "events": self._classify(batch["events"]), "source": source.key}
                        if "cursor" in batch:
                            input["cursor"] = batch["cursor"]
                        if batch.get("truncated"):
                            input["truncated"] = True
                        await self._scopes.submit(scope, input, True, combined, subscription.abort)
                        if not combined.aborted:
                            failures = 0
                    except Exception as error:
                        subscription.abort(error)
                        raise

                def emit(batch: SourceBatch, write: Callable[[SourceBatch], Awaitable[None]] = write, pending: list[asyncio.Future[None]] = pending) -> Awaitable[None]:
                    future = asyncio.ensure_future(write(batch))
                    future.add_done_callback(_consume)
                    pending[:] = [future]
                    return future

                try:
                    record = await read_record(self._store, scope, *self._modes)
                    cursor = record["cursors"].get(source.key) if record is not None else None
                    controller.signal.throw_if_aborted()
                    await source.subscribe(cursor, emit, combined)
                    if pending:
                        await pending[0]
                    combined.throw_if_aborted()
                    return
                except Exception as error:
                    if controller.signal.aborted:
                        return
                    failure = subscription.signal.reason if subscription.signal.aborted and subscription.signal.reason is not None else error
                    subscription.abort(failure)
                    if is_permanent(failure):
                        raise failure from None
                    failures += 1
                    rungs = self._policy["retryBackoffMs"]
                    index = min(failures, len(rungs)) - 1
                    delay = rungs[index] if 0 <= index < len(rungs) else self._policy["deliveryTimeoutMs"]
                    self._observe(
                        {
                            "kind": "retrying",
                            "scope": scope,
                            "stage": "source",
                            "id": source.key,
                            "attempt": failures,
                            "at": self._clock.now() + delay,
                            "message": message_of(failure),
                        }
                    )
                    await sleep(self._clock, delay, controller.signal)
                finally:
                    subscription.abort()
        finally:
            self._attached.discard(slot)
            self._sources.discard(controller)
            remove()

    # ─── runner ──────────────────────────────────────────────────────────────

    def run(self, *, signal: AbortSignal | None = None, sweep_ms: float = 1000, sweep_limit: int = 100) -> Coroutine[Any, Any, None]:
        """Settles, judges and delivers until ``signal`` aborts or ``stop()`` is called, then waits for work in flight.

        The run starts when called, as in TypeScript: ``stop()`` straight after
        ``asyncio.create_task(attend.run())`` stops it. Await the returned coroutine, or run it as a
        task; cancelling that task stops the run the same way.
        """
        if self._running is not None or (self._finished is not None and not self._finished.done()):
            raise RuntimeError("attend: already running")
        own = AbortController()
        self._stopper = own
        combined = AbortSignal.any([signal, own.signal]) if signal is not None else own.signal
        finished: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._finished = finished
        self._running = combined
        return self._run(combined, own, finished, sweep_ms, sweep_limit)

    async def _run(self, signal: AbortSignal, own: AbortController, finished: asyncio.Future[None], sweep_ms: float, sweep_limit: int) -> None:
        try:
            await self._run_loop(signal, sweep_ms, sweep_limit)
        finally:
            own.abort()
            try:
                await self._join_shutdown()
            finally:
                if not finished.done():
                    finished.set_result(None)

    async def stop(self) -> None:
        """Stops the run, whether or not it was given a signal, and returns when it has finished."""
        if owns_callback(self._intake_identity):
            raise IntakeError("attend: cannot stop an engine from its own callback")
        if self._stopper is not None:
            self._stopper.abort()
        if self._finished is not None:
            await asyncio.shield(self._finished)

    async def _sweep(self, now: float, limit: int, warm: bool) -> None:
        try:
            after = None
            while self.running:
                page = await self._store.due(now, limit, after)
                for key in page:
                    if not self.running:
                        break
                    due = await self._scopes.touch(key["scope"])
                    if not warm or (due is not None and due <= self._clock.now()):
                        self._tick(key["scope"])
                if len(page) < limit or not page:
                    break
                after = page[-1]
        except Exception as error:
            self._observe({"kind": "failed", "scope": "*", "input": "tick", "message": message_of(error)})

    def _on_abort(self) -> None:
        for intake in self._intakes.values():
            intake.fence()
        self._running = None
        self._scheduler.stop()
        for source in list(self._sources):
            source.abort()

    async def _run_loop(self, signal: AbortSignal, sweep_ms: float, sweep_limit: int) -> None:
        self._running = signal
        remove = signal.add_listener(self._on_abort)
        try:
            if signal.aborted:
                return
            self._scopes.arm_all()
            await self._sweep(9007199254740991, sweep_limit, True)
            while not signal.aborted:
                await self._sweep(self._clock.now(), sweep_limit, False)
                self._scopes.evict()
                await sleep(self._clock, sweep_ms, signal)
        finally:
            self._on_abort()
            remove()

    async def _join_shutdown(self) -> None:
        cleanup = asyncio.create_task(self._shutdown())
        interrupted: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as error:
                interrupted = error
        cleanup.result()
        if interrupted is not None:
            raise interrupted

    async def _shutdown(self) -> None:
        writes = self._scopes.pending()
        await asyncio.gather(*(self._settled_or_expired(task, lease) for task, lease in list(self._calls.items())))
        await writes
        while self._releases:
            await asyncio.wait(list(self._releases))
        for task in list(self._calls):
            if not task.done():
                task.cancel()
        await asyncio.gather(*(asyncio.shield(intake.fence()) for intake in self._intakes.values()), return_exceptions=True)

    async def _settled_or_expired(self, task: asyncio.Task[None], lease_until: float) -> None:
        if lease_until <= self._clock.now() or task.done():
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def wake(_: object = None) -> None:
            if not future.done():
                future.set_result(None)

        cancel = self._clock.at(lease_until, wake)
        task.add_done_callback(wake)
        try:
            await future
        finally:
            cancel()
            task.remove_done_callback(wake)


class _FunctionSink:
    def __init__(self, deliver: SinkFunction) -> None:
        self._deliver = deliver

    def deliver(self, decision: Decision, signal: AbortSignal) -> MaybeAwaitable[None]:
        return self._deliver(decision, signal)


def _kind_function(kind: Callable[[Event], Kind] | Mapping[str, Kind]) -> Callable[[Event], Kind]:
    if callable(kind):
        return kind
    if not isinstance(kind, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError("attend: kind must be a function or a map of event name to kind")
    table: dict[str, Kind] = {}
    for name, value in kind.items():
        if value not in KINDS:
            raise ValueError(f'attend: the kind for "{name}" must be one of {", ".join(KINDS)}')
        table[name] = value
    return lambda event: table.get(event["name"], "consequence")


def _is_judgment(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    judgment = cast(dict[str, object], value)
    if judgment.get("route") not in ROUTES:
        return False
    return all(not (name in judgment and not isinstance(judgment[name], (dict, list))) for name in ("probabilities", "predicates"))


def create_attend(
    *,
    kind: Callable[[Event], Kind] | Mapping[str, Kind],
    judge: Judge,
    sink: Sink | SinkFunction | Literal["log"],
    store: ScopeStore | None = None,
    dead_letter: DeadLetterHandler | Literal["log"] = "log",
    settle: Mapping[str, Any] | None = None,
    window_share: float = 0.5,
    act_threshold: float | None = None,
    guidance: str | None = None,
    criteria: Mapping[Route, str] | None = None,
    predicates: Mapping[str, str] | None = None,
    context: ContextFunction | None = None,
    capture: CaptureFunction | None = None,
    capture_timeout_ms: float = 500,
    observe: Callable[[Trace], None] | None = None,
    clock: Clock | None = None,
    max_queued_inputs: int = 20_000,
    max_lag_ms: float | None = None,
    judge_concurrency: int | None = None,
    cache_idle_ms: float = 300_000,
) -> Attend:
    """Creates attend over a store (default the memory store). See ``docs/reference.md`` for every setting.

    ``kind`` maps each event to its settle role: a function, or a map from event name to kind
    that gives every other name ``consequence``. ``sink`` is a callable or an object with
    ``deliver``, each taking ``(decision, signal)``, or ``"log"`` to make the decision row the
    delivery. ``settle`` overrides the default policy with snake_case names, such as
    ``{"closing_grace_ms": 250}``.
    """
    return Attend(
        kind=kind,
        judge=judge,
        sink=sink,
        store=store,
        dead_letter=dead_letter,
        settle=settle,
        window_share=window_share,
        act_threshold=act_threshold,
        guidance=guidance,
        criteria=criteria,
        predicates=predicates,
        context=context,
        capture=capture,
        capture_timeout_ms=capture_timeout_ms,
        observe=observe,
        clock=clock,
        max_queued_inputs=max_queued_inputs,
        max_lag_ms=max_lag_ms,
        judge_concurrency=judge_concurrency,
        cache_idle_ms=cache_idle_ms,
    )


type RouteHandler = Callable[[Decision, AbortSignal], MaybeAwaitable[None]]


class _RouteSink:
    def __init__(self, handlers: Mapping[DeliveredRoute, RouteHandler]) -> None:
        self._handlers = handlers

    def deliver(self, decision: Decision, signal: AbortSignal) -> MaybeAwaitable[None]:
        return self._handlers[decision["route"]](decision, signal)


def routes(*, interrupt: RouteHandler, queue: RouteHandler, note: RouteHandler, ask: RouteHandler) -> Sink:
    """A sink with a required handler for each delivered route."""
    return _RouteSink({"interrupt": interrupt, "queue": queue, "note": note, "ask": ask})
