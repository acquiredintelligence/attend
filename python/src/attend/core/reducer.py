"""The in-place reducer: one logged input applied to owned state, deterministic and free of I/O."""

from __future__ import annotations

import math
from typing import TypedDict, cast

from .ids import fnv_hex, fnv_start, fold_id
from .seen import seen_admit, seen_release, seen_trim
from .types import (
    ROUTES,
    ApplyConfig,
    ApplyOutput,
    DeadLetter,
    DecidedRoute,
    Decision,
    DeliveredRoute,
    Delivery,
    Entry,
    EventsInput,
    FnvState,
    HeldBatch,
    InFlightDelivery,
    InputKind,
    JudgedInput,
    JudgingPhase,
    Judgment,
    LoggedInput,
    OutboxLane,
    Route,
    ScopeState,
    SettlePolicy,
    SettleReason,
    Trace,
    Verdict,
    Window,
)
from .window import (
    estimate_rendered_bytes,
    open_window,
    shed_entries,
    window_append,
    window_deadline,
    window_prune,
    window_shed,
)

REDUCER_VERSION = 1
"""Bumped whenever a release changes a transition. Stored with every snapshot."""

_LANES: tuple[OutboxLane, OutboxLane] = ("decisions", "deadLetters")


class Validated(TypedDict):
    """A judgment after ``validate``: an offered route and normalised probabilities."""

    route: Route
    probabilities: dict[str, float]
    predicates: dict[str, float]
    confidence: float | None


def same(a: object, b: object) -> bool:
    """``Object.is``: NaN equals NaN, and 0 and -0 differ."""
    if a is None or b is None:
        return a is b
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        x, y = float(a), float(b)
        if x != x:
            return y != y
        if x == 0.0 and y == 0.0:
            return math.copysign(1.0, x) == math.copysign(1.0, y)
        return x == y
    return a == b


def _lane(state: ScopeState, lane: OutboxLane) -> list[Delivery]:
    return state.decisions if lane == "decisions" else state.dead_letters


class _Ctx:
    __slots__ = ("config", "now", "out")

    def __init__(self, config: ApplyConfig, now: float, out: ApplyOutput) -> None:
        self.config = config
        self.now = now
        self.out = out

    def trace(self, trace: Trace) -> None:
        if self.config.traces:
            self.out.effects.append({"kind": "trace", "trace": trace})


def apply(config: ApplyConfig, state: ScopeState, logged: LoggedInput, out: ApplyOutput) -> None:
    """Applies one logged input to ``state`` in place.

    Deterministic in (state, logged, config). Pushes effects and decision rows onto ``out``, and
    sets ``out.changed`` if anything changed. Never reads a clock and never calls app code.
    """
    now = logged["now"]
    ctx = _Ctx(config, now, out)
    input = logged["input"]
    kind = input["kind"]
    swept = False
    if input["kind"] == "events":
        _ingest(ctx, state, input)
    elif kind == "flush":
        if not state.open.entries and state.batch is None:
            ctx.trace({"kind": "stale", "scope": state.scope, "input": "flush", "why": "nothing to settle"})
        elif not same(state.open.forced_at, now):
            state.open.forced_at = now
            out.changed = True
    elif kind == "end":
        if not state.open.entries:
            ctx.trace({"kind": "stale", "scope": state.scope, "input": "end", "why": "nothing to settle"})
        elif not same(state.open.ended_at, now):
            state.open.ended_at = now
            out.changed = True
    elif input["kind"] == "judgeReleased" or input["kind"] == "judgeDeferred":
        b = _live_batch(ctx, state, input["batchId"], input["attempt"], input["kind"])
        if b is not None:
            phase = cast(JudgingPhase, b.phase)
            deferred = input["kind"] == "judgeDeferred"
            state.batch = HeldBatch(
                b.window,
                {
                    "phase": "retrying",
                    "attempt": phase["attempt"] - 1,
                    "failures": phase["failures"],
                    "at": input["until"] if input["kind"] == "judgeDeferred" else now,
                    "offerWait": phase["offerWait"],
                },
                b.hash,
            )
            out.changed = True
            if deferred:
                ctx.trace(
                    {
                        "kind": "deferred",
                        "scope": state.scope,
                        "batchId": input["batchId"],
                        "attempt": input["attempt"],
                        "until": input["until"] if input["kind"] == "judgeDeferred" else now,
                    }
                )
            else:
                ctx.trace({"kind": "released", "scope": state.scope, "stage": "judge", "id": b.window["id"], "attempt": phase["attempt"]})
    elif input["kind"] == "judged":
        _on_judged(ctx, state, input)
        swept = True
    elif input["kind"] == "judgeFailed":
        b = _live_batch(ctx, state, input["batchId"], input["attempt"], "judgeFailed")
        if b is not None:
            _fail_judge(ctx, state, b, input["message"], input["permanent"])
        swept = True
    elif input["kind"] == "delivered" or input["kind"] == "deliveryFailed" or input["kind"] == "deliveryReleased":
        lane = _live_delivery(ctx, state, input["deliveryId"], input["attempt"], input["kind"])
        if lane is not None:
            if input["kind"] == "deliveryFailed":
                _fail_delivery(ctx, state, lane, input["message"], input["permanent"])
            elif input["kind"] == "delivered":
                del _lane(state, lane)[0]
                out.changed = True
                ctx.trace({"kind": "delivered", "scope": state.scope, "deliveryId": input["deliveryId"]})
            else:
                queue = _lane(state, lane)
                d = queue[0]
                if d["phase"]["phase"] == "inFlight":
                    attempt = d["phase"]["attempt"]
                    queue[0] = {"item": d["item"], "phase": {"phase": "retrying", "attempt": attempt - 1, "failures": d["phase"]["failures"], "at": now}}
                    out.changed = True
                    ctx.trace(
                        {
                            "kind": "released",
                            "scope": state.scope,
                            "stage": "sink" if d["item"]["kind"] == "decision" else "deadLetter",
                            "id": d["item"]["id"],
                            "attempt": attempt,
                        }
                    )
        swept = input["kind"] != "deliveryReleased"
    elif kind == "tick":
        swept = True
    else:
        raise ValueError(f"attend: unknown input kind {kind!r}")
    if swept:
        _sweep(ctx, state)
    due = next_due(config.policy, state)
    if not same(state.due_at, due):
        state.due_at = due
        out.changed = True


def _removed(ctx: _Ctx, state: ScopeState, entries: list[Entry] | None, why: str) -> None:
    if entries is None:
        return
    for entry in entries:
        seen_release(state.known, entry["id"])
    ctx.out.changed = True
    ctx.trace({"kind": "dropped", "scope": state.scope, "entries": entries, "why": why})


def _prune(ctx: _Ctx, state: ScopeState) -> None:
    _removed(ctx, state, window_prune(state.open, ctx.config.policy["maxWindowMs"], ctx.now), "age")
    if not state.open.entries:
        state.open.ended_at = None


def _ingest(ctx: _Ctx, state: ScopeState, input: EventsInput) -> None:
    duplicates = 0
    policy = ctx.config.policy
    for event in input["events"]:
        if not seen_admit(state, event["id"]):
            duplicates += 1
            continue
        entry = cast(Entry, {**event, "seenAt": ctx.now})
        window_append(state.open, entry, estimate_rendered_bytes(entry))
        ctx.out.changed = True
    if state.seen.size > max(0, policy["seenCount"]):
        ctx.out.changed = True
    seen_trim(state, policy["seenCount"])
    _prune(ctx, state)
    _removed(ctx, state, window_shed(state.open, policy["maxEvents"]), "overflow")
    if not state.open.entries:
        state.open.ended_at = None
    source = input.get("source")
    if source is not None and "cursor" in input:
        cursor = input["cursor"]
        if source not in state.cursors or not same(state.cursors[source], cursor):
            state.cursors = {**state.cursors, source: cursor}
            ctx.out.changed = True
    if input.get("truncated"):
        if not state.open.gap:
            state.open.gap = True
            ctx.out.changed = True
        ctx.trace({"kind": "gap", "scope": state.scope, "source": source, "cursor": None if source is None else state.cursors.get(source)})
    ctx.trace({"kind": "ingested", "scope": state.scope, "count": len(input["events"]) - duplicates, "duplicates": duplicates})


def next_due(policy: SettlePolicy, state: ScopeState) -> float | None:
    """The instant the state next wants a tick: the batch's time, the open window's deadline unless judging, and each lane head's time."""
    at: float | None = None
    phase = state.batch.phase if state.batch is not None else None
    if phase is not None:
        if phase["phase"] == "judging":
            at = phase["leaseUntil"]
        elif phase["phase"] == "waiting":
            at = phase["until"]
        else:
            at = phase["at"]
    if phase is None or phase["phase"] != "judging":
        due = window_deadline(policy, state.open)
        if due is not None:
            at = due[0] if at is None else _min(at, due[0])
    for queue in (state.decisions, state.dead_letters):
        if queue:
            head = queue[0]["phase"]
            time = head["leaseUntil"] if head["phase"] == "inFlight" else head["at"]
            at = time if at is None else _min(at, time)
    return at


def _min(a: float, b: float) -> float:
    if a != a or b != b:
        return math.nan
    return a if a <= b else b


def _sweep(ctx: _Ctx, state: ScopeState) -> None:
    b = state.batch
    if b is not None and b.phase["phase"] == "judging" and b.phase["leaseUntil"] <= ctx.now:
        _fail_judge(ctx, state, b, "judge lease expired", False)
    for lane in _LANES:
        _pump_lane(ctx, state, lane)
    _prune(ctx, state)
    if state.batch is None or state.batch.phase["phase"] != "judging":
        due = window_deadline(ctx.config.policy, state.open)
        if due is not None and not (due[0] > ctx.now):
            _settle_buffer(ctx, state, due[1], True)
    held = state.batch
    if held is None:
        return
    phase = held.phase
    if phase["phase"] == "retrying" and phase["at"] <= ctx.now:
        _begin_judging(ctx, state, held.window, phase["attempt"], phase["failures"], phase["offerWait"], held.hash)
    elif phase["phase"] == "waiting" and phase["until"] <= ctx.now:
        if not state.open.entries:
            _begin_judging(ctx, state, held.window, phase["attempt"], 0, False, held.hash)
        else:
            _settle_buffer(ctx, state, held.window["reason"], False)


def _settle_reason(events: list[Entry], forced: bool, fired: SettleReason) -> SettleReason:
    if any(entry["kind"] == "flush" for entry in events):
        return "flush"
    return "forced" if forced else fired


def _settle_buffer(ctx: _Ctx, state: ScopeState, fired: SettleReason, may_wait: bool) -> None:
    held = state.batch
    open = state.open
    entries = open.entries
    forced = open.forced_at is not None
    if not entries:
        if open.forced_at is not None:
            open.forced_at = None
            ctx.out.changed = True
        if held is not None:
            window = cast(Window, {**held.window, "reason": _settle_reason(held.window["events"], forced, held.window["reason"])})
            attempt, failures = _counters(held)
            _begin_judging(ctx, state, window, attempt, failures, False, held.hash)
        return
    events = [*held.window["events"], *entries] if held is not None else entries
    dropped = open.dropped + (held.window["dropped"] if held is not None else 0)
    gap = open.gap or (held.window["gap"] if held is not None else False)
    waits = held.window["waits"] if held is not None else 0
    forced = forced or (held is not None and held.window["reason"] == "forced")
    gone = shed_entries(events, ctx.config.policy["maxEvents"])
    _removed(ctx, state, gone, "overflow")
    dropped += len(gone) if gone is not None else 0
    if held is None and gone is None and open.hash_valid:
        hash = open.hash
    elif held is not None and held.hash is not None and gone is None:
        hash = FnvState(held.hash.value)
        for entry in entries:
            fold_id(hash, entry["id"])
    else:
        hash = fnv_start(state.scope)
        for entry in events:
            fold_id(hash, entry["id"])
    reason = _settle_reason(events, forced, fired)
    window: Window = {"id": fnv_hex(hash), "events": events, "reason": reason, "waits": waits, "dropped": dropped, "gap": gap, "settledAt": ctx.now}
    state.open = open_window(state.scope)
    ctx.trace({"kind": "settled", "scope": state.scope, "batchId": window["id"], "reason": reason, "count": len(events), "merged": held is not None})
    attempt, failures = _counters(held) if held is not None else (0, 0)
    offer = may_wait and reason != "flush" and reason != "forced" and waits < ctx.config.policy["maxWaits"]
    _begin_judging(ctx, state, window, attempt, failures, offer, hash)


def _counters(held: HeldBatch) -> tuple[int, int]:
    phase = held.phase
    return phase["attempt"], (0 if phase["phase"] == "waiting" else phase["failures"])


def _begin_judging(ctx: _Ctx, state: ScopeState, window: Window, prior_attempt: int, failures: int, offer_wait: bool, hash: FnvState | None) -> None:
    attempt = prior_attempt + 1
    lease_until = ctx.now + ctx.config.policy["judgeTimeoutMs"] * 2
    state.batch = HeldBatch(window, {"phase": "judging", "attempt": attempt, "failures": failures, "leaseUntil": lease_until, "offerWait": offer_wait}, hash)
    ctx.out.changed = True
    ctx.out.effects.append(
        {
            "kind": "judge",
            "scope": state.scope,
            "batchId": window["id"],
            "attempt": attempt,
            "window": window,
            "offered": offered_routes(offer_wait),
            "recent": state.recent,
            "leaseUntil": lease_until,
        }
    )
    ctx.trace({"kind": "judging", "scope": state.scope, "batchId": window["id"], "attempt": attempt, "offerWait": offer_wait})


def offered_routes(offer_wait: bool) -> list[Route]:
    """Every route, with ``wait`` only when it is offered."""
    return [route for route in ROUTES if offer_wait or route != "wait"]


def _live_batch(ctx: _Ctx, state: ScopeState, batch_id: str, attempt: int, input: InputKind) -> HeldBatch | None:
    b = state.batch
    if b is not None and b.phase["phase"] == "judging" and b.window["id"] == batch_id and b.phase["attempt"] == attempt:
        return b
    ctx.trace({"kind": "stale", "scope": state.scope, "input": input, "why": f"no live judge attempt {_num(attempt)} for batch {batch_id}"})
    return None


def _num(value: float) -> str:
    from .._json import number

    return number(value)


def _release_held(state: ScopeState) -> None:
    if state.batch is not None:
        for entry in state.batch.window["events"]:
            seen_release(state.known, entry["id"])
    state.batch = None


def _on_judged(ctx: _Ctx, state: ScopeState, input: JudgedInput) -> None:
    b = _live_batch(ctx, state, input["batchId"], input["attempt"], "judged")
    if b is None:
        return
    phase = cast(JudgingPhase, b.phase)
    verdict = validate(input["judgment"], offered_routes(phase["offerWait"]))
    if verdict is None:
        judgment = input["judgment"]
        _fail_judge(ctx, state, b, f"route not offered: {_display(judgment['route']) if 'route' in judgment else 'undefined'}", False)
        return
    window = b.window
    ctx.out.changed = True
    route = verdict["route"]
    if route == "wait":
        until = ctx.now + ctx.config.policy["waitHorizonMs"]
        waits = window["waits"] + 1
        state.batch = HeldBatch(cast(Window, {**window, "waits": waits}), {"phase": "waiting", "attempt": phase["attempt"], "until": until}, b.hash)
        ctx.trace({"kind": "waiting", "scope": state.scope, "batchId": window["id"], "waits": waits, "until": until})
        return
    decided: DecidedRoute = route
    verdict_row: Verdict = {"id": window["id"], "route": decided, "at": ctx.now}
    recent = [*state.recent, verdict_row]
    count = ctx.config.policy["recentCount"]
    state.recent = [] if count <= 0 else recent[-count:]
    ctx.trace({"kind": "judged", "scope": state.scope, "batchId": window["id"], "route": decided})
    if decided != "skip":
        delivered: DeliveredRoute = decided
        decision: Decision = {
            "id": window["id"],
            "scope": state.scope,
            "route": delivered,
            "probabilities": verdict["probabilities"],
            "predicates": verdict["predicates"],
            "confidence": verdict["confidence"],
            "window": window,
            "at": ctx.now,
        }
        ctx.out.appends.append({"kind": "decision", "id": decision["id"], "scope": state.scope, "at": decision["at"], "route": delivered, "body": decision})
        if ctx.config.decisions == "lane":
            state.decisions.append(
                {"item": {"id": window["id"], "kind": "decision", "decision": decision}, "phase": {"phase": "retrying", "attempt": 0, "failures": 0, "at": ctx.now}}
            )
    _release_held(state)


def _display(value: object) -> str:
    """``String(value)`` for what a judge put in ``route``."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _num(value)
    return str(value)


def _unit(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) and 0 <= value <= 1 else None


def validate(judgment: Judgment, offered: list[Route]) -> Validated | None:
    """Validates the judge's answer at the boundary.

    The route must be offered, or the answer is None. Probabilities outside [0, 1] or for routes
    not offered count as 0 and the vector is renormalised; when it sums to 0 the chosen route gets 1.
    Predicates and a confidence outside [0, 1] are dropped.
    """
    route: Route | None = next((candidate for candidate in offered if candidate == judgment.get("route")), None)
    if route is None:
        return None
    raw: dict[str, float] = {"interrupt": 0, "queue": 0, "note": 0, "ask": 0, "skip": 0, "wait": 0}
    total = 0.0
    given = judgment.get("probabilities")
    for candidate in offered:
        p = _unit(given.get(candidate)) if isinstance(given, dict) else None
        if p is not None:
            raw[candidate] = p
            total += p
    probabilities = dict(raw)
    if total > 0:
        for candidate in ROUTES:
            probabilities[candidate] = raw[candidate] / total
    else:
        probabilities[route] = 1
    predicates: dict[str, float] = {}
    asked = judgment.get("predicates")
    if isinstance(asked, dict):
        for name, value in asked.items():
            p = _unit(value)
            if p is not None:
                predicates[name] = p
    result: Validated = {"route": route, "probabilities": probabilities, "predicates": predicates, "confidence": _unit(judgment.get("confidence"))}
    return result


def _dead_letter(ctx: _Ctx, state: ScopeState, letter: DeadLetter, attempt: int) -> None:
    decision = letter["decision"]
    ctx.out.appends.append(
        {"kind": "deadLetter", "id": letter["id"], "scope": state.scope, "at": letter["at"], "route": decision["route"] if decision is not None else None, "body": letter}
    )
    if ctx.config.dead_letters == "lane":
        state.dead_letters.append(
            {"item": {"id": letter["id"], "kind": "deadLetter", "letter": letter}, "phase": {"phase": "retrying", "attempt": attempt, "failures": 0, "at": ctx.now}}
        )
    ctx.trace({"kind": "deadLettered", "scope": state.scope, "letter": letter})
    ctx.out.changed = True


def _fail_judge(ctx: _Ctx, state: ScopeState, b: HeldBatch, message: str, permanent: bool) -> None:
    window = b.window
    phase = cast(JudgingPhase, b.phase)
    failures = phase["failures"] + 1
    backoff = ctx.config.policy["retryBackoffMs"]
    delay = None if permanent or failures - 1 >= len(backoff) else backoff[failures - 1]
    ctx.out.changed = True
    if delay is not None:
        at = ctx.now + delay
        state.batch = HeldBatch(window, {"phase": "retrying", "attempt": phase["attempt"], "failures": failures, "at": at, "offerWait": phase["offerWait"]}, b.hash)
        ctx.trace({"kind": "retrying", "scope": state.scope, "stage": "judge", "id": window["id"], "attempt": phase["attempt"], "at": at, "message": message})
        return
    letter: DeadLetter = {
        "id": window["id"],
        "scope": state.scope,
        "stage": "judge",
        "window": window,
        "decision": None,
        "message": message,
        "permanent": permanent,
        "attempts": phase["attempt"],
        "at": ctx.now,
    }
    _dead_letter(ctx, state, letter, 0)
    _release_held(state)


def _live_delivery(ctx: _Ctx, state: ScopeState, delivery_id: str, attempt: int, input: InputKind) -> OutboxLane | None:
    for lane in _LANES:
        queue = _lane(state, lane)
        if queue:
            head = queue[0]
            if head["item"]["id"] == delivery_id and head["phase"]["phase"] == "inFlight" and head["phase"]["attempt"] == attempt:
                return lane
    ctx.trace({"kind": "stale", "scope": state.scope, "input": input, "why": f"no live delivery attempt {_num(attempt)} for {delivery_id}"})
    return None


def _dead_letter_delay(policy: SettlePolicy, failures: int) -> float:
    rungs = policy["retryBackoffMs"]
    cap = policy["deadLetterBackoffMaxMs"]
    index = min(failures, len(rungs)) - 1
    base = rungs[index] if 0 <= index < len(rungs) else cap
    return _min(cap, base * 2 ** max(0, failures - len(rungs)))


def _fail_delivery(ctx: _Ctx, state: ScopeState, lane: OutboxLane, message: str, permanent: bool) -> None:
    queue = _lane(state, lane)
    if not queue:
        return
    d = queue[0]
    phase = d["phase"]
    if phase["phase"] != "inFlight":
        return
    item = d["item"]
    attempt = phase["attempt"]
    failures = phase["failures"] + 1
    ctx.out.changed = True
    backoff = ctx.config.policy["retryBackoffMs"]
    if item["kind"] == "deadLetter":
        delay: float | None = _dead_letter_delay(ctx.config.policy, failures)
    else:
        delay = None if permanent or failures - 1 >= len(backoff) else backoff[failures - 1]
    if delay is not None:
        at = ctx.now + delay
        queue[0] = {"item": item, "phase": {"phase": "retrying", "attempt": attempt, "failures": failures, "at": at}}
        ctx.trace(
            {
                "kind": "retrying",
                "scope": state.scope,
                "stage": "deadLetter" if item["kind"] == "deadLetter" else "sink",
                "id": item["id"],
                "attempt": attempt,
                "at": at,
                "message": message,
            }
        )
        return
    if item["kind"] == "decision":
        del queue[0]
        decision = item["decision"]
        letter: DeadLetter = {
            "id": item["id"],
            "scope": state.scope,
            "stage": "sink",
            "window": decision["window"],
            "decision": decision,
            "message": message,
            "permanent": permanent,
            "attempts": attempt,
            "at": ctx.now,
        }
        _dead_letter(ctx, state, letter, attempt)


def _pump_lane(ctx: _Ctx, state: ScopeState, lane: OutboxLane) -> None:
    queue = _lane(state, lane)
    if not queue:
        return
    head = queue[0]
    if head["phase"]["phase"] == "inFlight":
        if head["phase"]["leaseUntil"] > ctx.now:
            return
        _fail_delivery(ctx, state, lane, "delivery lease expired", False)
    if not queue:
        return
    nxt = queue[0]
    phase = nxt["phase"]
    if phase["phase"] != "retrying" or phase["at"] > ctx.now:
        return
    attempt = phase["attempt"] + 1
    in_flight: InFlightDelivery = {
        "item": nxt["item"],
        "phase": {"phase": "inFlight", "attempt": attempt, "failures": phase["failures"], "leaseUntil": ctx.now + ctx.config.policy["deliveryTimeoutMs"]},
    }
    queue[0] = cast(Delivery, in_flight)
    ctx.out.changed = True
    ctx.out.effects.append({"kind": "deliver", "scope": state.scope, "delivery": in_flight})
    ctx.trace({"kind": "delivering", "scope": state.scope, "deliveryId": nxt["item"]["id"], "attempt": attempt})
