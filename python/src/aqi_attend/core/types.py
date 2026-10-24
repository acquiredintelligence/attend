"""The domain, and the owned state the in-place reducer mutates.

Everything but ``ScopeState`` and its parts is plain JSON: a ``ScopeRecord`` is a scope's state as
a snapshot. These are dicts with the same field names as the TypeScript package, so a Python
process and a TypeScript process read and write one store the same way.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Literal, NotRequired, TypedDict

type JsonValue = str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None
type JsonObject = dict[str, JsonValue]

type Kind = Literal["closing", "flush", "continuous", "passive", "consequence", "context"]
KINDS: tuple[Kind, ...] = ("closing", "flush", "continuous", "passive", "consequence", "context")

type Route = Literal["interrupt", "queue", "note", "ask", "skip", "wait"]
ROUTES: tuple[Route, ...] = ("interrupt", "queue", "note", "ask", "skip", "wait")
type DecidedRoute = Literal["interrupt", "queue", "note", "ask", "skip"]
type DeliveredRoute = Literal["interrupt", "queue", "note", "ask"]

type SettleReason = Literal["closing", "flush", "stream_quiet", "passive_quiet", "overflow", "age", "forced", "size"]


class Event(TypedDict):
    """One event as a source hands it in. ``id`` is unique within the scope and is the dedup key."""

    id: str
    at: float
    name: str
    data: JsonObject
    text: NotRequired[str]


class KindedEvent(Event):
    kind: Kind


class Entry(KindedEvent):
    """An event in a window: the event, the kind it was given at ingest, and the local clock then."""

    seenAt: float


class SettlePolicy(TypedDict):
    """Round starting points, not tuned values. Every number is in ms unless it is a count."""

    closingGraceMs: float
    streamGapMs: float
    passiveQuietMs: float
    waitHorizonMs: float
    maxWaits: int
    maxEvents: int
    maxWindowMs: float
    retryBackoffMs: list[float]
    judgeTimeoutMs: float
    deliveryTimeoutMs: float
    recentCount: int
    seenCount: int
    maxOutbox: int
    maxDeadLetters: int
    deadLetterBackoffMaxMs: float
    maxWindowBytes: NotRequired[float]


def default_policy() -> SettlePolicy:
    """A fresh copy of the defaults in ``docs/reference.md``."""
    return {
        "closingGraceMs": 500,
        "streamGapMs": 1000,
        "passiveQuietMs": 4000,
        "waitHorizonMs": 10_000,
        "maxWaits": 2,
        "maxEvents": 250,
        "maxWindowMs": 60_000,
        "retryBackoffMs": [1000, 4000, 16_000],
        "judgeTimeoutMs": 15_000,
        "deliveryTimeoutMs": 10_000,
        "recentCount": 8,
        "seenCount": 500,
        "maxOutbox": 1000,
        "maxDeadLetters": 1000,
        "deadLetterBackoffMaxMs": 60_000,
    }


DEFAULT_POLICY: SettlePolicy = default_policy()


class Buffer(TypedDict):
    """The open window as a snapshot. Empty entries mean no window is open."""

    entries: list[Entry]
    dropped: int
    gap: bool
    forcedAt: float | None
    endedAt: NotRequired[float]


class Window(TypedDict):
    """A settled window: what the judge sees and what a decision carries."""

    id: str
    events: list[Entry]
    reason: SettleReason
    waits: int
    dropped: int
    gap: bool
    settledAt: float


class JudgingPhase(TypedDict):
    phase: Literal["judging"]
    attempt: int
    failures: int
    leaseUntil: float
    offerWait: bool


class WaitingPhase(TypedDict):
    phase: Literal["waiting"]
    attempt: int
    until: float


class RetryingBatchPhase(TypedDict):
    phase: Literal["retrying"]
    attempt: int
    failures: int
    at: float
    offerWait: bool


type BatchPhase = JudgingPhase | WaitingPhase | RetryingBatchPhase


class Batch(TypedDict):
    window: Window
    phase: BatchPhase


class Verdict(TypedDict):
    id: str
    route: DecidedRoute
    at: float


class Decision(TypedDict):
    """What a sink receives. ``id`` is the window id, the sink's idempotency key."""

    id: str
    scope: str
    route: DeliveredRoute
    probabilities: dict[str, float]
    predicates: dict[str, float]
    confidence: float | None
    window: Window
    at: float


class DeadLetter(TypedDict):
    """A window no judge or sink could take. Delivered to the dead-letter handler and never dropped."""

    id: str
    scope: str
    stage: Literal["judge", "sink"]
    window: Window
    decision: Decision | None
    message: str
    permanent: bool
    attempts: int
    at: float


class InFlightPhase(TypedDict):
    phase: Literal["inFlight"]
    attempt: int
    failures: int
    leaseUntil: float


class RetryingDeliveryPhase(TypedDict):
    phase: Literal["retrying"]
    attempt: int
    failures: int
    at: float


type DeliveryPhase = InFlightPhase | RetryingDeliveryPhase


class DecisionItem(TypedDict):
    id: str
    kind: Literal["decision"]
    decision: Decision


class DeadLetterItem(TypedDict):
    id: str
    kind: Literal["deadLetter"]
    letter: DeadLetter


type DeliveryItem = DecisionItem | DeadLetterItem


class Delivery(TypedDict):
    item: DeliveryItem
    phase: DeliveryPhase


type OutboxLane = Literal["decisions", "deadLetters"]


class Outbox(TypedDict):
    decisions: list[Delivery]
    deadLetters: list[Delivery]


class ScopeRecord(TypedDict):
    """A scope's state as one JSON document: the snapshot format, and what ``inspect`` returns."""

    version: int
    scope: str
    cursors: dict[str, str | None]
    seen: list[str]
    buffer: Buffer
    batch: Batch | None
    outbox: Outbox
    recent: list[Verdict]
    dueAt: float | None


class Judgment(TypedDict):
    """What a judge returns. Validated against the offered routes inside ``step``."""

    route: Route
    probabilities: NotRequired[dict[str, float]]
    predicates: NotRequired[dict[str, float]]
    confidence: NotRequired[float]


class EventsInput(TypedDict):
    kind: Literal["events"]
    events: list[KindedEvent]
    source: NotRequired[str]
    cursor: NotRequired[str | None]
    truncated: NotRequired[bool]


class FlushInput(TypedDict):
    kind: Literal["flush"]


class EndInput(TypedDict):
    kind: Literal["end"]


class TickInput(TypedDict):
    kind: Literal["tick"]


class JudgeReleasedInput(TypedDict):
    kind: Literal["judgeReleased"]
    batchId: str
    attempt: int


class DeliveryReleasedInput(TypedDict):
    kind: Literal["deliveryReleased"]
    deliveryId: str
    attempt: int


class JudgeDeferredInput(TypedDict):
    kind: Literal["judgeDeferred"]
    batchId: str
    attempt: int
    until: float


class JudgedInput(TypedDict):
    kind: Literal["judged"]
    batchId: str
    attempt: int
    judgment: Judgment


class JudgeFailedInput(TypedDict):
    kind: Literal["judgeFailed"]
    batchId: str
    attempt: int
    message: str
    permanent: bool


class DeliveredInput(TypedDict):
    kind: Literal["delivered"]
    deliveryId: str
    attempt: int


class DeliveryFailedInput(TypedDict):
    kind: Literal["deliveryFailed"]
    deliveryId: str
    attempt: int
    message: str
    permanent: bool


type ReleasedInput = JudgeReleasedInput | DeliveryReleasedInput
type Input = (
    EventsInput
    | FlushInput
    | EndInput
    | TickInput
    | JudgeReleasedInput
    | DeliveryReleasedInput
    | JudgeDeferredInput
    | JudgedInput
    | JudgeFailedInput
    | DeliveredInput
    | DeliveryFailedInput
)
type InputKind = Literal[
    "events",
    "flush",
    "end",
    "tick",
    "judgeReleased",
    "deliveryReleased",
    "judgeDeferred",
    "judged",
    "judgeFailed",
    "delivered",
    "deliveryFailed",
]


class InFlightDelivery(TypedDict):
    item: DeliveryItem
    phase: InFlightPhase


class JudgeEffect(TypedDict):
    kind: Literal["judge"]
    scope: str
    batchId: str
    attempt: int
    window: Window
    offered: list[Route]
    recent: list[Verdict]
    leaseUntil: float


class DeliverEffect(TypedDict):
    kind: Literal["deliver"]
    scope: str
    delivery: InFlightDelivery


type Trace = dict[str, Any]
"""One observation, as plain JSON. Its ``kind`` names it; the fields of each kind are those of ``Trace`` in ``src/core/types.ts``."""


class TraceEffect(TypedDict):
    kind: Literal["trace"]
    trace: Trace


type Effect = JudgeEffect | DeliverEffect | TraceEffect


class Stepped(TypedDict):
    record: ScopeRecord
    effects: list[Effect]


class LogEntry(TypedDict):
    position: str
    event: Event


class SourceBatch(TypedDict):
    events: list[Event]
    cursor: NotRequired[str | None]
    truncated: NotRequired[bool]


class OfferedRoute(TypedDict):
    route: Route
    criterion: NotRequired[str]


class JudgeImage(TypedDict):
    """A still image for the judge, as base64 with no ``data:`` prefix."""

    data: str
    mimeType: Literal["image/png", "image/jpeg", "image/webp", "image/gif"]


class PredicateQuestion(TypedDict):
    name: str
    question: str


class JudgeInput(TypedDict):
    """What a judge decides from. Plain JSON, so a judge can run in another process or on a server."""

    scope: str
    window: Window
    routes: list[OfferedRoute]
    guidance: NotRequired[str]
    predicates: list[PredicateQuestion]
    facts: JsonObject
    images: NotRequired[list[JudgeImage]]
    recent: list[Verdict]
    now: float


class JudgeBudget(TypedDict):
    """What a judge declares about its input limit."""

    contextTokens: float
    reservedTokens: NotRequired[float]
    bytesPerToken: NotRequired[float]


class DecisionAppendDecision(TypedDict):
    kind: Literal["decision"]
    id: str
    scope: str
    at: float
    route: DeliveredRoute
    body: Decision


class DecisionAppendDeadLetter(TypedDict):
    kind: Literal["deadLetter"]
    id: str
    scope: str
    at: float
    route: DeliveredRoute | None
    body: DeadLetter


type DecisionAppend = DecisionAppendDecision | DecisionAppendDeadLetter


class LoggedInput(TypedDict):
    """What the store keeps per committed input: the input, its events already kinded, and the clock it was applied at."""

    input: Input
    now: float


type DeliveryMode = Literal["lane", "log"]


class StepConfig(TypedDict):
    policy: SettlePolicy
    decisions: NotRequired[DeliveryMode]
    deadLetters: NotRequired[DeliveryMode]


@dataclass(slots=True)
class ApplyConfig:
    """What ``apply`` needs from the config. Nothing async and no app code: kinds arrive in the logged events."""

    policy: SettlePolicy
    decisions: DeliveryMode = "lane"
    dead_letters: DeliveryMode = "lane"
    traces: bool = True


@dataclass(slots=True)
class ApplyOutput:
    """Filled by ``apply``. ``changed`` is false when the input changed nothing; the shell then neither logs nor writes it."""

    effects: list[Effect] = field(default_factory=lambda: list[Effect]())
    appends: list[DecisionAppend] = field(default_factory=lambda: list[DecisionAppend]())
    changed: bool = False


@dataclass(slots=True)
class FnvState:
    """FNV-1a 64 state. ``fold_id`` mutates it in place."""

    value: int


@dataclass(slots=True)
class IdRing:
    """The seen ring: the last ``capacity`` ingested ids, oldest at ``head``."""

    ids: list[str]
    head: int
    size: int
    capacity: int


@dataclass(slots=True)
class TriggerSummary:
    """Everything ``deadline`` reads, kept O(1). NaN means none for the ``*_at`` fields; ``min_flush_at`` and ``prefix_min_sheddable_at`` use inf."""

    min_flush_at: float = math.inf
    last_closing_at: float = math.nan
    last_stream_at: float = math.nan
    last_passive_at: float = math.nan
    first_act_at: float = math.nan
    first_act_index: int = -1
    prefix_min_sheddable_at: float = math.inf
    sheddable: int = 0
    last_seen_at: float = math.nan


@dataclass(slots=True)
class OpenWindow:
    """The open window, appended in place by its single writer."""

    entries: list[Entry]
    sizes: list[int]
    bytes: int
    dropped: int
    gap: bool
    forced_at: float | None
    ended_at: float | None
    summary: TriggerSummary
    hash: FnvState
    hash_valid: bool
    monotone: bool


@dataclass(slots=True)
class HeldBatch:
    window: Window
    phase: BatchPhase
    hash: FnvState | None


@dataclass(slots=True)
class ScopeState:
    """A scope's state, owned by exactly one writer and mutated in place by ``apply``.

    Containers that effects, snapshots or decision rows may hold (cursors, recent, Window,
    Delivery) are replaced, never mutated.
    """

    scope: str
    version: int
    cursors: dict[str, str | None]
    seen: IdRing
    known: dict[str, int]
    open: OpenWindow
    batch: HeldBatch | None
    decisions: list[Delivery]
    dead_letters: list[Delivery]
    recent: list[Verdict]
    due_at: float | None
